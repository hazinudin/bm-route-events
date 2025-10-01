package outbox

import (
	"context"
	"fmt"
	"log"
	"time"
	"validation-gateway/infra"
	"validation-gateway/internal"
	"validation-gateway/internal/job"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

type OutboxConnector struct {
	conn         *pgconn.PgConn
	slotName     string
	pluginArgs   []string
	sysIdent     *pglogrepl.IdentifySystemResult
	dispatcher   *job.JobEventDispatcher
	db           *infra.Database
	outbox_table string
}

func NewOutboxConnector(conf *internal.Config) (*OutboxConnector, error) {
	var (
		host     = conf.DBHost
		user     = conf.DBUsername
		password = conf.DBPassword
		dbPort   = conf.DBPort
		dbName   = conf.DBName
	)

	config_str := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?replication=database", user, password, host, dbPort, dbName)

	// Connection for logical replication
	conn, err := pgconn.Connect(context.Background(), config_str)
	slot_name := "outbox_slot"
	plugin_args := []string{
		"proto_version '2'",
		"publication_names 'outbox_publication'",
		"messages 'true'",
		"streaming 'true'",
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect for logical replication: %w", err)
	}
	log.Println("connected for logical replication")

	// Standard connection for data query
	db, err := infra.NewDatabase(conf)

	if err != nil {
		return nil, err
	}
	log.Println("connected for database query")

	// Query for the slot name
	rows, err := db.Pool.Query(
		context.Background(),
		"select slot_name from pg_replication_slots where slot_name = $1",
		slot_name,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to query slot name: %w", err)
	}
	defer rows.Close()

	// If there is no slot named with the same slot name, create new permanent slot
	if rows.Next() {
		log.Printf("slot %s already exists.", slot_name)
	} else {
		_, err = pglogrepl.CreateReplicationSlot(
			context.Background(),
			conn,
			slot_name,
			"pgoutput",
			pglogrepl.CreateReplicationSlotOptions{
				Temporary: false,
			},
		)

		if err != nil {
			return nil, fmt.Errorf("failed to create replication slot: %w", err)
		}
		log.Println("replication slot created")
	}

	// System identifier
	sysIdent, err := pglogrepl.IdentifySystem(context.Background(), conn)

	if err != nil {
		return nil, fmt.Errorf("failed to identify system: %w", err)
	}

	// Create event dispatcher
	rmq_url := fmt.Sprintf("amqp://%s:%s", conf.RMQHost, conf.RMQPort)
	dispatcher := job.NewJobEventDispatcher(rmq_url)

	var connector OutboxConnector

	connector.conn = conn
	connector.pluginArgs = plugin_args
	connector.slotName = slot_name
	connector.sysIdent = &sysIdent
	connector.dispatcher = dispatcher
	connector.db = db
	connector.outbox_table = "validation_job_outbox"

	return &connector, nil
}

// Send standby update to the primary.
func (c *OutboxConnector) sendStandbyUpdateToPrimary(lsn pglogrepl.LSN) error {
	err := pglogrepl.SendStandbyStatusUpdate(
		context.Background(),
		c.conn,
		pglogrepl.StandbyStatusUpdate{
			WALWritePosition: lsn + 1,
			WALFlushPosition: lsn + 1,
			WALApplyPosition: lsn + 1,
		},
	)

	if err != nil {
		return err
	}

	return nil
}

func decodeTextColumnData(mi *pgtype.Map, data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := mi.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(mi, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}

func (c *OutboxConnector) StartReplication() {
	//Try custom LSN
	lsn, _ := pglogrepl.ParseLSN("0/0")

	err := pglogrepl.StartReplication(
		context.Background(),
		c.conn,
		c.slotName,
		lsn,
		pglogrepl.StartReplicationOptions{
			PluginArgs: c.pluginArgs,
		},
	)

	if err != nil {
		log.Fatalln("failed to start replication: ", err)
	}

	//stand by message timeout
	standbyMessageTimeout := time.Second * 10

	//the next timeout is basically current time + timeout
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	//client XLog Position or WAL position
	XLogPos := lsn

	// instream value
	inStream := false

	relations := map[uint32]*pglogrepl.RelationMessageV2{}
	typeMap := pgtype.NewMap()

	// Start the loop
	for {
		if time.Now().After(nextStandbyMessageDeadline) {
			err = c.sendStandbyUpdateToPrimary(XLogPos)

			if err != nil {
				log.Println("SendStandbyStatusUpdate failed: ", err)
			}
			log.Printf("sent standby status message at %s + 1\n", XLogPos.String())

			// Refreshed to the next deadline
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
		raw_msg, err := c.conn.ReceiveMessage(ctx)
		cancel()

		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}

			log.Fatalln("ReceiveMessage failed: ", err)
		}

		if errMsg, ok := raw_msg.(*pgproto3.ErrorResponse); ok {
			log.Fatalf("received postgres WAL error: %+v", errMsg)
		}

		msg, ok := raw_msg.(*pgproto3.CopyData)

		if !ok {
			log.Fatalln("received unexpected message")
		}

		switch msg.Data[0] {
		// Keep alive message
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])

			if err != nil {
				log.Fatalln("parse primary keep alive message failed:", err)
			}

			// Referesh the client WAL position using server WAL
			// if pkm.ServerWALEnd > XLogPos {
			// 	XLogPos = pkm.ServerWALEnd
			// }

			// Forced to reply
			if pkm.ReplyRequested {
				nextStandbyMessageDeadline = time.Time{}
			}

		// The transaction log data
		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])

			if err != nil {
				log.Fatalln("failed to parse XLogData")
			}

			walData := xld.WALData
			logicalMsg, err := pglogrepl.ParseV2(walData, inStream)

			if err != nil {
				log.Fatal("failed to parse WAL data: ", err)
			}

			switch logicalMsg := logicalMsg.(type) {
			case *pglogrepl.RelationMessageV2:
				relations[logicalMsg.RelationID] = logicalMsg
			case *pglogrepl.BeginMessage:
				log.Printf("begin message - WAL end: %s, Final LSN: %s", xld.ServerWALEnd.String(), logicalMsg.FinalLSN.String())
				XLogPos = logicalMsg.FinalLSN // Update the client position to final LSN

			case *pglogrepl.InsertMessageV2:
				c.insertHandler(logicalMsg, relations, typeMap)
				c.sendStandbyUpdateToPrimary(XLogPos)
			}
		}
	}
}
