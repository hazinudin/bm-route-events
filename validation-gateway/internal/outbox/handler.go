package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	job_pkg "validation-gateway/pkg/job"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

func (c *OutboxConnector) insertHandler(
	logicalMsg *pglogrepl.InsertMessageV2,
	relations map[uint32]*pglogrepl.RelationMessageV2,
	typeMap *pgtype.Map,
) {
	rel, ok := relations[logicalMsg.RelationID]

	if !ok {
		log.Fatalf("unknown relation ID %d", logicalMsg.RelationID)
	}

	var event_type string
	var event_json []byte
	var row_id int32

	for idx, col := range logicalMsg.Tuple.Columns {
		colName := rel.Columns[idx].Name
		switch col.DataType {
		case 'n': // null
		case 'u': // unchanged toast
		case 't':
			val, err := decodeTextColumnData(typeMap, col.Data, rel.Columns[idx].DataType)

			if err != nil {
				log.Fatalln("error decoding column data: ", err)
			}

			switch colName {
			case "event_name":
				event_type = val.(string)
			case "payload":
				event_json, err = json.Marshal(val.(map[string]interface{}))

				if err != nil {
					log.Fatalln("error serializing event json: %w", err)
				}
			case "id":
				row_id = val.(int32)
			}
		}
	}

	switch event_type {
	case string(job_pkg.ALL_MSG_ACCEPTED):
		var event job_pkg.AllMessagesAccepted

		if err := json.Unmarshal(event_json, &event); err != nil {
			log.Fatalln("failed to unmarshal event: %w", err)
		}

		err := c.dispatcher.PublishEvent(&event)

		if err != nil {
			log.Fatalln("failed to publish event: %w", err)
		}
		log.Printf("job %s event %s published.", event.JobID, event_type)
	case string(job_pkg.JOB_CREATED):
		var event job_pkg.JobCreated

		if err := json.Unmarshal(event_json, &event); err != nil {
			log.Fatalln("failed to unmarshal event: %w", err)
		}

		err := c.dispatcher.PublishEvent(&event)

		if err != nil {
			log.Fatalln("failed to publish event: %w", err)
		}
		log.Printf("job %s event %s published.", event.JobID, event_type)
	case string(job_pkg.DISPUTED_MSG_ACCEPTED):
		var event job_pkg.DisputedMessagesAccepted

		if err := json.Unmarshal(event_json, &event); err != nil {
			log.Fatalln("failed to unmarshal event: %w", err)
		}

		err := c.dispatcher.PublishEvent(&event)

		if err != nil {
			log.Fatalln("failed to publish event: %w", err)
		}
		log.Printf("job %s event %s published.", event.JobID, event_type)
	case string(job_pkg.REVIEWED_MSG_ACCEPTED):
		var event job_pkg.ReviewedMessagesAccepted

		if err := json.Unmarshal(event_json, &event); err != nil {
			log.Fatalln("failed to unmarshal event: %w", err)
		}

		err := c.dispatcher.PublishEvent(&event)

		if err != nil {
			log.Fatalln("failed to publish event: %w", err)
		}
		log.Printf("job %s event %s published.", event.JobID, event_type)
	}

	// Delete row
	ctx := context.Background()
	tx, err := c.db.Pool.Begin(ctx)

	if err != nil {
		log.Fatalln("failed to begin transaction for deletion.")
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", c.outbox_table)

	_, err = tx.Exec(
		ctx,
		query,
		row_id,
	)

	if err != nil {
		log.Fatalf("failed to delete row %d", row_id)
	}

	tx.Commit(ctx)
}
