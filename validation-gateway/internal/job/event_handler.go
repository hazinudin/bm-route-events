package job

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"log"
	"time"
	"validation-gateway/infra"
	"validation-gateway/pkg/job"
	"validation-gateway/pkg/repo"

	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/ipc"
	"github.com/apache/arrow/go/v16/arrow/memory"
	amqp "github.com/rabbitmq/amqp091-go"
)

type JobEventHandler struct {
	job_queue  *JobQueue           // For publishing validation job to workers
	dispatcher *JobEventDispatcher // For publishing Job events
	rabbitmqCh *amqp.Channel       // For listening incoming events
	db         *infra.Database
	repo       *repo.ValidationJobRepository // For storing events
}

func NewJobEventHandler(url string, db *infra.Database) *JobEventHandler {
	conn, err := amqp.Dial(url)

	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}

	chann, err := conn.Channel()

	if err != nil {
		log.Fatalf("Failed to open channel: %v", err)
	}

	log.Printf("Connected to RabbitMQ at %s", url)

	queue := JobQueue{
		rabbitmqConn: conn,
		rabbitmqCh:   chann,
	}

	dispatcher := JobEventDispatcher{
		rabbitmqConn:   conn,
		rabbitmqCh:     chann,
		EventQueueName: "job_event_queue",
	}

	repo := repo.NewValidationJobRepository(db)

	return &JobEventHandler{
		job_queue:  &queue,
		dispatcher: &dispatcher,
		rabbitmqCh: chann,
		db:         db,
		repo:       repo,
	}
}

func (j *JobEventHandler) HandleCreatedEvent(event *job.JobCreated) error {
	err := j.repo.AppendEvents(event)

	if err != nil {
		return err
	}

	err = j.job_queue.PublishJob(event.Job)

	if err != nil {
		return err
	}

	// Create new submitted event
	new_event := job.JobSubmitted{
		JobEvent: job.JobEvent{
			JobID:     event.GetJobID(),
			OccuredAt: time.Now().UnixMilli(),
		},
	}

	err = j.dispatcher.PublishEvent(&new_event)

	if err != nil {
		return err
	}

	return nil
}

func (j *JobEventHandler) HandleSuccededEvent(event *job.JobSucceded) error {
	// Apache Arrow decoding and serialization
	arrowBytes, err := base64.StdEncoding.DecodeString(event.ArrowBatches)

	if err != nil {
		log.Printf("Failed to decode Arrow data: %v", err)
	}

	mem := memory.NewGoAllocator()
	r := bytes.NewReader(arrowBytes)
	reader, err := ipc.NewReader(r, ipc.WithAllocator(mem))

	if err != nil {
		return err
	}

	var rows [][]any

	for reader.Next() {
		rec := reader.Record()
		rec.Retain()
		defer rec.Release()

		num_rows := int(rec.NumRows())
		num_cols := int(rec.NumCols())

		for i := range num_rows {
			row := make([]any, num_cols+1)
			row[0] = event.JobID

			// Extract values from each column
			for colIdx := range num_cols {
				col := rec.Column(colIdx)

				switch arr := col.(type) {
				case *array.Int16:
					row[colIdx+1] = arr.Value(i)
				case *array.LargeString:
					row[colIdx+1] = arr.Value(i)
				default:
					log.Printf("Uhandled Arrow Array type: %T", arr)
					row[colIdx+1] = nil // Handle unknown types
				}
			}

			rows = append(rows, row)
		}
	}

	defer reader.Release()

	err = j.repo.InsertJobResults(rows)

	if err != nil {
		return err
	}

	err = j.repo.AppendEvents(event)

	if err != nil {
		return err
	}

	return nil
}

func (j *JobEventHandler) GenericHandler(event job.JobEventInterface) error {
	err := j.repo.AppendEvents(event)

	if err != nil {
		return err
	}

	return nil
}

func (j *JobEventHandler) Listening() {
	messages, err := j.rabbitmqCh.Consume(
		j.dispatcher.EventQueueName,
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		log.Fatalf("Failed to register consumer; %v", err)
	}

	for msg := range messages {
		var envelope job.EventEnvelope

		if err := json.Unmarshal(msg.Body, &envelope); err != nil {
			log.Printf("Failed to unmarshal message: %v", err)
			continue
		}

		switch envelope.Type {
		// Job created event handler
		case job.JOB_CREATED:
			var event job.JobCreated

			if err := json.Unmarshal(envelope.Payload, &event); err != nil {
				log.Printf("Failed to unmarshal into event: %v", err)
				continue
			}

			err := j.HandleCreatedEvent(&event)

			if err != nil {
				log.Printf("Failed to handle event: %v", err)
				continue
			}

		// Generic job handler, just append the events to event store
		case job.JOB_SUBMITTED, job.JOB_EXECUTED, job.JOB_FAILED:
			var event job.JobEventInterface

			switch envelope.Type {
			case job.JOB_SUBMITTED:
				event = &job.JobSubmitted{}
			case job.JOB_EXECUTED:
				event = &job.JobExecuted{}
			case job.JOB_FAILED:
				event = &job.JobFailed{}
			}

			if err := json.Unmarshal(envelope.Payload, event); err != nil {
				log.Printf("Failed to unmarshal into even: %v", err)
				continue
			}

			err := j.GenericHandler(event)

			if err != nil {
				log.Printf("Failed to handle event: %v", err)
				continue
			}

		// Job succeeded event handler
		case job.JOB_SUCCEEDED:
			var event job.JobSucceded
			var payload map[string]interface{}

			if err := json.Unmarshal(envelope.Payload, &event); err != nil {
				log.Printf("Failed to unmarshal into event: %v", err)
				continue
			}

			if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
				log.Printf("Failed to unmarshal into map: %v", err)
				continue
			}

			event.ArrowBatches = payload["arrow_batches"].(string)

			err := j.HandleSuccededEvent(&event)

			if err != nil {
				log.Printf("Failed to handle event: %v", err)
				continue
			}
		}
	}
}
