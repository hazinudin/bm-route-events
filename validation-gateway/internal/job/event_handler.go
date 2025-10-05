package job

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"time"
	"validation-gateway/infra"
	"validation-gateway/pkg/job"
	"validation-gateway/pkg/repo"

	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/ipc"
	"github.com/apache/arrow/go/v16/arrow/memory"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
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

func (j *JobEventHandler) HandleCreatedEvent(event *job.JobCreated, ctx context.Context) error {
	tracer := otel.Tracer("event-handling")
	ctx, span := tracer.Start(ctx, "job-created-handling")
	defer span.End()

	err := j.repo.AppendEvents(event)

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	//validate = true, meaning validation will be executed
	err = j.job_queue.PublishJob(event.Job, true, ctx)

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	// Create new submitted event
	new_event := job.JobSubmitted{
		JobEvent: job.JobEvent{
			JobID:     event.GetJobID(),
			OccuredAt: time.Now().UnixMilli(),
		},
	}

	err = j.dispatcher.PublishEvent(&new_event, ctx)

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	span.SetStatus(codes.Ok, "event handling finished")
	return nil
}

func (j *JobEventHandler) HandleAllMsgAccepted(event *job.AllMessagesAccepted, ctx context.Context) error {
	tracer := otel.Tracer("event-handling")
	_, span := tracer.Start(ctx, "all-msg-accepted-handling")
	defer span.End()

	job_, err := j.repo.GetValidationJob(event.JobID)

	if err != nil {
		return err
	}

	// validate set to false
	err = j.job_queue.PublishJob(job_, false, ctx)

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

	err = j.dispatcher.PublishEvent(&new_event, ctx)

	if err != nil {
		return err
	}

	err = j.repo.AppendEvents(event)

	if err != nil {
		return err
	}

	return nil
}

func (j *JobEventHandler) HandleSucceededEvent(event *job.JobSuccedeed, ctx context.Context) error {
	tracer := otel.Tracer("event-handling")
	_, span := tracer.Start(ctx, "job-succeded-handling")
	defer span.End()

	// Apache Arrow decoding and serialization
	arrowBytes, err := base64.StdEncoding.DecodeString(event.ArrowBatches)

	if err != nil {
		log.Printf("failed to decode Arrow data: %v", err)
		return err
	}

	// Get the attempt number/attempt ID
	attempt_id, err := j.repo.GetJobAttemptNumber(event.JobID)

	if err != nil {
		log.Printf("failed to fetch the attempt ID for %s: %v", event.JobID, err)
		return err
	}

	// Set the attempt ID
	event.Result.AttemptID = attempt_id

	err = j.repo.InsertJobResult(event.Result)

	if err != nil {
		log.Printf("failed to insert: %v", err)
		return err
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
			row := make([]any, num_cols+2)
			row[0] = event.JobID
			row[1] = attempt_id

			// Extract values from each column
			for colIdx := range num_cols {
				col := rec.Column(colIdx)

				switch arr := col.(type) {
				case *array.Int16:
					row[colIdx+2] = arr.Value(i)
				case *array.LargeString:
					row[colIdx+2] = arr.Value(i)
				default:
					log.Printf("Uhandled Arrow Array type: %T", arr)
					row[colIdx+2] = nil // Handle unknown types
				}
			}

			rows = append(rows, row)
		}
	}

	defer reader.Release()

	err = j.repo.InsertJobResultMessages(rows)

	if err != nil {
		return err
	}

	err = j.repo.AppendEvents(event)

	if err != nil {
		return err
	}

	return nil
}

func (j *JobEventHandler) GenericHandler(event job.JobEventInterface, ctx context.Context) error {
	tracer := otel.Tracer("event-handling")
	_, span := tracer.Start(ctx, "job-generic-handling")
	defer span.End()

	err := j.repo.AppendEvents(event)

	if err != nil {
		return err
	}

	return nil
}

func (j *JobEventHandler) HandleJobRetried(event *job.JobRetried, ctx context.Context) error {
	tracer := otel.Tracer("event-handling")
	ctx, span := tracer.Start(ctx, "job-retried-handling")
	defer span.End()

	job_, err := j.repo.GetValidationJob(event.JobID)

	if err != nil {
		return fmt.Errorf("failed to fetch job data: %w", err)
	}

	err = j.job_queue.PublishJob(job_, true, ctx)

	if err != nil {
		return fmt.Errorf("failed to publish job to message queue: %w", err)
	}

	j.repo.AppendEvents(event)

	new_event := job.JobSubmitted{
		JobEvent: job.JobEvent{
			JobID:     event.JobID,
			OccuredAt: time.Now().UnixMilli(),
		},
	}

	err = j.dispatcher.PublishEvent(&new_event, ctx)

	if err != nil {
		return fmt.Errorf("failed to publish submitted event: %w", err)
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
		ctx := context.Background()

		headers := AmqpHeadersCarrier(msg.Headers)
		propagator := otel.GetTextMapPropagator()
		parentCtx := propagator.Extract(ctx, &headers)
		log.Printf("headers %+v", &headers)

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

			err := j.HandleCreatedEvent(&event, parentCtx)

			if err != nil {
				log.Printf("Failed to handle event: %v", err)
				continue
			}

		// Generic job handler, just append the events to event store
		case job.JOB_SUBMITTED, job.JOB_EXECUTED, job.JOB_FAILED, job.DISPUTED_MSG_ACCEPTED, job.REVIEWED_MSG_ACCEPTED:
			var event job.JobEventInterface

			switch envelope.Type {
			case job.JOB_SUBMITTED:
				event = &job.JobSubmitted{}
			case job.JOB_EXECUTED:
				event = &job.JobExecuted{}
			case job.JOB_FAILED:
				event = &job.JobFailed{}
			case job.DISPUTED_MSG_ACCEPTED:
				event = &job.DisputedMessagesAccepted{}
			case job.REVIEWED_MSG_ACCEPTED:
				event = &job.ReviewedMessagesAccepted{}
			}

			if err := json.Unmarshal(envelope.Payload, event); err != nil {
				log.Printf("Failed to unmarshal into even: %v", err)
				continue
			}

			err := j.GenericHandler(event, parentCtx)

			if err != nil {
				log.Printf("Failed to handle event: %v", err)
				continue
			}

		// All messages accepted handler
		case job.ALL_MSG_ACCEPTED:
			var event job.AllMessagesAccepted

			if err := json.Unmarshal(envelope.Payload, &event); err != nil {
				log.Printf("Failed to unmarshal into event: %v", err)
				continue
			}

			err := j.HandleAllMsgAccepted(&event, parentCtx)

			if err != nil {
				log.Printf("Failed to handle event: %v", err)
				continue
			}

		// Job retried handler
		case job.JOB_RETRIED:
			var event job.JobRetried

			if err := json.Unmarshal(envelope.Payload, &event); err != nil {
				log.Printf("Failed to unmarshal into event: %v", err)
				continue
			}

			err := j.HandleJobRetried(&event, parentCtx)

			if err != nil {
				log.Printf("Failed to handle job retried event: %v", err)
				continue
			}

		// Job succeeded event handler
		case job.JOB_SUCCEEDED:
			var event job.JobSuccedeed
			var payload map[string]any

			if err := json.Unmarshal(envelope.Payload, &event); err != nil {
				log.Printf("Failed to unmarshal into event: %v", err)
				continue
			}

			if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
				log.Printf("Failed to unmarshal into map: %v", err)
				continue
			}

			event.ArrowBatches = payload["arrow_batches"].(string)

			err := j.HandleSucceededEvent(&event, parentCtx)

			if err != nil {
				log.Printf("Failed to handle event: %v", err)
				continue
			}
		}
	}
}
