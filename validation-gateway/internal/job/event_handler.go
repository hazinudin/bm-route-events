package job

import (
	"encoding/json"
	"log"
	"time"
	"validation-gateway/infra"
	"validation-gateway/pkg/job"
	"validation-gateway/pkg/repo"

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

		// Job submitted event handler
		case job.JOB_SUBMITTED:
			var event job.JobSubmitted

			if err := json.Unmarshal(envelope.Payload, &event); err != nil {
				log.Printf("Failed to unmarshal into even: %v", err)
				continue
			}

			err := j.GenericHandler(&event)

			if err != nil {
				log.Printf("Failed to handle event: %v", err)
				continue
			}

		}
	}
}
