package job

import (
	"fmt"
	"log"
	"validation-gateway/pkg/job"

	amqp "github.com/rabbitmq/amqp091-go"
)

type JobQueue struct {
	rabbitmqConn *amqp.Connection
	rabbitmqCh   *amqp.Channel
}

func NewJobQueueClient(url string) *JobQueue {
	var err error

	conn, err := amqp.Dial(url)

	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}

	chann, err := conn.Channel()

	if err != nil {
		log.Fatalf("Failed to open channel: %v", err)
	}

	_, err = chann.QueueDeclare(
		"validation_queue",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		log.Fatalf("Failed to declare queue: %v", err)
	}

	log.Printf("Connected to RabbitMQ at %s", url)

	return &JobQueue{
		rabbitmqConn: conn,
		rabbitmqCh:   chann,
	}
}

func (jq *JobQueue) PublishJob(job *job.ValidationJob) error {
	body, err := job.AsJobMessage()

	if err != nil {
		return fmt.Errorf("failed to marshal json: %w", err)
	}

	err = jq.rabbitmqCh.Publish(
		"",
		"validation_queue",
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         body,
		},
	)

	if err != nil {
		return fmt.Errorf("failed to publish job: %w", err)
	}

	return nil
}
