package job

import (
	"context"
	"fmt"
	"log"
	"validation-gateway/pkg/job"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
)

type JobQueue struct {
	rabbitmqConn *amqp.Connection
	rabbitmqCh   *amqp.Channel
}

type AmqpHeadersCarrier map[string]interface{}

func (a AmqpHeadersCarrier) Get(key string) string {
	v, ok := a[key]
	if !ok {
		return ""
	}
	return v.(string)
}

func (a AmqpHeadersCarrier) Set(key string, value string) {
	a[key] = value
}

func (a AmqpHeadersCarrier) Keys() []string {
	i := 0
	r := make([]string, len(a))

	for k := range a {
		r[i] = k
		i++
	}

	return r
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

// Publish the job to validation job queue
//
// 'validate' parameter will determine whether the job will be execute all validation function or simply ran through basic check and written if verified.
// 'validate' false should be used for job which has all error messages accepted (disputed or reviewed)
func (jq *JobQueue) PublishJob(job *job.ValidationJob, validate bool) error {
	body, err := job.AsJobMessage(validate)

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
