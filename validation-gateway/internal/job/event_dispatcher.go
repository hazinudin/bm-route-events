package job

import (
	"context"
	"fmt"
	"log"
	tracer "validation-gateway/infra/tracing"
	"validation-gateway/pkg/job"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
)

type JobEventDispatcher struct {
	rabbitmqConn   *amqp.Connection
	rabbitmqCh     *amqp.Channel
	EventQueueName string
}

func NewJobEventDispatcher(url string) *JobEventDispatcher {
	var err error
	event_queue_name := "job_event_queue"

	conn, err := amqp.Dial(url)

	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}

	chann, err := conn.Channel()

	if err != nil {
		log.Fatalf("Failed to open channel: %v", err)
	}

	_, err = chann.QueueDeclare(
		event_queue_name,
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

	return &JobEventDispatcher{
		rabbitmqConn:   conn,
		rabbitmqCh:     chann,
		EventQueueName: event_queue_name,
	}
}

func (je *JobEventDispatcher) PublishEvent(event job.JobEventInterface, ctx context.Context) error {
	headers := make(AmqpHeadersCarrier)

	propagator := otel.GetTextMapPropagator()

	tracer_ := otel.Tracer("event-publishing")
	ctx, span := tracer_.Start(ctx, "event-publishing")
	defer span.End()

	propagator.Inject(ctx, headers)
	log.Printf("trace ID: %s", tracer.GetTraceID(ctx))
	log.Printf("headers %+v", &headers)
	log.Printf("traceparent injected: %v", headers["traceparent"])

	body, err := event.SerializeToEnvelope()

	if err != nil {
		return fmt.Errorf("failed to serialize event: %w", err)
	}

	err = je.rabbitmqCh.Publish(
		"",
		"job_event_queue",
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         body,
			Headers:      amqp.Table(headers),
		},
	)

	if err != nil {
		return fmt.Errorf("failed to publish job: %w", err)
	}

	return nil
}
