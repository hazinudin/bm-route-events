package job

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"validation-gateway/pkg/job"

	"github.com/apache/arrow/go/v16/arrow/ipc"
	"github.com/apache/arrow/go/v16/arrow/memory"
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

func (jq *JobQueue) ListenJobResult() {
	messages, err := jq.rabbitmqCh.Consume(
		"result_queue",
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		log.Printf("Failed to register consumer: %v", err)
	}

	log.Print("Listening to result_queue queue.")

	for msg := range messages {
		var result map[string]string

		if err := json.Unmarshal(msg.Body, &result); err != nil {
			log.Printf("Failed to unmarshal result: %v", err)
			continue
		}

		// Apache Arrow decoding and serialization
		arrowBytes, err := base64.StdEncoding.DecodeString(result["result"])

		if err != nil {
			log.Printf("Failed to decode Arrow data: %v", err)
		}

		mem := memory.NewGoAllocator()
		r := bytes.NewReader(arrowBytes)
		reader, err := ipc.NewReader(r, ipc.WithAllocator(mem))

		if err != nil {
			log.Printf("%s", err)
		}

		for reader.Next() {
			rec := reader.Record()
			log.Printf("Received result from job: %s, Arrow rows: %d, Arrow schema: %s", result["jobid"], rec.NumRows(), rec.Schema())
		}

	}
}
