package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"validation-gateway/infra"
	tracer "validation-gateway/infra/tracing"
	"validation-gateway/internal"
	"validation-gateway/internal/job"
)

func main() {
	conf := internal.LoadConfig()

	db, err := infra.NewDatabase(conf)

	if err != nil {
		log.Fatalf("%v", err)
	}

	// Setup context for application shutdown
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// 1. Initialize OpenTelemetry with OTLP Exporter
	tp, err := tracer.NewTracerProvider("event-listener", ctx, conf)
	if err != nil {
		log.Fatalf("Failed to initialize OpenTelemetry: %v", err)
	}

	// 2. Ensure resources are properly shut down (flushing all pending traces)
	defer func() {
		log.Println("Shutting down OpenTelemetry Tracer Provider...")
		if err := tp.Shutdown(ctx); err != nil {
			log.Fatalf("Error shutting down tracer provider: %v", err)
		}
	}()

	// RabbitMQ URL
	rmq_url := fmt.Sprintf("amqp://%s:%s", conf.RMQHost, conf.RMQPort)

	handler := job.NewJobEventHandler(rmq_url, db)

	handler.Listening()
}
