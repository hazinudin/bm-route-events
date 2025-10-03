package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	tracer "validation-gateway/infra/tracing"
	"validation-gateway/internal"
	"validation-gateway/internal/outbox"
)

func main() {
	conf := internal.LoadConfig()
	connector, err := outbox.NewOutboxConnector(conf)

	if err != nil {
		log.Fatalf("%v", err)
	}

	// Setup context for application shutdown
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// 1. Initialize OpenTelemetry with OTLP Exporter
	tp, err := tracer.NewTracerProvider("outbox-connector", ctx, conf)
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

	connector.StartReplication()
}
