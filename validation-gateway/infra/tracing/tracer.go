package tracer

import (
	"context"
	"fmt"
	"log"
	"validation-gateway/internal"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
)

// initTracerProvider creates and registers the OTLP gRPC TracerProvider.
func NewTracerProvider(service_name string, ctx context.Context, conf *internal.Config) (*sdktrace.TracerProvider, error) {
	// 1. Create the OTLP Exporter
	// We use WithInsecure() because we are connecting to a local collector without TLS.
	// We use WithEndpoint() to explicitly set the collector address and port.
	// WithDialOption is needed to ensure the connection is attempted synchronously.
	collectorEndpoint := conf.OtelExporterHost + ":" + conf.OtelExporterPort
	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithEndpoint(collectorEndpoint),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP trace exporter: %w", err)
	}

	// 2. Define the service resource
	r, err := resource.New(ctx,
		resource.WithAttributes(
			// REQUIRED: Specify the service name for traces in Jaeger
			semconv.ServiceNameKey.String(service_name),
			attribute.String("environment", "production"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// 3. Create the TracerProvider
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter), // Use a batch processor for efficiency
		sdktrace.WithResource(r),
	)

	// 4. Set the TracerProvider and propagator globally
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	log.Printf("OpenTelemetry initialized, exporting to OTLP gRPC at %s", collectorEndpoint)
	return tp, nil
}

// Extract Trace ID from the trace context.
func GetTraceID(ctx context.Context) string {
	spanCtx := trace.SpanContextFromContext(ctx)

	if spanCtx.HasTraceID() {
		return spanCtx.TraceID().String()
	}

	return ""
}
