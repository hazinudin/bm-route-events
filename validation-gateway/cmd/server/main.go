package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"validation-gateway/infra"
	tracer "validation-gateway/infra/tracing"
	"validation-gateway/internal"
	"validation-gateway/internal/job"
	"validation-gateway/internal/user"
	"validation-gateway/middleware"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

type Server struct {
	job_service  *job.JobService
	user_service *user.UserService
	conf         *internal.Config
}

func httpSpanName(operation string, req *http.Request) string {
	return fmt.Sprintf("%s %s", req.Method, req.Pattern)
}

func main() {
	conf := internal.LoadConfig()

	// Setup context for application shutdown
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// 1. Initialize OpenTelemetry with OTLP Exporter
	tp, err := tracer.NewTracerProvider("validation-api-server", ctx, conf)
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

	db, err := infra.NewDatabase(conf)

	if err != nil {
		log.Fatalf("%v", err)
	}

	server := Server{
		job_service:  job.NewJobService(db, conf),
		user_service: user.NewUserService(db),
		conf:         conf,
	}

	mux := http.NewServeMux()

	// Road data
	mux.HandleFunc("POST /road/{data_type}/validation/submit", middleware.Auth(server.PublishSMDValidationHandler, []byte(conf.TokenSecret)))
	mux.HandleFunc("GET /road/get_job_id", middleware.Auth(server.GetSMDJobIDHandler, []byte(conf.TokenSecret)))

	// Job actions
	mux.HandleFunc("POST /validation/{job_id}/retry", middleware.Auth(server.RetryJobHandler, []byte(conf.TokenSecret)))
	mux.HandleFunc("GET /validation/{job_id}/status", middleware.Auth(server.GetJobStatusHandler, []byte(conf.TokenSecret)))
	mux.HandleFunc("GET /validation/{job_id}/result", middleware.Auth(server.GetJobResultHandler, []byte(conf.TokenSecret)))
	mux.HandleFunc("GET /validation/{job_id}/result/msg", middleware.Auth(server.GetJobResultMessages, []byte(conf.TokenSecret)))
	mux.HandleFunc("POST /validation/{job_id}/result/accept_disputed", middleware.Auth(server.AcceptDisputedMessages, []byte(conf.TokenSecret)))
	mux.HandleFunc("POST /validation/{job_id}/result/accept_reviewed", middleware.Auth(server.AcceptReviewedMessages, []byte(conf.TokenSecret)))

	// Bridge data
	mux.HandleFunc("POST /bridge/{data_type}/validation/submit", middleware.Auth(server.PublishINVIJValidationHandler, []byte(conf.TokenSecret)))

	// Login
	mux.HandleFunc("POST /login", server.LoginHandler)

	otel_handler := otelhttp.NewHandler(mux, "", otelhttp.WithSpanNameFormatter(httpSpanName))

	log.Printf("Server starting on port %s", ":8080")
	log.Fatal(http.ListenAndServe(":8080", otel_handler))
}
