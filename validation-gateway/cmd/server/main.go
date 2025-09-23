package main

import (
	"log"
	"net/http"
	"validation-gateway/infra"
	"validation-gateway/internal"
	"validation-gateway/internal/job"
	"validation-gateway/internal/user"
	"validation-gateway/middleware"
)

type Server struct {
	job_service  *job.JobService
	user_service *user.UserService
	conf         *internal.Config
}

func main() {
	conf := internal.LoadConfig()

	db, err := infra.NewDatabase(conf)

	if err != nil {
		log.Fatalf("%v", err)
	}

	server := Server{
		job_service:  job.NewJobService(db, conf),
		user_service: user.NewUserService(db),
		conf:         conf,
	}

	// Road data
	http.HandleFunc("POST /road/{data_type}/validation/submit", middleware.Auth(server.PublishSMDValidationHandler, []byte(conf.TokenSecret)))

	// Job actions
	http.HandleFunc("GET /validation/{job_id}/status", middleware.Auth(server.GetJobStatusHandler, []byte(conf.TokenSecret)))
	http.HandleFunc("GET /validation/{job_id}/result", middleware.Auth(server.GetJobResultHandler, []byte(conf.TokenSecret)))
	http.HandleFunc("GET /validation/{job_id}/result/msg", middleware.Auth(server.GetJobResultMessages, []byte(conf.TokenSecret)))
	http.HandleFunc("POST /validation/{job_id}/result/accept_disputed", middleware.Auth(server.AcceptDisputedMessages, []byte(conf.TokenSecret)))
	http.HandleFunc("POST /validation/{job_id}/result/accept_reviewed", middleware.Auth(server.AcceptReviewedMessages, []byte(conf.TokenSecret)))

	// Bridge data
	http.HandleFunc("POST /bridge/{data_type}/validation/submit", middleware.Auth(server.PublishINVIJValidationHandler, []byte(conf.TokenSecret)))

	// Login
	http.HandleFunc("POST /login", server.LoginHandler)

	log.Printf("Server starting on port %s", ":8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
