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

	http.HandleFunc("POST /road/{data_type}/validation/submitJob", middleware.Auth(server.PublishSMDValidationHandler, []byte(conf.TokenSecret)))
	http.HandleFunc("POST /bridge/{data_type}/validation/submitJob", middleware.Auth(server.PublishINVIJValidationHandler, []byte(conf.TokenSecret)))
	http.HandleFunc("POST /login", server.LoginHandler)

	log.Printf("Server starting on port %s", ":8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
