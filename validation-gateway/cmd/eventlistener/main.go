package main

import (
	"fmt"
	"log"
	"validation-gateway/infra"
	"validation-gateway/internal"
	"validation-gateway/internal/job"
)

func main() {
	conf := internal.LoadConfig()

	db, err := infra.NewDatabase(conf)

	if err != nil {
		log.Fatalf("%v", err)
	}

	// RabbitMQ URL
	rmq_url := fmt.Sprintf("amqp://%s:%s", conf.RMQHost, conf.RMQPort)

	handler := job.NewJobEventHandler(rmq_url, db)

	handler.Listening()
}
