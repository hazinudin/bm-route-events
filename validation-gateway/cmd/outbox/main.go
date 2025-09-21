package main

import (
	"log"
	"validation-gateway/internal"
	"validation-gateway/internal/outbox"
)

func main() {
	conf := internal.LoadConfig()
	connector, err := outbox.NewOutboxConnector(conf)

	if err != nil {
		log.Fatalf("%v", err)
	}

	connector.StartReplication()
}
