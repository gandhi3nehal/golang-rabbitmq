package main

import (
	spec "github.com/erangaeb/ops-spec/ops-spec"
	"github.com/golang/protobuf/proto"
	"github.com/streadway/amqp"
	"log"
	"os"
)

// channel to publish rabbit messages
var dchan = make(chan spec.CreateDocumentMessage, 10)

func initProducer() {
	// conn
	conn, err := amqp.Dial("amqp://admin:admin@localhost:5672/")
	if err != nil {
		log.Printf("ERROR: fail init consumer: %s", err.Error())
		os.Exit(1)
	}

	log.Printf("INFO: done init producer conn")

	// create channel
	amqpChannel, err := conn.Channel()
	if err != nil {
		log.Printf("ERROR: fail create channel: %s", err.Error())
		os.Exit(1)
	}

	for {
		select {
		case docMsg := <-dchan:
			// marshal
			data, err := proto.Marshal(&docMsg)
			if err != nil {
				log.Printf("ERROR: fail marshal: %s", err.Error())
				continue
			}

			// publish message
			err = amqpChannel.Publish(
				"",      // exchange
				"hakka", // routing key
				false,   // mandatory
				false,   // immediate
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        data,
				},
			)
			if err != nil {
				log.Printf("ERROR: fail publish msg: %s", err.Error())
				continue
			}

			log.Printf("INFO: published msg: %v", docMsg)
		}
	}
}
