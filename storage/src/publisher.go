package main

import (
	spec "github.com/erangaeb/document-spec/document-spec"
	"github.com/golang/protobuf/proto"
	"github.com/streadway/amqp"
	"log"
	"os"
)

type RabbitMsg struct {
	QueueName string                   `json:"queueName"`
	Reply     spec.CreateDocumentReply `json:"reply"`
}

// channel to publish rabbit messages
var rchan = make(chan RabbitMsg, 10)

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
		case msg := <-rchan:
			// marshal
			data, err := proto.Marshal(&msg.Reply)
			if err != nil {
				log.Printf("ERROR: fail marshal: %s", err.Error())
				continue
			}

			// publish message
			err = amqpChannel.Publish(
				"",            // exchange
				msg.QueueName, // routing key
				false,         // mandatory
				false,         // immediate
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        data,
				},
			)
			if err != nil {
				log.Printf("ERROR: fail publish msg: %s", err.Error())
				continue
			}

			log.Printf("INFO: published msg: %v to: %s", msg.Reply, msg.QueueName)
		}
	}
}
