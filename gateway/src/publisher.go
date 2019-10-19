package main

import (
	spec "github.com/erangaeb/ops-spec/ops-spec"
	"github.com/golang/protobuf/proto"
	"github.com/streadway/amqp"
	"log"
	"os"
	"time"
)

type RabbitMsg struct {
	QueueName string                     `json:"queueName"`
	Message   spec.CreateDocumentMessage `json:"message"`
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
			data, err := proto.Marshal(&msg.Message)
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

			log.Printf("INFO: published msg: %v", msg.Message)

			// wait reply from rabbit
			//go waitReply(conn, docMsg)
		}
	}
}

func waitReply(conn *amqp.Connection, docMsg spec.CreateDocumentMessage) {
	// create channel
	amqpChannel, err := conn.Channel()
	if err != nil {
		log.Printf("ERROR: fail create channel: %s", err.Error())
		os.Exit(1)
	}

	// create queue
	queue, err := amqpChannel.QueueDeclare(
		docMsg.ReplyTo, // channelname
		false,          // durable
		true,           // delete when unused
		false,          // exclusive
		false,          // no-wait
		nil,            // arguments
	)
	if err != nil {
		log.Printf("ERROR: fail create queue: %s", err.Error())
		os.Exit(1)
	}

	// channel
	msgChannel, err := amqpChannel.Consume(
		queue.Name, // queue
		"",         // consumer
		false,      // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		log.Printf("ERROR: fail create channel: %s", err.Error())
		os.Exit(1)
	}

	// consume
	for {
		select {
		case msg := <-msgChannel:
			// unmarshal
			log.Printf("INFO: got reply: %v", msg)
			docRply := &spec.CreateDocumentReply{}
			err = proto.Unmarshal(msg.Body, docRply)
			if err != nil {
				log.Printf("ERROR: fail unmarshl: %s", msg.Body)
				continue
			}
			log.Printf("INFO: received reply: %v", docRply)

			// ack for message
			err = msg.Ack(true)
			if err != nil {
				log.Printf("ERROR: fail to ack: %s", err.Error())
			}
			return
		case <-time.After(10 * time.Second):
			log.Printf("ERROR: timeout request")
			return
		}
	}
}
