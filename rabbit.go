package main

import (
	"bufio"
	spec "github.com/erangaeb/ops-spec/ops-spec"
	"github.com/golang/protobuf/proto"
	"github.com/streadway/amqp"
	"log"
	"os"
)

type Rmsg struct {
	Queue string `json:"queue"`
	Uid   string `json:"uid"`
	Msg   string `json:"msg"`
}

// channel to publish rabbit messages
var rchan = make(chan Rmsg, 10)

func main() {
	// consuner
	go initConsumer()

	// producer
	go initProducer()
	// read commandline input
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		msg := scanner.Text()
		rmsg := Rmsg{
			Queue: "hakka",
			Uid:   "uid",
			Msg:   msg,
		}
		rchan <- rmsg
	}
}

func initConsumer() {
	// conn
	conn, err := amqp.Dial("amqp://admin:admin@localhost:5672/")
	if err != nil {
		log.Printf("ERROR: fail init consumer: %s", err.Error())
		os.Exit(1)
	}

	log.Printf("INFO: done init consumer conn")

	// create channel
	amqpChannel, err := conn.Channel()
	if err != nil {
		log.Printf("ERROR: fail create channel: %s", err.Error())
		os.Exit(1)
	}

	// create queue
	queue, err := amqpChannel.QueueDeclare(
		"hakka", // channelname
		false,   // durable
		false,   // delete when unused
		true,    // exclusive
		false,   // no-wait
		nil,     // arguments
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
			// create person
			p := &spec.Person{}
			err = proto.Unmarshal(msg.Body, p)
			if err != nil {
				log.Printf("ERROR: fail unmarshl: %s", msg.Body)
				continue
			}
			log.Printf("INFO: got obj: %v", p)

			// ack for message
			err = msg.Ack(true)
			if err != nil {
				log.Printf("ERROR: fail to ack: %s", err.Error())
			}
		}
	}
}

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
		case rmsg := <-rchan:
			// person
			p := &spec.Person{
				Name: rmsg.Msg,
				Age:  24,
			}

			// marshal
			data, err := proto.Marshal(p)
			if err != nil {
				log.Printf("ERROR: fail marshal: %s", err.Error())
				continue
			}

			// publish message
			err = amqpChannel.Publish(
				"",         // exchange
				rmsg.Queue, // routing key
				false,      // mandatory
				false,      // immediate
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        data,
				},
			)
			if err != nil {
				log.Printf("ERROR: fail publish: %s", err.Error())
				continue
			}

			log.Printf("INFO: published obj: %v, uid: %s, queue: %s",
				p, rmsg.Uid, rmsg.Queue)
		}
	}
}
