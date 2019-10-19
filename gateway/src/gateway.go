package main

import (
	"bufio"
	"os"
)

func main() {
	// consumer
	go initConsumer()

	// producer
	go initProducer()

	// read commandline input
	readInput()
}

func readInput() {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		name := scanner.Text()

		docMsg := docMsg(name)
		docMsg.ReplyTo = "gateway"

		msg := RabbitMsg{
			QueueName: "storage",
			Message:   *docMsg,
		}
		rchan <- msg
	}
}
