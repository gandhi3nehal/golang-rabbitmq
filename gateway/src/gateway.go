package main

import (
	"bufio"
	spec "github.com/erangaeb/ops-spec/ops-spec"
	"log"
	"os"
	"time"
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

		// create channel and add to rchans with uid
		rchan := make(chan spec.CreateDocumentReply)
		rchans[docMsg.Uid] = rchan

		// publish rabbit message
		msg := RabbitMsg{
			QueueName: "storage",
			Message:   *docMsg,
		}
		pchan <- msg

		// wait for reply
		waitReply(docMsg.Uid, rchan)
	}
}

func waitReply(uid string, rchan chan spec.CreateDocumentReply) {
	for {
		select {
		case docReply := <-rchan:
			// responses received
			log.Printf("INFO: received reply: %v uid: %s", docReply, uid)

			// remove channel from rchans
			delete(rchans, uid)
			return
		case <-time.After(10 * time.Second):
			// timeout
			log.Printf("ERROR: request timeout uid: %s", uid)

			// remove channel from rchans
			delete(rchans, uid)
			return
		}
	}
}
