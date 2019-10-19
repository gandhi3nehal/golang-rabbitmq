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

	// document REST api
	initApi()
}
