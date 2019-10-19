package main

import (
	"bufio"
	"os"
)

func main() {
	// consuner
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
		dchan <- *docMsg
	}
}
