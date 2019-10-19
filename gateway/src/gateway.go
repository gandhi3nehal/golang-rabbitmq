package main

import (
	"bufio"
	"os"
)

func main() {
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
		rchan <- *docMsg
	}
}
