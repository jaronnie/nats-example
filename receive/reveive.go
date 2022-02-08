package main

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	// Connect to a server
	nc, err := nats.Connect("nats://127.0.0.1:4222")
	if err != nil {
		fmt.Println(err)
		return
	}
	// Simple Async Subscriber
	nc.Subscribe("rdsutbbp-resources-update", func(m *nats.Msg) {
		fmt.Printf("Received a message: %s\n", string(m.Data))
	})
	time.Sleep(1000 * time.Second)
}
