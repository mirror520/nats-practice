package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	url, ok := os.LookupEnv("NATS_URL")
	if !ok {
		url = nats.DefaultURL
	}

	nc, err := nats.Connect(url)
	if err != nil {
		log.Fatal(err.Error())
		return
	}
	defer nc.Drain()

	nc.Publish("greet.joe", []byte("hello"))

	sub, err := nc.SubscribeSync("greet.*")
	if err != nil {
		log.Fatal(err.Error())
		return
	}
	defer sub.Unsubscribe()

	msg, _ := sub.NextMsg(10 * time.Millisecond)

	fmt.Println("subscribed after a publish...")
	fmt.Printf("msg is nil? %v\n", msg == nil)

	nc.Publish("greet.joe", []byte("hello"))
	nc.Publish("greet.pam", []byte("hello"))

	msg, _ = sub.NextMsg(10 * time.Millisecond)
	fmt.Printf("msg data: %q on subject %q\n", string(msg.Data), msg.Subject)

	msg, _ = sub.NextMsg(10 * time.Millisecond)
	fmt.Printf("msg data: %q on subject %q\n", string(msg.Data), msg.Subject)

	nc.Publish("greet.bob", []byte("hello"))

	msg, _ = sub.NextMsg(10 * time.Millisecond)
	fmt.Printf("msg data: %q on subject %q\n", string(msg.Data), msg.Subject)
}
