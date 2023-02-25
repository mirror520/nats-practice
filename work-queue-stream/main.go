package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

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

	js, err := nc.JetStream()
	if err != nil {
		log.Fatal(err.Error())
		return
	}

	cfg := &nats.StreamConfig{
		Name:      "EVENTS",
		Retention: nats.WorkQueuePolicy,
		Subjects: []string{
			"events.>",
		},
	}
	js.AddStream(cfg)
	fmt.Println("created the stream")

	js.Publish("events.us.page_loaded", nil)
	js.Publish("events.eu.mouse_clicked", nil)
	js.Publish("events.us.input_focused", nil)
	fmt.Println("published 3 messages")

	fmt.Println("# Stream info without any consumers")
	printStreamState(js, cfg.Name)

	sub1, err := js.PullSubscribe("", "processor-1", nats.BindStream(cfg.Name))
	if err != nil {
		log.Fatal(err.Error())
		return
	}

	msgs, _ := sub1.Fetch(3)
	for _, msg := range msgs {
		msg.AckSync()
	}

	fmt.Println("# Stream info with one consumer")
	printStreamState(js, cfg.Name)

	fmt.Println("# Create an overlapping consumer")
	if _, err := js.PullSubscribe("", "processor-2", nats.BindStream(cfg.Name)); err != nil {
		fmt.Println(err.Error())
	}

	sub1.Unsubscribe()

	sub2, err := js.PullSubscribe("", "processor-2", nats.BindStream(cfg.Name))
	fmt.Printf("created the new consumer? %v \n", err == nil)
	sub2.Unsubscribe()

	fmt.Println("# Create non-overlapping consumers")
	sub1, _ = js.PullSubscribe("events.us.>", "processor-us", nats.BindStream(cfg.Name))
	sub2, _ = js.PullSubscribe("events.eu.>", "processor-eu", nats.BindStream(cfg.Name))

	js.Publish("events.eu.mouse_clicked", nil)
	js.Publish("events.us.page_loaded", nil)
	js.Publish("events.us.input_focused", nil)
	js.Publish("events.eu.page_loaded", nil)
	fmt.Println("published 4 messages")

	msgs, _ = sub1.Fetch(2)
	for _, msg := range msgs {
		fmt.Printf("us sub got: %s\n", msg.Subject)
		msg.Ack()
	}

	msgs, _ = sub2.Fetch(2)
	for _, msg := range msgs {
		fmt.Printf("eu sub got: %s\n", msg.Subject)
		msg.Ack()
	}
}

func printStreamState(js nats.JetStreamContext, name string) error {
	info, err := js.StreamInfo(name)
	if err != nil {
		return err
	}

	bs, err := json.MarshalIndent(info.State, "", " ")
	if err != nil {
		return err
	}

	fmt.Println(string(bs))
	return nil
}
