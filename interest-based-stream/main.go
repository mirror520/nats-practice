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
		Name: "EVENTS",
		Subjects: []string{
			"events.>",
		},
		Retention: nats.InterestPolicy,
	}

	js.AddStream(cfg)
	fmt.Println("created the stream")

	js.Publish("events.page_loaded", nil)
	js.Publish("events.mouse_clicked", nil)
	ack, err := js.Publish("events.inpput_focused", nil)
	if err != nil {
		log.Fatal(err.Error())
		return
	}
	fmt.Println("published 3 messages")

	fmt.Printf("last message seq: %d\n", ack.Sequence)

	fmt.Println("# Stream info without any consumers")
	printStreamState(js, cfg.Name)

	js.AddConsumer(cfg.Name, &nats.ConsumerConfig{
		Durable:   "processor-1",
		AckPolicy: nats.AckExplicitPolicy,
	})

	js.Publish("events.mouse_clicked", nil)
	js.Publish("events.input_focused", nil)

	fmt.Println("# Stream info with one consumer")
	printStreamState(js, cfg.Name)

	sub1, err := js.PullSubscribe("", "processor-1", nats.Bind(cfg.Name, "processor-1"))
	if err != nil {
		log.Fatal(err.Error())
		return
	}
	defer sub1.Unsubscribe()

	msgs, err := sub1.Fetch(2)
	if err != nil {
		log.Fatal(err.Error())
		return
	}
	msgs[0].Ack()
	msgs[1].AckSync()

	fmt.Println("# Stream info with one consumer and acked messages")
	printStreamState(js, cfg.Name)

	js.AddConsumer(cfg.Name, &nats.ConsumerConfig{
		Durable:   "processor-2",
		AckPolicy: nats.AckExplicitPolicy,
	})

	js.Publish("events.input_focused", nil)
	js.Publish("events.mouse_clicked", nil)

	sub2, err := js.PullSubscribe("", "processor-2", nats.Bind(cfg.Name, "processor-2"))
	if err != nil {
		log.Fatal(err.Error())
		return
	}
	defer sub2.Unsubscribe()

	msgs, err = sub2.Fetch(2)
	if err != nil {
		log.Fatal(err.Error())
		return
	}
	md0, _ := msgs[0].Metadata()
	md1, _ := msgs[1].Metadata()
	fmt.Printf("msg seqs %d and %d\n", md0.Sequence.Stream, md1.Sequence.Stream)
	msgs[0].Ack()
	msgs[1].AckSync()

	fmt.Println("# Stream info with two consumers, but only one set of acked messages")
	printStreamState(js, cfg.Name)

	js.AddConsumer(cfg.Name, &nats.ConsumerConfig{
		Durable:       "processor-3",
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: "events.mouse_clicked",
	})

	js.Publish("events.input_focused", nil)

	msgs, _ = sub1.Fetch(1)
	msgs[0].Term()

	msgs, _ = sub2.Fetch(1)
	msgs[0].AckSync()

	fmt.Println("# Stream info with three consumers with interest from two")
	printStreamState(js, cfg.Name)
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
