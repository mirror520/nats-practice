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

	js, err := nc.JetStream()
	if err != nil {
		log.Fatal(err.Error())
		return
	}

	streamName := "EVENTS"
	js.AddStream(&nats.StreamConfig{
		Name: streamName,
		Subjects: []string{
			"events.>",
		},
	})

	js.Publish("events.1", nil)
	js.Publish("events.2", nil)
	js.Publish("events.3", nil)

	sub, _ := js.PullSubscribe("", "", nats.BindStream(streamName))

	ephemeraName := <-js.ConsumerNames(streamName)
	fmt.Println("ephemera name is " + ephemeraName)

	msgs, _ := sub.Fetch(2)
	fmt.Printf("got %d messages\n", len(msgs))

	msgs[0].Ack()
	msgs[1].Ack()

	msgs, _ = sub.Fetch(100)
	fmt.Printf("got %d messages\n", len(msgs))
	msgs[0].Ack()

	_, err = sub.Fetch(1, nats.MaxWait(time.Second))
	fmt.Printf("timeout? %v\n", err == nats.ErrTimeout)

	sub.Unsubscribe()

	consumerName := "processor"
	js.AddConsumer(streamName, &nats.ConsumerConfig{
		Durable:   consumerName,
		AckPolicy: nats.AckExplicitPolicy,
	})

	sub1, _ := js.PullSubscribe("", consumerName, nats.BindStream(streamName))

	msgs, _ = sub1.Fetch(1)
	fmt.Printf("received %q from sub1\n", msgs[0].Subject)
	msgs[0].Ack()

	sub1.Unsubscribe()
	sub1, _ = js.PullSubscribe("", consumerName, nats.BindStream(streamName))

	msgs, _ = sub1.Fetch(1)
	fmt.Printf("received %q from sub1 (after reconnect)\n", msgs[0].Subject)
	msgs[0].Ack()

	sub2, _ := js.PullSubscribe("", consumerName, nats.BindStream(streamName))

	msgs, _ = sub2.Fetch(1)
	fmt.Printf("received %q from sub2\n", msgs[0].Subject)
	msgs[0].Ack()

	_, err = sub1.Fetch(1, nats.MaxWait(time.Second))
	fmt.Printf("timeout on sub1? %v\n", err == nats.ErrTimeout)

	sub1.Unsubscribe()
	sub2.Unsubscribe()
}
