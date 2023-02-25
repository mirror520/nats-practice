package main

import (
	"encoding/json"
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

	cfg := &nats.StreamConfig{
		Name: "EVENTS",
		Subjects: []string{
			"events.>",
		},
		Retention: nats.LimitsPolicy,
		Storage:   nats.FileStorage,
	}

	js, err := nc.JetStream()
	if err != nil {
		log.Fatal(err.Error())
		return
	}

	js.AddStream(cfg)
	fmt.Println("created the stream")

	js.Publish("events.page_loaded", nil)
	js.Publish("events.mouse_clicked", nil)
	js.Publish("events.mouse_clicked", nil)
	js.Publish("events.page_loaded", nil)
	js.Publish("events.page_loaded", nil)
	js.Publish("events.input_focused", nil)
	fmt.Println("published 6 messages")

	js.PublishAsync("events.input_changed", nil)
	js.PublishAsync("events.input_blurred", nil)
	js.PublishAsync("events.key_pressed", nil)
	js.PublishAsync("events.input_focused", nil)
	js.PublishAsync("events.input_changed", nil)
	js.PublishAsync("events.input_blurred", nil)

	select {
	case <-js.PublishAsyncComplete():
		fmt.Println("published 6 messages")
	case <-time.After(time.Second):
		log.Fatal("publish took too long")
	}
	printStreamState(js, cfg.Name)

	cfg.MaxMsgs = 10
	js.UpdateStream(cfg)
	fmt.Println("set max messages to 10")
	printStreamState(js, cfg.Name)

	cfg.MaxBytes = 300
	js.UpdateStream(cfg)
	fmt.Println("set max bytes to 300")
	printStreamState(js, cfg.Name)

	cfg.MaxAge = time.Second
	js.UpdateStream(cfg)
	fmt.Println("set max age to one second")
	printStreamState(js, cfg.Name)

	fmt.Println("sleeping one second ...")
	time.Sleep(time.Second)

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

	fmt.Println("inspecting stream info")
	fmt.Println(string(bs))
	return nil
}
