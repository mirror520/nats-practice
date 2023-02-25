package main

import (
	"fmt"
	"log"
	"os"
	"strings"
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

	sub, err := nc.Subscribe("greet.*", func(msg *nats.Msg) {
		name := strings.TrimPrefix(msg.Subject, "greet.")
		msg.Respond([]byte("hello, " + name))
	})
	if err != nil {
		log.Fatal(err.Error())
		return
	}

	resp, _ := nc.Request("greet.joe", nil, time.Second)
	fmt.Println(string(resp.Data))

	resp, _ = nc.Request("greet.sue", nil, time.Second)
	fmt.Println(string(resp.Data))

	resp, _ = nc.Request("greet.bob", nil, time.Second)
	fmt.Println(string(resp.Data))

	sub.Unsubscribe()

	_, err = nc.Request("greet.joe", nil, time.Second)
	fmt.Println(err.Error())
}
