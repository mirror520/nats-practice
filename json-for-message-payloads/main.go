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
	}
	defer nc.Drain()

	nc.Subscribe("greet", greetingHandler)

	req := GreetRequest{
		Name: "joe",
	}
	data, _ := json.Marshal(req)

	msg, _ := nc.Request("greet", data, time.Second)

	var resp GreetReply
	json.Unmarshal(msg.Data, &resp)

	fmt.Printf("reply: %s\n", resp.Text)
}

type GreetRequest struct {
	Name string
}

type GreetReply struct {
	Text string
}

func greetingHandler(msg *nats.Msg) {
	var req GreetRequest
	err := json.Unmarshal(msg.Data, &req)
	if err != nil {
		log.Println(err.Error())
		return
	}

	resp := GreetReply{
		Text: fmt.Sprintf("hello %q!", req.Name),
	}

	data, err := json.Marshal(resp)
	if err != nil {
		log.Println(err.Error())
		return
	}

	msg.Respond(data)
}
