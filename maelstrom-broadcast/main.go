package main

import (
	"encoding/json"
	"log"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	messages := make([]int, 0, 100)
	var neighbors []string

	n := maelstrom.NewNode()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body as a loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		var message int = int(body["message"].(float64))
		for _, nei := range neighbors {
			n.Send(nei, map[string]any{"type": "broadcast", "message": message})
		}

		messages = append(messages, message)
		body["type"] = "broadcast_ok"
		delete(body, "message")
		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		// Unmarshal the message body as a loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "read_ok"
		body["messages"] = messages

		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		// Unmarshal the message body as a loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		topo := body["topology"].(map[string]any)
		neighborsRaw := topo[n.ID()].([]any)
		neighbors = make([]string, len(neighborsRaw))

		for i, v := range neighborsRaw {
			neighbors[i] = v.(string)
		}

		body["type"] = "topology_ok"
		delete(body, "topology")
		return n.Reply(msg, body)
	})

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
}
