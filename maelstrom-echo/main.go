package main

import (
	"encoding/json"
	"log"
	"os"
	"strconv"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	var counter atomic.Uint64
	n.Handle("generate", func(msg maelstrom.Message) error {
		// Unmarshal the message body as a loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		body["type"] = "generate_ok"

		body["id"] = n.ID() + strconv.Itoa(int(counter.Add(1)))

		// Echo original message back with the updated message type
		return n.Reply(msg, body)
	})

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
}
