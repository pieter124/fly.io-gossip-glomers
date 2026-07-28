package main

import (
	"encoding/json"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	n.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		delta := int(body["delta"].(float64))

		// Increment value of a single global counter by delta.

		return n.Reply(msg, map[string]any{
			"type": "add_ok",
		})

	})

	n.Handle("read", func(msg maelstrom.Message) error {
		/*
			Return the current value of the global counter.
			Remember that the counter service is only sequentially consistent.
		*/
		value := 1234 // placeholder

		return n.Reply(msg, map[string]any{
			"type":  "read_ok",
			"value": value,
		})
	})
}
