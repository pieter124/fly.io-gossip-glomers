package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type empty struct{}
type set map[int]empty

func main() {
	var mu sync.Mutex
	messages := make([]int, 0, 100)
	neighbors := make([]string, 0, 100)
	seen := make(set)

	n := maelstrom.NewNode()

	n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
		return nil
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body as a loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		var message int = int(body["message"].(float64))

		mu.Lock()
		_, exists := seen[message]

		if !exists {
			seen[message] = empty{}
			messages = append(messages, message)

			for _, nei := range neighbors {
				go func() {
					dest := nei
					for {
						ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

						_, err := n.SyncRPC(ctx, dest, map[string]any{
							"type":    "broadcast",
							"message": message,
						})

						cancel()

						if err == nil {
							break
						}

						time.Sleep(500 * time.Millisecond)
					}
				}()

			}
		}
		mu.Unlock()
		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{"type": "read_ok", "messages": messages})
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
		return n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
}
