package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var singleKey string = "g-counter"

func main() {
	node := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(node)

	node.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		delta := int(body["delta"].(float64))

		for {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			read, err := kv.ReadInt(ctx, singleKey)
			cancel()
			if err != nil {
				var rpcErr *maelstrom.RPCError
				if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.KeyDoesNotExist {
					read = 0 // key hasn't been created yet — treat current value as 0
				} else {
					time.Sleep(20 * time.Millisecond)
					continue
				}
			}

			newVal := delta + read
			ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
			err = kv.CompareAndSwap(ctx, singleKey, read, newVal, true)
			cancel()
			if err != nil {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			break
		}

		return node.Reply(msg, map[string]any{
			"type": "add_ok",
		})
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		/*
			Return the current value of the global counter.
			Remember that the counter service is sequentially consistent.
		*/
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			value, err := kv.ReadInt(ctx, singleKey)
			cancel()
			if err != nil {
				var rpcErr *maelstrom.RPCError
				if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.KeyDoesNotExist {
					value = 0 // key hasn't been created yet — treat current value as 0
				} else {
					time.Sleep(20 * time.Millisecond)
					continue
				}
			}

			ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
			err = kv.CompareAndSwap(ctx, singleKey, value, value, true)
			cancel()
			if err != nil {
				time.Sleep(20 * time.Millisecond)
				continue
			}

			return node.Reply(msg, map[string]any{
				"type":  "read_ok",
				"value": value,
			})
		}

	})

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := node.Run(); err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
}
