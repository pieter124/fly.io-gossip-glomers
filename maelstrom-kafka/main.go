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

type Server struct {
	node *maelstrom.Node
	kv   *maelstrom.KV
}

func (s *Server) handleSend(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	key := body["key"].(string)
	message := int(body["msg"].(float64))

	var offset int
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		var currentLog []int
		err := s.kv.ReadInto(ctx, key, &currentLog)
		cancel()
		if err != nil {
			var rpcErr *maelstrom.RPCError
			if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.KeyDoesNotExist {
				currentLog = []int{}
			} else {
				return err
			}
		}

		newLog := append(currentLog, message)
		offset = len(newLog) - 1

		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		err = s.kv.CompareAndSwap(ctx, key, currentLog, newLog, true)
		cancel()
		if err == nil {
			break
		}

	}

	return s.node.Reply(msg, map[string]any{
		"type":   "send_ok",
		"offset": offset,
	})
}

func (s *Server) handlePoll(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	rawOffsets := body["offsets"].(map[string]any)
	offsets := make(map[string]int, len(rawOffsets))
	for k, v := range rawOffsets {
		offsets[k] = int(v.(float64))
	}
	messages := make(map[string][][2]int)
	for k, startingOffset := range offsets {
		messages[k] = make([][2]int, 0)
		var currentLog []int

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		err := s.kv.ReadInto(ctx, k, &currentLog)
		cancel()
		if err != nil {
			var rpcErr *maelstrom.RPCError
			if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.KeyDoesNotExist {
				continue
			} else {
				return err
			}
		}
		if startingOffset < len(currentLog) {
			for i := startingOffset; i < len(currentLog); i++ {
				messages[k] = append(messages[k], [2]int{i, currentLog[i]})
			}
		}
	}

	return s.node.Reply(msg, map[string]any{
		"type": "poll_ok",
		"msgs": messages,
	})
}

func (s *Server) handleCommitOffsets(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	rawOffsets := body["offsets"].(map[string]any)
	offsets := make(map[string]int, len(rawOffsets))
	for k, v := range rawOffsets {
		offsets[k] = int(v.(float64))
	}

	for k, v := range offsets {
		for {
			committed_key := "committed " + k
			var committed_offset int
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			err := s.kv.ReadInto(ctx, committed_key, &committed_offset)
			cancel()
			if err != nil {
				var rpcErr *maelstrom.RPCError
				if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.KeyDoesNotExist {
					committed_offset = 0
				} else {
					return err
				}
			}

			ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
			err = s.kv.CompareAndSwap(ctx, committed_key, committed_offset, v, true)
			cancel()
			if err == nil {
				break
			}
		}

	}

	return s.node.Reply(msg, map[string]any{
		"type": "commit_offsets_ok",
	})
}

func (s *Server) handleListCommittedOffsets(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	rawKeys := body["keys"].([]any)
	keys := make([]string, len(rawKeys))
	for i, k := range rawKeys {
		keys[i] = k.(string)
	}

	offsets := make(map[string]int, len(keys))

	for _, k := range keys {
		var offset int
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		err := s.kv.ReadInto(ctx, k, offset)
		cancel()
		if err != nil {
			var rpcErr *maelstrom.RPCError
			if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.KeyDoesNotExist {
				continue
			}
			return err
		}

		offsets[k] = offset
	}

	return s.node.Reply(msg, map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": offsets,
	})
}

func main() {
	node := maelstrom.NewNode()
	server := &Server{
		node: node,
		kv:   maelstrom.NewLinKV(node),
	}

	server.node.Handle("send", server.handleSend)
	server.node.Handle("poll", server.handlePoll)
	server.node.Handle("commit_offsets", server.handleCommitOffsets)
	server.node.Handle("list_committed_offsets", server.handleListCommittedOffsets)

	if err := server.node.Run(); err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
}
