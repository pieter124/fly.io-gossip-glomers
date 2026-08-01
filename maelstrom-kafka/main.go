package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Server struct {
	node              *maelstrom.Node
	log               map[string][]int
	log_mu            sync.Mutex
	committed_offsets map[string]int
	committed_mu      sync.Mutex
}

func (s *Server) handleSend(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	key := body["key"].(string)
	message := int(body["msg"].(float64))

	s.log_mu.Lock()

	if _, exists := s.log[key]; !exists {
		s.log[key] = make([]int, 0, 1000)
	}
	s.log[key] = append(s.log[key], message)
	offset := len(s.log[key]) - 1

	s.log_mu.Unlock()

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

	s.log_mu.Lock()

	for k, v := range offsets {
		if _, exists := s.log[k]; !exists {
			messages[k] = make([][2]int, 0, 0)
		}

		if _, exists := messages[k]; !exists {
			messages[k] = make([][2]int, 0, 100)
		}

		for i := v; i < len(s.log[k]); i++ {
			pair := [2]int{
				i,
				s.log[k][i],
			}
			messages[k] = append(messages[k], pair)
		}
	}
	s.log_mu.Unlock()

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

	s.committed_mu.Lock()

	for k, v := range offsets {
		s.committed_offsets[k] = v
	}
	s.committed_mu.Unlock()

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

	s.committed_mu.Lock()
	offsets := make(map[string]int, len(s.committed_offsets))

	for _, k := range keys {
		if _, exists := s.committed_offsets[k]; !exists {
			continue
		}
		offsets[k] = s.committed_offsets[k]
	}
	s.committed_mu.Unlock()

	return s.node.Reply(msg, map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": offsets,
	})
}

func main() {
	server := &Server{
		node:              maelstrom.NewNode(),
		log:               make(map[string][]int, 100),
		committed_offsets: make(map[string]int, 100),
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
