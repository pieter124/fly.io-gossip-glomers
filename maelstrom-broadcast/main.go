package main

import (
	"encoding/json"
	"log"
	"os"
	"slices"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type empty struct{}
type set map[int]empty

type neighborBatcher struct {
	dest        string
	messageChan chan int
}

func newNeighborBatcher(node *maelstrom.Node, dest string) *neighborBatcher {
	nb := &neighborBatcher{dest: dest, messageChan: make(chan int, 32)}
	go nb.run(node)
	return nb
}

func (nb *neighborBatcher) run(node *maelstrom.Node) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	pending := make([]int, 0, 100)

	for {
		select {
		case message := <-nb.messageChan:
			pending = append(pending, message)
			if len(pending) >= 32 {
				batch := pending
				pending = make([]int, 0, 100)

				// Reset the ticker so we don't fire an empty batch right after
				ticker.Reset(10 * time.Millisecond)

				go func(b []int) {
					node.Send(nb.dest, map[string]any{
						"type":    "broadcast_batch",
						"message": b,
					})
				}(batch)
			}

		case <-ticker.C:
			if len(pending) == 0 {
				continue
			}
			batch := pending
			pending = make([]int, 0, 100)
			go func(b []int) {
				node.Send(nb.dest, map[string]any{
					"type":    "broadcast_batch",
					"message": batch,
				})
			}(batch)

		}
	}
}

type server struct {
	sync.RWMutex
	node      *maelstrom.Node
	messages  []int
	seen      set
	neighbors []string
	batchers  []*neighborBatcher
}

func (s *server) handleBroadcastOk(msg maelstrom.Message) error {
	return nil
}

func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	message := int(body["message"].(float64))
	src := msg.Src

	isNew := false
	s.Lock()
	_, exists := s.seen[message]
	if !exists {
		s.seen[message] = empty{}
		s.messages = append(s.messages, message)
		isNew = true
	}
	s.Unlock()

	if isNew {
		for i := range s.neighbors {
			if s.neighbors[i] == src {
				continue
			}
			s.batchers[i].messageChan <- message
		}
	}

	return s.node.Reply(msg, map[string]any{"type": "broadcast_ok"})
}

func (s *server) handleRead(msg maelstrom.Message) error {
	s.RLock()
	out := make([]int, len(s.messages))
	copy(out, s.messages)
	s.RUnlock()
	return s.node.Reply(msg, map[string]any{"type": "read_ok", "messages": out})
}

func (s *server) handleTopology(msg maelstrom.Message) error {
	allNodes := s.node.NodeIDs()
	slices.Sort(allNodes)

	bf := 4
	idx := -1

	for i, id := range allNodes {
		if id == s.node.ID() {
			idx = i
			break
		}
	}

	s.Lock()
	s.neighbors = make([]string, 0, bf+1)
	s.batchers = make([]*neighborBatcher, 0, bf+1)

	if idx > 0 {
		parent := (idx - 1) / bf
		parentID := allNodes[parent]
		s.neighbors = append(s.neighbors, parentID)
		s.batchers = append(s.batchers, newNeighborBatcher(s.node, parentID))
	}

	for j := 1; j <= bf; j++ {
		child := (idx * bf) + j
		if child < len(allNodes) {
			childID := allNodes[child]
			s.neighbors = append(s.neighbors, childID)
			s.batchers = append(s.batchers, newNeighborBatcher(s.node, childID))
		}
	}
	s.Unlock()

	return s.node.Reply(msg, map[string]any{"type": "topology_ok"})
}

func (s *server) handleBroadcastBatch(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	src := msg.Src

	rawBatch := body["message"].([]any)

	newMessages := make([]int, 0, len(rawBatch))

	s.Lock()
	for _, v := range rawBatch {
		message := int(v.(float64))
		_, exists := s.seen[message]
		if !exists {
			s.seen[message] = empty{}
			s.messages = append(s.messages, message)
			newMessages = append(newMessages, message)
		}
	}
	s.Unlock()

	for _, message := range newMessages {
		for i := range s.neighbors {
			if s.neighbors[i] == src {
				continue
			}
			s.batchers[i].messageChan <- message
		}
	}

	return s.node.Reply(msg, map[string]any{"type": "broadcast_ok"})
}

func main() {
	s := &server{
		node:     maelstrom.NewNode(),
		messages: make([]int, 0, 100),
		seen:     make(set),
	}

	s.node.Handle("broadcast_ok", s.handleBroadcastOk)
	s.node.Handle("broadcast", s.handleBroadcast)
	s.node.Handle("broadcast_batch", s.handleBroadcastBatch)
	s.node.Handle("read", s.handleRead)
	s.node.Handle("topology", s.handleTopology)

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := s.node.Run(); err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
}
