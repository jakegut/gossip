package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type kafka struct {
	currentOffsets   map[string]int
	committedOffsets map[string]int
	data             map[string]map[int]int
	mu               sync.RWMutex
}

type entry struct {
	value  int
	offset int
}

type server struct {
	n     *maelstrom.Node
	kafka *kafka
	kv    *maelstrom.KV
}

func main() {
	s := New()
	if err := s.Run(); err != nil {
		log.Fatal(err)
	}
}

func New() *server {
	n := maelstrom.NewNode()
	return &server{
		n: n,
		kafka: &kafka{
			currentOffsets:   make(map[string]int),
			data:             make(map[string]map[int]int),
			committedOffsets: make(map[string]int),
		},
		kv: maelstrom.NewLinKV(n),
	}
}

func (s *server) init() {
	s.n.Handle("send", s.sendHandler)
	s.n.Handle("poll", s.pollHandler)
	s.n.Handle("commit_offsets", s.commitOffsets)
	s.n.Handle("list_committed_offsets", s.listCommittedOffsets)
}

func (s *server) sendHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	key := body["key"].(string)
	newMsg := int(body["msg"].(float64))

	offsetKey := fmt.Sprintf("offset_%s", key)

	offset, err := s.kv.ReadInt(context.Background(), offsetKey)
	if err != nil {
		var rpcError *maelstrom.RPCError
		if errors.As(err, &rpcError) {
			if rpcError.Code == maelstrom.KeyDoesNotExist {
				offset = 0
			}
		}
	} else {
		offset++
	}

	for {
		err = s.kv.CompareAndSwap(context.Background(), offsetKey, offset-1, offset, true)
		if err != nil {
			log.Printf("cas retry: %s", err)
			offset++
			continue
		}
		break
	}

	offsetMsgKey := fmt.Sprintf("msg_%s_%d", key, offset)
	if err := s.kv.Write(context.Background(), offsetMsgKey, newMsg); err != nil {
		return err
	}
	s.kafka.mu.Lock()
	if _, ok := s.kafka.data[key]; !ok {
		s.kafka.data[key] = make(map[int]int)
	}
	s.kafka.data[key][offset] = newMsg
	s.kafka.mu.Unlock()

	return s.n.Reply(msg, map[string]any{
		"type":   "send_ok",
		"offset": offset,
	})
}

func (s *server) pollHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	msgs := make(map[string][][2]int)

	keys := body["offsets"].(map[string]interface{})
	// s.kafka.mu.RLock()
	// for key, initialOffset := range keys {
	// 	offset := int(initialOffset.(float64))
	// 	data := s.kafka.data[key]
	// 	if len(data) > 0 {
	// 		msgs[key] = [][2]int{}
	// 	} else {
	// 		continue
	// 	}
	// 	offset--
	// 	if offset < 0 {
	// 		offset = 0
	// 	}
	// 	for _, entry := range data[offset:] {
	// 		msgs[key] = append(msgs[key], [2]int{entry.offset, entry.value})
	// 	}
	// }
	// s.kafka.mu.RUnlock()

	for key, initialOffset := range keys {
		offset := int(initialOffset.(float64))
		msgs[key] = [][2]int{}
		for {
			msg, err := s.readValue(key, offset)
			if err != nil {
				var rpcError *maelstrom.RPCError
				if errors.As(err, &rpcError) {
					if rpcError.Code == maelstrom.KeyDoesNotExist {
						break
					}
				}
			}
			msgs[key] = append(msgs[key], [2]int{offset, msg})
			offset++
		}

	}

	return s.n.Reply(msg, map[string]any{
		"type": "poll_ok",
		"msgs": msgs,
	})
}

func (s *server) readValue(key string, offset int) (int, error) {
	s.kafka.mu.Lock()
	defer s.kafka.mu.Unlock()
	if _, ok := s.kafka.data[key]; !ok {
		s.kafka.data[key] = make(map[int]int)
	}
	val, ok := s.kafka.data[key][offset]
	if ok {
		return val, nil
	}
	msgKey := fmt.Sprintf("msg_%s_%d", key, offset)
	msg, err := s.kv.ReadInt(context.Background(), msgKey)
	if err != nil {
		return 0, err
	}
	s.kafka.data[key][offset] = msg
	return msg, nil
}

func (s *server) commitOffsets(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	offsets := body["offsets"].(map[string]interface{})
	// s.kafka.mu.Lock()
	// for key, offset := range offsets {
	// 	offset := int(offset.(float64))
	// 	s.kafka.committedOffsets[key] = offset
	// }
	// s.kafka.mu.Unlock()
	for key, offset := range offsets {
		offset := int(offset.(float64))
		offsetKey := fmt.Sprintf("commit_%s", key)
		s.kv.Write(context.Background(), offsetKey, offset)
	}

	return s.n.Reply(msg, map[string]any{
		"type": "commit_offsets_ok",
	})
}

func (s *server) listCommittedOffsets(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	offsets := make(map[string]int)

	keys := body["keys"].([]interface{})
	// s.kafka.mu.RLock()
	// for _, key := range keys {
	// 	key := key.(string)
	// 	offsets[key] = s.kafka.committedOffsets[key]
	// }
	// s.kafka.mu.RUnlock()
	for _, key := range keys {
		key := key.(string)
		commitKey := fmt.Sprintf("commit_%s", key)
		val, err := s.kv.ReadInt(context.Background(), commitKey)
		if err != nil {
			offsets[key] = 0
		}
		offsets[key] = val
	}

	return s.n.Reply(msg, map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": offsets,
	})
}

func (s *server) Run() error {
	s.init()
	return s.n.Run()
}
