package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	n *maelstrom.Node

	mu    sync.RWMutex
	store map[int]int
}

func main() {
	s := New()
	if err := s.Run(); err != nil {
		log.Fatal(err)
	}
}

func New() *server {
	return &server{
		n:     maelstrom.NewNode(),
		store: make(map[int]int),
	}
}

func (s *server) init() {
	s.n.Handle("txn", s.handleTxn)
	s.n.Handle("txn_ok", func(msg maelstrom.Message) error { return nil })
}

func (s *server) handleTxn(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	txns := body["txn"].([]interface{})

	txnResults := make([][]any, len(txns))
	wTxns := make([][]any, 0)

	for idx, txn := range txns {
		txn := txn.([]interface{})
		op := txn[0].(string)
		key := int(txn[1].(float64))
		switch op {
		case "r":
			s.mu.RLock()
			var res *int
			if v, ok := s.store[key]; ok {
				res = &v
			}
			s.mu.RUnlock()
			txnResults[idx] = []any{op, key, res}
		case "w":
			s.mu.Lock()
			val := int(txn[2].(float64))
			s.store[key] = val
			s.mu.Unlock()
			txnResults[idx] = txn
			wTxns = append(wTxns, txn)
		}
	}

	go s.sendTxns(wTxns)

	return s.n.Reply(msg, map[string]any{
		"type": "txn_ok",
		"txn":  txnResults,
	})
}

func (s *server) sendTxns(txns [][]any) {
	msg := map[string]any{
		"type": "txn",
		"txn":  txns,
	}
	for _, dest := range s.n.NodeIDs() {
		if dest == s.n.ID() {
			continue
		}

		go s.sendToNodeWithRetry(dest, msg)
	}
}

func (s *server) sendToNodeWithRetry(dest string, msg any) {
	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := s.n.SyncRPC(ctx, dest, msg)
		if err == nil {
			cancel()
			return
		}
		time.Sleep(time.Duration(i) * 100 * time.Millisecond)
		cancel()
	}
}

func (s *server) Run() error {
	s.init()
	return s.n.Run()
}
