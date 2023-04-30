package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type topology map[string][]string

type topologyMessage struct {
	Type     string   `json:"type"`
	Topology topology `json:"topology,omitempty"`
}

type broadcastMessage struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

type batchMsg map[string][]int

var batchMsgMu sync.RWMutex

func main() {
	n := maelstrom.NewNode()

	seen := make(map[int]struct{})
	seenSlice := []int{}
	var seenMu sync.RWMutex
	nodes := []string{}

	batchMsgs := batchMsg{}

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		go func() {
			n.Reply(msg, map[string]any{
				"type": "broadcast_ok",
			})
		}()

		newMessages := []int{}
		if _, ok := body["message"]; ok {
			message := int(body["message"].(float64))
			seenMu.RLock()
			if _, ok := seen[message]; ok {
				seenMu.RUnlock()
				return nil
			}
			seenMu.RUnlock()
			newMessages = append(newMessages, message)
		}

		seenMu.RLock()
		if _, ok := body["messages"]; ok {
			values := body["messages"].([]any)
			for _, v := range values {
				message := int(v.(float64))
				if _, exists := seen[message]; exists {
					continue
				} else {
					newMessages = append(newMessages, message)
				}
			}
		}
		seenMu.RUnlock()

		seenMu.Lock()
		for _, msg := range newMessages {
			seen[msg] = struct{}{}
			seenSlice = append(seenSlice, msg)
		}
		seenMu.Unlock()
		go func(num []int) {
			batchMsgMu.Lock()
			defer batchMsgMu.Unlock()
			for _, node := range nodes {
				// go broadcastNumToNode(n, node, num)
				batchMsgs[node] = append(batchMsgs[node], num...)
			}
		}(newMessages)

		return nil
	})

	n.Handle("broadcast_ok", func(msg maelstrom.Message) error { return nil })

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "read_ok"

		seenMu.RLock()
		defer seenMu.RUnlock()
		body["messages"] = seenSlice
		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body topologyMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body.Type = "topology_ok"
		log.Printf("%+v", body.Topology)
		if childs, ok := body.Topology[n.ID()]; ok {
			nodes = append(nodes, childs...)
			for _, node := range nodes {
				batchMsgs[node] = []int{}
			}
		} else {
			return fmt.Errorf("children not found for %q", n.ID())
		}
		body.Topology = nil
		return n.Reply(msg, body)
	})

	go func() {
		batchSend := time.NewTicker(250 * time.Millisecond)
		for range batchSend.C {
			batchRPC(n, batchMsgs)
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func batchRPC(n *maelstrom.Node, batch batchMsg) {
	batchMsgMu.Lock()
	defer batchMsgMu.Unlock()
	var wg sync.WaitGroup
	wg.Add(len(batch))
	for node, b := range batch {
		msg := map[string]interface{}{}
		msg["type"] = "broadcast"
		msg["messages"] = b
		go func(dest string) {
			for i := 0; i < 10; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				_, err := n.SyncRPC(ctx, dest, msg)
				if err != nil {
					cancel()
					time.Sleep(100 * time.Duration(i) * time.Millisecond)
					continue
				}
				cancel()
				break
			}
			wg.Done()
		}(node)
	}
	wg.Wait()
	for node := range batch {
		batch[node] = []int{}
	}
}

func broadcastNumToNode(n *maelstrom.Node, node string, num int) {
	msg := broadcastMessage{
		Type:    "broadcast",
		Message: num,
	}
	ack := make(chan struct{})
	tries := 0
	for {
		n.RPC(node, msg, func(msg maelstrom.Message) error {
			ack <- struct{}{}
			close(ack)

			return nil
		})

		select {
		case <-ack:
			return
		case <-time.After(time.Second):
			time.Sleep(time.Duration(tries) * time.Second)
			tries++
			continue
		}
	}

}
