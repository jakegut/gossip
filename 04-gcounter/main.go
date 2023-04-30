package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	n.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		delta := int(body["delta"].(float64))

		cur, err := kvGET(kv, n.ID())
		if err != nil {
			var rpcError *maelstrom.RPCError
			if errors.As(err, &rpcError) {
				if rpcError.Code == maelstrom.KeyDoesNotExist {
					if err = kvPUT(kv, n.ID(), 0); err != nil {
						return err
					}
					cur = 0
				}
			}
			return err
		}

		if err = kv.Write(context.Background(), n.ID(), cur+delta); err != nil {
			return err
		}

		return n.Reply(msg, map[string]any{
			"type": "add_ok",
		})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "read_ok"
		val := 0
		for _, node := range n.NodeIDs() {
			v, _ := kvGET(kv, node)
			val += v
		}
		body["value"] = val
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func kvPUT(kv *maelstrom.KV, key string, val int) error {
	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		if err := kv.Write(ctx, key, val); err != nil {
			var rpcError *maelstrom.RPCError
			if errors.As(err, &rpcError) {
				cancel()
				return rpcError
			}
			cancel()
			time.Sleep(time.Duration(i) * 100 * time.Millisecond)
			continue
		}
		cancel()
		return nil
	}
	return fmt.Errorf("timed out")
}

func kvGET(kv *maelstrom.KV, key string) (int, error) {
	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		cur, err := kv.ReadInt(ctx, key)
		if err != nil {
			var rpcError *maelstrom.RPCError
			if errors.As(err, &rpcError) {
				cancel()
				return 0, rpcError
			}
			cancel()
			time.Sleep(time.Duration(i) * 100 * time.Millisecond)
			continue
		}
		cancel()
		return cur, nil
	}
	return 0, fmt.Errorf("timed out")
}
