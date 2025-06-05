package monoceros

import (
	"encoding/binary"
	"encoding/json"
	"hash/fnv"
	"log"
	"time"

	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/transport"
	"github.com/c12s/plumtree"
)

type GossipNode struct {
	membership   plumtree.MembershipProtocol
	seenMessages map[string]bool
	logger       *log.Logger
	handler      func(msg []byte)
}

func NewGossipNode(membership plumtree.MembershipProtocol, logger *log.Logger) *GossipNode {
	gn := &GossipNode{
		membership:   membership,
		seenMessages: make(map[string]bool),
		logger:       logger,
	}
	gn.membership.AddCustomMsgHandler(gn.onGossipReceived)
	return gn
}

func (gn *GossipNode) Broadcast(msg []byte) {
	now := time.Now().Unix()
	nowBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(nowBytes, uint64(now))
	msgBytes := append(nowBytes, msg...)
	hashFn := fnv.New64()
	_, err := hashFn.Write(msgBytes)
	if err != nil {
		gn.logger.Println("Error creating hash:", err)
		return
	}
	msgId := hashFn.Sum(nil)
	if gn.seenMessages[string(msgId)] {
		gn.logger.Println("msg already seen:", msg)
		return
	}
	gn.seenMessages[string(msgId)] = true
	if gn.handler != nil {
		gn.handler(msg)
	}
	for _, peer := range gn.membership.GetPeers(100) {
		err := peer.Conn.Send(data.Message{
			Type:    data.CUSTOM,
			Payload: msg,
		})
		if err != nil {
			gn.logger.Println("error while broadcasting msg in global network", err)
		}
	}
}

func (gn *GossipNode) onGossipReceived(msgBytes1 []byte, from transport.Conn) error {
	msgBytes := make([]byte, 0)
	err := json.Unmarshal(msgBytes1, &msgBytes)
	if err != nil {
		gn.logger.Println(err)
		return err
	}
	gn.logger.Println("received gossip msg", msgBytes)
	hashFn := fnv.New64()
	_, err = hashFn.Write(msgBytes)
	if err != nil {
		gn.logger.Println("Error creating hash:", err)
		return nil
	}
	msgId := hashFn.Sum(nil)
	if gn.seenMessages[string(msgId)] {
		gn.logger.Println("msg already seen:", msgBytes)
		return nil
	}
	gn.seenMessages[string(msgId)] = true
	msg := msgBytes[8:]
	gn.logger.Println("Received:", msg)
	if gn.handler != nil {
		gn.handler(msg)
	}
	for _, peer := range gn.membership.GetPeers(100) {
		if peer.Conn.GetAddress() == from.GetAddress() {
			continue
		}
		err := peer.Conn.Send(data.Message{
			Type:    data.CUSTOM,
			Payload: msgBytes,
		})
		if err != nil {
			gn.logger.Println("error while forwarding msg in global network", err)
		}
	}
	return nil
}

func (gn *GossipNode) AddMsgHandler(handler func([]byte)) {
	gn.handler = handler
}
