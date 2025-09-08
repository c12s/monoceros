package monoceros

import (
	"encoding/binary"
	"hash/fnv"
	"sync"
	"time"

	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/hyparview"
	"github.com/c12s/hyparview/transport"
	"github.com/c12s/plumtree"
)

type msgSeen struct {
	time time.Time
}

const GLOBAL_GOSSIP_MSG_TYPE data.MessageType = data.UNKNOWN + 1

type GossipNode struct {
	membership    plumtree.MembershipProtocol
	seenMessages  map[string]msgSeen
	gossipHandler func(msg []byte, sender transport.Conn) bool
	peerUpHandler func() (send bool, msg []byte)
	lock          *sync.Mutex
}

func NewGossipNode(membership plumtree.MembershipProtocol) *GossipNode {
	gn := &GossipNode{
		membership:   membership,
		seenMessages: make(map[string]msgSeen),
		lock:         &sync.Mutex{},
	}
	gn.membership.AddClientMsgHandler(GLOBAL_GOSSIP_MSG_TYPE, gn.onGossipReceived)
	gn.membership.OnPeerUp(func(peer hyparview.Peer) {
		if gn.peerUpHandler == nil {
			return
		}
		send, msg := gn.peerUpHandler()
		if !send {
			// log.Println("nothing to send to peer")
			return
		}
		err := gn.Send(msg, peer.Conn)
		if err != nil {
			// log.Println("error while sending msg to peer in global network", err)
		}
	})
	go gn.clean()
	return gn
}

func (gn *GossipNode) clean() {
	for range time.NewTicker(5 * time.Second).C {
		gn.lock.Lock()
		remove := []string{}
		for id, t := range gn.seenMessages {
			if t.time.Add(10*time.Second).Before(time.Now()) {
				remove = append(remove, id)
			}
		}
		for _, id := range remove {
			delete(gn.seenMessages, id)
		}
		gn.lock.Unlock()

	}
}

func (gn *GossipNode) Broadcast(msg []byte) {
	now := time.Now().Unix()
	nowBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(nowBytes, uint64(now))
	msgBytes := append(nowBytes, msg...)
	hashFn := fnv.New64()
	_, err := hashFn.Write(msgBytes)
	if err != nil {
		// log.Println("Error creating hash:", err)
		return
	}
	msgId := hashFn.Sum(nil)
	gn.lock.Lock()
	defer gn.lock.Unlock()
	if _, ok := gn.seenMessages[string(msgId)]; ok {
		// log.Println("msg already seen:", msg)
		return
	}
	gn.seenMessages[string(msgId)] = msgSeen{time: time.Now()}
	if gn.gossipHandler != nil {
		gn.lock.Unlock()
		proceed := gn.gossipHandler(msg, nil)
		gn.lock.Lock()
		if !proceed {
			// log.Println("quit broadcasting signal ...")
			return
		}
	}
	for _, peer := range gn.membership.GetPeers(100) {
		err := peer.Conn.Send(data.Message{
			Type:    GLOBAL_GOSSIP_MSG_TYPE,
			Payload: msgBytes,
		})
		if err != nil {
			// log.Println("error while broadcasting msg in global network", err)
		}
	}
}

func (gn *GossipNode) Send(msg []byte, to transport.Conn) error {
	// log.Println("gn sending msg to peer")
	now := time.Now().Unix()
	nowBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(nowBytes, uint64(now))
	msgBytes := append(nowBytes, msg...)
	return to.Send(data.Message{
		Type:    GLOBAL_GOSSIP_MSG_TYPE,
		Payload: msgBytes,
	})
}

func (gn *GossipNode) onGossipReceived(msgBytes []byte, from hyparview.Peer) {
	// msgBytes := make([]byte, 0)

	// err := json.Unmarshal(msgBytes1, &msgBytes)
	// if err != nil {
	// 	// log.Println(err)
	// 	return err
	// }
	// log.Println("received gossip msg", msgBytes)
	hashFn := fnv.New64()
	_, err := hashFn.Write(msgBytes)
	if err != nil {
		// log.Println("Error creating hash:", err)
		return
	}
	msgId := hashFn.Sum(nil)
	gn.lock.Lock()
	defer gn.lock.Unlock()
	if _, ok := gn.seenMessages[string(msgId)]; ok {
		// log.Println("msg already seen:", msgBytes)
		return
	}
	gn.seenMessages[string(msgId)] = msgSeen{time: time.Now()}
	msg := msgBytes[8:]
	// log.Println("Received:", msg)
	if gn.gossipHandler != nil {
		gn.lock.Unlock()
		proceed := gn.gossipHandler(msg, from.Conn)
		gn.lock.Lock()
		if !proceed {
			// log.Println("quit broadcasting signal ...")
			return
		}
	}
	for _, peer := range gn.membership.GetPeers(100) {
		if peer.Conn.GetAddress() == from.Conn.GetAddress() {
			continue
		}
		err := peer.Conn.Send(data.Message{
			Type:    GLOBAL_GOSSIP_MSG_TYPE,
			Payload: msgBytes,
		})
		if err != nil {
			// log.Println("error while forwarding msg in global network", err)
		}
	}
}

func (gn *GossipNode) AddGossipHandler(handler func([]byte, transport.Conn) bool) {
	gn.gossipHandler = handler
}

func (gn *GossipNode) AddPeerUpHandler(handler func() (send bool, msg []byte)) {
	gn.peerUpHandler = handler
}
