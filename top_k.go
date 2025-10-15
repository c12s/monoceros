package monoceros

import (
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/hyparview"
	"github.com/c12s/hyparview/transport"
	"github.com/c12s/plumtree"
)

func (m *Monoceros) HasHigherScore(nodeID string, score int) bool {
	return score > int(m.Score()) || (score == int(m.Score()) && nodeID > m.config.NodeID)
}

func startPeriodic(fn func(), interval time.Duration) {
	go func() {
		for range time.NewTicker(interval).C {
			fn()
		}
	}()
}

const TOP_K_MSG_TYPE data.MessageType = plumtree.GRAFT_MSG_TYPE + 1

type TopKMsg struct {
	Nodes []NodeInfo
}

type NodeInfo struct {
	NodeID string
	Val    float64
	TTL    int
}

func (m *Monoceros) Score() float64 {
	valStr := strings.Split(m.config.NodeID, "_")[2]
	val, err := strconv.Atoi(valStr)
	if err != nil {
		m.logger.Println(err)
		return 0
	}
	return float64(val)
}

func (n *TreeOverlay) onTopKMsg(msgBytes []byte, sender hyparview.Peer) {
	msg := &TopKMsg{}
	err := transport.Deserialize(msgBytes, msg)
	if err != nil {
		n.logger.Println("failed to deserialize TopK message", "error", err)
		return
	}

	n.lock.Lock()
	defer n.lock.Unlock()

	maxMap := make(map[string]NodeInfo, len(n.Max))
	for _, ni := range n.Max {
		maxMap[ni.NodeID] = ni
	}

	for _, incoming := range msg.Nodes {
		if existing, ok := maxMap[incoming.NodeID]; ok {
			existing.Val = incoming.Val
			if incoming.TTL > existing.TTL {
				existing.TTL = incoming.TTL
			}
			maxMap[incoming.NodeID] = existing
		} else {
			maxMap[incoming.NodeID] = incoming
		}
	}

	merged := make([]NodeInfo, 0, len(maxMap))
	for _, ni := range maxMap {
		merged = append(merged, ni)
	}
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Val > merged[j].Val
	})
	if len(merged) > n.K {
		merged = merged[:n.K]
	}
	n.Max = merged
}

func (n *TreeOverlay) gossipTopK() {
	n.lock.Lock()

	// Step 1: decrease TTLs and remove expired nodes
	newMax := make([]NodeInfo, 0, len(n.Max))
	for _, ni := range n.Max {
		if ni.NodeID == n.plumtree.Protocol.Self().ID {
			newMax = append(newMax, ni)
			continue
		}
		ni.TTL--
		if ni.TTL > 0 {
			newMax = append(newMax, ni)
		}
	}
	n.Max = newMax

	// Step 2: ensure this node is present if needed
	found := false
	for _, ni := range n.Max {
		if ni.NodeID == n.plumtree.Protocol.Self().ID {
			found = true
			break
		}
	}

	if !found && len(n.Max) < n.K {
		n.Max = append(n.Max, NodeInfo{
			NodeID: n.plumtree.Protocol.Self().ID,
			Val:    float64(n.Value),
			TTL:    n.TTL,
		})
		found = true
	}
	sort.Slice(n.Max, func(i, j int) bool {
		return n.Max[i].Val > n.Max[j].Val
	})
	// if n.Max[0].NodeID == n.plumtree.Protocol.Self().ID {
	if found {
		n.SelfFirstRounds++
	} else {
		n.SelfFirstRounds = 0
	}

	// Step 3: prepare message
	msg := TopKMsg{Nodes: make([]NodeInfo, len(n.Max))}
	copy(msg.Nodes, n.Max)

	n.lock.Unlock()

	// Step 4: broadcast (or send)
	hvMsg := data.Message{
		Type:    TOP_K_MSG_TYPE,
		Payload: msg,
	}
	for _, peer := range n.plumtree.GetPeers() {
		err := peer.Conn.Send(hvMsg)
		if err != nil {
			n.logger.Println(err)
		}
	}

	// Optional: log for debugging
	n.logger.Printf("[%s] sent message: %+v\n", n.plumtree.Protocol.Self().ID, msg)
}

// locked by caller
func (n *TreeOverlay) shouldPromote() bool {
	if !n.joined || n.local != nil {
		return false
	}
	if !slices.ContainsFunc(n.Max, func(ni NodeInfo) bool {
		return ni.NodeID == n.plumtree.Protocol.Self().ID
	}) {
		return false
	}
	idx := slices.IndexFunc(n.Max, func(ni NodeInfo) bool {
		return ni.NodeID == n.plumtree.Protocol.Self().ID
	})
	stableInList := n.SelfFirstRounds >= n.TTL
	if idx == 0 && stableInList {
		return true
	}
	// todo: adaptive
	delayMS := 500
	timeForReq := n.lastAggregationTime+int64(n.TAggSec)*1000000000+int64(idx)*int64(delayMS)*1000000 < time.Now().UnixNano()
	backoffExpired := n.lastCancelled+int64(delayMS)*1000000 < time.Now().UnixNano()
	return stableInList && timeForReq && backoffExpired
}
