package monoceros

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/transport"
	"github.com/c12s/plumtree"
)

const (
	REGIONAL_NETWORK       = "RN"
	REGIONAL_ROOTS_NETWORK = "RRN"
)

type ActiveAggregationReq struct {
	Tree       plumtree.TreeMetadata
	Timestamp  int64
	WaitingFor []data.Node
	Aggregate  int64
	Scores     map[string]float64
}

type TreeOverlay struct {
	ID                  string
	plumtree            *plumtree.Plumtree
	lastAggregationTime int64
	rank                int64
	aggregate           chan struct{}
	local               *plumtree.TreeMetadata
	activeRequests      []*ActiveAggregationReq
	history             map[int64]any
	joined              bool
}

type Monoceros struct {
	GN            *GossipNode
	RN            *TreeOverlay
	RRN           *TreeOverlay
	regionalRoots map[string]string
	config        Config
	logger        *log.Logger
	lock          *sync.Mutex
	synced        bool
}

func NewMonoceros(rn, rrn *plumtree.Plumtree, gn *GossipNode, config Config, logger *log.Logger) *Monoceros {
	return &Monoceros{
		GN: gn,
		RN: &TreeOverlay{
			ID:             REGIONAL_NETWORK,
			plumtree:       rn,
			aggregate:      make(chan struct{}),
			activeRequests: make([]*ActiveAggregationReq, 0),
			history:        make(map[int64]any),
		},
		RRN: &TreeOverlay{
			ID:             REGIONAL_ROOTS_NETWORK,
			plumtree:       rrn,
			aggregate:      make(chan struct{}),
			activeRequests: make([]*ActiveAggregationReq, 0),
			history:        make(map[int64]any),
		},
		regionalRoots: make(map[string]string),
		config:        config,
		logger:        logger,
		lock:          new(sync.Mutex),
		synced:        false,
	}
}

// unlocked
func (m *Monoceros) Start() {
	m.GN.AddGossipHandler(m.onGlobalMsg)
	m.GN.AddPeerUpHandler(m.syncState)
	err := m.GN.membership.Join(m.config.GNContactID, m.config.GNContactAddr)
	if err != nil {
		log.Fatal(err)
	}
	err = m.RN.plumtree.Join(m.config.RNContactID, m.config.RNContactAddr)
	if err != nil {
		log.Fatal(err)
	}
	m.RN.joined = true
	if len(m.GN.membership.GetPeers(1)) == 0 {
		m.synced = true
		m.init()
	}
}

func (m *Monoceros) init() {
	m.initRN()
	go m.initAggregation(m.RN)
	go m.tryTriggerAggregation(m.RN)

	m.initRRN()
	go m.initAggregation(m.RRN)
	go m.tryTriggerAggregation(m.RRN)
}

// unlocked
func (m *Monoceros) initRN() {
	m.RN.plumtree.OnTreeDestroyed(func(tree plumtree.TreeMetadata) {
		m.cleanUpTree(m.RN, tree)
		m.leaveRRN(tree)
	})
	m.RN.plumtree.OnTreeConstructed(m.joinRRN)
	m.RN.plumtree.OnGossip(m.onGossipMsg)
	m.RN.plumtree.OnDirect(m.onDirectMsg)
	go m.tryPromote(m.RN)
}

// unlocked
func (m *Monoceros) initRRN() {
	m.RRN.plumtree.OnTreeDestroyed(func(tree plumtree.TreeMetadata) {
		m.cleanUpTree(m.RRN, tree)
	})
	m.RRN.plumtree.OnGossip(m.onGossipMsg)
	m.RRN.plumtree.OnDirect(m.onDirectMsg)
	go m.tryPromote(m.RRN)
}

// locked
func (m *Monoceros) cleanUpTree(network *TreeOverlay, tree plumtree.TreeMetadata) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.logger.Println("clean up tree", network.ID, tree)
	m.logger.Println("active requests", network.activeRequests)
	network.activeRequests = slices.DeleteFunc(network.activeRequests, func(r *ActiveAggregationReq) bool {
		return r.Tree.Id == tree.Id
	})
	m.logger.Println("active requests", network.activeRequests)
	if tree.Id == network.local.Id {
		network.local = nil
		m.logger.Println("local tree removed")
	}
}

// locked
func (m *Monoceros) tryPromote(network *TreeOverlay) {
	for range time.NewTicker(1 * time.Second).C {
		m.lock.Lock()
		peersNum := network.plumtree.GetPeersNum()
		expectedAggregationTime := (network.lastAggregationTime + 2*(network.rank*m.config.Aggregation.TAggSec))
		now := time.Now().Unix()
		// da se ne bi menjao root svaki put kad se prikljuci novi cvor
		// if (peersNum == 0 || (netwok.rank > 0 && expectedAggregationTime < now)) && network.local == nil {
		if network.joined && (peersNum == 0 || expectedAggregationTime < now) && network.local == nil {
			m.promote(network)
		}
		m.lock.Unlock()
	}
}

// locked by caller
func (m *Monoceros) promote(network *TreeOverlay) {
	m.logger.Println("promoting", network.ID)
	tree := plumtree.TreeMetadata{
		Id:    fmt.Sprintf("%s_%s", network.ID, m.config.NodeID),
		Score: 1,
	}
	err := network.plumtree.ConstructTree(tree)
	if err != nil {
		m.logger.Println("err while promoting node", err)
		return
	}
	network.local = &tree
}

// unlocked
func (m *Monoceros) tryTriggerAggregation(network *TreeOverlay) {
	for range time.NewTicker(time.Duration(m.config.Aggregation.TAggSec) * time.Second).C {
		if network.local != nil {
			network.aggregate <- struct{}{}
		}
	}
}

// locked
func (m *Monoceros) initAggregation(network *TreeOverlay) {
	for range network.aggregate {
		m.lock.Lock()
		if network.local == nil {
			m.lock.Unlock()
			continue
		}
		msg := AggregationReq{
			Timestamp: time.Now().Unix(),
		}
		m.logger.Println("init aggregation", msg)
		msgBytes, err := Serialize(msg)
		if err != nil {
			m.lock.Unlock()
			m.logger.Println("error serializing aggregation request", err)
			continue
		}
		networkId := network.local.Id
		m.lock.Unlock()
		err = network.plumtree.Gossip(networkId, AGGREGATION_REQ_MSG_TYPE, msgBytes)
		if err != nil {
			m.logger.Println("error broadcasting aggregation request", err)
		}
	}
}

// locked
func (m *Monoceros) onGossipMsg(tree plumtree.TreeMetadata, msgType string, msg []byte, sender data.Node) bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.logger.Println("received aggregation msg", tree, msgType, string(msg), "from", sender.ID)
	network := m.resolveNetwork(tree.Id)
	if network == nil {
		m.logger.Println("unknown network on message received", tree, msgType, string(msg))
		return false
	}
	if msgType == AGGREGATION_REQ_MSG_TYPE {
		req := AggregationReq{}
		err := json.Unmarshal(msg, &req)
		if err != nil {
			m.logger.Println("error while unmarshalling aggregation req", msg)
			return false
		}
		return m.onAggregationReq(network, tree, req)
	}
	if msgType == RANK_LIST_MSG_TYPE {
		list := RankList{}
		err := json.Unmarshal(msg, &list)
		if err != nil {
			m.logger.Println("error while unmarshalling rank list", msg)
			return true
		}
		m.onRanklist(network, list)
		return true
	}
	return true
}

// locked
func (m *Monoceros) onDirectMsg(tree plumtree.TreeMetadata, msgType string, msg []byte, sender data.Node) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.logger.Println("received direct msg", tree, msgType, string(msg), "from", sender.ID)
	network := m.resolveNetwork(tree.Id)
	if network == nil {
		m.logger.Println("unknown network on message received", tree, msgType, string(msg))
		return
	}
	if msgType == AGGREGATION_RESP_MSG_TYPE {
		resp := AggregationResp{}
		err := json.Unmarshal(msg, &resp)
		if err != nil {
			m.logger.Println("error while unmarshalling aggregation resp", msg)
			return
		}
		m.onAggregationResp(network, tree, resp, sender)
	} else if msgType == ABORT_RESP_MSG_TYPE {
		resp := AbortResp{}
		err := json.Unmarshal(msg, &resp)
		if err != nil {
			m.logger.Println("error while unmarshalling abort resp", msg, err)
			return
		}
		m.onAbortResp(network, tree, resp, sender)
	}
}

// locked
func (m *Monoceros) onGlobalMsg(msgBytes []byte, from transport.Conn) bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.logger.Println("received global network msg", msgBytes)
	msgType := msgBytes[0]
	if msgType == RRUPDATE_MSG_TYPE {
		m.logger.Println("received regional root update")
		msg := RRUpdate{}
		err := json.Unmarshal(msgBytes[1:], &msg)
		if err != nil {
			m.logger.Println("error while unmarshalling", msg, err)
			return false
		}
		if msg.Joined {
			m.regionalRoots[msg.Node.ID] = msg.Node.ListenAddress
		} else {
			delete(m.regionalRoots, msg.Node.ID)
		}
		m.logger.Println(m.regionalRoots)
		return true
	} else if msgType == SYNC_REQ_MSG_TYPE {
		m.logger.Println("received sync req")
		msg := SyncStateResp{
			RegionalRoots: m.regionalRoots,
		}
		msgBytes, err := json.Marshal(msg)
		if err != nil {
			m.logger.Println(err)
			return false
		}
		msgBytes = append([]byte{SYNC_RESP_MSG_TYPE}, msgBytes...)
		m.logger.Println("sending sync resp", msgBytes)
		if from == nil {
			m.logger.Println("sender is nil, cannot respond")
			return false
		}
		err = m.GN.Send(msgBytes, from)
		if err != nil {
			m.logger.Println("error sending sync resp", err)
		}
		return false
	} else if msgType == SYNC_RESP_MSG_TYPE {
		m.logger.Println("received sync resp")
		msg := SyncStateResp{}
		err := json.Unmarshal(msgBytes[1:], &msg)
		if err != nil {
			m.logger.Println("error while unmarshalling", msg, err)
			return false
		}
		if m.synced {
			m.logger.Println("already synced, ignoring msg")
			return false
		}
		m.regionalRoots = msg.RegionalRoots
		m.synced = true
		m.logger.Println(m.regionalRoots)
		m.init()
		return false
	} else {
		m.logger.Println("unknows msg type", msgType)
		return false
	}
}

// locked by caller
func (m *Monoceros) onAggregationReq(network *TreeOverlay, tree plumtree.TreeMetadata, req AggregationReq) bool {
	if network.local != nil && network.local.Id != tree.Id {
		if tree.HasHigherScore(*network.local) {
			m.logger.Println("tree", tree, "has higher score than local tree, destroying ...")
			err := network.plumtree.DestroyTree(*network.local)
			if err != nil {
				m.logger.Println("error while destorying local tree", err)
			}
		} else if network.plumtree.HasParent(tree.Id) {
			resp := AbortResp{
				Timestamp: req.Timestamp,
			}
			respBytes, err := Serialize(resp)
			if err != nil {
				m.logger.Println("error marshalling abort resp", err)
			} else {
				m.lock.Unlock()
				err = network.plumtree.SendToParent(tree.Id, ABORT_RESP_MSG_TYPE, respBytes)
				m.lock.Lock()
				if err != nil {
					m.logger.Println("error sending abort resp", err)
				} else {
					m.logger.Println("local tree", network.local, "has higher score than tree", tree, "destroying ...")
					err := network.plumtree.DestroyTree(tree)
					if err != nil {
						m.logger.Println("error while destorying local tree", err)
					}
					return false
				}
			}
		}
	}
	m.processAggregationReq(network, tree, req)
	return true
}

// locked by caller
func (m *Monoceros) onAggregationResp(network *TreeOverlay, tree plumtree.TreeMetadata, resp AggregationResp, sender data.Node) {
	index := slices.IndexFunc(network.activeRequests, func(r *ActiveAggregationReq) bool {
		return r.Tree.Id == tree.Id && r.Timestamp == resp.Timestamp
	})
	if index < 0 {
		m.logger.Println("could not find active request for response", resp, "active requests", network.activeRequests)
		return
	}
	req := network.activeRequests[index]
	senderIndex := slices.IndexFunc(req.WaitingFor, func(p data.Node) bool {
		return p.ID == sender.ID
	})
	if senderIndex < 0 {
		m.logger.Println("could not find sender in active request", req.WaitingFor, "sender", sender)
		return
	}
	req.Aggregate = req.Aggregate + resp.Aggregate
	for id, score := range resp.Scores {
		req.Scores[id] = score
	}
	req.WaitingFor = slices.Delete(req.WaitingFor, senderIndex, senderIndex+1)
	children, _ := network.plumtree.GetChildren(tree.Id)
	if len(IntersectPeers(children, req.WaitingFor)) == 0 {
		m.logger.Println(len(network.activeRequests))
		m.logger.Println(index)
		network.activeRequests = slices.Delete(network.activeRequests, index, index+1)
		m.completeAggregationReq(network, tree, req.Timestamp, req.Aggregate, req.Scores, false)
	}
}

// locked by caller
func (m *Monoceros) onAbortResp(network *TreeOverlay, tree plumtree.TreeMetadata, resp AbortResp, sender data.Node) {
	index := slices.IndexFunc(network.activeRequests, func(r *ActiveAggregationReq) bool {
		return r.Tree.Id == tree.Id && r.Timestamp == resp.Timestamp
	})
	if index < 0 {
		m.logger.Println("could not find active request for response", resp, "active requests", network.activeRequests)
		return
	}
	req := network.activeRequests[index]
	network.activeRequests = slices.Delete(network.activeRequests, index, index+1)
	m.completeAggregationReq(network, tree, req.Timestamp, req.Aggregate, req.Scores, true)
}

// locked by caller
func (m *Monoceros) onRanklist(network *TreeOverlay, list RankList) {
	m.logger.Println("received rank list", list)
	network.rank = GetNodeRank(m.config.NodeID, list.List)
	m.logger.Println("rank", network.rank)
}

// locked by caller
func (m *Monoceros) processAggregationReq(network *TreeOverlay, tree plumtree.TreeMetadata, req AggregationReq) {
	network.lastAggregationTime = int64(math.Max(float64(req.Timestamp), float64(network.lastAggregationTime)))
	var localVal int64 = 1
	localScores := map[string]float64{m.config.NodeID: 1}
	receivers, err := network.plumtree.GetChildren(tree.Id)
	if err != nil {
		m.logger.Println("error while fetching tree children", tree, err)
	}
	if len(receivers) == 0 {
		m.completeAggregationReq(network, tree, req.Timestamp, localVal, localScores, false)
	} else {
		aar := &ActiveAggregationReq{
			Tree:       tree,
			Timestamp:  req.Timestamp,
			WaitingFor: receivers,
			Aggregate:  localVal,
			Scores:     localScores,
		}
		network.activeRequests = append(network.activeRequests, aar)
	}
}

// locked by caller
func (m *Monoceros) completeAggregationReq(network *TreeOverlay, tree plumtree.TreeMetadata, timestamp int64, value int64, scores map[string]float64, abort bool) {
	m.logger.Println("complete aggregation req", network, tree)
	m.logger.Println(network.local)
	m.logger.Println(tree.Id)
	if network.plumtree.HasParent(tree.Id) {
		m.logger.Println("has parent")
		var resp any = nil
		var respType string = ""
		if abort {
			resp = AbortResp{
				Timestamp: timestamp,
			}
			respType = ABORT_RESP_MSG_TYPE
		} else {
			resp = AggregationResp{
				Timestamp: timestamp,
				Aggregate: value,
				Scores:    scores,
			}
			respType = AGGREGATION_RESP_MSG_TYPE
		}
		respBytes, err := Serialize(resp)
		if err != nil {
			m.logger.Println("error marshalling resp", err)
			return
		}
		m.lock.Unlock()
		err = network.plumtree.SendToParent(tree.Id, respType, respBytes)
		m.lock.Lock()
		if err != nil {
			m.logger.Println("error sending resp", err)
		}
	} else if network.local != nil && tree.Id == network.local.Id {
		m.logger.Println("req done")
		if abort {
			m.logger.Println("should destroy local tree")
			err := network.plumtree.DestroyTree(*network.local)
			if err != nil {
				m.logger.Println("error while destorying local tree", err)
			}
		} else {
			network.history[timestamp] = value
			m.logger.Println("history", network.history)
			list, err := Serialize(RankList{List: scores})
			if err != nil {
				m.logger.Println("error marshalling rank list", err)
			} else {
				m.logger.Println("sending rank list", list)
				m.lock.Unlock()
				err = network.plumtree.Gossip(tree.Id, RANK_LIST_MSG_TYPE, list)
				m.lock.Lock()
				if err != nil {
					m.logger.Println("error sending rank list", err)
				} else {
					m.logger.Println("sent rank list")
				}
			}
			// eval rules
			// demote if necessary and possible
		}
	} else {
		m.logger.Println("no conditions met")
	}
}

// locked
func (m *Monoceros) syncState() (bool, []byte) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.logger.Println("global network peer up")
	if m.synced {
		m.logger.Println("already synced, no need to send sync req")
		return false, nil
	}
	return true, []byte{SYNC_REQ_MSG_TYPE}
}

// locked
func (m *Monoceros) joinRRN(tree plumtree.TreeMetadata) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.logger.Println("tree constructed in RN, should join RRN", tree)
	if tree.Id != fmt.Sprintf("%s_%s", m.RN.ID, m.config.NodeID) {
		m.logger.Println("should not")
		return
	}
	for id, address := range m.regionalRoots {
		err := m.RRN.plumtree.Join(id, address)
		if err != nil {
			m.logger.Println("error while joining rrn")
			continue
		}
		break
	}
	m.RRN.joined = true
	gossip := RRUpdate{
		Joined: true,
		Node: data.Node{
			ID:            m.config.NodeID,
			ListenAddress: m.RRN.plumtree.ListenAddress(),
		},
	}
	gossipBytes, err := json.Marshal(gossip)
	if err != nil {
		m.logger.Println(err)
		return
	}
	gossipBytes = append([]byte{RRUPDATE_MSG_TYPE}, gossipBytes...)
	m.logger.Println("sending rrn update", gossipBytes)
	m.lock.Unlock()
	m.GN.Broadcast(gossipBytes)
	m.lock.Lock()
}

// locked
func (m *Monoceros) leaveRRN(tree plumtree.TreeMetadata) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.logger.Println("tree destroyed in RN, should leave RRN", tree)
	if tree.Id != fmt.Sprintf("%s_%s", m.RN.ID, m.config.NodeID) {
		m.logger.Println("should not")
		return
	}
	if m.RRN.local != nil {
		err := m.RRN.plumtree.DestroyTree(*m.RRN.local)
		if err != nil {
			m.logger.Println(err)
		}
	}
	m.RRN.joined = false
	m.RRN.plumtree.Leave()
	gossip := RRUpdate{
		Joined: false,
		Node: data.Node{
			ID:            m.config.NodeID,
			ListenAddress: m.RRN.plumtree.ListenAddress(),
		},
	}
	gossipBytes, err := json.Marshal(gossip)
	if err != nil {
		m.logger.Println(err)
		return
	}
	gossipBytes = append([]byte{RRUPDATE_MSG_TYPE}, gossipBytes...)
	m.logger.Println("sending rrn update", gossipBytes)
	m.lock.Unlock()
	m.GN.Broadcast(gossipBytes)
	m.lock.Lock()
}

func (m *Monoceros) resolveNetwork(treeId string) *TreeOverlay {
	parts := strings.Split(treeId, "_")
	if len(parts) < 2 {
		return nil
	}
	networkId := parts[0]
	if networkId == REGIONAL_NETWORK {
		return m.RN
	} else if networkId == REGIONAL_ROOTS_NETWORK {
		return m.RRN
	}
	return nil
}

func IntersectPeers(a, b []data.Node) []data.Node {
	idSet := make(map[string]struct{})
	var result []data.Node
	for _, peer := range b {
		idSet[peer.ID] = struct{}{}
	}
	for _, peer := range a {
		if _, found := idSet[peer.ID]; found {
			result = append(result, peer)
		}
	}
	return result
}

func GetNodeRank(nodeID string, scores map[string]float64) int64 {
	targetScore, exists := scores[nodeID]
	if !exists {
		return -1
	}
	rank := int64(1)
	for id, score := range scores {
		if id == nodeID {
			continue
		}
		if score > targetScore || (score == targetScore && id > nodeID) {
			rank++
		}
	}
	return rank
}
