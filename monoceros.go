package monoceros

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/c12s/hyparview/data"
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
}

type Monoceros struct {
	RN     *TreeOverlay
	RRN    *TreeOverlay
	config Config
	logger *log.Logger
}

func NewMonoceros(rn *plumtree.Plumtree, config Config, logger *log.Logger) *Monoceros {
	return &Monoceros{
		RN: &TreeOverlay{
			ID:             REGIONAL_NETWORK,
			plumtree:       rn,
			aggregate:      make(chan struct{}),
			activeRequests: make([]*ActiveAggregationReq, 0),
			history:        make(map[int64]any),
		},
		RRN:    nil,
		config: config,
		logger: logger,
	}
}

func (m *Monoceros) Start() {
	m.initRN()
	go m.initAggregation(m.RN)
	go m.tryTriggerAggregation(m.RN)
}

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

func (m *Monoceros) cleanUpTree(network *TreeOverlay, tree plumtree.TreeMetadata) {
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

func (m *Monoceros) tryPromote(network *TreeOverlay) {
	for range time.NewTicker(1 * time.Second).C {
		peersNum := network.plumtree.GetPeersNum()
		expectedAggregationTime := (network.lastAggregationTime + 2*(network.rank*m.config.TAggSec))
		now := time.Now().Unix()
		if (peersNum == 0 || (network.rank > 0 && expectedAggregationTime < now)) && network.local == nil {
			m.promote(network)
		}
	}
}

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

func (m *Monoceros) tryTriggerAggregation(network *TreeOverlay) {
	for range time.NewTicker(time.Duration(m.config.TAggSec) * time.Second).C {
		if network.local != nil {
			network.aggregate <- struct{}{}
		}
	}
}

func (m *Monoceros) initAggregation(network *TreeOverlay) {
	for range network.aggregate {
		msg := AggregationReq{
			Timestamp: time.Now().Unix(),
		}
		m.logger.Println("init aggregation", msg)
		msgBytes, err := Serialize(msg)
		if err != nil {
			m.logger.Println("error serializing aggregation request", err)
			continue
		}
		err = network.plumtree.Gossip(network.local.Id, AGGREGATION_REQ_MSG_TYPE, msgBytes)
		if err != nil {
			m.logger.Println("error broadcasting aggregation request", err)
		}
	}
}

func (m *Monoceros) onGossipMsg(tree plumtree.TreeMetadata, msgType string, msg []byte, sender data.Node) bool {
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

func (m *Monoceros) onDirectMsg(tree plumtree.TreeMetadata, msgType string, msg []byte, sender data.Node) {
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
				err = network.plumtree.SendToParent(tree.Id, ABORT_RESP_MSG_TYPE, respBytes)
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
		network.activeRequests = slices.Delete(network.activeRequests, index, index+1)
		m.completeAggregationReq(network, tree, req.Timestamp, req.Aggregate, req.Scores, false)
	}
}

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

func (m *Monoceros) onRanklist(network *TreeOverlay, list RankList) {
	m.logger.Println("received rank list", list)
	network.rank = GetNodeRank(m.config.NodeID, list.List)
	m.logger.Println("rank", network.rank)
}

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
		err = network.plumtree.SendToParent(tree.Id, respType, respBytes)
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
				err = network.plumtree.Gossip(tree.Id, RANK_LIST_MSG_TYPE, list)
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

func (m *Monoceros) joinRRN(tree plumtree.TreeMetadata) {

}

func (m *Monoceros) leaveRRN(tree plumtree.TreeMetadata) {

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
