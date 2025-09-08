package monoceros

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"maps"

	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/hyparview"
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
	Aggregate  []IntermediateMetric
	Scores     map[string]float64
	Cancel     bool
	Sender     transport.Conn
}

type TreeOverlay struct {
	ID                     string
	plumtree               *plumtree.Plumtree
	lastAggregationTime    int64
	rank                   int64
	maxRank                int64
	aggregate              chan struct{}
	local                  *plumtree.TreeMetadata
	localAggCount          int64
	activeRequests         []*ActiveAggregationReq
	joined                 bool
	getLocalMetrics        func() []IntermediateMetric
	AdditionalMetricLabels map[string]string
	quit                   chan struct{}
	knownScores            map[string]ScoreMsg
	lock                   *sync.Mutex
	logger                 *log.Logger
}

type Monoceros struct {
	GN              *GossipNode
	RN              *TreeOverlay
	RRN             *TreeOverlay
	Roots           map[string]RootInfo
	rules           []AggregationRule
	latestMetrics   map[string]string
	latestIM        map[string][]IntermediateMetric
	latestMetricsTs map[string]int64
	targets         []ScrapeTarget
	config          Config
	logger          *log.Logger
	lock            *sync.Mutex
	synced          bool
}

func NewMonoceros(rn, rrn *plumtree.Plumtree, gn *GossipNode, config Config, logger *log.Logger) *Monoceros {
	m := &Monoceros{
		GN:    gn,
		Roots: make(map[string]RootInfo),
		rules: []AggregationRule{
			{
				InputSelector: MetricMetadata{
					Name: "app_memory_usage_bytes",
				},
				Func: SUM_FUNC,
				Output: MetricMetadata{
					Name: "total_app_memory_usage_bytes",
					Labels: map[string]string{
						"func": "sum",
					},
				},
			},
			{
				InputSelector: MetricMetadata{
					Name: "app_memory_usage_bytes",
				},
				Func: AVG_FUNC,
				Output: MetricMetadata{
					Name: "avg_app_memory_usage_bytes",
					Labels: map[string]string{
						"func": "avg",
					},
				},
			},
			{
				InputSelector: MetricMetadata{
					Name: "avg_app_memory_usage_bytes",
				},
				Func: AVG_FUNC,
				Output: MetricMetadata{
					Name: "avg_app_memory_usage_bytes",
					Labels: map[string]string{
						"global": "y",
					},
				},
			},
			{
				InputSelector: MetricMetadata{
					Name: "total_app_memory_usage_bytes",
				},
				Func: SUM_FUNC,
				Output: MetricMetadata{
					Name: "total_app_memory_usage_bytes",
					Labels: map[string]string{
						"global": "y",
					},
				},
			},
		},
		latestMetrics:   make(map[string]string),
		latestIM:        make(map[string][]IntermediateMetric),
		latestMetricsTs: make(map[string]int64),
		targets: []ScrapeTarget{
			{
				Name:    "test_target",
				Address: "127.0.0.1:9200",
			},
		},
		config: config,
		logger: logger,
		lock:   new(sync.Mutex),
		synced: false,
	}
	m.RN = &TreeOverlay{
		ID:                     REGIONAL_NETWORK,
		plumtree:               rn,
		aggregate:              make(chan struct{}),
		activeRequests:         make([]*ActiveAggregationReq, 0),
		getLocalMetrics:        m.getLatestForNode,
		AdditionalMetricLabels: map[string]string{"level": "region", "regionID": config.Region},
		rank:                   1,
		maxRank:                1,
		quit:                   make(chan struct{}),
		knownScores:            make(map[string]ScoreMsg),
		lock:                   m.lock,
		logger:                 m.logger,
	}
	m.RRN = &TreeOverlay{
		ID:                     REGIONAL_ROOTS_NETWORK,
		plumtree:               rrn,
		aggregate:              make(chan struct{}),
		activeRequests:         make([]*ActiveAggregationReq, 0),
		getLocalMetrics:        m.getLatestForRegion,
		AdditionalMetricLabels: map[string]string{"level": "global"},
		rank:                   1,
		maxRank:                1,
		quit:                   make(chan struct{}),
		knownScores:            make(map[string]ScoreMsg),
		lock:                   m.lock,
		logger:                 m.logger,
	}
	rn.Protocol.AddClientMsgHandler(SCORE_MSG_TYPE, m.RN.onScoreMsg)
	// rrn.Protocol.AddClientMsgHandler(SCORE_MSG_TYPE, m.RRN.onScoreMsg)
	return m
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

	clearActive := func(network *TreeOverlay) {
		for range time.NewTicker(time.Second).C {
			// m.logger.Println("try lock")
			m.lock.Lock()
			toRemove := make([]*ActiveAggregationReq, 0)
			for _, aar := range network.activeRequests {
				if aar == nil {
					continue
				}
				// m.logger.Println("should clean up active request", *aar)
				children, _ := network.plumtree.GetChildren(aar.Tree.Id)
				// todo: ??
				if len(IntersectPeers(children, aar.WaitingFor)) == 0 || aar.Timestamp+(3*m.config.Aggregation.TAggSec*1000000000) < time.Now().UnixNano() {
					// if len(IntersectPeers(children, aar.WaitingFor)) == 0 {
					// m.logger.Println("should")
					toRemove = append(toRemove, aar)
					m.completeAggregationReq(network, aar)
				} else {
					// m.logger.Println("should not")
				}
			}
			for _, remove := range toRemove {
				network.activeRequests = slices.DeleteFunc(network.activeRequests, func(r *ActiveAggregationReq) bool {
					return r.Tree.Id == remove.Tree.Id && r.Timestamp == remove.Timestamp
				})
			}
			m.lock.Unlock()
		}
	}
	go clearActive(m.RN)
	// go clearActive(m.RRN)
	go func() {
		for range time.NewTicker(time.Second).C {
			m.exportMsgCount()
		}
	}()
}

func (m *Monoceros) init() {
	m.initRN()
	go m.initAggregation(m.RN)
	// go m.tryTriggerAggregation(m.RN)

	// m.initRRN()
	// go m.initAggregation(m.RRN)
	// go m.tryTriggerAggregation(m.RRN)
}

// unlocked
func (m *Monoceros) initRN() {
	m.RN.plumtree.OnTreeDestroyed(func(tree plumtree.TreeMetadata) {
		m.cleanUpTree(m.RN, tree)
		// m.leaveRRN(tree)
	})
	// m.RN.plumtree.OnTreeConstructed(m.joinRRN)
	m.RN.plumtree.OnGossip(m.onGossipMsg)
	m.RN.plumtree.OnDirect(m.onDirectMsg)
	m.RN.plumtree.OnPeerDown(func(p hyparview.Peer) {
		m.logger.Println("onPeerDown", p.Node.ID)
		m.RN.removeScoreForPeer(p.Node.ID)
	})
	startPeriodic(m.RN.broadcastScore, m.config.ScoreGossipInterval())
	go m.tryPromote(m.RN)
}

// unlocked
// func (m *Monoceros) initRRN() {
// 	m.RRN.plumtree.OnTreeDestroyed(func(tree plumtree.TreeMetadata) {
// 		m.cleanUpTree(m.RRN, tree)
// 	})
// 	m.RRN.plumtree.OnGossip(m.onGossipMsg)
// 	m.RRN.plumtree.OnDirect(m.onDirectMsg)
// 	m.RRN.plumtree.OnPeerDown(func(p hyparview.Peer) {
// 		m.logger.Println("onPeerDown", p.Node.ID)
// 		m.RRN.removeScoreForPeer(p.Node.ID)
// 	})
// 	startPeriodic(m.RRN.broadcastScore, m.config.ScoreGossipInterval())
// 	go m.tryPromote(m.RRN)
// 	go m.gossipRoot()
// 	go m.rejoinRRN()
// }

// locked
func (m *Monoceros) cleanUpTree(network *TreeOverlay, tree plumtree.TreeMetadata) {
	m.lock.Lock()
	defer m.lock.Unlock()
	network.activeRequests = slices.DeleteFunc(network.activeRequests, func(r *ActiveAggregationReq) bool {
		return r.Tree.Id == tree.Id
	})
	if network.local != nil && tree.Id == network.local.Id {
		network.local = nil
		network.localAggCount = 0
		go func() {
			network.quit <- struct{}{}
		}()
		// m.logger.Println("local tree removed")
	}
}

// locked
func (m *Monoceros) tryPromote(network *TreeOverlay) {
	// todo: ??
	// if !strings.HasSuffix(m.config.NodeID, "9") {
	// 	return
	// 	// time.Sleep(120 * time.Second)
	time.Sleep(time.Duration(3*m.config.Aggregation.TAggSec) * time.Second)
	// }
	for range time.NewTicker(500 * time.Millisecond).C {
		m.lock.Lock()
		if !network.joined || network.local != nil {
			m.lock.Unlock()
			continue
		}
		// peersNum := network.plumtree.GetPeersNum()
		// problem: ako sporo konvergira kada se veliki broj pokrnee odjednom
		// onda se moze desiti da, nakon sto unisti svoje stablo, opet promovise sebe
		// iako ne bi trebao, broj poruka ostaje zauvek preveliki
		// todo: ??
		expectedAggregationTime := float64(network.lastAggregationTime) + float64(m.config.Aggregation.TAggSec*1000000000) + float64(network.rank-1)/float64(network.maxRank)*float64(m.config.Aggregation.TAggMaxSec*1000000000)
		now := time.Now().UnixNano()
		if expectedAggregationTime < float64(now) && network.highestScoreInNeighborhood() && network.rank > 0 {
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
		Score: Score(),
	}
	network.local = &tree
	m.lock.Unlock()
	err := network.plumtree.ConstructTree(tree)
	// m.logger.Println("try lock")
	m.lock.Lock()
	if err != nil {
		m.logger.Println("err while promoting node", err)
		network.local = nil
		network.localAggCount = 0
	} else {
		go m.triggerAggregation(network)
	}
}

// unlocked
// func (m *Monoceros) tryTriggerAggregation(network *TreeOverlay) {
// 	for range time.NewTicker(time.Duration(m.config.Aggregation.TAggSec) * time.Second).C {
// 		if network.local != nil {
// 			network.aggregate <- struct{}{}
// 		}
// 	}
// }

func (m *Monoceros) triggerAggregation(network *TreeOverlay) {
	if network.local != nil {
		network.aggregate <- struct{}{}
	}
	for {
		select {
		case <-time.NewTicker(time.Duration(m.config.Aggregation.TAggSec) * time.Second).C:
			// m.lock.Lock()
			// if network.local != nil && !network.highestScoreInNeighborhood() {
			// 	tree := *network.local
			// 	m.lock.Unlock()
			// 	err := network.plumtree.DestroyTree(tree)
			// 	if err != nil {
			// 		m.logger.Println(err)
			// 	}
			// 	m.lock.Lock()
			// }
			// m.lock.Unlock()
			if network.local != nil && network.localAggCount > 0 {
				network.aggregate <- struct{}{}
			}
		case <-network.quit:
			return
		}
	}
}

// locked
func (m *Monoceros) initAggregation(network *TreeOverlay) {
	for range network.aggregate {
		// m.logger.Println("try lock")
		m.lock.Lock()
		if network.local == nil {
			m.lock.Unlock()
			continue
		}
		msg := AggregationReq{
			Timestamp: time.Now().UnixNano(),
		}
		m.logger.Println(network.ID, "init aggregation", msg)
		msgBytes, err := Serialize(msg)
		if err != nil {
			m.lock.Unlock()
			m.logger.Println("error serializing aggregation request", err)
			continue
		}
		networkId := network.local.Id
		m.lock.Unlock()
		err = network.plumtree.Broadcast(networkId, AGGREGATION_REQ_MSG_TYPE, msgBytes)
		if err != nil {
			m.logger.Println("error broadcasting aggregation request", err)
		}
		// ako je rrn, ukloni druge iz regiona
		// if network.ID == m.RRN.ID {
		// 	m.lock.Lock()
		// 	for id, region := range m.regionalRootRegions {
		// 		if region != m.config.Region || id == m.config.NodeID {
		// 			continue
		// 		}
		// 		gossip := RRUpdate{
		// 			Joined: false,
		// 			NodeInfo: data.Node{
		// 				ID:            id,
		// 				ListenAddress: m.regionalRootAddresses[id],
		// 			},
		// 			Region: region,
		// 		}
		// 		gossipBytes, err := json.Marshal(gossip)
		// 		if err != nil {
		// 			// m.logger.Println(err)
		// 			return
		// 		}
		// 		gossipBytes = append([]byte{RRUPDATE_MSG_TYPE}, gossipBytes...)
		// 		// m.logger.Println("sending rrn update", gossipBytes)
		// 		m.lock.Unlock()
		// 		m.GN.Broadcast(gossipBytes)
		// 		// m.logger.Println("try lock")
		// 		m.lock.Lock()
		// 	}
		// 	m.lock.Unlock()
		// }
	}
}

// locked
func (m *Monoceros) onGossipMsg(tree plumtree.TreeMetadata, msgType string, msg []byte, sender hyparview.Peer) bool {
	// m.logger.Println("try lock")
	m.lock.Lock()
	defer m.lock.Unlock()
	// m.logger.Println("received aggregation msg", tree, msgType, string(msg), "from", sender.ID)
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
		return m.onAggregationReq(network, tree, req, sender.Conn)
	} else if msgType == AGGREGATION_RESULT_MSG_TYPE {
		result := AggregationResult{}
		err := json.Unmarshal(msg, &result)
		if err != nil {
			m.logger.Println("error while unmarshalling aggregation result", msg)
			return false
		}
		m.onAggregationResult(network, tree, result)
		return true
	} else {
		return false
	}
}

// locked
func (m *Monoceros) onDirectMsg(tree plumtree.TreeMetadata, msgType string, msg []byte, sender data.Node) {
	// m.logger.Println("try lock")
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
			m.logger.Println("error while unmarshalling aggregation resp", err, msg)
			return
		}
		m.onAggregationResp(network, tree, resp, sender)
	}
}

// locked
func (m *Monoceros) onGlobalMsg(msgBytes []byte, from transport.Conn) bool {
	// m.logger.Println("try lock")
	m.lock.Lock()
	defer m.lock.Unlock()
	// m.logger.Println("received global network msg", msgBytes)
	msgType := msgBytes[0]
	if msgType == RRUPDATE_MSG_TYPE {
		m.logger.Println("received regional root update")
		msg := RootInfo{}
		err := json.Unmarshal(msgBytes[1:], &msg)
		if err != nil {
			m.logger.Println("error while unmarshalling", msg, err)
			return false
		}
		root, ok := m.Roots[msg.Region]
		if !ok || (root.Time+(2*m.config.Aggregation.TAggSec*1000000000) < time.Now().UnixNano() && msg.Time > root.Time) {
			m.Roots[msg.Region] = msg
		}
		return true
	} else if msgType == SYNC_REQ_MSG_TYPE {
		m.logger.Println("received sync req")
		msg := SyncStateResp{
			RegionalRoots: m.Roots,
			Rules:         m.rules,
		}
		msgBytes, err := json.Marshal(msg)
		if err != nil {
			// m.logger.Println(err)
			return false
		}
		msgBytes = append([]byte{SYNC_RESP_MSG_TYPE}, msgBytes...)
		// m.logger.Println("sending sync resp", msgBytes)
		if from == nil {
			// m.logger.Println("sender is nil, cannot respond")
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
			// m.logger.Println("already synced, ignoring msg")
			return false
		}
		m.Roots = msg.RegionalRoots
		m.rules = msg.Rules
		m.synced = true
		// m.logger.Println(m.regionalRootAddresses)
		// m.logger.Println(m.regionalRootRegions)
		m.init()
		return false
	} else if msgType == RULE_ADDED_MSG_TYPE {
		m.logger.Println("received rule added")
		msg := RuleAdded{}
		err := json.Unmarshal(msgBytes[1:], &msg)
		if err != nil {
			m.logger.Println("error while unmarshalling", msg, err)
			return false
		}
		m.rules = append(m.rules, msg.Rule)
		// m.logger.Println(m.rules)
		return true
	} else if msgType == RULE_REMOVED_MSG_TYPE {
		m.logger.Println("received rule removed")
		msg := RuleRemoved{}
		err := json.Unmarshal(msgBytes[1:], &msg)
		if err != nil {
			m.logger.Println("error while unmarshalling", msg, err)
			return false
		}
		m.rules = slices.DeleteFunc(m.rules, func(r AggregationRule) bool { return r.ID == msg.RuleID })
		return true
	} else {
		m.logger.Println("unknows msg type", msgType)
		return false
	}
}

// locked by caller
func (m *Monoceros) onAggregationReq(network *TreeOverlay, tree plumtree.TreeMetadata, req AggregationReq, sender transport.Conn) bool {
	m.logger.Println("received aggregation request", tree.Id, "msg", req)
	// todo: ?? da li azurirati samo ako nije cancel == true
	network.lastAggregationTime = int64(math.Max(float64(time.Now().UnixNano()), float64(network.lastAggregationTime)))
	localForAggregation := network.getLocalMetrics()
	localScores := map[string]float64{m.config.NodeID: float64(Score())}
	receivers, err := network.plumtree.GetChildren(tree.Id)
	if err != nil {
		m.logger.Println("error while fetching tree children", tree, err)
	}
	// m.logger.Println("children to send req", receivers)
	// m.logger.Println("has parent", network.plumtree.HasParent(tree.Id))
	cancel := false
	if (!m.HasHigherScore(tree.NodeID(), tree.Score) || slices.ContainsFunc(network.activeRequests, func(r *ActiveAggregationReq) bool {
		return r != nil && r.Tree.Score > tree.Score
	})) && network.plumtree.HasParent(tree.Id) {
		cancel = true
	}
	aar := &ActiveAggregationReq{
		Tree:       tree,
		Timestamp:  req.Timestamp,
		WaitingFor: receivers,
		Aggregate:  localForAggregation,
		Cancel:     cancel,
		Scores:     localScores,
		Sender:     sender,
	}
	if len(receivers) == 0 || cancel {
		m.completeAggregationReq(network, aar)
	} else {
		network.activeRequests = append(network.activeRequests, aar)
	}
	return !cancel
}

// locked by caller
func (m *Monoceros) onAggregationResp(network *TreeOverlay, tree plumtree.TreeMetadata, resp AggregationResp, sender data.Node) {
	m.logger.Println("received aggregation response", tree.Id, "from", sender.ID, "msg", resp)
	index := slices.IndexFunc(network.activeRequests, func(r *ActiveAggregationReq) bool {
		return r.Tree.Id == tree.Id && r.Timestamp == resp.Timestamp
	})
	if index < 0 {
		// m.logger.Println("could not find active request for response", resp, "active requests", network.activeRequests)
		return
	}
	req := network.activeRequests[index]
	senderIndex := slices.IndexFunc(req.WaitingFor, func(p data.Node) bool {
		return p.ID == sender.ID
	})
	if senderIndex < 0 {
		// m.logger.Println("could not find sender in active request", req.WaitingFor, "sender", sender)
		return
	}
	// m.logger.Println("before combined", req.Aggregate)
	// m.logger.Println(resp.Aggregate)
	req.Aggregate = combineAggregates(req.Aggregate, resp.Aggregate, m.rules)
	req.Cancel = req.Cancel || resp.Cancel
	// m.logger.Println("combined", req.Aggregate)
	for id, score := range resp.Scores {
		req.Scores[id] = score
	}
	req.WaitingFor = slices.Delete(req.WaitingFor, senderIndex, senderIndex+1)
	children, _ := network.plumtree.GetChildren(tree.Id)
	if len(IntersectPeers(children, req.WaitingFor)) == 0 {
		// m.logger.Println(len(network.activeRequests))
		// m.logger.Println(index)
		network.activeRequests = slices.Delete(network.activeRequests, index, index+1)
		m.completeAggregationReq(network, req)
	}
}

// locked by caller
func (m *Monoceros) onAggregationResult(network *TreeOverlay, tree plumtree.TreeMetadata, result AggregationResult) {
	m.logger.Println("received aggregation result", result)
	received := time.Now().UnixNano()
	// todo: ??
	if m.latestMetricsTs[result.NetworkID] > result.Timestamp {
		m.logger.Println("local ts higher than received ts", m.latestMetricsTs, result.Timestamp)
		return
	}
	m.latestMetrics[result.NetworkID] = result.Aggregate
	m.latestMetricsTs[result.NetworkID] = result.Timestamp
	m.latestIM[result.NetworkID] = result.IMs
	m.exportResult(result.IMs, tree.NodeID(), result.Timestamp, received)
	if result.NetworkID == network.ID {
		// todo: ??
		network.rank = GetNodeRank(m.config.NodeID, result.RankList)
		// if network.rank > 1 && network.local != nil {
		// 	tree := *network.local
		// 	m.lock.Unlock()
		// 	err := network.plumtree.DestroyTree(tree)
		// 	if err != nil {
		// 		m.logger.Println(err)
		// 	}
		// 	m.lock.Lock()
		// }
		// todo: ovde isto moze da unisti lokalno stablo
		network.maxRank = int64(len(slices.Collect(maps.Keys(result.RankList))))
		// m.logger.Println("rank", network.rank)
		if network.ID == m.RRN.ID {
			if m.RN.local == nil {
				// m.logger.Println("local rn tree is nil?")
				return
			}
			resBytes, err := Serialize(result)
			if err != nil {
				m.logger.Println("err while serializing agg result", err)
				return
			}
			m.lock.Unlock()
			err = m.RN.plumtree.Broadcast(m.RN.local.Id, AGGREGATION_RESULT_MSG_TYPE, resBytes)
			// m.logger.Println("try lock")
			m.lock.Lock()
			if err != nil {
				m.logger.Println("err while sending agg result", err)
			}
		}
	}
}

// locked by caller
func (m *Monoceros) completeAggregationReq(network *TreeOverlay, req *ActiveAggregationReq) {
	// m.logger.Println("complete aggregation req", network, req.Tree)
	// m.logger.Println(network.local)
	// m.logger.Println(req.Tree.Id)
	if req.Sender != nil {
		// m.logger.Println("has parent")
		var resp any = nil
		var respType string = ""
		resp = AggregationResp{
			Timestamp: req.Timestamp,
			Aggregate: req.Aggregate,
			Cancel:    req.Cancel,
			Scores:    req.Scores,
		}
		respType = AGGREGATION_RESP_MSG_TYPE
		respBytes, err := Serialize(resp)
		if err != nil {
			m.logger.Println("error marshalling resp", err)
			return
		}
		m.lock.Unlock()
		err = network.plumtree.SendDirectMsg(req.Tree.Id, respType, respBytes, req.Sender)
		// m.logger.Println("try lock")
		m.lock.Lock()
		if err != nil {
			m.logger.Println("error sending resp", err)
		}
	} else if network.local != nil && req.Tree.Id == network.local.Id {
		// m.logger.Println("req done")
		if req.Cancel {
			// m.logger.Println("should destroy local tree")
			tree := *network.local
			m.lock.Unlock()
			// todo: nil pointer
			err := network.plumtree.DestroyTree(tree)
			// m.logger.Println("try lock")
			m.lock.Lock()
			if err != nil {
				m.logger.Println("error while destorying local tree", err)
			}
		} else {
			network.localAggCount += 1
			for _, im := range req.Aggregate {
				maps.Copy(im.Metadata.Labels, network.AdditionalMetricLabels)
			}
			om, err := imToOpenMetrics(req.Aggregate)
			if err != nil {
				// m.logger.Println(err)
			}
			result, err := Serialize(AggregationResult{
				NetworkID: network.ID,
				Timestamp: req.Timestamp,
				Aggregate: om,
				IMs:       req.Aggregate,
				RankList:  req.Scores,
			})
			if err != nil {
				m.logger.Println("error marshalling rank list", err)
			} else {
				// m.logger.Println("sending aggregation result", result)
				m.lock.Unlock()
				err = network.plumtree.Broadcast(req.Tree.Id, AGGREGATION_RESULT_MSG_TYPE, result)
				// m.logger.Println("try lock")
				m.lock.Lock()
				if err != nil {
					m.logger.Println("error sending aggregation result", err)
				} else {
					// m.logger.Println("sent aggregation result")
				}
			}
		}
	} else {
		// m.logger.Println("no conditions met")
	}
}

// locked
func (m *Monoceros) syncState() (bool, []byte) {
	// m.logger.Println("try lock")
	m.lock.Lock()
	defer m.lock.Unlock()
	// m.logger.Println("global network peer up")
	if m.synced {
		// m.logger.Println("already synced, no need to send sync req")
		return false, nil
	}
	return true, []byte{SYNC_REQ_MSG_TYPE}
}

// func (m *Monoceros) gossipRoot() {
// 	for range time.NewTicker(time.Duration(m.config.Aggregation.TAggSec) * time.Second).C {
// 		m.lock.Lock()
// 		if !m.RRN.joined {
// 			m.lock.Unlock()
// 			continue
// 		}
// 		gossip := RootInfo{
// 			NodeInfo: data.Node{
// 				ID:            m.config.NodeID,
// 				ListenAddress: m.RRN.plumtree.ListenAddress(),
// 			},
// 			Region: m.config.Region,
// 			Time:   time.Now().UnixNano(),
// 		}
// 		gossipBytes, err := json.Marshal(gossip)
// 		if err != nil {
// 			m.lock.Unlock()
// 			m.logger.Println(err)
// 			continue
// 		}
// 		gossipBytes = append([]byte{RRUPDATE_MSG_TYPE}, gossipBytes...)
// 		m.logger.Println("sending rrn update", gossipBytes)
// 		m.lock.Unlock()
// 		m.GN.Broadcast(gossipBytes)
// 	}
// }

// func (m *Monoceros) rejoinRRN() {
// 	for range time.NewTicker(time.Duration(m.config.Aggregation.TAggSec) * time.Second).C {
// 		m.lock.Lock()
// 		if !m.RRN.joined {
// 			m.lock.Unlock()
// 			continue
// 		}
// 		if m.RRN.plumtree.GetPeersNum() == 0 {
// 			m.contactRRNNode()
// 		} else {
// 			// NOTE: TMP ONLY !!!!!!!
// 			perRegion := map[string][]hyparview.Peer{}
// 			for _, p := range m.RRN.plumtree.GetPeers() {
// 				region := strings.Split(p.Node.ID, "_")[0]
// 				_, ok := perRegion[region]
// 				if !ok {
// 					perRegion[region] = make([]hyparview.Peer, 0)
// 				}
// 				perRegion[region] = append(perRegion[region], p)
// 			}
// 			for _, peers := range perRegion {
// 				if len(peers) < 2 {
// 					continue
// 				}
// 				maxId := peers[0].Node.ID
// 				for _, p := range peers {
// 					if p.Node.ID > maxId {
// 						maxId = p.Node.ID
// 					}
// 				}
// 				for _, p := range peers {
// 					if p.Node.ID != maxId {
// 						err := p.Conn.Disconnect()
// 						if err != nil {
// 							m.logger.Println(err)
// 						}
// 					}
// 				}
// 			}
// 		}
// 		m.lock.Unlock()
// 	}
// }

// // locked by caller
// func (m *Monoceros) contactRRNNode() {
// 	contactedPeer := false
// 	for _, root := range m.Roots {
// 		if root.Region == m.config.Region {
// 			// m.logger.Println("same region, skip")
// 			continue
// 		}
// 		err := m.RRN.plumtree.Join(root.NodeInfo.ID, root.NodeInfo.ListenAddress)
// 		if err != nil {
// 			m.logger.Println("error while joining rrn", err)
// 			continue
// 		}
// 		contactedPeer = true
// 		break
// 	}
// 	if !contactedPeer {
// 		err := m.RRN.plumtree.Join(m.config.NodeID, m.RRN.plumtree.ListenAddress())
// 		if err != nil {
// 			m.logger.Println("error while joining rrn", err)
// 		}
// 	}
// }

// // locked
// func (m *Monoceros) joinRRN(tree plumtree.TreeMetadata) {
// 	// m.logger.Println("try lock")
// 	m.lock.Lock()
// 	defer m.lock.Unlock()
// 	m.logger.Println("tree constructed in RN, should join RRN", tree)
// 	if tree.Id != fmt.Sprintf("%s_%s", m.RN.ID, m.config.NodeID) {
// 		m.logger.Println("should not")
// 		return
// 	}
// 	m.lock.Unlock()
// 	// todo: ??
// 	// promovisi samo ako je i dalje != nil i proslo bar n agregacija
// 	for range time.NewTicker(100 * time.Millisecond).C {
// 		m.lock.Lock()
// 		if m.RN.local == nil {
// 			// m.lock.Unlock()
// 			m.logger.Println("local rn tree destroyed in the meantime, should not join rrn")
// 			return
// 		}
// 		if m.RN.localAggCount > 0 {
// 			// m.lock.Unlock()
// 			break
// 		}
// 		m.lock.Unlock()
// 	}
// 	// time.Sleep(time.Duration(3*m.config.Aggregation.TAggSec) * time.Second)
// 	// m.logger.Println("try lock")
// 	// m.lock.Lock()
// 	// if m.RN.local == nil {
// 	// 	// m.logger.Println("local rn tree destroyed in the meantime, should not join rrn")
// 	// 	return
// 	// }
// 	m.logger.Println("still join rrn??")
// 	m.contactRRNNode()
// 	m.RRN.joined = true
// }

// // locked
// func (m *Monoceros) leaveRRN(tree plumtree.TreeMetadata) {
// 	// m.logger.Println("try lock")
// 	m.lock.Lock()
// 	defer m.lock.Unlock()
// 	m.logger.Println("tree destroyed in RN, should leave RRN", tree)
// 	if tree.Id != fmt.Sprintf("%s_%s", m.RN.ID, m.config.NodeID) {
// 		m.logger.Println("should not")
// 		return
// 	}
// 	if m.RRN.local != nil {
// 		tree := *m.RRN.local
// 		m.lock.Unlock()
// 		// todo: nil pinter
// 		err := m.RRN.plumtree.DestroyTree(tree)
// 		// m.logger.Println("try lock")
// 		m.lock.Lock()
// 		if err != nil {
// 			m.logger.Println(err)
// 		}
// 	}
// 	m.RRN.joined = false
// 	m.logger.Println("dosao do leave")
// 	m.lock.Unlock()
// 	m.RRN.plumtree.Leave()
// 	m.lock.Lock()
// 	m.logger.Println("prosao leave")
// 	// gossip := RRUpdate{
// 	// 	Joined: false,
// 	// 	NodeInfo: data.Node{
// 	// 		ID:            m.config.NodeID,
// 	// 		ListenAddress: m.RRN.plumtree.ListenAddress(),
// 	// 	},
// 	// 	Region: m.config.Region,
// 	// }
// 	// gossipBytes, err := json.Marshal(gossip)
// 	// if err != nil {
// 	// 	// m.logger.Println(err)
// 	// 	return
// 	// }
// 	// gossipBytes = append([]byte{RRUPDATE_MSG_TYPE}, gossipBytes...)
// 	// m.logger.Println("sending rrn update", gossipBytes)
// 	// m.lock.Unlock()
// 	// m.GN.Broadcast(gossipBytes)
// 	// // m.logger.Println("try lock")
// 	// m.lock.Lock()
// }

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

func (m *Monoceros) exportResult(ims []IntermediateMetric, tree string, reqTimestamp, rcvTimestamp int64) {
	for _, im := range ims {
		name := im.Metadata.Name + "{ "
		for _, k := range slices.Sorted(maps.Keys(im.Metadata.Labels)) {
			name += k + "=" + im.Metadata.Labels[k] + " "
		}
		name += "}"
		filename := fmt.Sprintf("/var/log/monoceros/results/%s.csv", name)
		file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			m.logger.Printf("failed to open/create file: %v", err)
			continue
		}
		defer file.Close()
		writer := csv.NewWriter(file)
		defer writer.Flush()
		reqTsStr := strconv.Itoa(int(reqTimestamp))
		rcvTsStr := strconv.Itoa(int(rcvTimestamp))
		valStr := strconv.FormatFloat(im.Result.ComputeFinal(), 'f', -1, 64)
		err = writer.Write([]string{tree, reqTsStr, rcvTsStr, valStr})
		if err != nil {
			m.logger.Println(err)
		}
	}
}

func (m *Monoceros) exportMsgCount() {
	filename := "/var/log/monoceros/results/msg_count.csv"
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		m.logger.Printf("failed to open/create file: %v", err)
		return
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	tsStr := strconv.Itoa(int(time.Now().UnixNano()))
	transport.MessagesSentLock.Lock()
	sent := transport.MessagesSent - transport.MessagesSentSub
	transport.MessagesSentLock.Unlock()
	transport.MessagesRcvdLock.Lock()
	rcvd := transport.MessagesRcvd - transport.MessagesRcvdSub
	transport.MessagesRcvdLock.Unlock()
	sentStr := strconv.Itoa(sent)
	rcvdStr := strconv.Itoa(rcvd)
	err = writer.Write([]string{tsStr, sentStr, rcvdStr})
	if err != nil {
		m.logger.Println(err)
	}
}
