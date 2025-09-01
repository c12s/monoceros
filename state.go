package monoceros

import (
	"encoding/json"
	"maps"
	"net/http"
	"slices"

	"github.com/c12s/plumtree"
)

type State struct {
	ID                   string
	GlobalNetwork        any
	RegionalNetwork      any
	RegionalRootsNetwork any
	Synced               bool
	Roots                map[string]RootInfo
	LatestMetrics        map[string]string
	LatestMetricsTs      map[string]int64
}

type TreeOverlayState struct {
	Plumtree            any
	Joined              bool
	Local               *plumtree.TreeMetadata
	LastAggregationTime int64
	ActiveRequests      []*ActiveAggregationReq
	Scores              map[string]map[string]int
}

func (m *Monoceros) GetState() any {
	// m.lock.Lock()
	// defer m.lock.Unlock()
	s := State{
		ID:            m.config.NodeID,
		GlobalNetwork: m.GN.membership.GetState(),
		RegionalNetwork: TreeOverlayState{
			Plumtree:            m.RN.plumtree.GetState(),
			Joined:              m.RN.joined,
			Local:               m.RN.local,
			LastAggregationTime: m.RN.lastAggregationTime,
			ActiveRequests:      slices.Clone(m.RN.activeRequests),
			Scores:              maps.Clone(m.RN.knownScores),
		},
		RegionalRootsNetwork: TreeOverlayState{
			Plumtree:            m.RRN.plumtree.GetState(),
			Joined:              m.RRN.joined,
			Local:               m.RRN.local,
			LastAggregationTime: m.RRN.lastAggregationTime,
			ActiveRequests:      slices.Clone(m.RRN.activeRequests),
			Scores:              maps.Clone(m.RRN.knownScores),
		},
		Synced:          m.synced,
		Roots:           maps.Clone(m.Roots),
		LatestMetrics:   maps.Clone(m.latestMetrics),
		LatestMetricsTs: maps.Clone(m.latestMetricsTs),
	}
	return s
}

func (m *Monoceros) StateHandler(w http.ResponseWriter, _ *http.Request) {
	// m.logger.Println("/state request")
	state, err := json.Marshal(m.GetState())
	if err != nil {
		// m.logger.Println(err)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(state)
}
