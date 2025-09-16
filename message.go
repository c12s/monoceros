package monoceros

import (
	"encoding/json"

	"github.com/c12s/hyparview/data"
)

const (
	AGGREGATION_REQ_MSG_TYPE  = "A_REQ"
	AGGREGATION_RESP_MSG_TYPE = "A_RESP"
	// ABORT_RESP_MSG_TYPE         = "ABORT_RESP"
	AGGREGATION_RESULT_MSG_TYPE = "A_RESULT"
)

type AggregationReq struct {
	Timestamp int64
}

type AggregationResp struct {
	Timestamp int64
	Aggregate []IntermediateMetric
	Scores    map[string]float64
	Cancel    bool
}

// type AbortResp struct {
// 	Timestamp int64
// }

type AggregationResult struct {
	NetworkID string
	Timestamp int64
	RankList  map[string]float64
	Aggregate string
	IMs       []IntermediateMetric
}

func Serialize(v any) ([]byte, error) {
	return json.Marshal(v)
}

const (
	RRUPDATE_MSG_TYPE     byte = 1
	SYNC_REQ_MSG_TYPE     byte = 2
	SYNC_RESP_MSG_TYPE    byte = 3
	RULE_ADDED_MSG_TYPE   byte = 4
	RULE_REMOVED_MSG_TYPE byte = 5
)

type RootInfo struct {
	NodeInfo data.Node
	Region   string
	Time     int64
	Joined   bool
}

type SyncStateResp struct {
	RegionalRoots map[string][]RootInfo
	Rules         []AggregationRule
}

type RuleAdded struct {
	Rule AggregationRule
}

type RuleRemoved struct {
	RuleID string
}
