package monoceros

import (
	"encoding/json"

	"github.com/c12s/hyparview/data"
)

const (
	AGGREGATION_REQ_MSG_TYPE    = "A_REQ"
	AGGREGATION_RESP_MSG_TYPE   = "A_RESP"
	ABORT_RESP_MSG_TYPE         = "ABORT_RESP"
	AGGREGATION_RESULT_MSG_TYPE = "A_RESULT"
)

type AggregationReq struct {
	Timestamp int64
}

type AggregationResp struct {
	Timestamp int64
	Aggregate []IntermediateMetric
	Scores    map[string]float64
}

type AbortResp struct {
	Timestamp int64
}

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
	RRUPDATE_MSG_TYPE  byte = 1
	SYNC_REQ_MSG_TYPE  byte = 2
	SYNC_RESP_MSG_TYPE byte = 3
)

type RRUpdate struct {
	Joined   bool
	NodeInfo data.Node
	Region   string
}

type SyncStateResp struct {
	RegionalRootAddresses map[string]string
	RegionalRootRegions   map[string]string
}
