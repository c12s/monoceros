package monoceros

import "encoding/json"

const (
	AGGREGATION_REQ_MSG_TYPE  = "A_REQ"
	AGGREGATION_RESP_MSG_TYPE = "A_RESP"
	ABORT_RESP_MSG_TYPE       = "ABORT_RESP"
	RANK_LIST_MSG_TYPE        = "RL"
)

type AggregationReq struct {
	Timestamp int64
}

type AggregationResp struct {
	Timestamp int64
	Aggregate int64
	Scores    map[string]float64
}

type AbortResp struct {
	Timestamp int64
}

type RankList struct {
	List map[string]float64
}

func Serialize(v any) ([]byte, error) {
	return json.Marshal(v)
}
