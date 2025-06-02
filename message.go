package monoceros

import "encoding/json"

type AggregationReq struct {
	Timestamp int64
}

func (r AggregationReq) Serialize() ([]byte, error) {
	return json.Marshal(r)
}

type AggregationResp struct {
	Timestamp int64
	Aggregate int64
	Scores    map[int64]float64
}

func (r AggregationResp) Serialize() ([]byte, error) {
	return json.Marshal(r)
}

type RankList struct {
	List map[int64]float64
}

func (r RankList) Serialize() ([]byte, error) {
	return json.Marshal(r)
}
