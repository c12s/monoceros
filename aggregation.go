package monoceros

import (
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"math"
	"net/http"

	dto "github.com/prometheus/client_model/go"
)

type AggregationFunc byte

const (
	MIN_FUNC AggregationFunc = 1
	MAX_FUNC AggregationFunc = 2
	SUM_FUNC AggregationFunc = 3
	AVG_FUNC AggregationFunc = 4
)

type IntermediateResult interface {
	Aggregate(with IntermediateResult) (IntermediateResult, error)
	ComputeFinal() float64
}

// IR = intermediate result

type MinIR struct {
	Value float64
}

func (ir MinIR) Aggregate(with IntermediateResult) (IntermediateResult, error) {
	otherIR, ok := with.(MinIR)
	if !ok {
		return nil, fmt.Errorf("other intermediary not a %T, it is %T", ir, with)
	}
	ir.Value = math.Min(ir.Value, otherIR.Value)
	return ir, nil
}

func (ir MinIR) ComputeFinal() float64 {
	return ir.Value
}

type MaxIR struct {
	Value float64
}

func (ir MaxIR) Aggregate(with IntermediateResult) (IntermediateResult, error) {
	otherIR, ok := with.(MaxIR)
	if !ok {
		return nil, fmt.Errorf("other intermediary not a %T, it is %T", ir, with)
	}
	ir.Value = math.Max(ir.Value, otherIR.Value)
	return ir, nil
}

func (ir MaxIR) ComputeFinal() float64 {
	return ir.Value
}

type SumIR struct {
	Value float64
}

func (ir SumIR) Aggregate(with IntermediateResult) (IntermediateResult, error) {
	otherIR, ok := with.(SumIR)
	if !ok {
		return nil, fmt.Errorf("other intermediary not a %T, it is %T", ir, with)
	}
	ir.Value = ir.Value + otherIR.Value
	return ir, nil
}

func (ir SumIR) ComputeFinal() float64 {
	return ir.Value
}

type AvgIR struct {
	Sum   float64
	Count int64
}

func (ir AvgIR) Aggregate(with IntermediateResult) (IntermediateResult, error) {
	otherIR, ok := with.(AvgIR)
	if !ok {
		return nil, fmt.Errorf("other intermediary not a %T, it is %T", ir, with)
	}
	ir.Sum = ir.Sum + otherIR.Sum
	ir.Count = ir.Count + otherIR.Count
	return ir, nil
}

func (ir AvgIR) ComputeFinal() float64 {
	return ir.Sum / float64(ir.Count)
}

var MakeIR = map[AggregationFunc]func(value float64) IntermediateResult{
	MIN_FUNC: func(value float64) IntermediateResult { return MinIR{Value: value} },
	MAX_FUNC: func(value float64) IntermediateResult { return MaxIR{Value: value} },
	SUM_FUNC: func(value float64) IntermediateResult { return SumIR{Value: value} },
	AVG_FUNC: func(value float64) IntermediateResult { return AvgIR{Sum: value, Count: 1} },
}

type IntermediateMetric struct {
	Metadata MetricMetadata
	Result   IntermediateResult
}

func (im IntermediateMetric) MarshalJSON() ([]byte, error) {
	var result any

	switch v := im.Result.(type) {
	case MinIR:
		result = struct {
			Func  AggregationFunc `json:"func"`
			Value float64         `json:"value"`
		}{MIN_FUNC, v.Value}

	case MaxIR:
		result = struct {
			Func  AggregationFunc `json:"func"`
			Value float64         `json:"value"`
		}{MAX_FUNC, v.Value}

	case SumIR:
		result = struct {
			Func  AggregationFunc `json:"func"`
			Value float64         `json:"value"`
		}{SUM_FUNC, v.Value}

	case AvgIR:
		result = struct {
			Func  AggregationFunc `json:"func"`
			Sum   float64         `json:"sum"`
			Count int64           `json:"count"`
		}{AVG_FUNC, v.Sum, v.Count}

	default:
		return nil, fmt.Errorf("marshal: unsupported Result type %T", v)
	}

	wire := struct {
		Metadata MetricMetadata `json:"metadata"`
		Result   any            `json:"result"`
	}{
		Metadata: im.Metadata,
		Result:   result,
	}
	return json.Marshal(wire)
}

func (im *IntermediateMetric) UnmarshalJSON(data []byte) error {
	var aux struct {
		Metadata MetricMetadata  `json:"metadata"`
		Result   json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	var head struct {
		Func AggregationFunc `json:"func"`
	}
	if err := json.Unmarshal(aux.Result, &head); err != nil {
		return err
	}

	switch head.Func {
	case MIN_FUNC:
		var v struct {
			Value float64 `json:"value"`
		}
		if err := json.Unmarshal(aux.Result, &v); err != nil {
			return err
		}
		im.Result = MinIR{Value: v.Value}

	case MAX_FUNC:
		var v struct {
			Value float64 `json:"value"`
		}
		if err := json.Unmarshal(aux.Result, &v); err != nil {
			return err
		}
		im.Result = MaxIR{Value: v.Value}

	case SUM_FUNC:
		var v struct {
			Value float64 `json:"value"`
		}
		if err := json.Unmarshal(aux.Result, &v); err != nil {
			return err
		}
		im.Result = SumIR{Value: v.Value}

	case AVG_FUNC:
		var v struct {
			Sum   float64 `json:"sum"`
			Count int64   `json:"count"`
		}
		if err := json.Unmarshal(aux.Result, &v); err != nil {
			return err
		}
		im.Result = AvgIR{Sum: v.Sum, Count: v.Count}

	default:
		return fmt.Errorf("unmarshal: unknown func %d", head.Func)
	}

	im.Metadata = aux.Metadata
	return nil
}

type AggregationRule struct {
	ID            string
	InputSelector MetricMetadata
	Func          AggregationFunc
	Output        MetricMetadata
}

func (m *Monoceros) getLatestForNode() []IntermediateMetric {
	local := make([]IntermediateMetric, 0)
	metrics := m.fetchNodeMetrics()
	metrics = filterByTypes(metrics, []*dto.MetricType{dto.MetricType_GAUGE.Enum()})
	// m.logger.Println("get node metrics")
	for _, rule := range m.rules {
		// m.logger.Println(rule)
		input := selectRawMetricsValues(rule.InputSelector, metrics)
		// m.logger.Println(input)
		inputIM := rawMetricsToIM(input, rule)
		im := aggregate(inputIM)
		// m.logger.Println(im)
		if im == nil {
			continue
		}
		local = append(local, *im)
		// m.logger.Println("local", local)
	}
	return local
}

func (m *Monoceros) getLatestForRegion() []IntermediateMetric {
	local := make([]IntermediateMetric, 0)
	metrics := m.latestIM[m.RN.ID]
	for _, rule := range m.rules {
		input := selectIMValues(rule.InputSelector, metrics)
		im := aggregate(input)
		if im == nil {
			continue
		}
		im.Metadata.Name = rule.Output.Name
		im.Metadata.Labels = maps.Clone(rule.Output.Labels)
		local = append(local, *im)
	}
	return local
}

func combineAggregates(first, second []IntermediateMetric, rules []AggregationRule) []IntermediateMetric {
	aggregated := make([]IntermediateMetric, 0)
	combined := append(first, second...)
	for _, rule := range rules {
		input := selectIMValues(rule.Output, combined)
		im := aggregate(input)
		if im == nil {
			continue
		}
		aggregated = append(aggregated, *im)
	}
	return aggregated
}

func aggregate(input []IntermediateMetric) *IntermediateMetric {
	if len(input) == 0 {
		return nil
	}
	ir := input[0]
	var err error = nil
	for _, otherIR := range input[1:] {
		ir.Result, err = ir.Result.Aggregate(otherIR.Result)
		if err != nil {
			log.Println(err)
			continue
		}
	}
	return &ir
}

func (m *Monoceros) AddRulesHandler(w http.ResponseWriter, r *http.Request) {
	// m.logger.Println("POST /rules request")
	rule := AggregationRule{}
	err := json.NewDecoder(r.Body).Decode(&rule)
	if err != nil {
		// m.logger.Println(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// tmp fix
	if region, ok := rule.InputSelector.Labels["regionID"]; ok {
		rule.Output.Labels["regionID"] = region
	}
	if level, ok := rule.InputSelector.Labels["level"]; ok {
		if level == "region" {
			rule.Output.Labels["level"] = "global"
		}
	}
	gossip := RuleAdded{Rule: rule}
	gossipBytes, err := json.Marshal(gossip)
	if err != nil {
		// m.logger.Println(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	gossipBytes = append([]byte{RULE_ADDED_MSG_TYPE}, gossipBytes...)
	// m.logger.Println("sending rule added", gossipBytes)
	m.GN.Broadcast(gossipBytes)
	w.WriteHeader(http.StatusOK)
}

func (m *Monoceros) RemoveRulesHandler(w http.ResponseWriter, r *http.Request) {
	// m.logger.Println("DELETE /rules request")
	id := r.PathValue("id")
	gossip := RuleRemoved{RuleID: id}
	gossipBytes, err := json.Marshal(gossip)
	if err != nil {
		// m.logger.Println(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	gossipBytes = append([]byte{RULE_REMOVED_MSG_TYPE}, gossipBytes...)
	// m.logger.Println("sending rule removed", gossipBytes)
	m.GN.Broadcast(gossipBytes)
	w.WriteHeader(http.StatusOK)
}
