package types

import "sync"

type MetricDraft struct {
	Count           uint32
	SnortDstAddress *string
	SnortSrcAddress *string
	SnortDstSrcPort sync.Map
}

type Metric struct {
	Count           uint32 `json:"count"`
	SnortDstAddress *string `json:"snort_dst_address"`
	SnortSrcAddress *string `json:"snort_src_address"`
	SnortDstSrcPort map[string]uint32 `json:"snort_dst_src_port"`
}

func (m *MetricDraft) StoreOrIncrementDstSrcPort(key string) {
	d, loaded := m.SnortDstSrcPort.LoadOrStore(key, 1)
	if loaded {
		m.SnortDstSrcPort.Store(key, d.(int)+1)
	}

	m.Count++
}

func (m *MetricDraft) ToMetric() Metric {
	metric := Metric{
		Count:           m.Count,
		SnortDstAddress: m.SnortDstAddress,
		SnortSrcAddress: m.SnortSrcAddress,
		SnortDstSrcPort: make(map[string]uint32),
	}

	m.SnortDstSrcPort.Range(func(key, value interface{}) bool {
		metric.SnortDstSrcPort[key.(string)] = uint32(value.(int))
		return true
	})

	return metric
}

type Event struct {
	KeyHash             string `json:"key_hash"`
	EventMetricsCount   uint32 `json:"event_metrics_count"`
	SensorID            string `json:"sensor_id"`
	SnortPriority       string `json:"snort_priority"`
	SnortClassification string `json:"snort_classification"`
	SnortMessage        string `json:"snort_message"`
	SnortProtocol       string `json:"snort_protocol"`
	SnortSeconds        int64 `json:"snort_seconds"`
	Metrics             []Metric `json:"metrics"`
}
