package types

import "sync"

type Metric struct {
	Count           uint32
	SnortDstAddress *string
	SnortSrcAddress *string
	SnortDstSrcPort map[string]uint32
	mu              sync.RWMutex
}

func (m *Metric) StoreOrIncrementDstSrcPort(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.SnortDstSrcPort == nil {
		m.SnortDstSrcPort = make(map[string]uint32)
	}

	if _, ok := m.SnortDstSrcPort[key]; ok {
		m.SnortDstSrcPort[key]++
	} else {
		m.SnortDstSrcPort[key] = 1
	}

	m.Count++
}

type Event struct {
	EventMetricsCount   uint32
	SensorID            string
	SnortPriority       string
	SnortClassification string
	SnortMessage        string
	SnortProtocol       string
	SnortSeconds        int64
	Metrics             []*Metric
}
