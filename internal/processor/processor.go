package processor

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/alitto/pond/v2"
	"github.com/mata-elang-stable/report-command-service/internal/config"
	"github.com/mata-elang-stable/report-command-service/internal/pb"
	"github.com/mata-elang-stable/report-command-service/internal/schema"
	"sync"
)

// generateHashSHA256 generates a SHA256 hash from the attributes.
// It is used to identify the sensor event record.
func generateHashSHA256(payload string) string {
	hash := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(hash[:])
}

func roundTime(t int64, roundSeconds int64) int64 {
	return (t / roundSeconds) * roundSeconds
}

func parsePriority(priority int64) string {
	switch priority {
	case 1:
		return "High"
	case 2:
		return "Medium"
	case 3:
		return "Low"
	default:
		return "Informational"
	}
}

func generateKeyHash(event *schema.Event) string {
	// Round down the time to the nearest hour
	roundSeconds := int64(3600)

	roundedTime := roundTime(event.SnortSeconds, roundSeconds)
	event.SnortSeconds = roundedTime

	keyData := fmt.Sprintf("%s;%s;%s;%s;%s;%d",
		event.SensorID,
		event.SnortPriority,
		event.SnortClassification,
		event.SnortMessage,
		event.SnortProtocol,
		roundedTime,
	)

	return generateHashSHA256(keyData)
}

func ParseMetric(payload *pb.SensorEvent) *schema.Event {
	// Parse priority to string
	priority := parsePriority(payload.SnortPriority)

	r := schema.Event{
		KeyHash:             "",
		SensorID:            payload.SensorId,
		SnortClassification: *payload.SnortClassification,
		SnortMessage:        payload.SnortMessage,
		SnortPriority:       priority,
		SnortProtocol:       payload.SnortProtocol,
		SnortSeconds:        payload.SnortSeconds,
		EventMetricsCount:   uint32(payload.EventMetricsCount),
		Metrics:             []schema.Metric{},
	}

	r.KeyHash = generateKeyHash(&r)

	metricMap := sync.Map{}

	concurrencyNumber := min(config.GetConfig().MaxConcurrent, int(payload.EventMetricsCount))

	pool := pond.NewPool(concurrencyNumber)

	for _, metric := range payload.Metrics {
		// log.Infof("Metric: %v\n", metric)

		pool.Submit(func() {
			m := schema.MetricDraft{
				Count:           0,
				SnortDstAddress: metric.SnortDstAddress,
				SnortSrcAddress: metric.SnortSrcAddress,
			}

			dataKey := fmt.Sprintf("%s;%s", *metric.SnortSrcAddress, *metric.SnortDstAddress)
			// key := generateHashSHA256(dataKey)
			key := dataKey

			// Add port
			dstSrcPort := fmt.Sprintf("%d:%d", *metric.SnortDstPort, *metric.SnortSrcPort)

			d, _ := metricMap.LoadOrStore(key, &m)
			d.(*schema.MetricDraft).StoreOrIncrementDstSrcPort(dstSrcPort)
		})
	}

	pool.StopAndWait()

	// Convert the sync.Map to a slice of AggregatedMetric
	metricMap.Range(func(key, value interface{}) bool {
		r.Metrics = append(r.Metrics, value.(*schema.MetricDraft).ToMetric())
		return true
	})

	return &r
}
