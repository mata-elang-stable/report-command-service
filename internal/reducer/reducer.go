package reducer

import (
	"encoding/json"

	"gitlab.com/mataelang/report-command-service/internal/pb"
)

type Metric map[string]interface{}

func CleanseMetric(metric *pb.Metric) Metric {
	cleansedMetric := Metric{
		"snort_src_address": metric.SnortSrcAddress,
		"snort_dst_address": metric.SnortDstAddress,
		"snort_eth_src":     metric.SnortEthSrc,
		"snort_eth_dst":     metric.SnortEthDst,
	}
	return cleansedMetric
}

func CountMetrics(metrics []Metric) []Metric {
	counts := make(map[string]int)
	cleansedMetrics := make(map[string]Metric)

	for _, metric := range metrics {
		metricJSON, _ := json.Marshal(metric)
		metricStr := string(metricJSON)
		counts[metricStr]++
		cleansedMetrics[metricStr] = metric
	}

	var result []Metric
	for metricStr, count := range counts {
		metric := cleansedMetrics[metricStr]
		metric["count"] = count
		result = append(result, metric)
	}

	return result
}
