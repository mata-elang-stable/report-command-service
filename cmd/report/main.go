package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"gitlab.com/mataelang/report-command-service/internal/kafka_consumer"
	"gitlab.com/mataelang/report-command-service/internal/pb"
	"gitlab.com/mataelang/report-command-service/internal/reducer"
)

func main() {
	brokers := "127.0.0.1:9093"
	groupID := "dc-sensor-alerts"
	schemaRegistryURL := "http://localhost:8081"
	log.Println("Creating Kafka Consumer")
	consumer, err := kafka_consumer.NewKafkaConsumer(brokers, groupID, schemaRegistryURL)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	msgChan := make(chan *pb.SensorEvent)
	go func() {
		for msg := range msgChan {
			log.Printf("Consumed message: %v\n", msg)
		}
	}()

	consumedMessage, err := kafka_consumer.RunConsumer(consumer)
	if err != nil {
		log.Fatalf("Error running consumer: %v\n", err)
	}

	var cleansedMetrics []reducer.Metric
	for _, metric := range consumedMessage.Metrics {
		cleansedMetric := reducer.CleanseMetric(metric)
		cleansedMetrics = append(cleansedMetrics, cleansedMetric)
	}

	countedMetrics := reducer.CountMetrics(cleansedMetrics)

	outputData := map[string]interface{}{
		"metrics":               countedMetrics,
		"event_hash_sha256":     consumedMessage.EventHashSha256,
		"event_metrics_count":   consumedMessage.EventMetricsCount,
		"event_read_at":         consumedMessage.EventReadAt,
		"event_received_at":     consumedMessage.EventReceivedAt,
		"event_seconds":         consumedMessage.EventSeconds,
		"event_sent_at":         consumedMessage.EventSentAt,
		"sensor_id":             consumedMessage.SensorId,
		"snort_action":          consumedMessage.SnortAction,
		"snort_classification":  consumedMessage.SnortClassification,
		"snort_direction":       consumedMessage.SnortDirection,
		"snort_interface":       consumedMessage.SnortInterface,
		"snort_message":         consumedMessage.SnortMessage,
		"snort_priority":        consumedMessage.SnortPriority,
		"snort_protocol":        consumedMessage.SnortProtocol,
		"snort_rule":            consumedMessage.SnortRule,
		"snort_rule_gid":        consumedMessage.SnortRuleGid,
		"snort_rule_rev":        consumedMessage.SnortRuleRev,
		"snort_rule_sid":        consumedMessage.SnortRuleSid,
		"snort_seconds":         consumedMessage.SnortSeconds,
		"snort_service":         consumedMessage.SnortService,
		"snort_type_of_service": consumedMessage.SnortTypeOfService,
	}

	outputFilePath := "output_v2.json"
	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		log.Fatalf("Error creating output file: %v\n", err)
	}
	defer outputFile.Close()

	outputJSON, err := json.MarshalIndent(outputData, "", "    ")
	if err != nil {
		log.Fatalf("Error marshalling output data to JSON: %v\n", err)
	}

	_, err = outputFile.Write(outputJSON)
	if err != nil {
		log.Fatalf("Error writing to output file: %v\n", err)
	}

	fmt.Println("Output written to", outputFilePath)

}
