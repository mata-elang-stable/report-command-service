package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gitlab.com/mataelang/report-command-service/internal/kafka_consumer"
	"gitlab.com/mataelang/report-command-service/internal/kafka_producer"
	"gitlab.com/mataelang/report-command-service/internal/pb"
	"gitlab.com/mataelang/report-command-service/internal/reducer"
)

func main() {
	brokers := "127.0.0.1:9093"
	groupID := "dc-sensor-alerts"
	topic := "report-aggregated"
	schemaRegistryURL := "http://127.0.0.1:8081"

	log.Println("Creating Kafka Consumer")
	consumer, err := kafka_consumer.NewKafkaConsumer(brokers, groupID, schemaRegistryURL)
	if err != nil {
		panic(err)
	}

	producer, err := kafka_producer.NewKafkaProducer(brokers, schemaRegistryURL, topic)
	if err != nil {
		log.Fatalf("Error creating Kafka producer: %v\n", err)
	}
	defer producer.Close()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan bool)

	go func() {
		for {
			select {
			case <-ticker.C:
				runIteration(consumer, producer)
			case sig := <-sigChan:
				log.Printf("Received signal %v, shutting down", sig)
				done <- true
				return
			}
		}
	}()

	<-done
	log.Println("Shutdown complete")
	consumer.Close()
}

func runIteration(consumer *kafka_consumer.Consumer, producer *kafka_producer.Producer) {
	consumedMessages, err := kafka_consumer.RunConsumer(consumer)
	if err != nil {
		log.Fatalf("Error running consumer: %v\n", err)
	}

	if len(consumedMessages) == 0 {
		log.Println("No messages consumed")
		return
	}

	outputData := reducer.CreateOutputData(consumedMessages)

	var events []*pb.Event
	for _, data := range outputData {
		var metrics []*pb.AggregatedMetric
		for _, metric := range data["metrics"].([]reducer.Metric) {
			metrics = append(metrics, &pb.AggregatedMetric{
				Count:           int32(metric["count"].(int)),
				SnortDstAddress: metric["snort_dst_address"].(string),
				SnortDstPort:    int32(metric["snort_dst_port"].(int64)),
				SnortSrcAddress: metric["snort_src_address"].(string),
				SnortSrcPort:    int32(metric["snort_src_port"].(int64)),
			})
		}
		events = append(events, &pb.Event{
			EventMetricsCount:   int32(data["event_metrics_count"].(int64)),
			Metrics:             metrics,
			SensorId:            data["sensor_id"].(string),
			SnortClassification: *data["snort_classification"].(*string),
			SnortMessage:        data["snort_message"].(string),
			SnortPriority:       int32(data["snort_priority"].(int64)),
			SnortSeconds:        data["snort_seconds"].(int64),
		})
	}

	reportAggregated := &pb.ReportAggregated{
		Events: events,
	}

	// Produce the protobuf message
	err = producer.ProduceMessage(reportAggregated)
	if err != nil {
		log.Fatalf("Error producing message: %v\n", err)
	}

	fmt.Println("Produced message")
}
