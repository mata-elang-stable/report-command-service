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

	outputData := reducer.CreateOutputData(consumedMessage, countedMetrics)

	// Next steps: Hit the API with the outputData

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
