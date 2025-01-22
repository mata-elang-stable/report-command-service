package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/alitto/pond/v2"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"
	"github.com/mata-elang-stable/report-command-service/internal/config"
	"github.com/mata-elang-stable/report-command-service/internal/pb"
	"github.com/mata-elang-stable/report-command-service/internal/types"
	"github.com/spf13/cobra"
)

func runApp(cmd *cobra.Command, args []string) {
	conf := config.GetConfig()
	conf.SetupLogging()

	// Handle shutdown signals for entire application
	mainContext, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	//TODO - Create a new Kafka Consumer instance
	log.Infof("Creating Kafka consumer with brokers: %s\n", conf.KafkaBrokers)
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  conf.KafkaBrokers,
		"group.id":           "mataelang-dc-addons-report",
		"session.timeout.ms": 6000,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
	})
	if err != nil {
		log.Fatalf("Error creating Kafka consumer: %v\n", err)
	}
	log.Infoln("Kafka consumer created")

	//TODO - Create a new Kafka Producer instance
	log.Infof("Creating Kafka producer with brokers: %s\n", conf.KafkaBrokers)
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":        conf.KafkaBrokers,
		"linger.ms":                100,
		"message.send.max.retries": 5,
		"retry.backoff.ms":         300,
		"socket.keepalive.enable":  true,
	})
	if err != nil {
		log.Fatalf("Error creating Kafka producer: %v\n", err)
	}
	log.Infoln("Kafka producer created")

	//TODO - Create a new Schema Registry client instance
	log.Infof("Creating Schema Registry client with URL: %s\n", conf.SchemaRegistryUrl)
	client, err := schemaregistry.NewClient(schemaregistry.NewConfig(conf.SchemaRegistryUrl))
	if err != nil {
		log.Fatalf("Error creating Schema Registry client: %v\n", err)
	}
	log.Infoln("Schema Registry client created")

	//TODO - Create a new ProtoBuf serializer and deserializer instance
	deserializer, err := protobuf.NewDeserializer(client, serde.ValueSerde, protobuf.NewDeserializerConfig())
	if err != nil {
		log.Fatalf("Error creating deserializer: %v\n", err)
	}

	// serializer, err := protobuf.NewSerializer(client, serde.ValueSerde, protobuf.NewSerializerConfig())
	// if err != nil {
	// 	log.Fatalf("Error creating serializer: %v\n", err)
	// }

	//TODO - Register ProtoBuf message type
	if err := deserializer.ProtoRegistry.RegisterMessage((&pb.SensorEvent{}).ProtoReflect().Type()); err != nil {
		log.Fatalf("Error registering ProtoBuf message type: %v\n", err)
	}

	//TODO - Subscribe to Kafka topic
	if err := consumer.Subscribe(conf.InputKafkaTopic, nil); err != nil {
		log.Fatalf("Error subscribing to Kafka topic: %v\n", err)
	}
	log.Infof("Subscribed to Kafka topic: %s\n", conf.InputKafkaTopic)

	maxItems := 10
	currentItems := 0

	//TODO - Create a main loop to consume messages from Kafka with signal handling
MAINLOOP:
	for {
		select {
		case <-mainContext.Done():
			log.Infoln("Received shutdown signal, closing consumer and producer")
			consumer.Close()
			producer.Flush(15 * 1000)
			producer.Close()
			break MAINLOOP
		default:
			ev := consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				if currentItems >= maxItems {
					log.Infoln("Reached maximum items, closing consumer and producer")
					cancel()
					continue MAINLOOP
				}

				//TODO - Deserialize the message
				value, err := deserializer.Deserialize(*e.TopicPartition.Topic, e.Value)
				if err != nil {
					log.Errorf("Error deserializing message: %v\n", err)
					continue
				}

				// log.Tracef("Received message: %v\n", value)

				payload := value.(*pb.SensorEvent)
				// payloadJson, err := json.MarshalIndent(payload, "", "  ")
				if err != nil {
					log.Errorf("Error marshalling message: %v\n", err)
					continue
				}

				// log.Tracef("Processing message: %s\n", payloadJson)

				// if _, err := consumer.CommitMessage(e); err != nil {
				// 	log.Errorf("Error committing message: %v\n", err)
				// }

				processedEvent := parseMetric(payload)
				resultJson, err := json.MarshalIndent(processedEvent, "", "  ")
				if err != nil {
					log.Errorf("Error marshalling processed event: %v\n", err)
					continue
				}

				if payload.EventMetricsCount > 50 {
					// log.Tracef("Processed: %s\n", payloadJson)
					log.Tracef("Processed: %s\n", resultJson)
					// filenameBefore := fmt.Sprintf("output/compare-%s-before.json", payload.EventHashSha256[:8])
					// filenameAfter := fmt.Sprintf("output/compare-%s-after.json", payload.EventHashSha256[:8])

					// // Write the output to file
					// writeToFile(filenameBefore, payloadJson)
					// writeToFile(filenameAfter, resultJson)
					// break MAINLOOP
					// currentItems++
				} else {
					log.Tracef("Processed count: %d hash: %s\n", payload.EventMetricsCount, payload.EventHashSha256[:8])
				}
			case kafka.Error:
				log.Errorf("Kafka error: %v\n", e)
			default:
				log.Warnf("Ignored message: %v\n", e)
			}
		}
	}

	//TODO - Run the main loop
}

func writeToFile(filename string, data []byte) {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Errorf("Error opening file: %v\n", err)
	}
	defer f.Close()

	if _, err := f.Write(data); err != nil {
		log.Errorf("Error writing to file: %v\n", err)
	}
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

func parseMetric(payload *pb.SensorEvent) *types.Event {
	// Parse priority to string
	priority := parsePriority(payload.SnortPriority)

	r := types.Event{
		SensorID:            payload.SensorId,
		SnortClassification: *payload.SnortClassification,
		SnortMessage:        payload.SnortMessage,
		SnortPriority:       priority,
		SnortProtocol:       payload.SnortProtocol,
		SnortSeconds:        payload.SnortSeconds,
		EventMetricsCount:   uint32(payload.EventMetricsCount),
		Metrics:             []*types.Metric{},
	}

	metricMap := sync.Map{}

	pool := pond.NewPool(5)

	for _, metric := range payload.Metrics {
		// log.Infof("Metric: %v\n", metric)

		pool.Submit(func() {
			m := types.Metric{
				Count:           0,
				SnortDstAddress: metric.SnortDstAddress,
				SnortSrcAddress: metric.SnortSrcAddress,
			}

			dataKey := fmt.Sprintf("%s;%s", *metric.SnortSrcAddress, *metric.SnortDstAddress)
			key := generateHashSHA256(dataKey)

			// Add port
			dstSrcPort := fmt.Sprintf("%d:%d", *metric.SnortDstPort, *metric.SnortSrcPort)

			d, _ := metricMap.LoadOrStore(key, &m)
			d.(*types.Metric).StoreOrIncrementDstSrcPort(dstSrcPort)
		})
	}

	pool.StopAndWait()

	// Convert the sync.Map to a slice of AggregatedMetric
	metricMap.Range(func(key, value interface{}) bool {
		r.Metrics = append(r.Metrics, value.(*types.Metric))
		return true
	})

	return &r
}

// generateHashSHA256 generates a SHA256 hash from the attributes.
// It is used to identify the sensor event record.
func generateHashSHA256(payload string) string {
	hash := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(hash[:])
}

// func main2() {
// 	//TODO: move to config package
// 	//SECTION - Application configuration
// 	brokers := "127.0.0.1:9093"
// 	groupID := "dc-sensor-alerts"
// 	topic := "report-aggregated"
// 	schemaRegistryURL := "http://127.0.0.1:8081"
// 	//!SECTION

// 	log.Println("Creating Kafka Consumer")
// 	consumer, err := kafka_consumer.NewKafkaConsumer(brokers, groupID, schemaRegistryURL)
// 	if err != nil {
// 		panic(err)
// 	}
// 	//FIXME - Close consumer

// 	producer, err := kafka_producer.NewKafkaProducer(brokers, schemaRegistryURL, topic)
// 	if err != nil {
// 		log.Fatalf("Error creating Kafka producer: %v\n", err)
// 	}
// 	defer producer.Close()

// 	ticker := time.NewTicker(1 * time.Second)
// 	defer ticker.Stop()

// 	//SECTION Handle shutdown signals
// 	sigChan := make(chan os.Signal, 1)
// 	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
// 	//!SECTION

// 	done := make(chan bool)

// 	//SECTION - Run the app process loop in a goroutine with shutdown signal handling
// 	go func() {
// 		for {
// 			select {
// 			case <-ticker.C:
// 				runIteration(consumer, producer)
// 			case sig := <-sigChan:
// 				log.Printf("Received signal %v, shutting down", sig)
// 				done <- true
// 				return
// 			}
// 		}
// 	}()
// 	//!SECTION

// 	// Wait for the done signal
// 	<-done
// 	log.Println("Shutdown complete")
// 	consumer.Close()
// }

// func runIteration(consumer *kafka_consumer.Consumer, producer *kafka_producer.Producer) {
// 	// Consume messages from the Kafka topic
// 	consumedMessages, err := kafka_consumer.RunConsumer(consumer)
// 	if err != nil {
// 		log.Fatalf("Error running consumer: %v\n", err)
// 	}

// 	// If no messages were consumed, log and return
// 	if len(consumedMessages) == 0 {
// 		log.Println("No messages consumed")
// 		return
// 	}

// 	// Prepare the output data
// 	outputData := reducer.CreateOutputData(consumedMessages)

// 	var events []*pb.Event

// 	//FIXME - Reduce the complexity of this loop
// 	for _, data := range outputData {
// 		var metrics []*pb.AggregatedMetric
// 		for _, metric := range data["metrics"].([]reducer.Metric) {
// 			metrics = append(metrics, &pb.AggregatedMetric{
// 				Count:           int32(metric["count"].(int)),
// 				SnortDstAddress: metric["snort_dst_address"].(string),
// 				SnortDstPort:    int32(metric["snort_dst_port"].(int64)),
// 				SnortSrcAddress: metric["snort_src_address"].(string),
// 				SnortSrcPort:    int32(metric["snort_src_port"].(int64)),
// 			})
// 		}

// 		//FIXME - Possibly caused the memory leak
// 		events = append(events, &pb.Event{
// 			EventMetricsCount:   int32(data["event_metrics_count"].(int64)),
// 			Metrics:             metrics,
// 			SensorId:            data["sensor_id"].(string),
// 			SnortClassification: *data["snort_classification"].(*string),
// 			SnortMessage:        data["snort_message"].(string),
// 			SnortPriority:       int32(data["snort_priority"].(int64)),
// 			SnortSeconds:        data["snort_seconds"].(int64),
// 		})
// 	}

// 	reportAggregated := &pb.ReportAggregated{
// 		Events: events,
// 	}

// 	// Produce the protobuf message
// 	err = producer.ProduceMessage(reportAggregated)
// 	if err != nil {
// 		log.Fatalf("Error producing message: %v\n", err)
// 	}

// 	fmt.Println("Produced message")
// }
