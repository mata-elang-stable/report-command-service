package kafka_producer

import (
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde/protobuf"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"google.golang.org/protobuf/proto"
)

type Producer struct {
	p          *kafka.Producer
	serializer *protobuf.Serializer
	topic      string
}

func (p *Producer) Close() {
	p.p.Close()
}

func NewKafkaProducer(brokers, schemaRegistryURL, topic string) (*Producer, error) {
	config := &kafka.ConfigMap{
		"bootstrap.servers":        brokers,
		"acks":                     "all",
		"socket.keepalive.enable":  true,
		"retry.backoff.ms":         100,
		"enable.idempotence":       true,
		"message.send.max.retries": 10,
	}
	p, err := kafka.NewProducer(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "Error: %v\n", ev)
			default:
				fmt.Printf("Ignored event: %s\n", ev)
			}
		}
	}()

	client, err := schemaregistry.NewClient(schemaregistry.NewConfig(schemaRegistryURL))
	if err != nil {
		return nil, fmt.Errorf("failed to create schema registry client: %w", err)
	}

	serializer, err := protobuf.NewSerializer(client, serde.ValueSerde, protobuf.NewSerializerConfig())
	if err != nil {
		return nil, fmt.Errorf("failed to create serializer: %w", err)
	}

	fmt.Println("Created Kafka Producer")

	return &Producer{
		p:          p,
		serializer: serializer,
		topic:      topic,
	}, nil
}

func (p *Producer) ProduceMessage(message proto.Message) error {
	value, err := p.serializer.Serialize(p.topic, message)
	if err != nil {
		return fmt.Errorf("failed to serialize message: %w", err)
	}

	kafkaMessage := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
		Value:          value,
	}

	err = p.p.Produce(kafkaMessage, nil)
	if err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}

	p.p.Flush(15 * 1000)
	return nil
}
