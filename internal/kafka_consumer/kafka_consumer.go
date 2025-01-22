package kafka_consumer

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde/protobuf"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/mata-elang-stable/report-command-service/internal/pb"
)

type Consumer struct {
	consumer     *kafka.Consumer
	deserializer *protobuf.Deserializer
}

func NewKafkaConsumer(brokers, groupID, schemaRegistryURL string) (*Consumer, error) {
	config := &kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"group.id":           groupID,
		"session.timeout.ms": 6000,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": true,
	}

	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
	}

	client, err := schemaregistry.NewClient(schemaregistry.NewConfig(schemaRegistryURL))
	if err != nil {
		return nil, fmt.Errorf("failed to create schema registry client: %w", err)
	}

	deserializer, err := protobuf.NewDeserializer(client, serde.ValueSerde, protobuf.NewDeserializerConfig())
	if err != nil {
		return nil, fmt.Errorf("failed to create deserializer: %w", err)
	}

	deserializer.ProtoRegistry.RegisterMessage((&pb.SensorEvent{}).ProtoReflect().Type())

	return &Consumer{
		consumer:     consumer,
		deserializer: deserializer,
	}, nil
}

func (c *Consumer) Subscribe(topics []string) error {
	return c.consumer.SubscribeTopics(topics, nil)
}

func (c *Consumer) PollMessages(ctx context.Context, msgChan chan<- *pb.SensorEvent) {
	run := true
	sigChan := shutdownSignals()

	for run {
		select {
		case sig := <-sigChan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		case <-ctx.Done():
			run = false
		default:
			ev := c.consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				c.ConsumeMessage(e, msgChan)
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}

	fmt.Printf("Closing consumer\n")
}

func (c *Consumer) ConsumeMessage(msg *kafka.Message, msgChan chan<- *pb.SensorEvent) {
	value, err := c.deserializer.Deserialize(*msg.TopicPartition.Topic, msg.Value)
	if err != nil {
		fmt.Printf("Failed to deserialize payload: %s\n", err)
	} else {
		msgChan <- value.(*pb.SensorEvent)
	}
	if msg.Headers != nil {
		fmt.Printf("%% Headers: %v\n", msg.Headers)
	}
}

func shutdownSignals() chan os.Signal {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	return sigs
}

func (c *Consumer) Close() {
	c.consumer.Close()
}

func RunConsumer(consumer *Consumer) ([]*pb.SensorEvent, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	msgChan := make(chan *pb.SensorEvent)
	errChan := make(chan error)
	var consumedMessages []*pb.SensorEvent

	go func() {
		<-sigChan
		log.Println("Received shutdown signal")
		cancel()
	}()

	go func() {
		for {
			select {
			case msg := <-msgChan:
				consumedMessages = append(consumedMessages, msg)
			case <-ctx.Done():
				return
			}
		}
	}()

	log.Println("Subscribing to topics")
	err := consumer.Subscribe([]string{"sensor_events"})
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to topics: %w", err)
	}
	log.Println("Successfully subscribed to topics")

	log.Println("Starting to poll messages")
	go func() {
		consumer.PollMessages(ctx, msgChan)
		errChan <- nil
	}()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if len(consumedMessages) > 0 {
				return consumedMessages, nil
			}
		case err := <-errChan:
			return nil, err
		case <-ctx.Done():
			return consumedMessages, nil
		}
	}
}
