package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/mata-elang-stable/report-command-service/internal/logger"
)

var log = logger.GetLogger()

func MustNewConsumer(servers string, topic string) *kafka.Consumer {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  servers,
		"group.id":           "mataelang-dc-report",
		"session.timeout.ms": 6000,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
	})

	if err != nil {
		log.Fatalf("Failed to create consumer: %v\n", err)
	}

	if err := consumer.Subscribe(topic, nil); err != nil {
		log.Fatalf("Failed to subscribe to topic: %v\n", err)
	}

	return consumer
}
