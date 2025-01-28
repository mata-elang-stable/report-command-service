package app

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"
	"github.com/mata-elang-stable/report-command-service/internal/pb"
	"github.com/mata-elang-stable/report-command-service/internal/processor"
	"github.com/mata-elang-stable/report-command-service/internal/reporter"
)

type App struct {
	ctx          context.Context
	consumer     *kafka.Consumer
	deserializer *protobuf.Deserializer
	reporter     *reporter.HTTPReporter
	kafkaTopic   string
}

func NewApp(
	ctx context.Context,
	consumer *kafka.Consumer,
	deserializer *protobuf.Deserializer,
	reporter *reporter.HTTPReporter,
	topic string,
) *App {
	return &App{
		ctx:          ctx,
		consumer:     consumer,
		deserializer: deserializer,
		reporter:     reporter,
		kafkaTopic:   topic,
	}
}

func (a *App) Run() error {
	for {
		select {
		case <-a.ctx.Done():
			return a.shutdown()
		default:
			ev := a.consumer.Poll(100)
			if ev == nil {
				continue
			}

			if err := a.handleEvent(ev); err != nil {
				return err
			}
		}
	}
}

func (a *App) handleEvent(ev kafka.Event) error {
	switch e := ev.(type) {
	case *kafka.Message:
		return a.processMessage(e)
	case *kafka.Error:
		return e
	default:
		return nil
	}
}

func (a *App) processMessage(msg *kafka.Message) error {
	value, err := a.deserializer.Deserialize(a.kafkaTopic, msg.Value)
	if err != nil {
		return err
	}

	payload := value.(*pb.SensorEvent)
	processedEvent := processor.ParseMetric(payload)

	if err := a.reporter.PostEvent(a.ctx, processedEvent); err != nil {
		return err
	}

	if _, err := a.consumer.CommitMessage(msg); err != nil {
		return err
	}

	return nil
}

func (a *App) shutdown() error {
	return a.consumer.Close()
}
