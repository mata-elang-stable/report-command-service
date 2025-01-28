package main

import (
	"context"
	"github.com/mata-elang-stable/report-command-service/internal/app"
	"github.com/mata-elang-stable/report-command-service/internal/kafka"
	"github.com/mata-elang-stable/report-command-service/internal/reporter"
	"github.com/mata-elang-stable/report-command-service/internal/schema"
	"os"
	"os/signal"
	"syscall"

	"github.com/mata-elang-stable/report-command-service/internal/config"
	"github.com/spf13/cobra"
)

func runApp(cmd *cobra.Command, args []string) {
	conf := config.GetConfig()
	conf.SetupLogging()

	// Handle shutdown signals for entire application
	mainContext, cancel := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer cancel()

	kafkaConsumer := kafka.MustNewConsumer(conf.KafkaBrokers, conf.InputKafkaTopic)
	defer kafkaConsumer.Close()

	deserializer := schema.MustNewDeserializer(conf.SchemaRegistryUrl)
	eventReporter := reporter.NewHTTPReporter(conf)

	mainApp := app.NewApp(
		mainContext,
		kafkaConsumer,
		deserializer,
		eventReporter,
		conf.InputKafkaTopic,
	)

	if err := mainApp.Run(); err != nil {
		log.Fatalf("Failed to run app: %v", err)
	}
}
