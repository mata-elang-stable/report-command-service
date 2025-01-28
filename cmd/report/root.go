package main

import (
	"fmt"
	"os"

	"github.com/mata-elang-stable/report-command-service/internal/config"
	"github.com/mata-elang-stable/report-command-service/internal/logger"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	appVersion = "dev"
	appCommit  = "none"
	appLicense = "MIT"
)

var log = logger.GetLogger()

var rootCmd = &cobra.Command{
	Use:   "mea-report",
	Short: "Mata Elang Addon Report Command Service",
	Long: `Mata Elang Addon Report Command Service is a service that listens to incoming messages from Kafka,
processes them, and sends the result to another Kafka topic.`,
	Run:  runApp,
	Args: cobra.NoArgs,
}

func init() {
	// Read configuration from .env file in the current directory
	// viper.SetConfigFile("./.env")
	viper.SetConfigName(".env")
	viper.SetConfigType("env")
	viper.AddConfigPath(".")

	err := viper.ReadInConfig()
	if err != nil {
		log.WithField("error", err).Warnln("Failed to read configuration file")
	}

	viper.SetEnvPrefix("app")
	viper.AutomaticEnv()

	conf := config.GetConfig()

	viper.SetDefault("kafka_brokers", "localhost:9092")
	viper.SetDefault("kafka_topic_input", "sensor_events")
	viper.SetDefault("schema_registry_url", "http://localhost:8081")
	viper.SetDefault("report_api_url", "http://localhost:8000")
	viper.SetDefault("report_post_event_path", "/events")
	viper.SetDefault("http_timeout_seconds", 5)
	viper.SetDefault("http_max_retries", 3)
	viper.SetDefault("max_concurrent", 100)

	if err := viper.Unmarshal(&conf); err != nil {
		log.Fatalf("Failed to unmarshal configuration: %v", err)
	}

	flags := rootCmd.PersistentFlags()

	flags.StringVar(&conf.KafkaBrokers, "kafka-brokers", conf.KafkaBrokers, "")
	flags.StringVar(&conf.InputKafkaTopic, "input-topic", conf.InputKafkaTopic, "")
	flags.StringVar(&conf.SchemaRegistryUrl, "schema-registry-url", conf.SchemaRegistryUrl, "")
	flags.StringVar(&conf.ReportApiUrl, "url", conf.ReportApiUrl, "Mata Elang ReportApi Base URL")
	flags.CountVarP(&conf.VerboseCount, "verbose", "v", "Increase verbosity of the output.")

	if err := viper.BindPFlags(flags); err != nil {
		log.WithField("error", err).Fatalln("Failed to bind flags.")
	}
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
