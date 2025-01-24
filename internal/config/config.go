package config

import (
	"sync"

	"github.com/mata-elang-stable/report-command-service/internal/logger"
)

type Config struct {
	// SchemaRegistryUrl is the schema registry URL.
	SchemaRegistryUrl string `mapstructure:"schema_registry_url"`

	// KafkaBrokers is the Kafka broker to connect to.
	KafkaBrokers string `mapstructure:"kafka_brokers"`

	// InputKafkaTopic is the Kafka topic.
	InputKafkaTopic string `mapstructure:"kafka_topic_input"`

	// OutputKafkaTopic is the Kafka topic.
	OutputKafkaTopic string `mapstructure:"kafka_topic_output"`

	// VerboseCount is the verbose level.
	VerboseCount int `mapstructure:"verbose"`

	// MaxConcurrent is the maximum number of concurrent requests.
	MaxConcurrent int `mapstructure:"max_concurrent"`

	// RepoApiUrl is the repository API URL.
	RepoApiUrl string `mapstructure:"repo_api_url"`
}

var log = logger.GetLogger()

var instance *Config
var once sync.Once

func GetConfig() *Config {
	once.Do(func() {
		instance = &Config{}
	})

	return instance
}

func (c *Config) SetupLogging() {
	switch instance.VerboseCount {
	case 0:
		log.SetLevel(logger.InfoLevel)
	case 1:
		log.SetLevel(logger.DebugLevel)
	default:
		log.SetLevel(logger.TraceLevel)
	}
	log.WithFields(logger.Fields{
		"LOG_LEVEL": log.GetLevel().String(),
	}).Infoln("Logging level set.")
}
