package logger

import (
	"os"
	"sync"

	"github.com/sirupsen/logrus"
)

// Fields type, a map to store the entry's fields
type Fields = logrus.Fields

// Level type
type Level = logrus.Level

// Log levels
const (
	// PanicLevel level, highest level of severity. Logs and then calls panic with the message passed to Debug, Info, ...
	PanicLevel Level = logrus.PanicLevel

	// FatalLevel level. Logs and then calls `logger.Exit(1)`. It will exit even if the logging level is set to Panic.
	FatalLevel Level = logrus.FatalLevel

	// ErrorLevel level. Logs. Used for errors that should definitely be noted.
	ErrorLevel Level = logrus.ErrorLevel

	// WarnLevel level. Non-critical entries that deserve eyes.
	WarnLevel Level = logrus.WarnLevel

	// InfoLevel level. General operational entries about what's going on inside the application.
	InfoLevel Level = logrus.InfoLevel

	// DebugLevel level. Usually only enabled when debugging. Very verbose logging.
	DebugLevel Level = logrus.DebugLevel

	// TraceLevel level. Designates finer-grained informational events than the Debug.
	TraceLevel Level = logrus.TraceLevel
)

var (
	// instance is the singleton instance of the logger
	instance *logrus.Logger

	// once is used to enforce singleton
	once sync.Once
)

// GetLogger returns the singleton instance of the logger
func GetLogger() *logrus.Logger {
	once.Do(func() {
		// Create a new instance of the logger
		instance = logrus.New()

		// Set the log output to os.Stdout
		instance.SetOutput(os.Stdout)

		// Set the log level to info by default
		instance.SetLevel(InfoLevel)

		// Set the log format to text
		instance.SetFormatter(&logrus.TextFormatter{
			FullTimestamp:          true,
			DisableLevelTruncation: true,
			DisableTimestamp:       false,
			QuoteEmptyFields:       true,
			TimestampFormat:        "2006-01-02 15:04:05.000",
			DisableColors:          false,
			// timestamp format use epoch time or unix time
			// TimestampFormat: "1351700038",
		})
	})

	// Return the singleton instance
	return instance
}
