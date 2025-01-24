package output_http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/mata-elang-stable/report-command-service/internal/config"
	"github.com/mata-elang-stable/report-command-service/internal/logger"
	"github.com/mata-elang-stable/report-command-service/internal/types"
)

var log = logger.GetLogger()
var requestTimeoutSeconds = 5 * time.Second
var requestMaxRetries = 3
var conf = config.GetConfig()

func PostEvent(ctx context.Context, event types.Event) error {
	url := conf.RepoApiUrl + "/events"

	marshaled, err := json.Marshal(event)
	if err != nil {
		log.Errorf("Failed to marshal event: %v", err)
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(marshaled))
	if err != nil {
		log.Errorf("Failed to create request: %v", err)
		return err
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")

	// Send request with retries
	for i := 0; i < requestMaxRetries; i++ {
		err = makeRequest(req)
		if err == nil {
			return nil
		}
	}

	return errors.New("failed to send request after retries")
}

func makeRequest(req *http.Request) error {
	client := &http.Client{
		Timeout: requestTimeoutSeconds,
	}

	res, err := client.Do(req)
	if err != nil {
		log.Errorf("Failed to send request: %v", err)
		return err
	}

	if res.StatusCode != http.StatusOK {
		log.Errorf("Failed to send request: %v", err)
	}

	return nil
}
