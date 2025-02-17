package reporter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/mata-elang-stable/report-command-service/internal/schema"
	"net/http"
	"net/url"
	"time"

	"github.com/mata-elang-stable/report-command-service/internal/config"
	"github.com/mata-elang-stable/report-command-service/internal/logger"
)

type HTTPReporter struct {
	apiBaseURL      string
	apiPostEventURL string
	httpMaxTimeout  time.Duration
	httpMaxRetries  int
}

var log = logger.GetLogger()

func joinUrlPath(u string, path string) string {
	r, err := url.JoinPath(u, path)
	if err != nil {
		log.Fatalf("Error joining path %s: %s", u, err)
	}
	return r
}

func NewHTTPReporter(config *config.Config) *HTTPReporter {
	return &HTTPReporter{
		apiBaseURL:      config.ReportApiUrl,
		apiPostEventURL: joinUrlPath(config.ReportApiUrl, config.ReportPostEventPath),
		httpMaxTimeout:  time.Duration(config.HTTPTimeoutSeconds) * time.Second,
		httpMaxRetries:  config.HTTPMaxRetries,
	}
}

func (r *HTTPReporter) PostEvent(ctx context.Context, event *schema.Event) error {
	marshaled, err := json.Marshal(event)
	if err != nil {
		log.Errorf("Failed to marshal event: %v", err)
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.apiPostEventURL, bytes.NewReader(marshaled))
	if err != nil {
		log.Errorf("Failed to create request: %v", err)
		return err
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")

	// Send request with retries
	for i := 0; i < r.httpMaxRetries; i++ {
		err = makeRequest(req, r.httpMaxTimeout)
		if err == nil {
			return nil
		}
	}

	return errors.New("failed to send request after retries")
}

func makeRequest(req *http.Request, timeout time.Duration) error {
	client := &http.Client{
		Timeout: timeout,
	}

	res, err := client.Do(req)
	if err != nil {
		log.Errorf("Failed to send request: %v", err)
		return err
	}

	if res.StatusCode != http.StatusOK {
		log.Errorf("Failed to send request with status code: %d", res.StatusCode)
		log.Debugf("Reason: %v", res.Body)
		return errors.New("failed to send request")
	}

	return nil
}
