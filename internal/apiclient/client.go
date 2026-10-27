// Package apiclient provides an HTTP client for communicating with the Chiperka cloud API.
//
// It handles all worker↔cloud communication: registration, heartbeat, job claiming,
// progress reporting, log streaming, artifact uploads, and job completion.
//
// Design decisions (modeled after GitLab Runner / Sentry relay):
//   - Retry with exponential backoff on transient errors (429, 5xx, timeouts)
//   - Fail fast on non-retryable errors (400, 401, 403, 404)
//   - Idempotent job completion via X-Idempotency-Key header
//   - Job-scoped JWT tokens with transparent refresh before expiry
//   - Context propagation on all methods for cancellation support
//   - Connection pooling via shared http.Transport
package apiclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"mime/multipart"
	"net/http"
	"strings"
	"time"
)

// Client handles all HTTP communication with the Chiperka cloud API.
// Thread-safe — designed for concurrent use across multiple executor goroutines.
type Client struct {
	baseURL    string
	token      string // worker-level auth token (spt_ or PAT)
	httpClient *http.Client
	logger     *slog.Logger
}

// New creates a Client with production-tuned HTTP transport settings.
// The token is the worker-level auth token used for registration and heartbeat.
func New(baseURL, token string, logger *slog.Logger) *Client {
	if logger == nil {
		logger = slog.Default()
	}
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   token,
		logger:  logger,
		httpClient: &http.Client{
			// 35s > server-side long-poll max (30s) to avoid premature client timeouts
			Timeout: 35 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 20,
				IdleConnTimeout:     90 * time.Second,
				TLSHandshakeTimeout: 10 * time.Second,
			},
		},
	}
}

// BaseURL returns the API server base URL.
func (c *Client) BaseURL() string { return c.baseURL }

// --------------------------------------------------------------------------
// Core HTTP methods with retry and error classification
// --------------------------------------------------------------------------

// APIError represents an error response from the cloud API.
// StatusCode is set for HTTP errors; 0 for network/timeout errors.
type APIError struct {
	StatusCode int
	Status     string
	Body       string
	Err        error
}

func (e *APIError) Error() string {
	if e.Err != nil {
		return e.Err.Error()
	}
	return fmt.Sprintf("API error: %s - %s", e.Status, e.Body)
}

func (e *APIError) Unwrap() error { return e.Err }

// IsRetryable returns true for transient errors worth retrying:
// 429 (rate limited), 502/503/504 (server overload), and network errors.
func (e *APIError) IsRetryable() bool {
	if e.Err != nil {
		return true // network/timeout errors are always retryable
	}
	switch e.StatusCode {
	case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	}
	return false
}

// doRequest executes a single HTTP request and decodes the response.
// Returns *APIError for all error cases (network, HTTP 4xx/5xx).
func (c *Client) doRequest(ctx context.Context, method, path string, body interface{}, result interface{}, token string) error {
	var bodyReader io.Reader
	if body != nil {
		jsonBody, err := json.Marshal(body)
		if err != nil {
			return &APIError{Err: fmt.Errorf("marshal request body: %w", err)}
		}
		bodyReader = bytes.NewReader(jsonBody)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bodyReader)
	if err != nil {
		return &APIError{Err: fmt.Errorf("create request: %w", err)}
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	// Use provided token (job-scoped) or fall back to worker token
	authToken := token
	if authToken == "" {
		authToken = c.token
	}
	if authToken != "" {
		req.Header.Set("Authorization", "Bearer "+authToken)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return &APIError{Err: fmt.Errorf("request %s %s: %w", method, path, err)}
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return &APIError{
			StatusCode: resp.StatusCode,
			Status:     resp.Status,
			Body:       string(respBody),
		}
	}

	if result != nil {
		if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
			return &APIError{Err: fmt.Errorf("decode response from %s %s: %w", method, path, err)}
		}
	}

	return nil
}

// doWithRetry executes an HTTP request with exponential backoff on retryable errors.
// Non-retryable errors (400, 401, 403, 404) fail immediately.
func (c *Client) doWithRetry(ctx context.Context, method, path string, body interface{}, result interface{}, token string, maxRetries int) error {
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			wait := Backoff(500*time.Millisecond, attempt-1, 30*time.Second)
			c.logger.Debug("retrying request",
				"method", method,
				"path", path,
				"attempt", attempt+1,
				"wait", wait,
			)
			select {
			case <-ctx.Done():
				return &APIError{Err: fmt.Errorf("request cancelled: %w", ctx.Err())}
			case <-time.After(wait):
			}
		}

		err := c.doRequest(ctx, method, path, body, result, token)
		if err == nil {
			return nil
		}

		lastErr = err
		apiErr, ok := err.(*APIError)
		if !ok || !apiErr.IsRetryable() {
			return err // non-retryable, fail immediately
		}
	}
	return lastErr
}

// post is a convenience wrapper for POST requests with the worker token.
func (c *Client) post(ctx context.Context, path string, body interface{}, result interface{}) error {
	return c.doRequest(ctx, "POST", path, body, result, "")
}

// postWithRetry is a convenience wrapper for POST with retry and worker token.
func (c *Client) postWithRetry(ctx context.Context, path string, body interface{}, result interface{}, maxRetries int) error {
	return c.doWithRetry(ctx, "POST", path, body, result, "", maxRetries)
}

// --------------------------------------------------------------------------
// Worker registration & heartbeat
// --------------------------------------------------------------------------

// RegisterResponse is the API response from worker registration.
type RegisterResponse struct {
	Status   string `json:"status"`
	WorkerID string `json:"worker_id"`
}

// Register registers the worker with the cloud API.
// Retries up to 10 times with exponential backoff (cloud might not be ready yet).
func (c *Client) Register(ctx context.Context, hostname string, concurrency int, capacity int, maxContainers int, version string) (string, error) {
	body := map[string]interface{}{
		"hostname":       hostname,
		"concurrency":    concurrency,
		"capacity":       capacity,
		"max_containers": maxContainers,
		"version":        version,
	}

	var resp RegisterResponse
	var lastErr error
	for attempt := 0; attempt < 10; attempt++ {
		if err := c.post(ctx, "/api/workers/register", body, &resp); err != nil {
			lastErr = err
			wait := Backoff(2*time.Second, attempt, 30*time.Second)
			c.logger.Warn("registration failed, retrying",
				"attempt", attempt+1,
				"error", err,
				"retry_in", wait,
			)
			select {
			case <-ctx.Done():
				return "", fmt.Errorf("registration cancelled: %w", ctx.Err())
			case <-time.After(wait):
			}
			continue
		}
		return resp.WorkerID, nil
	}
	return "", fmt.Errorf("failed to register after 10 attempts: %w", lastErr)
}

// HeartbeatResponse is the API response from heartbeat.
type HeartbeatResponse struct {
	Status   string   `json:"status"`
	Commands []string `json:"commands,omitempty"`
}

// Heartbeat sends a heartbeat to the cloud API and returns any pending commands.
func (c *Client) Heartbeat(ctx context.Context, workerID string, activeJobs int, currentJobID string) (*HeartbeatResponse, error) {
	body := map[string]interface{}{
		"id":           workerID,
		"active_tests": activeJobs,
	}
	if currentJobID != "" {
		body["current_run_id"] = currentJobID
	}

	var resp HeartbeatResponse
	if err := c.post(ctx, "/api/workers/heartbeat", body, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// --------------------------------------------------------------------------
// Job lifecycle: claim → ack → progress → complete
// --------------------------------------------------------------------------

// ClaimJobs claims up to count jobs using long polling.
// The server holds the connection open for up to timeoutMs waiting for jobs.
// availableCapacity tells the server the max job weight the worker can accept.
// Returns nil (no jobs) or a slice of claimed jobs.
func (c *Client) ClaimJobs(ctx context.Context, workerID string, count int, timeoutMs int, availableCapacity int) ([]json.RawMessage, error) {
	body := map[string]interface{}{
		"worker_id":          workerID,
		"count":              count,
		"timeout_ms":         timeoutMs,
		"available_capacity": availableCapacity,
	}
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal claim request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/api/jobs/claim", bytes.NewReader(jsonBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return nil, nil // no jobs available
	}
	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("claim failed: %s - %s", resp.Status, string(bodyBytes))
	}

	var claimResp struct {
		Jobs []json.RawMessage `json:"jobs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&claimResp); err != nil {
		return nil, fmt.Errorf("decode claim response: %w", err)
	}

	return claimResp.Jobs, nil
}

// AckJobs confirms receipt of claimed jobs. Returns list of successfully acked job IDs.
// Jobs not in the returned list were already reset by the monitor — do NOT execute them.
func (c *Client) AckJobs(ctx context.Context, workerID string, jobIDs []string) ([]string, error) {
	body := map[string]interface{}{
		"worker_id": workerID,
		"job_ids":   jobIDs,
	}
	var resp struct {
		Acked  int      `json:"acked"`
		JobIDs []string `json:"job_ids"`
	}
	if err := c.post(ctx, "/api/jobs/ack", body, &resp); err != nil {
		return nil, fmt.Errorf("ack failed: %w", err)
	}
	return resp.JobIDs, nil
}

// ReportProgress sends a step progress update to the cloud.
// Best-effort: logs warning on failure but does not return error.
// This matches GitLab Runner's approach — progress is advisory, not critical.
func (c *Client) ReportProgress(ctx context.Context, jobID, token, workerID, stepName, status string, durationMs int64, errMsg string) {
	body := map[string]interface{}{
		"worker_id":   workerID,
		"step":        stepName,
		"status":      status,
		"duration_ms": durationMs,
	}
	if errMsg != "" {
		body["error"] = errMsg
	}
	path := fmt.Sprintf("/api/jobs/%s/progress", jobID)
	if err := c.doRequest(ctx, "POST", path, body, nil, token); err != nil {
		c.logger.Warn("progress report failed",
			"job_id", jobID,
			"step", stepName,
			"error", err,
		)
	}
}

// CompleteJob sends the final job result to the cloud.
// Uses idempotency key to prevent duplicate processing on retry.
// Retries up to 5 times with exponential backoff (critical path).
func (c *Client) CompleteJob(ctx context.Context, jobID, token, idempotencyKey string, result interface{}) error {
	jsonBody, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal job result: %w", err)
	}

	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		if attempt > 0 {
			wait := Backoff(time.Second, attempt-1, 30*time.Second)
			c.logger.Warn("retrying job completion",
				"job_id", jobID,
				"attempt", attempt+1,
				"wait", wait,
			)
			select {
			case <-ctx.Done():
				return fmt.Errorf("completion cancelled: %w", ctx.Err())
			case <-time.After(wait):
			}
		}

		req, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+fmt.Sprintf("/api/jobs/%s/complete", jobID), bytes.NewReader(jsonBody))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		} else if c.token != "" {
			req.Header.Set("Authorization", "Bearer "+c.token)
		}
		if idempotencyKey != "" {
			req.Header.Set("X-Idempotency-Key", idempotencyKey)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = err
			continue // network error, retry
		}
		resp.Body.Close()

		if resp.StatusCode < 400 {
			return nil // success
		}

		// Non-retryable HTTP errors
		if resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusNotFound {
			return fmt.Errorf("complete job %s failed: %s", jobID, resp.Status)
		}

		lastErr = fmt.Errorf("complete job %s: %s", jobID, resp.Status)
		// 5xx — retry
	}
	return fmt.Errorf("complete job %s failed after 5 attempts: %w", jobID, lastErr)
}

// --------------------------------------------------------------------------
// File transfers: downloads and uploads
// --------------------------------------------------------------------------

// DownloadFile downloads a file from the given URL with the specified token.
func (c *Client) DownloadFile(ctx context.Context, token, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	} else if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return nil, &APIError{
			StatusCode: resp.StatusCode,
			Status:     resp.Status,
			Body:       string(body),
		}
	}
	return io.ReadAll(resp.Body)
}

// DownloadJobFile downloads a file from a job's file namespace.
func (c *Client) DownloadJobFile(ctx context.Context, token, jobID, path string) ([]byte, error) {
	url := fmt.Sprintf("%s/api/jobs/%s/files/%s", c.baseURL, jobID, path)
	return c.DownloadFile(ctx, token, url)
}

// UploadArtifact uploads an artifact as multipart/form-data.
// Uses streaming multipart encoding via io.Pipe — content flows directly
// from the reader to the HTTP body without buffering the entire payload.
// Retries up to 3 times on transient failures.
func (c *Client) UploadArtifact(ctx context.Context, token, jobID, name, content string) error {
	return c.UploadArtifactStream(ctx, token, jobID, name, strings.NewReader(content), int64(len(content)))
}

// UploadArtifactStream uploads an artifact from an io.Reader via multipart/form-data.
// This is the streaming path — avoids holding entire artifact content as a string.
// size is used for Content-Length; pass -1 if unknown (chunked transfer).
func (c *Client) UploadArtifactStream(ctx context.Context, token, jobID, name string, content io.Reader, size int64) error {
	uploadPath := fmt.Sprintf("/api/jobs/%s/artifacts", jobID)

	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			wait := Backoff(500*time.Millisecond, attempt-1, 5*time.Second)
			select {
			case <-ctx.Done():
				return &APIError{Err: ctx.Err()}
			case <-time.After(wait):
			}
			// Content reader may be exhausted after first attempt — if it's a strings.Reader
			// or bytes.Reader, it supports Seek. Otherwise retry won't work.
			if seeker, ok := content.(io.Seeker); ok {
				seeker.Seek(0, io.SeekStart)
			}
		}

		// Build multipart body using io.Pipe for zero-buffer streaming
		pr, pw := io.Pipe()
		writer := multipart.NewWriter(pw)

		// Write multipart fields in a goroutine so the pipe streams to the HTTP request
		go func() {
			defer pw.Close()
			writer.WriteField("name", name)
			part, err := writer.CreateFormFile("file", name)
			if err != nil {
				pw.CloseWithError(err)
				return
			}
			if _, err := io.Copy(part, content); err != nil {
				pw.CloseWithError(err)
				return
			}
			writer.Close()
		}()

		req, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+uploadPath, pr)
		if err != nil {
			pr.Close()
			return &APIError{Err: err}
		}
		req.Header.Set("Content-Type", writer.FormDataContentType())
		authToken := token
		if authToken == "" {
			authToken = c.token
		}
		if authToken != "" {
			req.Header.Set("Authorization", "Bearer "+authToken)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = &APIError{Err: err}
			if ctx.Err() != nil {
				return lastErr
			}
			continue // retry on network error
		}
		io.Copy(io.Discard, resp.Body) // drain body to reuse HTTP connection
		resp.Body.Close()

		if resp.StatusCode < 400 {
			return nil // success
		}

		apiErr := &APIError{StatusCode: resp.StatusCode, Status: resp.Status}
		if !apiErr.IsRetryable() {
			return apiErr
		}
		lastErr = apiErr
	}
	return lastErr
}

// --------------------------------------------------------------------------
// Log streaming
// --------------------------------------------------------------------------

// SendLogs sends a batch of log entries to the cloud and returns any server-directed interval.
func (c *Client) SendLogs(ctx context.Context, token, jobID string, logs interface{}) (time.Duration, error) {
	body := map[string]interface{}{"logs": logs}
	resp := struct {
		UpdateInterval int `json:"update_interval"`
	}{}
	path := fmt.Sprintf("/api/jobs/%s/logs", jobID)
	err := c.doRequest(ctx, "POST", path, body, &resp, token)
	if err != nil {
		return 0, err
	}
	if resp.UpdateInterval > 0 {
		return time.Duration(resp.UpdateInterval) * time.Second, nil
	}
	return 0, nil
}

// --------------------------------------------------------------------------
// Utility
// --------------------------------------------------------------------------

// Backoff calculates exponential backoff capped at max.
// consecutive is the number of consecutive failures (0 = first backoff sleep).
func Backoff(base time.Duration, consecutive int, max time.Duration) time.Duration {
	if consecutive <= 0 {
		return base
	}
	d := time.Duration(float64(base) * math.Pow(2, float64(consecutive)))
	if d > max {
		return max
	}
	return d
}
