package apiclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"chiperka-worker/internal/instruction"
)

// --------------------------------------------------------------------------
// Retry logic tests
// --------------------------------------------------------------------------

func TestDoWithRetry_SuccessOnFirstAttempt(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}))
	defer srv.Close()

	c := New(srv.URL, "test-token", nil)
	var resp map[string]string
	err := c.doWithRetry(context.Background(), "POST", "/test", nil, &resp, "", 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp["status"] != "ok" {
		t.Errorf("expected ok, got %v", resp["status"])
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Errorf("expected 1 call, got %d", calls)
	}
}

func TestDoWithRetry_RetriesOnServerError(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}))
	defer srv.Close()

	c := New(srv.URL, "", nil)
	var resp map[string]string
	err := c.doWithRetry(context.Background(), "POST", "/test", nil, &resp, "", 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if atomic.LoadInt32(&calls) != 3 {
		t.Errorf("expected 3 calls (2 failures + 1 success), got %d", calls)
	}
}

func TestDoWithRetry_FailsImmediatelyOnBadRequest(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "bad request")
	}))
	defer srv.Close()

	c := New(srv.URL, "", nil)
	err := c.doWithRetry(context.Background(), "POST", "/test", nil, nil, "", 3)
	if err == nil {
		t.Fatal("expected error for 400")
	}
	apiErr, ok := err.(*APIError)
	if !ok {
		t.Fatalf("expected APIError, got %T", err)
	}
	if apiErr.StatusCode != 400 {
		t.Errorf("expected status 400, got %d", apiErr.StatusCode)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Errorf("expected 1 call (no retry for 400), got %d", calls)
	}
}

func TestDoWithRetry_RetriesOn429(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := New(srv.URL, "", nil)
	err := c.doWithRetry(context.Background(), "GET", "/test", nil, nil, "", 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if atomic.LoadInt32(&calls) != 2 {
		t.Errorf("expected 2 calls, got %d", calls)
	}
}

func TestDoWithRetry_ContextCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	c := New(srv.URL, "", nil)
	err := c.doWithRetry(ctx, "POST", "/test", nil, nil, "", 10)
	if err == nil {
		t.Fatal("expected error from context cancellation")
	}
}

// --------------------------------------------------------------------------
// Auth token tests
// --------------------------------------------------------------------------

func TestDoRequest_SetsAuthHeader(t *testing.T) {
	var authHeader string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := New(srv.URL, "worker-token-123", nil)
	c.doRequest(context.Background(), "POST", "/test", nil, nil, "")
	if authHeader != "Bearer worker-token-123" {
		t.Errorf("expected worker token, got %q", authHeader)
	}
}

func TestDoRequest_PrefersJobToken(t *testing.T) {
	var authHeader string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := New(srv.URL, "worker-token", nil)
	c.doRequest(context.Background(), "POST", "/test", nil, nil, "job-token-456")
	if authHeader != "Bearer job-token-456" {
		t.Errorf("expected job token, got %q", authHeader)
	}
}

// --------------------------------------------------------------------------
// CompleteJob tests
// --------------------------------------------------------------------------

func TestCompleteJob_SendsIdempotencyKey(t *testing.T) {
	var idempotencyKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		idempotencyKey = r.Header.Get("X-Idempotency-Key")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := New(srv.URL, "token", nil)
	err := c.CompleteJob(context.Background(), "42", "job-token", "my-idem-key", map[string]string{"status": "passed"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if idempotencyKey != "my-idem-key" {
		t.Errorf("expected idempotency key 'my-idem-key', got %q", idempotencyKey)
	}
}

func TestCompleteJob_RetriesOnServerError(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := New(srv.URL, "", nil)
	err := c.CompleteJob(context.Background(), "42", "", "", map[string]string{"status": "passed"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if atomic.LoadInt32(&calls) != 3 {
		t.Errorf("expected 3 calls, got %d", calls)
	}
}

func TestCompleteJob_FailsFastOnForbidden(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	c := New(srv.URL, "", nil)
	err := c.CompleteJob(context.Background(), "42", "", "", map[string]string{"status": "passed"})
	if err == nil {
		t.Fatal("expected error for 403")
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Errorf("expected 1 call (no retry for 403), got %d", calls)
	}
}

// --------------------------------------------------------------------------
// UploadArtifact tests
// --------------------------------------------------------------------------

func TestUploadArtifact_RetriesOnFailure(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := New(srv.URL, "", nil)
	err := c.UploadArtifact(context.Background(), "tok", "j1", "test.log", "content")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if atomic.LoadInt32(&calls) != 2 {
		t.Errorf("expected 2 calls, got %d", calls)
	}
}

// --------------------------------------------------------------------------
// APIError tests
// --------------------------------------------------------------------------

func TestAPIError_IsRetryable(t *testing.T) {
	tests := []struct {
		err      APIError
		expected bool
	}{
		{APIError{StatusCode: 429}, true},
		{APIError{StatusCode: 502}, true},
		{APIError{StatusCode: 503}, true},
		{APIError{StatusCode: 504}, true},
		{APIError{StatusCode: 400}, false},
		{APIError{StatusCode: 401}, false},
		{APIError{StatusCode: 403}, false},
		{APIError{StatusCode: 404}, false},
		{APIError{Err: fmt.Errorf("connection refused")}, true},
	}

	for _, tt := range tests {
		got := tt.err.IsRetryable()
		if got != tt.expected {
			t.Errorf("APIError{StatusCode: %d, Err: %v}.IsRetryable() = %v, want %v",
				tt.err.StatusCode, tt.err.Err, got, tt.expected)
		}
	}
}

// --------------------------------------------------------------------------
// Backoff tests
// --------------------------------------------------------------------------

func TestBackoff(t *testing.T) {
	tests := []struct {
		base        time.Duration
		consecutive int
		max         time.Duration
		expected    time.Duration
	}{
		{time.Second, 0, 30 * time.Second, time.Second},
		{time.Second, 1, 30 * time.Second, 2 * time.Second},
		{time.Second, 2, 30 * time.Second, 4 * time.Second},
		{time.Second, 3, 30 * time.Second, 8 * time.Second},
		{time.Second, 10, 30 * time.Second, 30 * time.Second}, // capped
		{500 * time.Millisecond, 1, 5 * time.Second, time.Second},
	}

	for _, tt := range tests {
		got := Backoff(tt.base, tt.consecutive, tt.max)
		if got != tt.expected {
			t.Errorf("Backoff(%v, %d, %v) = %v, want %v",
				tt.base, tt.consecutive, tt.max, got, tt.expected)
		}
	}
}

// --------------------------------------------------------------------------
// Register tests
// --------------------------------------------------------------------------

func TestRegister_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/workers/register" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		json.NewEncoder(w).Encode(RegisterResponse{Status: "registered", WorkerID: "w-123"})
	}))
	defer srv.Close()

	c := New(srv.URL, "spt_token", nil)
	id, err := c.Register(context.Background(), "", "host1", 4, 100, 0, "1.0.0")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != "w-123" {
		t.Errorf("expected w-123, got %s", id)
	}
}

func TestRegister_RetriesOnFailure(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(RegisterResponse{Status: "registered", WorkerID: "w-456"})
	}))
	defer srv.Close()

	c := New(srv.URL, "spt_token", nil)
	id, err := c.Register(context.Background(), "", "host1", 4, 100, 0, "1.0.0")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != "w-456" {
		t.Errorf("expected w-456, got %s", id)
	}
}

func TestRegister_AdoptsIssuedToken(t *testing.T) {
	var lastAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lastAuth = r.Header.Get("Authorization")
		if r.URL.Path == "/api/workers/register" {
			var body map[string]interface{}
			json.NewDecoder(r.Body).Decode(&body)
			if body["name"] != "ci-1" {
				t.Errorf("expected name ci-1, got %v", body["name"])
			}
			json.NewEncoder(w).Encode(RegisterResponse{Status: "registered", WorkerID: "wj-1", Token: "spt_issued"})
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok"})
	}))
	defer srv.Close()

	c := New(srv.URL, "join-secret", nil)
	if _, err := c.Register(context.Background(), "ci-1", "host1", 4, 100, 0, "1.0.0"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if lastAuth != "Bearer join-secret" {
		t.Errorf("register should use the join token, got %q", lastAuth)
	}
	c.Heartbeat(context.Background(), "wj-1", 0, "")
	if lastAuth != "Bearer spt_issued" {
		t.Errorf("later calls should use the issued token, got %q", lastAuth)
	}
}

// --------------------------------------------------------------------------
// ClaimJobs tests
// --------------------------------------------------------------------------

func TestClaimJobs_NoContent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	c := New(srv.URL, "", nil)
	jobs, err := c.ClaimJobs(context.Background(), "w1", 4, 1000, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if jobs != nil {
		t.Errorf("expected nil, got %v", jobs)
	}
}

func TestClaimJobs_ReturnsBatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"jobs": []map[string]interface{}{
				{"job_id": "1", "token": "t1"},
				{"job_id": "2", "token": "t2"},
			},
		})
	}))
	defer srv.Close()

	c := New(srv.URL, "", nil)
	jobs, err := c.ClaimJobs(context.Background(), "w1", 4, 1000, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(jobs) != 2 {
		t.Errorf("expected 2 jobs, got %d", len(jobs))
	}
}

// --------------------------------------------------------------------------
// ReportProgress tests
// --------------------------------------------------------------------------

func TestReportProgress_LogsWarningOnFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	// Should not panic — progress is best-effort
	c := New(srv.URL, "", nil)
	c.ReportProgress(context.Background(), "42", "", "w1", "execute", "completed", 1000, "")
}

// --------------------------------------------------------------------------
// Heartbeat tests
// --------------------------------------------------------------------------

func TestHeartbeat_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/workers/heartbeat" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if r.Method != "POST" {
			t.Errorf("expected POST, got %s", r.Method)
		}
		var body map[string]interface{}
		json.NewDecoder(r.Body).Decode(&body)
		if body["id"] != "w-1" {
			t.Errorf("expected worker id w-1, got %v", body["id"])
		}
		json.NewEncoder(w).Encode(HeartbeatResponse{
			Status:   "ok",
			Commands: []string{"shutdown"},
		})
	}))
	defer srv.Close()

	c := New(srv.URL, "token", nil)
	resp, err := c.Heartbeat(context.Background(), "w-1", 2, "job-55")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Status != "ok" {
		t.Errorf("expected status ok, got %s", resp.Status)
	}
	if len(resp.Commands) != 1 || resp.Commands[0] != "shutdown" {
		t.Errorf("expected [shutdown], got %v", resp.Commands)
	}
}

// --------------------------------------------------------------------------
// AckJobs tests
// --------------------------------------------------------------------------

func TestAckJobs_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/jobs/ack" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		var body map[string]interface{}
		json.NewDecoder(r.Body).Decode(&body)
		if body["worker_id"] != "w-1" {
			t.Errorf("unexpected worker_id: %v", body["worker_id"])
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"acked":   2,
			"job_ids": []string{"j1", "j2"},
		})
	}))
	defer srv.Close()

	c := New(srv.URL, "token", nil)
	ids, err := c.AckJobs(context.Background(), "w-1", []string{"j1", "j2"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 acked ids, got %d", len(ids))
	}
	if ids[0] != "j1" || ids[1] != "j2" {
		t.Errorf("unexpected acked ids: %v", ids)
	}
}

// --------------------------------------------------------------------------
// DownloadFile tests
// --------------------------------------------------------------------------

func TestDownloadFile_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Errorf("expected GET, got %s", r.Method)
		}
		auth := r.Header.Get("Authorization")
		if auth != "Bearer job-tok" {
			t.Errorf("expected job token, got %q", auth)
		}
		w.Write([]byte("file-content-here"))
	}))
	defer srv.Close()

	c := New("http://unused", "worker-tok", nil)
	data, err := c.DownloadFile(context.Background(), "job-tok", srv.URL+"/some/file")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "file-content-here" {
		t.Errorf("unexpected content: %s", string(data))
	}
}

func TestDownloadFile_FallsBackToWorkerToken(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Write([]byte("ok"))
	}))
	defer srv.Close()

	c := New("http://unused", "worker-tok", nil)
	_, err := c.DownloadFile(context.Background(), "", srv.URL+"/file")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotAuth != "Bearer worker-tok" {
		t.Errorf("expected worker token fallback, got %q", gotAuth)
	}
}

func TestDownloadFile_ErrorOn4xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("not found"))
	}))
	defer srv.Close()

	c := New("http://unused", "", nil)
	_, err := c.DownloadFile(context.Background(), "", srv.URL+"/missing")
	if err == nil {
		t.Fatal("expected error for 404")
	}
	apiErr, ok := err.(*APIError)
	if !ok {
		t.Fatalf("expected *APIError, got %T", err)
	}
	if apiErr.StatusCode != 404 {
		t.Errorf("expected 404, got %d", apiErr.StatusCode)
	}
	if apiErr.Body != "not found" {
		t.Errorf("expected body 'not found', got %q", apiErr.Body)
	}
}

// --------------------------------------------------------------------------
// DownloadJobFile tests
// --------------------------------------------------------------------------

func TestDownloadJobFile_URLConstruction(t *testing.T) {
	var requestedURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedURL = r.URL.Path
		w.Write([]byte("data"))
	}))
	defer srv.Close()

	c := New(srv.URL, "tok", nil)
	data, err := c.DownloadJobFile(context.Background(), "job-tok", "job-42", "instructions.json")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expectedPath := "/api/jobs/job-42/files/instructions.json"
	if requestedURL != expectedPath {
		t.Errorf("expected path %q, got %q", expectedPath, requestedURL)
	}
	if string(data) != "data" {
		t.Errorf("unexpected content: %s", string(data))
	}
}

// --------------------------------------------------------------------------
// SendLogs tests
// --------------------------------------------------------------------------

func TestSendLogs_SuccessWithInterval(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/jobs/j1/logs" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		auth := r.Header.Get("Authorization")
		if auth != "Bearer log-tok" {
			t.Errorf("expected log-tok, got %q", auth)
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"update_interval": 5,
		})
	}))
	defer srv.Close()

	c := New(srv.URL, "", nil)
	interval, err := c.SendLogs(context.Background(), "log-tok", "j1", []map[string]string{{"message": "hello"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if interval != 5*time.Second {
		t.Errorf("expected 5s interval, got %v", interval)
	}
}

func TestSendLogs_ZeroInterval(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"update_interval": 0,
		})
	}))
	defer srv.Close()

	c := New(srv.URL, "", nil)
	interval, err := c.SendLogs(context.Background(), "", "j1", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if interval != 0 {
		t.Errorf("expected 0 interval, got %v", interval)
	}
}

// --------------------------------------------------------------------------
// BaseURL tests
// --------------------------------------------------------------------------

func TestBaseURL_Getter(t *testing.T) {
	c := New("https://api.example.com/", "tok", nil)
	// New trims trailing slash
	if c.BaseURL() != "https://api.example.com" {
		t.Errorf("expected trimmed URL, got %q", c.BaseURL())
	}
}

func TestBaseURL_NoTrailingSlash(t *testing.T) {
	c := New("https://api.example.com", "tok", nil)
	if c.BaseURL() != "https://api.example.com" {
		t.Errorf("expected %q, got %q", "https://api.example.com", c.BaseURL())
	}
}

// --------------------------------------------------------------------------
// APIError.Unwrap tests
// --------------------------------------------------------------------------

func TestAPIError_Unwrap_ReturnsWrappedError(t *testing.T) {
	inner := fmt.Errorf("connection refused")
	apiErr := &APIError{Err: inner}
	if apiErr.Unwrap() != inner {
		t.Errorf("expected Unwrap to return inner error")
	}
}

func TestAPIError_Unwrap_NilWhenNoInnerError(t *testing.T) {
	apiErr := &APIError{StatusCode: 500, Status: "500 Internal Server Error", Body: "fail"}
	if apiErr.Unwrap() != nil {
		t.Errorf("expected nil from Unwrap when no inner error")
	}
}

func TestAPIError_ErrorString_WithErr(t *testing.T) {
	apiErr := &APIError{Err: fmt.Errorf("timeout")}
	if apiErr.Error() != "timeout" {
		t.Errorf("expected 'timeout', got %q", apiErr.Error())
	}
}

func TestAPIError_ErrorString_WithStatusAndBody(t *testing.T) {
	apiErr := &APIError{StatusCode: 403, Status: "403 Forbidden", Body: "denied"}
	expected := "API error: 403 Forbidden - denied"
	if apiErr.Error() != expected {
		t.Errorf("expected %q, got %q", expected, apiErr.Error())
	}
}

func TestAPIError_WorksWithErrorsIs(t *testing.T) {
	inner := fmt.Errorf("base error")
	apiErr := &APIError{Err: fmt.Errorf("wrapped: %w", inner)}
	if !errors.Is(apiErr, inner) {
		t.Error("expected errors.Is to find inner error through Unwrap chain")
	}
}

// --------------------------------------------------------------------------
// InstructionAdapter tests
// --------------------------------------------------------------------------

func TestInstructionAdapter_DownloadJobFile(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Write([]byte("file-data"))
	}))
	defer srv.Close()

	c := New(srv.URL, "tok", nil)
	adapter := &InstructionAdapter{Client: c}
	data, err := adapter.DownloadJobFile("job-tok", "j-99", "config.yml")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "file-data" {
		t.Errorf("unexpected data: %s", string(data))
	}
	if !strings.Contains(gotPath, "/api/jobs/j-99/files/config.yml") {
		t.Errorf("unexpected path: %s", gotPath)
	}
}

func TestInstructionAdapter_UploadArtifact(t *testing.T) {
	var gotPath string
	var gotName string
	var gotContent string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		// UploadArtifact now sends multipart/form-data
		if err := r.ParseMultipartForm(10 * 1024 * 1024); err != nil {
			t.Errorf("failed to parse multipart: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		gotName = r.FormValue("name")
		file, _, err := r.FormFile("file")
		if err != nil {
			t.Errorf("missing file: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		defer file.Close()
		data, _ := io.ReadAll(file)
		gotContent = string(data)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := New(srv.URL, "tok", nil)
	adapter := &InstructionAdapter{Client: c}
	err := adapter.UploadArtifact("job-tok", "j-99", "results.xml", "<xml/>")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotPath != "/api/jobs/j-99/artifacts" {
		t.Errorf("unexpected path: %s", gotPath)
	}
	if gotName != "results.xml" {
		t.Errorf("unexpected name: %v", gotName)
	}
	if gotContent != "<xml/>" {
		t.Errorf("unexpected content: %v", gotContent)
	}
}

// Verify InstructionAdapter satisfies instruction.APIClient interface at compile time.
var _ instruction.APIClient = (*InstructionAdapter)(nil)

// --------------------------------------------------------------------------
// LogSenderAdapter tests
// --------------------------------------------------------------------------

func TestLogSenderAdapter_SendLogs(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/jobs/j-10/logs" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		auth := r.Header.Get("Authorization")
		if auth != "Bearer adapter-tok" {
			t.Errorf("expected adapter-tok, got %q", auth)
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"update_interval": 3,
		})
	}))
	defer srv.Close()

	c := New(srv.URL, "", nil)
	adapter := &LogSenderAdapter{Client: c, Token: "adapter-tok"}
	logs := []instruction.LogEntry{
		{RelativeMs: 100, Level: "info", Message: "hello"},
	}
	interval, err := adapter.SendLogs("j-10", logs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if interval != 3*time.Second {
		t.Errorf("expected 3s, got %v", interval)
	}
}

// Verify LogSenderAdapter satisfies instruction.LogSender interface at compile time.
var _ instruction.LogSender = (*LogSenderAdapter)(nil)
