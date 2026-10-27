package workerloop

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"chiperka-worker/internal/apiclient"
)

// makeJobJSON builds a minimal JSON blob that dag.Job can unmarshal.
func makeJobJSON(jobID string) json.RawMessage {
	j := map[string]interface{}{
		"job_id": jobID,
		"steps":  map[string]interface{}{},
		"meta":   json.RawMessage(`{"type":"test","name":"` + jobID + `"}`),
	}
	b, _ := json.Marshal(j)
	return b
}

// claimHandler is a configurable HTTP handler for the claim/ack mock server.
type claimHandler struct {
	mu         sync.Mutex
	claimFunc  func(w http.ResponseWriter, r *http.Request)
	ackFunc    func(w http.ResponseWriter, r *http.Request)
	claimCalls int
	ackCalls   int
}

func (h *claimHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()

	switch r.URL.Path {
	case "/api/jobs/claim":
		h.claimCalls++
		if h.claimFunc != nil {
			h.claimFunc(w, r)
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
	case "/api/jobs/ack":
		h.ackCalls++
		if h.ackFunc != nil {
			h.ackFunc(w, r)
		} else {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{"acked": 0, "job_ids": []string{}})
		}
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func TestRunClaimLoop_ClaimReturnsJobs(t *testing.T) {
	h := &claimHandler{}
	var claimed bool

	h.claimFunc = func(w http.ResponseWriter, r *http.Request) {
		if !claimed {
			claimed = true
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"jobs": []json.RawMessage{
					makeJobJSON("job-1"),
					makeJobJSON("job-2"),
				},
			})
			return
		}
		// After first claim, return 204 so loop continues quietly until ctx cancels
		w.WriteHeader(http.StatusNoContent)
	}
	h.ackFunc = func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			JobIDs []string `json:"job_ids"`
		}
		json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"acked":   len(req.JobIDs),
			"job_ids": req.JobIDs,
		})
	}

	srv := httptest.NewServer(h)
	defer srv.Close()

	api := apiclient.New(srv.URL, "test-token", slog.Default())
	jobCh := make(chan *ClaimedJob, 10)
	state := NewState()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	go RunClaimLoop(ctx, api, "w1", jobCh, ClaimConfig{
		PollInterval: 50 * time.Millisecond,
		MaxSlots:     5,
		Capacity:     100,
	}, state, slog.Default())

	received := map[string]bool{}
	timeout := time.After(3 * time.Second)
	for len(received) < 2 {
		select {
		case j := <-jobCh:
			received[j.Job.JobID] = true
		case <-timeout:
			t.Fatalf("timed out waiting for jobs, got %d", len(received))
		}
	}

	if !received["job-1"] || !received["job-2"] {
		t.Errorf("expected job-1 and job-2, got %v", received)
	}
}

func TestRunClaimLoop_NoContent_LoopContinues(t *testing.T) {
	h := &claimHandler{} // default claimFunc returns 204

	srv := httptest.NewServer(h)
	defer srv.Close()

	api := apiclient.New(srv.URL, "test-token", slog.Default())
	jobCh := make(chan *ClaimedJob, 10)
	state := NewState()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	RunClaimLoop(ctx, api, "w1", jobCh, ClaimConfig{
		PollInterval: 50 * time.Millisecond,
		MaxSlots:     5,
		Capacity:     100,
	}, state, slog.Default())

	// Should have made multiple claim calls (loop continued on 204)
	h.mu.Lock()
	calls := h.claimCalls
	h.mu.Unlock()

	if calls < 2 {
		t.Errorf("expected multiple claim calls on 204 loop, got %d", calls)
	}

	// No jobs should have been sent
	select {
	case j := <-jobCh:
		t.Errorf("expected no jobs, got %s", j.Job.JobID)
	default:
		// good
	}
}

func TestRunClaimLoop_PartialAck(t *testing.T) {
	h := &claimHandler{}
	var claimed bool

	h.claimFunc = func(w http.ResponseWriter, r *http.Request) {
		if !claimed {
			claimed = true
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"jobs": []json.RawMessage{
					makeJobJSON("job-a"),
					makeJobJSON("job-b"),
					makeJobJSON("job-c"),
				},
			})
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
	// Only ack job-a and job-c, simulating job-b was already reset by monitor
	h.ackFunc = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"acked":   2,
			"job_ids": []string{"job-a", "job-c"},
		})
	}

	srv := httptest.NewServer(h)
	defer srv.Close()

	api := apiclient.New(srv.URL, "test-token", slog.Default())
	jobCh := make(chan *ClaimedJob, 10)
	state := NewState()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	go RunClaimLoop(ctx, api, "w1", jobCh, ClaimConfig{
		PollInterval: 50 * time.Millisecond,
		MaxSlots:     5,
		Capacity:     100,
	}, state, slog.Default())

	received := map[string]bool{}
	timeout := time.After(3 * time.Second)
	for len(received) < 2 {
		select {
		case j := <-jobCh:
			received[j.Job.JobID] = true
		case <-timeout:
			t.Fatalf("timed out waiting for acked jobs, got %v", received)
		}
	}

	// Wait a bit to make sure job-b doesn't sneak through
	time.Sleep(100 * time.Millisecond)

	if received["job-b"] {
		t.Error("job-b should NOT have been sent (not acked)")
	}
	if !received["job-a"] || !received["job-c"] {
		t.Errorf("expected job-a and job-c, got %v", received)
	}
}

func TestRunClaimLoop_ContextCancellation(t *testing.T) {
	h := &claimHandler{} // default returns 204

	srv := httptest.NewServer(h)
	defer srv.Close()

	api := apiclient.New(srv.URL, "test-token", slog.Default())
	jobCh := make(chan *ClaimedJob, 10)
	state := NewState()

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		RunClaimLoop(ctx, api, "w1", jobCh, ClaimConfig{
			PollInterval: 50 * time.Millisecond,
			MaxSlots:     5,
			Capacity:     100,
		}, state, slog.Default())
		close(done)
	}()

	// Let it run briefly, then cancel
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// good - exited cleanly
	case <-time.After(2 * time.Second):
		t.Fatal("RunClaimLoop did not exit after context cancellation")
	}
}

func TestRunClaimLoop_AllSlotsBusy(t *testing.T) {
	h := &claimHandler{}
	h.claimFunc = func(w http.ResponseWriter, r *http.Request) {
		// If we get here, the loop didn't wait for slots
		w.WriteHeader(http.StatusNoContent)
	}

	srv := httptest.NewServer(h)
	defer srv.Close()

	api := apiclient.New(srv.URL, "test-token", slog.Default())
	jobCh := make(chan *ClaimedJob, 10)
	state := NewState()

	// Fill all slots
	maxSlots := 2
	state.AddJob("busy-1", "test", func() {}, 1, 1)
	state.AddJob("busy-2", "test", func() {}, 1, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()

	h.mu.Lock()
	h.claimCalls = 0
	h.mu.Unlock()

	RunClaimLoop(ctx, api, "w1", jobCh, ClaimConfig{
		PollInterval: 50 * time.Millisecond,
		MaxSlots:     maxSlots,
		Capacity:     100,
	}, state, slog.Default())

	// With all slots busy for the entire duration, no claim calls should be made
	h.mu.Lock()
	calls := h.claimCalls
	h.mu.Unlock()

	if calls != 0 {
		t.Errorf("expected 0 claim calls when all slots busy, got %d", calls)
	}
}

func TestRunClaimLoop_SlotFreed_ResumesClaiming(t *testing.T) {
	h := &claimHandler{}
	var claimed bool

	h.claimFunc = func(w http.ResponseWriter, r *http.Request) {
		if !claimed {
			claimed = true
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"jobs": []json.RawMessage{makeJobJSON("job-freed")},
			})
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
	h.ackFunc = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"acked":   1,
			"job_ids": []string{"job-freed"},
		})
	}

	srv := httptest.NewServer(h)
	defer srv.Close()

	api := apiclient.New(srv.URL, "test-token", slog.Default())
	jobCh := make(chan *ClaimedJob, 10)
	state := NewState()

	// Start with all slots busy
	state.AddJob("busy-1", "test", func() {}, 1, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	go RunClaimLoop(ctx, api, "w1", jobCh, ClaimConfig{
		PollInterval: 50 * time.Millisecond,
		MaxSlots:     1,
		Capacity:     100,
	}, state, slog.Default())

	// Free a slot after a short delay
	time.Sleep(200 * time.Millisecond)
	state.RemoveJob("busy-1")

	select {
	case j := <-jobCh:
		if j.Job.JobID != "job-freed" {
			t.Errorf("expected job-freed, got %s", j.Job.JobID)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for job after slot freed")
	}
}

func TestRunClaimLoop_AckFailure_DropsJobs(t *testing.T) {
	h := &claimHandler{}
	claimCount := 0

	h.claimFunc = func(w http.ResponseWriter, r *http.Request) {
		claimCount++
		if claimCount == 1 {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"jobs": []json.RawMessage{makeJobJSON("job-drop")},
			})
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
	h.ackFunc = func(w http.ResponseWriter, r *http.Request) {
		// Return 500 to simulate ack failure
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"internal error"}`))
	}

	srv := httptest.NewServer(h)
	defer srv.Close()

	api := apiclient.New(srv.URL, "test-token", slog.Default())
	jobCh := make(chan *ClaimedJob, 10)
	state := NewState()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	RunClaimLoop(ctx, api, "w1", jobCh, ClaimConfig{
		PollInterval: 50 * time.Millisecond,
		MaxSlots:     5,
		Capacity:     100,
	}, state, slog.Default())

	// No jobs should come through since ack failed
	select {
	case j := <-jobCh:
		t.Errorf("expected no jobs after ack failure, got %s", j.Job.JobID)
	default:
		// good
	}
}

func TestRunClaimLoop_CapacityFreed_WakesBackpressure(t *testing.T) {
	// Test that RemoveJob immediately wakes the claim loop from back-pressure wait,
	// instead of waiting for the 500ms sleep to expire.
	h := &claimHandler{}
	var claimCount atomic.Int32

	h.claimFunc = func(w http.ResponseWriter, r *http.Request) {
		n := claimCount.Add(1)
		if n == 1 {
			// Return a job after capacity freed
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"jobs": []json.RawMessage{makeJobJSON("freed-job")},
			})
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
	h.ackFunc = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"acked":   1,
			"job_ids": []string{"freed-job"},
		})
	}

	srv := httptest.NewServer(h)
	defer srv.Close()

	api := apiclient.New(srv.URL, "test-token", slog.Default())
	jobCh := make(chan *ClaimedJob, 10)
	state := NewState()

	// Fill capacity completely — claim loop will be stuck in back-pressure wait
	state.AddJob("blocker", "test", func() {}, 50, 4)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go RunClaimLoop(ctx, api, "w1", jobCh, ClaimConfig{
		PollInterval: 50 * time.Millisecond,
		MaxSlots:     5,
		Capacity:     50,
	}, state, slog.Default())

	// Give the loop time to enter back-pressure wait
	time.Sleep(50 * time.Millisecond)

	// Free capacity — should wake immediately, not after 500ms
	start := time.Now()
	state.RemoveJob("blocker")

	select {
	case j := <-jobCh:
		elapsed := time.Since(start)
		if j.Job.JobID != "freed-job" {
			t.Errorf("expected freed-job, got %s", j.Job.JobID)
		}
		// Should respond much faster than 500ms (the old sleep duration)
		if elapsed > 400*time.Millisecond {
			t.Errorf("took %v to claim after capacity freed — expected < 400ms", elapsed)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for job after capacity freed")
	}
}

func TestRunClaimLoop_MetaParsed(t *testing.T) {
	h := &claimHandler{}
	var claimed bool

	// Job with meta embedded
	jobWithMeta := map[string]interface{}{
		"job_id": "meta-job",
		"steps":  map[string]interface{}{},
		"meta":   json.RawMessage(`{"type":"test","name":"Login Test","run_id":"r-123","test_id":42}`),
	}
	jobBytes, _ := json.Marshal(jobWithMeta)

	h.claimFunc = func(w http.ResponseWriter, r *http.Request) {
		if !claimed {
			claimed = true
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"jobs": []json.RawMessage{json.RawMessage(jobBytes)},
			})
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
	h.ackFunc = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"acked":   1,
			"job_ids": []string{"meta-job"},
		})
	}

	srv := httptest.NewServer(h)
	defer srv.Close()

	api := apiclient.New(srv.URL, "test-token", slog.Default())
	jobCh := make(chan *ClaimedJob, 10)
	state := NewState()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	go RunClaimLoop(ctx, api, "w1", jobCh, ClaimConfig{
		PollInterval: 50 * time.Millisecond,
		MaxSlots:     5,
		Capacity:     100,
	}, state, slog.Default())

	select {
	case j := <-jobCh:
		if j.Meta.Type != "test" {
			t.Errorf("expected meta type 'test', got %q", j.Meta.Type)
		}
		if j.Meta.Name != "Login Test" {
			t.Errorf("expected meta name 'Login Test', got %q", j.Meta.Name)
		}
		if j.Meta.RunID != "r-123" {
			t.Errorf("expected run_id 'r-123', got %q", j.Meta.RunID)
		}
		if j.Meta.TestID != 42 {
			t.Errorf("expected test_id 42, got %d", j.Meta.TestID)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for job with meta")
	}
}
