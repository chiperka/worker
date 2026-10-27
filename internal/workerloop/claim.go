package workerloop

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync/atomic"
	"time"

	"chiperka-worker/internal/apiclient"
	"chiperka-worker/internal/dag"
)

const (
	// LongPollTimeoutMs is the long-poll timeout for job claiming.
	// Server max is 30s, HTTP client timeout is 35s — 25s gives enough margin.
	LongPollTimeoutMs = 25000
)

// JobMeta carries metadata for mapping job results back to cloud entities.
type JobMeta struct {
	Type          string `json:"type"`
	TestID        int64  `json:"test_id,omitempty"`
	RunID         string `json:"run_id,omitempty"`
	SuiteID       int64  `json:"suite_id,omitempty"`
	UUID          string `json:"uuid,omitempty"`
	Name          string `json:"name,omitempty"`
	SuiteName     string `json:"suite_name,omitempty"`
	SuiteFilePath string `json:"suite_file_path,omitempty"`
	ExecutionID   int64  `json:"execution_id,omitempty"`
	ProjectID     int64  `json:"project_id,omitempty"`
}

// ClaimedJob carries a parsed job with its metadata, ready for execution.
type ClaimedJob struct {
	Job  *dag.Job
	Meta JobMeta
}

// ClaimConfig holds configuration for the claim loop.
type ClaimConfig struct {
	PollInterval  time.Duration
	MaxSlots      int // max concurrent tests (hard limit)
	Capacity      int // max concurrent weight
	MaxContainers int // max Docker containers (0 = unlimited)
}

// RunClaimLoop is the single claim goroutine for the entire worker.
// It uses long polling to efficiently wait for jobs, claims in batches
// (up to free executor slots), and sends only acked jobs to the executor channel.
//
// Design notes (mirrors GitLab Runner / Sentry relay patterns):
//   - Single claimer per worker reduces API load from N*concurrency to 1 request
//   - Long-poll eliminates polling overhead when no jobs are available
//   - Batch claiming + ack prevents race conditions with the health monitor
//   - CPU backpressure prevents overloading the host machine
func RunClaimLoop(ctx context.Context, api *apiclient.Client, workerID string, jobCh chan<- *ClaimedJob, cfg ClaimConfig, state *State, logger *slog.Logger) {
	consecutiveErrors := 0

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Back-pressure: wait if all executor slots are busy
		if state.Count() >= cfg.MaxSlots {
			select {
			case <-ctx.Done():
				return
			case <-state.CapacityFreed():
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}

		// Back-pressure: wait if weight capacity is fully used
		availableCapacity := cfg.Capacity - state.UsedWeight()
		if availableCapacity <= 0 {
			select {
			case <-ctx.Done():
				return
			case <-state.CapacityFreed():
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}

		// Claim up to the number of free executor slots
		freeSlots := cfg.MaxSlots - state.Count()
		if freeSlots <= 0 {
			freeSlots = 1
		}

		// Use a context that cancels when capacity is freed (job completed),
		// so the long-poll returns immediately and re-evaluates with new capacity.
		claimCtx, claimCancel := context.WithCancel(ctx)
		var capacityChanged atomic.Bool
		go func() {
			select {
			case <-state.CapacityFreed():
				capacityChanged.Store(true)
				claimCancel()
			case <-claimCtx.Done():
			}
		}()

		rawJobs, err := api.ClaimJobs(claimCtx, workerID, freeSlots, LongPollTimeoutMs, availableCapacity)
		claimCancel() // ensure goroutine exits

		// If the claim was cancelled because capacity freed up, retry immediately
		// with recalculated capacity — this is not an error.
		if err != nil && capacityChanged.Load() && ctx.Err() == nil {
			logger.Debug("claim interrupted, capacity freed — retrying with new capacity")
			continue
		}

		if err != nil {
			consecutiveErrors++
			wait := apiclient.Backoff(cfg.PollInterval, consecutiveErrors, 30*time.Second)
			logger.Error("claim failed",
				"error", err,
				"consecutive_errors", consecutiveErrors,
				"retry_in", wait,
			)
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
			continue
		}
		consecutiveErrors = 0

		if len(rawJobs) == 0 {
			continue // long-poll timeout, reconnect
		}

		// Parse jobs from raw JSON
		jobs := make([]*dag.Job, 0, len(rawJobs))
		for _, raw := range rawJobs {
			var job dag.Job
			if err := json.Unmarshal(raw, &job); err != nil {
				logger.Error("failed to parse claimed job", "error", err)
				continue
			}
			jobs = append(jobs, &job)
		}

		if len(jobs) == 0 {
			continue
		}

		// Phase 2: Acknowledge receipt — only acked jobs should be executed
		jobIDs := make([]string, len(jobs))
		for i, j := range jobs {
			jobIDs[i] = j.JobID
		}

		ackedIDs, ackErr := api.AckJobs(ctx, workerID, jobIDs)
		if ackErr != nil {
			logger.Error("ack failed, dropping jobs",
				"error", ackErr,
				"job_count", len(jobs),
			)
			continue
		}

		if len(ackedIDs) < len(jobs) {
			logger.Warn("partial ack",
				"acked", len(ackedIDs),
				"claimed", len(jobs),
			)
		}

		// Build acked set for O(1) lookup
		ackedSet := make(map[string]bool, len(ackedIDs))
		for _, id := range ackedIDs {
			ackedSet[id] = true
		}

		// Send only acked jobs to executor pool
		for _, job := range jobs {
			if !ackedSet[job.JobID] {
				continue
			}

			var meta JobMeta
			if len(job.Meta) > 0 {
				if err := json.Unmarshal(job.Meta, &meta); err != nil {
					logger.Warn("failed to parse job meta",
						"job_id", job.JobID,
						"error", err,
					)
				}
			}

			select {
			case jobCh <- &ClaimedJob{Job: job, Meta: meta}:
			case <-ctx.Done():
				return
			}
		}
	}
}
