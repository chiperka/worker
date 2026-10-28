package workerloop

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"chiperka-worker/internal/apiclient"
	"chiperka-worker/internal/dag"
	"chiperka-worker/internal/docker"
	"chiperka-worker/internal/instruction"
)

// ExecuteJob runs a single job through the DAG executor and submits the result.
// This is the core execution function called by each executor goroutine.
//
// Lifecycle:
//  1. Create log collector (3s flush, server-adjustable)
//  2. Build instruction context with Docker manager, API client, logs
//  3. Execute DAG (parallel steps, run conditions, finally blocks)
//  4. Stop log collector (final flush)
//  5. Build result from step outputs
//  6. Submit result with idempotency key (5 retries)
func ExecuteJob(ctx context.Context, api *apiclient.Client, job *dag.Job, meta *JobMeta, executorID, workerID string, logger *slog.Logger) {
	startTime := time.Now()

	// Per-job logger with correlation attributes
	jlog := logger.With(
		"job_id", job.JobID,
		"executor", executorID,
		"test_name", meta.Name,
	)

	// Panic recovery — ensure we never crash the executor goroutine
	defer func() {
		if r := recover(); r != nil {
			jlog.Error("executor panic recovered",
				"panic", fmt.Sprintf("%v", r),
				"stack", string(debug.Stack()),
			)
		}
	}()

	// Job-scoped token for authenticated API calls (falls back to worker token)
	jobToken := job.Token
	if jobToken == "" {
		jlog.Warn("no job-scoped token, falling back to worker token")
	}

	// Log collector: batches logs every 3s, server can adjust interval
	logSender := &apiclient.LogSenderAdapter{Client: api, Token: jobToken}
	logCollector := instruction.NewLogCollector(logSender, job.JobID, startTime)
	defer logCollector.Stop()

	// Shared state bag between DAG executor and instruction handlers
	stateBag := dag.NewStateBag()

	ictx := &instruction.Context{
		State:     stateBag,
		APIClient: &apiclient.InstructionAdapter{Client: api},
		Logs:      logCollector,
		JobID:     job.JobID,
		JobToken:  jobToken,
		WorkerID:  workerID,
	}

	registry := instruction.DefaultRegistry()
	dagHandlers := registry.BuildDAGHandlers(ictx)

	// Progress callback: report step transitions to cloud + flush logs on completion/failure
	progress := func(stepName, instr string, status dag.StepStatus, result *dag.StepResult) {
		var durationMs int64
		var errMsg string
		if result != nil {
			durationMs = result.Duration.Milliseconds()
			errMsg = result.Error
		}
		api.ReportProgress(ctx, job.JobID, jobToken, workerID, stepName, string(status), durationMs, errMsg)

		// Force flush logs on step completion/failure for timely delivery
		if status == dag.StepCompleted || status == dag.StepFailed {
			logCollector.Flush()
		}
	}

	ictx.Log("info", "job_started", "", fmt.Sprintf("Job %s started on %s", job.JobID, executorID), "")
	jlog.Info("job started")

	// Image pulls in this job may use the registry credentials the hub attached to it
	creds := make([]docker.RegistryCredential, 0, len(job.RegistryAuth))
	for _, ra := range job.RegistryAuth {
		creds = append(creds, docker.RegistryCredential{Host: ra.Host, Username: ra.Username, Password: ra.Password})
	}
	ctx = docker.WithRegistryCredentials(ctx, creds)

	// Execute DAG
	executor := dag.NewExecutor(dagHandlers, progress)
	resultState, err := executor.RunWithState(ctx, job, stateBag)

	// Stop log collector (final flush) before submitting result
	logCollector.Stop()

	// Build and submit result
	workResult := BuildJobResult(resultState, workerID, startTime)
	if err != nil {
		workResult["status"] = "error"
		workResult["error_message"] = fmt.Sprintf("DAG execution error: %v", err)
	}

	// Idempotency key: deterministic per job execution attempt
	// Prevents duplicate data if completion is retried after a network error
	idempotencyKey := fmt.Sprintf("%x", sha256.Sum256([]byte(
		fmt.Sprintf("%s:%s:%d", job.JobID, workerID, startTime.UnixNano()),
	)))

	// Submit result (critical — job stays stuck in "running" if this fails)
	submitCtx, submitCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer submitCancel()

	if submitErr := api.CompleteJob(submitCtx, job.JobID, jobToken, idempotencyKey, workResult); submitErr != nil {
		jlog.Error("FAILED to submit job result", "error", submitErr)
	} else {
		jlog.Info("job completed",
			"status", workResult["status"],
			"duration_s", fmt.Sprintf("%.2f", time.Since(startTime).Seconds()),
		)
	}
}

// BuildJobResult constructs the completion payload from DAG step results.
// The worker sends raw step outputs — cloud is responsible for mapping them
// to test_results, assertion_results, http_exchanges, etc.
func BuildJobResult(state *dag.StateBag, workerID string, startTime time.Time) map[string]interface{} {
	status := "passed"
	errorMessage := ""

	// Assert step determines pass/fail
	if ar := state.Get("assert"); ar != nil && ar.Status == dag.StepFailed {
		status = "failed"
		errorMessage = ar.Error
		// Extract first failing assertion message for human-readable error
		if results, ok := ar.Data["results"].([]interface{}); ok {
			for _, r := range results {
				if m, ok := r.(map[string]interface{}); ok {
					if passed, ok := m["passed"].(bool); ok && !passed {
						if msg, ok := m["message"].(string); ok {
							errorMessage = msg
						}
						break
					}
				}
			}
		}
	}

	// Infrastructure step failure (not assert/upload/cleanup) = error
	if status == "passed" && state.Failed() {
		for name, r := range state.AllResults() {
			if r.Status == dag.StepFailed && name != "assert" && name != "upload_artifacts" && name != "cleanup" {
				status = "error"
				errorMessage = fmt.Sprintf("Step %s failed: %s", name, r.Error)
				break
			}
		}
	}

	// Build step_results: raw output of every step
	stepResults := make(map[string]interface{}, len(state.AllResults()))
	for name, r := range state.AllResults() {
		step := map[string]interface{}{
			"status":   string(r.Status),
			"duration": r.Duration.Milliseconds(),
		}
		if r.Error != "" {
			step["error"] = r.Error
		}
		if r.Data != nil {
			step["data"] = r.Data
		}
		if !r.Timing.StartedAt.IsZero() {
			step["timing"] = map[string]interface{}{
				"queued_at":    r.Timing.QueuedAt.UnixMilli(),
				"started_at":   r.Timing.StartedAt.UnixMilli(),
				"completed_at": r.Timing.CompletedAt.UnixMilli(),
				"wait_ms":      r.Timing.WaitMs,
				"exec_ms":      r.Timing.ExecMs,
			}
		}
		stepResults[name] = step
	}

	return map[string]interface{}{
		"worker_id":    workerID,
		"status":       status,
		"error_message": errorMessage,
		"duration_ms":  time.Since(startTime).Milliseconds(),
		"step_results": stepResults,
	}
}
