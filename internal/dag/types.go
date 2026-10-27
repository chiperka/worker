// Package dag implements a generic DAG (directed acyclic graph) executor.
// Jobs consist of named steps with dependency edges. The executor runs steps
// in topological order, parallelizing independent steps, and supports
// run conditions (on_success/always/on_fail), retry, timeout, and finally blocks.
//
// The executor is instruction-agnostic - it delegates step execution to
// pluggable Handler functions and tracks results in a thread-safe StateBag.
package dag

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"
)

// Job represents a unit of work sent from cloud to worker.
// It contains a DAG of steps that the worker executes.
type Job struct {
	JobID          string           `json:"job_id"`
	Token          string           `json:"token"`
	Timeout        string           `json:"timeout,omitempty"`
	Steps          map[string]*Step `json:"steps"`
	Finally        []string         `json:"finally,omitempty"`
	Meta           json.RawMessage  `json:"meta,omitempty"`
	Weight         int              `json:"weight"`
	ContainerCount int              `json:"container_count"`
}

// Step is a single instruction in the job DAG.
type Step struct {
	Instruction string          `json:"instruction"`
	DependsOn   []string        `json:"depends_on,omitempty"`
	Params      json.RawMessage `json:"params"`
	Run         RunCondition    `json:"run,omitempty"`
	Retry       *RetryPolicy    `json:"retry,omitempty"`
	Timeout     string          `json:"timeout,omitempty"`
}

// RunCondition determines when a step should execute relative to the DAG's failure state.
type RunCondition string

const (
	// RunOnSuccess executes only if no prior step has failed (default).
	RunOnSuccess RunCondition = "on_success"
	// RunAlways executes regardless of failure state.
	RunAlways RunCondition = "always"
	// RunOnFail executes only if a prior step has failed.
	RunOnFail RunCondition = "on_fail"
)

// EffectiveRunCondition returns the run condition, defaulting to RunOnSuccess.
func (s *Step) EffectiveRunCondition() RunCondition {
	if s.Run == "" {
		return RunOnSuccess
	}
	return s.Run
}

// RetryPolicy configures retry behavior for a step.
type RetryPolicy struct {
	Max      int    `json:"max"`
	Interval string `json:"interval,omitempty"`
}

// StepResult holds the output of an executed step.
type StepResult struct {
	Data     map[string]interface{} `json:"data,omitempty"`
	Error    string                 `json:"error,omitempty"`
	Duration time.Duration          `json:"duration"`
	Status   StepStatus             `json:"status"`
	Timing   StepTiming             `json:"timing"`
}

// StepTiming captures when a step was queued, started, and completed.
// Used for benchmarking and Gantt chart visualization on the cloud side.
type StepTiming struct {
	QueuedAt    time.Time `json:"queued_at"`    // deps satisfied, step ready to run
	StartedAt   time.Time `json:"started_at"`   // handler execution began
	CompletedAt time.Time `json:"completed_at"` // handler execution ended
	WaitMs      int64     `json:"wait_ms"`      // QueuedAt → StartedAt
	ExecMs      int64     `json:"exec_ms"`      // StartedAt → CompletedAt
}

// StepStatus represents the execution status of a step.
type StepStatus string

const (
	StepPending   StepStatus = "pending"
	StepRunning   StepStatus = "running"
	StepCompleted StepStatus = "completed"
	StepFailed    StepStatus = "failed"
	StepSkipped   StepStatus = "skipped"
)

// StateBag is a thread-safe store of step outputs.
// It tracks which steps have completed, their outputs, and the overall failure state.
//
// The failed flag uses atomic.Bool to avoid RWMutex contention in the hot path —
// during parallel step execution, Failed() is called frequently by the executor
// and shouldRun() checks.
type StateBag struct {
	mu      sync.RWMutex
	results map[string]*StepResult
	failed  atomic.Bool
}

// NewStateBag creates a new empty state bag.
func NewStateBag() *StateBag {
	return &StateBag{
		results: make(map[string]*StepResult),
	}
}

// Set stores a step's result and updates the failure flag.
func (s *StateBag) Set(name string, result *StepResult) {
	s.mu.Lock()
	s.results[name] = result
	s.mu.Unlock()
	if result.Status == StepFailed {
		s.failed.Store(true)
	}
}

// Get returns a step's result, or nil if not yet completed.
func (s *StateBag) Get(name string) *StepResult {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.results[name]
}

// GetData returns a specific field from a step's output data.
func (s *StateBag) GetData(stepName, field string) (interface{}, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r := s.results[stepName]
	if r == nil || r.Data == nil {
		return nil, false
	}
	v, ok := r.Data[field]
	return v, ok
}

// Failed returns true if any step has failed. Lock-free via atomic.
func (s *StateBag) Failed() bool {
	return s.failed.Load()
}

// HasCompleted returns true if the named step has a result (completed, failed, or skipped).
func (s *StateBag) HasCompleted(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.results[name] != nil
}

// StepFailed returns true if the named step specifically failed.
func (s *StateBag) StepFailed(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r := s.results[name]
	return r != nil && r.Status == StepFailed
}

// AllResults returns a copy of all step results.
func (s *StateBag) AllResults() map[string]*StepResult {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]*StepResult, len(s.results))
	for k, v := range s.results {
		out[k] = v
	}
	return out
}

// ProgressCallback is called by the executor when a step changes state.
type ProgressCallback func(stepName string, instruction string, status StepStatus, result *StepResult)
