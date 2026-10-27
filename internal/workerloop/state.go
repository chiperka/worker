// Package workerloop implements the worker's claim-execute-complete lifecycle.
// It contains the claim loop, executor pool, job execution, and worker state tracking.
package workerloop

import (
	"context"
	"sync"
)

// jobEntry tracks a single active job with its cancellation handle.
type jobEntry struct {
	jobType    string             // "test" or "execution"
	cancel     context.CancelFunc // cancels this job's context
	weight     int                // resource weight consumed by this job
	containers int                // number of Docker containers
}

// State tracks active jobs for heartbeat reporting, concurrency control,
// and per-job cancellation. Thread-safe — accessed by claim loop,
// executor goroutines, and heartbeat goroutine.
type State struct {
	mu             sync.Mutex
	activeJobs     map[string]*jobEntry
	usedWeight     int
	usedContainers int
	capacityFreed  chan struct{} // signaled when a job is removed (capacity freed)
}

// NewState creates an empty worker state.
func NewState() *State {
	return &State{
		activeJobs:    make(map[string]*jobEntry),
		capacityFreed: make(chan struct{}, 1),
	}
}

// AddJob registers a job as active with its cancellation function and resource usage.
func (s *State) AddJob(jobID, jobType string, cancel context.CancelFunc, weight, containers int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activeJobs[jobID] = &jobEntry{jobType: jobType, cancel: cancel, weight: weight, containers: containers}
	s.usedWeight += weight
	s.usedContainers += containers
}

// RemoveJob marks a job as no longer active and releases its resources.
// Signals the claim loop to re-evaluate available capacity immediately.
func (s *State) RemoveJob(jobID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry, ok := s.activeJobs[jobID]; ok {
		s.usedWeight -= entry.weight
		s.usedContainers -= entry.containers
		delete(s.activeJobs, jobID)
		// Non-blocking signal — if channel already has a pending signal, skip.
		select {
		case s.capacityFreed <- struct{}{}:
		default:
		}
	}
}

// CapacityFreed returns a channel that receives a signal when a job is removed.
// Used by the claim loop to cancel in-flight long-polls immediately.
func (s *State) CapacityFreed() <-chan struct{} {
	return s.capacityFreed
}

// CancelJob cancels a specific running job by calling its context cancel function.
// Returns true if the job was found and cancelled, false if not found (already finished).
func (s *State) CancelJob(jobID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry, ok := s.activeJobs[jobID]; ok && entry.cancel != nil {
		entry.cancel()
		return true
	}
	return false
}

// Count returns the number of currently active jobs.
func (s *State) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.activeJobs)
}

// UsedWeight returns the total weight of all active jobs.
func (s *State) UsedWeight() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.usedWeight
}

// UsedContainers returns the total container count of all active jobs.
func (s *State) UsedContainers() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.usedContainers
}

// FirstJobID returns any active job ID for heartbeat reporting.
// Returns "" if no jobs are active.
func (s *State) FirstJobID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.activeJobs {
		return id
	}
	return ""
}
