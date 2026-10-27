package dag

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// Handler executes an instruction with the given params and returns output data.
type Handler func(ctx context.Context, params json.RawMessage) (*StepResult, error)

// Executor runs a Job's step DAG with parallel execution, run conditions, and finally steps.
type Executor struct {
	handlers map[string]Handler
	progress ProgressCallback
}

// NewExecutor creates a DAG executor with the given instruction handlers.
func NewExecutor(handlers map[string]Handler, progress ProgressCallback) *Executor {
	if progress == nil {
		progress = func(string, string, StepStatus, *StepResult) {}
	}
	return &Executor{
		handlers: handlers,
		progress: progress,
	}
}

// Run executes the job's DAG and returns the final state bag with all step results.
func (e *Executor) Run(ctx context.Context, job *Job) (*StateBag, error) {
	return e.RunWithState(ctx, job, NewStateBag())
}

// RunWithState executes the job's DAG using the provided state bag.
// Use this when instruction handlers need to read/write the same state as the executor
// (e.g. when assert reads output from http_request).
func (e *Executor) RunWithState(ctx context.Context, job *Job, state *StateBag) (*StateBag, error) {
	// Apply job-level timeout
	if job.Timeout != "" {
		d, err := time.ParseDuration(job.Timeout)
		if err != nil {
			return nil, fmt.Errorf("invalid job timeout %q: %w", job.Timeout, err)
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d)
		defer cancel()
	}

	// Separate main steps from finally steps
	mainSteps, finallySteps, err := e.splitSteps(job)
	if err != nil {
		return nil, err
	}

	// Validate the main DAG (cycle detection, missing deps)
	if err := validateDAG(mainSteps); err != nil {
		return nil, err
	}

	// Execute main DAG
	e.executeDAG(ctx, mainSteps, state)

	// Execute finally steps sequentially (always, even if main DAG failed or ctx cancelled).
	// Use a generous timeout to prevent infinite hangs on cleanup, but never skip finally.
	finallyCtx, cancelFinally := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancelFinally()
	e.executeFinally(finallyCtx, finallySteps, job.Steps, state)

	return state, nil
}

// splitSteps separates main DAG steps from finally steps.
func (e *Executor) splitSteps(job *Job) (main map[string]*Step, finally []string, err error) {
	finallySet := make(map[string]bool)
	for _, name := range job.Finally {
		if _, exists := job.Steps[name]; !exists {
			return nil, nil, fmt.Errorf("finally step %q not found in steps", name)
		}
		finallySet[name] = true
	}

	main = make(map[string]*Step)
	for name, step := range job.Steps {
		if !finallySet[name] {
			main[name] = step
		}
	}

	return main, job.Finally, nil
}

// validateDAG checks for missing dependencies and cycles using Kahn's algorithm.
func validateDAG(steps map[string]*Step) error {
	// Check all dependencies exist
	for _, step := range steps {
		for _, dep := range step.DependsOn {
			if _, exists := steps[dep]; !exists {
				return fmt.Errorf("step depends on %q which is not in the DAG", dep)
			}
		}
	}

	// Kahn's algorithm: count in-degrees and do topological sort
	deg := make(map[string]int, len(steps))
	for name := range steps {
		deg[name] = len(steps[name].DependsOn)
	}

	var queue []string
	for name, d := range deg {
		if d == 0 {
			queue = append(queue, name)
		}
	}

	visited := 0
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		visited++

		for name, step := range steps {
			for _, dep := range step.DependsOn {
				if dep == current {
					deg[name]--
					if deg[name] == 0 {
						queue = append(queue, name)
					}
				}
			}
		}
	}

	if visited != len(steps) {
		return fmt.Errorf("DAG contains a cycle")
	}

	return nil
}

// executeDAG runs the main DAG using Kahn's level-by-level approach.
func (e *Executor) executeDAG(ctx context.Context, steps map[string]*Step, state *StateBag) {
	// Build in-degree map
	deg := make(map[string]int, len(steps))
	for name := range steps {
		deg[name] = len(steps[name].DependsOn)
	}

	// Track remaining steps
	remaining := make(map[string]bool, len(steps))
	for name := range steps {
		remaining[name] = true
	}

	for len(remaining) > 0 {
		// Collect ready steps (in-degree 0 and not yet executed)
		var ready []string
		for name := range remaining {
			if deg[name] == 0 {
				ready = append(ready, name)
			}
		}

		if len(ready) == 0 {
			// All remaining steps have unmet dependencies (predecessors failed/skipped)
			// Skip them all
			for name := range remaining {
				result := &StepResult{Status: StepSkipped}
				state.Set(name, result)
				e.progress(name, steps[name].Instruction, StepSkipped, result)
			}
			break
		}

		// Execute ready steps in parallel
		queuedAt := time.Now()
		var wg sync.WaitGroup
		for _, name := range ready {
			delete(remaining, name)
			wg.Add(1)
			go func(stepName string) {
				defer wg.Done()
				// Early context check — avoid starting steps if already cancelled
				if ctx.Err() != nil {
					result := &StepResult{Status: StepSkipped, Error: "job cancelled or timed out"}
					state.Set(stepName, result)
					e.progress(stepName, steps[stepName].Instruction, StepSkipped, result)
					return
				}
				e.executeStep(ctx, stepName, steps[stepName], state, queuedAt)
			}(name)
		}
		wg.Wait()

		// Check context cancellation
		if ctx.Err() != nil {
			// Skip all remaining steps
			for name := range remaining {
				result := &StepResult{Status: StepSkipped, Error: "job cancelled or timed out"}
				state.Set(name, result)
				e.progress(name, steps[name].Instruction, StepSkipped, result)
			}
			break
		}

		// Update in-degrees for remaining steps
		for name := range remaining {
			step := steps[name]
			count := 0
			for _, dep := range step.DependsOn {
				if !state.HasCompleted(dep) {
					count++
				}
			}
			deg[name] = count
		}
	}
}

// executeStep runs a single step, respecting run conditions, retry, and timeout.
// queuedAt is when the step became ready (deps satisfied).
func (e *Executor) executeStep(ctx context.Context, name string, step *Step, state *StateBag, queuedAt time.Time) {
	// Check run condition
	if !e.shouldRun(step, state) {
		result := &StepResult{Status: StepSkipped}
		state.Set(name, result)
		e.progress(name, step.Instruction, StepSkipped, result)
		return
	}

	// Look up handler
	handler, exists := e.handlers[step.Instruction]
	if !exists {
		result := &StepResult{
			Status: StepFailed,
			Error:  fmt.Sprintf("unknown instruction: %s", step.Instruction),
		}
		state.Set(name, result)
		e.progress(name, step.Instruction, StepFailed, result)
		return
	}

	// Apply step-level timeout
	stepCtx := ctx
	if step.Timeout != "" {
		d, err := time.ParseDuration(step.Timeout)
		if err == nil {
			var cancel context.CancelFunc
			stepCtx, cancel = context.WithTimeout(ctx, d)
			defer cancel()
		}
	}

	// Resolve params
	resolvedParams, err := ResolveParams(step.Params, state)
	if err != nil {
		result := &StepResult{
			Status: StepFailed,
			Error:  fmt.Sprintf("param resolution failed: %v", err),
		}
		state.Set(name, result)
		e.progress(name, step.Instruction, StepFailed, result)
		return
	}

	// Record timing: handler starts now
	startedAt := time.Now()
	e.progress(name, step.Instruction, StepRunning, nil)

	// Execute with retry
	maxAttempts := 1
	retryInterval := time.Second
	if step.Retry != nil && step.Retry.Max > 0 {
		maxAttempts = step.Retry.Max + 1 // max retries + initial attempt
		if step.Retry.Interval != "" {
			if d, err := time.ParseDuration(step.Retry.Interval); err == nil {
				retryInterval = d
			}
		}
	}

	var result *StepResult
	start := startedAt

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-stepCtx.Done():
				result = &StepResult{
					Status:   StepFailed,
					Error:    "step timed out during retry",
					Duration: time.Since(start),
				}
				state.Set(name, result)
				e.progress(name, step.Instruction, StepFailed, result)
				return
			case <-time.After(retryInterval):
			}
		}

		result, err = handler(stepCtx, resolvedParams)
		if err != nil {
			result = &StepResult{
				Status:   StepFailed,
				Error:    err.Error(),
				Duration: time.Since(start),
			}
			continue // retry
		}

		if result == nil {
			result = &StepResult{Status: StepCompleted}
		}

		if result.Status == "" {
			result.Status = StepCompleted
		}
		result.Duration = time.Since(start)

		if result.Status != StepFailed {
			break // success
		}
		// result.Status == StepFailed → retry
	}

	// Set timing
	completedAt := time.Now()
	result.Timing = StepTiming{
		QueuedAt:    queuedAt,
		StartedAt:   startedAt,
		CompletedAt: completedAt,
		WaitMs:      startedAt.Sub(queuedAt).Milliseconds(),
		ExecMs:      completedAt.Sub(startedAt).Milliseconds(),
	}

	state.Set(name, result)
	e.progress(name, step.Instruction, result.Status, result)
}

// shouldRun checks whether a step should execute based on its run condition and the state.
func (e *Executor) shouldRun(step *Step, state *StateBag) bool {
	rc := step.EffectiveRunCondition()
	switch rc {
	case RunAlways:
		return true
	case RunOnFail:
		return state.Failed()
	case RunOnSuccess:
		// Skip if any direct dependency failed or was skipped.
		// Only checks direct deps - unrelated step failures don't affect this step.
		for _, dep := range step.DependsOn {
			r := state.Get(dep)
			if r != nil && (r.Status == StepFailed || r.Status == StepSkipped) {
				return false
			}
		}
		return true
	default:
		return true
	}
}

// executeFinally runs finally steps sequentially, always.
func (e *Executor) executeFinally(ctx context.Context, finallyNames []string, allSteps map[string]*Step, state *StateBag) {
	for _, name := range finallyNames {
		step, exists := allSteps[name]
		if !exists {
			continue
		}

		handler, exists := e.handlers[step.Instruction]
		if !exists {
			result := &StepResult{
				Status: StepFailed,
				Error:  fmt.Sprintf("unknown instruction: %s", step.Instruction),
			}
			state.Set(name, result)
			e.progress(name, step.Instruction, StepFailed, result)
			continue
		}

		resolvedParams, err := ResolveParams(step.Params, state)
		if err != nil {
			result := &StepResult{
				Status: StepFailed,
				Error:  fmt.Sprintf("param resolution failed: %v", err),
			}
			state.Set(name, result)
			e.progress(name, step.Instruction, StepFailed, result)
			continue
		}

		e.progress(name, step.Instruction, StepRunning, nil)
		start := time.Now()

		result, err := handler(ctx, resolvedParams)
		if err != nil {
			result = &StepResult{
				Status:   StepFailed,
				Error:    err.Error(),
				Duration: time.Since(start),
			}
		} else if result == nil {
			result = &StepResult{Status: StepCompleted}
		}
		if result.Status == "" {
			result.Status = StepCompleted
		}
		result.Duration = time.Since(start)

		state.Set(name, result)
		e.progress(name, step.Instruction, result.Status, result)
	}
}
