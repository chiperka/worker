package dag

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

func mockHandler(order *[]string, mu *sync.Mutex, name string, data map[string]interface{}) Handler {
	return func(ctx context.Context, params json.RawMessage) (*StepResult, error) {
		mu.Lock()
		*order = append(*order, name)
		mu.Unlock()
		return &StepResult{Status: StepCompleted, Data: data}, nil
	}
}

func failHandler(msg string) Handler {
	return func(ctx context.Context, params json.RawMessage) (*StepResult, error) {
		return nil, fmt.Errorf("%s", msg)
	}
}

func slowHandler(d time.Duration) Handler {
	return func(ctx context.Context, params json.RawMessage) (*StepResult, error) {
		select {
		case <-time.After(d):
			return &StepResult{Status: StepCompleted}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func noop() Handler {
	return func(ctx context.Context, p json.RawMessage) (*StepResult, error) {
		return &StepResult{Status: StepCompleted}, nil
	}
}

func trackHandler(order *[]string, mu *sync.Mutex, name string) Handler {
	return func(ctx context.Context, p json.RawMessage) (*StepResult, error) {
		mu.Lock()
		*order = append(*order, name)
		mu.Unlock()
		return &StepResult{Status: StepCompleted}, nil
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Basic DAG tests
// ──────────────────────────────────────────────────────────────────────────────

func TestExecutor_LinearDAG(t *testing.T) {
	var order []string
	var mu sync.Mutex

	handler := func(ctx context.Context, params json.RawMessage) (*StepResult, error) {
		var p map[string]string
		json.Unmarshal(params, &p)
		mu.Lock()
		order = append(order, p["name"])
		mu.Unlock()
		return &StepResult{Status: StepCompleted}, nil
	}

	job := &Job{
		Steps: map[string]*Step{
			"a": {Instruction: "step", Params: json.RawMessage(`{"name":"a"}`)},
			"b": {Instruction: "step", DependsOn: []string{"a"}, Params: json.RawMessage(`{"name":"b"}`)},
			"c": {Instruction: "step", DependsOn: []string{"b"}, Params: json.RawMessage(`{"name":"c"}`)},
		},
	}

	exec := NewExecutor(map[string]Handler{"step": handler}, nil)
	state, err := exec.Run(context.Background(), job)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(order) != 3 || order[0] != "a" || order[1] != "b" || order[2] != "c" {
		t.Errorf("expected [a b c], got %v", order)
	}
	for _, name := range []string{"a", "b", "c"} {
		r := state.Get(name)
		if r == nil || r.Status != StepCompleted {
			t.Errorf("step %s: expected completed, got %v", name, r)
		}
	}
}

func TestExecutor_ParallelExecution(t *testing.T) {
	var running int32
	var peak int32

	handler := func(ctx context.Context, params json.RawMessage) (*StepResult, error) {
		cur := atomic.AddInt32(&running, 1)
		// Update peak
		for {
			old := atomic.LoadInt32(&peak)
			if cur <= old {
				break
			}
			if atomic.CompareAndSwapInt32(&peak, old, cur) {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
		atomic.AddInt32(&running, -1)
		return &StepResult{Status: StepCompleted}, nil
	}

	job := &Job{
		Steps: map[string]*Step{
			"a": {Instruction: "work", Params: json.RawMessage(`{}`)},
			"b": {Instruction: "work", Params: json.RawMessage(`{}`)},
			"c": {Instruction: "work", Params: json.RawMessage(`{}`)},
			"d": {Instruction: "work", DependsOn: []string{"a", "b", "c"}, Params: json.RawMessage(`{}`)},
		},
	}

	exec := NewExecutor(map[string]Handler{"work": handler}, nil)
	_, err := exec.Run(context.Background(), job)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if atomic.LoadInt32(&peak) < 2 {
		t.Errorf("expected parallel execution of at least 2 steps, peak was %d", atomic.LoadInt32(&peak))
	}
}

func TestExecutor_CycleDetection(t *testing.T) {
	job := &Job{
		Steps: map[string]*Step{
			"a": {Instruction: "x", DependsOn: []string{"c"}, Params: json.RawMessage(`{}`)},
			"b": {Instruction: "x", DependsOn: []string{"a"}, Params: json.RawMessage(`{}`)},
			"c": {Instruction: "x", DependsOn: []string{"b"}, Params: json.RawMessage(`{}`)},
		},
	}
	exec := NewExecutor(map[string]Handler{"x": noop()}, nil)
	_, err := exec.Run(context.Background(), job)
	if err == nil {
		t.Fatal("expected cycle detection error")
	}
}

func TestExecutor_MissingDependency(t *testing.T) {
	job := &Job{
		Steps: map[string]*Step{
			"a": {Instruction: "x", DependsOn: []string{"nonexistent"}, Params: json.RawMessage(`{}`)},
		},
	}
	exec := NewExecutor(map[string]Handler{"x": noop()}, nil)
	_, err := exec.Run(context.Background(), job)
	if err == nil {
		t.Fatal("expected missing dependency error")
	}
}

func TestExecutor_EmptyJob(t *testing.T) {
	exec := NewExecutor(map[string]Handler{}, nil)
	state, err := exec.Run(context.Background(), &Job{Steps: map[string]*Step{}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if state.Failed() {
		t.Error("empty job should not be failed")
	}
}

func TestExecutor_DiamondDAG(t *testing.T) {
	var order []string
	var mu sync.Mutex

	handler := func(ctx context.Context, p json.RawMessage) (*StepResult, error) {
		var params map[string]string
		json.Unmarshal(p, &params)
		mu.Lock()
		order = append(order, params["name"])
		mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		return &StepResult{Status: StepCompleted}, nil
	}

	job := &Job{
		Steps: map[string]*Step{
			"a": {Instruction: "t", Params: json.RawMessage(`{"name":"a"}`)},
			"b": {Instruction: "t", DependsOn: []string{"a"}, Params: json.RawMessage(`{"name":"b"}`)},
			"c": {Instruction: "t", DependsOn: []string{"a"}, Params: json.RawMessage(`{"name":"c"}`)},
			"d": {Instruction: "t", DependsOn: []string{"b", "c"}, Params: json.RawMessage(`{"name":"d"}`)},
		},
	}

	exec := NewExecutor(map[string]Handler{"t": handler}, nil)
	state, _ := exec.Run(context.Background(), job)

	if order[0] != "a" {
		t.Errorf("expected a first, got %v", order)
	}
	if order[3] != "d" {
		t.Errorf("expected d last, got %v", order)
	}
	for _, name := range []string{"a", "b", "c", "d"} {
		if state.Get(name).Status != StepCompleted {
			t.Errorf("step %s should be completed", name)
		}
	}
}

func TestExecutor_UnknownInstruction(t *testing.T) {
	job := &Job{
		Steps: map[string]*Step{
			"a": {Instruction: "nonexistent", Params: json.RawMessage(`{}`)},
		},
	}
	exec := NewExecutor(map[string]Handler{}, nil)
	state, _ := exec.Run(context.Background(), job)
	r := state.Get("a")
	if r == nil || r.Status != StepFailed {
		t.Errorf("expected step to fail for unknown instruction, got %v", r)
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Run conditions
// ──────────────────────────────────────────────────────────────────────────────

func TestExecutor_RunCondition_OnSuccess(t *testing.T) {
	handlers := map[string]Handler{"ok": noop(), "fail": failHandler("boom")}
	job := &Job{
		Steps: map[string]*Step{
			"a": {Instruction: "fail", Params: json.RawMessage(`{}`)},
			"b": {Instruction: "ok", DependsOn: []string{"a"}, Params: json.RawMessage(`{}`), Run: RunOnSuccess},
		},
	}
	state, _ := NewExecutor(handlers, nil).Run(context.Background(), job)
	if state.Get("b").Status != StepSkipped {
		t.Error("b should be skipped when dependency fails")
	}
}

func TestExecutor_RunCondition_Always(t *testing.T) {
	handlers := map[string]Handler{"ok": noop(), "fail": failHandler("boom")}
	job := &Job{
		Steps: map[string]*Step{
			"a": {Instruction: "fail", Params: json.RawMessage(`{}`)},
			"b": {Instruction: "ok", DependsOn: []string{"a"}, Params: json.RawMessage(`{}`), Run: RunAlways},
		},
	}
	state, _ := NewExecutor(handlers, nil).Run(context.Background(), job)
	if state.Get("b").Status != StepCompleted {
		t.Error("b should run (run=always)")
	}
}

func TestExecutor_RunCondition_OnFail(t *testing.T) {
	var ran bool
	handlers := map[string]Handler{
		"ok":   noop(),
		"fail": failHandler("boom"),
		"track": func(ctx context.Context, p json.RawMessage) (*StepResult, error) {
			ran = true
			return &StepResult{Status: StepCompleted}, nil
		},
	}

	// No failure → on_fail should be skipped
	job1 := &Job{
		Steps: map[string]*Step{
			"a": {Instruction: "ok", Params: json.RawMessage(`{}`)},
			"b": {Instruction: "track", DependsOn: []string{"a"}, Params: json.RawMessage(`{}`), Run: RunOnFail},
		},
	}
	state1, _ := NewExecutor(handlers, nil).Run(context.Background(), job1)
	if ran {
		t.Error("on_fail step should not run when no failure")
	}
	if state1.Get("b").Status != StepSkipped {
		t.Error("b should be skipped")
	}

	// With failure → on_fail should run
	ran = false
	job2 := &Job{
		Steps: map[string]*Step{
			"a": {Instruction: "fail", Params: json.RawMessage(`{}`)},
			"b": {Instruction: "track", DependsOn: []string{"a"}, Params: json.RawMessage(`{}`), Run: RunOnFail},
		},
	}
	state2, _ := NewExecutor(handlers, nil).Run(context.Background(), job2)
	if !ran {
		t.Error("on_fail step should run when failure occurred")
	}
	if state2.Get("b").Status != StepCompleted {
		t.Error("b should complete")
	}
}

func TestExecutor_RunCondition_IndependentBranches(t *testing.T) {
	var bRan, dRan bool
	handlers := map[string]Handler{
		"ok":   noop(),
		"fail": failHandler("boom"),
		"track_b": func(ctx context.Context, p json.RawMessage) (*StepResult, error) {
			bRan = true
			return &StepResult{Status: StepCompleted}, nil
		},
		"track_d": func(ctx context.Context, p json.RawMessage) (*StepResult, error) {
			dRan = true
			return &StepResult{Status: StepCompleted}, nil
		},
	}

	// A→B independent of C→D. C fails. B should still run.
	job := &Job{
		Steps: map[string]*Step{
			"a": {Instruction: "ok", Params: json.RawMessage(`{}`)},
			"b": {Instruction: "track_b", DependsOn: []string{"a"}, Params: json.RawMessage(`{}`)},
			"c": {Instruction: "fail", Params: json.RawMessage(`{}`)},
			"d": {Instruction: "track_d", DependsOn: []string{"c"}, Params: json.RawMessage(`{}`)},
		},
	}

	state, _ := NewExecutor(handlers, nil).Run(context.Background(), job)

	if !bRan {
		t.Error("B should run - its dependency A succeeded")
	}
	if state.Get("b").Status != StepCompleted {
		t.Error("B should be completed")
	}
	if dRan {
		t.Error("D should NOT run - its dependency C failed")
	}
	if state.Get("d").Status != StepSkipped {
		t.Error("D should be skipped")
	}
}

func TestExecutor_RunCondition_SkippedDependencySkipsDownstream(t *testing.T) {
	// A fails → B skipped (on_success) → C should also be skipped (dep B was skipped)
	handlers := map[string]Handler{"ok": noop(), "fail": failHandler("boom")}
	job := &Job{
		Steps: map[string]*Step{
			"a": {Instruction: "fail", Params: json.RawMessage(`{}`)},
			"b": {Instruction: "ok", DependsOn: []string{"a"}, Params: json.RawMessage(`{}`)},
			"c": {Instruction: "ok", DependsOn: []string{"b"}, Params: json.RawMessage(`{}`)},
		},
	}
	state, _ := NewExecutor(handlers, nil).Run(context.Background(), job)
	if state.Get("b").Status != StepSkipped {
		t.Error("B should be skipped (dep A failed)")
	}
	if state.Get("c").Status != StepSkipped {
		t.Error("C should be skipped (dep B was skipped)")
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Finally
// ──────────────────────────────────────────────────────────────────────────────

func TestExecutor_FinallySteps(t *testing.T) {
	var order []string
	var mu sync.Mutex

	handlers := map[string]Handler{
		"fail":    failHandler("boom"),
		"cleanup": trackHandler(&order, &mu, "cleanup"),
		"report":  trackHandler(&order, &mu, "report"),
	}

	job := &Job{
		Steps: map[string]*Step{
			"a":       {Instruction: "fail", Params: json.RawMessage(`{}`)},
			"cleanup": {Instruction: "cleanup", Params: json.RawMessage(`{}`)},
			"report":  {Instruction: "report", Params: json.RawMessage(`{}`)},
		},
		Finally: []string{"cleanup", "report"},
	}

	state, _ := NewExecutor(handlers, nil).Run(context.Background(), job)

	if !state.Failed() {
		t.Error("expected failed state")
	}
	if len(order) != 2 || order[0] != "cleanup" || order[1] != "report" {
		t.Errorf("expected [cleanup report], got %v", order)
	}
}

func TestExecutor_FinallyWithContextCancelled(t *testing.T) {
	var cleanupRan bool

	handlers := map[string]Handler{
		"slow": slowHandler(5 * time.Second),
		"cleanup": func(ctx context.Context, p json.RawMessage) (*StepResult, error) {
			cleanupRan = true
			return &StepResult{Status: StepCompleted}, nil
		},
	}

	job := &Job{
		Timeout: "100ms",
		Steps: map[string]*Step{
			"a":       {Instruction: "slow", Params: json.RawMessage(`{}`)},
			"cleanup": {Instruction: "cleanup", Params: json.RawMessage(`{}`)},
		},
		Finally: []string{"cleanup"},
	}

	NewExecutor(handlers, nil).Run(context.Background(), job)
	if !cleanupRan {
		t.Error("finally step should run even after job timeout")
	}
}

func TestExecutor_FinallyMultipleSteps_AllRun(t *testing.T) {
	var order []string
	var mu sync.Mutex

	handlers := map[string]Handler{
		"ok": noop(),
		"f1": trackHandler(&order, &mu, "f1"),
		"f2": trackHandler(&order, &mu, "f2"),
		"f3": trackHandler(&order, &mu, "f3"),
	}

	job := &Job{
		Steps: map[string]*Step{
			"main": {Instruction: "ok", Params: json.RawMessage(`{}`)},
			"f1":   {Instruction: "f1", Params: json.RawMessage(`{}`)},
			"f2":   {Instruction: "f2", Params: json.RawMessage(`{}`)},
			"f3":   {Instruction: "f3", Params: json.RawMessage(`{}`)},
		},
		Finally: []string{"f1", "f2", "f3"},
	}

	NewExecutor(handlers, nil).Run(context.Background(), job)
	if len(order) != 3 || order[0] != "f1" || order[1] != "f2" || order[2] != "f3" {
		t.Errorf("expected [f1 f2 f3] in order, got %v", order)
	}
}

func TestExecutor_FinallyRunsEvenWhenFinallyStepFails(t *testing.T) {
	var order []string
	var mu sync.Mutex

	handlers := map[string]Handler{
		"ok":   noop(),
		"fail": failHandler("f1 failed"),
		"f2":   trackHandler(&order, &mu, "f2"),
	}

	job := &Job{
		Steps: map[string]*Step{
			"main": {Instruction: "ok", Params: json.RawMessage(`{}`)},
			"f1":   {Instruction: "fail", Params: json.RawMessage(`{}`)},
			"f2":   {Instruction: "f2", Params: json.RawMessage(`{}`)},
		},
		Finally: []string{"f1", "f2"},
	}

	NewExecutor(handlers, nil).Run(context.Background(), job)
	if len(order) != 1 || order[0] != "f2" {
		t.Errorf("f2 should run even when f1 fails, got %v", order)
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// State passing
// ──────────────────────────────────────────────────────────────────────────────

func TestExecutor_StatePassingBetweenSteps(t *testing.T) {
	handlers := map[string]Handler{
		"produce": func(ctx context.Context, p json.RawMessage) (*StepResult, error) {
			return &StepResult{Status: StepCompleted, Data: map[string]interface{}{"value": "hello"}}, nil
		},
		"consume": func(ctx context.Context, p json.RawMessage) (*StepResult, error) {
			var params map[string]interface{}
			json.Unmarshal(p, &params)
			return &StepResult{Status: StepCompleted, Data: map[string]interface{}{"received": params["input"]}}, nil
		},
	}

	job := &Job{
		Steps: map[string]*Step{
			"producer": {Instruction: "produce", Params: json.RawMessage(`{}`)},
			"consumer": {Instruction: "consume", DependsOn: []string{"producer"}, Params: json.RawMessage(`{"input": "$.producer.value"}`)},
		},
	}

	state, _ := NewExecutor(handlers, nil).Run(context.Background(), job)
	if state.Get("consumer").Data["received"] != "hello" {
		t.Errorf("expected 'hello', got %v", state.Get("consumer").Data["received"])
	}
}

func TestExecutor_StatePassingChain(t *testing.T) {
	// A produces x=1, B reads x and produces y=x+1, C reads y
	handlers := map[string]Handler{
		"produce": func(ctx context.Context, p json.RawMessage) (*StepResult, error) {
			return &StepResult{Status: StepCompleted, Data: map[string]interface{}{"x": float64(1)}}, nil
		},
		"transform": func(ctx context.Context, p json.RawMessage) (*StepResult, error) {
			var params map[string]interface{}
			json.Unmarshal(p, &params)
			x := params["input"].(float64)
			return &StepResult{Status: StepCompleted, Data: map[string]interface{}{"y": x + 1}}, nil
		},
		"consume": func(ctx context.Context, p json.RawMessage) (*StepResult, error) {
			var params map[string]interface{}
			json.Unmarshal(p, &params)
			return &StepResult{Status: StepCompleted, Data: map[string]interface{}{"final": params["val"]}}, nil
		},
	}

	job := &Job{
		Steps: map[string]*Step{
			"a": {Instruction: "produce", Params: json.RawMessage(`{}`)},
			"b": {Instruction: "transform", DependsOn: []string{"a"}, Params: json.RawMessage(`{"input": "$.a.x"}`)},
			"c": {Instruction: "consume", DependsOn: []string{"b"}, Params: json.RawMessage(`{"val": "$.b.y"}`)},
		},
	}

	state, _ := NewExecutor(handlers, nil).Run(context.Background(), job)
	if state.Get("c").Data["final"] != float64(2) {
		t.Errorf("expected 2, got %v", state.Get("c").Data["final"])
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Timeout and retry
// ──────────────────────────────────────────────────────────────────────────────

func TestExecutor_StepTimeout(t *testing.T) {
	handlers := map[string]Handler{"slow": slowHandler(5 * time.Second)}
	job := &Job{
		Steps: map[string]*Step{
			"a": {Instruction: "slow", Params: json.RawMessage(`{}`), Timeout: "100ms"},
		},
	}
	state, _ := NewExecutor(handlers, nil).Run(context.Background(), job)
	if state.Get("a").Status != StepFailed {
		t.Error("expected step to fail due to timeout")
	}
}

func TestExecutor_JobTimeout(t *testing.T) {
	handlers := map[string]Handler{"slow": slowHandler(5 * time.Second)}
	job := &Job{
		Timeout: "100ms",
		Steps: map[string]*Step{
			"a": {Instruction: "slow", Params: json.RawMessage(`{}`)},
			"b": {Instruction: "slow", DependsOn: []string{"a"}, Params: json.RawMessage(`{}`)},
		},
	}
	state, _ := NewExecutor(handlers, nil).Run(context.Background(), job)
	if state.Get("b").Status != StepSkipped {
		t.Error("b should be skipped after job timeout")
	}
}

func TestExecutor_JobTimeoutInvalidDuration(t *testing.T) {
	job := &Job{
		Timeout: "not-a-duration",
		Steps:   map[string]*Step{"a": {Instruction: "x", Params: json.RawMessage(`{}`)}},
	}
	_, err := NewExecutor(map[string]Handler{"x": noop()}, nil).Run(context.Background(), job)
	if err == nil {
		t.Error("expected error for invalid timeout")
	}
}

func TestExecutor_Retry(t *testing.T) {
	var attempts int32
	handlers := map[string]Handler{
		"flaky": func(ctx context.Context, p json.RawMessage) (*StepResult, error) {
			if atomic.AddInt32(&attempts, 1) < 3 {
				return nil, fmt.Errorf("transient error")
			}
			return &StepResult{Status: StepCompleted, Data: map[string]interface{}{"ok": true}}, nil
		},
	}

	job := &Job{
		Steps: map[string]*Step{
			"a": {Instruction: "flaky", Params: json.RawMessage(`{}`), Retry: &RetryPolicy{Max: 5, Interval: "10ms"}},
		},
	}

	state, _ := NewExecutor(handlers, nil).Run(context.Background(), job)
	if state.Get("a").Status != StepCompleted {
		t.Error("expected step to succeed after retries")
	}
	if atomic.LoadInt32(&attempts) != 3 {
		t.Errorf("expected 3 attempts, got %d", atomic.LoadInt32(&attempts))
	}
}

func TestExecutor_RetryExhausted(t *testing.T) {
	handlers := map[string]Handler{"fail": failHandler("always fails")}
	job := &Job{
		Steps: map[string]*Step{
			"a": {Instruction: "fail", Params: json.RawMessage(`{}`), Retry: &RetryPolicy{Max: 2, Interval: "10ms"}},
		},
	}
	state, _ := NewExecutor(handlers, nil).Run(context.Background(), job)
	if state.Get("a").Status != StepFailed {
		t.Error("expected step to fail after exhausted retries")
	}
}

func TestExecutor_RetryWithRunCondition(t *testing.T) {
	// Flaky step retries and succeeds → downstream on_success step should run
	var attempts int32
	handlers := map[string]Handler{
		"flaky": func(ctx context.Context, p json.RawMessage) (*StepResult, error) {
			if atomic.AddInt32(&attempts, 1) < 2 {
				return nil, fmt.Errorf("transient")
			}
			return &StepResult{Status: StepCompleted}, nil
		},
		"ok": noop(),
	}

	job := &Job{
		Steps: map[string]*Step{
			"a": {Instruction: "flaky", Params: json.RawMessage(`{}`), Retry: &RetryPolicy{Max: 3, Interval: "10ms"}},
			"b": {Instruction: "ok", DependsOn: []string{"a"}, Params: json.RawMessage(`{}`)},
		},
	}

	state, _ := NewExecutor(handlers, nil).Run(context.Background(), job)
	if state.Get("a").Status != StepCompleted {
		t.Error("a should succeed after retry")
	}
	if state.Get("b").Status != StepCompleted {
		t.Error("b should run (a succeeded after retry)")
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Timing
// ──────────────────────────────────────────────────────────────────────────────

func TestExecutor_StepTiming(t *testing.T) {
	handlers := map[string]Handler{
		"fast": func(ctx context.Context, params json.RawMessage) (*StepResult, error) {
			time.Sleep(20 * time.Millisecond)
			return &StepResult{Status: StepCompleted}, nil
		},
		"slow": func(ctx context.Context, params json.RawMessage) (*StepResult, error) {
			time.Sleep(50 * time.Millisecond)
			return &StepResult{Status: StepCompleted}, nil
		},
	}

	job := &Job{
		Steps: map[string]*Step{
			"a": {Instruction: "slow", Params: json.RawMessage(`{}`)},
			"b": {Instruction: "fast", DependsOn: []string{"a"}, Params: json.RawMessage(`{}`)},
		},
	}

	state, _ := NewExecutor(handlers, nil).Run(context.Background(), job)

	a := state.Get("a")
	if a.Timing.QueuedAt.IsZero() || a.Timing.StartedAt.IsZero() || a.Timing.CompletedAt.IsZero() {
		t.Error("step a timing has zero timestamps")
	}
	if a.Timing.ExecMs < 40 {
		t.Errorf("step a exec_ms too low: %d", a.Timing.ExecMs)
	}

	b := state.Get("b")
	if b.Timing.ExecMs < 15 {
		t.Errorf("step b exec_ms too low: %d", b.Timing.ExecMs)
	}
	if b.Timing.StartedAt.Before(a.Timing.CompletedAt) {
		t.Error("step b started before a completed")
	}
}

func TestExecutor_StepTiming_Parallel(t *testing.T) {
	handlers := map[string]Handler{
		"work": func(ctx context.Context, params json.RawMessage) (*StepResult, error) {
			time.Sleep(30 * time.Millisecond)
			return &StepResult{Status: StepCompleted}, nil
		},
	}

	job := &Job{
		Steps: map[string]*Step{
			"a": {Instruction: "work", Params: json.RawMessage(`{}`)},
			"b": {Instruction: "work", Params: json.RawMessage(`{}`)},
			"c": {Instruction: "work", DependsOn: []string{"a", "b"}, Params: json.RawMessage(`{}`)},
		},
	}

	state, _ := NewExecutor(handlers, nil).Run(context.Background(), job)

	a := state.Get("a")
	b := state.Get("b")
	c := state.Get("c")

	diff := a.Timing.QueuedAt.Sub(b.Timing.QueuedAt)
	if diff < 0 {
		diff = -diff
	}
	if diff > time.Millisecond {
		t.Errorf("parallel steps a and b should have same queued_at, diff: %v", diff)
	}

	if c.Timing.QueuedAt.Before(a.Timing.CompletedAt) {
		t.Error("step c queued before a completed")
	}
}

func TestExecutor_StepTiming_SkippedStepHasNoTiming(t *testing.T) {
	handlers := map[string]Handler{"ok": noop(), "fail": failHandler("boom")}
	job := &Job{
		Steps: map[string]*Step{
			"a": {Instruction: "fail", Params: json.RawMessage(`{}`)},
			"b": {Instruction: "ok", DependsOn: []string{"a"}, Params: json.RawMessage(`{}`)},
		},
	}
	state, _ := NewExecutor(handlers, nil).Run(context.Background(), job)
	b := state.Get("b")
	if b.Timing.StartedAt.IsZero() == false {
		t.Error("skipped step should have zero timing")
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Progress callback
// ──────────────────────────────────────────────────────────────────────────────

func TestExecutor_ProgressCallback(t *testing.T) {
	var events []string
	var mu sync.Mutex

	progress := func(stepName string, instruction string, status StepStatus, result *StepResult) {
		mu.Lock()
		events = append(events, fmt.Sprintf("%s:%s", stepName, status))
		mu.Unlock()
	}

	job := &Job{
		Steps: map[string]*Step{
			"a": {Instruction: "ok", Params: json.RawMessage(`{}`)},
		},
	}
	NewExecutor(map[string]Handler{"ok": noop()}, progress).Run(context.Background(), job)

	found := map[string]bool{}
	for _, e := range events {
		found[e] = true
	}
	if !found["a:running"] {
		t.Error("missing a:running event")
	}
	if !found["a:completed"] {
		t.Error("missing a:completed event")
	}
}

func TestExecutor_ProgressCallback_AllStatuses(t *testing.T) {
	var events []string
	var mu sync.Mutex

	progress := func(stepName string, _ string, status StepStatus, _ *StepResult) {
		mu.Lock()
		events = append(events, fmt.Sprintf("%s:%s", stepName, status))
		mu.Unlock()
	}

	handlers := map[string]Handler{"ok": noop(), "fail": failHandler("boom")}
	job := &Job{
		Steps: map[string]*Step{
			"a": {Instruction: "fail", Params: json.RawMessage(`{}`)},
			"b": {Instruction: "ok", DependsOn: []string{"a"}, Params: json.RawMessage(`{}`)}, // will be skipped
		},
	}
	NewExecutor(handlers, progress).Run(context.Background(), job)

	found := map[string]bool{}
	for _, e := range events {
		found[e] = true
	}
	if !found["a:running"] {
		t.Error("missing a:running")
	}
	if !found["a:failed"] {
		t.Error("missing a:failed")
	}
	if !found["b:skipped"] {
		t.Error("missing b:skipped")
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Large DAG
// ──────────────────────────────────────────────────────────────────────────────

func TestExecutor_LargeLinearDAG(t *testing.T) {
	// 50-step linear chain
	n := 50
	handlers := map[string]Handler{"ok": noop()}
	steps := make(map[string]*Step, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("step_%d", i)
		step := &Step{Instruction: "ok", Params: json.RawMessage(`{}`)}
		if i > 0 {
			step.DependsOn = []string{fmt.Sprintf("step_%d", i-1)}
		}
		steps[name] = step
	}

	state, err := NewExecutor(handlers, nil).Run(context.Background(), &Job{Steps: steps})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for i := 0; i < n; i++ {
		r := state.Get(fmt.Sprintf("step_%d", i))
		if r == nil || r.Status != StepCompleted {
			t.Fatalf("step_%d should be completed", i)
		}
	}
}

func TestExecutor_WideParallelDAG(t *testing.T) {
	// 20 parallel steps → 1 join step
	n := 20
	handlers := map[string]Handler{"ok": noop()}
	steps := make(map[string]*Step, n+1)
	deps := make([]string, n)

	for i := 0; i < n; i++ {
		name := fmt.Sprintf("p_%d", i)
		steps[name] = &Step{Instruction: "ok", Params: json.RawMessage(`{}`)}
		deps[i] = name
	}
	steps["join"] = &Step{Instruction: "ok", DependsOn: deps, Params: json.RawMessage(`{}`)}

	state, err := NewExecutor(handlers, nil).Run(context.Background(), &Job{Steps: steps})
	if err != nil {
		t.Fatal(err)
	}
	if state.Get("join").Status != StepCompleted {
		t.Error("join step should complete")
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// RunWithState
// ──────────────────────────────────────────────────────────────────────────────

func TestExecutor_RunWithState_SharedState(t *testing.T) {
	state := NewStateBag()
	// Pre-populate state
	state.Set("external", &StepResult{
		Status: StepCompleted,
		Data:   map[string]interface{}{"key": "pre-existing"},
	})

	handlers := map[string]Handler{
		"read": func(ctx context.Context, p json.RawMessage) (*StepResult, error) {
			var params map[string]interface{}
			json.Unmarshal(p, &params)
			return &StepResult{Status: StepCompleted, Data: map[string]interface{}{"got": params["val"]}}, nil
		},
	}

	job := &Job{
		Steps: map[string]*Step{
			"a": {Instruction: "read", Params: json.RawMessage(`{"val": "$.external.key"}`)},
		},
	}

	resultState, _ := NewExecutor(handlers, nil).RunWithState(context.Background(), job, state)
	if resultState.Get("a").Data["got"] != "pre-existing" {
		t.Errorf("expected pre-existing, got %v", resultState.Get("a").Data["got"])
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// executeFinally coverage
// ──────────────────────────────────────────────────────────────────────────────

func TestExecutor_Finally_UnknownInstruction(t *testing.T) {
	// Finally step references an instruction with no registered handler.
	handlers := map[string]Handler{"ok": noop()}

	job := &Job{
		Steps: map[string]*Step{
			"main":    {Instruction: "ok", Params: json.RawMessage(`{}`)},
			"cleanup": {Instruction: "nonexistent_instruction", Params: json.RawMessage(`{}`)},
		},
		Finally: []string{"cleanup"},
	}

	state, err := NewExecutor(handlers, nil).Run(context.Background(), job)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	r := state.Get("cleanup")
	if r == nil {
		t.Fatal("expected cleanup result to be set")
	}
	if r.Status != StepFailed {
		t.Errorf("expected failed status, got %s", r.Status)
	}
	if r.Error != "unknown instruction: nonexistent_instruction" {
		t.Errorf("unexpected error message: %s", r.Error)
	}
}

func TestExecutor_Finally_ParamResolutionFailure(t *testing.T) {
	// Finally step references a step without output data, causing param resolution error.
	handlers := map[string]Handler{
		"ok": noop(),
		"cleanup": func(ctx context.Context, p json.RawMessage) (*StepResult, error) {
			return &StepResult{Status: StepCompleted}, nil
		},
	}

	job := &Job{
		Steps: map[string]*Step{
			"main":    {Instruction: "ok", Params: json.RawMessage(`{}`)},
			"cleanup": {Instruction: "cleanup", Params: json.RawMessage(`{"ref": "$.main.value"}`)},
		},
		Finally: []string{"cleanup"},
	}

	state, err := NewExecutor(handlers, nil).Run(context.Background(), job)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	r := state.Get("cleanup")
	if r == nil {
		t.Fatal("expected cleanup result to be set")
	}
	if r.Status != StepFailed {
		t.Errorf("expected failed status, got %s", r.Status)
	}
	if r.Error == "" {
		t.Error("expected non-empty error for param resolution failure")
	}
}

func TestExecutor_Finally_HandlerReturnsNilResult(t *testing.T) {
	// Handler returns nil result (no error) — executeFinally should treat it as completed.
	handlers := map[string]Handler{
		"ok": noop(),
		"nil_handler": func(ctx context.Context, p json.RawMessage) (*StepResult, error) {
			return nil, nil
		},
	}

	job := &Job{
		Steps: map[string]*Step{
			"main":    {Instruction: "ok", Params: json.RawMessage(`{}`)},
			"cleanup": {Instruction: "nil_handler", Params: json.RawMessage(`{}`)},
		},
		Finally: []string{"cleanup"},
	}

	state, err := NewExecutor(handlers, nil).Run(context.Background(), job)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	r := state.Get("cleanup")
	if r == nil {
		t.Fatal("expected cleanup result to be set")
	}
	if r.Status != StepCompleted {
		t.Errorf("expected completed status for nil result, got %s", r.Status)
	}
}

func TestExecutor_Finally_HandlerReturnsError(t *testing.T) {
	// Handler returns an error — executeFinally should record it as failed.
	handlers := map[string]Handler{
		"ok":   noop(),
		"fail": failHandler("cleanup exploded"),
	}

	job := &Job{
		Steps: map[string]*Step{
			"main":    {Instruction: "ok", Params: json.RawMessage(`{}`)},
			"cleanup": {Instruction: "fail", Params: json.RawMessage(`{}`)},
		},
		Finally: []string{"cleanup"},
	}

	state, err := NewExecutor(handlers, nil).Run(context.Background(), job)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	r := state.Get("cleanup")
	if r == nil {
		t.Fatal("expected cleanup result to be set")
	}
	if r.Status != StepFailed {
		t.Errorf("expected failed status, got %s", r.Status)
	}
	if r.Error != "cleanup exploded" {
		t.Errorf("expected 'cleanup exploded', got %s", r.Error)
	}
	if r.Duration == 0 {
		t.Error("expected non-zero duration on failed finally step")
	}
}

func TestExecutor_Finally_ContextCancellationWithTimeout(t *testing.T) {
	// Call executeFinally directly with a short-lived context to verify that
	// context cancellation propagates to finally handlers.
	var handlerCtxErr error

	handlers := map[string]Handler{
		"slow_cleanup": func(ctx context.Context, p json.RawMessage) (*StepResult, error) {
			select {
			case <-time.After(10 * time.Second):
				return &StepResult{Status: StepCompleted}, nil
			case <-ctx.Done():
				handlerCtxErr = ctx.Err()
				return nil, ctx.Err()
			}
		},
	}

	allSteps := map[string]*Step{
		"cleanup": {Instruction: "slow_cleanup", Params: json.RawMessage(`{}`)},
	}
	finallyNames := []string{"cleanup"}
	state := NewStateBag()

	exec := NewExecutor(handlers, nil)

	// Create a context that expires quickly — the handler should see cancellation.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	exec.executeFinally(ctx, finallyNames, allSteps, state)

	r := state.Get("cleanup")
	if r == nil {
		t.Fatal("expected cleanup result to be set")
	}
	if r.Status != StepFailed {
		t.Errorf("expected failed status due to context cancellation, got %s", r.Status)
	}
	if handlerCtxErr == nil {
		t.Error("expected handler to observe context cancellation")
	}
}
