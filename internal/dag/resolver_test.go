package dag

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
)

func TestResolveParams_NoRefs(t *testing.T) {
	params := json.RawMessage(`{"method": "GET", "url": "/health", "count": 42}`)
	state := NewStateBag()

	resolved, err := ResolveParams(params, state)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var result map[string]interface{}
	json.Unmarshal(resolved, &result)

	if result["method"] != "GET" {
		t.Errorf("expected GET, got %v", result["method"])
	}
	if result["count"] != float64(42) {
		t.Errorf("expected 42, got %v", result["count"])
	}
}

func TestResolveParams_SimpleRef(t *testing.T) {
	state := NewStateBag()
	state.Set("start_services", &StepResult{
		Status: StepCompleted,
		Data:   map[string]interface{}{"network_name": "chiperka-test-abc"},
	})

	params := json.RawMessage(`{"network": "$.start_services.network_name"}`)
	resolved, err := ResolveParams(params, state)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var result map[string]interface{}
	json.Unmarshal(resolved, &result)

	if result["network"] != "chiperka-test-abc" {
		t.Errorf("expected chiperka-test-abc, got %v", result["network"])
	}
}

func TestResolveParams_NestedRef(t *testing.T) {
	state := NewStateBag()
	state.Set("execute", &StepResult{
		Status: StepCompleted,
		Data: map[string]interface{}{
			"response": map[string]interface{}{
				"status_code": float64(200),
				"body":        `{"token":"abc"}`,
			},
		},
	})

	params := json.RawMessage(`{"status": "$.execute.response.status_code"}`)
	resolved, err := ResolveParams(params, state)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var result map[string]interface{}
	json.Unmarshal(resolved, &result)

	if result["status"] != float64(200) {
		t.Errorf("expected 200, got %v", result["status"])
	}
}

func TestResolveParams_DeeplyNested(t *testing.T) {
	state := NewStateBag()
	state.Set("s", &StepResult{
		Status: StepCompleted,
		Data: map[string]interface{}{
			"a": map[string]interface{}{
				"b": map[string]interface{}{
					"c": map[string]interface{}{
						"value": "deep",
					},
				},
			},
		},
	})

	params := json.RawMessage(`{"x": "$.s.a.b.c.value"}`)
	resolved, err := ResolveParams(params, state)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var result map[string]interface{}
	json.Unmarshal(resolved, &result)

	if result["x"] != "deep" {
		t.Errorf("expected deep, got %v", result["x"])
	}
}

func TestResolveParams_PreservesType(t *testing.T) {
	state := NewStateBag()
	state.Set("svc", &StepResult{
		Status: StepCompleted,
		Data: map[string]interface{}{
			"ready":  true,
			"count":  float64(3),
			"labels": []interface{}{"a", "b"},
		},
	})

	params := json.RawMessage(`{"ready": "$.svc.ready", "count": "$.svc.count", "labels": "$.svc.labels"}`)
	resolved, err := ResolveParams(params, state)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var result map[string]interface{}
	json.Unmarshal(resolved, &result)

	if result["ready"] != true {
		t.Errorf("expected true, got %v (%T)", result["ready"], result["ready"])
	}
	if result["count"] != float64(3) {
		t.Errorf("expected 3, got %v", result["count"])
	}
	labels, ok := result["labels"].([]interface{})
	if !ok || len(labels) != 2 {
		t.Errorf("expected 2-element array, got %v", result["labels"])
	}
}

func TestResolveParams_MissingStep(t *testing.T) {
	state := NewStateBag()
	_, err := ResolveParams(json.RawMessage(`{"val": "$.missing.field"}`), state)
	if err == nil {
		t.Fatal("expected error for missing step")
	}
}

func TestResolveParams_MissingField(t *testing.T) {
	state := NewStateBag()
	state.Set("step", &StepResult{Status: StepCompleted, Data: map[string]interface{}{"a": 1}})
	_, err := ResolveParams(json.RawMessage(`{"val": "$.step.nonexistent"}`), state)
	if err == nil {
		t.Fatal("expected error for missing field")
	}
}

func TestResolveParams_MissingNestedField(t *testing.T) {
	state := NewStateBag()
	state.Set("s", &StepResult{
		Status: StepCompleted,
		Data:   map[string]interface{}{"a": map[string]interface{}{"b": 1}},
	})
	_, err := ResolveParams(json.RawMessage(`{"val": "$.s.a.nonexistent"}`), state)
	if err == nil {
		t.Fatal("expected error for missing nested field")
	}
}

func TestResolveParams_TraverseNonObject(t *testing.T) {
	state := NewStateBag()
	state.Set("s", &StepResult{
		Status: StepCompleted,
		Data:   map[string]interface{}{"x": "string_value"},
	})
	_, err := ResolveParams(json.RawMessage(`{"val": "$.s.x.nested"}`), state)
	if err == nil {
		t.Fatal("expected error when traversing non-object")
	}
}

func TestResolveParams_StepWithNilData(t *testing.T) {
	state := NewStateBag()
	state.Set("s", &StepResult{Status: StepCompleted, Data: nil})
	_, err := ResolveParams(json.RawMessage(`{"val": "$.s.field"}`), state)
	if err == nil {
		t.Fatal("expected error for nil data")
	}
}

func TestResolveParams_InArray(t *testing.T) {
	state := NewStateBag()
	state.Set("svc", &StepResult{
		Status: StepCompleted,
		Data:   map[string]interface{}{"id": "container-1"},
	})

	params := json.RawMessage(`{"containers": ["$.svc.id", "static"]}`)
	resolved, err := ResolveParams(params, state)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var result map[string]interface{}
	json.Unmarshal(resolved, &result)

	arr := result["containers"].([]interface{})
	if arr[0] != "container-1" || arr[1] != "static" {
		t.Errorf("expected [container-1 static], got %v", arr)
	}
}

func TestResolveParams_NestedInArray(t *testing.T) {
	state := NewStateBag()
	state.Set("a", &StepResult{Status: StepCompleted, Data: map[string]interface{}{"x": "v1"}})
	state.Set("b", &StepResult{Status: StepCompleted, Data: map[string]interface{}{"y": "v2"}})

	params := json.RawMessage(`{"items": ["$.a.x", "$.b.y", "literal"]}`)
	resolved, err := ResolveParams(params, state)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var result map[string]interface{}
	json.Unmarshal(resolved, &result)

	arr := result["items"].([]interface{})
	if len(arr) != 3 || arr[0] != "v1" || arr[1] != "v2" || arr[2] != "literal" {
		t.Errorf("expected [v1 v2 literal], got %v", arr)
	}
}

func TestResolveParams_EmptyParams(t *testing.T) {
	resolved, err := ResolveParams(json.RawMessage(`{}`), NewStateBag())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(resolved) != "{}" {
		t.Errorf("expected {}, got %s", resolved)
	}
}

func TestResolveParams_NilParams(t *testing.T) {
	resolved, err := ResolveParams(nil, NewStateBag())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resolved != nil {
		t.Errorf("expected nil, got %s", resolved)
	}
}

func TestResolveParams_NonRefDollarString(t *testing.T) {
	params := json.RawMessage(`{"price": "$100", "path": "/api/$version"}`)
	resolved, err := ResolveParams(params, NewStateBag())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var result map[string]interface{}
	json.Unmarshal(resolved, &result)

	if result["price"] != "$100" {
		t.Errorf("expected $100, got %v", result["price"])
	}
	if result["path"] != "/api/$version" {
		t.Errorf("expected /api/$version, got %v", result["path"])
	}
}

func TestResolveParams_RefReturnsObject(t *testing.T) {
	state := NewStateBag()
	state.Set("s", &StepResult{
		Status: StepCompleted,
		Data: map[string]interface{}{
			"config": map[string]interface{}{"host": "localhost", "port": float64(5432)},
		},
	})

	params := json.RawMessage(`{"db": "$.s.config"}`)
	resolved, err := ResolveParams(params, state)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var result map[string]interface{}
	json.Unmarshal(resolved, &result)

	db, ok := result["db"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected object, got %T: %v", result["db"], result["db"])
	}
	if db["host"] != "localhost" || db["port"] != float64(5432) {
		t.Errorf("unexpected db config: %v", db)
	}
}

func TestResolveParams_MultipleRefsInOneObject(t *testing.T) {
	state := NewStateBag()
	state.Set("a", &StepResult{Status: StepCompleted, Data: map[string]interface{}{"x": "val_a"}})
	state.Set("b", &StepResult{Status: StepCompleted, Data: map[string]interface{}{"y": "val_b"}})

	params := json.RawMessage(`{"from_a": "$.a.x", "from_b": "$.b.y", "literal": "keep"}`)
	resolved, err := ResolveParams(params, state)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var result map[string]interface{}
	json.Unmarshal(resolved, &result)

	if result["from_a"] != "val_a" || result["from_b"] != "val_b" || result["literal"] != "keep" {
		t.Errorf("unexpected: %v", result)
	}
}

func TestResolveParams_InvalidJSON(t *testing.T) {
	_, err := ResolveParams(json.RawMessage(`{not json`), NewStateBag())
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// StateBag tests
// ──────────────────────────────────────────────────────────────────────────────

func TestStateBag_GetSet(t *testing.T) {
	s := NewStateBag()
	if s.Get("x") != nil {
		t.Error("expected nil for missing key")
	}

	s.Set("x", &StepResult{Status: StepCompleted, Data: map[string]interface{}{"v": 1}})
	r := s.Get("x")
	if r == nil || r.Data["v"] != 1 {
		t.Error("expected stored result")
	}
}

func TestStateBag_FailedFlag(t *testing.T) {
	s := NewStateBag()
	if s.Failed() {
		t.Error("new bag should not be failed")
	}

	s.Set("ok", &StepResult{Status: StepCompleted})
	if s.Failed() {
		t.Error("should not be failed after completed step")
	}

	s.Set("bad", &StepResult{Status: StepFailed})
	if !s.Failed() {
		t.Error("should be failed after failed step")
	}
}

func TestStateBag_StepFailed(t *testing.T) {
	s := NewStateBag()
	s.Set("a", &StepResult{Status: StepCompleted})
	s.Set("b", &StepResult{Status: StepFailed})
	s.Set("c", &StepResult{Status: StepSkipped})

	if s.StepFailed("a") {
		t.Error("a should not be failed")
	}
	if !s.StepFailed("b") {
		t.Error("b should be failed")
	}
	if s.StepFailed("c") {
		t.Error("c should not be failed (skipped)")
	}
	if s.StepFailed("nonexistent") {
		t.Error("nonexistent should not be failed")
	}
}

func TestStateBag_HasCompleted(t *testing.T) {
	s := NewStateBag()
	if s.HasCompleted("x") {
		t.Error("should not be completed")
	}
	s.Set("x", &StepResult{Status: StepSkipped})
	if !s.HasCompleted("x") {
		t.Error("skipped counts as completed")
	}
}

func TestStateBag_GetData(t *testing.T) {
	s := NewStateBag()
	s.Set("s", &StepResult{Status: StepCompleted, Data: map[string]interface{}{"key": "value"}})

	v, ok := s.GetData("s", "key")
	if !ok || v != "value" {
		t.Errorf("expected value, got %v (ok=%v)", v, ok)
	}

	_, ok = s.GetData("s", "missing")
	if ok {
		t.Error("expected not found")
	}

	_, ok = s.GetData("nonexistent", "key")
	if ok {
		t.Error("expected not found for missing step")
	}
}

func TestStateBag_AllResults(t *testing.T) {
	s := NewStateBag()
	s.Set("a", &StepResult{Status: StepCompleted})
	s.Set("b", &StepResult{Status: StepFailed})

	all := s.AllResults()
	if len(all) != 2 {
		t.Errorf("expected 2 results, got %d", len(all))
	}
	// Verify it's a copy
	delete(all, "a")
	if s.Get("a") == nil {
		t.Error("deleting from AllResults should not affect original")
	}
}

func TestStateBag_ConcurrentAccess(t *testing.T) {
	s := NewStateBag()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			name := fmt.Sprintf("step_%d", n)
			s.Set(name, &StepResult{Status: StepCompleted, Data: map[string]interface{}{"n": n}})
			s.Get(name)
			s.Failed()
			s.AllResults()
		}(i)
	}
	wg.Wait()

	all := s.AllResults()
	if len(all) != 100 {
		t.Errorf("expected 100 results, got %d", len(all))
	}
}
