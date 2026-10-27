package assertion

import (
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"chiperka-worker/internal/executor"
	"chiperka-worker/internal/model"
)

func intPtr(v int) *int       { return &v }
func int64Ptr(v int64) *int64 { return &v }
func boolPtr(v bool) *bool    { return &v }

// ---------------------------------------------------------------------------
// walkJsonPath
// ---------------------------------------------------------------------------

func TestWalkJsonPath_SimpleField(t *testing.T) {
	data := map[string]interface{}{"name": "alice"}
	val, found := walkJsonPath(data, "$.name")
	if !found || val != "alice" {
		t.Fatalf("expected alice, got %v (found=%v)", val, found)
	}
}

func TestWalkJsonPath_NestedField(t *testing.T) {
	data := map[string]interface{}{
		"user": map[string]interface{}{
			"age": float64(30),
		},
	}
	val, found := walkJsonPath(data, "$.user.age")
	if !found || val != float64(30) {
		t.Fatalf("expected 30, got %v (found=%v)", val, found)
	}
}

func TestWalkJsonPath_ArrayIndex(t *testing.T) {
	data := map[string]interface{}{
		"items": []interface{}{"a", "b", "c"},
	}
	val, found := walkJsonPath(data, "$.items[1]")
	if !found || val != "b" {
		t.Fatalf("expected b, got %v (found=%v)", val, found)
	}
}

func TestWalkJsonPath_NegativeIndex(t *testing.T) {
	data := map[string]interface{}{
		"items": []interface{}{"a", "b", "c"},
	}
	val, found := walkJsonPath(data, "$.items[-1]")
	if !found || val != "c" {
		t.Fatalf("expected c, got %v (found=%v)", val, found)
	}
}

func TestWalkJsonPath_Wildcard(t *testing.T) {
	data := map[string]interface{}{
		"users": []interface{}{
			map[string]interface{}{"name": "alice"},
			map[string]interface{}{"name": "bob"},
		},
	}
	val, found := walkJsonPath(data, "$.users[*].name")
	if !found {
		t.Fatal("expected found")
	}
	arr, ok := val.([]interface{})
	if !ok || len(arr) != 2 || arr[0] != "alice" || arr[1] != "bob" {
		t.Fatalf("unexpected result: %v", val)
	}
}

func TestWalkJsonPath_RootOnly(t *testing.T) {
	data := map[string]interface{}{"x": float64(1)}
	val, found := walkJsonPath(data, "$")
	if !found || val == nil {
		t.Fatalf("expected root data, got %v (found=%v)", val, found)
	}
}

func TestWalkJsonPath_InvalidPrefix(t *testing.T) {
	_, found := walkJsonPath(nil, "foo.bar")
	if found {
		t.Fatal("expected not found for path without $ prefix")
	}
}

func TestWalkJsonPath_MissingField(t *testing.T) {
	data := map[string]interface{}{"a": float64(1)}
	_, found := walkJsonPath(data, "$.b")
	if found {
		t.Fatal("expected not found for missing field")
	}
}

func TestWalkJsonPath_IndexOutOfBounds(t *testing.T) {
	data := map[string]interface{}{
		"items": []interface{}{"a"},
	}
	_, found := walkJsonPath(data, "$.items[5]")
	if found {
		t.Fatal("expected not found for out of bounds index")
	}
}

func TestWalkJsonPath_NotAnObject(t *testing.T) {
	data := "just a string"
	_, found := walkJsonPath(data, "$.field")
	if found {
		t.Fatal("expected not found when data is not an object")
	}
}

func TestWalkJsonPath_NotAnArray(t *testing.T) {
	data := map[string]interface{}{"items": "not-array"}
	_, found := walkJsonPath(data, "$.items[0]")
	if found {
		t.Fatal("expected not found when field is not an array")
	}
}

func TestWalkJsonPath_WildcardEmpty(t *testing.T) {
	data := map[string]interface{}{
		"items": []interface{}{},
	}
	_, found := walkJsonPath(data, "$.items[*].name")
	if found {
		t.Fatal("expected not found for wildcard on empty array")
	}
}

func TestWalkJsonPath_BareArrayIndex(t *testing.T) {
	// path like $[0] on a top-level array
	data := []interface{}{"x", "y"}
	val, found := walkJsonPath(data, "$[0]")
	if !found || val != "x" {
		t.Fatalf("expected x, got %v (found=%v)", val, found)
	}
}

// ---------------------------------------------------------------------------
// jsonValueToString
// ---------------------------------------------------------------------------

func TestJsonValueToString(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected string
	}{
		{"hello", "hello"},
		{float64(42), "42"},
		{float64(3.14), "3.14"},
		{true, "true"},
		{false, "false"},
		{nil, "null"},
		{[]interface{}{float64(1), float64(2)}, "[1,2]"},
		{map[string]interface{}{"a": float64(1)}, `{"a":1}`},
	}
	for _, tt := range tests {
		got := jsonValueToString(tt.input)
		if got != tt.expected {
			t.Errorf("jsonValueToString(%v) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}

// ---------------------------------------------------------------------------
// truncateString
// ---------------------------------------------------------------------------

func TestTruncateString(t *testing.T) {
	if truncateString("short", 100) != "short" {
		t.Error("short string should not be truncated")
	}
	long := "abcdefghij"
	result := truncateString(long, 8)
	if result != "abcde..." {
		t.Errorf("expected abcde..., got %q", result)
	}
}

// ---------------------------------------------------------------------------
// findArtifact
// ---------------------------------------------------------------------------

func TestFindArtifact(t *testing.T) {
	arts := []ArtifactInfo{
		{Name: "logs/app.log", Path: "/tmp/app.log", Size: 100},
		{Name: "data.json", Path: "/tmp/data.json", Size: 200},
	}
	// exact match
	a := findArtifact("data.json", arts)
	if a == nil || a.Size != 200 {
		t.Fatalf("expected data.json artifact")
	}
	// basename match
	a = findArtifact("app.log", arts)
	if a == nil || a.Size != 100 {
		t.Fatalf("expected app.log artifact via basename")
	}
	// not found
	a = findArtifact("missing", arts)
	if a != nil {
		t.Fatal("expected nil for missing artifact")
	}
}

// ---------------------------------------------------------------------------
// Status code assertions
// ---------------------------------------------------------------------------

func TestEvaluateStatusCode_Pass(t *testing.T) {
	e := NewEvaluator()
	code := 200
	assertions := []model.Assertion{
		{Response: &model.ResponseAssertion{StatusCode: &code}},
	}
	ctx := EvalContext{
		HTTPResponse: &executor.HTTPResponse{StatusCode: 200, Headers: http.Header{}},
	}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if !allPassed || len(results) != 1 || !results[0].Passed {
		t.Fatalf("expected pass: %+v", results)
	}
}

func TestEvaluateStatusCode_Fail(t *testing.T) {
	e := NewEvaluator()
	code := 201
	assertions := []model.Assertion{
		{Response: &model.ResponseAssertion{StatusCode: &code}},
	}
	ctx := EvalContext{
		HTTPResponse: &executor.HTTPResponse{StatusCode: 500, Headers: http.Header{}},
	}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if allPassed || results[0].Passed {
		t.Fatalf("expected fail: %+v", results)
	}
}

func TestEvaluateStatusCode_NilResponse(t *testing.T) {
	e := NewEvaluator()
	code := 200
	assertions := []model.Assertion{
		{Response: &model.ResponseAssertion{StatusCode: &code}},
	}
	ctx := EvalContext{}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if allPassed || results[0].Passed {
		t.Fatalf("expected fail when HTTPResponse is nil: %+v", results)
	}
	if results[0].Actual != "0" {
		t.Fatalf("expected actual=0 when response nil, got %q", results[0].Actual)
	}
}

// ---------------------------------------------------------------------------
// Header assertions
// ---------------------------------------------------------------------------

func TestEvaluateHeader_Equals(t *testing.T) {
	e := NewEvaluator()
	assertions := []model.Assertion{
		{Response: &model.ResponseAssertion{
			Headers: map[string]model.HeaderMatcher{
				"Content-Type": {Equals: "application/json"},
			},
		}},
	}
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	ctx := EvalContext{HTTPResponse: &executor.HTTPResponse{StatusCode: 200, Headers: h}}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if !allPassed || !results[0].Passed {
		t.Fatalf("expected pass: %+v", results)
	}
}

func TestEvaluateHeader_Contains(t *testing.T) {
	e := NewEvaluator()
	assertions := []model.Assertion{
		{Response: &model.ResponseAssertion{
			Headers: map[string]model.HeaderMatcher{
				"Content-Type": {Contains: "json"},
			},
		}},
	}
	h := http.Header{}
	h.Set("Content-Type", "application/json; charset=utf-8")
	ctx := EvalContext{HTTPResponse: &executor.HTTPResponse{StatusCode: 200, Headers: h}}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if !allPassed || !results[0].Passed {
		t.Fatalf("expected pass: %+v", results)
	}
}

func TestEvaluateHeader_Exists(t *testing.T) {
	e := NewEvaluator()
	tr := true
	assertions := []model.Assertion{
		{Response: &model.ResponseAssertion{
			Headers: map[string]model.HeaderMatcher{
				"X-Custom": {Exists: &tr},
			},
		}},
	}
	h := http.Header{}
	h.Set("X-Custom", "val")
	ctx := EvalContext{HTTPResponse: &executor.HTTPResponse{StatusCode: 200, Headers: h}}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if !allPassed || !results[0].Passed {
		t.Fatalf("expected pass: %+v", results)
	}
}

func TestEvaluateHeader_NotExists(t *testing.T) {
	e := NewEvaluator()
	f := false
	assertions := []model.Assertion{
		{Response: &model.ResponseAssertion{
			Headers: map[string]model.HeaderMatcher{
				"X-Missing": {Exists: &f},
			},
		}},
	}
	h := http.Header{}
	ctx := EvalContext{HTTPResponse: &executor.HTTPResponse{StatusCode: 200, Headers: h}}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if !allPassed || !results[0].Passed {
		t.Fatalf("expected pass: %+v", results)
	}
}

func TestEvaluateHeader_ExistsFail(t *testing.T) {
	e := NewEvaluator()
	tr := true
	assertions := []model.Assertion{
		{Response: &model.ResponseAssertion{
			Headers: map[string]model.HeaderMatcher{
				"X-Missing": {Exists: &tr},
			},
		}},
	}
	h := http.Header{}
	ctx := EvalContext{HTTPResponse: &executor.HTTPResponse{StatusCode: 200, Headers: h}}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if allPassed || results[0].Passed {
		t.Fatalf("expected fail: %+v", results)
	}
}

func TestEvaluateHeader_NotExistsFail(t *testing.T) {
	e := NewEvaluator()
	f := false
	assertions := []model.Assertion{
		{Response: &model.ResponseAssertion{
			Headers: map[string]model.HeaderMatcher{
				"X-Present": {Exists: &f},
			},
		}},
	}
	h := http.Header{}
	h.Set("X-Present", "val")
	ctx := EvalContext{HTTPResponse: &executor.HTTPResponse{StatusCode: 200, Headers: h}}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if allPassed || results[0].Passed {
		t.Fatalf("expected fail: %+v", results)
	}
}

func TestEvaluateHeader_NoMatcher(t *testing.T) {
	e := NewEvaluator()
	assertions := []model.Assertion{
		{Response: &model.ResponseAssertion{
			Headers: map[string]model.HeaderMatcher{
				"X-Any": {},
			},
		}},
	}
	h := http.Header{}
	ctx := EvalContext{HTTPResponse: &executor.HTTPResponse{StatusCode: 200, Headers: h}}
	results, _ := e.EvaluateAll(assertions, ctx)
	if !results[0].Passed || results[0].Message != "No header matcher specified" {
		t.Fatalf("expected pass with no matcher: %+v", results)
	}
}

// ---------------------------------------------------------------------------
// Body assertions
// ---------------------------------------------------------------------------

func TestEvaluateBody_Contains(t *testing.T) {
	e := NewEvaluator()
	assertions := []model.Assertion{
		{Response: &model.ResponseAssertion{
			Body: &model.ResponseBodyAssertion{Contains: "hello"},
		}},
	}
	ctx := EvalContext{HTTPResponse: &executor.HTTPResponse{
		StatusCode: 200, Headers: http.Header{}, Body: []byte("say hello world"),
	}}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if !allPassed || !results[0].Passed {
		t.Fatalf("expected pass: %+v", results)
	}
}

func TestEvaluateBody_ContainsFail(t *testing.T) {
	e := NewEvaluator()
	assertions := []model.Assertion{
		{Response: &model.ResponseAssertion{
			Body: &model.ResponseBodyAssertion{Contains: "missing"},
		}},
	}
	ctx := EvalContext{HTTPResponse: &executor.HTTPResponse{
		StatusCode: 200, Headers: http.Header{}, Body: []byte("nothing here"),
	}}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if allPassed || results[0].Passed {
		t.Fatalf("expected fail: %+v", results)
	}
}

func TestEvaluateBody_Equals(t *testing.T) {
	e := NewEvaluator()
	assertions := []model.Assertion{
		{Response: &model.ResponseAssertion{
			Body: &model.ResponseBodyAssertion{Equals: "exact"},
		}},
	}
	ctx := EvalContext{HTTPResponse: &executor.HTTPResponse{
		StatusCode: 200, Headers: http.Header{}, Body: []byte("exact"),
	}}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if !allPassed || !results[0].Passed {
		t.Fatalf("expected pass: %+v", results)
	}
}

func TestEvaluateBody_EqualsFail(t *testing.T) {
	e := NewEvaluator()
	assertions := []model.Assertion{
		{Response: &model.ResponseAssertion{
			Body: &model.ResponseBodyAssertion{Equals: "exact"},
		}},
	}
	ctx := EvalContext{HTTPResponse: &executor.HTTPResponse{
		StatusCode: 200, Headers: http.Header{}, Body: []byte("different"),
	}}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if allPassed || results[0].Passed {
		t.Fatalf("expected fail: %+v", results)
	}
}

func TestEvaluateBody_MinSize(t *testing.T) {
	e := NewEvaluator()
	minSize := int64(5)
	assertions := []model.Assertion{
		{Response: &model.ResponseAssertion{
			Body: &model.ResponseBodyAssertion{MinSize: &minSize},
		}},
	}
	ctx := EvalContext{HTTPResponse: &executor.HTTPResponse{
		StatusCode: 200, Headers: http.Header{}, Body: []byte("hello world"),
	}}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if !allPassed || !results[0].Passed {
		t.Fatalf("expected pass: %+v", results)
	}
}

func TestEvaluateBody_MinSizeFail(t *testing.T) {
	e := NewEvaluator()
	minSize := int64(100)
	assertions := []model.Assertion{
		{Response: &model.ResponseAssertion{
			Body: &model.ResponseBodyAssertion{MinSize: &minSize},
		}},
	}
	ctx := EvalContext{HTTPResponse: &executor.HTTPResponse{
		StatusCode: 200, Headers: http.Header{}, Body: []byte("hi"),
	}}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if allPassed || results[0].Passed {
		t.Fatalf("expected fail: %+v", results)
	}
}

// ---------------------------------------------------------------------------
// JSON path assertions
// ---------------------------------------------------------------------------

func TestEvaluateJsonPath_ValueMatch(t *testing.T) {
	e := NewEvaluator()
	assertions := []model.Assertion{
		{Response: &model.ResponseAssertion{
			Body: &model.ResponseBodyAssertion{
				JsonPath: []model.JsonPathCheck{
					{Path: "$.name", Expected: "alice"},
				},
			},
		}},
	}
	ctx := EvalContext{HTTPResponse: &executor.HTTPResponse{
		StatusCode: 200, Headers: http.Header{}, Body: []byte(`{"name":"alice"}`),
	}}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if !allPassed || !results[0].Passed {
		t.Fatalf("expected pass: %+v", results)
	}
}

func TestEvaluateJsonPath_ValueMismatch(t *testing.T) {
	e := NewEvaluator()
	assertions := []model.Assertion{
		{Response: &model.ResponseAssertion{
			Body: &model.ResponseBodyAssertion{
				JsonPath: []model.JsonPathCheck{
					{Path: "$.name", Expected: "bob"},
				},
			},
		}},
	}
	ctx := EvalContext{HTTPResponse: &executor.HTTPResponse{
		StatusCode: 200, Headers: http.Header{}, Body: []byte(`{"name":"alice"}`),
	}}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if allPassed || results[0].Passed {
		t.Fatalf("expected fail: %+v", results)
	}
}

func TestEvaluateJsonPath_Exists(t *testing.T) {
	e := NewEvaluator()
	assertions := []model.Assertion{
		{Response: &model.ResponseAssertion{
			Body: &model.ResponseBodyAssertion{
				JsonPath: []model.JsonPathCheck{
					{Path: "$.name", Expected: "exists"},
				},
			},
		}},
	}
	ctx := EvalContext{HTTPResponse: &executor.HTTPResponse{
		StatusCode: 200, Headers: http.Header{}, Body: []byte(`{"name":"alice"}`),
	}}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if !allPassed || !results[0].Passed {
		t.Fatalf("expected pass: %+v", results)
	}
}

func TestEvaluateJsonPath_ExistsNotFound(t *testing.T) {
	e := NewEvaluator()
	assertions := []model.Assertion{
		{Response: &model.ResponseAssertion{
			Body: &model.ResponseBodyAssertion{
				JsonPath: []model.JsonPathCheck{
					{Path: "$.missing", Expected: "exists"},
				},
			},
		}},
	}
	ctx := EvalContext{HTTPResponse: &executor.HTTPResponse{
		StatusCode: 200, Headers: http.Header{}, Body: []byte(`{"name":"alice"}`),
	}}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if allPassed || results[0].Passed {
		t.Fatalf("expected fail: %+v", results)
	}
}

func TestEvaluateJsonPath_InvalidJSON(t *testing.T) {
	e := NewEvaluator()
	assertions := []model.Assertion{
		{Response: &model.ResponseAssertion{
			Body: &model.ResponseBodyAssertion{
				JsonPath: []model.JsonPathCheck{
					{Path: "$.name", Expected: "alice"},
				},
			},
		}},
	}
	ctx := EvalContext{HTTPResponse: &executor.HTTPResponse{
		StatusCode: 200, Headers: http.Header{}, Body: []byte(`not json`),
	}}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if allPassed || results[0].Passed {
		t.Fatalf("expected fail for invalid JSON: %+v", results)
	}
	if results[0].Actual != "(invalid JSON)" {
		t.Fatalf("expected (invalid JSON) actual, got %q", results[0].Actual)
	}
}

func TestEvaluateJsonPath_PathNotFound(t *testing.T) {
	e := NewEvaluator()
	assertions := []model.Assertion{
		{Response: &model.ResponseAssertion{
			Body: &model.ResponseBodyAssertion{
				JsonPath: []model.JsonPathCheck{
					{Path: "$.deep.missing", Expected: "value"},
				},
			},
		}},
	}
	ctx := EvalContext{HTTPResponse: &executor.HTTPResponse{
		StatusCode: 200, Headers: http.Header{}, Body: []byte(`{"deep":{}}`),
	}}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if allPassed || results[0].Passed {
		t.Fatalf("expected fail: %+v", results)
	}
	if results[0].Actual != "(path not found)" {
		t.Fatalf("expected (path not found) actual, got %q", results[0].Actual)
	}
}

func TestEvaluateJsonPath_NumberValue(t *testing.T) {
	e := NewEvaluator()
	assertions := []model.Assertion{
		{Response: &model.ResponseAssertion{
			Body: &model.ResponseBodyAssertion{
				JsonPath: []model.JsonPathCheck{
					{Path: "$.count", Expected: "42"},
				},
			},
		}},
	}
	ctx := EvalContext{HTTPResponse: &executor.HTTPResponse{
		StatusCode: 200, Headers: http.Header{}, Body: []byte(`{"count":42}`),
	}}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if !allPassed || !results[0].Passed {
		t.Fatalf("expected pass: %+v", results)
	}
}

func TestEvaluateJsonPath_BooleanValue(t *testing.T) {
	e := NewEvaluator()
	assertions := []model.Assertion{
		{Response: &model.ResponseAssertion{
			Body: &model.ResponseBodyAssertion{
				JsonPath: []model.JsonPathCheck{
					{Path: "$.active", Expected: "true"},
				},
			},
		}},
	}
	ctx := EvalContext{HTTPResponse: &executor.HTTPResponse{
		StatusCode: 200, Headers: http.Header{}, Body: []byte(`{"active":true}`),
	}}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if !allPassed || !results[0].Passed {
		t.Fatalf("expected pass: %+v", results)
	}
}

func TestEvaluateJsonPath_NullValue(t *testing.T) {
	e := NewEvaluator()
	assertions := []model.Assertion{
		{Response: &model.ResponseAssertion{
			Body: &model.ResponseBodyAssertion{
				JsonPath: []model.JsonPathCheck{
					{Path: "$.value", Expected: "null"},
				},
			},
		}},
	}
	ctx := EvalContext{HTTPResponse: &executor.HTTPResponse{
		StatusCode: 200, Headers: http.Header{}, Body: []byte(`{"value":null}`),
	}}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if !allPassed || !results[0].Passed {
		t.Fatalf("expected pass: %+v", results)
	}
}

// ---------------------------------------------------------------------------
// Response time assertions
// ---------------------------------------------------------------------------

func TestEvaluateTime_Pass(t *testing.T) {
	e := NewEvaluator()
	assertions := []model.Assertion{
		{Response: &model.ResponseAssertion{
			Time: &model.ResponseTimeAssertion{MaxMs: 500},
		}},
	}
	ctx := EvalContext{
		HTTPResponse:      &executor.HTTPResponse{StatusCode: 200, Headers: http.Header{}},
		ExecutionDuration: 200 * time.Millisecond,
	}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if !allPassed || !results[0].Passed {
		t.Fatalf("expected pass: %+v", results)
	}
}

func TestEvaluateTime_Fail(t *testing.T) {
	e := NewEvaluator()
	assertions := []model.Assertion{
		{Response: &model.ResponseAssertion{
			Time: &model.ResponseTimeAssertion{MaxMs: 100},
		}},
	}
	ctx := EvalContext{
		HTTPResponse:      &executor.HTTPResponse{StatusCode: 200, Headers: http.Header{}},
		ExecutionDuration: 200 * time.Millisecond,
	}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if allPassed || results[0].Passed {
		t.Fatalf("expected fail: %+v", results)
	}
}

// ---------------------------------------------------------------------------
// CLI assertions
// ---------------------------------------------------------------------------

func TestEvaluateCLI_ExitCode(t *testing.T) {
	e := NewEvaluator()
	assertions := []model.Assertion{
		{CLI: &model.CLIAssertion{ExitCode: intPtr(0)}},
	}
	ctx := EvalContext{CLIResponse: &executor.CLIResponse{ExitCode: 0}}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if !allPassed || !results[0].Passed {
		t.Fatalf("expected pass: %+v", results)
	}
}

func TestEvaluateCLI_ExitCodeFail(t *testing.T) {
	e := NewEvaluator()
	assertions := []model.Assertion{
		{CLI: &model.CLIAssertion{ExitCode: intPtr(0)}},
	}
	ctx := EvalContext{CLIResponse: &executor.CLIResponse{ExitCode: 1}}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if allPassed || results[0].Passed {
		t.Fatalf("expected fail: %+v", results)
	}
}

func TestEvaluateCLI_ExitCodeNilResponse(t *testing.T) {
	e := NewEvaluator()
	assertions := []model.Assertion{
		{CLI: &model.CLIAssertion{ExitCode: intPtr(0)}},
	}
	ctx := EvalContext{}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if !allPassed || !results[0].Passed {
		// exit code 0 == 0 when nil response defaults to 0
		t.Fatalf("expected pass (default 0): %+v", results)
	}
}

func TestEvaluateCLI_StdoutContains(t *testing.T) {
	e := NewEvaluator()
	assertions := []model.Assertion{
		{CLI: &model.CLIAssertion{
			Stdout: &model.CLIOutputAssertion{Contains: "success"},
		}},
	}
	ctx := EvalContext{CLIResponse: &executor.CLIResponse{
		ExitCode: 0, Stdout: []byte("operation success!"), Stderr: nil,
	}}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if !allPassed || !results[0].Passed {
		t.Fatalf("expected pass: %+v", results)
	}
}

func TestEvaluateCLI_StdoutEquals(t *testing.T) {
	e := NewEvaluator()
	assertions := []model.Assertion{
		{CLI: &model.CLIAssertion{
			Stdout: &model.CLIOutputAssertion{Equals: "exact output"},
		}},
	}
	ctx := EvalContext{CLIResponse: &executor.CLIResponse{
		ExitCode: 0, Stdout: []byte("exact output"),
	}}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if !allPassed || !results[0].Passed {
		t.Fatalf("expected pass: %+v", results)
	}
}

func TestEvaluateCLI_StderrContains(t *testing.T) {
	e := NewEvaluator()
	assertions := []model.Assertion{
		{CLI: &model.CLIAssertion{
			Stderr: &model.CLIOutputAssertion{Contains: "error"},
		}},
	}
	ctx := EvalContext{CLIResponse: &executor.CLIResponse{
		ExitCode: 1, Stderr: []byte("fatal error occurred"),
	}}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if !allPassed || !results[0].Passed {
		t.Fatalf("expected pass: %+v", results)
	}
}

// ---------------------------------------------------------------------------
// Artifact assertions
// ---------------------------------------------------------------------------

func TestEvaluateArtifact_Exists(t *testing.T) {
	e := NewEvaluator()
	assertions := []model.Assertion{
		{Artifact: &model.ArtifactAssertion{Name: "app.log", Exists: boolPtr(true)}},
	}
	ctx := EvalContext{
		ArtifactInfos: []ArtifactInfo{{Name: "app.log", Path: "/tmp/app.log", Size: 50}},
	}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if !allPassed || !results[0].Passed {
		t.Fatalf("expected pass: %+v", results)
	}
}

func TestEvaluateArtifact_ExistsFail(t *testing.T) {
	e := NewEvaluator()
	assertions := []model.Assertion{
		{Artifact: &model.ArtifactAssertion{Name: "missing.log", Exists: boolPtr(true)}},
	}
	ctx := EvalContext{}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if allPassed || results[0].Passed {
		t.Fatalf("expected fail: %+v", results)
	}
}

func TestEvaluateArtifact_NotExists(t *testing.T) {
	e := NewEvaluator()
	assertions := []model.Assertion{
		{Artifact: &model.ArtifactAssertion{Name: "missing.log", Exists: boolPtr(false)}},
	}
	ctx := EvalContext{}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if !allPassed || !results[0].Passed {
		t.Fatalf("expected pass: %+v", results)
	}
}

func TestEvaluateArtifact_NotExistsFail(t *testing.T) {
	e := NewEvaluator()
	assertions := []model.Assertion{
		{Artifact: &model.ArtifactAssertion{Name: "app.log", Exists: boolPtr(false)}},
	}
	ctx := EvalContext{
		ArtifactInfos: []ArtifactInfo{{Name: "app.log", Path: "/tmp/app.log", Size: 50}},
	}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if allPassed || results[0].Passed {
		t.Fatalf("expected fail: %+v", results)
	}
}

func TestEvaluateArtifact_SizeChecks(t *testing.T) {
	e := NewEvaluator()

	// within bounds
	assertions := []model.Assertion{
		{Artifact: &model.ArtifactAssertion{
			Name: "data.bin", Exists: boolPtr(true),
			MinSize: int64Ptr(10), MaxSize: int64Ptr(1000),
		}},
	}
	ctx := EvalContext{
		ArtifactInfos: []ArtifactInfo{{Name: "data.bin", Path: "/tmp/data.bin", Size: 500}},
	}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if !allPassed {
		t.Fatalf("expected all pass: %+v", results)
	}
}

func TestEvaluateArtifact_MinSizeFail(t *testing.T) {
	e := NewEvaluator()
	assertions := []model.Assertion{
		{Artifact: &model.ArtifactAssertion{
			Name: "data.bin", Exists: boolPtr(true), MinSize: int64Ptr(1000),
		}},
	}
	ctx := EvalContext{
		ArtifactInfos: []ArtifactInfo{{Name: "data.bin", Path: "/tmp/data.bin", Size: 50}},
	}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if allPassed {
		t.Fatalf("expected fail: %+v", results)
	}
}

func TestEvaluateArtifact_MaxSizeFail(t *testing.T) {
	e := NewEvaluator()
	assertions := []model.Assertion{
		{Artifact: &model.ArtifactAssertion{
			Name: "data.bin", Exists: boolPtr(true), MaxSize: int64Ptr(10),
		}},
	}
	ctx := EvalContext{
		ArtifactInfos: []ArtifactInfo{{Name: "data.bin", Path: "/tmp/data.bin", Size: 500}},
	}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if allPassed {
		t.Fatalf("expected fail: %+v", results)
	}
}

func TestEvaluateArtifact_SizeCheckImpliesExists(t *testing.T) {
	e := NewEvaluator()
	assertions := []model.Assertion{
		{Artifact: &model.ArtifactAssertion{
			Name: "missing.bin", MinSize: int64Ptr(10),
		}},
	}
	ctx := EvalContext{}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if allPassed || results[0].Passed {
		t.Fatalf("expected fail when artifact missing for size check: %+v", results)
	}
}

// ---------------------------------------------------------------------------
// Body snapshot assertions
// ---------------------------------------------------------------------------

func TestEvaluateBodySnapshot_Match(t *testing.T) {
	tmpDir := t.TempDir()
	snapshotFile := "snap.json"
	snapshotPath := filepath.Join(tmpDir, snapshotFile)
	body := []byte(`{"result":"ok"}`)
	os.WriteFile(snapshotPath, body, 0644)

	e := NewEvaluator()
	assertions := []model.Assertion{
		{Response: &model.ResponseAssertion{
			Body: &model.ResponseBodyAssertion{Snapshot: snapshotFile},
		}},
	}
	ctx := EvalContext{
		HTTPResponse:  &executor.HTTPResponse{StatusCode: 200, Headers: http.Header{}, Body: body},
		SuiteFilePath: filepath.Join(tmpDir, "suite.yaml"),
	}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if !allPassed || !results[0].Passed {
		t.Fatalf("expected pass: %+v", results)
	}
}

func TestEvaluateBodySnapshot_Mismatch(t *testing.T) {
	tmpDir := t.TempDir()
	snapshotFile := "snap.json"
	snapshotPath := filepath.Join(tmpDir, snapshotFile)
	os.WriteFile(snapshotPath, []byte(`{"result":"old"}`), 0644)

	e := NewEvaluator()
	assertions := []model.Assertion{
		{Response: &model.ResponseAssertion{
			Body: &model.ResponseBodyAssertion{Snapshot: snapshotFile},
		}},
	}
	ctx := EvalContext{
		HTTPResponse:  &executor.HTTPResponse{StatusCode: 200, Headers: http.Header{}, Body: []byte(`{"result":"new"}`)},
		SuiteFilePath: filepath.Join(tmpDir, "suite.yaml"),
	}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if allPassed || results[0].Passed {
		t.Fatalf("expected fail: %+v", results)
	}
}

func TestEvaluateBodySnapshot_MissingFile(t *testing.T) {
	tmpDir := t.TempDir()
	e := NewEvaluator()
	assertions := []model.Assertion{
		{Response: &model.ResponseAssertion{
			Body: &model.ResponseBodyAssertion{Snapshot: "nonexistent.json"},
		}},
	}
	ctx := EvalContext{
		HTTPResponse:  &executor.HTTPResponse{StatusCode: 200, Headers: http.Header{}, Body: []byte("data")},
		SuiteFilePath: filepath.Join(tmpDir, "suite.yaml"),
	}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if allPassed || results[0].Passed {
		t.Fatalf("expected fail: %+v", results)
	}
}

func TestEvaluateBodySnapshot_Regenerate(t *testing.T) {
	tmpDir := t.TempDir()
	snapshotFile := "snap.json"
	body := []byte(`{"result":"new"}`)

	e := NewEvaluator()
	assertions := []model.Assertion{
		{Response: &model.ResponseAssertion{
			Body: &model.ResponseBodyAssertion{Snapshot: snapshotFile},
		}},
	}
	ctx := EvalContext{
		HTTPResponse:  &executor.HTTPResponse{StatusCode: 200, Headers: http.Header{}, Body: body},
		SuiteFilePath: filepath.Join(tmpDir, "suite.yaml"),
		Regenerate:    true,
	}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if !allPassed || !results[0].Passed {
		t.Fatalf("expected pass on regenerate: %+v", results)
	}
	written, err := os.ReadFile(filepath.Join(tmpDir, snapshotFile))
	if err != nil {
		t.Fatalf("snapshot file not written: %v", err)
	}
	if string(written) != string(body) {
		t.Fatalf("snapshot content mismatch")
	}
}

// ---------------------------------------------------------------------------
// CLI output snapshot
// ---------------------------------------------------------------------------

func TestEvaluateCLIOutputSnapshot_Match(t *testing.T) {
	tmpDir := t.TempDir()
	snapFile := "stdout.snap"
	content := []byte("hello world")
	os.WriteFile(filepath.Join(tmpDir, snapFile), content, 0644)

	e := NewEvaluator()
	assertions := []model.Assertion{
		{CLI: &model.CLIAssertion{
			Stdout: &model.CLIOutputAssertion{Snapshot: snapFile},
		}},
	}
	ctx := EvalContext{
		CLIResponse:   &executor.CLIResponse{ExitCode: 0, Stdout: content},
		SuiteFilePath: filepath.Join(tmpDir, "suite.yaml"),
	}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if !allPassed || !results[0].Passed {
		t.Fatalf("expected pass: %+v", results)
	}
}

func TestEvaluateCLIOutputSnapshot_Mismatch(t *testing.T) {
	tmpDir := t.TempDir()
	snapFile := "stdout.snap"
	os.WriteFile(filepath.Join(tmpDir, snapFile), []byte("old"), 0644)

	e := NewEvaluator()
	assertions := []model.Assertion{
		{CLI: &model.CLIAssertion{
			Stdout: &model.CLIOutputAssertion{Snapshot: snapFile},
		}},
	}
	ctx := EvalContext{
		CLIResponse:   &executor.CLIResponse{ExitCode: 0, Stdout: []byte("new")},
		SuiteFilePath: filepath.Join(tmpDir, "suite.yaml"),
	}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if allPassed || results[0].Passed {
		t.Fatalf("expected fail: %+v", results)
	}
}

func TestEvaluateCLIOutputSnapshot_Missing(t *testing.T) {
	tmpDir := t.TempDir()
	e := NewEvaluator()
	assertions := []model.Assertion{
		{CLI: &model.CLIAssertion{
			Stdout: &model.CLIOutputAssertion{Snapshot: "missing.snap"},
		}},
	}
	ctx := EvalContext{
		CLIResponse:   &executor.CLIResponse{ExitCode: 0, Stdout: []byte("data")},
		SuiteFilePath: filepath.Join(tmpDir, "suite.yaml"),
	}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if allPassed || results[0].Passed {
		t.Fatalf("expected fail: %+v", results)
	}
}

func TestEvaluateCLIOutputSnapshot_Regenerate(t *testing.T) {
	tmpDir := t.TempDir()
	snapFile := "stdout.snap"
	content := []byte("regenerated")

	e := NewEvaluator()
	assertions := []model.Assertion{
		{CLI: &model.CLIAssertion{
			Stdout: &model.CLIOutputAssertion{Snapshot: snapFile},
		}},
	}
	ctx := EvalContext{
		CLIResponse:   &executor.CLIResponse{ExitCode: 0, Stdout: content},
		SuiteFilePath: filepath.Join(tmpDir, "suite.yaml"),
		Regenerate:    true,
	}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if !allPassed || !results[0].Passed {
		t.Fatalf("expected pass: %+v", results)
	}
	written, _ := os.ReadFile(filepath.Join(tmpDir, snapFile))
	if string(written) != "regenerated" {
		t.Fatalf("snapshot not written correctly")
	}
}

// ---------------------------------------------------------------------------
// EvaluateAll combined
// ---------------------------------------------------------------------------

func TestEvaluateAll_MultipleAssertions(t *testing.T) {
	e := NewEvaluator()
	code := 200
	assertions := []model.Assertion{
		{Response: &model.ResponseAssertion{
			StatusCode: &code,
			Body:       &model.ResponseBodyAssertion{Contains: "ok"},
		}},
		{CLI: &model.CLIAssertion{ExitCode: intPtr(0)}},
	}
	ctx := EvalContext{
		HTTPResponse: &executor.HTTPResponse{
			StatusCode: 200, Headers: http.Header{}, Body: []byte("ok"),
		},
		CLIResponse: &executor.CLIResponse{ExitCode: 0},
	}
	results, allPassed := e.EvaluateAll(assertions, ctx)
	if !allPassed {
		t.Fatalf("expected all pass: %+v", results)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
}

func TestEvaluateAll_Empty(t *testing.T) {
	e := NewEvaluator()
	results, allPassed := e.EvaluateAll(nil, EvalContext{})
	if !allPassed || len(results) != 0 {
		t.Fatalf("expected empty pass: %+v", results)
	}
}

// ---------------------------------------------------------------------------
// nextSegment
// ---------------------------------------------------------------------------

func TestNextSegment(t *testing.T) {
	tests := []struct {
		path    string
		segment string
		rest    string
	}{
		{"name", "name", ""},
		{"name.age", "name", "age"},
		{"items[0].name", "items[0]", "name"},
		{"items[0]", "items[0]", ""},
		{"a.b.c", "a", "b.c"},
	}
	for _, tt := range tests {
		seg, rest := nextSegment(tt.path)
		if seg != tt.segment || rest != tt.rest {
			t.Errorf("nextSegment(%q) = (%q, %q), want (%q, %q)", tt.path, seg, rest, tt.segment, tt.rest)
		}
	}
}
