// Package assertion provides assertion evaluation for test responses.
package assertion

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"chiperka-worker/internal/executor"
	"chiperka-worker/internal/model"
)

// Evaluator checks assertions against test responses.
type Evaluator struct{}

// NewEvaluator creates a new assertion evaluator.
func NewEvaluator() *Evaluator {
	return &Evaluator{}
}

// ArtifactInfo describes a collected artifact.
type ArtifactInfo struct {
	Name string
	Path string
	Size int64
}

// EvalContext provides all context needed for assertion evaluation.
type EvalContext struct {
	HTTPResponse      *executor.HTTPResponse
	CLIResponse       *executor.CLIResponse
	ExecutionDuration time.Duration
	SuiteFilePath     string
	Regenerate        bool
	ArtifactInfos     []ArtifactInfo
}

// EvaluateAll checks all assertions against the provided context.
// Returns a slice of assertion results and whether all assertions passed.
func (e *Evaluator) EvaluateAll(assertions []model.Assertion, ctx EvalContext) ([]model.AssertionResult, bool) {
	var results []model.AssertionResult
	allPassed := true

	for _, assertion := range assertions {
		var subResults []model.AssertionResult

		if assertion.Response != nil {
			subResults = e.evaluateResponse(assertion.Response, ctx)
		}
		if assertion.CLI != nil {
			subResults = append(subResults, e.evaluateCLI(assertion.CLI, ctx)...)
		}
		if assertion.Artifact != nil {
			subResults = append(subResults, e.evaluateArtifact(assertion.Artifact, ctx)...)
		}

		for i := range subResults {
			subResults[i].Duration = 0 // Individual sub-result timing not meaningful
			if !subResults[i].Passed {
				allPassed = false
			}
		}
		results = append(results, subResults...)
	}

	return results, allPassed
}

// --- Response assertions ---

func (e *Evaluator) evaluateResponse(ra *model.ResponseAssertion, ctx EvalContext) []model.AssertionResult {
	var results []model.AssertionResult
	resp := ctx.HTTPResponse

	// statusCode
	if ra.StatusCode != nil {
		actual := 0
		if resp != nil {
			actual = resp.StatusCode
		}
		passed := actual == *ra.StatusCode
		r := model.AssertionResult{
			Passed:   passed,
			Type:     "response.statusCode",
			Expected: fmt.Sprintf("%d", *ra.StatusCode),
			Actual:   fmt.Sprintf("%d", actual),
		}
		if passed {
			r.Message = fmt.Sprintf("Status code is %d", actual)
		} else {
			r.Message = fmt.Sprintf("Expected status code %d, got %d", *ra.StatusCode, actual)
		}
		results = append(results, r)
	}

	// headers
	if ra.Headers != nil && resp != nil {
		for name, matcher := range ra.Headers {
			results = append(results, e.evaluateHeader(name, matcher, resp))
		}
	}

	// body
	if ra.Body != nil && resp != nil {
		results = append(results, e.evaluateResponseBody(ra.Body, resp, ctx)...)
	}

	// time
	if ra.Time != nil {
		actualMs := ctx.ExecutionDuration.Milliseconds()
		passed := actualMs <= int64(ra.Time.MaxMs)
		r := model.AssertionResult{
			Passed:   passed,
			Type:     "response.time",
			Expected: fmt.Sprintf("<= %dms", ra.Time.MaxMs),
			Actual:   fmt.Sprintf("%dms", actualMs),
		}
		if passed {
			r.Message = fmt.Sprintf("Response time %dms within limit %dms", actualMs, ra.Time.MaxMs)
		} else {
			r.Message = fmt.Sprintf("Response time %dms exceeds limit %dms", actualMs, ra.Time.MaxMs)
		}
		results = append(results, r)
	}

	return results
}

func (e *Evaluator) evaluateHeader(name string, matcher model.HeaderMatcher, resp *executor.HTTPResponse) model.AssertionResult {
	actual := resp.Headers.Get(name)
	headerExists := resp.Headers.Get(name) != "" || len(resp.Headers.Values(name)) > 0

	if matcher.Exists != nil {
		passed := headerExists == *matcher.Exists
		r := model.AssertionResult{
			Passed: passed,
			Type:   fmt.Sprintf("response.headers.%s", name),
		}
		if *matcher.Exists {
			r.Expected = "exists"
			if headerExists {
				r.Actual = "exists"
				r.Message = fmt.Sprintf("Header %q exists", name)
			} else {
				r.Actual = "missing"
				r.Message = fmt.Sprintf("Header %q not found", name)
			}
		} else {
			r.Expected = "not exists"
			if headerExists {
				r.Actual = "exists"
				r.Message = fmt.Sprintf("Header %q exists but expected it not to", name)
			} else {
				r.Actual = "missing"
				r.Message = fmt.Sprintf("Header %q correctly absent", name)
			}
		}
		return r
	}

	if matcher.Equals != "" {
		passed := actual == matcher.Equals
		r := model.AssertionResult{
			Passed:   passed,
			Type:     fmt.Sprintf("response.headers.%s", name),
			Expected: matcher.Equals,
			Actual:   actual,
		}
		if passed {
			r.Message = fmt.Sprintf("Header %q equals %q", name, matcher.Equals)
		} else {
			r.Message = fmt.Sprintf("Header %q: expected %q, got %q", name, matcher.Equals, actual)
		}
		return r
	}

	if matcher.Contains != "" {
		passed := strings.Contains(actual, matcher.Contains)
		r := model.AssertionResult{
			Passed:   passed,
			Type:     fmt.Sprintf("response.headers.%s", name),
			Expected: fmt.Sprintf("contains %q", matcher.Contains),
			Actual:   actual,
		}
		if passed {
			r.Message = fmt.Sprintf("Header %q contains %q", name, matcher.Contains)
		} else {
			r.Message = fmt.Sprintf("Header %q does not contain %q", name, matcher.Contains)
		}
		return r
	}

	return model.AssertionResult{
		Passed:  true,
		Type:    fmt.Sprintf("response.headers.%s", name),
		Message: "No header matcher specified",
	}
}

func (e *Evaluator) evaluateResponseBody(body *model.ResponseBodyAssertion, resp *executor.HTTPResponse, ctx EvalContext) []model.AssertionResult {
	var results []model.AssertionResult
	bodyStr := string(resp.Body)

	// jsonPath checks
	for i, jp := range body.JsonPath {
		results = append(results, e.evaluateJsonPath(i, jp, resp.Body))
	}

	// contains
	if body.Contains != "" {
		passed := strings.Contains(bodyStr, body.Contains)
		r := model.AssertionResult{
			Passed:   passed,
			Type:     "response.body.contains",
			Expected: fmt.Sprintf("contains %q", body.Contains),
			Actual:   truncateString(bodyStr, 200),
		}
		if passed {
			r.Message = fmt.Sprintf("Body contains %q", body.Contains)
		} else {
			r.Message = fmt.Sprintf("Body does not contain %q", body.Contains)
		}
		results = append(results, r)
	}

	// equals
	if body.Equals != "" {
		passed := bodyStr == body.Equals
		r := model.AssertionResult{
			Passed:   passed,
			Type:     "response.body.equals",
			Expected: truncateString(body.Equals, 200),
			Actual:   truncateString(bodyStr, 200),
		}
		if passed {
			r.Message = "Body matches expected value"
		} else {
			r.Message = "Body does not match expected value"
		}
		results = append(results, r)
	}

	// minSize
	if body.MinSize != nil {
		size := int64(len(resp.Body))
		passed := size >= *body.MinSize
		r := model.AssertionResult{
			Passed:   passed,
			Type:     "response.body.minSize",
			Expected: fmt.Sprintf(">= %d", *body.MinSize),
			Actual:   fmt.Sprintf("%d", size),
		}
		if passed {
			r.Message = fmt.Sprintf("Body size %d >= %d", size, *body.MinSize)
		} else {
			r.Message = fmt.Sprintf("Body size %d < minimum %d", size, *body.MinSize)
		}
		results = append(results, r)
	}

	// snapshot
	if body.Snapshot != "" {
		results = append(results, e.evaluateBodySnapshot(body.Snapshot, resp.Body, ctx))
	}

	return results
}

func (e *Evaluator) evaluateJsonPath(index int, jp model.JsonPathCheck, body []byte) model.AssertionResult {
	typeName := fmt.Sprintf("response.body.jsonPath[%d]", index)

	var data interface{}
	if err := json.Unmarshal(body, &data); err != nil {
		return model.AssertionResult{
			Passed:   false,
			Type:     typeName,
			Expected: jp.Expected,
			Actual:   "(invalid JSON)",
			Message:  fmt.Sprintf("Failed to parse response body as JSON: %v", err),
		}
	}

	value, found := walkJsonPath(data, jp.Path)

	if jp.Expected == "exists" {
		passed := found
		r := model.AssertionResult{
			Passed:   passed,
			Type:     typeName,
			Expected: fmt.Sprintf("%s exists", jp.Path),
		}
		if found {
			r.Actual = "exists"
			r.Message = fmt.Sprintf("Path %s exists", jp.Path)
		} else {
			r.Actual = "not found"
			r.Message = fmt.Sprintf("Path %s not found", jp.Path)
		}
		return r
	}

	if !found {
		return model.AssertionResult{
			Passed:   false,
			Type:     typeName,
			Expected: jp.Expected,
			Actual:   "(path not found)",
			Message:  fmt.Sprintf("Path %s not found in response", jp.Path),
		}
	}

	actualStr := jsonValueToString(value)
	passed := actualStr == jp.Expected
	r := model.AssertionResult{
		Passed:   passed,
		Type:     typeName,
		Expected: jp.Expected,
		Actual:   actualStr,
	}
	if passed {
		r.Message = fmt.Sprintf("Path %s equals %q", jp.Path, jp.Expected)
	} else {
		r.Message = fmt.Sprintf("Path %s: expected %q, got %q", jp.Path, jp.Expected, actualStr)
	}
	return r
}

func (e *Evaluator) evaluateBodySnapshot(snapshotFile string, body []byte, ctx EvalContext) model.AssertionResult {
	suiteDir := filepath.Dir(ctx.SuiteFilePath)
	snapshotPath := filepath.Join(suiteDir, snapshotFile)

	if ctx.Regenerate {
		if err := os.MkdirAll(filepath.Dir(snapshotPath), 0755); err != nil {
			return model.AssertionResult{Passed: false, Type: "response.body.snapshot", Message: fmt.Sprintf("Failed to create directory: %v", err)}
		}
		if err := os.WriteFile(snapshotPath, body, 0644); err != nil {
			return model.AssertionResult{Passed: false, Type: "response.body.snapshot", Message: fmt.Sprintf("Failed to write snapshot: %v", err)}
		}
		return model.AssertionResult{Passed: true, Type: "response.body.snapshot", Expected: snapshotFile, Actual: snapshotFile, Message: fmt.Sprintf("Snapshot %q updated", snapshotFile)}
	}

	expected, err := os.ReadFile(snapshotPath)
	if err != nil {
		if os.IsNotExist(err) {
			return model.AssertionResult{Passed: false, Type: "response.body.snapshot", Expected: snapshotFile, Actual: "(file missing)", Message: fmt.Sprintf("Snapshot file %q not found. Run with --regenerate-snapshots to create.", snapshotFile)}
		}
		return model.AssertionResult{Passed: false, Type: "response.body.snapshot", Message: fmt.Sprintf("Failed to read snapshot: %v", err)}
	}

	if bytes.Equal(body, expected) {
		return model.AssertionResult{Passed: true, Type: "response.body.snapshot", Expected: snapshotFile, Actual: snapshotFile, Message: fmt.Sprintf("Body matches snapshot %q", snapshotFile)}
	}
	return model.AssertionResult{Passed: false, Type: "response.body.snapshot", Expected: snapshotFile, Actual: "(differs)", Message: fmt.Sprintf("Body does not match snapshot %q. Run with --regenerate-snapshots to update.", snapshotFile)}
}

// --- CLI assertions ---

func (e *Evaluator) evaluateCLI(ca *model.CLIAssertion, ctx EvalContext) []model.AssertionResult {
	var results []model.AssertionResult
	resp := ctx.CLIResponse

	if ca.ExitCode != nil {
		actual := 0
		if resp != nil {
			actual = resp.ExitCode
		}
		passed := actual == *ca.ExitCode
		r := model.AssertionResult{
			Passed:   passed,
			Type:     "cli.exitCode",
			Expected: fmt.Sprintf("%d", *ca.ExitCode),
			Actual:   fmt.Sprintf("%d", actual),
		}
		if passed {
			r.Message = fmt.Sprintf("Exit code is %d", actual)
		} else {
			r.Message = fmt.Sprintf("Expected exit code %d, got %d", *ca.ExitCode, actual)
		}
		results = append(results, r)
	}

	if ca.Stdout != nil && resp != nil {
		results = append(results, e.evaluateCLIOutput("cli.stdout", ca.Stdout, resp.Stdout, ctx)...)
	}

	if ca.Stderr != nil && resp != nil {
		results = append(results, e.evaluateCLIOutput("cli.stderr", ca.Stderr, resp.Stderr, ctx)...)
	}

	return results
}

func (e *Evaluator) evaluateCLIOutput(typeName string, oa *model.CLIOutputAssertion, output []byte, ctx EvalContext) []model.AssertionResult {
	var results []model.AssertionResult
	actual := string(output)

	if oa.Contains != "" {
		passed := strings.Contains(actual, oa.Contains)
		r := model.AssertionResult{
			Passed:   passed,
			Type:     typeName,
			Expected: fmt.Sprintf("contains %q", oa.Contains),
			Actual:   truncateString(actual, 200),
		}
		if passed {
			r.Message = fmt.Sprintf("%s contains %q", typeName, oa.Contains)
		} else {
			r.Message = fmt.Sprintf("%s does not contain %q", typeName, oa.Contains)
		}
		results = append(results, r)
	}

	if oa.Equals != "" {
		passed := actual == oa.Equals
		r := model.AssertionResult{
			Passed:   passed,
			Type:     typeName,
			Expected: truncateString(oa.Equals, 200),
			Actual:   truncateString(actual, 200),
		}
		if passed {
			r.Message = fmt.Sprintf("%s matches expected value", typeName)
		} else {
			r.Message = fmt.Sprintf("%s does not match expected value", typeName)
		}
		results = append(results, r)
	}

	if oa.Snapshot != "" {
		suiteDir := filepath.Dir(ctx.SuiteFilePath)
		snapshotPath := filepath.Join(suiteDir, oa.Snapshot)

		if ctx.Regenerate {
			if err := os.MkdirAll(filepath.Dir(snapshotPath), 0755); err != nil {
				results = append(results, model.AssertionResult{Passed: false, Type: typeName, Message: fmt.Sprintf("Failed to create directory: %v", err)})
				return results
			}
			if err := os.WriteFile(snapshotPath, output, 0644); err != nil {
				results = append(results, model.AssertionResult{Passed: false, Type: typeName, Message: fmt.Sprintf("Failed to write snapshot: %v", err)})
				return results
			}
			results = append(results, model.AssertionResult{Passed: true, Type: typeName, Expected: oa.Snapshot, Actual: oa.Snapshot, Message: fmt.Sprintf("Snapshot %q updated", oa.Snapshot)})
			return results
		}

		expected, err := os.ReadFile(snapshotPath)
		if err != nil {
			if os.IsNotExist(err) {
				results = append(results, model.AssertionResult{Passed: false, Type: typeName, Expected: oa.Snapshot, Actual: "(file missing)", Message: fmt.Sprintf("Snapshot file %q not found. Run with --regenerate-snapshots to create.", oa.Snapshot)})
			} else {
				results = append(results, model.AssertionResult{Passed: false, Type: typeName, Message: fmt.Sprintf("Failed to read snapshot: %v", err)})
			}
			return results
		}

		if bytes.Equal(output, expected) {
			results = append(results, model.AssertionResult{Passed: true, Type: typeName, Expected: oa.Snapshot, Actual: oa.Snapshot, Message: fmt.Sprintf("%s matches snapshot %q", typeName, oa.Snapshot)})
		} else {
			results = append(results, model.AssertionResult{Passed: false, Type: typeName, Expected: oa.Snapshot, Actual: "(differs)", Message: fmt.Sprintf("%s does not match snapshot %q", typeName, oa.Snapshot)})
		}
	}

	return results
}

// --- Artifact assertions ---

func (e *Evaluator) evaluateArtifact(aa *model.ArtifactAssertion, ctx EvalContext) []model.AssertionResult {
	var results []model.AssertionResult
	artifact := findArtifact(aa.Name, ctx.ArtifactInfos)
	hasOtherChecks := aa.MinSize != nil || aa.MaxSize != nil || aa.Snapshot != ""

	// exists check
	if aa.Exists != nil {
		if *aa.Exists {
			if artifact == nil {
				results = append(results, model.AssertionResult{
					Passed: false, Type: fmt.Sprintf("artifact.%s.exists", aa.Name),
					Expected: "exists", Actual: "not found",
					Message: fmt.Sprintf("Artifact %q not found", aa.Name),
				})
				return results
			}
			results = append(results, model.AssertionResult{
				Passed: true, Type: fmt.Sprintf("artifact.%s.exists", aa.Name),
				Expected: "exists", Actual: "exists",
				Message: fmt.Sprintf("Artifact %q exists", aa.Name),
			})
		} else {
			if artifact != nil {
				results = append(results, model.AssertionResult{
					Passed: false, Type: fmt.Sprintf("artifact.%s.exists", aa.Name),
					Expected: "not exists", Actual: "exists",
					Message: fmt.Sprintf("Artifact %q exists but expected it not to", aa.Name),
				})
			} else {
				results = append(results, model.AssertionResult{
					Passed: true, Type: fmt.Sprintf("artifact.%s.exists", aa.Name),
					Expected: "not exists", Actual: "not found",
					Message: fmt.Sprintf("Artifact %q correctly does not exist", aa.Name),
				})
			}
			return results
		}
	} else if hasOtherChecks && artifact == nil {
		results = append(results, model.AssertionResult{
			Passed: false, Type: fmt.Sprintf("artifact.%s.exists", aa.Name),
			Expected: "exists", Actual: "not found",
			Message: fmt.Sprintf("Artifact %q not found (required by other checks)", aa.Name),
		})
		return results
	}

	if artifact == nil {
		return results
	}

	// size checks
	if aa.MinSize != nil || aa.MaxSize != nil {
		passed := true
		var msg string
		if aa.MinSize != nil && artifact.Size < *aa.MinSize {
			passed = false
			msg = fmt.Sprintf("Artifact %q size %d below minimum %d", aa.Name, artifact.Size, *aa.MinSize)
		} else if aa.MaxSize != nil && artifact.Size > *aa.MaxSize {
			passed = false
			msg = fmt.Sprintf("Artifact %q size %d exceeds maximum %d", aa.Name, artifact.Size, *aa.MaxSize)
		} else {
			msg = fmt.Sprintf("Artifact %q size %d within bounds", aa.Name, artifact.Size)
		}

		expected := "size"
		if aa.MinSize != nil {
			expected += fmt.Sprintf(" >= %d", *aa.MinSize)
		}
		if aa.MaxSize != nil {
			expected += fmt.Sprintf(" <= %d", *aa.MaxSize)
		}

		results = append(results, model.AssertionResult{
			Passed:   passed,
			Type:     fmt.Sprintf("artifact.%s.size", aa.Name),
			Expected: expected,
			Actual:   fmt.Sprintf("%d", artifact.Size),
			Message:  msg,
		})
	}

	// snapshot
	if aa.Snapshot != "" {
		suiteDir := filepath.Dir(ctx.SuiteFilePath)
		snapshotPath := filepath.Join(suiteDir, aa.Snapshot)
		typeName := fmt.Sprintf("artifact.%s.snapshot", aa.Name)

		actualContent, err := os.ReadFile(artifact.Path)
		if err != nil {
			results = append(results, model.AssertionResult{Passed: false, Type: typeName, Message: fmt.Sprintf("Failed to read artifact: %v", err)})
			return results
		}

		if ctx.Regenerate {
			if err := os.MkdirAll(filepath.Dir(snapshotPath), 0755); err != nil {
				results = append(results, model.AssertionResult{Passed: false, Type: typeName, Message: fmt.Sprintf("Failed to create directory: %v", err)})
				return results
			}
			if err := os.WriteFile(snapshotPath, actualContent, 0644); err != nil {
				results = append(results, model.AssertionResult{Passed: false, Type: typeName, Message: fmt.Sprintf("Failed to write snapshot: %v", err)})
				return results
			}
			results = append(results, model.AssertionResult{Passed: true, Type: typeName, Expected: aa.Snapshot, Actual: aa.Snapshot, Message: fmt.Sprintf("Snapshot %q updated", aa.Snapshot)})
			return results
		}

		expectedContent, err := os.ReadFile(snapshotPath)
		if err != nil {
			if os.IsNotExist(err) {
				results = append(results, model.AssertionResult{Passed: false, Type: typeName, Expected: aa.Snapshot, Actual: "(file missing)", Message: fmt.Sprintf("Snapshot file %q not found. Run with --regenerate-snapshots to create.", aa.Snapshot)})
			} else {
				results = append(results, model.AssertionResult{Passed: false, Type: typeName, Message: fmt.Sprintf("Failed to read snapshot: %v", err)})
			}
			return results
		}

		if bytes.Equal(actualContent, expectedContent) {
			results = append(results, model.AssertionResult{Passed: true, Type: typeName, Expected: aa.Snapshot, Actual: aa.Snapshot, Message: fmt.Sprintf("Artifact %q matches snapshot %q", aa.Name, aa.Snapshot)})
		} else {
			results = append(results, model.AssertionResult{Passed: false, Type: typeName, Expected: aa.Snapshot, Actual: "(differs)", Message: fmt.Sprintf("Artifact %q does not match snapshot %q", aa.Name, aa.Snapshot)})
		}
	}

	return results
}

// --- JSONPath walker ---

// walkJsonPath evaluates a simple JSONPath expression against parsed JSON data.
// Supports: $.field, $.nested.field, $.array[0], $.array[-1], $.array[*].field
func walkJsonPath(data interface{}, path string) (interface{}, bool) {
	if !strings.HasPrefix(path, "$") {
		return nil, false
	}

	path = strings.TrimPrefix(path, "$")
	if path == "" {
		return data, true
	}
	path = strings.TrimPrefix(path, ".")

	return walkPath(data, path)
}

func walkPath(data interface{}, path string) (interface{}, bool) {
	if path == "" {
		return data, true
	}

	// Parse next segment
	segment, rest := nextSegment(path)

	// Handle array index: segment[index]
	if idx := strings.Index(segment, "["); idx >= 0 {
		field := segment[:idx]
		indexStr := segment[idx+1 : len(segment)-1]

		var current interface{} = data
		if field != "" {
			obj, ok := current.(map[string]interface{})
			if !ok {
				return nil, false
			}
			val, exists := obj[field]
			if !exists {
				return nil, false
			}
			current = val
		}

		arr, ok := current.([]interface{})
		if !ok {
			return nil, false
		}

		if indexStr == "*" {
			// Wildcard: collect from all elements
			var collected []interface{}
			for _, elem := range arr {
				if rest == "" {
					collected = append(collected, elem)
				} else {
					if val, found := walkPath(elem, rest); found {
						collected = append(collected, val)
					}
				}
			}
			if len(collected) == 0 {
				return nil, false
			}
			return collected, true
		}

		index, err := strconv.Atoi(indexStr)
		if err != nil {
			return nil, false
		}

		// Support negative indexing
		if index < 0 {
			index = len(arr) + index
		}
		if index < 0 || index >= len(arr) {
			return nil, false
		}

		return walkPath(arr[index], rest)
	}

	// Regular field access
	obj, ok := data.(map[string]interface{})
	if !ok {
		return nil, false
	}
	val, exists := obj[segment]
	if !exists {
		return nil, false
	}
	return walkPath(val, rest)
}

func nextSegment(path string) (string, string) {
	inBracket := false
	for i, ch := range path {
		if ch == '[' {
			inBracket = true
		} else if ch == ']' {
			inBracket = false
		} else if ch == '.' && !inBracket {
			return path[:i], path[i+1:]
		}
	}
	return path, ""
}

func jsonValueToString(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case float64:
		if val == float64(int64(val)) {
			return strconv.FormatInt(int64(val), 10)
		}
		return strconv.FormatFloat(val, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(val)
	case nil:
		return "null"
	default:
		b, _ := json.Marshal(val)
		return string(b)
	}
}

// --- Helpers ---

func findArtifact(name string, artifacts []ArtifactInfo) *ArtifactInfo {
	for i := range artifacts {
		if artifacts[i].Name == name {
			return &artifacts[i]
		}
	}
	for i := range artifacts {
		if filepath.Base(artifacts[i].Name) == name {
			return &artifacts[i]
		}
	}
	return nil
}

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}
