package instruction

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"testing"

	"chiperka-worker/internal/dag"
)

// --- Mock API Client ---

type mockAPIClient struct {
	mu        sync.Mutex
	downloads map[string][]byte
	uploaded  []uploadCall
	failOn   string
}

type uploadCall struct {
	Token, JobID, Name, Content string
}

func (m *mockAPIClient) DownloadJobFile(token, jobID, path string) ([]byte, error) {
	key := jobID + "/" + path
	if m.failOn == key {
		return nil, fmt.Errorf("download failed: %s", key)
	}
	if data, ok := m.downloads[key]; ok {
		return data, nil
	}
	return nil, fmt.Errorf("file not found: %s", key)
}

func (m *mockAPIClient) UploadArtifact(token, jobID, name, content string) error {
	if m.failOn == name {
		return fmt.Errorf("upload failed: %s", name)
	}
	m.mu.Lock()
	m.uploaded = append(m.uploaded, uploadCall{token, jobID, name, content})
	m.mu.Unlock()
	return nil
}

func (m *mockAPIClient) UploadArtifactStream(token, jobID, name string, content io.Reader, size int64) error {
	data, _ := io.ReadAll(content)
	return m.UploadArtifact(token, jobID, name, string(data))
}

// --- buildHTTPResult / parseCurlHeaderDump tests ---
//
// Curl now writes the response body and headers to files inside the container
// instead of muxing everything onto stdout. These tests cover the pure parsing
// helpers that turn (stdout, body file, headers file) into the (code, headers,
// body) triple that the HTTP handler returns.

func TestBuildHTTPResult_SimpleResponse(t *testing.T) {
	headersFile := []byte("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n")
	bodyFile := []byte(`{"ok":true}`)
	stdout := []byte("200")

	code, headers, body, err := buildHTTPResult(stdout, bodyFile, headersFile)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if code != 200 {
		t.Errorf("expected 200, got %d", code)
	}
	if headers.Get("Content-Type") != "application/json" {
		t.Errorf("expected application/json, got %s", headers.Get("Content-Type"))
	}
	if body != `{"ok":true}` {
		t.Errorf("expected {\"ok\":true}, got %q", body)
	}
}

func TestParseCurlHeaderDump_100Continue(t *testing.T) {
	dump := []byte("HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 201 Created\r\nLocation: /items/1\r\n\r\n")
	headers := parseCurlHeaderDump(dump)
	if headers.Get("Location") != "/items/1" {
		t.Errorf("expected /items/1 (last block), got %s", headers.Get("Location"))
	}
}

func TestBuildHTTPResult_EmptyBody(t *testing.T) {
	headersFile := []byte("HTTP/1.1 204 No Content\r\n\r\n")
	code, _, body, err := buildHTTPResult([]byte("204"), nil, headersFile)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if code != 204 {
		t.Errorf("expected 204, got %d", code)
	}
	if body != "" {
		t.Errorf("expected empty body, got %q", body)
	}
}

func TestParseCurlHeaderDump_MultipleHeaders(t *testing.T) {
	dump := []byte("HTTP/1.1 200 OK\r\nX-Foo: bar\r\nX-Foo: baz\r\n\r\n")
	headers := parseCurlHeaderDump(dump)
	vals := headers.Values("X-Foo")
	if len(vals) != 2 || vals[0] != "bar" || vals[1] != "baz" {
		t.Errorf("expected [bar baz], got %v", vals)
	}
}

func TestBuildHTTPResult_InvalidStatusCode(t *testing.T) {
	_, _, _, err := buildHTTPResult([]byte("notanumber"), nil, nil)
	if err == nil {
		t.Fatal("expected error for invalid status code")
	}
}

func TestBuildHTTPResult_EmptyStdout(t *testing.T) {
	_, _, _, err := buildHTTPResult([]byte(""), nil, nil)
	if err == nil {
		t.Fatal("expected error for empty stdout (no status code)")
	}
}

// --- Assert handler: buildHTTPResponse tests ---

func TestBuildHTTPResponse(t *testing.T) {
	state := dag.NewStateBag()
	state.Set("execute", &dag.StepResult{
		Status: dag.StepCompleted,
		Data: map[string]interface{}{
			"status_code": float64(201),
			"headers": map[string]interface{}{
				"Content-Type": "application/json",
				"Set-Cookie":   []interface{}{"a=1", "b=2"},
			},
			"body": `{"id":42}`,
		},
	})

	resp, err := buildHTTPResponse(state, "execute")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.StatusCode != 201 {
		t.Errorf("expected 201, got %d", resp.StatusCode)
	}
	if string(resp.Body) != `{"id":42}` {
		t.Errorf("expected {\"id\":42}, got %s", string(resp.Body))
	}
	if resp.Headers.Get("Content-Type") != "application/json" {
		t.Errorf("expected application/json, got %s", resp.Headers.Get("Content-Type"))
	}
	cookies := resp.Headers.Values("Set-Cookie")
	if len(cookies) != 2 {
		t.Errorf("expected 2 cookies, got %d", len(cookies))
	}
}

func TestBuildHTTPResponse_MissingStep(t *testing.T) {
	state := dag.NewStateBag()
	_, err := buildHTTPResponse(state, "nonexistent")
	if err == nil {
		t.Fatal("expected error for missing step")
	}
}

func TestBuildCLIResponse(t *testing.T) {
	state := dag.NewStateBag()
	state.Set("exec", &dag.StepResult{
		Status: dag.StepCompleted,
		Data: map[string]interface{}{
			"exit_code": float64(0),
			"stdout":    "hello world",
			"stderr":    "",
		},
	})

	resp, err := buildCLIResponse(state, "exec")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.ExitCode != 0 {
		t.Errorf("expected exit code 0, got %d", resp.ExitCode)
	}
	if string(resp.Stdout) != "hello world" {
		t.Errorf("expected 'hello world', got %s", string(resp.Stdout))
	}
}

func TestBuildArtifactInfos(t *testing.T) {
	state := dag.NewStateBag()
	state.Set("logs", &dag.StepResult{
		Status: dag.StepCompleted,
		Data: map[string]interface{}{
			"artifacts": []interface{}{
				map[string]interface{}{"name": "error.log", "size": float64(1234)},
				map[string]interface{}{"name": "app.log", "size": float64(5678)},
			},
		},
	})

	infos := buildArtifactInfos(state, "logs")
	if len(infos) != 2 {
		t.Fatalf("expected 2 infos, got %d", len(infos))
	}
	if infos[0].Name != "error.log" || infos[0].Size != 1234 {
		t.Errorf("unexpected first artifact: %+v", infos[0])
	}
}

func TestBuildArtifactInfos_Empty(t *testing.T) {
	state := dag.NewStateBag()
	infos := buildArtifactInfos(state, "nonexistent")
	if len(infos) != 0 {
		t.Errorf("expected empty, got %d", len(infos))
	}
}

// --- Upload handler: extractArtifacts tests ---

func TestExtractArtifacts(t *testing.T) {
	state := dag.NewStateBag()
	state.Set("collect", &dag.StepResult{
		Status: dag.StepCompleted,
		Data: map[string]interface{}{
			"logs": []interface{}{
				map[string]interface{}{"name": "api.log", "content": "log data"},
			},
			"artifacts": []interface{}{
				map[string]interface{}{"name": "dump.sql", "content": "sql data"},
				map[string]interface{}{"name": "", "content": "no name"},
			},
		},
	})

	logs := extractPendingArtifacts(state, "collect", "logs")
	if len(logs) != 1 || logs[0].Name != "api.log" {
		t.Errorf("unexpected logs: %v", logs)
	}

	arts := extractPendingArtifacts(state, "collect", "artifacts")
	if len(arts) != 1 || arts[0].Name != "dump.sql" {
		t.Errorf("unexpected artifacts: %v (expected empty-name to be skipped)", arts)
	}
}

func TestExtractArtifacts_MissingStep(t *testing.T) {
	state := dag.NewStateBag()
	result := extractPendingArtifacts(state, "nonexistent", "logs")
	if len(result) != 0 {
		t.Errorf("expected empty, got %d", len(result))
	}
}

func TestExtractArtifacts_SkipsAlreadyUploaded(t *testing.T) {
	state := dag.NewStateBag()
	state.Set("collect", &dag.StepResult{
		Status: dag.StepCompleted,
		Data: map[string]interface{}{
			"logs": []interface{}{
				map[string]interface{}{"name": "uploaded.log", "size": float64(100), "uploaded": true},
				map[string]interface{}{"name": "pending.log", "content": "log data"},
			},
		},
	})

	logs := extractPendingArtifacts(state, "collect", "logs")
	if len(logs) != 1 || logs[0].Name != "pending.log" {
		t.Errorf("expected only pending.log, got %v", logs)
	}
}

// --- Download handler tests ---

func TestDownloadFiles_EmptyPaths(t *testing.T) {
	handler := &DownloadFilesHandler{}
	ictx := &Context{State: dag.NewStateBag()}

	result, err := handler.Execute(context.Background(), ictx, json.RawMessage(`{"paths":[]}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Data["downloaded"] != float64(0) {
		t.Errorf("expected 0 downloads, got %v", result.Data["downloaded"])
	}
}

func TestDownloadFiles_NoAPIClient(t *testing.T) {
	handler := &DownloadFilesHandler{}
	ictx := &Context{State: dag.NewStateBag()}

	_, err := handler.Execute(context.Background(), ictx, json.RawMessage(`{"paths":["a.json"],"base_url":"http://x","run_id":"r1"}`))
	if err == nil {
		t.Fatal("expected error when no API client")
	}
}

func TestDownloadFiles_Success(t *testing.T) {
	client := &mockAPIClient{
		downloads: map[string][]byte{
			"j1/tests/snap.json": []byte(`{"expected": "value"}`),
		},
	}
	handler := &DownloadFilesHandler{}
	ictx := &Context{State: dag.NewStateBag(), APIClient: client, JobID: "j1"}

	params := `{"paths":["tests/snap.json"],"suite_file_path":"tests/suite.chiperka"}`
	result, err := handler.Execute(context.Background(), ictx, json.RawMessage(params))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Data["downloaded"] != float64(1) {
		t.Errorf("expected 1 download, got %v", result.Data["downloaded"])
	}
	if result.Data["temp_dir"] == "" {
		t.Error("expected temp_dir in output")
	}
	if result.Data["effective_suite_file_path"] == "" {
		t.Error("expected effective_suite_file_path in output")
	}

	// Cleanup
	if dir, ok := result.Data["temp_dir"].(string); ok {
		os.RemoveAll(dir)
	}
}

func TestDownloadFiles_DownloadFails(t *testing.T) {
	client := &mockAPIClient{
		downloads: map[string][]byte{},
	}
	handler := &DownloadFilesHandler{}
	ictx := &Context{State: dag.NewStateBag(), APIClient: client}

	params := `{"paths":["missing.json"],"base_url":"http://api","run_id":"r1"}`
	_, err := handler.Execute(context.Background(), ictx, json.RawMessage(params))
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

// --- Upload handler tests ---

func TestUploadFiles_EmptyArtifacts(t *testing.T) {
	client := &mockAPIClient{}
	handler := &UploadFilesHandler{}
	ictx := &Context{State: dag.NewStateBag(), APIClient: client, JobID: "j1", JobToken: "tok"}

	result, err := handler.Execute(context.Background(), ictx, json.RawMessage(`{"collect_logs_step":"logs"}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Data["uploaded"] != float64(0) {
		t.Errorf("expected 0 uploads, got %v", result.Data["uploaded"])
	}
}

func TestUploadFiles_Success(t *testing.T) {
	state := dag.NewStateBag()
	state.Set("collect", &dag.StepResult{
		Status: dag.StepCompleted,
		Data: map[string]interface{}{
			"logs": []interface{}{
				map[string]interface{}{"name": "api.log", "content": "log data"},
			},
			"artifacts": []interface{}{
				map[string]interface{}{"name": "dump.sql", "content": "sql data"},
			},
		},
	})

	client := &mockAPIClient{}
	handler := &UploadFilesHandler{}
	ictx := &Context{State: state, APIClient: client, JobID: "j1", JobToken: "tok"}

	result, err := handler.Execute(context.Background(), ictx, json.RawMessage(`{"collect_logs_step":"collect"}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Data["uploaded"] != float64(2) {
		t.Errorf("expected 2 uploads, got %v", result.Data["uploaded"])
	}
	if len(client.uploaded) != 2 {
		t.Errorf("expected 2 upload calls, got %d", len(client.uploaded))
	}
}

func TestUploadFiles_UploadFails(t *testing.T) {
	state := dag.NewStateBag()
	state.Set("collect", &dag.StepResult{
		Status: dag.StepCompleted,
		Data: map[string]interface{}{
			"logs": []interface{}{
				map[string]interface{}{"name": "fail.log", "content": "data"},
			},
			"artifacts": []interface{}{},
		},
	})

	client := &mockAPIClient{failOn: "fail.log"}
	handler := &UploadFilesHandler{}
	ictx := &Context{State: state, APIClient: client, JobID: "j1", JobToken: "tok"}

	_, err := handler.Execute(context.Background(), ictx, json.RawMessage(`{"collect_logs_step":"collect"}`))
	if err == nil {
		t.Fatal("expected error for failed upload")
	}
}

func TestUploadFiles_NoAPIClient(t *testing.T) {
	handler := &UploadFilesHandler{}
	ictx := &Context{State: dag.NewStateBag()}

	_, err := handler.Execute(context.Background(), ictx, json.RawMessage(`{}`))
	if err == nil {
		t.Fatal("expected error when no API client")
	}
}

// --- Cleanup handler tests ---

func TestCleanup_NoManager(t *testing.T) {
	handler := &CleanupHandler{}
	ictx := &Context{State: dag.NewStateBag()}

	result, err := handler.Execute(context.Background(), ictx, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != dag.StepCompleted {
		t.Errorf("expected completed, got %s", result.Status)
	}
}

// --- CLI Exec handler tests ---

func TestCLIExec_NoManager(t *testing.T) {
	handler := &CLIExecHandler{}
	ictx := &Context{State: dag.NewStateBag()}

	_, err := handler.Execute(context.Background(), ictx, json.RawMessage(`{"service":"api","command":"echo hi"}`))
	if err == nil {
		t.Fatal("expected error when no manager")
	}
}

func TestCLIExec_MissingService(t *testing.T) {
	handler := &CLIExecHandler{}
	ictx := &Context{State: dag.NewStateBag()}

	_, err := handler.Execute(context.Background(), ictx, json.RawMessage(`{"service":"","command":"echo"}`))
	if err == nil {
		t.Fatal("expected error for empty service")
	}
}

func TestCLIExec_MissingCommand(t *testing.T) {
	handler := &CLIExecHandler{}
	ictx := &Context{State: dag.NewStateBag()}

	_, err := handler.Execute(context.Background(), ictx, json.RawMessage(`{"service":"api","command":""}`))
	if err == nil {
		t.Fatal("expected error for empty command")
	}
}

// --- HTTP Request handler tests ---

func TestHTTPRequest_NoManager(t *testing.T) {
	handler := &HTTPRequestHandler{}
	ictx := &Context{State: dag.NewStateBag()}

	_, err := handler.Execute(context.Background(), ictx, json.RawMessage(`{"target":"http://api","method":"GET","url":"/health"}`))
	if err == nil {
		t.Fatal("expected error when no manager")
	}
}

func TestHTTPRequest_InvalidParams(t *testing.T) {
	handler := &HTTPRequestHandler{}
	ictx := &Context{State: dag.NewStateBag()}

	_, err := handler.Execute(context.Background(), ictx, json.RawMessage(`not json`))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

// --- Start Services handler tests ---

func TestStartServices_NoServices(t *testing.T) {
	handler := &StartServicesHandler{}
	ictx := &Context{State: dag.NewStateBag()}

	_, err := handler.Execute(context.Background(), ictx, json.RawMessage(`{"services":[]}`))
	if err == nil {
		t.Fatal("expected error for empty services")
	}
}

func TestStartServices_InvalidParams(t *testing.T) {
	handler := &StartServicesHandler{}
	ictx := &Context{State: dag.NewStateBag()}

	_, err := handler.Execute(context.Background(), ictx, json.RawMessage(`not json`))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

// --- Registry tests ---

func TestRegistry_DefaultRegistry(t *testing.T) {
	r := DefaultRegistry()
	expected := []string{"start_services", "http_request", "cli_exec", "assert", "collect_logs", "cleanup", "download_files", "upload_files"}
	for _, name := range expected {
		if r.Get(name) == nil {
			t.Errorf("missing handler: %s", name)
		}
	}
}

func TestRegistry_Validate(t *testing.T) {
	r := DefaultRegistry()

	validJob := &dag.Job{
		Steps: map[string]*dag.Step{
			"a": {Instruction: "cleanup"},
			"b": {Instruction: "assert"},
		},
	}
	if err := r.Validate(validJob); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	invalidJob := &dag.Job{
		Steps: map[string]*dag.Step{
			"a": {Instruction: "nonexistent"},
		},
	}
	if err := r.Validate(invalidJob); err == nil {
		t.Error("expected error for unknown instruction")
	}
}

func TestRegistry_BuildDAGHandlers(t *testing.T) {
	r := NewRegistry()
	called := false
	r.Register("test", HandlerFunc(func(ctx context.Context, ictx *Context, params json.RawMessage) (*dag.StepResult, error) {
		called = true
		if ictx.JobID != "j1" {
			t.Errorf("expected job ID j1, got %s", ictx.JobID)
		}
		return &dag.StepResult{Status: dag.StepCompleted}, nil
	}))

	ictx := &Context{
		State: dag.NewStateBag(),
		JobID: "j1",
	}

	dagHandlers := r.BuildDAGHandlers(ictx)
	handler := dagHandlers["test"]
	if handler == nil {
		t.Fatal("handler not found")
	}

	result, err := handler(context.Background(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Error("handler was not called")
	}
	if result.Status != dag.StepCompleted {
		t.Errorf("expected completed, got %s", result.Status)
	}
}

// --- Context thread safety tests ---

func TestContext_ManagerThreadSafety(t *testing.T) {
	ictx := &Context{State: dag.NewStateBag()}

	if ictx.Manager() != nil {
		t.Error("expected nil manager initially")
	}

	// SetManager and Manager() should be safe for concurrent use
	// (though in practice SetManager is called once by start_services)
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			_ = ictx.Manager()
			done <- true
		}()
	}
	for i := 0; i < 10; i++ {
		<-done
	}
}

// --- Full integration test with mock handlers through DAG executor ---

func TestFullDAGExecution_WithMockHandlers(t *testing.T) {
	state := dag.NewStateBag()

	r := NewRegistry()

	// Mock start_services
	r.Register("start_services", HandlerFunc(func(ctx context.Context, ictx *Context, params json.RawMessage) (*dag.StepResult, error) {
		return &dag.StepResult{
			Status: dag.StepCompleted,
			Data: map[string]interface{}{
				"network_name": "test-net",
				"services": map[string]interface{}{
					"api": map[string]interface{}{"image": "myapp:latest"},
				},
			},
		}, nil
	}))

	// Mock http_request
	r.Register("http_request", HandlerFunc(func(ctx context.Context, ictx *Context, params json.RawMessage) (*dag.StepResult, error) {
		return &dag.StepResult{
			Status: dag.StepCompleted,
			Data: map[string]interface{}{
				"status_code": float64(200),
				"headers":     map[string]interface{}{"Content-Type": "application/json"},
				"body":        `{"token":"abc123"}`,
			},
		}, nil
	}))

	// Real assert handler (no Docker dependency)
	r.Register("assert", &AssertHandler{})

	// Mock collect_logs
	r.Register("collect_logs", HandlerFunc(func(ctx context.Context, ictx *Context, params json.RawMessage) (*dag.StepResult, error) {
		return &dag.StepResult{
			Status: dag.StepCompleted,
			Data: map[string]interface{}{
				"logs":      []interface{}{},
				"artifacts": []interface{}{},
			},
		}, nil
	}))

	// Mock cleanup
	r.Register("cleanup", HandlerFunc(func(ctx context.Context, ictx *Context, params json.RawMessage) (*dag.StepResult, error) {
		return &dag.StepResult{Status: dag.StepCompleted, Data: map[string]interface{}{"cleaned": true}}, nil
	}))

	ictx := &Context{State: state, JobID: "test-123"}
	dagHandlers := r.BuildDAGHandlers(ictx)

	job := &dag.Job{
		JobID:   "test-123",
		Timeout: "30s",
		Steps: map[string]*dag.Step{
			"start": {
				Instruction: "start_services",
				Params:      json.RawMessage(`{"services":[{"name":"api"}]}`),
			},
			"execute": {
				Instruction: "http_request",
				DependsOn:   []string{"start"},
				Params:      json.RawMessage(`{"target":"http://api:8080","method":"GET","url":"/health"}`),
			},
			"collect": {
				Instruction: "collect_logs",
				DependsOn:   []string{"execute"},
				Run:         dag.RunAlways,
				Params:      json.RawMessage(`{}`),
			},
			"assert": {
				Instruction: "assert",
				DependsOn:   []string{"collect"},
				Params:      json.RawMessage(`{"assertions":[{"response":{"status_code":200}}],"http_response_step":"execute"}`),
			},
			"cleanup": {
				Instruction: "cleanup",
				Params:      json.RawMessage(`{}`),
			},
		},
		Finally: []string{"cleanup"},
	}

	// Use RunWithState so instruction handlers and DAG executor share the same state bag
	exec := dag.NewExecutor(dagHandlers, nil)
	resultState, err := exec.RunWithState(context.Background(), job, state)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify all steps completed
	for _, name := range []string{"start", "execute", "collect", "assert", "cleanup"} {
		r := resultState.Get(name)
		if r == nil {
			t.Errorf("step %s has no result", name)
			continue
		}
		if r.Status != dag.StepCompleted {
			t.Errorf("step %s: expected completed, got %s (error: %s)", name, r.Status, r.Error)
		}
	}

	// Verify assert passed
	assertResult := resultState.Get("assert")
	if assertResult.Data["all_passed"] != true {
		t.Errorf("expected assertions to pass, got %v", assertResult.Data)
	}
}

func TestFullDAGExecution_FailurePathRunsCleanup(t *testing.T) {
	var cleanupRan bool

	r := NewRegistry()
	r.Register("start_services", HandlerFunc(func(ctx context.Context, ictx *Context, params json.RawMessage) (*dag.StepResult, error) {
		return nil, fmt.Errorf("Docker not available")
	}))
	r.Register("http_request", HandlerFunc(func(ctx context.Context, ictx *Context, params json.RawMessage) (*dag.StepResult, error) {
		t.Error("http_request should not run when start_services fails")
		return nil, nil
	}))
	r.Register("cleanup", HandlerFunc(func(ctx context.Context, ictx *Context, params json.RawMessage) (*dag.StepResult, error) {
		cleanupRan = true
		return &dag.StepResult{Status: dag.StepCompleted}, nil
	}))

	state := dag.NewStateBag()
	ictx := &Context{State: state, JobID: "test-fail"}
	dagHandlers := r.BuildDAGHandlers(ictx)

	job := &dag.Job{
		Steps: map[string]*dag.Step{
			"start":   {Instruction: "start_services", Params: json.RawMessage(`{"services":[{"name":"x"}]}`)},
			"execute": {Instruction: "http_request", DependsOn: []string{"start"}, Params: json.RawMessage(`{}`)},
			"cleanup": {Instruction: "cleanup", Params: json.RawMessage(`{}`)},
		},
		Finally: []string{"cleanup"},
	}

	exec := dag.NewExecutor(dagHandlers, nil)
	state, err := exec.RunWithState(context.Background(), job, state)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !cleanupRan {
		t.Error("cleanup should run even when start_services fails")
	}

	// start failed
	if !state.StepFailed("start") {
		t.Error("expected start to be failed")
	}

	// execute was skipped (dependency failed)
	r2 := state.Get("execute")
	if r2 == nil || r2.Status != dag.StepSkipped {
		t.Errorf("expected execute to be skipped, got %v", r2)
	}
}

func TestFullDAGExecution_AssertFailureStillUploads(t *testing.T) {
	var uploadRan bool

	r := NewRegistry()
	r.Register("http_request", HandlerFunc(func(ctx context.Context, ictx *Context, params json.RawMessage) (*dag.StepResult, error) {
		return &dag.StepResult{
			Status: dag.StepCompleted,
			Data: map[string]interface{}{
				"status_code": float64(500),
				"headers":     map[string]interface{}{},
				"body":        "error",
			},
		}, nil
	}))
	r.Register("assert", &AssertHandler{})
	r.Register("upload_files", HandlerFunc(func(ctx context.Context, ictx *Context, params json.RawMessage) (*dag.StepResult, error) {
		uploadRan = true
		return &dag.StepResult{Status: dag.StepCompleted}, nil
	}))
	r.Register("cleanup", HandlerFunc(func(ctx context.Context, ictx *Context, params json.RawMessage) (*dag.StepResult, error) {
		return &dag.StepResult{Status: dag.StepCompleted}, nil
	}))

	state := dag.NewStateBag()
	ictx := &Context{State: state, JobID: "test-assert-fail"}
	dagHandlers := r.BuildDAGHandlers(ictx)

	job := &dag.Job{
		Steps: map[string]*dag.Step{
			"execute": {
				Instruction: "http_request",
				Params:      json.RawMessage(`{}`),
			},
			"assert": {
				Instruction: "assert",
				DependsOn:   []string{"execute"},
				Params:      json.RawMessage(`{"assertions":[{"response":{"status_code":200}}],"http_response_step":"execute"}`),
			},
			"upload": {
				Instruction: "upload_files",
				DependsOn:   []string{"assert"},
				Run:         dag.RunAlways,
				Params:      json.RawMessage(`{}`),
			},
			"cleanup": {Instruction: "cleanup", Params: json.RawMessage(`{}`)},
		},
		Finally: []string{"cleanup"},
	}

	exec := dag.NewExecutor(dagHandlers, nil)
	state, _ = exec.RunWithState(context.Background(), job, state)

	// Assert should fail (expected 200, got 500)
	if !state.StepFailed("assert") {
		t.Error("expected assert to fail")
	}

	// Upload should still run (run: always)
	if !uploadRan {
		t.Error("upload should run even when assert fails (run: always)")
	}
}

// --- buildHTTPResult / parseCurlHeaderDump edge cases ---

func TestParseCurlHeaderDump_LFOnly(t *testing.T) {
	dump := []byte("HTTP/1.1 200 OK\nContent-Type: text/plain\n\n")
	headers := parseCurlHeaderDump(dump)
	if headers.Get("Content-Type") != "text/plain" {
		t.Errorf("expected text/plain, got %s", headers.Get("Content-Type"))
	}
}

// The whole reason for the file-based approach: the body file is read directly
// from the container tar stream, so arbitrary bytes (including invalid UTF-8)
// must round-trip through buildHTTPResult untouched at the byte level. Note
// that the worker's StepResult subsequently JSON-marshals the body string,
// which will mangle invalid UTF-8 — so this only verifies the curl-parsing
// stage is binary-clean.
func TestBuildHTTPResult_BinaryBytesPreservedAtParseStage(t *testing.T) {
	body := []byte{
		'%', 'P', 'D', 'F', '-', '1', '.', '7', '\n',
		0xFF, 0xFE, 0x00, 0x80, 0xC0, 0xC1,
		0x00,
		0x0A, 0x0D, 0x0A, 0x0D,
	}
	headersFile := []byte("HTTP/1.1 200 OK\r\nContent-Type: application/pdf\r\n\r\n")
	code, _, parsed, err := buildHTTPResult([]byte("200"), body, headersFile)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if code != 200 {
		t.Errorf("expected 200, got %d", code)
	}
	if []byte(parsed)[0] != '%' || []byte(parsed)[len([]byte(parsed))-1] != 0x0D {
		t.Errorf("expected first/last byte preserved, got first=%x last=%x", []byte(parsed)[0], []byte(parsed)[len([]byte(parsed))-1])
	}
	if len([]byte(parsed)) != len(body) {
		t.Errorf("expected len %d, got %d (bytes mangled in parse stage)", len(body), len([]byte(parsed)))
	}
}

// Verify header map preserves response type structure
func TestHTTPRequestHandler_ResponseStructure(t *testing.T) {
	// This is a unit test for the output structure, not for actual HTTP calls
	headerMap := make(map[string]interface{})
	headers := http.Header{}
	headers.Add("X-Single", "one")
	headers.Add("X-Multi", "a")
	headers.Add("X-Multi", "b")

	for k, v := range headers {
		if len(v) == 1 {
			headerMap[k] = v[0]
		} else {
			iface := make([]interface{}, len(v))
			for i, s := range v {
				iface[i] = s
			}
			headerMap[k] = iface
		}
	}

	if headerMap["X-Single"] != "one" {
		t.Errorf("expected single header as string, got %T", headerMap["X-Single"])
	}
	multi, ok := headerMap["X-Multi"].([]interface{})
	if !ok || len(multi) != 2 {
		t.Errorf("expected multi header as array, got %v", headerMap["X-Multi"])
	}
}

// --- Assert handler Execute tests ---

func TestAssertHandler_HTTPStatusPass(t *testing.T) {
	state := dag.NewStateBag()
	state.Set("execute", &dag.StepResult{
		Status: dag.StepCompleted,
		Data: map[string]interface{}{
			"status_code": float64(200),
			"headers":     map[string]interface{}{},
			"body":        "ok",
		},
	})

	handler := &AssertHandler{}
	ictx := &Context{State: state}
	params := `{"assertions":[{"response":{"status_code":200}}],"http_response_step":"execute"}`

	result, err := handler.Execute(context.Background(), ictx, json.RawMessage(params))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != dag.StepCompleted {
		t.Errorf("expected completed (pass), got %s", result.Status)
	}
	if result.Data["all_passed"] != true {
		t.Error("expected all_passed=true")
	}
}

func TestAssertHandler_HTTPStatusFail(t *testing.T) {
	state := dag.NewStateBag()
	state.Set("execute", &dag.StepResult{
		Status: dag.StepCompleted,
		Data: map[string]interface{}{
			"status_code": float64(500),
			"headers":     map[string]interface{}{},
			"body":        "error",
		},
	})

	handler := &AssertHandler{}
	ictx := &Context{State: state}
	params := `{"assertions":[{"response":{"status_code":200}}],"http_response_step":"execute"}`

	result, err := handler.Execute(context.Background(), ictx, json.RawMessage(params))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != dag.StepFailed {
		t.Errorf("expected failed, got %s", result.Status)
	}
	if result.Data["all_passed"] != false {
		t.Error("expected all_passed=false")
	}
}

func TestAssertHandler_MissingResponseStep(t *testing.T) {
	state := dag.NewStateBag()
	handler := &AssertHandler{}
	ictx := &Context{State: state}
	params := `{"assertions":[{"response":{"status_code":200}}],"http_response_step":"nonexistent"}`

	_, err := handler.Execute(context.Background(), ictx, json.RawMessage(params))
	if err == nil {
		t.Fatal("expected error for missing response step")
	}
}

func TestAssertHandler_CLIExitCode(t *testing.T) {
	state := dag.NewStateBag()
	state.Set("exec", &dag.StepResult{
		Status: dag.StepCompleted,
		Data: map[string]interface{}{
			"exit_code": float64(0),
			"stdout":    "hello",
			"stderr":    "",
		},
	})

	handler := &AssertHandler{}
	ictx := &Context{State: state}
	params := `{"assertions":[{"cli":{"exit_code":0}}],"cli_response_step":"exec"}`

	result, err := handler.Execute(context.Background(), ictx, json.RawMessage(params))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != dag.StepCompleted {
		t.Errorf("expected completed, got %s", result.Status)
	}
}

func TestAssertHandler_InvalidParams(t *testing.T) {
	handler := &AssertHandler{}
	ictx := &Context{State: dag.NewStateBag()}

	_, err := handler.Execute(context.Background(), ictx, json.RawMessage(`not json`))
	if err == nil {
		t.Fatal("expected error for invalid params")
	}
}

// --- Cleanup handler with temp dir cleanup ---

func TestCleanup_CleansUpTempDirs(t *testing.T) {
	// Create a temp dir to simulate download_files output
	tmpDir, err := os.MkdirTemp("", "chiperka-test-cleanup-*")
	if err != nil {
		t.Fatal(err)
	}
	os.WriteFile(tmpDir+"/test.txt", []byte("data"), 0644)

	state := dag.NewStateBag()
	state.Set("download_files", &dag.StepResult{
		Status: dag.StepCompleted,
		Data:   map[string]interface{}{"temp_dir": tmpDir},
	})

	handler := &CleanupHandler{}
	ictx := &Context{State: state}

	result, err := handler.Execute(context.Background(), ictx, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != dag.StepCompleted {
		t.Errorf("expected completed, got %s", result.Status)
	}

	// Verify temp dir was cleaned up
	if _, statErr := os.Stat(tmpDir); !os.IsNotExist(statErr) {
		t.Errorf("expected temp dir to be removed: %s", tmpDir)
		os.RemoveAll(tmpDir) // cleanup anyway
	}
}

// --- Download files with BaseURL validation ---

func TestDownloadFiles_MultipleFiles(t *testing.T) {
	client := &mockAPIClient{
		downloads: map[string][]byte{
			"j1/a.json": []byte("data1"),
			"j1/b.json": []byte("data2"),
		},
	}
	handler := &DownloadFilesHandler{}
	ictx := &Context{State: dag.NewStateBag(), APIClient: client, JobID: "j1"}

	params := `{"paths":["a.json","b.json"]}`
	result, err := handler.Execute(context.Background(), ictx, json.RawMessage(params))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Data["downloaded"] != float64(2) {
		t.Errorf("expected 2 downloads, got %v", result.Data["downloaded"])
	}
	// Cleanup
	if dir, ok := result.Data["temp_dir"].(string); ok {
		os.RemoveAll(dir)
	}
}

// --- Collect logs with nil manager ---

func TestCollectLogs_NoManager(t *testing.T) {
	handler := &CollectLogsHandler{}
	ictx := &Context{State: dag.NewStateBag()}

	result, err := handler.Execute(context.Background(), ictx, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("should not error without manager, got: %v", err)
	}
	if result.Status != dag.StepCompleted {
		t.Errorf("expected completed, got %s", result.Status)
	}
	// Should return empty logs
	logs := result.Data["logs"].([]interface{})
	if len(logs) != 0 {
		t.Errorf("expected empty logs, got %d", len(logs))
	}
}

// --- Context Log with nil collector ---

func TestContext_LogWithNilCollector(t *testing.T) {
	ictx := &Context{State: dag.NewStateBag()}
	// Should not panic
	ictx.Log("info", "test", "", "msg", "step")
	ictx.FlushLogs()
}

// --- Start services invalid JSON service definition ---

func TestStartServices_InvalidServiceJSON(t *testing.T) {
	handler := &StartServicesHandler{}
	ictx := &Context{State: dag.NewStateBag()}

	// Services field is not an array
	_, err := handler.Execute(context.Background(), ictx, json.RawMessage(`{"services":"not-array"}`))
	if err == nil {
		t.Fatal("expected error for invalid services")
	}
}

// --- Path traversal validation tests ---

func TestValidateFilePath(t *testing.T) {
	tests := []struct {
		path    string
		wantErr bool
	}{
		{"tests/snap.json", false},
		{"a.json", false},
		{"nested/deep/file.txt", false},
		{"../etc/passwd", true},        // directory traversal
		{"tests/../../etc/passwd", true}, // directory traversal
		{"/etc/passwd", true},           // absolute path
		{"", true},                      // empty
		{".", true},                      // current dir
	}

	for _, tt := range tests {
		err := validateFilePath(tt.path)
		if (err != nil) != tt.wantErr {
			t.Errorf("validateFilePath(%q) error = %v, wantErr %v", tt.path, err, tt.wantErr)
		}
	}
}

func TestDownloadFiles_PathTraversal(t *testing.T) {
	client := &mockAPIClient{
		downloads: map[string][]byte{
			"j1/../../../etc/passwd": []byte("root:x:0:0"),
		},
	}
	handler := &DownloadFilesHandler{}
	ictx := &Context{State: dag.NewStateBag(), APIClient: client, JobID: "j1"}

	params := `{"paths":["../../../etc/passwd"]}`
	_, err := handler.Execute(context.Background(), ictx, json.RawMessage(params))
	if err == nil {
		t.Fatal("expected error for path traversal attempt")
	}
}
