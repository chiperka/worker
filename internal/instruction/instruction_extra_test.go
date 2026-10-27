package instruction

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"chiperka-worker/internal/dag"
)

// ---------------------------------------------------------------------------
// Context methods
// ---------------------------------------------------------------------------

func TestContext_SetManagerAndManager(t *testing.T) {
	ictx := &Context{State: dag.NewStateBag()}
	if ictx.Manager() != nil {
		t.Fatal("expected nil manager before SetManager")
	}
	// SetManager(nil) should be fine
	ictx.SetManager(nil)
	if ictx.Manager() != nil {
		t.Fatal("expected nil manager after SetManager(nil)")
	}
}

func TestContext_LogWithNilCollector_MultipleArgs(t *testing.T) {
	// Should not panic when Logs is nil, even with various args
	ictx := &Context{State: dag.NewStateBag()}
	ictx.Log("info", "test", "svc", "msg", "step")
	ictx.Log("warn", "", "", "", "")
	ictx.Log("fail", "action", "service", "message", "step")
}

func TestContext_FlushLogsWithNilCollector(t *testing.T) {
	ictx := &Context{State: dag.NewStateBag()}
	ictx.FlushLogs() // should not panic
}

func TestContext_LogWithCollector(t *testing.T) {
	sender := &noopLogSender{}
	lc := NewLogCollector(sender, "j1", time.Now())
	defer lc.Stop()

	ictx := &Context{State: dag.NewStateBag(), Logs: lc}
	ictx.Log("info", "test_action", "svc1", "hello", "step1")

	if lc.Count() != 1 {
		t.Errorf("expected 1 buffered log, got %d", lc.Count())
	}
}

func TestContext_FlushLogsWithCollector(t *testing.T) {
	sender := &noopLogSender{}
	lc := NewLogCollector(sender, "j1", time.Now())
	defer lc.Stop()

	ictx := &Context{State: dag.NewStateBag(), Logs: lc}
	ictx.Log("info", "act", "", "msg", "")
	ictx.FlushLogs()

	// After flush, buffer should be empty (sender succeeds)
	if lc.Count() != 0 {
		t.Errorf("expected 0 buffered logs after flush, got %d", lc.Count())
	}
}

func TestContext_DockerLogger(t *testing.T) {
	sender := &capturingLogSender{}
	lc := NewLogCollector(sender, "j1", time.Now())
	defer lc.Stop()

	ictx := &Context{State: dag.NewStateBag(), Logs: lc}
	logger := ictx.DockerLogger()

	logger.Info("info message")
	logger.Warn("warn message")
	logger.Fail("fail message")

	if lc.Count() != 3 {
		t.Errorf("expected 3 buffered logs, got %d", lc.Count())
	}

	// Flush and check levels
	lc.Flush()
	if len(sender.entries) != 3 {
		t.Fatalf("expected 3 sent entries, got %d", len(sender.entries))
	}
	if sender.entries[0].Level != "info" {
		t.Errorf("expected level info, got %s", sender.entries[0].Level)
	}
	if sender.entries[1].Level != "warn" {
		t.Errorf("expected level warn, got %s", sender.entries[1].Level)
	}
	if sender.entries[2].Level != "fail" {
		t.Errorf("expected level fail, got %s", sender.entries[2].Level)
	}
	// All should have step "start_services" and action "docker"
	for _, e := range sender.entries {
		if e.Action != "docker" {
			t.Errorf("expected action docker, got %s", e.Action)
		}
		if e.Step != "start_services" {
			t.Errorf("expected step start_services, got %s", e.Step)
		}
	}
}

func TestContext_DockerLoggerNilLogs(t *testing.T) {
	ictx := &Context{State: dag.NewStateBag()}
	logger := ictx.DockerLogger()
	// Should not panic
	logger.Info("info")
	logger.Warn("warn")
	logger.Fail("fail")
}

// ---------------------------------------------------------------------------
// HandlerFunc adapter
// ---------------------------------------------------------------------------

func TestHandlerFunc(t *testing.T) {
	called := false
	fn := HandlerFunc(func(ctx context.Context, ictx *Context, params json.RawMessage) (*dag.StepResult, error) {
		called = true
		return &dag.StepResult{Status: dag.StepCompleted}, nil
	})
	result, err := fn.Execute(context.Background(), &Context{State: dag.NewStateBag()}, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Error("handler func not called")
	}
	if result.Status != dag.StepCompleted {
		t.Errorf("expected completed, got %s", result.Status)
	}
}

// ---------------------------------------------------------------------------
// Registry: ListInstructions, Get for unknown, RegisterFunc
// ---------------------------------------------------------------------------

func TestRegistry_ListInstructions(t *testing.T) {
	r := DefaultRegistry()
	names := r.ListInstructions()
	if len(names) < 8 {
		t.Errorf("expected at least 8 instructions, got %d", len(names))
	}
	// Check that all expected names appear
	expected := map[string]bool{
		"start_services": true,
		"http_request":   true,
		"cli_exec":       true,
		"assert":         true,
		"collect_logs":   true,
		"cleanup":        true,
		"download_files": true,
		"upload_files":   true,
	}
	for _, name := range names {
		delete(expected, name)
	}
	if len(expected) > 0 {
		t.Errorf("missing instructions: %v", expected)
	}
}

func TestRegistry_GetUnknown(t *testing.T) {
	r := DefaultRegistry()
	if r.Get("totally_unknown_instruction") != nil {
		t.Error("expected nil for unknown instruction")
	}
}

func TestRegistry_RegisterFunc(t *testing.T) {
	r := NewRegistry()
	called := false
	r.RegisterFunc("my_func", func(ctx context.Context, ictx *Context, params json.RawMessage) (*dag.StepResult, error) {
		called = true
		return &dag.StepResult{Status: dag.StepCompleted}, nil
	})

	h := r.Get("my_func")
	if h == nil {
		t.Fatal("handler not found after RegisterFunc")
	}

	_, err := h.Execute(context.Background(), &Context{State: dag.NewStateBag()}, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Error("RegisterFunc handler not called")
	}
}

func TestRegistry_ValidateEmptyJob(t *testing.T) {
	r := DefaultRegistry()
	job := &dag.Job{Steps: map[string]*dag.Step{}}
	if err := r.Validate(job); err != nil {
		t.Errorf("expected no error for empty job, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Cleanup: temp dirs and state cleanup
// ---------------------------------------------------------------------------

func TestCleanup_TempDirs(t *testing.T) {
	// Create real temp dirs
	dir1, err := os.MkdirTemp("", "cleanup-test-1-*")
	if err != nil {
		t.Fatal(err)
	}
	dir2, err := os.MkdirTemp("", "cleanup-test-2-*")
	if err != nil {
		t.Fatal(err)
	}

	// Verify they exist
	if _, err := os.Stat(dir1); err != nil {
		t.Fatal("dir1 should exist before cleanup")
	}

	handler := &CleanupHandler{}
	ictx := &Context{State: dag.NewStateBag()}

	params, _ := json.Marshal(CleanupParams{TempDirs: []string{dir1, dir2}})
	result, err := handler.Execute(context.Background(), ictx, params)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != dag.StepCompleted {
		t.Errorf("expected completed, got %s", result.Status)
	}
	if result.Data["cleaned"] != true {
		t.Error("expected cleaned=true")
	}

	// Verify dirs were removed
	if _, err := os.Stat(dir1); !os.IsNotExist(err) {
		t.Error("dir1 should have been removed")
	}
	if _, err := os.Stat(dir2); !os.IsNotExist(err) {
		t.Error("dir2 should have been removed")
	}
}

func TestCleanup_StateTempDirs(t *testing.T) {
	// Create a real temp dir and reference it in state
	stateDir, err := os.MkdirTemp("", "cleanup-state-*")
	if err != nil {
		t.Fatal(err)
	}

	state := dag.NewStateBag()
	state.Set("download_step", &dag.StepResult{
		Status: dag.StepCompleted,
		Data: map[string]interface{}{
			"temp_dir": stateDir,
		},
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

	// Verify state-referenced temp dir was cleaned up
	if _, err := os.Stat(stateDir); !os.IsNotExist(err) {
		t.Error("state temp dir should have been removed")
	}
}

func TestCleanup_EmptyTempDirsIgnored(t *testing.T) {
	handler := &CleanupHandler{}
	ictx := &Context{State: dag.NewStateBag()}

	params, _ := json.Marshal(CleanupParams{TempDirs: []string{"", ""}})
	result, err := handler.Execute(context.Background(), ictx, params)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != dag.StepCompleted {
		t.Errorf("expected completed, got %s", result.Status)
	}
}

func TestCleanup_NilParams(t *testing.T) {
	handler := &CleanupHandler{}
	ictx := &Context{State: dag.NewStateBag()}

	result, err := handler.Execute(context.Background(), ictx, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != dag.StepCompleted {
		t.Errorf("expected completed, got %s", result.Status)
	}
}

func TestCleanup_StateWithNilData(t *testing.T) {
	state := dag.NewStateBag()
	state.Set("empty_step", &dag.StepResult{
		Status: dag.StepCompleted,
		Data:   nil,
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
}

// ---------------------------------------------------------------------------
// Assert: buildCLIResponse edge cases
// ---------------------------------------------------------------------------

func TestBuildCLIResponse_MissingStep(t *testing.T) {
	state := dag.NewStateBag()
	_, err := buildCLIResponse(state, "nonexistent")
	if err == nil {
		t.Fatal("expected error for missing step")
	}
}

func TestBuildCLIResponse_NilData(t *testing.T) {
	state := dag.NewStateBag()
	state.Set("exec", &dag.StepResult{Status: dag.StepCompleted, Data: nil})
	_, err := buildCLIResponse(state, "exec")
	if err == nil {
		t.Fatal("expected error for nil data")
	}
}

func TestBuildCLIResponse_EmptyData(t *testing.T) {
	state := dag.NewStateBag()
	state.Set("exec", &dag.StepResult{
		Status: dag.StepCompleted,
		Data:   map[string]interface{}{},
	})
	resp, err := buildCLIResponse(state, "exec")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Fields should be zero values
	if resp.ExitCode != 0 {
		t.Errorf("expected exit code 0, got %d", resp.ExitCode)
	}
	if len(resp.Stdout) != 0 {
		t.Errorf("expected empty stdout, got %q", string(resp.Stdout))
	}
	if len(resp.Stderr) != 0 {
		t.Errorf("expected empty stderr, got %q", string(resp.Stderr))
	}
}

func TestBuildCLIResponse_WithStderr(t *testing.T) {
	state := dag.NewStateBag()
	state.Set("exec", &dag.StepResult{
		Status: dag.StepCompleted,
		Data: map[string]interface{}{
			"exit_code": float64(1),
			"stdout":    "",
			"stderr":    "error: file not found",
		},
	})
	resp, err := buildCLIResponse(state, "exec")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.ExitCode != 1 {
		t.Errorf("expected exit code 1, got %d", resp.ExitCode)
	}
	if string(resp.Stderr) != "error: file not found" {
		t.Errorf("unexpected stderr: %q", string(resp.Stderr))
	}
}

// ---------------------------------------------------------------------------
// Assert: buildHTTPResponse edge cases
// ---------------------------------------------------------------------------

func TestBuildHTTPResponse_NilData(t *testing.T) {
	state := dag.NewStateBag()
	state.Set("http", &dag.StepResult{Status: dag.StepCompleted, Data: nil})
	_, err := buildHTTPResponse(state, "http")
	if err == nil {
		t.Fatal("expected error for nil data")
	}
}

func TestBuildHTTPResponse_EmptyData(t *testing.T) {
	state := dag.NewStateBag()
	state.Set("http", &dag.StepResult{
		Status: dag.StepCompleted,
		Data:   map[string]interface{}{},
	})
	resp, err := buildHTTPResponse(state, "http")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 0 {
		t.Errorf("expected status 0, got %d", resp.StatusCode)
	}
	if len(resp.Body) != 0 {
		t.Errorf("expected empty body, got %q", string(resp.Body))
	}
	if resp.Headers != nil {
		t.Errorf("expected nil headers, got %v", resp.Headers)
	}
}

func TestBuildHTTPResponse_NoHeaders(t *testing.T) {
	state := dag.NewStateBag()
	state.Set("http", &dag.StepResult{
		Status: dag.StepCompleted,
		Data: map[string]interface{}{
			"status_code": float64(200),
			"body":        "OK",
		},
	})
	resp, err := buildHTTPResponse(state, "http")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
	if string(resp.Body) != "OK" {
		t.Errorf("expected OK, got %q", string(resp.Body))
	}
}

// ---------------------------------------------------------------------------
// Assert: buildArtifactInfos edge cases
// ---------------------------------------------------------------------------

func TestBuildArtifactInfos_NilData(t *testing.T) {
	state := dag.NewStateBag()
	state.Set("logs", &dag.StepResult{Status: dag.StepCompleted, Data: nil})
	infos := buildArtifactInfos(state, "logs")
	if len(infos) != 0 {
		t.Errorf("expected empty, got %d", len(infos))
	}
}

func TestBuildArtifactInfos_WrongType(t *testing.T) {
	state := dag.NewStateBag()
	state.Set("logs", &dag.StepResult{
		Status: dag.StepCompleted,
		Data: map[string]interface{}{
			"artifacts": "not_an_array",
		},
	})
	infos := buildArtifactInfos(state, "logs")
	if len(infos) != 0 {
		t.Errorf("expected empty for wrong type, got %d", len(infos))
	}
}

func TestBuildArtifactInfos_WithPath(t *testing.T) {
	// Simulate collect_logs writing artifact to disk and storing path
	tmpDir, _ := os.MkdirTemp("", "test-artifact-*")
	defer os.RemoveAll(tmpDir)
	tmpPath := filepath.Join(tmpDir, "report.json")
	os.WriteFile(tmpPath, []byte(`{"status":"pass"}`), 0644)

	state := dag.NewStateBag()
	state.Set("logs", &dag.StepResult{
		Status: dag.StepCompleted,
		Data: map[string]interface{}{
			"artifacts": []interface{}{
				map[string]interface{}{
					"name":     "report.json",
					"size":     float64(42),
					"uploaded": true,
					"path":     tmpPath,
				},
			},
		},
	})
	infos := buildArtifactInfos(state, "logs")
	if len(infos) != 1 {
		t.Fatalf("expected 1 info, got %d", len(infos))
	}
	if infos[0].Name != "report.json" {
		t.Errorf("expected report.json, got %s", infos[0].Name)
	}
	if infos[0].Path != tmpPath {
		t.Errorf("expected path %s, got %s", tmpPath, infos[0].Path)
	}
	data, err := os.ReadFile(infos[0].Path)
	if err != nil {
		t.Fatalf("failed to read artifact file: %v", err)
	}
	if string(data) != `{"status":"pass"}` {
		t.Errorf("unexpected file content: %q", string(data))
	}
}

func TestBuildArtifactInfos_NonMapItems(t *testing.T) {
	state := dag.NewStateBag()
	state.Set("logs", &dag.StepResult{
		Status: dag.StepCompleted,
		Data: map[string]interface{}{
			"artifacts": []interface{}{
				"not a map",
				42,
			},
		},
	})
	infos := buildArtifactInfos(state, "logs")
	if len(infos) != 0 {
		t.Errorf("expected 0 infos for non-map items, got %d", len(infos))
	}
}

func TestBuildArtifactInfos_NoArtifactsKey(t *testing.T) {
	state := dag.NewStateBag()
	state.Set("logs", &dag.StepResult{
		Status: dag.StepCompleted,
		Data: map[string]interface{}{
			"other_key": "value",
		},
	})
	infos := buildArtifactInfos(state, "logs")
	if len(infos) != 0 {
		t.Errorf("expected empty for missing artifacts key, got %d", len(infos))
	}
}

// ---------------------------------------------------------------------------
// Assert: full handler with assertions
// ---------------------------------------------------------------------------

func TestAssert_InvalidParams(t *testing.T) {
	handler := &AssertHandler{}
	ictx := &Context{State: dag.NewStateBag()}
	_, err := handler.Execute(context.Background(), ictx, json.RawMessage(`not json`))
	if err == nil {
		t.Fatal("expected error for invalid params")
	}
}

func TestAssert_EmptyAssertions(t *testing.T) {
	handler := &AssertHandler{}
	ictx := &Context{State: dag.NewStateBag()}
	result, err := handler.Execute(context.Background(), ictx, json.RawMessage(`{"assertions":[]}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != dag.StepCompleted {
		t.Errorf("expected completed for empty assertions, got %s", result.Status)
	}
	if result.Data["all_passed"] != true {
		t.Errorf("expected all_passed=true, got %v", result.Data["all_passed"])
	}
}

func TestAssert_CLIResponseMissing(t *testing.T) {
	handler := &AssertHandler{}
	ictx := &Context{State: dag.NewStateBag()}
	params := `{"assertions":[],"cli_response_step":"missing"}`
	_, err := handler.Execute(context.Background(), ictx, json.RawMessage(params))
	if err == nil {
		t.Fatal("expected error for missing CLI response step")
	}
}

func TestAssert_HTTPResponseMissing(t *testing.T) {
	handler := &AssertHandler{}
	ictx := &Context{State: dag.NewStateBag()}
	params := `{"assertions":[],"http_response_step":"missing"}`
	_, err := handler.Execute(context.Background(), ictx, json.RawMessage(params))
	if err == nil {
		t.Fatal("expected error for missing HTTP response step")
	}
}

func TestAssert_WithExecutionDurationStep(t *testing.T) {
	state := dag.NewStateBag()
	state.Set("exec", &dag.StepResult{
		Status:   dag.StepCompleted,
		Duration: 500 * time.Millisecond,
		Data:     map[string]interface{}{},
	})

	handler := &AssertHandler{}
	ictx := &Context{State: state}
	params := `{"assertions":[],"execution_duration_step":"exec"}`
	result, err := handler.Execute(context.Background(), ictx, json.RawMessage(params))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != dag.StepCompleted {
		t.Errorf("expected completed, got %s", result.Status)
	}
}

func TestAssert_WithDownloadStep(t *testing.T) {
	state := dag.NewStateBag()
	state.Set("dl", &dag.StepResult{
		Status: dag.StepCompleted,
		Data: map[string]interface{}{
			"effective_suite_file_path": "/tmp/test/suite.chiperka",
		},
	})

	handler := &AssertHandler{}
	ictx := &Context{State: state}
	params := `{"assertions":[],"download_step":"dl","suite_file_path":"original.chiperka"}`
	result, err := handler.Execute(context.Background(), ictx, json.RawMessage(params))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != dag.StepCompleted {
		t.Errorf("expected completed, got %s", result.Status)
	}
}

func TestAssert_WithArtifactInfosStep(t *testing.T) {
	state := dag.NewStateBag()
	state.Set("collect", &dag.StepResult{
		Status: dag.StepCompleted,
		Data: map[string]interface{}{
			"artifacts": []interface{}{
				map[string]interface{}{"name": "test.log", "size": float64(100)},
			},
		},
	})

	handler := &AssertHandler{}
	ictx := &Context{State: state}
	params := `{"assertions":[],"artifact_infos_step":"collect"}`
	result, err := handler.Execute(context.Background(), ictx, json.RawMessage(params))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != dag.StepCompleted {
		t.Errorf("expected completed, got %s", result.Status)
	}
}

func TestAssert_CLIOutputArtifacts(t *testing.T) {
	suiteDir := t.TempDir()
	os.WriteFile(filepath.Join(suiteDir, "stdout.txt"), []byte("[{\"id\":1}]"), 0644)

	state := dag.NewStateBag()
	state.Set("exec", &dag.StepResult{
		Status: dag.StepCompleted,
		Data:   map[string]interface{}{"exit_code": float64(0), "stdout": "[{\"id\":1}]", "stderr": ""},
	})

	handler := &AssertHandler{}
	ictx := &Context{State: state}
	params, _ := json.Marshal(map[string]interface{}{
		"assertions": []interface{}{
			map[string]interface{}{"artifact": map[string]interface{}{"name": "stdout.txt", "snapshot": "./stdout.txt"}},
			map[string]interface{}{"artifact": map[string]interface{}{"name": "stderr.txt", "exists": true}},
		},
		"cli_response_step": "exec",
		"suite_file_path":   filepath.Join(suiteDir, "test.chiperka"),
	})
	result, err := handler.Execute(context.Background(), ictx, params)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != dag.StepCompleted {
		t.Errorf("expected completed, got %s: %v", result.Status, result.Data["results"])
	}
	tempDir, _ := result.Data["temp_dir"].(string)
	if tempDir == "" {
		t.Fatal("expected temp_dir for the cleanup step")
	}
	os.RemoveAll(tempDir)
}

func TestAssert_DiffOutputArtifact(t *testing.T) {
	suiteDir := t.TempDir()
	os.WriteFile(filepath.Join(suiteDir, "db_state.diff"), []byte("--- a\n+++ b\n"), 0644)
	outputPath := filepath.Join(t.TempDir(), "db_state.diff")
	os.WriteFile(outputPath, []byte("--- a\n+++ b\n"), 0644)

	state := dag.NewStateBag()
	state.Set("collect", &dag.StepResult{Status: dag.StepCompleted, Data: map[string]interface{}{"artifacts": []interface{}{}}})
	state.Set("hook_after_1", &dag.StepResult{
		Status: dag.StepCompleted,
		Data:   map[string]interface{}{"type": "diff", "output": "db_state.diff", "output_path": outputPath, "output_size": 12},
	})

	handler := &AssertHandler{}
	ictx := &Context{State: state}
	params, _ := json.Marshal(map[string]interface{}{
		"assertions": []interface{}{
			map[string]interface{}{"artifact": map[string]interface{}{"name": "db_state.diff", "snapshot": "./db_state.diff"}},
		},
		"artifact_infos_step": "collect",
		"suite_file_path":     filepath.Join(suiteDir, "test.chiperka"),
	})
	result, err := handler.Execute(context.Background(), ictx, params)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != dag.StepCompleted {
		t.Errorf("expected completed, got %s: %v", result.Status, result.Data["results"])
	}
}

// ---------------------------------------------------------------------------
// CollectLogs: no manager (short-circuit)
// ---------------------------------------------------------------------------

func TestCollectLogs_NoManager_ReturnsEmptyArrays(t *testing.T) {
	handler := &CollectLogsHandler{}
	ictx := &Context{State: dag.NewStateBag()}

	result, err := handler.Execute(context.Background(), ictx, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != dag.StepCompleted {
		t.Errorf("expected completed, got %s", result.Status)
	}
	logs, ok := result.Data["logs"].([]interface{})
	if !ok {
		t.Fatal("expected logs key")
	}
	if len(logs) != 0 {
		t.Errorf("expected empty logs, got %d", len(logs))
	}
}

// ---------------------------------------------------------------------------
// CLIExec: invalid params
// ---------------------------------------------------------------------------

func TestCLIExec_InvalidParams(t *testing.T) {
	handler := &CLIExecHandler{}
	ictx := &Context{State: dag.NewStateBag()}
	_, err := handler.Execute(context.Background(), ictx, json.RawMessage(`not json`))
	if err == nil {
		t.Fatal("expected error for invalid JSON params")
	}
}

// ---------------------------------------------------------------------------
// StartServices: additional param parsing tests
// ---------------------------------------------------------------------------

func TestStartServices_EmptyServices(t *testing.T) {
	handler := &StartServicesHandler{}
	ictx := &Context{State: dag.NewStateBag()}

	// services key present but empty array
	_, err := handler.Execute(context.Background(), ictx, json.RawMessage(`{"services":[],"test_name":"t1"}`))
	if err == nil {
		t.Fatal("expected error for empty services")
	}
}

// ---------------------------------------------------------------------------
// Test helpers (log senders)
// ---------------------------------------------------------------------------

type noopLogSender struct{}

func (s *noopLogSender) SendLogs(jobID string, logs []LogEntry) (time.Duration, error) {
	return 0, nil
}

type capturingLogSender struct {
	entries []LogEntry
}

func (s *capturingLogSender) SendLogs(jobID string, logs []LogEntry) (time.Duration, error) {
	s.entries = append(s.entries, logs...)
	return 0, nil
}

func TestComputeDiff_CLIFormat(t *testing.T) {
	before := "{\n  \"rowCount\": 141,\n  \"tables\": [\n    {\n      \"name\": \"products\",\n      \"rowCount\": 0\n    }\n  ]\n}\n"
	after := "{\n  \"rowCount\": 183,\n  \"tables\": [\n    {\n      \"name\": \"products\",\n      \"rowCount\": 15\n    }\n  ]\n}\n"

	expected := "--- db_state_before.json\n+++ db_state_after.json\n" +
		"-  \"rowCount\": 141,\n+  \"rowCount\": 183,\n" +
		"-      \"rowCount\": 0\n+      \"rowCount\": 15\n"
	if got := computeDiff("db_state_before.json", "db_state_after.json", before, after); got != expected {
		t.Errorf("unexpected diff:\n%s", got)
	}

	identical := "--- db_state_before.json\n+++ db_state_after.json\n"
	if got := computeDiff("db_state_before.json", "db_state_after.json", before, before); got != identical {
		t.Errorf("expected header only for identical files, got:\n%s", got)
	}
}
