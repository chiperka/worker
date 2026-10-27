package workerloop

import (
	"testing"
	"time"

	"chiperka-worker/internal/dag"
)

func TestBuildJobResult_AllPassed(t *testing.T) {
	state := dag.NewStateBag()
	state.Set("start_services", &dag.StepResult{Status: dag.StepCompleted, Duration: time.Second})
	state.Set("execute", &dag.StepResult{Status: dag.StepCompleted, Duration: 2 * time.Second})
	state.Set("assert", &dag.StepResult{
		Status:   dag.StepCompleted,
		Duration: 100 * time.Millisecond,
		Data: map[string]interface{}{
			"results": []interface{}{
				map[string]interface{}{"passed": true, "message": "ok"},
			},
		},
	})

	result := BuildJobResult(state, "w1", time.Now().Add(-5*time.Second))
	if result["status"] != "passed" {
		t.Errorf("expected passed, got %v", result["status"])
	}
	if result["error_message"] != "" {
		t.Errorf("expected no error message, got %v", result["error_message"])
	}
}

func TestBuildJobResult_AssertionFailed(t *testing.T) {
	state := dag.NewStateBag()
	state.Set("execute", &dag.StepResult{Status: dag.StepCompleted})
	state.Set("assert", &dag.StepResult{
		Status: dag.StepFailed,
		Error:  "assertion failed",
		Data: map[string]interface{}{
			"results": []interface{}{
				map[string]interface{}{"passed": false, "message": "expected 200, got 500"},
			},
		},
	})

	result := BuildJobResult(state, "w1", time.Now())
	if result["status"] != "failed" {
		t.Errorf("expected failed, got %v", result["status"])
	}
	if result["error_message"] != "expected 200, got 500" {
		t.Errorf("unexpected error message: %v", result["error_message"])
	}
}

func TestBuildJobResult_InfrastructureError(t *testing.T) {
	state := dag.NewStateBag()
	state.Set("start_services", &dag.StepResult{
		Status: dag.StepFailed,
		Error:  "docker: image pull failed",
	})
	state.Set("execute", &dag.StepResult{Status: dag.StepSkipped})

	result := BuildJobResult(state, "w1", time.Now())
	if result["status"] != "error" {
		t.Errorf("expected error, got %v", result["status"])
	}
	if result["error_message"] == "" {
		t.Error("expected error message for infrastructure failure")
	}
}

func TestBuildJobResult_StepResults(t *testing.T) {
	state := dag.NewStateBag()
	state.Set("execute", &dag.StepResult{
		Status:   dag.StepCompleted,
		Duration: 500 * time.Millisecond,
		Data:     map[string]interface{}{"status_code": float64(200)},
	})

	result := BuildJobResult(state, "w1", time.Now())
	stepResults, ok := result["step_results"].(map[string]interface{})
	if !ok {
		t.Fatal("step_results not found or wrong type")
	}

	execStep, ok := stepResults["execute"].(map[string]interface{})
	if !ok {
		t.Fatal("execute step not found")
	}
	if execStep["status"] != "completed" {
		t.Errorf("expected completed, got %v", execStep["status"])
	}
	if execStep["data"] == nil {
		t.Error("expected data in step result")
	}
}

func TestBuildJobResult_UploadAndCleanupFailsDontAffectStatus(t *testing.T) {
	state := dag.NewStateBag()
	state.Set("execute", &dag.StepResult{Status: dag.StepCompleted})
	state.Set("assert", &dag.StepResult{Status: dag.StepCompleted})
	state.Set("upload_artifacts", &dag.StepResult{Status: dag.StepFailed, Error: "upload failed"})
	state.Set("cleanup", &dag.StepResult{Status: dag.StepFailed, Error: "cleanup failed"})

	result := BuildJobResult(state, "w1", time.Now())
	// upload_artifacts and cleanup failures should NOT change status to "error"
	if result["status"] != "passed" {
		t.Errorf("expected passed (upload/cleanup excluded), got %v", result["status"])
	}
}
