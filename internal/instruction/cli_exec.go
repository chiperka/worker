package instruction

import (
	"context"
	"encoding/json"
	"fmt"

	"chiperka-worker/internal/dag"
)

// CLIExecParams are the parameters for the cli_exec instruction.
type CLIExecParams struct {
	Service     string `json:"service"`
	Command     string `json:"command"`
	WorkingDir  string `json:"working_dir,omitempty"`
	FailOnError bool   `json:"fail_on_error,omitempty"`
}

// CLIExecHandler executes a command inside a running container.
type CLIExecHandler struct{}

func (h *CLIExecHandler) Execute(ctx context.Context, ictx *Context, params json.RawMessage) (*dag.StepResult, error) {
	var p CLIExecParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, fmt.Errorf("invalid cli_exec params: %w", err)
	}

	manager := ictx.Manager()
	if manager == nil {
		return nil, fmt.Errorf("cli_exec: no Docker manager (start_services must run first)")
	}

	if p.Service == "" {
		return nil, fmt.Errorf("cli_exec: service is required")
	}
	if p.Command == "" {
		return nil, fmt.Errorf("cli_exec: command is required")
	}

	ictx.Log("info", "cli_exec", p.Service, fmt.Sprintf("Running: %s", p.Command), "")

	result, err := manager.ExecInContainer(ctx, p.Service, p.Command, p.WorkingDir)
	if err != nil {
		ictx.Log("fail", "cli_exec", p.Service, fmt.Sprintf("Failed: %v", err), "")
		return nil, fmt.Errorf("cli_exec failed: %w", err)
	}

	ictx.Log("info", "cli_exec", p.Service, fmt.Sprintf("Exit code: %d", result.ExitCode), "")

	status := dag.StepCompleted
	if p.FailOnError && result.ExitCode != 0 {
		status = dag.StepFailed
		ictx.Log("fail", "cli_exec", p.Service, fmt.Sprintf("Command failed with exit code %d", result.ExitCode), "")
	}

	return &dag.StepResult{
		Status: status,
		Data: map[string]interface{}{
			"exit_code": float64(result.ExitCode),
			"stdout":    string(result.Stdout),
			"stderr":    string(result.Stderr),
		},
	}, nil
}
