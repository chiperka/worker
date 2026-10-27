package instruction

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"chiperka-worker/internal/assertion"
	"chiperka-worker/internal/dag"
	"chiperka-worker/internal/executor"
	"chiperka-worker/internal/model"
)

// AssertParams are the parameters for the assert instruction.
type AssertParams struct {
	Assertions            []model.Assertion `json:"assertions"`
	HTTPResponseStep      string            `json:"http_response_step,omitempty"`
	CLIResponseStep       string            `json:"cli_response_step,omitempty"`
	ExecutionDurationStep string            `json:"execution_duration_step,omitempty"`
	SuiteFilePath         string            `json:"suite_file_path,omitempty"`
	DownloadStep          string            `json:"download_step,omitempty"`
	ArtifactInfosStep     string            `json:"artifact_infos_step,omitempty"`
}

// AssertHandler evaluates assertions against step outputs.
type AssertHandler struct{}

func (h *AssertHandler) Execute(ctx context.Context, ictx *Context, params json.RawMessage) (*dag.StepResult, error) {
	var p AssertParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, fmt.Errorf("invalid assert params: %w", err)
	}

	// If snapshots were downloaded, use effective_suite_file_path from download step
	// which points into the temp dir where files were extracted
	suiteFilePath := p.SuiteFilePath
	if p.DownloadStep != "" {
		if dlResult := ictx.State.Get(p.DownloadStep); dlResult != nil && dlResult.Data != nil {
			if efp, ok := dlResult.Data["effective_suite_file_path"].(string); ok && efp != "" {
				suiteFilePath = efp
			}
		}
	}

	evalCtx := assertion.EvalContext{
		SuiteFilePath: suiteFilePath,
	}

	if p.HTTPResponseStep != "" {
		httpResp, err := buildHTTPResponse(ictx.State, p.HTTPResponseStep)
		if err != nil {
			return nil, fmt.Errorf("assert: failed to build HTTP response from step %q: %w", p.HTTPResponseStep, err)
		}
		evalCtx.HTTPResponse = httpResp
	}

	if p.CLIResponseStep != "" {
		cliResp, err := buildCLIResponse(ictx.State, p.CLIResponseStep)
		if err != nil {
			return nil, fmt.Errorf("assert: failed to build CLI response from step %q: %w", p.CLIResponseStep, err)
		}
		evalCtx.CLIResponse = cliResp
	}

	if p.ExecutionDurationStep != "" {
		r := ictx.State.Get(p.ExecutionDurationStep)
		if r != nil {
			evalCtx.ExecutionDuration = r.Duration
		}
	}

	if p.ArtifactInfosStep != "" {
		evalCtx.ArtifactInfos = buildArtifactInfos(ictx.State, p.ArtifactInfosStep)
	}
	evalCtx.ArtifactInfos = append(evalCtx.ArtifactInfos, buildDiffArtifactInfos(ictx.State)...)

	// The CLI output is available as the stdout.txt and stderr.txt artifacts.
	var tempDir string
	if evalCtx.CLIResponse != nil {
		var err error
		tempDir, err = os.MkdirTemp("", "chiperka-cli-output-*")
		if err != nil {
			return nil, fmt.Errorf("assert: failed to create CLI output dir: %w", err)
		}
		cliArtifacts, err := writeCLIOutputArtifacts(ictx, tempDir, evalCtx.CLIResponse)
		if err != nil {
			return nil, fmt.Errorf("assert: %w", err)
		}
		evalCtx.ArtifactInfos = append(evalCtx.ArtifactInfos, cliArtifacts...)
	}

	evaluator := assertion.NewEvaluator()
	results, allPassed := evaluator.EvaluateAll(p.Assertions, evalCtx)

	serializedResults := make([]interface{}, len(results))
	for i, r := range results {
		serializedResults[i] = map[string]interface{}{
			"passed":      r.Passed,
			"type":        r.Type,
			"expected":    r.Expected,
			"actual":      r.Actual,
			"message":     r.Message,
			"duration_ms": r.Duration.Milliseconds(),
		}
	}

	status := dag.StepCompleted
	if !allPassed {
		status = dag.StepFailed
	}

	data := map[string]interface{}{
		"all_passed": allPassed,
		"results":    serializedResults,
	}
	if tempDir != "" {
		data["temp_dir"] = tempDir // removed by the cleanup step
	}

	return &dag.StepResult{
		Status: status,
		Data:   data,
	}, nil
}

func buildHTTPResponse(state *dag.StateBag, stepName string) (*executor.HTTPResponse, error) {
	r := state.Get(stepName)
	if r == nil || r.Data == nil {
		return nil, fmt.Errorf("step %q has no output", stepName)
	}

	resp := &executor.HTTPResponse{}

	if sc, ok := r.Data["status_code"]; ok {
		if f, ok := sc.(float64); ok {
			resp.StatusCode = int(f)
		}
	}

	if body, ok := r.Data["body"]; ok {
		if s, ok := body.(string); ok {
			resp.Body = []byte(s)
		}
	}

	if hdrs, ok := r.Data["headers"]; ok {
		resp.Headers = make(http.Header)
		if hdrMap, ok := hdrs.(map[string]interface{}); ok {
			for k, v := range hdrMap {
				switch val := v.(type) {
				case string:
					resp.Headers.Set(k, val)
				case []interface{}:
					for _, item := range val {
						if s, ok := item.(string); ok {
							resp.Headers.Add(k, s)
						}
					}
				}
			}
		}
	}

	return resp, nil
}

func buildCLIResponse(state *dag.StateBag, stepName string) (*executor.CLIResponse, error) {
	r := state.Get(stepName)
	if r == nil || r.Data == nil {
		return nil, fmt.Errorf("step %q has no output", stepName)
	}

	resp := &executor.CLIResponse{}

	if ec, ok := r.Data["exit_code"]; ok {
		if f, ok := ec.(float64); ok {
			resp.ExitCode = int(f)
		}
	}
	if stdout, ok := r.Data["stdout"]; ok {
		if s, ok := stdout.(string); ok {
			resp.Stdout = []byte(s)
		}
	}
	if stderr, ok := r.Data["stderr"]; ok {
		if s, ok := stderr.(string); ok {
			resp.Stderr = []byte(s)
		}
	}

	return resp, nil
}

func buildArtifactInfos(state *dag.StateBag, stepName string) []assertion.ArtifactInfo {
	r := state.Get(stepName)
	if r == nil || r.Data == nil {
		return nil
	}

	artifacts, ok := r.Data["artifacts"]
	if !ok {
		return nil
	}
	arr, ok := artifacts.([]interface{})
	if !ok {
		return nil
	}

	var infos []assertion.ArtifactInfo
	for _, item := range arr {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		info := assertion.ArtifactInfo{}
		if name, ok := m["name"].(string); ok {
			info.Name = name
		}
		if size, ok := m["size"].(float64); ok {
			info.Size = int64(size)
		}
		if p, ok := m["path"].(string); ok {
			info.Path = p
		}
		infos = append(infos, info)
	}
	return infos
}

// buildDiffArtifactInfos returns the outputs of diff steps (e.g. db_state.diff),
// which are produced after the artifacts were extracted from the services.
func buildDiffArtifactInfos(state *dag.StateBag) []assertion.ArtifactInfo {
	var infos []assertion.ArtifactInfo
	for _, r := range state.AllResults() {
		if r == nil || r.Data == nil || r.Data["type"] != "diff" {
			continue
		}
		name, _ := r.Data["output"].(string)
		path, _ := r.Data["output_path"].(string)
		if name == "" || path == "" {
			continue
		}
		var size int64
		switch v := r.Data["output_size"].(type) {
		case int:
			size = int64(v)
		case float64:
			size = int64(v)
		}
		infos = append(infos, assertion.ArtifactInfo{Name: name, Path: path, Size: size})
	}
	return infos
}

// writeCLIOutputArtifacts writes the CLI stdout and stderr to dir and uploads them as artifacts.
func writeCLIOutputArtifacts(ictx *Context, dir string, resp *executor.CLIResponse) ([]assertion.ArtifactInfo, error) {
	outputs := []struct {
		name    string
		content []byte
	}{
		{"stdout.txt", resp.Stdout},
		{"stderr.txt", resp.Stderr},
	}

	var infos []assertion.ArtifactInfo
	for _, o := range outputs {
		path := filepath.Join(dir, o.name)
		if err := os.WriteFile(path, o.content, 0644); err != nil {
			return nil, fmt.Errorf("failed to write %s: %w", o.name, err)
		}
		if ictx.APIClient != nil && len(o.content) > 0 {
			if err := ictx.APIClient.UploadArtifact(ictx.JobToken, ictx.JobID, o.name, string(o.content)); err != nil {
				ictx.Log("warn", "assert", "", fmt.Sprintf("Failed to upload artifact %s: %v", o.name, err), "assert")
			}
		}
		infos = append(infos, assertion.ArtifactInfo{Name: o.name, Path: path, Size: int64(len(o.content))})
	}
	return infos, nil
}
