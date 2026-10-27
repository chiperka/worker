package instruction

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"chiperka-worker/internal/dag"
)

// CollectLogsHandler collects service logs (stdout/stderr) from running containers
// and uploads them to the cloud. Run: "always" — logs are collected even if the test fails.
type CollectLogsHandler struct{}

func (h *CollectLogsHandler) Execute(ctx context.Context, ictx *Context, params json.RawMessage) (*dag.StepResult, error) {
	manager := ictx.Manager()
	if manager == nil {
		return &dag.StepResult{
			Status: dag.StepCompleted,
			Data:   map[string]interface{}{"logs": []interface{}{}},
		}, nil
	}

	bgCtx := context.Background()
	var uploadedMu sync.Mutex
	var uploadedLogs []interface{}
	var uploadErrors []string

	serviceLogs := manager.GetServiceLogs(bgCtx)
	for _, slog := range serviceLogs {
		name := fmt.Sprintf("%s.log", slog.ServiceName)
		size := int64(len(slog.Content))

		if ictx.APIClient != nil && size > 0 {
			if err := ictx.APIClient.UploadArtifactStream(ictx.JobToken, ictx.JobID, name, bytes.NewReader(slog.Content), size); err != nil {
				uploadErrors = append(uploadErrors, fmt.Sprintf("upload %s: %v", name, err))
				ictx.Log("warn", "collect_logs", slog.ServiceName, fmt.Sprintf("Failed to upload log: %v", err), "collect_logs")
			}
		}

		uploadedMu.Lock()
		uploadedLogs = append(uploadedLogs, map[string]interface{}{
			"service":  slog.ServiceName,
			"name":     name,
			"size":     float64(size),
			"uploaded": true,
		})
		uploadedMu.Unlock()
	}

	data := map[string]interface{}{"logs": uploadedLogs}
	if len(uploadErrors) > 0 {
		data["upload_errors"] = uploadErrors
	}

	return &dag.StepResult{
		Status: dag.StepCompleted,
		Data:   data,
	}, nil
}
