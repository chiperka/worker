package instruction

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"chiperka-worker/internal/dag"
)

// ExtractArtifactsHandler extracts user-defined files from service containers
// and streams them to disk. Paths are stored in step result so downstream
// steps (diff hooks, assertions) can read them.
type ExtractArtifactsHandler struct{}

func (h *ExtractArtifactsHandler) Execute(ctx context.Context, ictx *Context, params json.RawMessage) (*dag.StepResult, error) {
	manager := ictx.Manager()
	if manager == nil {
		return &dag.StepResult{
			Status: dag.StepCompleted,
			Data:   map[string]interface{}{"artifacts": []interface{}{}},
		}, nil
	}

	bgCtx := context.Background()
	var uploadedMu sync.Mutex
	var uploadedArtifacts []interface{}
	var uploadErrors []string

	artifactDir, _ := os.MkdirTemp("", "chiperka-artifacts-*")
	serviceArtifacts := manager.GetServiceArtifacts(bgCtx, artifactDir)
	for _, a := range serviceArtifacts {
		if ictx.APIClient != nil && a.Size > 0 {
			f, err := os.Open(a.Path)
			if err == nil {
				if err := ictx.APIClient.UploadArtifactStream(ictx.JobToken, ictx.JobID, a.Name, f, a.Size); err != nil {
					uploadErrors = append(uploadErrors, fmt.Sprintf("upload %s: %v", a.Name, err))
					ictx.Log("warn", "extract_artifacts", "", fmt.Sprintf("Failed to upload artifact %s: %v", a.Name, err), "extract_artifacts")
				}
				f.Close()
			}
		}

		uploadedMu.Lock()
		uploadedArtifacts = append(uploadedArtifacts, map[string]interface{}{
			"name":     a.Name,
			"size":     float64(a.Size),
			"uploaded": true,
			"path":     a.Path,
		})
		uploadedMu.Unlock()
	}

	data := map[string]interface{}{"artifacts": uploadedArtifacts}
	if len(uploadErrors) > 0 {
		data["upload_errors"] = uploadErrors
	}

	return &dag.StepResult{
		Status: dag.StepCompleted,
		Data:   data,
	}, nil
}
