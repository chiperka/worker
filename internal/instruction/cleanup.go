package instruction

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"chiperka-worker/internal/dag"
)

// CleanupParams are optional parameters for the cleanup instruction.
type CleanupParams struct {
	TempDirs []string `json:"temp_dirs,omitempty"`
}

// CleanupHandler stops all containers, removes the Docker network,
// and cleans up temporary directories.
type CleanupHandler struct{}

func (h *CleanupHandler) Execute(ctx context.Context, ictx *Context, params json.RawMessage) (*dag.StepResult, error) {
	var p CleanupParams
	if len(params) > 0 {
		json.Unmarshal(params, &p) // ignore errors, params are optional
	}

	// Clean up Docker resources with a timeout to prevent hanging on dead daemon
	manager := ictx.Manager()
	if manager != nil {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		manager.Cleanup(cleanupCtx)
		cleanupCancel()
	}

	// Clean up temp directories (e.g. snapshot downloads)
	for _, dir := range p.TempDirs {
		if dir != "" {
			os.RemoveAll(dir)
		}
	}

	// Also clean up temp dirs referenced in state from download_files steps
	results := ictx.State.AllResults()
	for _, r := range results {
		if r.Data == nil {
			continue
		}
		if tempDir, ok := r.Data["temp_dir"].(string); ok && tempDir != "" {
			os.RemoveAll(tempDir)
		}
	}

	return &dag.StepResult{
		Status: dag.StepCompleted,
		Data:   map[string]interface{}{"cleaned": true},
	}, nil
}
