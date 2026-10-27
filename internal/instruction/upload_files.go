package instruction

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"chiperka-worker/internal/dag"
)

// UploadFilesParams are the parameters for the upload_files instruction.
type UploadFilesParams struct {
	CollectLogsStep string `json:"collect_logs_step,omitempty"`
}

// UploadFilesHandler uploads collected artifacts (logs, files) to the cloud.
//
// Since collect_logs now uploads artifacts directly during collection,
// this handler only uploads artifacts that were NOT already uploaded
// (e.g., due to a transient failure during collection).
//
// All uploads include retry logic with exponential backoff.
type UploadFilesHandler struct{}

func (h *UploadFilesHandler) Execute(ctx context.Context, ictx *Context, params json.RawMessage) (*dag.StepResult, error) {
	var p UploadFilesParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, fmt.Errorf("invalid upload_files params: %w", err)
	}

	if ictx.APIClient == nil {
		return nil, fmt.Errorf("upload_files: no API client configured")
	}

	// Extract artifacts that still need uploading (have content and weren't already uploaded)
	var files []artifactEntry
	if p.CollectLogsStep != "" {
		files = append(files, extractPendingArtifacts(ictx.State, p.CollectLogsStep, "logs")...)
		files = append(files, extractPendingArtifacts(ictx.State, p.CollectLogsStep, "artifacts")...)
	}

	if len(files) == 0 {
		return &dag.StepResult{
			Status: dag.StepCompleted,
			Data:   map[string]interface{}{"uploaded": float64(0)},
		}, nil
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var uploadErr error
	uploaded := 0

	for _, f := range files {
		wg.Add(1)
		go func(entry artifactEntry) {
			defer wg.Done()

			// Retry up to 3 times with exponential backoff
			var err error
			for attempt := 0; attempt < 3; attempt++ {
				if attempt > 0 {
					time.Sleep(time.Duration(1<<uint(attempt)) * 500 * time.Millisecond) // 500ms, 1s, 2s
				}
				err = ictx.APIClient.UploadArtifact(ictx.JobToken, ictx.JobID, entry.Name, entry.Content)
				if err == nil {
					break
				}
			}

			mu.Lock()
			if err != nil {
				if uploadErr == nil {
					uploadErr = fmt.Errorf("failed to upload %s after 3 attempts: %w", entry.Name, err)
				}
			} else {
				uploaded++
			}
			mu.Unlock()
		}(f)
	}
	wg.Wait()

	if uploadErr != nil {
		return nil, uploadErr
	}

	return &dag.StepResult{
		Status: dag.StepCompleted,
		Data:   map[string]interface{}{"uploaded": float64(uploaded)},
	}, nil
}

type artifactEntry struct {
	Name    string
	Content string
}

// extractPendingArtifacts returns artifacts from step data that have content
// and were NOT already uploaded by collect_logs.
func extractPendingArtifacts(state *dag.StateBag, stepName, field string) []artifactEntry {
	r := state.Get(stepName)
	if r == nil || r.Data == nil {
		return nil
	}

	arr, ok := r.Data[field].([]interface{})
	if !ok {
		return nil
	}

	var entries []artifactEntry
	for _, item := range arr {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		// Skip already-uploaded artifacts (collect_logs sets "uploaded": true)
		if uploaded, _ := m["uploaded"].(bool); uploaded {
			continue
		}

		name, _ := m["name"].(string)
		content, _ := m["content"].(string)
		if name != "" && content != "" {
			entries = append(entries, artifactEntry{Name: name, Content: content})
		}
	}
	return entries
}
