package instruction

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"chiperka-worker/internal/dag"
)

// DownloadFilesParams are the parameters for the download_files instruction.
type DownloadFilesParams struct {
	Paths         []string `json:"paths"`
	SuiteFilePath string   `json:"suite_file_path,omitempty"`
}

// DownloadFilesHandler downloads job-related files (snapshots, configs) from the cloud.
// Uses GET /api/jobs/{job_id}/files/{path} with retry on transient failures.
type DownloadFilesHandler struct{}

func (h *DownloadFilesHandler) Execute(ctx context.Context, ictx *Context, params json.RawMessage) (*dag.StepResult, error) {
	var p DownloadFilesParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, fmt.Errorf("invalid download_files params: %w", err)
	}

	if len(p.Paths) == 0 {
		return &dag.StepResult{
			Status: dag.StepCompleted,
			Data:   map[string]interface{}{"downloaded": float64(0)},
		}, nil
	}

	if ictx.APIClient == nil {
		return nil, fmt.Errorf("download_files: no API client configured")
	}

	tempDir, err := os.MkdirTemp("", "chiperka-snapshots-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp directory: %w", err)
	}

	for _, filePath := range p.Paths {
		// Security: validate path to prevent directory traversal attacks
		if err := validateFilePath(filePath); err != nil {
			os.RemoveAll(tempDir)
			return nil, fmt.Errorf("invalid file path %q: %w", filePath, err)
		}

		var content []byte
		var dlErr error
		for attempt := 0; attempt < 3; attempt++ {
			content, dlErr = ictx.APIClient.DownloadJobFile(ictx.JobToken, ictx.JobID, filePath)
			if dlErr == nil {
				break
			}
			// Retry on transient errors (429, 5xx)
			if isRetryableDownloadError(dlErr) {
				time.Sleep(time.Duration(attempt+1) * time.Second)
				continue
			}
			break // non-retryable
		}
		if dlErr != nil {
			os.RemoveAll(tempDir)
			return nil, fmt.Errorf("failed to download %s: %w", filePath, dlErr)
		}

		destPath := filepath.Join(tempDir, filePath)
		if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
			os.RemoveAll(tempDir)
			return nil, fmt.Errorf("failed to create directory for %s: %w", filePath, err)
		}

		if err := os.WriteFile(destPath, content, 0644); err != nil {
			os.RemoveAll(tempDir)
			return nil, fmt.Errorf("failed to write %s: %w", filePath, err)
		}
	}

	effectiveSuiteFilePath := ""
	if p.SuiteFilePath != "" {
		effectiveSuiteFilePath = filepath.Join(tempDir, "root.chiperka")
	}

	return &dag.StepResult{
		Status: dag.StepCompleted,
		Data: map[string]interface{}{
			"temp_dir":                  tempDir,
			"effective_suite_file_path": effectiveSuiteFilePath,
			"downloaded":                float64(len(p.Paths)),
		},
	}, nil
}

// validateFilePath rejects paths that could escape the download directory.
func validateFilePath(path string) error {
	// Reject absolute paths
	if filepath.IsAbs(path) {
		return fmt.Errorf("absolute paths not allowed")
	}
	// Reject path traversal
	clean := filepath.Clean(path)
	if strings.HasPrefix(clean, "..") || strings.Contains(clean, string(filepath.Separator)+"..") {
		return fmt.Errorf("path traversal not allowed")
	}
	// Reject empty paths
	if clean == "." || clean == "" {
		return fmt.Errorf("empty path not allowed")
	}
	return nil
}

// isRetryableDownloadError checks if an error is worth retrying.
func isRetryableDownloadError(err error) bool {
	s := err.Error()
	return strings.Contains(s, "429") ||
		strings.Contains(s, "500") ||
		strings.Contains(s, "502") ||
		strings.Contains(s, "503") ||
		strings.Contains(s, "504")
}
