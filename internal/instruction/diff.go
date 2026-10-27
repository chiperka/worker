package instruction

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"chiperka-worker/internal/dag"
)

// DiffHandler computes a unified diff between two collected artifact files.
type DiffHandler struct{}

type DiffParams struct {
	Source          string `json:"source"`             // source artifact name
	Target          string `json:"target"`             // target artifact name
	Output          string `json:"output"`             // output artifact name
	Optional        bool   `json:"optional,omitempty"` // skip if files missing
	CollectLogsStep string `json:"collect_logs_step"`  // step name to get artifact paths from
}

func (h *DiffHandler) Execute(ctx context.Context, ictx *Context, params json.RawMessage) (*dag.StepResult, error) {
	var p DiffParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, fmt.Errorf("diff: invalid params: %w", err)
	}
	// Find artifact paths from collect_logs step result
	sourcePath, sourceFound := findArtifactPath(ictx, p.CollectLogsStep, p.Source)
	targetPath, targetFound := findArtifactPath(ictx, p.CollectLogsStep, p.Target)

	if !sourceFound || !targetFound {
		if p.Optional {
			ictx.Log("info", "transform", "", fmt.Sprintf("Skipping diff: source=%v target=%v (optional)", sourceFound, targetFound), "transform")
			return &dag.StepResult{
				Status: dag.StepCompleted,
				Data: map[string]interface{}{
					"skipped": true,
					"reason":  "source or target artifact not found",
				},
			}, nil
		}
		missing := []string{}
		if !sourceFound {
			missing = append(missing, p.Source)
		}
		if !targetFound {
			missing = append(missing, p.Target)
		}
		return &dag.StepResult{
			Status: dag.StepFailed,
			Data: map[string]interface{}{
				"error": fmt.Sprintf("artifact(s) not found: %v", missing),
			},
		}, nil
	}

	// Read both files
	sourceContent, err := os.ReadFile(sourcePath)
	if err != nil {
		return nil, fmt.Errorf("transform: failed to read source %q: %w", p.Source, err)
	}
	targetContent, err := os.ReadFile(targetPath)
	if err != nil {
		return nil, fmt.Errorf("transform: failed to read target %q: %w", p.Target, err)
	}

	// Compute diff
	diff := computeDiff(p.Source, p.Target, string(sourceContent), string(targetContent))

	// Write diff to temp file
	outputDir := filepath.Dir(sourcePath)
	outputPath := filepath.Join(outputDir, p.Output)
	os.MkdirAll(filepath.Dir(outputPath), 0755)
	if err := os.WriteFile(outputPath, []byte(diff), 0644); err != nil {
		return nil, fmt.Errorf("transform: failed to write output %q: %w", p.Output, err)
	}

	// Upload diff as artifact
	if ictx.APIClient != nil && len(diff) > 0 {
		f, err := os.Open(outputPath)
		if err == nil {
			ictx.APIClient.UploadArtifactStream(ictx.JobToken, ictx.JobID, p.Output, f, int64(len(diff)))
			f.Close()
		}
	}

	ictx.Log("info", "transform", "", fmt.Sprintf("Diff %s vs %s → %s (%d bytes)", p.Source, p.Target, p.Output, len(diff)), "transform")

	return &dag.StepResult{
		Status: dag.StepCompleted,
		Data: map[string]interface{}{
			"type":        "diff",
			"source":      p.Source,
			"target":      p.Target,
			"output":      p.Output,
			"output_path": outputPath,
			"output_size": len(diff),
			"identical":   bytes.Equal(sourceContent, targetContent),
		},
	}, nil
}

// findArtifactPath looks up an artifact's disk path from the collect_logs step result.
func findArtifactPath(ictx *Context, stepName, artifactName string) (string, bool) {
	r := ictx.State.Get(stepName)
	if r == nil || r.Data == nil {
		return "", false
	}
	artifacts, ok := r.Data["artifacts"]
	if !ok {
		return "", false
	}
	arr, ok := artifacts.([]interface{})
	if !ok {
		return "", false
	}
	for _, item := range arr {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		name, _ := m["name"].(string)
		path, _ := m["path"].(string)
		if path == "" {
			continue
		}
		// Match by exact name or basename
		if name == artifactName || filepath.Base(name) == artifactName {
			return path, true
		}
	}
	return "", false
}

// computeDiff produces the diff format of the Chiperka CLI: the ---/+++ header
// followed by the deleted and inserted lines only, without hunks or context.
func computeDiff(nameA, nameB, contentA, contentB string) string {
	linesA := splitLines(contentA)
	linesB := splitLines(contentB)

	if len(linesA) == 0 && len(linesB) == 0 {
		return ""
	}

	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("--- %s\n", nameA))
	buf.WriteString(fmt.Sprintf("+++ %s\n", nameB))

	for _, e := range diffLines(linesA, linesB) {
		switch e.op {
		case opDelete:
			buf.WriteString("-" + e.line + "\n")
		case opInsert:
			buf.WriteString("+" + e.line + "\n")
		}
	}
	return buf.String()
}

type editOp int

const (
	opEqual editOp = iota
	opDelete
	opInsert
)

type edit struct {
	op   editOp
	line string
}

func splitLines(s string) []string {
	if s == "" {
		return nil
	}
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}

// diffLines computes edits to transform A into B using a simple O(NM) LCS.
func diffLines(a, b []string) []edit {
	n, m := len(a), len(b)

	// For very large files, fall back to simple output
	if n*m > 10_000_000 {
		var edits []edit
		for _, line := range a {
			edits = append(edits, edit{op: opDelete, line: line})
		}
		for _, line := range b {
			edits = append(edits, edit{op: opInsert, line: line})
		}
		return edits
	}

	// LCS table
	dp := make([][]int, n+1)
	for i := range dp {
		dp[i] = make([]int, m+1)
	}
	for i := 1; i <= n; i++ {
		for j := 1; j <= m; j++ {
			if a[i-1] == b[j-1] {
				dp[i][j] = dp[i-1][j-1] + 1
			} else if dp[i-1][j] >= dp[i][j-1] {
				dp[i][j] = dp[i-1][j]
			} else {
				dp[i][j] = dp[i][j-1]
			}
		}
	}

	// Backtrack to produce edits
	var edits []edit
	i, j := n, m
	for i > 0 || j > 0 {
		if i > 0 && j > 0 && a[i-1] == b[j-1] {
			edits = append(edits, edit{op: opEqual, line: a[i-1]})
			i--
			j--
		} else if j > 0 && (i == 0 || dp[i][j-1] >= dp[i-1][j]) {
			edits = append(edits, edit{op: opInsert, line: b[j-1]})
			j--
		} else {
			edits = append(edits, edit{op: opDelete, line: a[i-1]})
			i--
		}
	}

	// Reverse
	for l, r := 0, len(edits)-1; l < r; l, r = l+1, r-1 {
		edits[l], edits[r] = edits[r], edits[l]
	}
	return edits
}
