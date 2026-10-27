package apiclient

import (
	"context"
	"io"
	"time"

	"chiperka-worker/internal/instruction"
)

// InstructionAdapter bridges the Client to the instruction.APIClient interface.
// Instruction handlers use a minimal interface; this adapter translates.
type InstructionAdapter struct {
	Client *Client
}

func (a *InstructionAdapter) DownloadJobFile(token, jobID, path string) ([]byte, error) {
	return a.Client.DownloadJobFile(context.Background(), token, jobID, path)
}

func (a *InstructionAdapter) UploadArtifact(token, jobID, name, content string) error {
	return a.Client.UploadArtifact(context.Background(), token, jobID, name, content)
}

func (a *InstructionAdapter) UploadArtifactStream(token, jobID, name string, content io.Reader, size int64) error {
	return a.Client.UploadArtifactStream(context.Background(), token, jobID, name, content, size)
}

// LogSenderAdapter bridges the Client to the instruction.LogSender interface.
type LogSenderAdapter struct {
	Client *Client
	Token  string // job-scoped token for log submissions
}

func (a *LogSenderAdapter) SendLogs(jobID string, logs []instruction.LogEntry) (time.Duration, error) {
	return a.Client.SendLogs(context.Background(), a.Token, jobID, logs)
}
