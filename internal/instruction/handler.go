// Package instruction implements instruction handlers for the DAG executor.
// Each handler wraps an existing capability (Docker, HTTP, CLI, assertions)
// and exposes it as a generic instruction that the DAG executor can invoke.
//
// Handlers receive a shared Context with the Docker manager, state bag,
// API client, and log collector. They are registered in a Registry which
// bridges them to the DAG executor's handler interface.
package instruction

import (
	"context"
	"encoding/json"
	"io"
	"sync"

	"chiperka-worker/internal/dag"
	"chiperka-worker/internal/docker"
)

// Handler executes an instruction and returns a step result.
// context.Context is passed separately to avoid race conditions in parallel steps.
type Handler interface {
	Execute(ctx context.Context, ictx *Context, params json.RawMessage) (*dag.StepResult, error)
}

// HandlerFunc adapts a function to the Handler interface.
type HandlerFunc func(ctx context.Context, ictx *Context, params json.RawMessage) (*dag.StepResult, error)

func (f HandlerFunc) Execute(ctx context.Context, ictx *Context, params json.RawMessage) (*dag.StepResult, error) {
	return f(ctx, ictx, params)
}

// Context carries shared mutable resources through the instruction execution lifecycle.
// It is safe for concurrent use - Manager is protected by a mutex since it's
// set by start_services and read by all subsequent handlers.
type Context struct {
	mu        sync.RWMutex
	manager   *docker.Manager
	State     *dag.StateBag
	APIClient APIClient
	Logs      *LogCollector
	JobID     string
	JobToken  string
	WorkerID  string
}

// APIClient is the interface instruction handlers use for cloud communication.
type APIClient interface {
	DownloadJobFile(token, jobID, path string) ([]byte, error)
	UploadArtifact(token string, jobID string, name string, content string) error
	UploadArtifactStream(token, jobID, name string, content io.Reader, size int64) error
}

// Manager returns the Docker manager (nil until start_services creates it).
func (c *Context) Manager() *docker.Manager {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.manager
}

// SetManager stores the Docker manager in the context.
// Called by start_services after creating the manager.
func (c *Context) SetManager(m *docker.Manager) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.manager = m
}

// Log writes a log entry to the collector (if present). Non-blocking.
func (c *Context) Log(level, action, service, message, step string) {
	if c.Logs != nil {
		c.Logs.Log(level, action, service, message, step)
	}
}

// FlushLogs forces an immediate flush of buffered logs.
// Call on step transitions for timely delivery.
func (c *Context) FlushLogs() {
	if c.Logs != nil {
		c.Logs.Flush()
	}
}

// DockerLogger returns a docker.Logger that routes Docker manager logs
// into the job's LogCollector. Docker operations (pull, start, healthcheck)
// appear in the job's log stream.
func (c *Context) DockerLogger() docker.Logger {
	return &logCollectorLogger{ctx: c}
}

// logCollectorLogger adapts the instruction Context to docker.Logger.
type logCollectorLogger struct {
	ctx *Context
}

func (l *logCollectorLogger) Info(msg string) {
	l.ctx.Log("info", "docker", "", msg, "start_services")
}

func (l *logCollectorLogger) Warn(msg string) {
	l.ctx.Log("warn", "docker", "", msg, "start_services")
}

func (l *logCollectorLogger) Fail(msg string) {
	l.ctx.Log("fail", "docker", "", msg, "start_services")
}
