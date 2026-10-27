package instruction

import (
	"context"
	"encoding/json"
	"fmt"

	"chiperka-worker/internal/dag"
)

// Registry maps instruction types to their handlers.
type Registry struct {
	handlers map[string]Handler
}

// NewRegistry creates an empty instruction registry.
func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[string]Handler),
	}
}

// Register adds a handler for an instruction type.
func (r *Registry) Register(instruction string, handler Handler) {
	r.handlers[instruction] = handler
}

// RegisterFunc adds a function handler for an instruction type.
func (r *Registry) RegisterFunc(instruction string, fn func(context.Context, *Context, json.RawMessage) (*dag.StepResult, error)) {
	r.handlers[instruction] = HandlerFunc(fn)
}

// Get returns the handler for an instruction type, or nil.
func (r *Registry) Get(instruction string) Handler {
	return r.handlers[instruction]
}

// BuildDAGHandlers returns a map of dag.Handler functions that wrap the instruction
// handlers with the shared Context. Each DAG handler receives its own context.Context
// from the executor (safe for parallel execution), while sharing the mutable Context.
func (r *Registry) BuildDAGHandlers(instrCtx *Context) map[string]dag.Handler {
	dagHandlers := make(map[string]dag.Handler, len(r.handlers))
	for name, handler := range r.handlers {
		h := handler // capture
		dagHandlers[name] = func(ctx context.Context, params json.RawMessage) (*dag.StepResult, error) {
			return h.Execute(ctx, instrCtx, params)
		}
	}
	return dagHandlers
}

// DefaultRegistry creates a registry with all built-in instruction handlers.
func DefaultRegistry() *Registry {
	r := NewRegistry()
	r.Register("start_services", &StartServicesHandler{})
	r.Register("http_request", &HTTPRequestHandler{})
	r.Register("cli_exec", &CLIExecHandler{})
	r.Register("assert", &AssertHandler{})
	r.Register("collect_logs", &CollectLogsHandler{})
	r.Register("extract_artifacts", &ExtractArtifactsHandler{})
	r.Register("cleanup", &CleanupHandler{})
	r.Register("download_files", &DownloadFilesHandler{})
	r.Register("upload_files", &UploadFilesHandler{})
	r.Register("diff", &DiffHandler{})
	return r
}

// ListInstructions returns all registered instruction names.
func (r *Registry) ListInstructions() []string {
	names := make([]string, 0, len(r.handlers))
	for name := range r.handlers {
		names = append(names, name)
	}
	return names
}

// Validate checks that all instructions in a job have registered handlers.
func (r *Registry) Validate(job *dag.Job) error {
	for name, step := range job.Steps {
		if _, ok := r.handlers[step.Instruction]; !ok {
			return fmt.Errorf("step %q uses unknown instruction %q", name, step.Instruction)
		}
	}
	return nil
}
