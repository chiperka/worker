package instruction

import (
	"context"
	"encoding/json"
	"fmt"

	"chiperka-worker/internal/dag"
	"chiperka-worker/internal/docker"
	"chiperka-worker/internal/model"
)

// StartServicesParams are the parameters for the start_services instruction.
type StartServicesParams struct {
	Services []model.Service `json:"services"`
	TestName string          `json:"test_name,omitempty"`
}

// StartServicesHandler creates a Docker manager, pulls images, starts services,
// and runs healthchecks.
type StartServicesHandler struct{}

func (h *StartServicesHandler) Execute(ctx context.Context, ictx *Context, params json.RawMessage) (*dag.StepResult, error) {
	var p StartServicesParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, fmt.Errorf("invalid start_services params: %w", err)
	}

	if len(p.Services) == 0 {
		return nil, fmt.Errorf("start_services: no services defined")
	}

	testName := p.TestName
	if testName == "" {
		testName = ictx.JobID
	}

	// Create Docker manager
	manager, err := docker.NewManager(ictx.DockerLogger(), testName)
	if err != nil {
		return nil, fmt.Errorf("failed to create Docker manager: %w", err)
	}

	// Pre-pull images
	for _, svc := range p.Services {
		ictx.Log("info", "image_pull", svc.Name, fmt.Sprintf("Pulling %s", svc.Image), "start_services")
	}
	docker.PullImages(ctx, p.Services)

	// Start services (creates network, starts containers, healthchecks)
	ictx.Log("info", "services_starting", "", fmt.Sprintf("Starting %d service(s)", len(p.Services)), "start_services")
	serviceResults, err := manager.StartServices(ctx, p.Services)
	if err != nil {
		ictx.Log("fail", "services_failed", "", fmt.Sprintf("Failed to start services: %v", err), "start_services")
		manager.Cleanup(ctx)
		return nil, fmt.Errorf("failed to start services: %w", err)
	}

	// Store manager in shared context for subsequent instructions
	ictx.SetManager(manager)

	for _, sr := range serviceResults {
		ictx.Log("pass", "service_ready", sr.Name, fmt.Sprintf("Service %s ready (%.1fs)", sr.Name, sr.Duration.Seconds()), "start_services")
	}

	// Build output data
	services := make(map[string]interface{})
	for _, sr := range serviceResults {
		services[sr.Name] = map[string]interface{}{
			"image":                       sr.Image,
			"duration_ms":                 sr.Duration.Milliseconds(),
			"image_resolve_duration_ms":   sr.ImageResolveDuration.Milliseconds(),
			"container_start_duration_ms": sr.ContainerStartDuration.Milliseconds(),
			"health_check_duration_ms":    sr.HealthCheckDuration.Milliseconds(),
		}
	}

	return &dag.StepResult{
		Status: dag.StepCompleted,
		Data: map[string]interface{}{
			"network_name": manager.NetworkName(),
			"services":     services,
		},
	}, nil
}
