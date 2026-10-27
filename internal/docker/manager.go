// Package docker provides Docker container management for test services.
package docker

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	imagetypes "github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	dockerclient "github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"

	"chiperka-worker/internal/model"
)

// Logger is a simple logging interface for Docker operations.
type Logger interface {
	Info(msg string)
	Warn(msg string)
	Fail(msg string)
}

// noopLogger discards all log messages.
type noopLogger struct{}

func (noopLogger) Info(string) {}
func (noopLogger) Warn(string) {}
func (noopLogger) Fail(string) {}

// Manager handles Docker container lifecycle for test services.
// Each test gets its own isolated network for parallel execution.
type Manager struct {
	// client is the Docker API client (interface for testability)
	client DockerAPI
	// networkID is the isolated network for this test
	networkID string
	// networkName is the human-readable network name
	networkName string
	// runningContainers tracks containers started by this manager for cleanup
	runningContainers map[string]string // service name -> container ID
	// containersMu protects runningContainers
	containersMu sync.Mutex
	// logger is the logger for this manager
	logger Logger
	// testName is the name of the test this manager belongs to
	testName string
	// pool is the network pool for reusing networks (nil if not using pool)
	pool *NetworkPool
	// serviceArtifactDefs stores artifact definitions per service name
	serviceArtifactDefs map[string][]model.ServiceArtifact
}

// defaultDockerClient is a shared Docker SDK client for API calls.
// Uses client.FromEnv to respect DOCKER_HOST, DOCKER_TLS_VERIFY, etc.
var defaultDockerClient DockerAPI
var defaultDockerClientErr error

func init() {
	defaultDockerClient, defaultDockerClientErr = dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation())
}

// DefaultClient returns the package-level Docker API client.
// Returns error if Docker SDK initialization failed.
func DefaultClient() (DockerAPI, error) {
	if defaultDockerClientErr != nil {
		return nil, defaultDockerClientErr
	}
	return defaultDockerClient, nil
}

// WaitForDaemon blocks until the Docker daemon is reachable or the context is cancelled.
// It retries every 2 seconds and logs each attempt.
func WaitForDaemon(ctx context.Context) error {
	if defaultDockerClientErr != nil {
		return fmt.Errorf("docker client init failed: %w", defaultDockerClientErr)
	}

	for {
		_, err := defaultDockerClient.Ping(ctx)
		if err == nil {
			return nil
		}

		fmt.Printf("Waiting for Docker daemon... (%v)\n", err)

		select {
		case <-ctx.Done():
			return fmt.Errorf("docker daemon not available: %w", ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
}

const (
	maxServiceAttempts    = 3
	serviceRetryDelay    = 3 * time.Second
	dockerRetryAttempts  = 3
	dockerRetryBaseDelay = 500 * time.Millisecond
)

// retryDockerCall retries a Docker API call with exponential backoff.
// Only retries on transient errors (timeouts, connection resets), not on
// logical errors (container not found, etc.).
func retryDockerCall[T any](ctx context.Context, operation string, logger Logger, testName string, fn func() (T, error)) (T, error) {
	var lastErr error
	for attempt := 1; attempt <= dockerRetryAttempts; attempt++ {
		result, err := fn()
		if err == nil {
			return result, nil
		}
		if !isTransientDockerError(err) || ctx.Err() != nil {
			return result, err
		}
		lastErr = err
		if attempt < dockerRetryAttempts {
			delay := dockerRetryBaseDelay * time.Duration(1<<(attempt-1)) // 500ms, 1s
			if logger != nil {
				logger.Warn(fmt.Sprintf("[%s] Docker API call %s failed, retrying in %s: %v", testName, operation, delay, err))
			}
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return result, ctx.Err()
			}
		}
	}
	var zero T
	return zero, lastErr
}

// retryDockerCallNoResult is like retryDockerCall but for functions that return only error.
func retryDockerCallNoResult(ctx context.Context, operation string, logger Logger, testName string, fn func() error) error {
	_, err := retryDockerCall(ctx, operation, logger, testName, func() (struct{}, error) {
		return struct{}{}, fn()
	})
	return err
}

func isTransientDockerError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "context deadline exceeded") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "i/o timeout") ||
		strings.Contains(msg, "TLS handshake timeout") ||
		strings.Contains(msg, "EOF") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "connection timed out") ||
		strings.Contains(msg, "server misbehaving")
}

// containerSem limits concurrent ContainerCreate+ContainerStart calls across all
// Manager instances to prevent overwhelming the Docker daemon.
var containerSem chan struct{}

// SetMaxConcurrentContainers limits how many Docker containers can be
// created simultaneously across all managers. Default (0) means unlimited.
func SetMaxConcurrentContainers(n int) {
	if n > 0 {
		containerSem = make(chan struct{}, n)
	}
}

func acquireContainerSlot(ctx context.Context) error {
	if containerSem == nil {
		return nil
	}
	select {
	case containerSem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func releaseContainerSlot() {
	if containerSem != nil {
		<-containerSem
	}
}

// NewManager creates a new Docker manager with an isolated network.
// Uses the default Docker client.
func NewManager(logger Logger, testName string) (*Manager, error) {
	client, err := DefaultClient()
	if err != nil {
		return nil, err
	}
	return NewManagerWithClient(client, logger, testName)
}

// NewManagerWithClient creates a new Docker manager with a custom Docker API client.
// Use this in tests with a mock DockerAPI.
func NewManagerWithClient(client DockerAPI, logger Logger, testName string) (*Manager, error) {
	if logger == nil {
		logger = noopLogger{}
	}
	// Verify docker is available
	if _, err := client.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("docker not available: %w", err)
	}

	// Create unique network name using random bytes
	randomBytes := make([]byte, 8)
	if _, err := rand.Read(randomBytes); err != nil {
		return nil, fmt.Errorf("failed to generate random network name: %w", err)
	}
	networkName := fmt.Sprintf("chiperka-%s", hex.EncodeToString(randomBytes))

	logger.Info(fmt.Sprintf("[%s] Creating isolated Docker network %s", testName, networkName))

	// Create isolated network with retry on pool exhaustion
	networkID, err := createNetworkWithRetry(client, networkName, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create network: %w", err)
	}

	m := &Manager{
		client:             client,
		networkID:          networkID,
		networkName:        networkName,
		runningContainers: make(map[string]string),
		logger:            logger,
		testName:          testName,
		pool:              nil,
	}

	return m, nil
}

// NewManagerWithPool creates a new Docker manager using a network from the pool.
func NewManagerWithPool(logger Logger, testName string, pool *NetworkPool) (*Manager, error) {
	client, err := DefaultClient()
	if err != nil {
		return nil, err
	}
	if logger == nil {
		logger = noopLogger{}
	}
	// Verify docker is available
	if _, err := client.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("docker not available: %w", err)
	}

	// Acquire network from pool
	networkName, err := pool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("failed to acquire network from pool: %w", err)
	}

	logger.Info(fmt.Sprintf("[%s] Acquired network %s from pool", testName, networkName))

	m := &Manager{
		client:             client,
		networkID:          networkName, // Pool networks use name as ID
		networkName:        networkName,
		runningContainers: make(map[string]string),
		logger:            logger,
		testName:          testName,
		pool:              pool,
	}

	return m, nil
}

// createNetworkWithRetry creates a Docker network, retrying once after cleaning stale
// chiperka networks if the first attempt fails due to address pool exhaustion.
func createNetworkWithRetry(client DockerAPI, networkName string, logger Logger) (string, error) {
	resp, err := client.NetworkCreate(context.Background(), networkName, network.CreateOptions{})
	if err == nil {
		return resp.ID, nil
	}

	// If the error is NOT about address pool exhaustion, fail immediately
	if !strings.Contains(err.Error(), "non-overlapping") {
		return "", err
	}

	// Address pool exhausted -- clean up stale chiperka networks and retry
	logger.Warn("Docker network pool exhausted, cleaning up stale chiperka networks")
	CleanupStaleNetworks()

	resp, err = client.NetworkCreate(context.Background(), networkName, network.CreateOptions{})
	if err != nil {
		return "", fmt.Errorf("failed even after cleanup: %w", err)
	}
	return resp.ID, nil
}

// CleanupStaleNetworks removes all chiperka- networks that have no running containers.
func CleanupStaleNetworks() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	networks, err := defaultDockerClient.NetworkList(ctx, network.ListOptions{
		Filters: filters.NewArgs(filters.Arg("name", "chiperka-")),
	})
	if err != nil {
		return
	}

	var wg sync.WaitGroup
	for _, net := range networks {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			// NetworkRemove will fail if the network is in use -- that's fine
			defaultDockerClient.NetworkRemove(ctx, id)
		}(net.ID)
	}
	wg.Wait()
}

// CleanupStaleContainers removes all chiperka- containers that are still running.
// This catches orphaned containers from previous runs where cleanup failed.
func CleanupStaleContainers() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	containers, err := defaultDockerClient.ContainerList(ctx, container.ListOptions{
		Filters: filters.NewArgs(filters.Arg("name", "chiperka-")),
	})
	if err != nil {
		return
	}

	if len(containers) == 0 {
		return
	}

	var wg sync.WaitGroup
	for _, c := range containers {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			defaultDockerClient.ContainerRemove(ctx, id, container.RemoveOptions{Force: true, RemoveVolumes: true})
		}(c.ID)
	}
	wg.Wait()
}

// ServiceTimings holds per-phase timing data for a service startup.
type ServiceTimings struct {
	ImageResolve   time.Duration
	ContainerStart time.Duration
	HealthCheck    time.Duration
}

// NetworkName returns the name of the isolated network.
func (m *Manager) NetworkName() string {
	return m.networkName
}

// StartServices starts all services defined in the test in parallel.
// Returns service results with setup durations, or an error if any service fails.
func (m *Manager) StartServices(ctx context.Context, services []model.Service) ([]model.ServiceResult, error) {
	// Store artifact definitions for later collection
	m.serviceArtifactDefs = make(map[string][]model.ServiceArtifact)
	for _, svc := range services {
		if len(svc.Artifacts) > 0 {
			m.serviceArtifactDefs[svc.Name] = svc.Artifacts
		}
	}

	results := make([]model.ServiceResult, len(services))
	errors := make([]error, len(services))

	var wg sync.WaitGroup
	for i, service := range services {
		wg.Add(1)
		go func(idx int, svc model.Service) {
			defer wg.Done()
			imageName, timings, err := m.startService(ctx, svc)
			if err != nil {
				errors[idx] = fmt.Errorf("failed to start service %s: %w", svc.Name, err)
				return
			}
			results[idx] = model.ServiceResult{
				Name:                   svc.Name,
				Image:                  imageName,
				Duration:               timings.ImageResolve + timings.ContainerStart + timings.HealthCheck,
				ImageResolveDuration:   timings.ImageResolve,
				ContainerStartDuration: timings.ContainerStart,
				HealthCheckDuration:    timings.HealthCheck,
			}
		}(i, service)
	}
	wg.Wait()

	// Check for errors
	for _, err := range errors {
		if err != nil {
			return results, err
		}
	}

	return results, nil
}

// startService starts a single Docker service in the isolated network.
// Returns the image name, phase timings, and any error.
// If the service fails healthcheck, it is restarted up to maxAttempts times.
func (m *Manager) startService(ctx context.Context, service model.Service) (string, ServiceTimings, error) {
	var timings ServiceTimings

	// Resolve image once (build or pull)
	resolveStart := time.Now()
	imageName, err := m.resolveImage(ctx, service)
	timings.ImageResolve = time.Since(resolveStart)
	if err != nil {
		return "", timings, err
	}

	var lastErr error

	for attempt := 1; attempt <= maxServiceAttempts; attempt++ {
		if attempt > 1 {
			m.logger.Warn(fmt.Sprintf("[%s] Restarting service (attempt %d/%d): %v", m.testName, attempt, maxServiceAttempts, lastErr))
			time.Sleep(serviceRetryDelay)
			// Clean up previous failed container
			containerName := fmt.Sprintf("%s-%s", m.networkName, service.Name)
			if service.ContainerName != "" {
				containerName = service.ContainerName
			}
			m.containersMu.Lock()
			if oldID, ok := m.runningContainers[service.Name]; ok {
				delete(m.runningContainers, service.Name)
				m.containersMu.Unlock()
				m.client.ContainerRemove(ctx, oldID, container.RemoveOptions{Force: true, RemoveVolumes: true})
			} else {
				m.containersMu.Unlock()
				m.client.ContainerRemove(ctx, containerName, container.RemoveOptions{Force: true, RemoveVolumes: true})
			}
		}

		containerStart, healthCheck, err := m.startAndHealthcheck(ctx, service, imageName)
		timings.ContainerStart = containerStart
		timings.HealthCheck = healthCheck
		if err == nil {
			return imageName, timings, nil
		}
		lastErr = err
	}

	return "", timings, fmt.Errorf("service %s failed after %d attempts: %w", service.Name, maxServiceAttempts, lastErr)
}

// resolveImage pulls the Docker image for a service if not already present.
func (m *Manager) resolveImage(ctx context.Context, service model.Service) (string, error) {
	if service.Image == "" {
		return "", fmt.Errorf("service %s must have 'image' specified", service.Name)
	}

	_, _, err := m.client.ImageInspectWithRaw(ctx, service.Image)
	if err != nil {
		m.logger.Info(fmt.Sprintf("[%s] Pulling Docker image", m.testName))
		reader, pullErr := m.client.ImagePull(ctx, service.Image, imagetypes.PullOptions{
			RegistryAuth: getRegistryAuth(service.Image),
		})
		if pullErr != nil {
			return "", fmt.Errorf("failed to pull image %s: %w", service.Image, pullErr)
		}
		defer reader.Close()
		io.Copy(io.Discard, reader) // drain to complete pull
	} else {
		m.logger.Info(fmt.Sprintf("[%s] Using existing local image", m.testName))
	}
	return service.Image, nil
}

// imagePullInflight deduplicates concurrent pulls of the same image.
// Multiple tests requesting the same image will share a single pull operation.
var imagePullInflight sync.Map // image name -> *pullResult

type pullResult struct {
	done chan struct{} // closed when pull completes
}

// PullImages pre-pulls all images for the given services before the test timeout starts.
// This ensures image pull time doesn't eat into the healthcheck/execution budget.
// Concurrent calls for the same image are deduplicated — only one pull runs at a time.
// Errors are silently ignored — if a pull fails here, it will fail again during
// StartServices where the error is properly handled.
func PullImages(ctx context.Context, services []model.Service) {
	if defaultDockerClient == nil {
		return
	}
	var wg sync.WaitGroup
	for _, svc := range services {
		if svc.Image == "" {
			continue
		}
		wg.Add(1)
		go func(image string) {
			defer wg.Done()
			pullImageOnce(ctx, image)
		}(svc.Image)
	}
	wg.Wait()
}

func pullImageOnce(ctx context.Context, image string) {
	// Skip if image already exists locally
	if _, _, err := defaultDockerClient.ImageInspectWithRaw(ctx, image); err == nil {
		return
	}

	// Deduplicate: if another goroutine is already pulling this image, wait for it
	pr := &pullResult{done: make(chan struct{})}
	if existing, loaded := imagePullInflight.LoadOrStore(image, pr); loaded {
		// Another pull is in progress — wait for it
		existing := existing.(*pullResult)
		select {
		case <-existing.done:
			return
		case <-ctx.Done():
			return
		}
	}

	// We own this pull
	defer func() {
		close(pr.done)
		imagePullInflight.Delete(image)
	}()

	reader, err := defaultDockerClient.ImagePull(ctx, image, imagetypes.PullOptions{
		RegistryAuth: getRegistryAuth(image),
	})
	if err != nil {
		return
	}
	io.Copy(io.Discard, reader)
	reader.Close()
}

// buildHealthConfig converts a model.HealthCheck to a Docker SDK HealthConfig.
func buildHealthConfig(hc *model.HealthCheck) *container.HealthConfig {
	if hc.Test == "" {
		return nil
	}

	interval := parseDurationOrDefault(hc.Interval, time.Second)
	timeout := parseDurationOrDefault(hc.Timeout, 3*time.Second)
	startPeriod := parseDurationOrDefault(hc.StartPeriod, 0)
	retries := hc.Retries
	if retries == 0 {
		retries = 30
	}

	cfg := &container.HealthConfig{
		Test:        []string{"CMD-SHELL", string(hc.Test)},
		Interval:    interval,
		Timeout:     timeout,
		Retries:     retries,
		StartPeriod: startPeriod,
	}
	if hc.StartInterval != "" {
		cfg.StartInterval = parseDurationOrDefault(hc.StartInterval, 0)
	}

	return cfg
}

// parseDurationOrDefault parses a duration string, returning the default on error or empty input.
func parseDurationOrDefault(s string, def time.Duration) time.Duration {
	if s == "" {
		return def
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return def
	}
	return d
}

// startAndHealthcheck starts a container and runs its healthcheck.
// Returns (containerStartDuration, healthCheckDuration, error).
func (m *Manager) startAndHealthcheck(ctx context.Context, service model.Service, imageName string) (time.Duration, time.Duration, error) {
	m.logger.Info(fmt.Sprintf("[%s] Starting container", m.testName))

	// Determine container name
	containerName := fmt.Sprintf("%s-%s", m.networkName, service.Name)
	if service.ContainerName != "" {
		containerName = service.ContainerName
	}

	// Pre-cleanup: remove any stale container with the same name (from interrupted runs)
	m.client.ContainerRemove(ctx, containerName, container.RemoveOptions{Force: true, RemoveVolumes: true})

	// Build container config
	cfg := &container.Config{
		Image: imageName,
	}

	if service.HealthCheck != nil {
		cfg.Healthcheck = buildHealthConfig(service.HealthCheck)
	}

	if service.WorkingDir != "" {
		cfg.WorkingDir = service.WorkingDir
	}

	for k, v := range service.Environment {
		cfg.Env = append(cfg.Env, fmt.Sprintf("%s=%s", k, v))
	}

	if len(service.Command) > 0 {
		cfg.Cmd = []string(service.Command)
	}

	// Host config
	hostCfg := &container.HostConfig{
		NetworkMode: container.NetworkMode(m.networkName),
	}

	// Network config with alias
	networkCfg := &network.NetworkingConfig{
		EndpointsConfig: map[string]*network.EndpointSettings{
			m.networkName: {
				Aliases: []string{service.Name},
			},
		},
	}

	containerStartTime := time.Now()
	if err := acquireContainerSlot(ctx); err != nil {
		return 0, 0, fmt.Errorf("failed to acquire container slot: %w", err)
	}
	resp, err := m.client.ContainerCreate(ctx, cfg, hostCfg, networkCfg, nil, containerName)
	if err != nil {
		releaseContainerSlot()
		return time.Since(containerStartTime), 0, fmt.Errorf("failed to create container: %w", err)
	}

	if err := m.client.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		releaseContainerSlot()
		return time.Since(containerStartTime), 0, fmt.Errorf("failed to start container: %w", err)
	}
	releaseContainerSlot()

	m.containersMu.Lock()
	m.runningContainers[service.Name] = resp.ID
	m.containersMu.Unlock()
	containerStartDuration := time.Since(containerStartTime)

	// Wait for health check if configured
	var healthCheckDuration time.Duration
	if service.HealthCheck != nil {
		hcStart := time.Now()
		if err := m.waitForHealthy(ctx, service); err != nil {
			return containerStartDuration, time.Since(hcStart), fmt.Errorf("health check failed: %w", err)
		}
		healthCheckDuration = time.Since(hcStart)
	}

	return containerStartDuration, healthCheckDuration, nil
}

// waitForHealthy polls Docker's native health status until the container reports healthy.
// The actual health checks are performed by Docker daemon (configured via --health-* flags on docker run).
func (m *Manager) waitForHealthy(ctx context.Context, service model.Service) error {
	hc := service.HealthCheck

	m.containersMu.Lock()
	containerID := m.runningContainers[service.Name]
	m.containersMu.Unlock()

	hcMode := "image"
	if hc.Test != "" {
		hcMode = "test"
	}
	m.logger.Info(fmt.Sprintf("[%s] Waiting for service (type=%s)", m.testName, hcMode))

	start := time.Now()
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()

	// Check immediately first, then poll
	checkHealth := func() (done bool, err error) {
		status := GetHealthStatus(ctx, containerID)
		switch status {
		case "healthy":
			m.logger.Info(fmt.Sprintf("[%s] Service is healthy (waited %.1fs)", m.testName, time.Since(start).Seconds()))
			return true, nil
		case "unhealthy":
			m.logger.Fail(fmt.Sprintf("[%s] Service unhealthy after %.1fs", m.testName, time.Since(start).Seconds()))
			return true, fmt.Errorf("service %s is unhealthy", service.Name)
		}
		return false, nil
	}

	// First check immediately
	if done, err := checkHealth(); done {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			m.logger.Fail(fmt.Sprintf("[%s] Healthcheck timed out after %.1fs", m.testName, time.Since(start).Seconds()))
			return fmt.Errorf("service %s healthcheck timed out", service.Name)
		case <-ticker.C:
			if done, err := checkHealth(); done {
				return err
			}
		}
	}
}

// GetHealthStatus queries Docker for the container's health status via SDK.
func GetHealthStatus(ctx context.Context, containerID string) string {
	inspect, err := retryDockerCall(ctx, "ContainerInspect", nil, "", func() (container.InspectResponse, error) {
		return defaultDockerClient.ContainerInspect(ctx, containerID)
	})
	if err != nil {
		return "unhealthy"
	}
	if inspect.State == nil || inspect.State.Health == nil {
		return "unhealthy"
	}
	return inspect.State.Health.Status
}

// BuildHealthCheckArgs converts a HealthCheck config into Docker --health-* flags.
// Returns nil when Test is empty (healthCheck: true -> use image's built-in HEALTHCHECK).
func BuildHealthCheckArgs(hc *model.HealthCheck) []string {
	if hc.Test == "" {
		return nil
	}

	interval := hc.Interval
	if interval == "" {
		interval = "1s"
	}
	timeout := hc.Timeout
	if timeout == "" {
		timeout = "3s"
	}
	retries := hc.Retries
	if retries == 0 {
		retries = 30
	}
	startPeriod := hc.StartPeriod
	if startPeriod == "" {
		startPeriod = "0s"
	}

	args := []string{
		"--health-cmd", string(hc.Test),
		"--health-interval", interval,
		"--health-timeout", timeout,
		"--health-retries", fmt.Sprintf("%d", retries),
		"--health-start-period", startPeriod,
	}
	if hc.StartInterval != "" {
		args = append(args, "--health-start-interval", hc.StartInterval)
	}

	return args
}

// RunInNetwork executes a command in the isolated network.
// This is used to run the test executor (e.g., HTTP client) inside the network.
func (m *Manager) RunInNetwork(ctx context.Context, image string, command []string) ([]byte, error) {
	// Ensure image exists locally (pull if needed, e.g. in DinD with empty cache)
	_, _, inspectErr := m.client.ImageInspectWithRaw(ctx, image)
	if inspectErr != nil {
		reader, pullErr := m.client.ImagePull(ctx, image, imagetypes.PullOptions{})
		if pullErr != nil {
			return nil, fmt.Errorf("failed to pull image %s: %w", image, pullErr)
		}
		io.Copy(io.Discard, reader)
		reader.Close()
	}

	cfg := &container.Config{
		Image: image,
		Cmd:   command,
	}
	hostCfg := &container.HostConfig{
		NetworkMode: container.NetworkMode(m.networkName),
	}

	if err := acquireContainerSlot(ctx); err != nil {
		return nil, fmt.Errorf("failed to acquire container slot: %w", err)
	}
	resp, err := m.client.ContainerCreate(ctx, cfg, hostCfg, nil, nil, "")
	if err != nil {
		releaseContainerSlot()
		return nil, fmt.Errorf("failed to create container: %w", err)
	}
	// Use background context so cleanup succeeds even when test ctx is cancelled
	defer m.client.ContainerRemove(context.Background(), resp.ID, container.RemoveOptions{Force: true, RemoveVolumes: true})

	if err := m.client.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		releaseContainerSlot()
		return nil, err
	}
	releaseContainerSlot()

	// Wait for container to finish
	statusCh, errCh := m.client.ContainerWait(ctx, resp.ID, container.WaitConditionNotRunning)
	var statusCode int64
	select {
	case err := <-errCh:
		if err != nil {
			return nil, err
		}
	case status := <-statusCh:
		statusCode = status.StatusCode
	}

	// Read logs
	var stdoutBuf, stderrBuf bytes.Buffer
	logReader, err := retryDockerCall(ctx, "ContainerLogs", m.logger, m.testName, func() (io.ReadCloser, error) {
		return m.client.ContainerLogs(ctx, resp.ID, container.LogsOptions{ShowStdout: true, ShowStderr: true})
	})
	if err == nil {
		stdcopy.StdCopy(&stdoutBuf, &stderrBuf, logReader)
		logReader.Close()
	}

	if statusCode != 0 {
		if stderrBuf.Len() > 0 {
			return stdoutBuf.Bytes(), fmt.Errorf("exit code %d: %s", statusCode, stderrBuf.String())
		}
		return stdoutBuf.Bytes(), fmt.Errorf("exit code %d", statusCode)
	}
	return stdoutBuf.Bytes(), nil
}

// RunInNetworkAndCopyOut executes a command in the isolated network and, after
// the container finishes, extracts the contents of outPaths from the container
// filesystem. Use this when the command produces binary output that must not
// pass through Docker's logging driver — the json-file driver mangles invalid
// UTF-8 byte sequences in stdout, corrupting binary HTTP response bodies (PDF,
// octet-stream, images, etc.).
//
// outPaths are absolute container paths whose contents are copied out via the
// CopyFromContainer tar stream and returned in outFiles, keyed by the requested
// path. Missing or unreadable paths are silently omitted.
func (m *Manager) RunInNetworkAndCopyOut(ctx context.Context, image string, command []string, outPaths []string) (stdout []byte, outFiles map[string][]byte, err error) {
	// Ensure image exists locally (pull if needed, e.g. in DinD with empty cache)
	_, _, inspectErr := m.client.ImageInspectWithRaw(ctx, image)
	if inspectErr != nil {
		reader, pullErr := m.client.ImagePull(ctx, image, imagetypes.PullOptions{})
		if pullErr != nil {
			return nil, nil, fmt.Errorf("failed to pull image %s: %w", image, pullErr)
		}
		io.Copy(io.Discard, reader)
		reader.Close()
	}

	cfg := &container.Config{
		Image: image,
		Cmd:   command,
	}
	hostCfg := &container.HostConfig{
		NetworkMode: container.NetworkMode(m.networkName),
	}

	if err := acquireContainerSlot(ctx); err != nil {
		return nil, nil, fmt.Errorf("failed to acquire container slot: %w", err)
	}
	resp, err := m.client.ContainerCreate(ctx, cfg, hostCfg, nil, nil, "")
	if err != nil {
		releaseContainerSlot()
		return nil, nil, fmt.Errorf("failed to create container: %w", err)
	}
	defer m.client.ContainerRemove(context.Background(), resp.ID, container.RemoveOptions{Force: true, RemoveVolumes: true})

	if err := m.client.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		releaseContainerSlot()
		return nil, nil, err
	}
	releaseContainerSlot()

	statusCh, errCh := m.client.ContainerWait(ctx, resp.ID, container.WaitConditionNotRunning)
	var exitCode int64
	select {
	case err := <-errCh:
		if err != nil {
			return nil, nil, err
		}
	case status := <-statusCh:
		exitCode = status.StatusCode
	}

	var stdoutBuf, stderrBuf bytes.Buffer
	logReader, logErr := retryDockerCall(ctx, "ContainerLogs", m.logger, m.testName, func() (io.ReadCloser, error) {
		return m.client.ContainerLogs(ctx, resp.ID, container.LogsOptions{ShowStdout: true, ShowStderr: true})
	})
	if logErr == nil {
		stdcopy.StdCopy(&stdoutBuf, &stderrBuf, logReader)
		logReader.Close()
	}

	outFiles = make(map[string][]byte, len(outPaths))
	for _, p := range outPaths {
		data, copyErr := copyFileFromContainer(ctx, m.client, resp.ID, p)
		if copyErr != nil {
			continue
		}
		outFiles[p] = data
	}

	if exitCode != 0 {
		if stderrBuf.Len() > 0 {
			return stdoutBuf.Bytes(), outFiles, fmt.Errorf("exit code %d: %s", exitCode, stderrBuf.String())
		}
		return stdoutBuf.Bytes(), outFiles, fmt.Errorf("exit code %d", exitCode)
	}
	return stdoutBuf.Bytes(), outFiles, nil
}

// copyFileFromContainer extracts a single regular file from a stopped or
// running container as raw bytes. Returns an error if the path is missing or
// is not a regular file.
func copyFileFromContainer(ctx context.Context, client DockerAPI, containerID, path string) ([]byte, error) {
	reader, _, err := client.CopyFromContainer(ctx, containerID, path)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	tr := tar.NewReader(reader)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			return nil, fmt.Errorf("file %q not found in container", path)
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read tar entry: %w", err)
		}
		if header.Typeflag != tar.TypeReg {
			continue
		}
		data, err := io.ReadAll(tr)
		if err != nil {
			return nil, fmt.Errorf("failed to read tar content: %w", err)
		}
		return data, nil
	}
}

// Cleanup stops all containers and removes the network.
func (m *Manager) Cleanup(ctx context.Context) {
	// Copy container map under lock, then stop all in parallel
	m.containersMu.Lock()
	containers := make(map[string]string, len(m.runningContainers))
	for k, v := range m.runningContainers {
		containers[k] = v
	}
	m.runningContainers = make(map[string]string)
	m.containersMu.Unlock()

	var wg sync.WaitGroup
	for name, containerID := range containers {
		wg.Add(1)
		go func(name, containerID string) {
			defer wg.Done()
			m.logger.Info(fmt.Sprintf("[%s] Stopping container %s", m.testName, name))
			// Use background context — container removal must succeed even if cleanup context times out
			removeCtx, removeCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer removeCancel()
			if err := m.client.ContainerRemove(removeCtx, containerID, container.RemoveOptions{Force: true, RemoveVolumes: true}); err != nil {
				m.logger.Warn(fmt.Sprintf("[%s] Failed to remove container %s: %v", m.testName, name, err))
			}
		}(name, containerID)
	}
	wg.Wait()

	// Release network back to pool or remove it
	if m.networkName != "" {
		if m.pool != nil {
			m.logger.Info(fmt.Sprintf("[%s] Releasing network back to pool", m.testName))
			m.pool.Release(context.Background(), m.networkName)
		} else {
			m.logger.Info(fmt.Sprintf("[%s] Removing Docker network", m.testName))
			removeCtx, removeCancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := m.client.NetworkRemove(removeCtx, m.networkName); err != nil {
				m.logger.Warn(fmt.Sprintf("[%s] Failed to remove network: %v", m.testName, err))
			}
			removeCancel()
		}
	}
}

// ServiceArtifactFile holds an extracted file from a service container.
// Content is streamed directly to disk to avoid holding large files in memory.
type ServiceArtifactFile struct {
	Name string
	Path string // temp file on disk
	Size int64
}

// GetServiceArtifacts extracts external artifacts from all running containers.
// Files are streamed directly to outputDir on disk. This should be called BEFORE Cleanup.
func (m *Manager) GetServiceArtifacts(ctx context.Context, outputDir string) []ServiceArtifactFile {
	var files []ServiceArtifactFile
	var mu sync.Mutex
	var wg sync.WaitGroup

	for name, containerID := range m.runningContainers {
		defs, ok := m.serviceArtifactDefs[name]
		if !ok || len(defs) == 0 {
			continue
		}
		wg.Add(1)
		go func(name, containerID string, defs []model.ServiceArtifact) {
			defer wg.Done()
			for _, def := range defs {
				extracted := m.extractServiceArtifact(ctx, name, containerID, def, outputDir)
				if len(extracted) > 0 {
					mu.Lock()
					files = append(files, extracted...)
					mu.Unlock()
				}
			}
		}(name, containerID, defs)
	}

	wg.Wait()
	return files
}

// extractServiceArtifact extracts a single artifact definition from a container to disk.
func (m *Manager) extractServiceArtifact(ctx context.Context, serviceName, containerID string, def model.ServiceArtifact, outputDir string) []ServiceArtifactFile {
	// Validate artifact path — reject traversal attempts
	if strings.Contains(def.Path, "..") {
		m.logger.Warn(fmt.Sprintf("[%s] Rejecting artifact path with traversal: %s", m.testName, def.Path))
		return nil
	}

	artifactName := def.Name
	if artifactName == "" {
		artifactName = filepath.Base(def.Path)
	}

	reader, stat, err := m.client.CopyFromContainer(ctx, containerID, def.Path)
	if err != nil {
		m.logger.Warn(fmt.Sprintf("[%s] Failed to copy artifact: %v", m.testName, err))
		return nil
	}
	defer reader.Close()

	tr := tar.NewReader(reader)
	isDir := stat.Mode.IsDir()
	var files []ServiceArtifactFile

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}
		if header.Typeflag != tar.TypeReg {
			continue
		}

		// Cap artifact size at 50MB
		const maxArtifactSize = 50 * 1024 * 1024
		if header.Size > maxArtifactSize {
			m.logger.Warn(fmt.Sprintf("[%s] Skipping oversized artifact %s (%d bytes > %d limit)", m.testName, header.Name, header.Size, maxArtifactSize))
			continue
		}

		var name string
		if isDir {
			relPath := header.Name
			if idx := strings.Index(relPath, "/"); idx >= 0 {
				relPath = relPath[idx+1:]
			}
			if relPath == "" {
				continue
			}
			name = artifactName + "/" + relPath
		} else {
			name = artifactName
		}

		// Stream directly to disk
		diskPath := filepath.Join(outputDir, name)
		if err := os.MkdirAll(filepath.Dir(diskPath), 0755); err != nil {
			continue
		}
		f, err := os.Create(diskPath)
		if err != nil {
			continue
		}
		written, err := io.Copy(f, io.LimitReader(tr, maxArtifactSize))
		f.Close()
		if err != nil {
			os.Remove(diskPath)
			continue
		}

		files = append(files, ServiceArtifactFile{
			Name: name,
			Path: diskPath,
			Size: written,
		})
	}

	return files
}

// Close is an alias for Cleanup for interface compatibility.
func (m *Manager) Close() error {
	m.Cleanup(context.Background())
	return nil
}

// ServiceLog holds log content for a service container.
type ServiceLog struct {
	ServiceName string
	Content     []byte
}

// GetServiceLogs collects logs from all running containers.
// This should be called BEFORE Cleanup.
func (m *Manager) GetServiceLogs(ctx context.Context) []ServiceLog {
	var logs []ServiceLog
	var mu sync.Mutex
	var wg sync.WaitGroup

	for name, containerID := range m.runningContainers {
		wg.Add(1)
		go func(name, containerID string) {
			defer wg.Done()

			logReader, err := retryDockerCall(ctx, "ContainerLogs", m.logger, m.testName, func() (io.ReadCloser, error) {
				return m.client.ContainerLogs(ctx, containerID, container.LogsOptions{ShowStdout: true, ShowStderr: true})
			})
			if err != nil {
				m.logger.Warn(fmt.Sprintf("[%s] Failed to collect logs: %v", m.testName, err))
				return
			}
			defer logReader.Close()

			var buf bytes.Buffer
			stdcopy.StdCopy(&buf, &buf, logReader)

			if buf.Len() > 0 {
				mu.Lock()
				logs = append(logs, ServiceLog{
					ServiceName: name,
					Content:     buf.Bytes(),
				})
				mu.Unlock()
			}
		}(name, containerID)
	}

	wg.Wait()
	return logs
}

// ExecResult holds the result of executing a command in a container.
type ExecResult struct {
	ExitCode int
	Stdout   []byte
	Stderr   []byte
}

// ExecInContainer executes a command inside a running service container.
// The command is passed to sh -c for execution.
func (m *Manager) ExecInContainer(ctx context.Context, serviceName, command, workingDir string) (*ExecResult, error) {
	m.containersMu.Lock()
	containerID, ok := m.runningContainers[serviceName]
	m.containersMu.Unlock()

	if !ok {
		return nil, fmt.Errorf("service %s not found in running containers", serviceName)
	}

	m.logger.Info(fmt.Sprintf("[%s] Executing command in container: %s", m.testName, command))

	// Create exec
	execCfg := container.ExecOptions{
		Cmd:          []string{"sh", "-c", command},
		AttachStdout: true,
		AttachStderr: true,
	}
	if workingDir != "" {
		execCfg.WorkingDir = workingDir
	}

	execResp, err := retryDockerCall(ctx, "ContainerExecCreate", m.logger, m.testName, func() (container.ExecCreateResponse, error) {
		return m.client.ContainerExecCreate(ctx, containerID, execCfg)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create exec: %w", err)
	}

	attachResp, err := m.client.ContainerExecAttach(ctx, execResp.ID, container.ExecAttachOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to attach exec: %w", err)
	}

	// Read stdout/stderr in a goroutine. Close the attach connection when context
	// is cancelled so StdCopy unblocks — otherwise a long-running command keeps
	// the goroutine stuck until the process finishes, ignoring the timeout.
	var stdout, stderr bytes.Buffer
	copyDone := make(chan struct{})
	go func() {
		defer close(copyDone)
		stdcopy.StdCopy(&stdout, &stderr, attachResp.Reader)
	}()

	select {
	case <-copyDone:
		// Command finished normally
		attachResp.Close()
	case <-ctx.Done():
		// Timeout or cancellation — force-close the connection to unblock StdCopy
		attachResp.Close()
		<-copyDone // wait for goroutine to finish
		return nil, fmt.Errorf("exec timed out: %w", ctx.Err())
	}

	// Get exit code
	// Use a fresh context — the exec already finished, we just need the exit code.
	inspectCtx, inspectCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer inspectCancel()
	inspectResp, err := retryDockerCall(inspectCtx, "ContainerExecInspect", m.logger, m.testName, func() (container.ExecInspect, error) {
		return m.client.ContainerExecInspect(inspectCtx, execResp.ID)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to inspect exec: %w", err)
	}

	m.logger.Info(fmt.Sprintf("[%s] Command completed with exit code %d", m.testName, inspectResp.ExitCode))

	return &ExecResult{
		ExitCode: inspectResp.ExitCode,
		Stdout:   stdout.Bytes(),
		Stderr:   stderr.Bytes(),
	}, nil
}
