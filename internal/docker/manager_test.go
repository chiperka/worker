package docker

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"chiperka-worker/internal/model"
)

// --------------------------------------------------------------------------
// NewManagerWithClient tests
// --------------------------------------------------------------------------

func TestNewManagerWithClient_Success(t *testing.T) {
	mock := &MockDockerAPI{}
	m, err := NewManagerWithClient(mock, nil, "test-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m == nil {
		t.Fatal("expected non-nil manager")
	}
	if m.client != mock {
		t.Error("expected mock client to be stored")
	}
	if !strings.HasPrefix(m.networkName, "chiperka-") {
		t.Errorf("expected network name to start with chiperka-, got %s", m.networkName)
	}
	if m.testName != "test-1" {
		t.Errorf("expected testName test-1, got %s", m.testName)
	}
	// Cleanup
	m.Cleanup(context.Background())
}

func TestNewManagerWithClient_PingFailure(t *testing.T) {
	mock := &MockDockerAPI{
		PingFn: func(ctx context.Context) (types.Ping, error) {
			return types.Ping{}, fmt.Errorf("connection refused")
		},
	}
	_, err := NewManagerWithClient(mock, nil, "test-1")
	if err == nil {
		t.Fatal("expected error when ping fails")
	}
	if !strings.Contains(err.Error(), "docker not available") {
		t.Errorf("expected 'docker not available', got %v", err)
	}
}

func TestNewManagerWithClient_NetworkCreateFailure(t *testing.T) {
	mock := &MockDockerAPI{
		NetworkCreateFn: func(ctx context.Context, name string, options network.CreateOptions) (network.CreateResponse, error) {
			return network.CreateResponse{}, fmt.Errorf("network limit reached")
		},
	}
	_, err := NewManagerWithClient(mock, nil, "test-1")
	if err == nil {
		t.Fatal("expected error when network creation fails")
	}
}

func TestNewManagerWithClient_NilLoggerUsesNoop(t *testing.T) {
	mock := &MockDockerAPI{}
	m, err := NewManagerWithClient(mock, nil, "test-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should not panic
	m.logger.Info("test")
	m.logger.Warn("test")
	m.logger.Fail("test")
	m.Cleanup(context.Background())
}

// --------------------------------------------------------------------------
// Cleanup tests
// --------------------------------------------------------------------------

func TestCleanup_RemovesContainersAndNetwork(t *testing.T) {
	var mu sync.Mutex
	var removedContainers []string
	var removedNetwork string
	mock := &MockDockerAPI{
		ContainerRemoveFn: func(ctx context.Context, containerID string, options container.RemoveOptions) error {
			mu.Lock()
			removedContainers = append(removedContainers, containerID)
			mu.Unlock()
			return nil
		},
		NetworkRemoveFn: func(ctx context.Context, networkID string) error {
			removedNetwork = networkID
			return nil
		},
	}

	m, err := NewManagerWithClient(mock, nil, "cleanup-test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Simulate running containers
	m.containersMu.Lock()
	m.runningContainers["svc1"] = "ctr-111"
	m.runningContainers["svc2"] = "ctr-222"
	m.containersMu.Unlock()

	m.Cleanup(context.Background())

	if len(removedContainers) != 2 {
		t.Errorf("expected 2 containers removed, got %d", len(removedContainers))
	}
	if removedNetwork == "" {
		t.Error("expected network to be removed")
	}
}


// --------------------------------------------------------------------------
// StartServices tests (with mock)
// --------------------------------------------------------------------------

func TestStartServices_EmptyServices(t *testing.T) {
	mock := &MockDockerAPI{}
	m, err := NewManagerWithClient(mock, nil, "empty-svc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer m.Cleanup(context.Background())

	results, err := m.StartServices(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}

func TestStartServices_SingleServiceNoHealthcheck(t *testing.T) {
	var createdContainers []string
	mock := &MockDockerAPI{
		// Image already exists locally
		ImageInspectFn: func(ctx context.Context, imageID string) (types.ImageInspect, []byte, error) {
			return types.ImageInspect{}, nil, nil
		},
		ContainerCreateFn: func(ctx context.Context, config *container.Config, hostConfig *container.HostConfig, networkingConfig *network.NetworkingConfig, platform *ocispec.Platform, containerName string) (container.CreateResponse, error) {
			createdContainers = append(createdContainers, containerName)
			return container.CreateResponse{ID: "ctr-" + containerName}, nil
		},
		ContainerInspectFn: func(ctx context.Context, containerID string) (types.ContainerJSON, error) {
			return types.ContainerJSON{
				ContainerJSONBase: &types.ContainerJSONBase{
					State: &types.ContainerState{Running: true},
				},
			}, nil
		},
	}

	m, err := NewManagerWithClient(mock, nil, "single-svc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer m.Cleanup(context.Background())

	services := []model.Service{
		{Name: "redis", Image: "redis:7"},
	}
	results, err := m.StartServices(context.Background(), services)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Name != "redis" {
		t.Errorf("expected redis, got %s", results[0].Name)
	}
}

func TestStartServices_ContainerCreateFails(t *testing.T) {
	mock := &MockDockerAPI{
		ImageInspectFn: func(ctx context.Context, imageID string) (types.ImageInspect, []byte, error) {
			return types.ImageInspect{}, nil, nil
		},
		ContainerCreateFn: func(ctx context.Context, config *container.Config, hostConfig *container.HostConfig, networkingConfig *network.NetworkingConfig, platform *ocispec.Platform, containerName string) (container.CreateResponse, error) {
			return container.CreateResponse{}, fmt.Errorf("no space left on device")
		},
	}

	m, err := NewManagerWithClient(mock, nil, "fail-create")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer m.Cleanup(context.Background())

	services := []model.Service{
		{Name: "db", Image: "postgres:15"},
	}
	_, err = m.StartServices(context.Background(), services)
	if err == nil {
		t.Fatal("expected error when container creation fails")
	}
}

// --------------------------------------------------------------------------
// Helper tests
// --------------------------------------------------------------------------

func TestRetryDockerCall_SuccessOnFirstTry(t *testing.T) {
	result, err := retryDockerCall(context.Background(), "test", nil, "t", func() (string, error) {
		return "ok", nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "ok" {
		t.Errorf("expected 'ok', got %s", result)
	}
}

func TestRetryDockerCall_RetriesOnTransientError(t *testing.T) {
	calls := 0
	result, err := retryDockerCall(context.Background(), "test", nil, "t", func() (string, error) {
		calls++
		if calls < 3 {
			return "", fmt.Errorf("connection reset")
		}
		return "ok", nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "ok" {
		t.Errorf("expected 'ok', got %s", result)
	}
	if calls != 3 {
		t.Errorf("expected 3 calls, got %d", calls)
	}
}

func TestRetryDockerCall_FailsFastOnNonTransient(t *testing.T) {
	calls := 0
	_, err := retryDockerCall(context.Background(), "test", nil, "t", func() (string, error) {
		calls++
		return "", fmt.Errorf("container not found")
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if calls != 1 {
		t.Errorf("expected 1 call (non-transient), got %d", calls)
	}
}

func TestIsTransientDockerError(t *testing.T) {
	tests := []struct {
		err    error
		expect bool
	}{
		{nil, false},
		{fmt.Errorf("connection reset"), true},
		{fmt.Errorf("i/o timeout"), true},
		{fmt.Errorf("EOF"), true},
		{fmt.Errorf("broken pipe"), true},
		{fmt.Errorf("TLS handshake timeout"), true},
		{fmt.Errorf("container not found"), false},
		{fmt.Errorf("image not found"), false},
		{fmt.Errorf("permission denied"), false},
	}

	for _, tt := range tests {
		got := isTransientDockerError(tt.err)
		if got != tt.expect {
			t.Errorf("isTransientDockerError(%v) = %v, want %v", tt.err, got, tt.expect)
		}
	}
}

func TestDefaultClient_ReturnsNonNil(t *testing.T) {
	// On a machine without Docker, this might return error — that's OK
	client, err := DefaultClient()
	if err != nil {
		t.Skipf("Docker not available: %v", err)
	}
	if client == nil {
		t.Error("expected non-nil client")
	}
}

// --------------------------------------------------------------------------
// createNetworkWithRetry tests
// --------------------------------------------------------------------------

func TestCreateNetworkWithRetry_NonOverlappingErrorCleansUpAndRetries(t *testing.T) {
	calls := 0
	mock := &MockDockerAPI{
		NetworkCreateFn: func(ctx context.Context, name string, options network.CreateOptions) (network.CreateResponse, error) {
			calls++
			if calls == 1 {
				return network.CreateResponse{}, fmt.Errorf("could not find an available, non-overlapping IPv4 address pool")
			}
			return network.CreateResponse{ID: "net-retry-ok"}, nil
		},
	}
	// createNetworkWithRetry calls CleanupStaleNetworks which uses defaultDockerClient,
	// but we only care that the retry succeeded.
	id, err := createNetworkWithRetry(mock, "test-net", noopLogger{})
	if err != nil {
		t.Fatalf("expected success after retry, got: %v", err)
	}
	if id != "net-retry-ok" {
		t.Errorf("expected net-retry-ok, got %s", id)
	}
	if calls != 2 {
		t.Errorf("expected 2 NetworkCreate calls, got %d", calls)
	}
}

func TestCreateNetworkWithRetry_NonOverlappingErrorFailsOnNonOverlapError(t *testing.T) {
	mock := &MockDockerAPI{
		NetworkCreateFn: func(ctx context.Context, name string, options network.CreateOptions) (network.CreateResponse, error) {
			return network.CreateResponse{}, fmt.Errorf("permission denied")
		},
	}
	_, err := createNetworkWithRetry(mock, "test-net", noopLogger{})
	if err == nil {
		t.Fatal("expected error for non-overlapping unrelated error")
	}
	if !strings.Contains(err.Error(), "permission denied") {
		t.Errorf("expected 'permission denied' in error, got: %v", err)
	}
}

// --------------------------------------------------------------------------
// acquireContainerSlot / releaseContainerSlot tests
// --------------------------------------------------------------------------

func TestContainerSlot_UnlimitedByDefault(t *testing.T) {
	// Reset to default (nil)
	containerSem = nil
	err := acquireContainerSlot(context.Background())
	if err != nil {
		t.Fatalf("expected nil error when semaphore is nil, got: %v", err)
	}
	// releaseContainerSlot should be safe even when nil
	releaseContainerSlot()
}

func TestContainerSlot_LimitedSemaphore(t *testing.T) {
	SetMaxConcurrentContainers(2)
	defer func() { containerSem = nil }()

	// Acquire two slots - should succeed immediately
	if err := acquireContainerSlot(context.Background()); err != nil {
		t.Fatalf("slot 1: %v", err)
	}
	if err := acquireContainerSlot(context.Background()); err != nil {
		t.Fatalf("slot 2: %v", err)
	}

	// Third acquire should block, so use a cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := acquireContainerSlot(ctx)
	if err == nil {
		t.Fatal("expected error when semaphore is full and context is cancelled")
	}

	// Release one slot and acquire again
	releaseContainerSlot()
	if err := acquireContainerSlot(context.Background()); err != nil {
		t.Fatalf("slot after release: %v", err)
	}

	// Clean up remaining slots
	releaseContainerSlot()
	releaseContainerSlot()
}


// --------------------------------------------------------------------------
// retryDockerCallNoResult tests
// --------------------------------------------------------------------------

func TestRetryDockerCallNoResult_SuccessOnFirstTry(t *testing.T) {
	err := retryDockerCallNoResult(context.Background(), "test", nil, "t", func() error {
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRetryDockerCallNoResult_RetriesOnTransientError(t *testing.T) {
	calls := 0
	err := retryDockerCallNoResult(context.Background(), "test", nil, "t", func() error {
		calls++
		if calls < 2 {
			return fmt.Errorf("connection reset")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 2 {
		t.Errorf("expected 2 calls, got %d", calls)
	}
}

func TestRetryDockerCallNoResult_FailsFastOnNonTransient(t *testing.T) {
	calls := 0
	err := retryDockerCallNoResult(context.Background(), "test", nil, "t", func() error {
		calls++
		return fmt.Errorf("no such container")
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if calls != 1 {
		t.Errorf("expected 1 call (non-transient), got %d", calls)
	}
}

// --------------------------------------------------------------------------
// Cleanup with pool tests
// --------------------------------------------------------------------------

func TestCleanup_WithPool_ReleasesNetworkToPool(t *testing.T) {
	var networkRemoved bool
	var releasedNetwork string
	mock := &MockDockerAPI{
		ContainerRemoveFn: func(ctx context.Context, containerID string, options container.RemoveOptions) error {
			return nil
		},
		NetworkRemoveFn: func(ctx context.Context, networkID string) error {
			networkRemoved = true
			return nil
		},
	}

	m, err := NewManagerWithClient(mock, nil, "pool-test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Create a fake pool and set it on the manager
	pool := &NetworkPool{
		available: make([]string, 0),
		inUse:     map[string]bool{m.networkName: true},
		poolSize:  2,
	}
	m.pool = pool

	// Track what pool.Release does by replacing the default client temporarily
	// The pool's Release calls disconnectAllFromNetwork which uses defaultDockerClient.
	// We override NetworkInspect to avoid issues.
	savedClient := defaultDockerClient
	defaultDockerClient = &MockDockerAPI{
		NetworkInspectFn: func(ctx context.Context, networkID string, options network.InspectOptions) (network.Inspect, error) {
			return network.Inspect{}, nil
		},
	}
	defer func() { defaultDockerClient = savedClient }()

	m.containersMu.Lock()
	m.runningContainers["svc1"] = "ctr-111"
	m.containersMu.Unlock()

	m.Cleanup(context.Background())

	// Network should NOT have been removed (it was released to pool)
	if networkRemoved {
		t.Error("expected network NOT to be removed when pool is present")
	}

	// Pool should have the network back as available
	avail, inUse := pool.Size()
	if avail != 1 {
		t.Errorf("expected 1 available in pool, got %d", avail)
	}
	if inUse != 0 {
		t.Errorf("expected 0 in-use in pool, got %d", inUse)
	}
	_ = releasedNetwork
}

// --------------------------------------------------------------------------
// startAndHealthcheck tests
// --------------------------------------------------------------------------

func TestStartAndHealthcheck_ContainerStartFailure(t *testing.T) {
	mock := &MockDockerAPI{
		ContainerCreateFn: func(ctx context.Context, config *container.Config, hostConfig *container.HostConfig, networkingConfig *network.NetworkingConfig, platform *ocispec.Platform, containerName string) (container.CreateResponse, error) {
			return container.CreateResponse{ID: "ctr-will-fail"}, nil
		},
		ContainerStartFn: func(ctx context.Context, containerID string, options container.StartOptions) error {
			return fmt.Errorf("OCI runtime create failed")
		},
	}

	// Reset container semaphore to avoid interference
	containerSem = nil

	m, err := NewManagerWithClient(mock, nil, "start-fail")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer m.Cleanup(context.Background())

	svc := model.Service{Name: "db", Image: "postgres:15"}
	containerDur, healthDur, startErr := m.startAndHealthcheck(context.Background(), svc, "postgres:15")
	if startErr == nil {
		t.Fatal("expected error when container start fails")
	}
	if !strings.Contains(startErr.Error(), "failed to start container") {
		t.Errorf("expected 'failed to start container' in error, got: %v", startErr)
	}
	if containerDur == 0 {
		t.Error("expected non-zero container start duration even on failure")
	}
	if healthDur != 0 {
		t.Error("expected zero health check duration when start fails")
	}
}

func TestStartAndHealthcheck_ContainerCreateFailure(t *testing.T) {
	mock := &MockDockerAPI{
		ContainerCreateFn: func(ctx context.Context, config *container.Config, hostConfig *container.HostConfig, networkingConfig *network.NetworkingConfig, platform *ocispec.Platform, containerName string) (container.CreateResponse, error) {
			return container.CreateResponse{}, fmt.Errorf("no space left on device")
		},
	}

	containerSem = nil

	m, err := NewManagerWithClient(mock, nil, "create-fail")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer m.Cleanup(context.Background())

	svc := model.Service{Name: "db", Image: "postgres:15"}
	_, _, startErr := m.startAndHealthcheck(context.Background(), svc, "postgres:15")
	if startErr == nil {
		t.Fatal("expected error when container create fails")
	}
	if !strings.Contains(startErr.Error(), "failed to create container") {
		t.Errorf("expected 'failed to create container' in error, got: %v", startErr)
	}
}

// --------------------------------------------------------------------------
// buildHealthConfig tests
// --------------------------------------------------------------------------

func TestBuildHealthConfig_NilWhenTestEmpty(t *testing.T) {
	hc := &model.HealthCheck{Test: ""}
	cfg := buildHealthConfig(hc)
	if cfg != nil {
		t.Error("expected nil HealthConfig when Test is empty")
	}
}

func TestBuildHealthConfig_DefaultValues(t *testing.T) {
	hc := &model.HealthCheck{
		Test: "curl -f http://localhost/health",
	}
	cfg := buildHealthConfig(hc)
	if cfg == nil {
		t.Fatal("expected non-nil HealthConfig")
	}
	if len(cfg.Test) != 2 || cfg.Test[0] != "CMD-SHELL" || cfg.Test[1] != "curl -f http://localhost/health" {
		t.Errorf("unexpected Test: %v", cfg.Test)
	}
	if cfg.Interval != time.Second {
		t.Errorf("expected default Interval 1s, got %v", cfg.Interval)
	}
	if cfg.Timeout != 3*time.Second {
		t.Errorf("expected default Timeout 3s, got %v", cfg.Timeout)
	}
	if cfg.Retries != 30 {
		t.Errorf("expected default Retries 30, got %d", cfg.Retries)
	}
	if cfg.StartPeriod != 0 {
		t.Errorf("expected default StartPeriod 0, got %v", cfg.StartPeriod)
	}
}

func TestBuildHealthConfig_CustomValues(t *testing.T) {
	hc := &model.HealthCheck{
		Test:          "pg_isready",
		Interval:      "5s",
		Timeout:       "10s",
		Retries:       5,
		StartPeriod:   "2s",
		StartInterval: "500ms",
	}
	cfg := buildHealthConfig(hc)
	if cfg == nil {
		t.Fatal("expected non-nil HealthConfig")
	}
	if cfg.Interval != 5*time.Second {
		t.Errorf("expected Interval 5s, got %v", cfg.Interval)
	}
	if cfg.Timeout != 10*time.Second {
		t.Errorf("expected Timeout 10s, got %v", cfg.Timeout)
	}
	if cfg.Retries != 5 {
		t.Errorf("expected Retries 5, got %d", cfg.Retries)
	}
	if cfg.StartPeriod != 2*time.Second {
		t.Errorf("expected StartPeriod 2s, got %v", cfg.StartPeriod)
	}
	if cfg.StartInterval != 500*time.Millisecond {
		t.Errorf("expected StartInterval 500ms, got %v", cfg.StartInterval)
	}
}

// --------------------------------------------------------------------------
// parseDurationOrDefault tests
// --------------------------------------------------------------------------

func TestParseDurationOrDefault_EmptyString(t *testing.T) {
	d := parseDurationOrDefault("", 5*time.Second)
	if d != 5*time.Second {
		t.Errorf("expected 5s default, got %v", d)
	}
}

func TestParseDurationOrDefault_ValidDurations(t *testing.T) {
	tests := []struct {
		input    string
		expected time.Duration
	}{
		{"1s", time.Second},
		{"500ms", 500 * time.Millisecond},
		{"2m", 2 * time.Minute},
		{"1h", time.Hour},
		{"100us", 100 * time.Microsecond},
		{"0s", 0},
	}
	for _, tt := range tests {
		got := parseDurationOrDefault(tt.input, 99*time.Second)
		if got != tt.expected {
			t.Errorf("parseDurationOrDefault(%q) = %v, want %v", tt.input, got, tt.expected)
		}
	}
}

func TestParseDurationOrDefault_InvalidStrings(t *testing.T) {
	tests := []string{
		"abc",
		"5",
		"seconds",
		"1x",
	}
	for _, input := range tests {
		got := parseDurationOrDefault(input, 42*time.Second)
		if got != 42*time.Second {
			t.Errorf("parseDurationOrDefault(%q) = %v, want default 42s", input, got)
		}
	}
}

// --------------------------------------------------------------------------
// Compile-time interface check
// --------------------------------------------------------------------------

var _ DockerAPI = (*MockDockerAPI)(nil)
