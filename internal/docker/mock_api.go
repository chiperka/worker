package docker

import (
	"context"
	"io"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	imagetypes "github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// MockDockerAPI is a test double for DockerAPI.
// Set the function fields to control behavior per test.
// Unset functions return zero values and nil errors.
type MockDockerAPI struct {
	PingFn              func(ctx context.Context) (types.Ping, error)
	NetworkCreateFn     func(ctx context.Context, name string, options network.CreateOptions) (network.CreateResponse, error)
	NetworkRemoveFn     func(ctx context.Context, networkID string) error
	NetworkInspectFn    func(ctx context.Context, networkID string, options network.InspectOptions) (network.Inspect, error)
	NetworkDisconnectFn func(ctx context.Context, networkID, containerID string, force bool) error
	NetworkListFn       func(ctx context.Context, options network.ListOptions) ([]network.Summary, error)
	ImageInspectFn      func(ctx context.Context, imageID string) (types.ImageInspect, []byte, error)
	ImagePullFn         func(ctx context.Context, refStr string, options imagetypes.PullOptions) (io.ReadCloser, error)
	ContainerCreateFn   func(ctx context.Context, config *container.Config, hostConfig *container.HostConfig, networkingConfig *network.NetworkingConfig, platform *ocispec.Platform, containerName string) (container.CreateResponse, error)
	ContainerStartFn    func(ctx context.Context, containerID string, options container.StartOptions) error
	ContainerRemoveFn   func(ctx context.Context, containerID string, options container.RemoveOptions) error
	ContainerInspectFn  func(ctx context.Context, containerID string) (types.ContainerJSON, error)
	ContainerWaitFn     func(ctx context.Context, containerID string, condition container.WaitCondition) (<-chan container.WaitResponse, <-chan error)
	ContainerLogsFn     func(ctx context.Context, containerID string, options container.LogsOptions) (io.ReadCloser, error)
	CopyFromContainerFn func(ctx context.Context, containerID, srcPath string) (io.ReadCloser, container.PathStat, error)
	ExecCreateFn        func(ctx context.Context, container string, config container.ExecOptions) (types.IDResponse, error)
	ExecAttachFn        func(ctx context.Context, execID string, config container.ExecAttachOptions) (types.HijackedResponse, error)
	ExecInspectFn       func(ctx context.Context, execID string) (container.ExecInspect, error)
}

func (m *MockDockerAPI) Ping(ctx context.Context) (types.Ping, error) {
	if m.PingFn != nil {
		return m.PingFn(ctx)
	}
	return types.Ping{}, nil
}

func (m *MockDockerAPI) NetworkCreate(ctx context.Context, name string, options network.CreateOptions) (network.CreateResponse, error) {
	if m.NetworkCreateFn != nil {
		return m.NetworkCreateFn(ctx, name, options)
	}
	return network.CreateResponse{ID: "net-" + name}, nil
}

func (m *MockDockerAPI) NetworkRemove(ctx context.Context, networkID string) error {
	if m.NetworkRemoveFn != nil {
		return m.NetworkRemoveFn(ctx, networkID)
	}
	return nil
}

func (m *MockDockerAPI) NetworkInspect(ctx context.Context, networkID string, options network.InspectOptions) (network.Inspect, error) {
	if m.NetworkInspectFn != nil {
		return m.NetworkInspectFn(ctx, networkID, options)
	}
	return network.Inspect{}, nil
}

func (m *MockDockerAPI) NetworkDisconnect(ctx context.Context, networkID, containerID string, force bool) error {
	if m.NetworkDisconnectFn != nil {
		return m.NetworkDisconnectFn(ctx, networkID, containerID, force)
	}
	return nil
}

func (m *MockDockerAPI) NetworkList(ctx context.Context, options network.ListOptions) ([]network.Summary, error) {
	if m.NetworkListFn != nil {
		return m.NetworkListFn(ctx, options)
	}
	return nil, nil
}

func (m *MockDockerAPI) ImageInspectWithRaw(ctx context.Context, imageID string) (types.ImageInspect, []byte, error) {
	if m.ImageInspectFn != nil {
		return m.ImageInspectFn(ctx, imageID)
	}
	return types.ImageInspect{}, nil, nil
}

func (m *MockDockerAPI) ImagePull(ctx context.Context, refStr string, options imagetypes.PullOptions) (io.ReadCloser, error) {
	if m.ImagePullFn != nil {
		return m.ImagePullFn(ctx, refStr, options)
	}
	return io.NopCloser(io.LimitReader(nil, 0)), nil
}

func (m *MockDockerAPI) ContainerCreate(ctx context.Context, config *container.Config, hostConfig *container.HostConfig, networkingConfig *network.NetworkingConfig, platform *ocispec.Platform, containerName string) (container.CreateResponse, error) {
	if m.ContainerCreateFn != nil {
		return m.ContainerCreateFn(ctx, config, hostConfig, networkingConfig, platform, containerName)
	}
	return container.CreateResponse{ID: "ctr-" + containerName}, nil
}

func (m *MockDockerAPI) ContainerStart(ctx context.Context, containerID string, options container.StartOptions) error {
	if m.ContainerStartFn != nil {
		return m.ContainerStartFn(ctx, containerID, options)
	}
	return nil
}

func (m *MockDockerAPI) ContainerRemove(ctx context.Context, containerID string, options container.RemoveOptions) error {
	if m.ContainerRemoveFn != nil {
		return m.ContainerRemoveFn(ctx, containerID, options)
	}
	return nil
}

func (m *MockDockerAPI) ContainerInspect(ctx context.Context, containerID string) (types.ContainerJSON, error) {
	if m.ContainerInspectFn != nil {
		return m.ContainerInspectFn(ctx, containerID)
	}
	return types.ContainerJSON{}, nil
}

func (m *MockDockerAPI) ContainerWait(ctx context.Context, containerID string, condition container.WaitCondition) (<-chan container.WaitResponse, <-chan error) {
	if m.ContainerWaitFn != nil {
		return m.ContainerWaitFn(ctx, containerID, condition)
	}
	ch := make(chan container.WaitResponse, 1)
	ch <- container.WaitResponse{StatusCode: 0}
	return ch, make(chan error)
}

func (m *MockDockerAPI) ContainerLogs(ctx context.Context, containerID string, options container.LogsOptions) (io.ReadCloser, error) {
	if m.ContainerLogsFn != nil {
		return m.ContainerLogsFn(ctx, containerID, options)
	}
	return io.NopCloser(io.LimitReader(nil, 0)), nil
}

func (m *MockDockerAPI) CopyFromContainer(ctx context.Context, containerID, srcPath string) (io.ReadCloser, container.PathStat, error) {
	if m.CopyFromContainerFn != nil {
		return m.CopyFromContainerFn(ctx, containerID, srcPath)
	}
	return io.NopCloser(io.LimitReader(nil, 0)), container.PathStat{}, nil
}

func (m *MockDockerAPI) ContainerExecCreate(ctx context.Context, ctr string, config container.ExecOptions) (types.IDResponse, error) {
	if m.ExecCreateFn != nil {
		return m.ExecCreateFn(ctx, ctr, config)
	}
	return types.IDResponse{ID: "exec-1"}, nil
}

func (m *MockDockerAPI) ContainerExecAttach(ctx context.Context, execID string, config container.ExecAttachOptions) (types.HijackedResponse, error) {
	if m.ExecAttachFn != nil {
		return m.ExecAttachFn(ctx, execID, config)
	}
	return types.HijackedResponse{}, nil
}

func (m *MockDockerAPI) ContainerExecInspect(ctx context.Context, execID string) (container.ExecInspect, error) {
	if m.ExecInspectFn != nil {
		return m.ExecInspectFn(ctx, execID)
	}
	return container.ExecInspect{ExitCode: 0}, nil
}

func (m *MockDockerAPI) ContainerList(ctx context.Context, options container.ListOptions) ([]types.Container, error) {
	return nil, nil
}

// Compile-time check that MockDockerAPI implements DockerAPI.
var _ DockerAPI = (*MockDockerAPI)(nil)
