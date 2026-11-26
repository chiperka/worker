package docker

import (
	"context"
	"sort"
	"sync"
	"testing"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
)

func TestCleanupRemovesOnlyTestResources(t *testing.T) {
	var mu sync.Mutex
	var removed []string
	saved := defaultDockerClient
	defer func() { defaultDockerClient = saved }()
	defaultDockerClient = &MockDockerAPI{
		ContainerListFn: func(context.Context, container.ListOptions) ([]types.Container, error) {
			// Docker's name filter matches substrings, so all of these come back.
			return []types.Container{
				{ID: "aaa", Names: []string{"/chiperka-0123456789abcdef-api"}},
				{ID: "bbb", Names: []string{"/chiperka-worker"}},
				{ID: "ccc", Names: []string{"/geobase-chiperka-worker-1"}},
				{ID: "ddd", Names: []string{"/chiperka-dev-ci-1234"}},
			}, nil
		},
		ContainerRemoveFn: func(_ context.Context, id string, _ container.RemoveOptions) error {
			mu.Lock()
			removed = append(removed, id)
			mu.Unlock()
			return nil
		},
		NetworkListFn: func(context.Context, network.ListOptions) ([]network.Summary, error) {
			return []network.Summary{{ID: "n1", Name: "chiperka-0123456789abcdef"}, {ID: "n2", Name: "chiperka-local_default"}}, nil
		},
		NetworkRemoveFn: func(_ context.Context, id string) error {
			mu.Lock()
			removed = append(removed, id)
			mu.Unlock()
			return nil
		},
	}
	CleanupStaleContainers()
	CleanupStaleNetworks()
	sort.Strings(removed)
	if len(removed) != 2 || removed[0] != "aaa" || removed[1] != "n1" {
		t.Fatalf("removed %v, want only the test container aaa and network n1", removed)
	}
}

func TestCleanupNeverRemovesItself(t *testing.T) {
	c := types.Container{ID: "4f2a9c1b7e3d0123456789", Names: []string{"/chiperka-0123456789abcdef-x"}}
	if isStaleTestContainer(c, "4f2a9c1b7e3d") {
		t.Fatal("the worker's own container must be kept")
	}
	if !isStaleTestContainer(c, "other-host") {
		t.Fatal("a test container must be removed")
	}
}
