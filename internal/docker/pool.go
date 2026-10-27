// Package docker provides Docker container management for test services.
package docker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/docker/docker/api/types/network"
)

// NetworkPool manages a pool of reusable Docker networks.
type NetworkPool struct {
	mu        sync.Mutex
	available []string // network names available for use
	inUse     map[string]bool
	poolSize  int
}

// NewNetworkPool creates a new network pool and pre-creates networks.
func NewNetworkPool(size int) *NetworkPool {
	pool := &NetworkPool{
		available: make([]string, 0, size),
		inUse:     make(map[string]bool),
		poolSize:  size,
	}

	// Pre-create networks in parallel
	var wg sync.WaitGroup
	var mu sync.Mutex
	networks := make([]string, 0, size)

	for i := 0; i < size; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			name, err := createPoolNetwork()
			if err == nil {
				mu.Lock()
				networks = append(networks, name)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	pool.available = networks
	return pool
}

// createPoolNetwork creates a single network for the pool.
func createPoolNetwork() (string, error) {
	randomBytes := make([]byte, 8)
	if _, err := rand.Read(randomBytes); err != nil {
		return "", err
	}
	networkName := fmt.Sprintf("chiperka-pool-%s", hex.EncodeToString(randomBytes))

	resp, err := defaultDockerClient.NetworkCreate(context.Background(), networkName, network.CreateOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to create network: %w", err)
	}
	_ = resp
	return networkName, nil
}

// Acquire gets a network from the pool, creating one if none available.
func (p *NetworkPool) Acquire() (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Try to get from available pool
	if len(p.available) > 0 {
		name := p.available[len(p.available)-1]
		p.available = p.available[:len(p.available)-1]
		p.inUse[name] = true
		return name, nil
	}

	// Create a new network if pool is empty
	name, err := createPoolNetwork()
	if err != nil {
		return "", err
	}
	p.inUse[name] = true
	return name, nil
}

// Release returns a network to the pool after cleaning up containers.
func (p *NetworkPool) Release(ctx context.Context, networkName string) {
	// First disconnect any remaining containers from the network
	disconnectAllFromNetwork(ctx, networkName)

	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.inUse[networkName] {
		// Network not from pool, just remove it
		defaultDockerClient.NetworkRemove(ctx, networkName)
		return
	}

	delete(p.inUse, networkName)
	p.available = append(p.available, networkName)
}

// disconnectAllFromNetwork disconnects all containers from a network.
func disconnectAllFromNetwork(ctx context.Context, networkName string) {
	// Get list of containers connected to the network
	inspect, err := defaultDockerClient.NetworkInspect(ctx, networkName, network.InspectOptions{})
	if err != nil {
		return
	}

	for _, endpoint := range inspect.Containers {
		if endpoint.Name != "" {
			defaultDockerClient.NetworkDisconnect(ctx, networkName, endpoint.Name, true)
		}
	}
}

// Close cleans up all networks in the pool.
func (p *NetworkPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	ctx := context.Background()

	// Remove all available networks
	for _, name := range p.available {
		defaultDockerClient.NetworkRemove(ctx, name)
	}
	p.available = nil

	// Remove all in-use networks (shouldn't happen normally)
	for name := range p.inUse {
		defaultDockerClient.NetworkRemove(ctx, name)
	}
	p.inUse = make(map[string]bool)
}

// Size returns current pool status.
func (p *NetworkPool) Size() (available, inUse int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.available), len(p.inUse)
}
