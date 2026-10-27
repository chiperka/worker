package workerloop

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"chiperka-worker/internal/apiclient"
	"chiperka-worker/internal/docker"
)

// Config holds all worker configuration parsed from flags/env.
type Config struct {
	APIURL        string
	Token         string
	PollInterval  time.Duration
	Concurrency   int // max parallel test executions (hard limit)
	Capacity      int // max concurrent weight (0 = auto: CPU count * 2)
	MaxContainers int // max concurrent Docker containers (0 = unlimited)
	TestTimeout   int
	WorkerID      string
	DrainTimeout  time.Duration
	Version       string
}

// Run is the main worker lifecycle: register → claim → execute → shutdown.
// It blocks until the worker is stopped via SIGINT/SIGTERM.
func Run(cfg Config, logger *slog.Logger) error {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = runtime.NumCPU()
	}
	if cfg.Concurrency < 1 {
		cfg.Concurrency = 1
	}
	if cfg.Capacity <= 0 {
		cfg.Capacity = runtime.NumCPU() * 2
	}

	containerLimit := cfg.Concurrency
	if cfg.MaxContainers > 0 {
		containerLimit = cfg.MaxContainers
	}
	docker.SetMaxConcurrentContainers(containerLimit)

	hostname, _ := os.Hostname()

	logger.Info("worker starting",
		"api_url", cfg.APIURL,
		"concurrency", cfg.Concurrency,
		"capacity", cfg.Capacity,
		"max_containers", cfg.MaxContainers,
		"hostname", hostname,
	)

	// Wait for Docker daemon
	logger.Info("checking Docker daemon...")
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	if err := docker.WaitForDaemon(waitCtx); err != nil {
		waitCancel()
		return fmt.Errorf("docker daemon not ready: %w", err)
	}
	waitCancel()
	logger.Info("Docker daemon ready")

	api := apiclient.New(cfg.APIURL, cfg.Token, logger)

	// Register with retries (cloud might not be ready yet)
	regCtx, regCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	registeredID, err := api.Register(regCtx, hostname, cfg.Concurrency, cfg.Capacity, cfg.MaxContainers, cfg.Version)
	regCancel()
	if err != nil {
		return fmt.Errorf("registration failed: %w", err)
	}
	cfg.WorkerID = registeredID
	logger.Info("registered", "worker_id", registeredID)

	docker.CleanupStaleContainers()
	docker.CleanupStaleNetworks()

	// Two-phase shutdown:
	// 1. stopClaim: stop claiming new jobs (first SIGINT/SIGTERM)
	// 2. hardCtx: force-kill running jobs (drain timeout or second signal)
	stopClaim, cancelClaim := context.WithCancel(context.Background())
	hardCtx, cancelHard := context.WithCancel(context.Background())
	defer cancelHard()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	state := NewState()

	go func() {
		<-sigChan
		logger.Info("graceful shutdown: stopping claim loop",
			"active_jobs", state.Count(),
			"drain_timeout", cfg.DrainTimeout,
		)
		cancelClaim()

		select {
		case <-time.After(cfg.DrainTimeout):
			logger.Warn("drain timeout exceeded, force stopping")
			cancelHard()
		case sig := <-sigChan:
			logger.Warn("received second signal, force stopping", "signal", sig.String())
			cancelHard()
		}
	}()

	// Heartbeat goroutine — also processes per-job cancellation commands from cloud.
	// When the cloud wants to cancel a running job, it queues a "cancel:{jobID}" command
	// that arrives in the heartbeat response. The worker cancels that job's context.
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-hardCtx.Done():
				return
			case <-ticker.C:
				resp, err := api.Heartbeat(hardCtx, cfg.WorkerID, state.Count(), state.FirstJobID())
				if err != nil {
					logger.Warn("heartbeat failed", "error", err)
					continue
				}
				// Process commands from cloud (e.g. per-job cancellation)
				for _, cmd := range resp.Commands {
					if strings.HasPrefix(cmd, "cancel:") {
						jobID := strings.TrimPrefix(cmd, "cancel:")
						if state.CancelJob(jobID) {
							logger.Info("job cancelled by cloud", "job_id", jobID)
						}
					}
				}
			}
		}
	}()

	// Central claim loop + executor pool
	jobCh := make(chan *ClaimedJob, cfg.Concurrency)

	var wg sync.WaitGroup

	// Single claim goroutine for the entire worker
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(jobCh)
		RunClaimLoop(stopClaim, api, cfg.WorkerID, jobCh, ClaimConfig{
			PollInterval:  cfg.PollInterval,
			MaxSlots:      cfg.Concurrency,
			Capacity:      cfg.Capacity,
			MaxContainers: cfg.MaxContainers,
		}, state, logger)
	}()

	// Executor goroutines — pick jobs from the channel.
	// Each job gets its own cancellable context derived from hardCtx.
	// The heartbeat goroutine can cancel individual jobs via state.CancelJob().
	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		executorID := fmt.Sprintf("%s-%d", cfg.WorkerID, i)
		go func(id string) {
			defer wg.Done()
			for msg := range jobCh {
				jobCtx, jobCancel := context.WithCancel(hardCtx)
				weight := msg.Job.Weight
				if weight < 1 {
					weight = 1
				}
				containers := msg.Job.ContainerCount
				if containers < 1 {
					containers = 1
				}
				state.AddJob(msg.Job.JobID, msg.Meta.Type, jobCancel, weight, containers)
				logger.Info("claimed job",
					"executor", id,
					"job_id", msg.Job.JobID,
					"test_name", msg.Meta.Name,
					"weight", weight,
				)
				ExecuteJob(jobCtx, api, msg.Job, &msg.Meta, id, cfg.WorkerID, logger)
				jobCancel() // ensure context resources freed
				state.RemoveJob(msg.Job.JobID)
			}
		}(executorID)
	}

	wg.Wait()
	logger.Info("worker stopped")
	return nil
}

