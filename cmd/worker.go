// cmd/worker.go is the thin Cobra command that wires flags to the workerloop package.
// All business logic lives in internal packages — this file only does CLI parsing.
package cmd

import (
	"log/slog"
	"os"
	"time"

	"github.com/spf13/cobra"

	"chiperka-worker/internal/workerloop"
)

var workerAPIURL string
var workerPollInterval time.Duration
var workerParallel int
var workerCapacity int
var workerMaxContainers int
var workerTestTimeout int
var workerID string
var workerToken string
var workerName string
var workerDrainTimeout time.Duration

var workerCmd = &cobra.Command{
	Use:   "worker",
	Short: "Process pending jobs from API server",
	Long: `Start a worker process that connects to the API server and executes jobs.

The worker registers with the API, polls for pending jobs, executes them
via DAG instruction execution, and submits results back to the API.

Example:
  chiperka-worker worker --api http://localhost:8080
  chiperka-worker worker --api http://localhost:8080 --concurrency 4
  CHIPERKA_API_URL=https://hub.example.com CHIPERKA_WORKER_TOKEN=spt_... chiperka-worker worker

The token is either a worker token from the hub UI, or the hub's join token
(CHIPERKA_WORKER_JOIN_TOKEN on the hub), with which the worker registers itself.`,
	RunE: runWorkerCmd,
}

func init() {
	rootCmd.AddCommand(workerCmd)
	workerCmd.Flags().StringVar(&workerAPIURL, "api", "http://localhost:8080", "API server URL (env: CHIPERKA_API_URL)")
	workerCmd.Flags().DurationVar(&workerPollInterval, "poll-interval", time.Second, "Polling interval for new work")
	workerCmd.Flags().IntVar(&workerParallel, "concurrency", 0, "Maximum number of parallel test executions (0 = CPU count)")
	workerCmd.Flags().IntVar(&workerCapacity, "capacity", 0, "Maximum concurrent weight for test scheduling (0 = auto: CPU count * 2)")
	workerCmd.Flags().IntVar(&workerMaxContainers, "max-containers", 0, "Maximum concurrent Docker containers (0 = unlimited)")
	workerCmd.Flags().IntVar(&workerTestTimeout, "timeout", 300, "Maximum time in seconds for each test execution")
	workerCmd.Flags().StringVar(&workerID, "id", "", "Worker ID (auto-generated if not provided)")
	workerCmd.Flags().StringVar(&workerToken, "token", "", "Worker token or the hub's join token (env: CHIPERKA_WORKER_TOKEN)")
	workerCmd.Flags().StringVar(&workerName, "name", "", "Name shown in the hub, defaults to the hostname (env: CHIPERKA_WORKER_NAME)")
	workerCmd.Flags().DurationVar(&workerDrainTimeout, "drain-timeout", 5*time.Minute, "Max time to wait for running jobs to finish on shutdown")
}

func runWorkerCmd(cmd *cobra.Command, args []string) error {
	if workerToken == "" {
		workerToken = os.Getenv("CHIPERKA_WORKER_TOKEN")
	}
	if workerName == "" {
		workerName = os.Getenv("CHIPERKA_WORKER_NAME")
	}
	if !cmd.Flags().Changed("api") {
		if url := os.Getenv("CHIPERKA_API_URL"); url != "" {
			workerAPIURL = url
		}
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	return workerloop.Run(workerloop.Config{
		APIURL:        workerAPIURL,
		Token:         workerToken,
		Name:          workerName,
		PollInterval:  workerPollInterval,
		Concurrency:   workerParallel,
		Capacity:      workerCapacity,
		MaxContainers: workerMaxContainers,
		TestTimeout:   workerTestTimeout,
		WorkerID:      workerID,
		DrainTimeout:  workerDrainTimeout,
		Version:       Version,
	}, logger)
}
