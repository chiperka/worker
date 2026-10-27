// Package model defines core domain types for the test runner.
package model

import "time"

// TestStatus represents the outcome of a test execution.
type TestStatus string

const (
	StatusPassed  TestStatus = "passed"
	StatusFailed  TestStatus = "failed"
	StatusError   TestStatus = "error"
	StatusSkipped TestStatus = "skipped"
)

// AssertionResult holds the outcome of a single assertion.
type AssertionResult struct {
	Passed   bool
	Type     string // "response.statusCode", "cli.exitCode", "artifact.<name>.exists", etc.
	Expected string
	Actual   string
	Message  string
	Duration time.Duration
}

// LogEntry holds a single log entry for a test.
type LogEntry struct {
	RelativeTime string // "0.145s"
	Level        string // "info", "warn", "fail", "pass"
	Action       string
	Service      string
	Message      string
}

// Artifact holds information about a collected artifact file.
type Artifact struct {
	Name string
	Path string
	Size int64
}

// ServiceResult holds information about a service setup.
type ServiceResult struct {
	Name     string
	Image    string
	Duration time.Duration
	// Phase breakdown
	ImageResolveDuration   time.Duration
	ContainerStartDuration time.Duration
	HealthCheckDuration    time.Duration
}

// SetupResult holds the outcome of a single setup instruction.
type SetupResult struct {
	Type     string        // "http" or "cli"
	Duration time.Duration
	Success  bool
	Error    error
	// HTTP-specific fields
	HTTPStatusCode int
	// CLI-specific fields
	CLIExitCode int
}

// HTTPResponseData holds HTTP response data from test execution.
type HTTPResponseData struct {
	StatusCode   int
	Headers      map[string][]string
	BodyArtifact *Artifact // Reference to the body artifact, nil if body was empty
}

// CLIResponseData holds CLI command execution data from test execution.
type CLIResponseData struct {
	ExitCode       int
	StdoutArtifact *Artifact // Reference to stdout artifact, nil if empty
	StderrArtifact *Artifact // Reference to stderr artifact, nil if empty
}

// HTTPExchangeResult captures a full HTTP request/response exchange during test execution.
type HTTPExchangeResult struct {
	Phase              string
	PhaseSeq           int
	RequestMethod      string
	RequestURL         string
	RequestHeaders     map[string]string
	RequestBody        string
	ResponseStatusCode int
	ResponseHeaders    map[string][]string
	ResponseBody       string
	Duration           time.Duration
	Error              error
}

// CLIExecutionResult captures a CLI command execution during test execution.
type CLIExecutionResult struct {
	Phase      string
	PhaseSeq   int
	Service    string
	Command    string
	WorkingDir string
	ExitCode   int
	Stdout     string
	Stderr     string
	Duration   time.Duration
	Error      error
}

// TestResult holds the outcome of a single test execution.
type TestResult struct {
	Test              Test
	Status            TestStatus
	AssertionResults  []AssertionResult
	Error             error
	Duration          time.Duration
	ExecutionDuration time.Duration // Time spent executing the test
	UUID              string
	Artifacts         []Artifact
	HTTPResponse      *HTTPResponseData
	CLIResponse       *CLIResponseData
	ServiceResults    []ServiceResult
	SetupResults      []SetupResult
	TeardownResults   []SetupResult
	HTTPExchanges     []HTTPExchangeResult
	CLIExecutions     []CLIExecutionResult
	// Phase totals
	NetworkDuration   time.Duration
	ServicesDuration  time.Duration
	SetupDuration     time.Duration
	TeardownDuration  time.Duration
	AssertionDuration time.Duration
	CleanupDuration   time.Duration
	// Log entries for HTML report
	LogEntries []LogEntry
}

// IsPassed returns true if the test passed.
func (r *TestResult) IsPassed() bool {
	return r.Status == StatusPassed
}

// SuiteResult holds the outcomes of all tests in a suite.
type SuiteResult struct {
	Suite       Suite
	TestResults []TestResult
}

// PassedCount returns the number of passed tests.
func (r *SuiteResult) PassedCount() int {
	count := 0
	for _, tr := range r.TestResults {
		if tr.IsPassed() {
			count++
		}
	}
	return count
}

// FailedCount returns the number of failed tests (excluding skipped).
func (r *SuiteResult) FailedCount() int {
	count := 0
	for _, tr := range r.TestResults {
		if tr.Status == StatusFailed || tr.Status == StatusError {
			count++
		}
	}
	return count
}

// SkippedCount returns the number of skipped tests.
func (r *SuiteResult) SkippedCount() int {
	count := 0
	for _, tr := range r.TestResults {
		if tr.Status == StatusSkipped {
			count++
		}
	}
	return count
}

// RunResult holds the outcomes of all suite executions.
type RunResult struct {
	SuiteResults []SuiteResult
}

// TotalPassed returns the total number of passed tests.
func (r *RunResult) TotalPassed() int {
	count := 0
	for _, sr := range r.SuiteResults {
		count += sr.PassedCount()
	}
	return count
}

// TotalFailed returns the total number of failed tests.
func (r *RunResult) TotalFailed() int {
	count := 0
	for _, sr := range r.SuiteResults {
		count += sr.FailedCount()
	}
	return count
}

// TotalTests returns the total number of tests.
func (r *RunResult) TotalTests() int {
	count := 0
	for _, sr := range r.SuiteResults {
		count += len(sr.TestResults)
	}
	return count
}

// TotalErrors returns the total number of tests with errors.
func (r *RunResult) TotalErrors() int {
	count := 0
	for _, sr := range r.SuiteResults {
		for _, tr := range sr.TestResults {
			if tr.Status == StatusError {
				count++
			}
		}
	}
	return count
}

// TotalSkipped returns the total number of skipped tests.
func (r *RunResult) TotalSkipped() int {
	count := 0
	for _, sr := range r.SuiteResults {
		count += sr.SkippedCount()
	}
	return count
}

// HasFailures returns true if any test failed.
func (r *RunResult) HasFailures() bool {
	return r.TotalFailed() > 0
}
