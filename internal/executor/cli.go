// Package executor provides test execution implementations.
package executor

// CLIResponse holds the response data from a CLI command execution.
type CLIResponse struct {
	ExitCode int
	Stdout   []byte
	Stderr   []byte
}
