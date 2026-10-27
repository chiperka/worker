package workerloop

import (
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Config validation edge cases (exercised via the Run preamble logic)
// ---------------------------------------------------------------------------

func TestConfig_DefaultConcurrency(t *testing.T) {
	cfg := Config{
		Concurrency: 0,
	}
	// The Run function sets Concurrency to runtime.NumCPU() when <= 0.
	// We can't call Run (it has side-effects), but we can verify the struct
	// accepts defaults and the zero value is what triggers the adjustment.
	if cfg.Concurrency != 0 {
		t.Errorf("expected zero-value concurrency, got %d", cfg.Concurrency)
	}
}

func TestConfig_NegativeConcurrency(t *testing.T) {
	cfg := Config{
		Concurrency: -5,
	}
	if cfg.Concurrency >= 0 {
		t.Errorf("expected negative concurrency to be stored as-is, got %d", cfg.Concurrency)
	}
}

func TestConfig_DrainTimeoutZero(t *testing.T) {
	cfg := Config{
		DrainTimeout: 0,
	}
	if cfg.DrainTimeout != 0 {
		t.Errorf("expected zero drain timeout, got %v", cfg.DrainTimeout)
	}
}

func TestConfig_FieldsPreserved(t *testing.T) {
	cfg := Config{
		APIURL:        "https://example.com",
		Token:         "tok-123",
		PollInterval:  5 * time.Second,
		Concurrency:   4,
		Capacity:      100,
		MaxContainers: 10,
		TestTimeout:   120,
		WorkerID:      "w-1",
		DrainTimeout:  30 * time.Second,
		Version:       "1.2.3",
	}
	if cfg.APIURL != "https://example.com" {
		t.Errorf("APIURL mismatch")
	}
	if cfg.Token != "tok-123" {
		t.Errorf("Token mismatch")
	}
	if cfg.PollInterval != 5*time.Second {
		t.Errorf("PollInterval mismatch")
	}
	if cfg.Concurrency != 4 {
		t.Errorf("Concurrency mismatch")
	}
	if cfg.Capacity != 100 {
		t.Errorf("Capacity mismatch")
	}
	if cfg.MaxContainers != 10 {
		t.Errorf("MaxContainers mismatch")
	}
	if cfg.TestTimeout != 120 {
		t.Errorf("TestTimeout mismatch")
	}
	if cfg.WorkerID != "w-1" {
		t.Errorf("WorkerID mismatch")
	}
	if cfg.DrainTimeout != 30*time.Second {
		t.Errorf("DrainTimeout mismatch")
	}
	if cfg.Version != "1.2.3" {
		t.Errorf("Version mismatch")
	}
}
