package model

import "testing"

func TestTestResult_IsPassed(t *testing.T) {
	tests := []struct {
		status TestStatus
		want   bool
	}{
		{StatusPassed, true},
		{StatusFailed, false},
		{StatusError, false},
		{StatusSkipped, false},
	}
	for _, tt := range tests {
		tr := &TestResult{Status: tt.status}
		if tr.IsPassed() != tt.want {
			t.Errorf("IsPassed() for %q: expected %v", tt.status, tt.want)
		}
	}
}

func makeSuiteResult() *SuiteResult {
	return &SuiteResult{
		TestResults: []TestResult{
			{Status: StatusPassed},
			{Status: StatusPassed},
			{Status: StatusFailed},
			{Status: StatusError},
			{Status: StatusSkipped},
		},
	}
}

func TestSuiteResult_PassedCount(t *testing.T) {
	sr := makeSuiteResult()
	if sr.PassedCount() != 2 {
		t.Fatalf("expected 2, got %d", sr.PassedCount())
	}
}

func TestSuiteResult_FailedCount(t *testing.T) {
	sr := makeSuiteResult()
	if sr.FailedCount() != 2 {
		t.Fatalf("expected 2 (failed+error), got %d", sr.FailedCount())
	}
}

func TestSuiteResult_SkippedCount(t *testing.T) {
	sr := makeSuiteResult()
	if sr.SkippedCount() != 1 {
		t.Fatalf("expected 1, got %d", sr.SkippedCount())
	}
}

func makeRunResult() *RunResult {
	return &RunResult{
		SuiteResults: []SuiteResult{
			{TestResults: []TestResult{
				{Status: StatusPassed},
				{Status: StatusFailed},
			}},
			{TestResults: []TestResult{
				{Status: StatusPassed},
				{Status: StatusError},
				{Status: StatusSkipped},
			}},
		},
	}
}

func TestRunResult_TotalPassed(t *testing.T) {
	rr := makeRunResult()
	if rr.TotalPassed() != 2 {
		t.Fatalf("expected 2, got %d", rr.TotalPassed())
	}
}

func TestRunResult_TotalFailed(t *testing.T) {
	rr := makeRunResult()
	if rr.TotalFailed() != 2 {
		t.Fatalf("expected 2, got %d", rr.TotalFailed())
	}
}

func TestRunResult_TotalTests(t *testing.T) {
	rr := makeRunResult()
	if rr.TotalTests() != 5 {
		t.Fatalf("expected 5, got %d", rr.TotalTests())
	}
}

func TestRunResult_TotalErrors(t *testing.T) {
	rr := makeRunResult()
	if rr.TotalErrors() != 1 {
		t.Fatalf("expected 1, got %d", rr.TotalErrors())
	}
}

func TestRunResult_TotalSkipped(t *testing.T) {
	rr := makeRunResult()
	if rr.TotalSkipped() != 1 {
		t.Fatalf("expected 1, got %d", rr.TotalSkipped())
	}
}

func TestRunResult_HasFailures(t *testing.T) {
	rr := makeRunResult()
	if !rr.HasFailures() {
		t.Fatal("expected HasFailures=true")
	}

	clean := &RunResult{
		SuiteResults: []SuiteResult{
			{TestResults: []TestResult{{Status: StatusPassed}}},
		},
	}
	if clean.HasFailures() {
		t.Fatal("expected HasFailures=false")
	}
}
