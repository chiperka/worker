package instruction

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mockLogSender records batches sent.
type mockLogSender struct {
	mu            sync.Mutex
	batches       [][]LogEntry
	totalEntries  int
	nextInterval  time.Duration
	failCount     int32 // atomic: number of sends to fail before succeeding
}

func (m *mockLogSender) SendLogs(jobID string, logs []LogEntry) (time.Duration, error) {
	if n := atomic.AddInt32(&m.failCount, -1); n >= 0 {
		return 0, &sendError{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.batches = append(m.batches, logs)
	m.totalEntries += len(logs)
	return m.nextInterval, nil
}

func (m *mockLogSender) getBatches() [][]LogEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.batches
}

func (m *mockLogSender) getTotal() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.totalEntries
}

type sendError struct{}

func (e *sendError) Error() string { return "send failed" }

func TestLogCollector_BasicFlush(t *testing.T) {
	sender := &mockLogSender{}
	c := NewLogCollector(sender, "job-1", time.Now())
	defer c.Stop()

	c.Log("info", "test_start", "", "Test started", "start")
	c.Log("pass", "assertion", "", "Assertion passed", "assert")

	// Force flush
	c.Flush()

	batches := sender.getBatches()
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}
	if len(batches[0]) != 2 {
		t.Errorf("expected 2 entries, got %d", len(batches[0]))
	}
	if batches[0][0].Level != "info" {
		t.Errorf("expected level=info, got %s", batches[0][0].Level)
	}
	if batches[0][1].Message != "Assertion passed" {
		t.Errorf("unexpected message: %s", batches[0][1].Message)
	}
}

func TestLogCollector_EmptyFlush(t *testing.T) {
	sender := &mockLogSender{}
	c := NewLogCollector(sender, "job-1", time.Now())
	defer c.Stop()

	// Flush with nothing buffered
	c.Flush()

	if len(sender.getBatches()) != 0 {
		t.Error("should not send empty batch")
	}
}

func TestLogCollector_RelativeMs(t *testing.T) {
	start := time.Now()
	sender := &mockLogSender{}
	c := NewLogCollector(sender, "job-1", start)
	defer c.Stop()

	time.Sleep(50 * time.Millisecond)
	c.Log("info", "", "", "after 50ms", "")

	c.Flush()

	batches := sender.getBatches()
	if len(batches) != 1 || len(batches[0]) != 1 {
		t.Fatal("expected 1 entry")
	}
	if batches[0][0].RelativeMs < 40 {
		t.Errorf("relative_ms too low: %d", batches[0][0].RelativeMs)
	}
}

func TestLogCollector_StopFlushesRemaining(t *testing.T) {
	sender := &mockLogSender{}
	c := NewLogCollector(sender, "job-1", time.Now())

	c.Log("info", "", "", "msg1", "")
	c.Log("info", "", "", "msg2", "")

	// Stop should flush
	c.Stop()

	if sender.getTotal() != 2 {
		t.Errorf("expected 2 entries after stop, got %d", sender.getTotal())
	}

	// Logs after stop are ignored
	c.Log("info", "", "", "msg3", "")
	c.Flush()

	if sender.getTotal() != 2 {
		t.Errorf("should not accept logs after stop, got %d", sender.getTotal())
	}
}

func TestLogCollector_StopIdempotent(t *testing.T) {
	sender := &mockLogSender{}
	c := NewLogCollector(sender, "job-1", time.Now())
	c.Log("info", "", "", "msg", "")

	c.Stop()
	c.Stop() // second stop should not panic
	c.Stop()

	if sender.getTotal() != 1 {
		t.Errorf("expected 1 entry, got %d", sender.getTotal())
	}
}

func TestLogCollector_PeriodicFlush(t *testing.T) {
	sender := &mockLogSender{}
	c := &LogCollector{
		sender:    sender,
		jobID:     "job-1",
		startTime: time.Now(),
		interval:  100 * time.Millisecond, // fast interval for testing
		buf:       make([]LogEntry, 0, 256),
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
	go c.run()

	c.Log("info", "", "", "msg1", "")
	time.Sleep(200 * time.Millisecond)

	c.Log("info", "", "", "msg2", "")
	time.Sleep(200 * time.Millisecond)

	c.Stop()

	total := sender.getTotal()
	if total != 2 {
		t.Errorf("expected 2 total entries, got %d", total)
	}
	batches := sender.getBatches()
	if len(batches) < 2 {
		t.Errorf("expected at least 2 batches (periodic), got %d", len(batches))
	}
}

func TestLogCollector_ConcurrentWrites(t *testing.T) {
	sender := &mockLogSender{}
	c := NewLogCollector(sender, "job-1", time.Now())

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			c.Log("info", "", "", "concurrent msg", "step")
		}(i)
	}
	wg.Wait()

	c.Stop()

	if sender.getTotal() != 100 {
		t.Errorf("expected 100 entries, got %d", sender.getTotal())
	}
}

func TestLogCollector_ServerDirectedInterval(t *testing.T) {
	sender := &mockLogSender{nextInterval: 5 * time.Second}
	c := &LogCollector{
		sender:    sender,
		jobID:     "job-1",
		startTime: time.Now(),
		interval:  1 * time.Second, // start slow
		buf:       make([]LogEntry, 0, 256),
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
	go c.run()

	c.Log("info", "", "", "trigger interval change", "")
	c.Flush()

	// After flush, interval should be updated to 5s (clamped by MinFlushInterval)
	c.mu.Lock()
	interval := c.interval
	c.mu.Unlock()

	c.Stop()

	if interval != 5*time.Second {
		t.Errorf("expected interval 5s after server response, got %v", interval)
	}
}

func TestLogCollector_RetryOnFailure(t *testing.T) {
	sender := &mockLogSender{}
	atomic.StoreInt32(&sender.failCount, 1) // first send fails, second succeeds

	c := NewLogCollector(sender, "job-1", time.Now())

	c.Log("info", "", "", "will retry", "")
	c.Flush() // this one fails, logs re-queued

	if c.Count() != 1 {
		t.Errorf("expected 1 buffered entry after failed flush, got %d", c.Count())
	}

	c.Flush() // this one succeeds

	if sender.getTotal() != 1 {
		t.Errorf("expected 1 entry after retry, got %d", sender.getTotal())
	}
	if c.Count() != 0 {
		t.Errorf("expected 0 buffered after successful retry, got %d", c.Count())
	}

	c.Stop()
}

func TestLogCollector_StepField(t *testing.T) {
	sender := &mockLogSender{}
	c := NewLogCollector(sender, "job-1", time.Now())
	defer c.Stop()

	c.Log("info", "service_ready", "api", "API is ready", "start_services")
	c.Flush()

	batches := sender.getBatches()
	if len(batches) != 1 || len(batches[0]) != 1 {
		t.Fatal("expected 1 entry")
	}
	entry := batches[0][0]
	if entry.Step != "start_services" {
		t.Errorf("expected step=start_services, got %s", entry.Step)
	}
	if entry.Service != "api" {
		t.Errorf("expected service=api, got %s", entry.Service)
	}
	if entry.Action != "service_ready" {
		t.Errorf("expected action=service_ready, got %s", entry.Action)
	}
}

func TestLogCollector_MaxBatchForceFlush(t *testing.T) {
	sender := &mockLogSender{}
	c := NewLogCollector(sender, "job-1", time.Now())
	defer c.Stop()

	// Fill buffer past MaxBatchSize to trigger auto-flush
	for i := 0; i < MaxBatchSize+10; i++ {
		c.Log("info", "", "", "msg", "")
	}

	// Give the async flush goroutine a moment
	time.Sleep(50 * time.Millisecond)

	total := sender.getTotal()
	if total < MaxBatchSize {
		t.Errorf("expected auto-flush at MaxBatchSize, only %d sent", total)
	}
}

func TestLogCollector_BufferCap(t *testing.T) {
	// Sender always fails → logs accumulate but should be capped at MaxBufferSize
	sender := &mockLogSender{}
	atomic.StoreInt32(&sender.failCount, 100000) // always fail

	c := NewLogCollector(sender, "job-1", time.Now())

	// Write more than MaxBufferSize entries
	for i := 0; i < MaxBufferSize+1000; i++ {
		c.Log("info", "", "", "msg", "")
	}

	count := c.Count()
	if count > MaxBufferSize {
		t.Errorf("buffer should be capped at %d, got %d", MaxBufferSize, count)
	}

	c.Stop()
}

func TestLogCollector_NilSender(t *testing.T) {
	// Should not panic with nil sender
	c := NewLogCollector(nil, "job-1", time.Now())
	c.Log("info", "", "", "msg", "")
	c.Flush() // should silently no-op
	c.Stop()
}

func TestLogCollector_FlushAfterStop(t *testing.T) {
	sender := &mockLogSender{}
	c := NewLogCollector(sender, "job-1", time.Now())
	c.Log("info", "", "", "msg1", "")
	c.Stop()

	// Flush after stop should be safe
	c.Flush()

	if sender.getTotal() != 1 {
		t.Errorf("expected 1 entry, got %d", sender.getTotal())
	}
}

func TestLogCollector_MultipleStops(t *testing.T) {
	sender := &mockLogSender{}
	c := NewLogCollector(sender, "job-1", time.Now())
	c.Log("info", "", "", "msg", "")

	c.Stop()
	c.Stop()
	c.Stop()

	if sender.getTotal() != 1 {
		t.Errorf("expected 1, got %d", sender.getTotal())
	}
}

func TestLogCollector_LevelAndFields(t *testing.T) {
	sender := &mockLogSender{}
	c := NewLogCollector(sender, "job-1", time.Now())

	c.Log("pass", "assertion_passed", "api", "Status code matches", "assert")
	c.Log("fail", "assertion_failed", "db", "Timeout exceeded", "assert")
	c.Log("warn", "deprecation", "", "API v1 deprecated", "execute")
	c.Flush()
	c.Stop()

	batches := sender.getBatches()
	if len(batches) != 1 || len(batches[0]) != 3 {
		t.Fatalf("expected 1 batch with 3 entries, got %d batches", len(batches))
	}

	e := batches[0][0]
	if e.Level != "pass" || e.Action != "assertion_passed" || e.Service != "api" || e.Step != "assert" {
		t.Errorf("unexpected entry: %+v", e)
	}

	e2 := batches[0][2]
	if e2.Level != "warn" || e2.Step != "execute" {
		t.Errorf("unexpected entry: %+v", e2)
	}
}
