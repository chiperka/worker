package instruction

import (
	"sync"
	"time"
)

const (
	// DefaultFlushInterval is the default log flush interval (matches GitLab Runner).
	DefaultFlushInterval = 3 * time.Second
	// MaxBatchSize is the maximum number of log entries per flush.
	MaxBatchSize = 500
	// MaxBufferSize is the hard cap on buffered log entries.
	// If cloud is unreachable, oldest entries are dropped to prevent unbounded memory growth.
	MaxBufferSize = 10000
	// MinFlushInterval is the minimum allowed flush interval (prevents busy-loop from bad server value).
	MinFlushInterval = time.Second
	// MaxFlushInterval is the maximum allowed flush interval.
	MaxFlushInterval = 60 * time.Second
)

// LogEntry represents a single log line from a job.
type LogEntry struct {
	RelativeMs int64  `json:"relative_ms"`
	Level      string `json:"level"`            // info, warn, pass, fail
	Action     string `json:"action,omitempty"`
	Service    string `json:"service,omitempty"`
	Message    string `json:"message"`
	Step       string `json:"step,omitempty"` // which step produced this log
}

// LogSender is the interface for sending batched logs to the cloud.
type LogSender interface {
	SendLogs(jobID string, logs []LogEntry) (nextInterval time.Duration, err error)
}

// LogCollector buffers log entries and flushes them to the cloud periodically.
// Thread-safe — handlers call Log() from any goroutine.
//
// Lifecycle: NewLogCollector() → Log()/Flush() → Stop()
//
// Design (matches GitLab Runner trace buffer):
//   - Background goroutine flushes at configurable interval
//   - Server can adjust interval via SendLogs response
//   - Buffer caps at MaxBufferSize, oldest entries dropped on overflow
//   - Stop() waits for all in-flight flushes before final flush
type LogCollector struct {
	mu        sync.Mutex
	buf       []LogEntry
	sender    LogSender
	jobID     string
	startTime time.Time
	interval  time.Duration

	stopCh  chan struct{} // closed once to signal run() to exit
	doneCh  chan struct{} // closed when run() has exited
	flushWg sync.WaitGroup
	stopped bool
}

// NewLogCollector creates a log collector that flushes every interval.
// Call Stop() when the job is done to flush remaining logs and stop the background loop.
func NewLogCollector(sender LogSender, jobID string, startTime time.Time) *LogCollector {
	c := &LogCollector{
		sender:    sender,
		jobID:     jobID,
		startTime: startTime,
		interval:  DefaultFlushInterval,
		buf:       make([]LogEntry, 0, 256), // pre-allocate for typical job
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
	go c.run()
	return c
}

// run is the background flush loop. Uses time.After with dynamic interval
// instead of a ticker to safely support server-directed interval changes.
// Exits when stopCh is closed.
func (c *LogCollector) run() {
	defer close(c.doneCh)
	for {
		c.mu.Lock()
		interval := c.interval
		c.mu.Unlock()

		select {
		case <-time.After(interval):
			c.flush()
		case <-c.stopCh:
			return
		}
	}
}

// Log appends a log entry to the buffer. Non-blocking.
func (c *LogCollector) Log(level, action, service, message, step string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return
	}
	// Drop oldest batch if buffer is at hard cap (cloud unreachable)
	if len(c.buf) >= MaxBufferSize {
		c.buf = c.buf[MaxBatchSize:]
	}
	c.buf = append(c.buf, LogEntry{
		RelativeMs: time.Since(c.startTime).Milliseconds(),
		Level:      level,
		Action:     action,
		Service:    service,
		Message:    message,
		Step:       step,
	})
	// Force flush if buffer is getting large — tracked by flushWg so Stop() can wait
	if len(c.buf) >= MaxBatchSize {
		c.flushWg.Add(1)
		go func() {
			defer c.flushWg.Done()
			c.flush()
		}()
	}
}

// Flush sends all buffered logs immediately. Safe to call from any goroutine.
// Use this on step transitions (step_complete, step_fail) for timely delivery.
func (c *LogCollector) Flush() {
	c.flush()
}

// Stop flushes remaining logs and stops the background loop.
// Must be called before CompleteJob. Safe to call multiple times.
// Blocks until all in-flight flushes complete and the background loop exits.
func (c *LogCollector) Stop() {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return
	}
	c.stopped = true
	c.mu.Unlock()

	close(c.stopCh)
	<-c.doneCh       // wait for background loop to exit
	c.flushWg.Wait() // wait for any in-flight async flushes
	c.flush()         // final flush of anything remaining
}

// flush sends all buffered entries to the cloud.
func (c *LogCollector) flush() {
	c.mu.Lock()
	if len(c.buf) == 0 {
		c.mu.Unlock()
		return
	}
	batch := c.buf
	c.buf = make([]LogEntry, 0, 256)
	c.mu.Unlock()

	if c.sender == nil {
		return
	}

	// Send and handle server-directed interval
	nextInterval, err := c.sender.SendLogs(c.jobID, batch)
	if err != nil {
		// Re-queue failed logs (prepend to buffer for next flush)
		c.mu.Lock()
		c.buf = append(batch, c.buf...)
		c.mu.Unlock()
		return
	}

	// Server-directed interval update with bounds checking
	if nextInterval > 0 && nextInterval != c.interval {
		if nextInterval < MinFlushInterval {
			nextInterval = MinFlushInterval
		}
		if nextInterval > MaxFlushInterval {
			nextInterval = MaxFlushInterval
		}
		c.mu.Lock()
		c.interval = nextInterval
		c.mu.Unlock()
	}
}

// Count returns the number of buffered (unsent) log entries.
func (c *LogCollector) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.buf)
}
