package workerloop

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestState_AddRemoveCount(t *testing.T) {
	s := NewState()
	if s.Count() != 0 {
		t.Errorf("expected 0, got %d", s.Count())
	}

	s.AddJob("j1", "test", func() {}, 1, 1)
	s.AddJob("j2", "execution", func() {}, 1, 1)
	if s.Count() != 2 {
		t.Errorf("expected 2, got %d", s.Count())
	}

	s.RemoveJob("j1")
	if s.Count() != 1 {
		t.Errorf("expected 1, got %d", s.Count())
	}

	s.RemoveJob("nonexistent") // should not panic
	if s.Count() != 1 {
		t.Errorf("expected 1, got %d", s.Count())
	}
}

func TestState_FirstJobID(t *testing.T) {
	s := NewState()
	if id := s.FirstJobID(); id != "" {
		t.Errorf("expected empty, got %q", id)
	}

	s.AddJob("j1", "test", func() {}, 1, 1)
	if id := s.FirstJobID(); id == "" {
		t.Error("expected non-empty job ID")
	}
}

func TestState_ConcurrentAccess(t *testing.T) {
	s := NewState()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			id := "j" + string(rune('0'+n%10))
			s.AddJob(id, "test", func() {}, 1, 1)
			s.Count()
			s.FirstJobID()
			s.RemoveJob(id)
		}(i)
	}
	wg.Wait()
}

func TestState_CancelJob(t *testing.T) {
	s := NewState()

	var cancelled atomic.Bool
	s.AddJob("j1", "test", func() { cancelled.Store(true) }, 1, 1)

	// Cancel existing job
	if !s.CancelJob("j1") {
		t.Error("expected CancelJob to return true for active job")
	}
	if !cancelled.Load() {
		t.Error("expected cancel function to be called")
	}

	// Cancel non-existent job
	if s.CancelJob("j999") {
		t.Error("expected CancelJob to return false for unknown job")
	}

	// Cancel after remove
	s.RemoveJob("j1")
	if s.CancelJob("j1") {
		t.Error("expected CancelJob to return false after remove")
	}
}

func TestState_CancelJob_NilCancel(t *testing.T) {
	s := NewState()
	s.AddJob("j1", "test", nil, 1, 1)

	// Should not panic with nil cancel func
	if s.CancelJob("j1") {
		t.Error("expected false for nil cancel func")
	}
}
