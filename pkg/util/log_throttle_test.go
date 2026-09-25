package util

import (
	"sync"
	"testing"
	"time"
)

func TestLogThrottle_ConcurrentBurstAllowsExactlyOnce(t *testing.T) {
	th := NewLogThrottle(50 * time.Millisecond)

	const calls = 200
	var wg sync.WaitGroup
	var mu sync.Mutex
	allowed := 0
	var totalSuppressed int

	wg.Add(calls)
	for i := 0; i < calls; i++ {
		go func() {
			defer wg.Done()
			if ok, suppressed := th.Allow(); ok {
				mu.Lock()
				allowed++
				totalSuppressed += suppressed
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if allowed != 1 {
		t.Errorf("allowed = %d, want exactly 1 within the window", allowed)
	}
	// The remaining (calls-1) calls all lost the race to become the one
	// allowed call, so they must all have been counted as suppressed by it.
	if totalSuppressed != 0 {
		t.Errorf("totalSuppressed on the single allowed call = %d, want 0 (nothing preceded it)", totalSuppressed)
	}
}

func TestLogThrottle_AllowsAgainAfterWindowElapses(t *testing.T) {
	th := NewLogThrottle(10 * time.Millisecond)

	ok, suppressed := th.Allow()
	if !ok || suppressed != 0 {
		t.Fatalf("first Allow() = (%v, %d), want (true, 0)", ok, suppressed)
	}

	// These two land inside the window and must be suppressed, not logged.
	if ok, _ := th.Allow(); ok {
		t.Error("Allow() within window = true, want false")
	}
	if ok, _ := th.Allow(); ok {
		t.Error("Allow() within window = true, want false")
	}

	time.Sleep(20 * time.Millisecond)

	ok, suppressed = th.Allow()
	if !ok {
		t.Fatal("Allow() after window elapsed = false, want true")
	}
	if suppressed != 2 {
		t.Errorf("suppressed = %d, want 2 (the two calls made within the window)", suppressed)
	}
}
