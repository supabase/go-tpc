package util

import (
	"sync"
	"time"
)

// LogThrottle rate-limits a burst of near-duplicate log lines from many
// concurrent goroutines hitting the same failure repeatedly (e.g. every
// worker timing out or failing to prepare statements during a DB stall).
// The first call within a window is logged; further calls within the same
// window are counted and folded into the next line once the window elapses.
type LogThrottle struct {
	window time.Duration

	mu         sync.Mutex
	lastLog    time.Time
	suppressed int
}

// NewLogThrottle returns a LogThrottle that allows at most one log line per
// window, regardless of how many goroutines call Allow concurrently.
func NewLogThrottle(window time.Duration) *LogThrottle {
	return &LogThrottle{window: window}
}

// Allow reports whether the caller should print now, and how many prior
// calls were suppressed since the last time Allow returned true.
func (t *LogThrottle) Allow() (ok bool, suppressed int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	if !t.lastLog.IsZero() && now.Sub(t.lastLog) < t.window {
		t.suppressed++
		return false, 0
	}

	suppressed = t.suppressed
	t.suppressed = 0
	t.lastLog = now
	return true, suppressed
}
