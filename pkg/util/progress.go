package util

import (
	"fmt"
	"sync"
	"time"
)

// progressRenderInterval throttles interactive redraws so concurrent Add
// calls from many goroutines don't turn into a flood of terminal writes.
const progressRenderInterval = 150 * time.Millisecond

// Progress reports coarse-grained progress for a long-running loop driven by
// multiple goroutines, such as tpcc prepare's per-warehouse/per-district
// loading. Add is safe to call concurrently.
//
// In an interactive terminal it redraws a single line in place. In a
// non-interactive terminal (piped output, redirected to a file, CI) it
// instead prints one line each time cumulative progress crosses a 10%
// milestone, so long runs don't flood captured logs.
type Progress struct {
	label       string
	total       int64
	interactive bool

	mu            sync.Mutex
	count         int64
	lastMilestone int64
	lastRender    time.Time
	done          bool
}

// NewProgress creates a Progress tracker for total units of work and prints
// the initial state. A total <= 0 makes Add a no-op (nothing to track).
func NewProgress(label string, total int64) *Progress {
	p := &Progress{
		label:       label,
		total:       total,
		interactive: IsInteractiveStdout(),
	}
	if total <= 0 {
		return p
	}
	if p.interactive {
		p.render()
	} else {
		p.renderMilestone(0)
	}
	return p
}

// Add records n completed units of work.
func (p *Progress) Add(n int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.done || p.total <= 0 {
		return
	}

	p.count += n
	if p.count >= p.total {
		p.count = p.total
		p.done = true
		if p.interactive {
			p.render()
			fmt.Println()
		} else {
			p.renderMilestone(100)
		}
		return
	}

	if p.interactive {
		if time.Since(p.lastRender) >= progressRenderInterval {
			p.render()
		}
		return
	}

	pct := p.count * 100 / p.total
	if pct >= p.lastMilestone+10 {
		p.lastMilestone = (pct / 10) * 10
		p.renderMilestone(p.lastMilestone)
	}
}

// render redraws the current line in place. Callers must hold p.mu.
func (p *Progress) render() {
	pct := p.count * 100 / p.total
	fmt.Printf("\r%s: %d%% (%d/%d)", p.label, pct, p.count, p.total)
	p.lastRender = time.Now()
}

// renderMilestone prints one progress line. Callers must hold p.mu.
func (p *Progress) renderMilestone(pct int64) {
	fmt.Printf("%s: %d%% (%d/%d)\n", p.label, pct, p.count, p.total)
}
