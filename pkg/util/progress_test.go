package util

import (
	"bufio"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// captureStdout redirects os.Stdout to a pipe for the duration of f and
// returns every line f wrote to it.
func captureStdout(t *testing.T, f func()) []string {
	t.Helper()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	os.Stdout = w

	f()

	os.Stdout = old
	w.Close()

	var lines []string
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines
}

var milestoneLineRE = regexp.MustCompile(`^test: (\d+)% \(\d+/\d+\)$`)

func TestProgressNonInteractiveMilestones(t *testing.T) {
	// os.Stdout is a pipe (not a terminal) for the whole test, so Progress
	// detects non-interactive mode and prints one line per 10% milestone.
	var p *Progress
	lines := captureStdout(t, func() {
		p = NewProgress("test", 100)

		var wg sync.WaitGroup
		for g := 0; g < 10; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 10; i++ {
					p.Add(1)
				}
			}()
		}
		wg.Wait()
	})

	seen := make(map[int]int)
	for _, line := range lines {
		m := milestoneLineRE.FindStringSubmatch(line)
		if m == nil {
			t.Fatalf("unexpected line: %q", line)
		}
		pct, err := strconv.Atoi(m[1])
		if err != nil {
			t.Fatalf("parse percent from %q: %v", line, err)
		}
		seen[pct]++
	}

	for pct := 0; pct <= 100; pct += 10 {
		if seen[pct] != 1 {
			t.Errorf("milestone %d%% printed %d times, want exactly 1", pct, seen[pct])
		}
	}
	if len(seen) != 11 {
		t.Errorf("got %d distinct milestone lines, want 11 (0%%..100%% by 10)", len(seen))
	}
}

func TestProgressInteractiveRedraw(t *testing.T) {
	// interactive is forced directly (same package) since a real TTY isn't
	// reachable in tests; term.IsTerminal itself is a thin, trusted stdlib-
	// adjacent wrapper and isn't what this test is checking.
	var p *Progress
	lines := captureStdout(t, func() {
		p = &Progress{label: "test", total: 10, interactive: true}
		p.Add(10) // completes in one call
	})

	if len(lines) != 1 {
		t.Fatalf("got %d lines, want 1 (the final line): %q", len(lines), lines)
	}
	got := strings.TrimPrefix(lines[0], "\r")
	if want := "test: 100% (10/10)"; got != want {
		t.Errorf("final line = %q, want %q", got, want)
	}
}

func TestProgressStopsAtTotal(t *testing.T) {
	lines := captureStdout(t, func() {
		p := NewProgress("test", 10)
		p.Add(10)
		// Further Add calls after completion must be no-ops.
		p.Add(1)
		p.Add(1)
	})

	count := 0
	for _, line := range lines {
		if milestoneLineRE.MatchString(line) {
			count++
		}
	}
	// 0% (from NewProgress) + 100% (from the completing Add) = 2 lines total.
	if count != 2 {
		t.Errorf("got %d milestone lines, want 2", count)
	}
}
