package tpcc

import (
	"bufio"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/supabase/go-tpc/pkg/measurement"
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

// TestOutputRtMeasurement_QuietTickStillRecordsSideEffects covers the bug
// fixed alongside quietTick: a prior change silenced the whole periodic tick
// (including Prometheus gauge updates and the --raw-samples-file append) in
// non-interactive terminals, not just its table print. os.Stdout is a pipe in
// `go test` (never a real TTY), so this tick is always the "quiet" case here.
func TestOutputRtMeasurement_QuietTickStillRecordsSideEffects(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raw.csv")
	m := measurement.NewMeasurement(measurement.WithRawSamplesFile(path))
	m.Measure("new_order", 10*time.Millisecond, nil)

	lines := captureStdout(t, func() {
		m.Output(false, "plain", outputRtMeasurement)
	})
	if len(lines) != 0 {
		t.Errorf("quiet tick printed %d lines, want 0 (non-interactive): %v", len(lines), lines)
	}

	if got := testutil.ToFloat64(elapsedVec.WithLabelValues("NEW_ORDER")); got <= 0 {
		t.Errorf("elapsedVec[NEW_ORDER] = %v, want > 0: gauge update must not depend on interactivity", got)
	}
	if got := testutil.ToFloat64(countVec.WithLabelValues("NEW_ORDER")); got != 1 {
		t.Errorf("countVec[NEW_ORDER] = %v, want 1: gauge update must not depend on interactivity", got)
	}

	rows := readRawSamplesCSV(t, path)
	if len(rows) != 3 { // header + ok row + error row
		t.Fatalf("raw-samples-file has %d rows, want 3 (header+2): a quiet tick must still append: %v", len(rows), rows)
	}
}

// TestOutputRtMeasurement_SummaryAlwaysPrints ensures the final summary
// (measurement.SummaryPrefix) is never treated as a quiet tick, regardless of
// interactivity: CI logs must still show the end-of-run report.
func TestOutputRtMeasurement_SummaryAlwaysPrints(t *testing.T) {
	m := measurement.NewMeasurement()
	m.Measure("new_order", 10*time.Millisecond, nil)

	lines := captureStdout(t, func() {
		m.Output(true, "plain", outputRtMeasurement)
	})
	if len(lines) == 0 {
		t.Error("summary report printed no lines, want at least one regardless of interactivity")
	}
}

func readRawSamplesCSV(t *testing.T, path string) [][]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	var rows [][]string
	for scanner.Scan() {
		rows = append(rows, []string{scanner.Text()})
	}
	return rows
}
