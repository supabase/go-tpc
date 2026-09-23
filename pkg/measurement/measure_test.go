package measurement

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/supabase/go-tpc/pkg/util"
)

func noopRender(string, string, map[string]*Histogram) {}

// captureRender records the histograms Output hands the renderer, which is
// what the command-line tables are built from.
func captureRender(dst *map[string]*Histogram) func(string, string, map[string]*Histogram) {
	return func(_, _ string, hists map[string]*Histogram) { *dst = hists }
}

func TestAppendRawSamples_DisabledByDefault(t *testing.T) {
	m := NewMeasurement()
	m.Measure("new_order", time.Millisecond, nil)
	m.Output(false, util.OutputStylePlain, noopRender)
	m.Output(true, util.OutputStylePlain, noopRender)
	// Nothing to assert on disk -- just that no path means no error and no
	// panic anywhere in the write path.
}

func TestAppendRawSamples_WritesHeaderAndRowsPerTick(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raw.csv")
	m := NewMeasurement(WithRawSamplesFile(path))

	m.Measure("new_order", 10*time.Millisecond, nil)
	m.Measure("new_order", 20*time.Millisecond, errors.New("boom"))
	m.Output(false, util.OutputStylePlain, noopRender)

	rows := readCSV(t, path)
	if len(rows) != 3 { // header + ok row + error row
		t.Fatalf("after tick 1: want 3 rows (header+2), got %d: %v", len(rows), rows)
	}
	if got := strings.Join(rows[0], ","); got != strings.Join(rawSamplesHeader, ",") {
		t.Fatalf("header = %q, want %q", got, strings.Join(rawSamplesHeader, ","))
	}
	byStatus := map[string][]string{}
	for _, r := range rows[1:] {
		byStatus[r[2]] = r
	}
	ok, ok1 := byStatus["ok"]
	if !ok1 {
		t.Fatalf("no ok row in %v", rows)
	}
	if ok[1] != "NEW_ORDER" {
		t.Errorf("transaction = %q, want NEW_ORDER", ok[1])
	}
	errRow, ok2 := byStatus["error"]
	if !ok2 {
		t.Fatalf("no error row in %v", rows)
	}
	if errRow[1] != "NEW_ORDER" {
		t.Errorf("error row transaction = %q, want NEW_ORDER", errRow[1])
	}

	// File must already be flushed/readable mid-run, before any close.
	// Second tick: OpCurMeasurement was drained by the first Output(false)
	// call, so only what's measured after that point shows up now.
	m.Measure("new_order", 5*time.Millisecond, nil)
	m.Output(false, util.OutputStylePlain, noopRender)
	rows = readCSV(t, path)
	// header + 2 rows for tick 1 + 2 rows for tick 2. Tick 2 recorded only a
	// success, but the error row is still written, with count 0.
	if len(rows) != 5 {
		t.Fatalf("after tick 2: want 5 rows, got %d: %v", len(rows), rows)
	}

	// t_seconds must be non-decreasing across ticks and parse as a float.
	var elapsed []float64
	for _, r := range rows[1:] {
		v, err := strconv.ParseFloat(r[0], 64)
		if err != nil {
			t.Fatalf("t_seconds %q not a float: %v", r[0], err)
		}
		elapsed = append(elapsed, v)
	}
	for i := 1; i < len(elapsed); i++ {
		if elapsed[i] < elapsed[i-1] {
			t.Errorf("t_seconds went backwards: %v", elapsed)
		}
	}

	// Finalizing must close the file without losing anything written so far.
	m.Output(true, util.OutputStylePlain, noopRender)
	rows = readCSV(t, path)
	if len(rows) != 5 {
		t.Fatalf("after finalize: want 5 rows still, got %d: %v", len(rows), rows)
	}
}

func TestSummary_SplitsStatusAndSortsByName(t *testing.T) {
	m := NewMeasurement()
	m.Measure("payment", time.Millisecond, nil)
	m.Measure("new_order", time.Millisecond, nil)
	m.Measure("new_order", time.Millisecond, errors.New("boom"))

	summary := m.Summary()
	if len(summary) != 3 {
		t.Fatalf("want 3 entries (new_order ok/error, payment ok), got %d: %+v", len(summary), summary)
	}
	// Sorted by raw op key (new_order, new_order_ERR, payment) before
	// upper-casing -- so NEW_ORDER (ok) sorts before NEW_ORDER (error).
	if summary[0].Transaction != "NEW_ORDER" || summary[0].Status != "ok" {
		t.Errorf("summary[0] = %+v, want NEW_ORDER/ok", summary[0])
	}
	if summary[1].Transaction != "NEW_ORDER" || summary[1].Status != "error" {
		t.Errorf("summary[1] = %+v, want NEW_ORDER/error", summary[1])
	}
	if summary[2].Transaction != "PAYMENT" || summary[2].Status != "ok" {
		t.Errorf("summary[2] = %+v, want PAYMENT/ok", summary[2])
	}

	data, err := json.Marshal(summary)
	if err != nil {
		t.Fatalf("marshal summary: %v", err)
	}
	if !strings.Contains(string(data), `"transaction":"NEW_ORDER"`) {
		t.Errorf("marshaled summary missing expected field: %s", data)
	}
}

func TestFreeze_FixesSummaryElapsedAcrossRepeatedCalls(t *testing.T) {
	m := NewMeasurement()
	m.Measure("new_order", time.Millisecond, nil)
	m.Measure("payment", time.Millisecond, nil)

	time.Sleep(5 * time.Millisecond)
	m.Freeze(time.Now())

	before := m.Summary()
	time.Sleep(5 * time.Millisecond)
	after := m.Summary()

	if len(before) != len(after) {
		t.Fatalf("summary length changed: before=%d after=%d", len(before), len(after))
	}
	for i := range before {
		if before[i].TakesSeconds != after[i].TakesSeconds {
			t.Errorf("%s: TakesSeconds drifted after freeze: before=%v after=%v",
				before[i].Transaction, before[i].TakesSeconds, after[i].TakesSeconds)
		}
	}
}

func readCSV(t *testing.T, path string) [][]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatalf("read csv %s: %v", path, err)
	}
	return rows
}

func TestInterval_ElapsedSpansWholeTick(t *testing.T) {
	const idle = 60 * time.Millisecond

	m := NewMeasurement()
	// Nothing happens for a while, then a single transaction lands just before
	// the tick is flushed. The window is still the whole tick.
	time.Sleep(idle)
	m.Measure("new_order", time.Millisecond, nil)

	var tick map[string]*Histogram
	m.Output(false, util.OutputStylePlain, captureRender(&tick))

	info := tick["new_order"].GetInfo()
	if info.Elapsed < idle.Seconds() {
		t.Fatalf("Elapsed = %vs, want at least %vs: the window must start at the previous flush, not at the first transaction",
			info.Elapsed, idle.Seconds())
	}
	if want := float64(info.Count) / info.Elapsed; info.Ops != want {
		t.Errorf("Ops = %v, want %v", info.Ops, want)
	}
}

func TestInterval_WindowsAreContiguous(t *testing.T) {
	const gap = 30 * time.Millisecond

	t0 := time.Now()
	m := NewMeasurement()
	var tick map[string]*Histogram
	render := captureRender(&tick)

	m.Measure("new_order", time.Millisecond, nil)
	m.Output(false, util.OutputStylePlain, render)
	t1 := time.Now()
	first := tick["new_order"].GetInfo().Elapsed

	time.Sleep(gap)
	m.Measure("new_order", time.Millisecond, nil)
	m.Output(false, util.OutputStylePlain, render)
	t2 := time.Now()
	second := tick["new_order"].GetInfo().Elapsed

	if first > t1.Sub(t0).Seconds() {
		t.Errorf("first window %vs exceeds the wall time before the first flush (%vs)", first, t1.Sub(t0).Seconds())
	}
	if second < gap.Seconds() {
		t.Errorf("second window = %vs, want at least the %vs gap between flushes", second, gap.Seconds())
	}
	if second > t2.Sub(t1).Seconds() {
		t.Errorf("second window %vs exceeds the wall time between flushes (%vs)", second, t2.Sub(t1).Seconds())
	}
	// Contiguous, not overlapping: the two windows together cover no more than
	// the whole run.
	if total := t2.Sub(t0).Seconds(); first+second > total {
		t.Errorf("windows overlap: %vs + %vs > %vs", first, second, total)
	}
}

func TestInterval_IdleTickWritesZeroRowToCSVOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raw.csv")
	m := NewMeasurement(WithRawSamplesFile(path))

	m.Measure("new_order", 10*time.Millisecond, nil)
	m.Output(false, util.OutputStylePlain, noopRender)
	busy := len(readCSV(t, path))

	// Second tick records nothing at all.
	var tick map[string]*Histogram
	m.Output(false, util.OutputStylePlain, captureRender(&tick))

	rows := readCSV(t, path)
	if len(rows) != busy+2 {
		t.Fatalf("idle tick wrote %d rows, want 2 (ok and error for new_order): %v", len(rows)-busy, rows)
	}
	for _, r := range rows[busy:] {
		if r[1] != "NEW_ORDER" {
			t.Errorf("transaction = %q, want NEW_ORDER", r[1])
		}
		if r[3] != "0" {
			t.Errorf("count = %q, want 0", r[3])
		}
		if r[4] != "0.0" {
			t.Errorf("tpm = %q, want 0.0", r[4])
		}
	}

	// The command line stays quiet: renderers skip empty histograms, so the
	// zero rows reach the raw-samples file only.
	hist, ok := tick["new_order"]
	if !ok {
		t.Fatal("idle tick did not carry new_order forward")
	}
	if !hist.Empty() {
		t.Error("idle tick histogram is not empty, so it would be rendered to the command line")
	}
}

func TestInterval_TpmIsCountOverTheTick(t *testing.T) {
	m := NewMeasurement()
	for i := 0; i < 5; i++ {
		m.Measure("new_order", time.Millisecond, nil)
	}
	time.Sleep(20 * time.Millisecond)

	var tick map[string]*Histogram
	m.Output(false, util.OutputStylePlain, captureRender(&tick))

	info := tick["new_order"].GetInfo()
	if info.Count != 5 {
		t.Fatalf("Count = %d, want 5", info.Count)
	}
	tpm := info.Ops * 60
	if want := float64(info.Count) * 60 / info.Elapsed; math.Abs(tpm-want) > 1e-9*want {
		t.Errorf("tpm = %v, want %v", tpm, want)
	}

	// An empty histogram must report zero, never NaN or +Inf.
	errInfo := tick["new_order_ERR"].GetInfo()
	if errInfo.Ops != 0 {
		t.Errorf("empty histogram Ops = %v, want 0", errInfo.Ops)
	}
}

func TestInterval_ConcurrentMeasureAndOutput(t *testing.T) {
	m := NewMeasurement(WithRawSamplesFile(filepath.Join(t.TempDir(), "raw.csv")))

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var recordErr error
			if i%2 == 0 {
				recordErr = errors.New("boom")
			}
			for {
				select {
				case <-stop:
					return
				default:
					m.Measure("new_order", time.Millisecond, recordErr)
					m.Measure("payment", time.Millisecond, nil)
				}
			}
		}(i)
	}
	for i := 0; i < 100; i++ {
		m.Output(false, util.OutputStylePlain, noopRender)
	}
	close(stop)
	wg.Wait()
	m.Output(true, util.OutputStylePlain, noopRender)
}
