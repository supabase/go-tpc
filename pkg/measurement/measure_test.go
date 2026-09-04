package measurement

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/supabase/go-tpc/pkg/util"
)

func noopRender(string, string, map[string]*Histogram) {}

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
	if len(rows) != 4 { // header + 3 data rows across both ticks
		t.Fatalf("after tick 2: want 4 rows, got %d: %v", len(rows), rows)
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
	if len(rows) != 4 {
		t.Fatalf("after finalize: want 4 rows still, got %d: %v", len(rows), rows)
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
