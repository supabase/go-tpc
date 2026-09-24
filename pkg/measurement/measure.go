package measurement

import (
	"encoding/csv"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/supabase/go-tpc/pkg/util"
)

const (
	sigFigs           = 1
	defaultMinLatency = 1 * time.Millisecond
	DefaultMaxLatency = 16 * time.Second
)

// column set for the --raw-samples-file CSV.
var rawSamplesHeader = []string{
	"t_seconds", "transaction", "status", "count", "tpm",
	"avg_latency_ms", "p50_latency_ms", "p90_latency_ms", "p95_latency_ms",
	"p99_latency_ms", "p99_9_latency_ms", "max_latency_ms",
}

type Measurement struct {
	warmUp int32 // use as bool, 1 means in warmup progress, 0 means warmup finished.
	sync.RWMutex

	MinLatency       time.Duration
	MaxLatency       time.Duration
	SigFigs          int
	OpCurMeasurement map[string]*Histogram
	OpSumMeasurement map[string]*Histogram

	// startTime anchors t_seconds in the raw-samples file.
	startTime time.Time
	// curStartTime is the instant the current interval window opened, i.e. the
	// previous flush. Interval histograms are pinned to
	// [curStartTime, flush instant] so their Ops covers the whole tick.
	curStartTime time.Time

	rawSamplesFile string
	rawMu          sync.Mutex
	rawFile        *os.File
	rawWriter      *csv.Writer
}

// OpSummary is one operation's cumulative, structured summary
type OpSummary struct {
	Transaction   string  `json:"transaction"`
	Status        string  `json:"status"`
	Count         int64   `json:"count"`
	TPM           float64 `json:"tpm"`
	TakesSeconds  float64 `json:"takes_s"`
	AvgLatencyMs  float64 `json:"avg_latency_ms"`
	P50LatencyMs  float64 `json:"p50_latency_ms"`
	P90LatencyMs  float64 `json:"p90_latency_ms"`
	P95LatencyMs  float64 `json:"p95_latency_ms"`
	P99LatencyMs  float64 `json:"p99_latency_ms"`
	P999LatencyMs float64 `json:"p99_9_latency_ms"`
	MaxLatencyMs  float64 `json:"max_latency_ms"`
}

// splitOpStatus strips the _ERR suffix getHist pairs every operation with,
// returning (transaction name, "ok"|"error").
func splitOpStatus(op string) (name string, status string) {
	if strings.HasSuffix(op, "_ERR") {
		return op[:len(op)-4], "error"
	}
	return op, "ok"
}

// WithRawSamplesFile makes Output(false, ...) append one row per (tick,
// operation, status) to path
func WithRawSamplesFile(path string) func(*Measurement) {
	return func(m *Measurement) { m.rawSamplesFile = path }
}

// opMeasurementLocked returns the interval or cumulative map. Callers must hold
// m's read or write lock: takeCurMeasurement replaces OpCurMeasurement outright,
// so reading the field itself has to be synchronised.
func (m *Measurement) opMeasurementLocked(current bool) map[string]*Histogram {
	if current {
		return m.OpCurMeasurement
	}
	return m.OpSumMeasurement
}

func (m *Measurement) getHist(op string, err error, current bool) *Histogram {
	// Create hist of {op} and {op}_ERR at the same time, or else the TPM would be incorrect
	opPairedKey := fmt.Sprintf("%s_ERR", op)
	if err != nil {
		op, opPairedKey = opPairedKey, op
	}

	m.RLock()
	opM, ok := m.opMeasurementLocked(current)[op]
	m.RUnlock()
	if ok {
		return opM
	}

	m.Lock()
	defer m.Unlock()
	opMeasurement := m.opMeasurementLocked(current)
	if opM, ok = opMeasurement[op]; ok {
		return opM
	}
	opM = NewHistogram(m.MinLatency, m.MaxLatency, m.SigFigs)
	opMeasurement[op] = opM
	if _, ok := opMeasurement[opPairedKey]; !ok {
		opMeasurement[opPairedKey] = NewHistogram(m.MinLatency, m.MaxLatency, m.SigFigs)
	}
	return opM
}

func (m *Measurement) measure(op string, err error, lan time.Duration) {
	m.getHist(op, err, true).Measure(lan)
	m.getHist(op, err, false).Measure(lan)
}

// takeCurMeasurement detaches the current interval's histograms and opens the
// next window at now. Each detached histogram is pinned to the window it
// actually covers, so Ops is count over the full tick rather than count over
// the span since the tick's first transaction.
//
// The next window is seeded with a fresh histogram for every operation seen so
// far, so a tick with no transactions still reports a zero row instead of
// disappearing from the series. getHist only ever adds an operation once it
// records something (and always adds its _ERR twin alongside), so seeding from
// every key in OpSumMeasurement gives each operation the same set of rows in
// every tick from the one it first appears in.
func (m *Measurement) takeCurMeasurement(now time.Time) map[string]*Histogram {
	m.Lock()
	tick := m.OpCurMeasurement
	start := m.curStartTime
	m.curStartTime = now

	next := make(map[string]*Histogram, len(m.OpSumMeasurement))
	for op := range m.OpSumMeasurement {
		next[op] = NewHistogram(m.MinLatency, m.MaxLatency, m.SigFigs)
	}
	m.OpCurMeasurement = next
	m.Unlock()

	for _, hist := range tick {
		hist.SetWindow(start, now)
	}
	return tick
}

// Freeze fixes now as the stop instant for every histogram in
// OpSumMeasurement, so a final summary report (stdout and --summary-file)
// computes Elapsed/Ops from the exact moment the run stopped rather than a
// fresh, drifting time.Now() per call.
func (m *Measurement) Freeze(now time.Time) {
	m.RLock()
	defer m.RUnlock()
	for _, h := range m.OpSumMeasurement {
		h.Freeze(now)
	}
}

func (m *Measurement) getOpName() []string {
	m.RLock()
	defer m.RUnlock()

	res := make([]string, 0, len(m.OpSumMeasurement))
	for op := range m.OpSumMeasurement {
		res = append(res, op)
	}
	return res
}

// CurrentPrefix and SummaryPrefix are the prefixes Output passes to
// outputFunc, letting a renderer tell a periodic tick apart from the final
// summary -- e.g. to suppress the tick's own printing in a non-interactive
// terminal while leaving unrelated per-tick side effects (Prometheus gauge
// updates, the raw-samples-file row below) unconditional.
const (
	CurrentPrefix = "[Current] "
	SummaryPrefix = "[Summary] "
)

// Output always hands outputFunc the tick (or final summary), whether or not
// it prints anything: outputFunc may have side effects beyond rendering to
// stdout (see CurrentPrefix), and the raw-samples-file append is likewise
// unconditional regardless of what outputFunc does with the data.
func (m *Measurement) Output(ifSummaryReport bool, outputStyle string, outputFunc func(string, string, map[string]*Histogram)) {
	if ifSummaryReport {
		m.RLock()
		defer m.RUnlock()
		outputFunc(outputStyle, SummaryPrefix, m.OpSumMeasurement)
		if err := m.closeRawSamples(); err != nil {
			fmt.Fprintf(os.Stderr, "raw samples file: %v\n", err)
		}
		return
	}
	// Clear current measure data every time
	now := time.Now()
	tick := m.takeCurMeasurement(now)
	// The renderers skip empty histograms, so the zero-count entries seeded by
	// takeCurMeasurement reach the raw-samples file only.
	outputFunc(outputStyle, CurrentPrefix, tick)
	if err := m.appendRawSamples(now, tick); err != nil {
		fmt.Fprintf(os.Stderr, "raw samples file: %v\n", err)
	}
}

// appendRawSamples writes one row per (operation, status) in this tick's
// measurements to rawSamplesFile, flushing immediately after. Operations that
// recorded nothing this tick are written with count 0 and tpm 0 so the file is
// a regular time series: every tick contributes a row for every operation seen
// so far, and averaging the tpm column does not silently drop idle ticks.
func (m *Measurement) appendRawSamples(now time.Time, tick map[string]*Histogram) error {
	if m.rawSamplesFile == "" {
		return nil
	}
	elapsed := now.Sub(m.startTime).Seconds()

	keys := make([]string, 0, len(tick))
	for op := range tick {
		keys = append(keys, op)
	}
	sort.Strings(keys)

	m.rawMu.Lock()
	defer m.rawMu.Unlock()
	if m.rawWriter == nil {
		if err := m.openRawWriterLocked(); err != nil {
			return err
		}
	}
	for _, op := range keys {
		info := tick[op].GetInfo()
		name, status := splitOpStatus(op)
		row := []string{
			util.FloatToOneString(elapsed),
			strings.ToUpper(name),
			status,
			util.IntToString(info.Count),
			util.FloatToOneString(info.Ops * 60),
			util.FloatToOneString(info.Avg),
			util.FloatToOneString(info.P50),
			util.FloatToOneString(info.P90),
			util.FloatToOneString(info.P95),
			util.FloatToOneString(info.P99),
			util.FloatToOneString(info.P999),
			util.FloatToOneString(info.Max),
		}
		if err := m.rawWriter.Write(row); err != nil {
			return fmt.Errorf("write raw sample row: %w", err)
		}
	}
	m.rawWriter.Flush()
	return m.rawWriter.Error()
}

// openRawWriterLocked creates rawSamplesFile and writes its header. Callers
// must hold rawMu.
func (m *Measurement) openRawWriterLocked() error {
	f, err := os.Create(m.rawSamplesFile)
	if err != nil {
		return fmt.Errorf("create raw samples file: %w", err)
	}
	w := csv.NewWriter(f)
	if err := w.Write(rawSamplesHeader); err != nil {
		f.Close() //nolint:errcheck
		return fmt.Errorf("write raw samples header: %w", err)
	}
	w.Flush()
	if err := w.Error(); err != nil {
		f.Close() //nolint:errcheck
		return err
	}
	m.rawFile = f
	m.rawWriter = w
	return nil
}

// closeRawSamples flushes and closes the raw-samples file, if one was ever
// opened. Safe to call even when rawSamplesFile is "".
func (m *Measurement) closeRawSamples() error {
	m.rawMu.Lock()
	defer m.rawMu.Unlock()
	if m.rawFile == nil {
		return nil
	}
	m.rawWriter.Flush()
	err := m.rawWriter.Error()
	if cerr := m.rawFile.Close(); err == nil {
		err = cerr
	}
	m.rawFile = nil
	m.rawWriter = nil
	return err
}

// Summary returns a structured, sorted snapshot of every non-empty
// operation in OpSumMeasurement. Intended for a workload's
// OutputStats(true) to embed in its own --summary-file JSON document.
func (m *Measurement) Summary() []OpSummary {
	m.RLock()
	defer m.RUnlock()

	keys := make([]string, 0, len(m.OpSumMeasurement))
	for op := range m.OpSumMeasurement {
		keys = append(keys, op)
	}
	sort.Strings(keys)

	out := make([]OpSummary, 0, len(keys))
	for _, op := range keys {
		hist := m.OpSumMeasurement[op]
		if hist.Empty() {
			continue
		}
		info := hist.GetInfo()
		name, status := splitOpStatus(op)
		out = append(out, OpSummary{
			Transaction:   strings.ToUpper(name),
			Status:        status,
			Count:         info.Count,
			TPM:           info.Ops * 60,
			TakesSeconds:  info.Elapsed,
			AvgLatencyMs:  info.Avg,
			P50LatencyMs:  info.P50,
			P90LatencyMs:  info.P90,
			P95LatencyMs:  info.P95,
			P99LatencyMs:  info.P99,
			P999LatencyMs: info.P999,
			MaxLatencyMs:  info.Max,
		})
	}
	return out
}

// EnableWarmUp sets whether to enable warm-up.
func (m *Measurement) EnableWarmUp(b bool) {
	if b {
		atomic.StoreInt32(&m.warmUp, 1)
	} else {
		atomic.StoreInt32(&m.warmUp, 0)
	}
}

// IsWarmUpFinished returns whether warm-up is finished or not.
func (m *Measurement) IsWarmUpFinished() bool {
	return atomic.LoadInt32(&m.warmUp) == 0
}

// Measure measures the operation.
func (m *Measurement) Measure(op string, lan time.Duration, err error) {
	if !m.IsWarmUpFinished() {
		return
	}
	m.measure(op, err, lan)
}

func NewMeasurement(opts ...func(*Measurement)) *Measurement {
	m := &Measurement{
		warmUp:           0,
		RWMutex:          sync.RWMutex{},
		MinLatency:       defaultMinLatency,
		MaxLatency:       DefaultMaxLatency,
		SigFigs:          sigFigs,
		OpCurMeasurement: make(map[string]*Histogram, 16),
		OpSumMeasurement: make(map[string]*Histogram, 16),
	}
	m.startTime = time.Now()
	m.curStartTime = m.startTime
	for _, opt := range opts {
		if opt != nil {
			opt(m)
		}
	}
	return m
}
