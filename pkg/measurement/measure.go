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

func (m *Measurement) getHist(op string, err error, current bool) *Histogram {
	opMeasurement := m.OpSumMeasurement
	if current {
		opMeasurement = m.OpCurMeasurement
	}

	// Create hist of {op} and {op}_ERR at the same time, or else the TPM would be incorrect
	opPairedKey := fmt.Sprintf("%s_ERR", op)
	if err != nil {
		op, opPairedKey = opPairedKey, op
	}

	m.RLock()
	opM, ok := opMeasurement[op]
	m.RUnlock()
	if !ok {
		opM = NewHistogram(m.MinLatency, m.MaxLatency, m.SigFigs)
		opPairedM := NewHistogram(m.MinLatency, m.MaxLatency, m.SigFigs)
		m.Lock()
		opMeasurement[op] = opM
		opMeasurement[opPairedKey] = opPairedM
		m.Unlock()
	}
	return opM
}

func (m *Measurement) measure(op string, err error, lan time.Duration) {
	m.getHist(op, err, true).Measure(lan)
	m.getHist(op, err, false).Measure(lan)
}

func (m *Measurement) takeCurMeasurement() (ret map[string]*Histogram) {
	m.RLock()
	defer m.RUnlock()
	ret, m.OpCurMeasurement = m.OpCurMeasurement, make(map[string]*Histogram, 16)
	return
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

// Output prints the measurement summary.
func (m *Measurement) Output(ifSummaryReport bool, outputStyle string, outputFunc func(string, string, map[string]*Histogram)) {
	if ifSummaryReport {
		m.RLock()
		defer m.RUnlock()
		outputFunc(outputStyle, "[Summary] ", m.OpSumMeasurement)
		if err := m.closeRawSamples(); err != nil {
			fmt.Fprintf(os.Stderr, "raw samples file: %v\n", err)
		}
		return
	}
	// Clear current measure data every time
	var opCurMeasurement = m.takeCurMeasurement()
	m.RLock()
	defer m.RUnlock()
	outputFunc(outputStyle, "[Current] ", opCurMeasurement)
	if err := m.appendRawSamples(opCurMeasurement); err != nil {
		fmt.Fprintf(os.Stderr, "raw samples file: %v\n", err)
	}
}

// appendRawSamples writes one row per non-empty (operation, status) in this
// tick's measurements to rawSamplesFile, flushing immediately after.
func (m *Measurement) appendRawSamples(tick map[string]*Histogram) error {
	if m.rawSamplesFile == "" {
		return nil
	}
	elapsed := time.Since(m.startTime).Seconds()

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
		hist := tick[op]
		if hist.Empty() {
			continue
		}
		info := hist.GetInfo()
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
		startTime:        time.Now(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(m)
		}
	}
	return m
}
