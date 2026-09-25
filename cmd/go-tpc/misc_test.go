package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/supabase/go-tpc/pkg/workload"
)

// ---- backoff tests ----

func TestBackoffDelay_StaysWithinExpectedBound(t *testing.T) {
	cases := []struct {
		consecutiveFailures int
		wantMax             time.Duration
	}{
		{1, backoffBase},
		{2, 2 * backoffBase},
		{3, 4 * backoffBase},
		{20, backoffMax}, // large enough to exercise the overflow guard
	}

	for _, c := range cases {
		for i := 0; i < 50; i++ { // sample the jitter range
			d := backoffDelay(c.consecutiveFailures)
			if d < 0 {
				t.Fatalf("backoffDelay(%d) = %v, want >= 0", c.consecutiveFailures, d)
			}
			if d > c.wantMax {
				t.Fatalf("backoffDelay(%d) = %v, want <= %v", c.consecutiveFailures, d, c.wantMax)
			}
		}
	}
}

func TestWaitForBackoff_ReturnsFalsePromptlyWhenCtxDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan bool, 1)
	go func() { done <- waitForBackoff(ctx, 20) }() // consecutiveFailures=20 would otherwise sleep up to backoffMax

	select {
	case ok := <-done:
		if ok {
			t.Error("waitForBackoff() = true, want false for an already-canceled context")
		}
	case <-time.After(time.Second):
		t.Fatal("waitForBackoff did not return promptly for an already-canceled context")
	}
}

func TestWaitForBackoff_TrueWhenDelayElapsesBeforeCtxDone(t *testing.T) {
	if ok := waitForBackoff(context.Background(), 1); !ok {
		t.Error("waitForBackoff() = false, want true when ctx never ends")
	}
}

// countingFailThenSucceed fails its first failCount Run calls, then succeeds.
type countingFailThenSucceed struct {
	fakeWorkloader
	failCount int

	mu    sync.Mutex
	calls int
}

func (f *countingFailThenSucceed) Run(ctx context.Context, threadID int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.calls <= f.failCount {
		return errors.New("transient failure")
	}
	return nil
}

func TestExecute_BacksOffBetweenRetriesAndRecoversOnSuccess(t *testing.T) {
	withGlobals(t, 1, 3, true, true, 10*time.Second, false) // ignoreError=true, txnTimeout unset (0) so failures take the generic-error path

	fake := &countingFailThenSucceed{fakeWorkloader: fakeWorkloader{name: "faketest"}, failCount: 2}

	err := execute(context.Background(), fake, "run", threads, 0)
	if err != nil {
		t.Fatalf("execute() = %v, want nil (ignoreError=true should let it recover)", err)
	}
	// 2 failures + 1 success = 3 calls, matching count = totalCount/threads = 3.
	// This exercises the full path through waitForBackoff on both failures
	// without hanging (backoffBase is small), and confirms consecutiveFailures
	// resetting on success didn't skip or repeat an iteration.
	if fake.calls != 3 {
		t.Errorf("Run() called %d times, want 3", fake.calls)
	}
}

// fakeWorkloader is a minimal workload.Workloader for exercising
// checkPrepare/executeWorkload without a real database.
type fakeWorkloader struct {
	name string

	prepareErr      map[int]error
	checkPrepareErr map[int]error
	runErr          map[int]error
	cleanupErr      map[int]error
	checkErr        map[int]error
	analyzeErr      error

	mu                 sync.Mutex
	checkPrepareCalled bool
	// order records the post-prepare steps in the order they ran.
	order []string
}

func (f *fakeWorkloader) record(step string) {
	f.mu.Lock()
	f.order = append(f.order, step)
	f.mu.Unlock()
}

func (f *fakeWorkloader) steps() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.order...)
}

func (f *fakeWorkloader) AnalyzeTables(context.Context) error {
	f.record("analyze")
	return f.analyzeErr
}

var _ workload.Workloader = (*fakeWorkloader)(nil)

func (f *fakeWorkloader) Name() string                                          { return f.name }
func (f *fakeWorkloader) InitThread(ctx context.Context, _ int) context.Context { return ctx }
func (f *fakeWorkloader) CleanupThread(context.Context, int)                    {}
func (f *fakeWorkloader) Prepare(_ context.Context, threadID int) error {
	return f.prepareErr[threadID]
}
func (f *fakeWorkloader) CheckPrepare(_ context.Context, threadID int) error {
	f.mu.Lock()
	f.checkPrepareCalled = true
	f.mu.Unlock()
	f.record("checkPrepare")
	return f.checkPrepareErr[threadID]
}
func (f *fakeWorkloader) Run(_ context.Context, threadID int) error { return f.runErr[threadID] }
func (f *fakeWorkloader) Cleanup(_ context.Context, threadID int) error {
	return f.cleanupErr[threadID]
}
func (f *fakeWorkloader) Check(_ context.Context, threadID int) error { return f.checkErr[threadID] }
func (f *fakeWorkloader) OutputStats(bool)                            {}
func (f *fakeWorkloader) DBName() string                              { return "test" }
func (f *fakeWorkloader) IsPlanReplayerDumpEnabled() bool             { return false }
func (f *fakeWorkloader) PreparePlanReplayerDump() error              { return nil }
func (f *fakeWorkloader) FinishPlanReplayerDump() error               { return nil }
func (f *fakeWorkloader) Exec(string) error                           { return nil }

// withGlobals sets the package-level flags executeWorkload/execute/checkPrepare read,
// and restores the previous values when the test finishes. Not safe for t.Parallel().
func withGlobals(t *testing.T, th, tc int, sil, ignore bool, interval time.Duration, drop bool) {
	t.Helper()
	origThreads, origTotalCount, origSilence, origIgnoreError, origOutputInterval, origDropData :=
		threads, totalCount, silence, ignoreError, outputInterval, dropData
	t.Cleanup(func() {
		threads, totalCount, silence, ignoreError, outputInterval, dropData =
			origThreads, origTotalCount, origSilence, origIgnoreError, origOutputInterval, origDropData
	})
	threads, totalCount, silence, ignoreError, outputInterval, dropData = th, tc, sil, ignore, interval, drop
}

// withTpccPrepareFlags sets the tpcc prepare-only flags analyzePrepared and
// checkPrepare read, and restores them when the test finishes.
func withTpccPrepareFlags(t *testing.T, analyze, noCheck bool) {
	t.Helper()
	origAnalyze, origNoCheck := tpccConfig.Analyze, tpccConfig.NoCheck
	t.Cleanup(func() {
		tpccConfig.Analyze, tpccConfig.NoCheck = origAnalyze, origNoCheck
	})
	tpccConfig.Analyze, tpccConfig.NoCheck = analyze, noCheck
}

func TestExecuteWorkload_PrepareAnalyzeOrderAndGating(t *testing.T) {
	cases := []struct {
		desc      string
		name      string
		analyze   bool
		noCheck   bool
		wantSteps []string
	}{
		{"analyze runs before the check", "tpcc", true, false, []string{"analyze", "checkPrepare"}},
		{"no analyze without the flag", "tpcc", false, false, []string{"checkPrepare"}},
		{"--no-check skips only the check", "tpcc", true, true, []string{"analyze"}},
		{"csv output never analyzes", "tpcc-csv", true, false, nil},
	}

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			withGlobals(t, 1, 0, true, false, 10*time.Second, false)
			withTpccPrepareFlags(t, c.analyze, c.noCheck)

			fake := &fakeWorkloader{name: c.name}
			if err := executeWorkload(context.Background(), fake, threads, "prepare"); err != nil {
				t.Fatalf("executeWorkload(prepare) = %v, want nil", err)
			}

			got := fake.steps()
			if len(got) != len(c.wantSteps) {
				t.Fatalf("steps = %v, want %v", got, c.wantSteps)
			}
			for i := range got {
				if got[i] != c.wantSteps[i] {
					t.Fatalf("steps = %v, want %v", got, c.wantSteps)
				}
			}
		})
	}
}

func TestExecuteWorkload_AnalyzeErrorSkipsCheckPrepare(t *testing.T) {
	withGlobals(t, 1, 0, true, false, 10*time.Second, false)
	withTpccPrepareFlags(t, true, false)

	wantErr := errors.New("analyze boom")
	fake := &fakeWorkloader{name: "tpcc", analyzeErr: wantErr}

	err := executeWorkload(context.Background(), fake, threads, "prepare")
	if !errors.Is(err, wantErr) {
		t.Fatalf("executeWorkload(prepare) = %v, want %v", err, wantErr)
	}
	if fake.checkPrepareCalled {
		t.Fatal("checkPrepare should be skipped when analyze failed")
	}
}

func TestCheckPrepare_NoErrorsReturnsPromptly(t *testing.T) {
	withGlobals(t, 4, 0, true, false, 10*time.Second, false)

	fake := &fakeWorkloader{name: "faketest"}

	done := make(chan error, 1)
	go func() {
		done <- checkPrepare(context.Background(), fake)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("checkPrepare() = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("checkPrepare deadlocked with zero errors")
	}
}

func TestCheckPrepare_ReturnsWorkerError(t *testing.T) {
	withGlobals(t, 4, 0, true, false, 10*time.Second, false)

	wantErr := errors.New("check prepare boom")
	fake := &fakeWorkloader{name: "faketest", checkPrepareErr: map[int]error{0: wantErr}}

	if err := checkPrepare(context.Background(), fake); !errors.Is(err, wantErr) {
		t.Fatalf("checkPrepare() = %v, want %v", err, wantErr)
	}
}

func TestExecuteWorkload_PrepareWorkerErrorSkipsCheckPrepare(t *testing.T) {
	withGlobals(t, 2, 0, true, false, 10*time.Second, false)

	wantErr := errors.New("prepare boom")
	fake := &fakeWorkloader{name: "faketest", prepareErr: map[int]error{0: wantErr}}

	err := executeWorkload(context.Background(), fake, threads, "prepare")
	if !errors.Is(err, wantErr) {
		t.Fatalf("executeWorkload(prepare) = %v, want %v", err, wantErr)
	}
	if fake.checkPrepareCalled {
		t.Fatal("checkPrepare should be skipped when a prepare worker already failed")
	}
}

func TestExecuteWorkload_PrepareSuccessRunsCheckPrepare(t *testing.T) {
	withGlobals(t, 2, 0, true, false, 10*time.Second, false)

	wantErr := errors.New("inconsistent data")
	fake := &fakeWorkloader{name: "faketest", checkPrepareErr: map[int]error{0: wantErr}}

	err := executeWorkload(context.Background(), fake, threads, "prepare")
	if !errors.Is(err, wantErr) {
		t.Fatalf("executeWorkload(prepare) = %v, want %v", err, wantErr)
	}
	if !fake.checkPrepareCalled {
		t.Fatal("expected checkPrepare to run when every prepare worker succeeded")
	}
}

func TestExecuteWorkload_WorkerErrorPropagatesForAllActions(t *testing.T) {
	withGlobals(t, 1, 1, true, false, 10*time.Second, false)

	cases := []struct {
		action string
		setErr func(f *fakeWorkloader, err error)
	}{
		{"run", func(f *fakeWorkloader, err error) { f.runErr = map[int]error{0: err} }},
		{"cleanup", func(f *fakeWorkloader, err error) { f.cleanupErr = map[int]error{0: err} }},
		{"check", func(f *fakeWorkloader, err error) { f.checkErr = map[int]error{0: err} }},
	}

	for _, tc := range cases {
		t.Run(tc.action, func(t *testing.T) {
			wantErr := errors.New("boom-" + tc.action)
			fake := &fakeWorkloader{name: "faketest"}
			tc.setErr(fake, wantErr)

			err := executeWorkload(context.Background(), fake, threads, tc.action)
			if !errors.Is(err, wantErr) {
				t.Fatalf("executeWorkload(%q) = %v, want %v", tc.action, err, wantErr)
			}
		})
	}
}

// ---- refactored execute helper tests ----

// withTxnTimeout sets the txnTimeout global runTransaction reads, and restores
// it when the test finishes.
func withTxnTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	orig := txnTimeout
	t.Cleanup(func() { txnTimeout = orig })
	txnTimeout = d
}

func TestActionContext(t *testing.T) {
	cases := []struct {
		action        string
		wantCancelled bool
	}{
		{"prepare", false},
		{"cleanup", false},
		{"check", false},
		{"run", true},
	}

	for _, c := range cases {
		t.Run(c.action, func(t *testing.T) {
			timeoutCtx, cancel := context.WithCancel(context.Background())
			cancel()

			got := actionContext(timeoutCtx, c.action)
			if cancelled := got.Err() != nil; cancelled != c.wantCancelled {
				t.Errorf("actionContext(cancelled ctx, %q) cancelled = %v, want %v",
					c.action, cancelled, c.wantCancelled)
			}
		})
	}
}

// blockingRun blocks in Run until its context ends, then returns ctx.Err().
type blockingRun struct {
	fakeWorkloader

	mu    sync.Mutex
	calls int
}

func (b *blockingRun) Run(ctx context.Context, _ int) error {
	b.mu.Lock()
	b.calls++
	b.mu.Unlock()
	<-ctx.Done()
	return ctx.Err()
}

func (b *blockingRun) runCalls() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls
}

func TestRunTransaction_ReportsTxnTimeout(t *testing.T) {
	withGlobals(t, 1, 1, true, true, 10*time.Second, false)
	withTxnTimeout(t, 20*time.Millisecond)

	fake := &blockingRun{fakeWorkloader: fakeWorkloader{name: "faketest"}}

	timedOut, err := runTransaction(context.Background(), fake, 0)
	if !timedOut {
		t.Error("runTransaction() timedOut = false, want true when txnTimeout fires")
	}
	if err == nil {
		t.Error("runTransaction() err = nil, want the context deadline error")
	}
}

func TestRunTransaction_NoTimeoutPassesErrorThrough(t *testing.T) {
	withGlobals(t, 1, 1, true, true, 10*time.Second, false)
	withTxnTimeout(t, 0) // txnTimeout disabled

	wantErr := errors.New("run boom")
	fake := &fakeWorkloader{name: "faketest", runErr: map[int]error{0: wantErr}}

	timedOut, err := runTransaction(context.Background(), fake, 0)
	if timedOut {
		t.Error("runTransaction() timedOut = true, want false when txnTimeout is disabled")
	}
	if !errors.Is(err, wantErr) {
		t.Errorf("runTransaction() err = %v, want %v", err, wantErr)
	}
}

func TestRunLoop_TxnTimeoutsIgnoreIgnoreError(t *testing.T) {
	// ignoreError=false: a per-transaction timeout must still not abort the
	// worker, unlike a generic error.
	withGlobals(t, 1, 2, true, false, 10*time.Second, false)
	withTxnTimeout(t, 20*time.Millisecond)

	fake := &blockingRun{fakeWorkloader: fakeWorkloader{name: "faketest"}}

	if err := runLoop(context.Background(), fake, "run", 0, 2); err != nil {
		t.Fatalf("runLoop() = %v, want nil (txn timeouts are not fatal)", err)
	}
	if got := fake.runCalls(); got != 2 {
		t.Errorf("Run() called %d times, want 2 (the loop should run to completion)", got)
	}
}

func TestRunLoop_ReturnsErrorWhenIgnoreErrorUnset(t *testing.T) {
	withGlobals(t, 1, 5, true, false, 10*time.Second, false)
	withTxnTimeout(t, 0)

	wantErr := errors.New("run boom")
	fake := &fakeWorkloader{name: "faketest", runErr: map[int]error{0: wantErr}}

	if err := runLoop(context.Background(), fake, "run", 0, 5); !errors.Is(err, wantErr) {
		t.Fatalf("runLoop() = %v, want %v on the first failure", err, wantErr)
	}
}

func TestRunLoop_StopsWhenCtxAlreadyDone(t *testing.T) {
	withGlobals(t, 1, 1, true, false, 10*time.Second, false)
	withTxnTimeout(t, 0)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	fake := &blockingRun{fakeWorkloader: fakeWorkloader{name: "faketest"}}

	if err := runLoop(ctx, fake, "run", 0, 0); err != nil { // count=0 would otherwise loop forever
		t.Fatalf("runLoop() = %v, want nil", err)
	}
	if got := fake.runCalls(); got != 0 {
		t.Errorf("Run() called %d times, want 0 for an already-canceled context", got)
	}
}
