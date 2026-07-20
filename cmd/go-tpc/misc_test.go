package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/supabase/go-tpc/pkg/workload"
)

// fakeWorkloader is a minimal workload.Workloader for exercising
// checkPrepare/executeWorkload without a real database.
type fakeWorkloader struct {
	name string

	prepareErr      map[int]error
	checkPrepareErr map[int]error
	runErr          map[int]error
	cleanupErr      map[int]error
	checkErr        map[int]error

	mu                 sync.Mutex
	checkPrepareCalled bool
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
