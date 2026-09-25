package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/supabase/go-tpc/pkg/util"
	"github.com/supabase/go-tpc/pkg/workload"
)

// timeFormat is the timestamp prefix used on workers" progress and error lines
const timeFormat = "2006-01-02 15:04:05"

// txnThrottleWindow bounds how often the loop-continuing timeout/error lines
// below are logged. With many workers hitting the same error condition around
// the same moment, an unthrottled print would flood the output without
// providing value (the retries themselves back off separately, see
// waitForBackoff).
const txnThrottleWindow = 2 * time.Second

var txnTimeoutThrottle = util.NewLogThrottle(txnThrottleWindow)

// logEvent prints a timestamped line unless --silence is set.
func logEvent(format string, args ...any) {
	if silence {
		return
	}
	fmt.Printf("[%s] %s\n", time.Now().Format(timeFormat), fmt.Sprintf(format, args...))
}

func logThrottledEvent(format string, args ...any) {
	if silence {
		return
	}
	ok, suppressed := txnTimeoutThrottle.Allow()
	if !ok {
		return
	}
	msg := fmt.Sprintf(format, args...)
	if suppressed > 0 {
		msg += fmt.Sprintf(" (+%d more suppressed in the last %v)", suppressed, txnThrottleWindow)
	}
	logEvent("%s", msg)
}

func checkPrepare(ctx context.Context, w workload.Workloader) error {
	// skip preparation check in csv case
	if w.Name() == "tpcc-csv" {
		fmt.Println("Skip preparing checking. Please load CSV data into database and check later.")
		return nil
	}
	if w.Name() == "tpcc" && tpccConfig.NoCheck {
		return nil
	}

	errCh := make(chan error, threads)
	var wg sync.WaitGroup
	wg.Add(threads)
	for i := 0; i < threads; i++ {
		go func(index int) {
			defer wg.Done()

			ctx := w.InitThread(ctx, index)
			defer w.CleanupThread(ctx, index)

			if err := w.CheckPrepare(ctx, index); err != nil {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

// tableAnalyzer is implemented by workloaders that can refresh optimizer
// statistics for the tables they just loaded. tpcc.CSVWorkLoader deliberately
// does not implement it: it writes files instead of loading tables.
type tableAnalyzer interface {
	AnalyzeTables(ctx context.Context) error
}

// analyzePrepared refreshes optimizer statistics after a successful
// `tpcc prepare --analyze`. tpch and ch have their own --analyze, handled
// inside their Prepare.
func analyzePrepared(ctx context.Context, w workload.Workloader) error {
	if w.Name() != "tpcc" || !tpccConfig.Analyze {
		return nil
	}
	a, ok := w.(tableAnalyzer)
	if !ok {
		return nil
	}
	return a.AnalyzeTables(ctx)
}

// analyzeTables refreshes optimizer statistics unconditionally, for the
// standalone `tpcc analyze` command.
func analyzeTables(ctx context.Context, w workload.Workloader) error {
	a, ok := w.(tableAnalyzer)
	if !ok {
		return fmt.Errorf("workload %s cannot analyze tables", w.Name())
	}
	return a.AnalyzeTables(ctx)
}

// actionContext returns the parent context for a worker. Only "run" is bound
// by the benchmark deadline; prepare, cleanup and check must be allowed to
// finish regardless of it.
func actionContext(timeoutCtx context.Context, action string) context.Context {
	switch action {
	case "prepare", "cleanup", "check":
		return context.Background()
	default:
		return timeoutCtx
	}
}

func execute(timeoutCtx context.Context, w workload.Workloader, action string, threads, index int) error {
	ctx := w.InitThread(actionContext(timeoutCtx, action), index)
	defer w.CleanupThread(ctx, index)

	switch action {
	case "prepare":
		// Do cleanup only if dropData is set and not generate csv data.
		if dropData {
			if err := w.Cleanup(ctx, index); err != nil {
				return err
			}
		}
		return w.Prepare(ctx, index)
	case "cleanup":
		return w.Cleanup(ctx, index)
	case "check":
		return w.Check(ctx, index)
	default:
		return runLoop(ctx, w, action, index, totalCount/threads)
	}
}

// runTransaction executes a single transaction, bounding it by txnTimeout when
// set. It reports whether that per-transaction timeout fired, which is distinct
// from the benchmark deadline because runCtx is detached from it.
func runTransaction(runCtx context.Context, w workload.Workloader, index int) (timedOut bool, err error) {
	// Bound how long a single transaction attempt may run, independent of the
	// benchmark deadline, so a hung statement (e.g. blocked on a lock) doesn't
	// run forever now that runCtx is detached from the deadline.
	ctx := runCtx
	if txnTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(runCtx, txnTimeout)
		defer cancel()
	}
	err = w.Run(ctx, index)
	return ctx.Err() != nil, err
}

// runLoop drives the "run" action: it repeats transactions until count is
// reached (count <= 0 means unbounded) or ctx ends.
//
// Transactions already in flight when the deadline hits should be allowed to
// finish (commit or roll back normally) rather than have the driver tear down
// the connection mid-statement. So transactions run on runCtx, detached from
// the deadline; only the loop-boundary check and the ctx.Err() check after Run
// returns use the real, cancelable ctx to decide whether to stop.
func runLoop(ctx context.Context, w workload.Workloader, action string, index, count int) error {
	runCtx := context.WithoutCancel(ctx)

	// consecutiveFailures drives the backoff before the next retry (see
	// waitForBackoff) and resets to 0 the moment a transaction succeeds again.
	consecutiveFailures := 0

	for i := 0; i < count || count <= 0; i++ {
		// Check if timeout has occurred before starting next query
		select {
		case <-ctx.Done():
			logEvent("%s worker %d stopped due to timeout after %d iterations", action, index, i)
			return nil
		default:
		}

		timedOut, err := runTransaction(runCtx, w, index)
		if err == nil {
			consecutiveFailures = 0
			continue
		}

		if ctx.Err() != nil {
			logEvent("%s worker %d stopped due to timeout: %v", action, index, err)
			return nil // Don't treat the benchmark deadline as an error
		}

		consecutiveFailures++
		if timedOut {
			// The transaction's own timeout fired. Treat it the same as a
			// transient conflict or a connection error: this transaction
			// didn't count as completed, but the worker keeps going
			// regardless of --ignore-error.
			logThrottledEvent("%s worker %d transaction timed out after %v, treating as failed, continuing",
				action, index, txnTimeout)
		} else {
			logThrottledEvent("execute %s failed, err %v", action, err)
			if !ignoreError {
				return err
			}
		}

		if !waitForBackoff(ctx, consecutiveFailures) {
			return nil
		}
	}

	return nil
}

const (
	backoffBase = 100 * time.Millisecond
	backoffMax  = 5 * time.Second
)

// backoffDelay returns a jittered exponential backoff delay for the given
// number of consecutive failures (1-indexed), capped at backoffMax. Full
// jitter (a uniform random delay between 0 and the capped exponential value)
// keeps many workers recovering from a shared stall from retrying in lockstep.
func backoffDelay(consecutiveFailures int) time.Duration {
	shift := consecutiveFailures - 1
	if shift > 10 { // backoffBase<<10 already exceeds backoffMax; avoids overflow
		shift = 10
	}
	d := backoffBase << shift
	if d > backoffMax {
		d = backoffMax
	}
	return rand.N(d + 1)
}

// waitForBackoff sleeps for backoffDelay(consecutiveFailures) before a worker
// retries a failed transaction, so a sustained DB stall isn't hammered by
// every worker retrying instantly forever. It returns false if ctx ends
// before the delay elapses, so a backing-off worker doesn't linger past the
// benchmark deadline.
func waitForBackoff(ctx context.Context, consecutiveFailures int) bool {
	select {
	case <-time.After(backoffDelay(consecutiveFailures)):
		return true
	case <-ctx.Done():
		return false
	}
}

func executeWorkload(ctx context.Context, w workload.Workloader, threads int, action string) error {
	var wg sync.WaitGroup
	wg.Add(threads)

	outputCtx, outputCancel := context.WithCancel(ctx)
	ch := make(chan struct{}, 1)
	go func() {
		ticker := time.NewTicker(outputInterval)
		defer ticker.Stop()

		for {
			select {
			case <-outputCtx.Done():
				ch <- struct{}{}
				return
			case <-ticker.C:
				w.OutputStats(false)
			}
		}
	}()
	if w.Name() == "tpch" && action == "run" {
		err := w.Exec(`create or replace view revenue0 (supplier_no, total_revenue) as
	select
		l_suppkey,
		sum(l_extendedprice * (1 - l_discount))
	from
		lineitem
	where
		l_shipdate >= '1997-07-01'
		and l_shipdate < date_add('1997-07-01', interval '3' month)
	group by
		l_suppkey;`)
		if err != nil {
			panic(fmt.Sprintf("a fatal occurred when preparing view data: %v", err))
		}
	}
	// CH benchmark requires the revenue1 view for analytical queries.
	// During normal prepare flow, this view is created in prepareView() method.
	// However, when using CSV data ingestion, the prepare stage is skipped and
	// the view won't exist. So we create it here when action is "run" to ensure
	// the view is available regardless of how data was loaded.
	if w.Name() == "ch" && action == "run" {
		err := w.Exec(`create or replace view revenue1 (supplier_no, total_revenue) as (
    select	mod((s_w_id * s_i_id),10000) as supplier_no,
              sum(ol_amount) as total_revenue
    from	order_line, stock
    where ol_i_id = s_i_id and ol_supply_w_id = s_w_id
      and ol_delivery_d >= '2007-01-02 00:00:00.000000'
    group by mod((s_w_id * s_i_id),10000));`)
		if err != nil {
			panic(fmt.Sprintf("a fatal occurred when preparing view data: %v", err))
		}
	}
	enabledDumpPlanReplayer := w.IsPlanReplayerDumpEnabled()
	if enabledDumpPlanReplayer {
		err := w.PreparePlanReplayerDump()
		if err != nil {
			fmt.Printf("[%s] prepare plan replayer failed, err%v\n",
				time.Now().Format("2006-01-02 15:04:05"), err)
		}
		defer func() {
			err = w.FinishPlanReplayerDump()
			if err != nil {
				fmt.Printf("[%s] dump plan replayer failed, err%v\n",
					time.Now().Format("2006-01-02 15:04:05"), err)
			}
		}()
	}

	errCh := make(chan error, threads)
	for i := 0; i < threads; i++ {
		go func(index int) {
			defer wg.Done()
			if err := execute(ctx, w, action, threads, index); err != nil {
				if !silence {
					fmt.Printf("execute %s failed, err %v\n", action, err)
				}
				errCh <- err
			}
		}(i)
	}

	wg.Wait()

	var workerErr error
	select {
	case err := <-errCh:
		workerErr = err
	default:
	}

	var postPrepareErr error
	if action == "prepare" && workerErr == nil {
		// Only run the post-prepare steps when every prepare worker succeeded;
		// analyzing or checking data a failed worker left incomplete would just
		// produce confusing secondary errors that obscure the real failure.
		//
		// Analyze runs first: the consistency checks are full-table aggregates
		// and a freshly loaded database has no statistics for them yet. It also
		// has to run when --no-check skips the check itself.
		if postPrepareErr = analyzePrepared(ctx, w); postPrepareErr == nil {
			postPrepareErr = checkPrepare(ctx, w)
		}
	}
	outputCancel()

	<-ch

	if workerErr != nil {
		return workerErr
	}
	return postPrepareErr
}
