package tpcc

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/supabase/go-tpc/pkg/util"
)

// analyzeSpecWarning is printed whenever --analyze is used.
const analyzeSpecWarning = `[tpcc] --analyze refreshes optimizer statistics after the load, i.e. work performed
[tpcc] outside the measurement interval. TPC-C Clause 4.2.3(2) permits that only if the
[tpcc] same work is also performed during the measurement interval. --analyze is
[tpcc] therefore spec-compliant only when the database does this maintenance
[tpcc] automatically during the run: autovacuum on PostgreSQL or
[tpcc] innodb_stats_auto_recalc on MySQL.`

// analyzeStmt returns the statement that refreshes optimizer statistics for
// table, or "" when the driver has no equivalent.
//
// PostgreSQL uses VACUUM ANALYZE rather than a plain ANALYZE, so unset
// visibility-map bits left behind by the bulk load are cleaned up too.
//
// MySQL uses ANALYZE TABLE, which recalculates the persistent InnoDB
// statistics; InnoDB has no VACUUM to invoke, since its purge threads
// run continuously and cannot be disabled.
func analyzeStmt(driver, table string) string {
	switch driver {
	case "postgres":
		return "VACUUM ANALYZE " + table
	case "mysql":
		return "ANALYZE TABLE " + table
	default:
		return ""
	}
}

// autoMaintenanceSetting names the server setting that decides whether the
// analyzeStmt work also happens automatically during the measurement interval,
// and the query to read it. See analyzeSpecWarning.
func autoMaintenanceSetting(driver string) (name, query string) {
	switch driver {
	case "postgres":
		return "autovacuum", "SHOW autovacuum"
	case "mysql":
		return "innodb_stats_auto_recalc", "SELECT @@innodb_stats_auto_recalc"
	default:
		return "", ""
	}
}

// isEnabledSetting reports whether a boolean server setting is on. PostgreSQL
// SHOW returns "on"/"off", MySQL returns "1"/"0".
func isEnabledSetting(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "on", "1", "true", "yes":
		return true
	default:
		return false
	}
}

// AnalyzeTables refreshes optimizer statistics for every TPC-C table.
//
// The command layer calls this once, after every prepare worker has finished
// loading, so it uses the shared *sql.DB rather than a per-thread connection.
func (w *Workloader) AnalyzeTables(ctx context.Context) error {
	if w.db == nil {
		return nil
	}

	util.StdErrLogger.Print(analyzeSpecWarning)
	w.warnIfAutoMaintenanceDisabled(ctx)

	for _, tbl := range tables {
		stmt := analyzeStmt(w.cfg.Driver, tbl)
		if stmt == "" {
			return fmt.Errorf("--analyze is not supported for driver %q", w.cfg.Driver)
		}
		fmt.Printf("analyzing table %s\n", tbl)
		start := time.Now()
		// One statement per ExecContext, and no bind parameters to avoid that
		// the statement is put inside a transaction block, where PostgreSQL
		// refuses to run VACUUM. With zero arguments lib/pq uses the simple
		// query protocol, which is what VACUUM needs. Table names are package
		// constants, so building the statement by concatenation is safe.
		//
		// On MySQL this discards the result set ANALYZE TABLE returns, so a
		// per-table failure reported as a row with Msg_type='Error' is not
		// surfaced.
		if _, err := w.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("analyze table %s: %w", tbl, err)
		}
		fmt.Printf("analyze table %s done in %s\n", tbl, time.Since(start).Round(time.Millisecond))
	}
	return nil
}

// warnIfAutoMaintenanceDisabled warns when the server will not repeat the
// analyze work during the measurement interval. It reads only the global
// setting, not per-table storage parameters such as autovacuum_enabled=false
// which is acceptable as this tool creates these tables in the prepare step.
func (w *Workloader) warnIfAutoMaintenanceDisabled(ctx context.Context) {
	name, query := autoMaintenanceSetting(w.cfg.Driver)
	if query == "" {
		return
	}
	var value string
	if err := w.db.QueryRowContext(ctx, query).Scan(&value); err != nil {
		util.StdErrLogger.Printf("[tpcc] could not read %s to verify TPC-C Clause 4.2.3(2) compliance: %v", name, err)
		return
	}
	if isEnabledSetting(value) {
		return
	}
	util.StdErrLogger.Printf("[tpcc] WARNING: %s is %q, so this maintenance never happens during the\n"+
		"[tpcc] measurement interval. Using --analyze on this server violates TPC-C Clause 4.2.3(2).", name, value)
}
