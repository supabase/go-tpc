package sink

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/lib/pq"
)

// attempt records one execution of a batch for diagnostics.
type attempt struct {
	start    time.Time
	duration time.Duration
	label    string
	err      error
}

func formatTime(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

// tableName returns the table an INSERT statement writes to, or "" if the
// statement does not have the form "INSERT ... INTO <table> ...".
func tableName(insert string) string {
	fields := strings.Fields(insert)
	for i := 0; i < len(fields)-1; i++ {
		if strings.EqualFold(fields[i], "INTO") {
			return strings.SplitN(fields[i+1], "(", 2)[0]
		}
	}
	return ""
}

// formatAttempts writes one entry per attempt. Consecutive attempts that
// failed with the same label and error are merged into one entry, so the
// first error and every change of error stay visible.
func formatAttempts(b *strings.Builder, attempts []attempt) {
	for i := 0; i < len(attempts); {
		a := attempts[i]
		j := i + 1
		for j < len(attempts) && attempts[j].label == a.label && attempts[j].err.Error() == a.err.Error() {
			j++
		}
		if j-i == 1 {
			fmt.Fprintf(b, "    #%d %s %s %s: %v\n", i+1, formatTime(a.start), a.duration.Round(time.Millisecond), a.label, a.err)
		} else {
			last := attempts[j-1]
			fmt.Fprintf(b, "    #%d-#%d %s to %s %s: %v\n", i+1, j, formatTime(a.start), formatTime(last.start), a.label, a.err)
		}
		for _, line := range describeError(a.err) {
			fmt.Fprintf(b, "         %s\n", line)
		}
		i = j
	}
}

// describeError returns every diagnostic field the driver exposes for err, one
// "name: value" entry per non-empty field.
func describeError(err error) []string {
	var lines []string
	add := func(name, value string) {
		if value != "" {
			lines = append(lines, name+": "+strings.ReplaceAll(value, "\n", "\n           "))
		}
	}

	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		add("severity", pqErr.Severity)
		add("sqlstate", fmt.Sprintf("%s %s", pqErr.Code, pqErr.Code.Name()))
		add("message", pqErr.Message)
		add("detail", pqErr.Detail)
		add("hint", pqErr.Hint)
		add("position", pqErr.Position)
		add("internal query", pqErr.InternalQuery)
		add("internal position", pqErr.InternalPosition)
		add("where", pqErr.Where)
		add("schema", pqErr.Schema)
		add("table", pqErr.Table)
		add("column", pqErr.Column)
		add("data type", pqErr.DataTypeName)
		add("constraint", pqErr.Constraint)
		if pqErr.File != "" || pqErr.Routine != "" {
			add("source", fmt.Sprintf("%s:%s %s", pqErr.File, pqErr.Line, pqErr.Routine))
		}
		return lines
	}

	var myErr *mysql.MySQLError
	if errors.As(err, &myErr) {
		add("number", fmt.Sprintf("%d", myErr.Number))
		add("sqlstate", string(myErr.SQLState[:]))
		add("message", myErr.Message)
		return lines
	}

	// Not a server error: show the Go types in the error chain, e.g.
	// *net.OpError -> *os.SyscallError, which tell how the connection broke.
	var types []string
	for e := err; e != nil; e = errors.Unwrap(e) {
		types = append(types, fmt.Sprintf("%T", e))
	}
	add("type", strings.Join(types, " -> "))
	return lines
}
