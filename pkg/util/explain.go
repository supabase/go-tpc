package util

import (
	"bytes"
	"database/sql"

	"github.com/olekukonko/tablewriter"
)

func RenderExplainAnalyze(rows *sql.Rows) (text string, err error) {
	cols, err := rows.Columns()
	if err != nil {
		return "", err
	}

	buf := new(bytes.Buffer)
	tb := tablewriter.NewWriter(buf)
	tb.Header(cols)

	for rows.Next() {
		rawResult := make([][]byte, len(cols))
		row := make([]string, len(cols))
		dest := make([]interface{}, len(cols))

		for i := range rawResult {
			dest[i] = &rawResult[i]
		}

		if err := rows.Scan(dest...); err != nil {
			return "", err
		}

		for i, raw := range rawResult {
			row[i] = string(raw)
		}
		tb.Append(row)
	}
	tb.Render()
	return buf.String(), nil
}
