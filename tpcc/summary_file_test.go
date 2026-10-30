package tpcc

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/supabase/go-tpc/pkg/connadmit"
	"github.com/supabase/go-tpc/pkg/measurement"
)

func TestWriteSummaryFile_Disabled(t *testing.T) {
	if err := writeSummaryFile("", tpccSummaryDoc{}); err != nil {
		t.Fatalf("empty path should be a no-op, got: %v", err)
	}
}

func TestWriteSummaryFile_WritesStructuredJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "summary.json")
	tpm, tpmTotal, efc := 5131.7, 10480.4, 71.9
	doc := tpccSummaryDoc{
		Transactions: []measurement.OpSummary{
			{Transaction: "NEW_ORDER", Status: "ok", Count: 153943, TPM: tpm, AvgLatencyMs: 6.6},
			{Transaction: "NEW_ORDER", Status: "error", Count: 12, TPM: 0.4},
		},
		Tpm:           &tpm,
		TpmTotal:      &tpmTotal,
		EfficiencyPct: &efc,
	}

	if err := writeSummaryFile(path, doc); err != nil {
		t.Fatalf("writeSummaryFile: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var got tpccSummaryDoc
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal written summary: %v\n%s", err, data)
	}
	if len(got.Transactions) != 2 {
		t.Fatalf("transactions = %+v, want 2 entries", got.Transactions)
	}
	if got.Tpm == nil || *got.Tpm != tpm {
		t.Errorf("tpm = %v, want %v", got.Tpm, tpm)
	}
	if got.Transactions[1].Status != "error" {
		t.Errorf("transactions[1].Status = %q, want %q", got.Transactions[1].Status, "error")
	}
}

func TestWriteSummaryFile_ConnAdmission(t *testing.T) {
	stats := &connadmit.Stats{
		SQL:        "SELECT pg_backend_pid() % 2 = 0, pg_backend_pid()::text",
		Accepted:   2,
		Rejections: 3,
		Connections: []connadmit.Connection{
			{Label: "4242", Rejections: 0},
			{Label: "4250", Rejections: 3},
		},
	}
	cases := []struct {
		name  string
		stats *connadmit.Stats
	}{
		{"flag set", stats},
		// OutputStats fills the key from a nil Admitter when the flag is unset.
		{"flag unset", (*connadmit.Admitter)(nil).Stats()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "summary.json")
			if err := writeSummaryFile(path, tpccSummaryDoc{ConnAdmission: tc.stats}); err != nil {
				t.Fatalf("writeSummaryFile: %v", err)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %s: %v", path, err)
			}
			var raw map[string]json.RawMessage
			if err := json.Unmarshal(data, &raw); err != nil {
				t.Fatalf("unmarshal written summary: %v\n%s", err, data)
			}
			blob, present := raw["conn_admission"]
			if tc.stats == nil {
				if present {
					t.Fatalf("conn_admission present without the flag:\n%s", data)
				}
				return
			}
			if !present {
				t.Fatalf("conn_admission missing:\n%s", data)
			}
			var got connadmit.Stats
			if err := json.Unmarshal(blob, &got); err != nil {
				t.Fatalf("unmarshal conn_admission: %v", err)
			}
			if !reflect.DeepEqual(&got, tc.stats) {
				t.Errorf("conn_admission = %+v, want %+v", got, *tc.stats)
			}
		})
	}
}
