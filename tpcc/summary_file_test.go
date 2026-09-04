package tpcc

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/supabase/go-tpc/pkg/measurement"
)

func TestWriteSummaryFile_Disabled(t *testing.T) {
	if err := writeSummaryFile("", tpccSummaryDoc{}); err != nil {
		t.Fatalf("empty path should be a no-op, got: %v", err)
	}
}

func TestWriteSummaryFile_WritesStructuredJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "summary.json")
	tpmC, tpmTotal, efc := 5131.7, 10480.4, 71.9
	doc := tpccSummaryDoc{
		Transactions: []measurement.OpSummary{
			{Transaction: "NEW_ORDER", Status: "ok", Count: 153943, TPM: tpmC, AvgLatencyMs: 6.6},
			{Transaction: "NEW_ORDER", Status: "error", Count: 12, TPM: 0.4},
		},
		TpmC:          &tpmC,
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
	if got.TpmC == nil || *got.TpmC != tpmC {
		t.Errorf("tpmC = %v, want %v", got.TpmC, tpmC)
	}
	if got.Transactions[1].Status != "error" {
		t.Errorf("transactions[1].Status = %q, want %q", got.Transactions[1].Status, "error")
	}
}
