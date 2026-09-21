package tpcc

import "testing"

func TestAnalyzeStmt(t *testing.T) {
	cases := []struct {
		driver string
		table  string
		want   string
	}{
		{"postgres", tableOrderLine, "VACUUM ANALYZE order_line"},
		{"mysql", tableOrderLine, "ANALYZE TABLE order_line"},
		{"cockroach", tableOrderLine, ""},
		{"", tableOrderLine, ""},
	}

	for _, c := range cases {
		if got := analyzeStmt(c.driver, c.table); got != c.want {
			t.Errorf("analyzeStmt(%q, %q) = %q, want %q", c.driver, c.table, got, c.want)
		}
	}
}

func TestAutoMaintenanceSetting(t *testing.T) {
	cases := []struct {
		driver    string
		wantName  string
		wantQuery string
	}{
		{"postgres", "autovacuum", "SHOW autovacuum"},
		{"mysql", "innodb_stats_auto_recalc", "SELECT @@innodb_stats_auto_recalc"},
		{"cockroach", "", ""},
	}

	for _, c := range cases {
		name, query := autoMaintenanceSetting(c.driver)
		if name != c.wantName || query != c.wantQuery {
			t.Errorf("autoMaintenanceSetting(%q) = (%q, %q), want (%q, %q)",
				c.driver, name, query, c.wantName, c.wantQuery)
		}
	}
}

func TestIsEnabledSetting(t *testing.T) {
	cases := []struct {
		value string
		want  bool
	}{
		{"on", true},
		{"ON", true},
		{" on ", true},
		{"1", true},
		{"true", true},
		{"off", false},
		{"OFF", false},
		{"0", false},
		{"", false},
	}

	for _, c := range cases {
		if got := isEnabledSetting(c.value); got != c.want {
			t.Errorf("isEnabledSetting(%q) = %v, want %v", c.value, got, c.want)
		}
	}
}
