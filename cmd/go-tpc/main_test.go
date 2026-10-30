package main

import (
	sqldrv "database/sql/driver"
	"net/url"
	"reflect"
	"testing"

	"github.com/lib/pq"
	"github.com/supabase/go-tpc/pkg/connadmit"
)

func TestApplyDirectSSLWorkaround_PassthroughWithoutDirectNegotiation(t *testing.T) {
	for _, in := range []string{"", "sslmode=disable", "sslmode=require"} {
		out, err := applyDirectSSLWorkaround(in)
		if err != nil {
			t.Fatalf("applyDirectSSLWorkaround(%q): %v", in, err)
		}
		if out != in {
			t.Errorf("applyDirectSSLWorkaround(%q) = %q, want unchanged", in, out)
		}
	}
}

func TestApplyDirectSSLWorkaround_RewritesSslmodeForDirectNegotiation(t *testing.T) {
	out, err := applyDirectSSLWorkaround("sslmode=require&sslnegotiation=direct")
	if err != nil {
		t.Fatalf("applyDirectSSLWorkaround: %v", err)
	}
	values, err := url.ParseQuery(out)
	if err != nil {
		t.Fatalf("parse result: %v", err)
	}
	if got := values.Get("sslmode"); got != "pqgo-direct-require" {
		t.Errorf("sslmode = %q, want pqgo-direct-require", got)
	}
	if got := values.Get("sslnegotiation"); got != "direct" {
		t.Errorf("sslnegotiation = %q, want direct", got)
	}
}

func TestApplyDirectSSLWorkaround_InvalidConnParams(t *testing.T) {
	if _, err := applyDirectSSLWorkaround("%zz"); err == nil {
		t.Fatal("expected error for invalid conn-params")
	}
}

func TestNewDB_ConnAdmitWrapsOnlyWhenSet(t *testing.T) {
	admit := &connadmit.Admitter{SQL: "SELECT true", MaxRejections: 50}
	cases := []struct {
		name    string
		targets []string
		admit   *connadmit.Admitter
		want    sqldrv.Driver
	}{
		{"one target, no flag", []string{"127.0.0.1:5432"}, nil, &pq.Driver{}},
		{"two targets, no flag", []string{"127.0.0.1:5432", "127.0.0.1:5433"}, nil, &MuxDriver{}},
		{"one target, flag set", []string{"127.0.0.1:5432"}, admit, &connadmit.Connector{}},
		{"two targets, flag set", []string{"127.0.0.1:5432", "127.0.0.1:5433"}, admit, &connadmit.Connector{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, err := newDB(tc.targets, pgDriver, "u", "secret", "d", "sslmode=disable", tc.admit)
			if err != nil {
				t.Fatalf("newDB: %v", err)
			}
			defer db.Close()
			if got, want := reflect.TypeOf(db.Driver()), reflect.TypeOf(tc.want); got != want {
				t.Errorf("db.Driver() is %v, want %v", got, want)
			}
		})
	}
}
