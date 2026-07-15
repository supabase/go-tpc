package main

import (
	"net/url"
	"testing"
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
