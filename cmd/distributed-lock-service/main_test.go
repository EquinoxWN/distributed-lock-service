package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestDemoEndToEnd(t *testing.T) {
	var out bytes.Buffer
	if err := run([]string{"demo", "-clients", "4", "-rounds", "10", "-ttl", "150ms"}, &out); err != nil {
		t.Fatalf("demo: %v\n%s", err, out.String())
	}
	s := out.String()
	for _, want := range []string{"contention: 4 clients x 10 rounds = 40 acquisitions", "verify: overlaps=0 token_regressions=0 renewal_steals=0 missed_loss=0 late_takeover=0 leases_left=0"} {
		if !strings.Contains(s, want) {
			t.Fatalf("output lacks %q:\n%s", want, s)
		}
	}
}

func TestBadArguments(t *testing.T) {
	bad := [][]string{
		nil, {"nope"}, {"demo", "-clients", "1"}, {"demo", "-clients", "65"}, {"demo", "-rounds", "0"},
		{"demo", "-ttl", "10ms"}, {"demo", "-ttl", "11s"}, {"demo", "extra"}, {"demo", "-bogus"},
		{"serve", "extra"}, {"serve", "-etcd", "127.0.0.1:2379"}, {"serve", "-addr", "256.0.0.1:x"},
	}
	for _, args := range bad {
		var out bytes.Buffer
		if err := run(args, &out); err == nil {
			t.Errorf("run(%q) succeeded", args)
		}
	}
}

func TestBackendSelection(t *testing.T) {
	if _, desc, err := backend(""); err != nil || !strings.Contains(desc, "in-process") {
		t.Fatalf("default backend: %q %v", desc, err)
	}
	if _, desc, err := backend("http://127.0.0.1:2379,http://127.0.0.1:22379"); err != nil || !strings.HasPrefix(desc, "etcd ") {
		t.Fatalf("etcd backend: %q %v", desc, err)
	}
}
