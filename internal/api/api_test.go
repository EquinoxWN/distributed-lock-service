package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/EquinoxWN/distributed-lock-service/internal/kv"
	"github.com/EquinoxWN/distributed-lock-service/internal/memkv"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }

func setup(t *testing.T) (*httptest.Server, *memkv.Store, *fakeClock) {
	t.Helper()
	clk := &fakeClock{t: time.Unix(0, 0)}
	s := memkv.New(clk.now)
	srv := httptest.NewServer(New(s, nil).Handler())
	t.Cleanup(srv.Close)
	return srv, s, clk
}

func do(t *testing.T, method, url, body string) (int, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	raw, _ := io.ReadAll(res.Body)
	out := map[string]any{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("%s %s: bad JSON %q", method, url, raw)
		}
	}
	return res.StatusCode, out
}

func TestAcquireKeepAliveReleaseFlow(t *testing.T) {
	srv, s, clk := setup(t)
	code, a := do(t, "POST", srv.URL+"/v1/locks/orders", `{"owner":"worker-a","ttl_ms":3000}`)
	if code != 200 || a["owner"] != "worker-a" || a["ttl_ms"] != 3000.0 || a["token"].(float64) <= 0 {
		t.Fatalf("acquire: %d %v", code, a)
	}
	lease := a["lease_id"].(string)
	code, b := do(t, "POST", srv.URL+"/v1/locks/orders", `{"owner":"worker-b","ttl_ms":3000}`)
	holder, _ := b["holder"].(map[string]any)
	if code != 409 || b["error"] != "held" || holder["owner"] != "worker-a" || holder["lease_id"] != lease || holder["token"] != a["token"] {
		t.Fatalf("second acquire: %d %v", code, b)
	}
	if s.LeaseCount() != 1 {
		t.Fatalf("%d leases, want the loser's revoked", s.LeaseCount())
	}
	clk.t = clk.t.Add(2 * time.Second)
	if code, k := do(t, "POST", srv.URL+"/v1/leases/"+lease+"/keepalive", ""); code != 200 || k["ttl_ms"] != 3000.0 {
		t.Fatalf("keepalive: %d %v", code, k)
	}
	clk.t = clk.t.Add(2 * time.Second) // 4 s after grant, 2 s after the renewal
	if code, g := do(t, "GET", srv.URL+"/v1/locks/orders", ""); code != 200 || g["owner"] != "worker-a" {
		t.Fatalf("get while renewed: %d %v", code, g)
	}
	for range 2 {
		if code, _ := do(t, "DELETE", srv.URL+"/v1/locks/orders?lease_id="+lease, ""); code != 204 {
			t.Fatalf("release: %d", code)
		}
	}
	if code, _ := do(t, "GET", srv.URL+"/v1/locks/orders", ""); code != 404 {
		t.Fatalf("get after release: %d", code)
	}
	code, c := do(t, "POST", srv.URL+"/v1/locks/orders", `{"owner":"worker-b","ttl_ms":3000}`)
	if code != 200 || c["token"].(float64) <= a["token"].(float64) {
		t.Fatalf("reacquire: %d %v (old token %v)", code, c, a["token"])
	}
}

func TestExpiredLeaseFreesTheLock(t *testing.T) {
	srv, _, clk := setup(t)
	_, a := do(t, "POST", srv.URL+"/v1/locks/job", `{"owner":"a","ttl_ms":1000}`)
	clk.t = clk.t.Add(time.Second)
	if code, k := do(t, "POST", srv.URL+"/v1/leases/"+a["lease_id"].(string)+"/keepalive", ""); code != 404 || k["error"] != "lease_not_found" {
		t.Fatalf("keepalive after expiry: %d %v", code, k)
	}
	if code, b := do(t, "POST", srv.URL+"/v1/locks/job", `{"owner":"b","ttl_ms":1000}`); code != 200 || b["token"].(float64) <= a["token"].(float64) {
		t.Fatalf("takeover: %d %v", code, b)
	}
}

func TestStaleReleaseLeavesNewHolder(t *testing.T) {
	srv, _, clk := setup(t)
	_, a := do(t, "POST", srv.URL+"/v1/locks/job", `{"owner":"a","ttl_ms":1000}`)
	clk.t = clk.t.Add(time.Second)
	do(t, "POST", srv.URL+"/v1/locks/job", `{"owner":"b","ttl_ms":1000}`)
	if code, _ := do(t, "DELETE", srv.URL+"/v1/locks/job?lease_id="+a["lease_id"].(string), ""); code != 204 {
		t.Fatalf("stale release: %d", code)
	}
	if code, g := do(t, "GET", srv.URL+"/v1/locks/job", ""); code != 200 || g["owner"] != "b" {
		t.Fatalf("holder after stale release: %d %v", code, g)
	}
}

func TestBadRequests(t *testing.T) {
	srv, _, _ := setup(t)
	cases := []struct{ method, path, body string }{
		{"POST", "/v1/locks/bad%20name", `{"owner":"a","ttl_ms":1000}`},
		{"POST", "/v1/locks/job", `{"owner":"a b","ttl_ms":1000}`},
		{"POST", "/v1/locks/job", `{"owner":"a","ttl_ms":50}`},
		{"POST", "/v1/locks/job", `{"owner":"a","ttl_ms":600001}`},
		{"POST", "/v1/locks/job", `{"owner":"a","ttl_ms":-1}`},
		{"POST", "/v1/locks/job", `{"owner":"a","ttl_ms":1000,"extra":1}`},
		{"POST", "/v1/locks/job", `{"owner":"a","ttl_ms":1000}{}`},
		{"POST", "/v1/locks/job", `{"owner":"` + strings.Repeat("a", 5000) + `","ttl_ms":1000}`},
		{"POST", "/v1/locks/job", `not json`},
		{"DELETE", "/v1/locks/job?lease_id=abc", ""},
		{"DELETE", "/v1/locks/job", ""},
		{"POST", "/v1/leases/0/keepalive", ""},
		{"GET", "/v1/locks/a.b%2Fc", ""},
	}
	for _, c := range cases {
		if code, out := do(t, c.method, srv.URL+c.path, c.body); code != 400 || out["error"] != "invalid" {
			t.Errorf("%s %s %.40s: %d %v, want 400 invalid", c.method, c.path, c.body, code, out)
		}
	}
	req, _ := http.NewRequest("POST", srv.URL+"/v1/locks/job", strings.NewReader(`{"owner":"a","ttl_ms":1000}`))
	req.Header.Set("Content-Type", "text/plain")
	res, err := http.DefaultClient.Do(req)
	if err != nil || res.StatusCode != 400 {
		t.Fatalf("wrong content type: %v %v", res.StatusCode, err)
	}
	_ = res.Body.Close()
}

func TestStoreUnavailableIs503(t *testing.T) {
	srv, s, _ := setup(t)
	s.SetFault(func(string) error { return kv.ErrUnavailable })
	if code, out := do(t, "POST", srv.URL+"/v1/locks/job", `{"owner":"a","ttl_ms":1000}`); code != 503 || out["error"] != "unavailable" {
		t.Fatalf("%d %v", code, out)
	}
	s.SetFault(func(string) error { return errors.New("disk on fire") })
	if code, out := do(t, "GET", srv.URL+"/v1/locks/job", ""); code != 500 || strings.Contains(out["message"].(string), "fire") {
		t.Fatalf("internal error leaked or wrong code: %d %v", code, out)
	}
}

func TestHealthz(t *testing.T) {
	srv, _, _ := setup(t)
	if code, out := do(t, "GET", srv.URL+"/healthz", ""); code != 200 || out["status"] != "ok" {
		t.Fatalf("%d %v", code, out)
	}
}
