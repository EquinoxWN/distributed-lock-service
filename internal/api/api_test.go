package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/EquinoxWN/distributed-lock-service/internal/kv"
	"github.com/EquinoxWN/distributed-lock-service/internal/memkv"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

// syncBuf is a log sink safe for the server's goroutines.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

func setupLog(t *testing.T) (*httptest.Server, *memkv.Store, *fakeClock, *syncBuf) {
	t.Helper()
	clk := &fakeClock{t: time.Unix(0, 0)}
	s := memkv.New(clk.now)
	logs := &syncBuf{}
	h, err := New(s, slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h.Handler())
	t.Cleanup(srv.Close)
	return srv, s, clk, logs
}

func setup(t *testing.T) (*httptest.Server, *memkv.Store, *fakeClock) {
	srv, s, clk, _ := setupLog(t)
	return srv, s, clk
}

// do sends one request; secret, when not empty, goes in the Lock-Secret header.
func do(t *testing.T, method, url, body, secret string) (int, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if secret != "" {
		req.Header.Set(SecretHeader, secret)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	raw, _ := io.ReadAll(res.Body)
	out := map[string]any{}
	if len(raw) > 0 && json.Unmarshal(raw, &out) != nil {
		if strings.HasPrefix(res.Header.Get("Content-Type"), "application/json") {
			t.Fatalf("%s %s: bad JSON %q", method, url, raw)
		}
		out["raw"] = string(raw) // mux answers such as "404 page not found"
	}
	return res.StatusCode, out
}

// acquire takes a lock or fails the test.
func acquire(t *testing.T, srv *httptest.Server, name, owner string, ttlMs int) map[string]any {
	t.Helper()
	code, a := do(t, "POST", srv.URL+"/v1/locks/"+name, `{"owner":"`+owner+`","ttl_ms":`+strconv.Itoa(ttlMs)+`}`, "")
	if code != 200 {
		t.Fatalf("acquire %s by %s: %d %v", name, owner, code, a)
	}
	return a
}

func TestAcquireKeepAliveReleaseFlow(t *testing.T) {
	srv, s, clk := setup(t)
	a := acquire(t, srv, "orders", "worker-a", 3000)
	secret, _ := a["secret"].(string)
	if a["owner"] != "worker-a" || a["ttl_ms"] != 3000.0 || a["token"].(float64) <= 0 || len(secret) != 64 {
		t.Fatalf("acquire: %v", a)
	}
	code, b := do(t, "POST", srv.URL+"/v1/locks/orders", `{"owner":"worker-b","ttl_ms":3000}`, "")
	holder, _ := b["holder"].(map[string]any)
	if code != 409 || b["error"] != "held" || holder["owner"] != "worker-a" || holder["lease_id"] != a["lease_id"] || holder["token"] != a["token"] {
		t.Fatalf("second acquire: %d %v", code, b)
	}
	if _, leaked := holder["secret"]; leaked || strings.Contains(b["message"].(string), secret) {
		t.Fatalf("409 leaks the secret: %v", b)
	}
	if s.LeaseCount() != 1 {
		t.Fatalf("%d leases, want the loser's revoked", s.LeaseCount())
	}
	clk.advance(2 * time.Second)
	if code, k := do(t, "POST", srv.URL+"/v1/locks/orders/keepalive", "", secret); code != 200 || k["ttl_ms"] != 3000.0 {
		t.Fatalf("keepalive: %d %v", code, k)
	}
	clk.advance(2 * time.Second) // 4 s after grant, 2 s after the renewal
	if code, g := do(t, "GET", srv.URL+"/v1/locks/orders", "", ""); code != 200 || g["owner"] != "worker-a" || g["secret"] != nil || g["secret_sha256"] != nil {
		t.Fatalf("get while renewed: %d %v", code, g)
	}
	for range 2 {
		if code, _ := do(t, "DELETE", srv.URL+"/v1/locks/orders", "", secret); code != 204 {
			t.Fatalf("release: %d", code)
		}
	}
	if code, _ := do(t, "GET", srv.URL+"/v1/locks/orders", "", ""); code != 404 {
		t.Fatalf("get after release: %d", code)
	}
	if code, k := do(t, "POST", srv.URL+"/v1/locks/orders/keepalive", "", secret); code != 404 || k["error"] != "not_held" {
		t.Fatalf("keepalive after release: %d %v", code, k)
	}
	c := acquire(t, srv, "orders", "worker-b", 3000)
	if c["token"].(float64) <= a["token"].(float64) {
		t.Fatalf("reacquire token %v not above %v", c["token"], a["token"])
	}
}

func TestReleaseAndKeepAliveNeedTheHoldersSecret(t *testing.T) {
	srv, s, _ := setup(t)
	a := acquire(t, srv, "job", "a", 3000)
	other := acquire(t, srv, "other", "b", 3000)
	lease := a["lease_id"].(string)
	wrong := strings.Repeat("0", 64)
	cases := []struct {
		method, path, secret string
		code                 int
		errCode              string
	}{
		{"DELETE", "/v1/locks/job", "", 401, "secret_required"},
		{"POST", "/v1/locks/job/keepalive", "", 401, "secret_required"},
		{"DELETE", "/v1/locks/job", wrong, 403, "forbidden"},
		{"POST", "/v1/locks/job/keepalive", wrong, 403, "forbidden"},
		{"DELETE", "/v1/locks/job", other["secret"].(string), 403, "forbidden"}, // a real secret of another lock
		{"POST", "/v1/locks/job/keepalive", other["secret"].(string), 403, "forbidden"},
		{"DELETE", "/v1/locks/job", lease, 403, "forbidden"}, // the guessed lease id is not a secret
		{"DELETE", "/v1/locks/job?lease_id=" + lease, "", 401, "secret_required"},
		{"POST", "/v1/leases/" + lease + "/keepalive", "", 404, ""}, // the old lease-id route is gone
		{"DELETE", "/v1/locks/job", strings.Repeat("x", 4096), 403, "forbidden"},
	}
	for _, c := range cases {
		code, out := do(t, c.method, srv.URL+c.path, "", c.secret)
		if code != c.code || (c.errCode != "" && out["error"] != c.errCode) {
			t.Errorf("%s %s secret %.10q: %d %v, want %d %s", c.method, c.path, c.secret, code, out, c.code, c.errCode)
		}
	}
	if code, g := do(t, "GET", srv.URL+"/v1/locks/job", "", ""); code != 200 || g["owner"] != "a" || g["token"] != a["token"] {
		t.Fatalf("holder changed after rejected calls: %d %v", code, g)
	}
	if n := s.LeaseCount(); n != 2 {
		t.Fatalf("%d leases, want 2", n)
	}
}

func TestSecretsDifferPerAcquisitionAndAreNotLogged(t *testing.T) {
	srv, s, _, logs := setupLog(t)
	seen := map[string]bool{}
	for i := range 20 {
		a := acquire(t, srv, "job", "a", 1000)
		secret := a["secret"].(string)
		if seen[secret] {
			t.Fatalf("acquisition %d repeated a secret", i)
		}
		seen[secret] = true
		do(t, "DELETE", srv.URL+"/v1/locks/job", "", strings.Repeat("f", 64)) // rejected, logged nowhere
		if code, _ := do(t, "DELETE", srv.URL+"/v1/locks/job", "", secret); code != 204 {
			t.Fatalf("release %d: %d", i, code)
		}
	}
	s.SetFault(func(string) error { return kv.ErrUnavailable })
	for secret := range seen {
		do(t, "POST", srv.URL+"/v1/locks/job/keepalive", "", secret) // 503, logged as a warning
	}
	out := logs.String()
	if !strings.Contains(out, "store unavailable") {
		t.Fatalf("expected warnings in the log, got %q", out)
	}
	for secret := range seen {
		if strings.Contains(out, secret) {
			t.Fatal("a secret appears in the server log")
		}
	}
}

func TestExpiredLeaseFreesTheLock(t *testing.T) {
	srv, _, clk := setup(t)
	a := acquire(t, srv, "job", "a", 1000)
	clk.advance(time.Second)
	if code, k := do(t, "POST", srv.URL+"/v1/locks/job/keepalive", "", a["secret"].(string)); code != 404 || k["error"] != "not_held" {
		t.Fatalf("keepalive after expiry: %d %v", code, k)
	}
	if b := acquire(t, srv, "job", "b", 1000); b["token"].(float64) <= a["token"].(float64) {
		t.Fatalf("takeover token %v not above %v", b["token"], a["token"])
	}
}

func TestStaleHolderCannotTouchTheNewHolder(t *testing.T) {
	srv, _, clk := setup(t)
	a := acquire(t, srv, "job", "a", 1000)
	clk.advance(time.Second)
	acquire(t, srv, "job", "b", 1000)
	if code, out := do(t, "DELETE", srv.URL+"/v1/locks/job", "", a["secret"].(string)); code != 403 || out["error"] != "forbidden" {
		t.Fatalf("stale release: %d %v", code, out)
	}
	if code, _ := do(t, "POST", srv.URL+"/v1/locks/job/keepalive", "", a["secret"].(string)); code != 403 {
		t.Fatalf("stale keepalive: %d", code)
	}
	if code, g := do(t, "GET", srv.URL+"/v1/locks/job", "", ""); code != 200 || g["owner"] != "b" {
		t.Fatalf("holder after stale calls: %d %v", code, g)
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
		{"DELETE", "/v1/locks/bad%20name", ""},
		{"POST", "/v1/locks/bad%20name/keepalive", ""},
		{"GET", "/v1/locks/a.b%2Fc", ""},
	}
	for _, c := range cases {
		if code, out := do(t, c.method, srv.URL+c.path, c.body, "s"); code != 400 || out["error"] != "invalid" {
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
	if code, out := do(t, "POST", srv.URL+"/v1/locks/job", `{"owner":"a","ttl_ms":1000}`, ""); code != 503 || out["error"] != "unavailable" {
		t.Fatalf("%d %v", code, out)
	}
	s.SetFault(func(string) error { return errors.New("disk on fire") })
	if code, out := do(t, "GET", srv.URL+"/v1/locks/job", "", ""); code != 500 || strings.Contains(out["message"].(string), "fire") {
		t.Fatalf("internal error leaked or wrong code: %d %v", code, out)
	}
}

func TestHealthz(t *testing.T) {
	srv, _, _ := setup(t)
	if code, out := do(t, "GET", srv.URL+"/healthz", "", ""); code != 200 || out["status"] != "ok" {
		t.Fatalf("%d %v", code, out)
	}
}
