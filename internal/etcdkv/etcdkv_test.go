package etcdkv

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/EquinoxWN/distributed-lock-service/internal/kv"
	"github.com/EquinoxWN/distributed-lock-service/internal/kv/kvtest"
	"github.com/EquinoxWN/distributed-lock-service/internal/memkv"
)

// fakeGateway serves the subset of etcd's JSON gateway this client uses, backed by memkv.
// It follows the request and response shapes of the etcd v3 gateway; the real gateway is
// exercised by the CI integration test (build tag etcd).
type fakeGateway struct {
	store *memkv.Store
}

func unb64(t *testing.T, s string) string {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		t.Errorf("bad base64 %q", s)
	}
	return string(b)
}

func atoi(s string) int64 { n, _ := strconv.ParseInt(s, 10, 64); return n }

func wire(e kv.KeyValue) map[string]string {
	return map[string]string{"key": b64(e.Key), "value": b64(e.Value), "create_revision": strconv.FormatInt(e.CreateRevision, 10),
		"mod_revision": strconv.FormatInt(e.ModRevision, 10), "lease": strconv.FormatInt(int64(e.Lease), 10)}
}

func (g *fakeGateway) handler(t *testing.T) http.Handler {
	notFound := func(w http.ResponseWriter) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"etcdserver: requested lease not found","code":5,"message":"etcdserver: requested lease not found"}`))
	}
	reply := func(w http.ResponseWriter, v any) { _ = json.NewEncoder(w).Encode(v) }
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v3/lease/grant", func(w http.ResponseWriter, r *http.Request) {
		var req struct{ TTL string }
		_ = json.NewDecoder(r.Body).Decode(&req)
		id, ttl, err := g.store.Grant(r.Context(), time.Duration(atoi(req.TTL))*time.Second)
		if err != nil {
			http.Error(w, `{"message":"`+err.Error()+`"}`, http.StatusBadRequest)
			return
		}
		reply(w, map[string]string{"ID": strconv.FormatInt(int64(id), 10), "TTL": strconv.FormatInt(int64(ttl/time.Second), 10)})
	})
	mux.HandleFunc("POST /v3/lease/keepalive", func(w http.ResponseWriter, r *http.Request) {
		var req struct{ ID string }
		_ = json.NewDecoder(r.Body).Decode(&req)
		ttl, err := g.store.KeepAlive(r.Context(), kv.LeaseID(atoi(req.ID)))
		if err != nil {
			reply(w, map[string]any{"result": map[string]string{"ID": req.ID}})
			return
		}
		reply(w, map[string]any{"result": map[string]string{"ID": req.ID, "TTL": strconv.FormatInt(int64(ttl/time.Second), 10)}})
	})
	mux.HandleFunc("POST /v3/lease/revoke", func(w http.ResponseWriter, r *http.Request) {
		var req struct{ ID string }
		_ = json.NewDecoder(r.Body).Decode(&req)
		if err := g.store.Revoke(r.Context(), kv.LeaseID(atoi(req.ID))); err != nil {
			notFound(w)
			return
		}
		reply(w, map[string]any{})
	})
	mux.HandleFunc("POST /v3/kv/range", func(w http.ResponseWriter, r *http.Request) {
		var req struct{ Key string }
		_ = json.NewDecoder(r.Body).Decode(&req)
		e, ok, _ := g.store.Get(r.Context(), unb64(t, req.Key))
		out := map[string]any{"header": map[string]string{"revision": strconv.FormatInt(g.store.Revision(), 10)}}
		if ok {
			out["kvs"] = []any{wire(e)}
			out["count"] = "1"
		}
		reply(w, out)
	})
	mux.HandleFunc("POST /v3/kv/txn", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Compare []map[string]string `json:"compare"`
			Success []struct {
				Put *struct{ Key, Value, Lease string } `json:"request_put"`
				Del *struct{ Key string }               `json:"request_delete_range"`
			} `json:"success"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Compare) != 1 || len(req.Success) != 1 {
			http.Error(w, `{"message":"bad txn"}`, http.StatusBadRequest)
			return
		}
		cmp := req.Compare[0]
		key := unb64(t, cmp["key"])
		switch {
		case cmp["target"] == "CREATE" && cmp["create_revision"] == "0" && req.Success[0].Put != nil:
			p := req.Success[0].Put
			ok, e, err := g.store.CreateIfAbsent(r.Context(), key, unb64(t, p.Value), kv.LeaseID(atoi(p.Lease)))
			if errors.Is(err, kv.ErrLeaseNotFound) {
				notFound(w)
				return
			}
			if err != nil {
				http.Error(w, `{"message":"bad request"}`, http.StatusBadRequest)
				return
			}
			out := map[string]any{"header": map[string]string{"revision": strconv.FormatInt(g.store.Revision(), 10)}}
			if ok {
				out["succeeded"] = true
				out["responses"] = []any{map[string]any{"response_put": map[string]any{}}}
			} else {
				out["responses"] = []any{map[string]any{"response_range": map[string]any{"kvs": []any{wire(e)}, "count": "1"}}}
			}
			reply(w, out)
		case cmp["target"] == "LEASE" && req.Success[0].Del != nil:
			ok, _ := g.store.DeleteIfLease(r.Context(), key, kv.LeaseID(atoi(cmp["lease"])))
			out := map[string]any{"header": map[string]string{"revision": strconv.FormatInt(g.store.Revision(), 10)}}
			if ok {
				out["succeeded"] = true
			}
			reply(w, out)
		default:
			http.Error(w, `{"message":"unsupported txn"}`, http.StatusBadRequest)
		}
	})
	return mux
}

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

func newClient(t *testing.T, o Options) *Client {
	t.Helper()
	c, err := New(o)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestContractThroughGatewayShapes(t *testing.T) {
	clk := &clock{t: time.Unix(0, 0)}
	g := &fakeGateway{store: memkv.New(clk.now)}
	srv := httptest.NewServer(g.handler(t))
	defer srv.Close()
	c := newClient(t, Options{Endpoints: []string{srv.URL}, Backoff: time.Millisecond})
	kvtest.Run(t, kvtest.Config{New: func(*testing.T) kv.Backend { return c }, Advance: clk.advance, TTL: time.Second, Prefix: "/etcdkv"})
}

func TestNewValidatesOptions(t *testing.T) {
	bad := []Options{
		{},
		{Endpoints: []string{"127.0.0.1:2379"}},
		{Endpoints: []string{"ftp://h:1"}},
		{Endpoints: []string{"http://u:p@h:1"}},
		{Endpoints: []string{"http://h:1/v3"}},
		{Endpoints: []string{"http://h:1?x=1"}},
		{Endpoints: []string{"http://"}},
		{Endpoints: []string{"http://h:1"}, Attempts: 11},
		{Endpoints: []string{"http://h:1"}, RequestTimeout: -1},
		{Endpoints: make([]string, 17)},
	}
	for i, o := range bad {
		if _, err := New(o); err == nil {
			t.Errorf("case %d: options %+v accepted", i, o)
		}
	}
	c := newClient(t, Options{Endpoints: []string{"http://127.0.0.1:2379/", "https://etcd-1:2379"}})
	if c.endpoints[0] != "http://127.0.0.1:2379" || c.timeout != DefaultRequestTimeout || c.attempts != DefaultAttempts {
		t.Fatalf("defaults not applied: %+v", c)
	}
}

func TestRetriesServerErrorsThenSucceeds(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) < 3 {
			http.Error(w, `{"message":"etcdserver: leader changed"}`, http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"ID":"7","TTL":"5"}`))
	}))
	defer srv.Close()
	c := newClient(t, Options{Endpoints: []string{srv.URL}, Backoff: time.Millisecond})
	id, ttl, err := c.Grant(context.Background(), 5*time.Second)
	if err != nil || id != 7 || ttl != 5*time.Second || calls.Load() != 3 {
		t.Fatalf("id %d ttl %v err %v after %d calls", id, ttl, err, calls.Load())
	}
}

func TestGivesUpAfterAttempts(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.Error(w, `{"message":"etcdserver: no leader"}`, http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	c := newClient(t, Options{Endpoints: []string{srv.URL}, Attempts: 4, Backoff: time.Millisecond})
	_, _, err := c.Get(context.Background(), "k")
	if !errors.Is(err, kv.ErrUnavailable) || calls.Load() != 4 || !strings.Contains(err.Error(), "no leader") {
		t.Fatalf("err %v after %d calls, want ErrUnavailable after 4", err, calls.Load())
	}
}

func TestClientErrorsAreNotRetried(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.Error(w, `{"message":"etcdserver: request is too large"}`, http.StatusBadRequest)
	}))
	defer srv.Close()
	c := newClient(t, Options{Endpoints: []string{srv.URL}, Backoff: time.Millisecond})
	if _, _, err := c.Get(context.Background(), "k"); err == nil || errors.Is(err, kv.ErrUnavailable) || calls.Load() != 1 {
		t.Fatalf("err %v after %d calls, want one non-retried error", err, calls.Load())
	}
}

func TestFailsOverToNextEndpoint(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close() // connection refused from now on
	live := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"header":{"revision":"3"}}`))
	}))
	defer live.Close()
	c := newClient(t, Options{Endpoints: []string{deadURL, live.URL}, Backoff: time.Millisecond})
	if _, found, err := c.Get(context.Background(), "k"); err != nil || found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	if got := c.endpoints[c.next.Load()]; got != live.URL {
		t.Fatalf("next endpoint %s, want the live one %s", got, live.URL)
	}
}

func TestRequestTimeout(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)
	c := newClient(t, Options{Endpoints: []string{srv.URL}, RequestTimeout: 50 * time.Millisecond, Attempts: 2, Backoff: time.Millisecond})
	start := time.Now()
	_, err := c.KeepAlive(context.Background(), 1)
	if !errors.Is(err, kv.ErrUnavailable) || time.Since(start) > 2*time.Second {
		t.Fatalf("err %v after %v, want ErrUnavailable quickly", err, time.Since(start))
	}
}

func TestCallerCancelStopsRetries(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.Error(w, "{}", http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	c := newClient(t, Options{Endpoints: []string{srv.URL}, Attempts: 10, Backoff: time.Second})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := c.Revoke(ctx, 1)
	if !errors.Is(err, context.DeadlineExceeded) || calls.Load() != 1 {
		t.Fatalf("err %v after %d calls", err, calls.Load())
	}
}

func TestMalformedAnswers(t *testing.T) {
	cases := map[string]func(c *Client) error{
		`not json`: func(c *Client) error { _, _, err := c.Get(context.Background(), "k"); return err },
		`{"ID":"x"}`: func(c *Client) error {
			_, _, err := c.Grant(context.Background(), time.Second)
			return err
		},
		`{"ID":"0","TTL":"0"}`: func(c *Client) error {
			_, _, err := c.Grant(context.Background(), time.Second)
			return err
		},
		`{"kvs":[{"key":"%%%"}]}`: func(c *Client) error { _, _, err := c.Get(context.Background(), "k"); return err },
		`{"succeeded":false,"responses":[]}`: func(c *Client) error {
			_, _, err := c.CreateIfAbsent(context.Background(), "k", "v", 1)
			return err
		},
		`{"succeeded":true,"header":{}}`: func(c *Client) error {
			_, _, err := c.CreateIfAbsent(context.Background(), "k", "v", 1)
			return err
		},
		`{"error":{"message":"etcdserver: requested lease not found"}}`: func(c *Client) error {
			_, err := c.KeepAlive(context.Background(), 1)
			if !errors.Is(err, kv.ErrLeaseNotFound) {
				return nil
			}
			return err
		},
	}
	for body, fn := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(body)) }))
		c := newClient(t, Options{Endpoints: []string{srv.URL}, Attempts: 1})
		if err := fn(c); err == nil {
			t.Errorf("answer %s: no error", body)
		}
		srv.Close()
	}
}

func TestTTLRoundsUpToSeconds(t *testing.T) {
	for in, want := range map[time.Duration]int64{time.Millisecond: 1, time.Second: 1, 1001 * time.Millisecond: 2, 10 * time.Second: 10} {
		if got := ttlSeconds(in); got != want {
			t.Errorf("ttlSeconds(%v) = %d, want %d", in, got, want)
		}
	}
	c := newClient(t, Options{Endpoints: []string{"http://127.0.0.1:1"}})
	if _, _, err := c.Grant(context.Background(), 0); err == nil {
		t.Fatal("zero ttl accepted")
	}
	if _, _, err := c.CreateIfAbsent(context.Background(), "", "v", 1); err == nil {
		t.Fatal("empty key accepted")
	}
}

func TestInt64AcceptsStringAndNumber(t *testing.T) {
	var v struct{ A, B, C i64 }
	if err := json.Unmarshal([]byte(`{"A":"9007199254740993","B":42}`), &v); err != nil || v.A != 9007199254740993 || v.B != 42 || v.C != 0 {
		t.Fatalf("%+v %v", v, err)
	}
	if err := json.Unmarshal([]byte(`{"A":"1.5"}`), &v); err == nil {
		t.Fatal("fraction accepted")
	}
}
