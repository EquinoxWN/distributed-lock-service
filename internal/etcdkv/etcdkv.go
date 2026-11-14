// Package etcdkv implements kv.Backend on etcd v3 through its JSON gateway (/v3/...), with net/http.
//
// Every request has a timeout; transport errors and 5xx answers are retried a bounded number of
// times with backoff, rotating through the endpoints. Retried writes are safe at the lock layer:
// a create that already succeeded is recognised by its lease id, a repeated delete reports false,
// and a lease granted twice expires on its own after its TTL.
package etcdkv

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/EquinoxWN/distributed-lock-service/internal/kv"
)

// Defaults for Options left at zero.
const (
	DefaultRequestTimeout = 3 * time.Second
	DefaultAttempts       = 3
	DefaultBackoff        = 100 * time.Millisecond
	maxResponseBytes      = 1 << 20
)

// Options configure a Client.
type Options struct {
	Endpoints      []string      // base URLs such as http://127.0.0.1:2379
	RequestTimeout time.Duration // per HTTP request
	Attempts       int           // tries per call across endpoints, 1..10
	Backoff        time.Duration // first retry delay, doubled each retry
	HTTPClient     *http.Client  // optional; its own Timeout is left as given
}

// Client talks to an etcd cluster.
type Client struct {
	endpoints []string
	hc        *http.Client
	timeout   time.Duration
	attempts  int
	backoff   time.Duration
	next      atomic.Uint32
}

var _ kv.Backend = (*Client)(nil)

// New validates the options and returns a client.
func New(o Options) (*Client, error) {
	if len(o.Endpoints) == 0 || len(o.Endpoints) > 16 {
		return nil, fmt.Errorf("etcdkv: %d endpoints, want 1..16", len(o.Endpoints))
	}
	eps := make([]string, 0, len(o.Endpoints))
	for _, e := range o.Endpoints {
		u, err := url.Parse(e)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil ||
			(u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
			return nil, fmt.Errorf("etcdkv: endpoint %q must be http(s)://host:port", e)
		}
		eps = append(eps, u.Scheme+"://"+u.Host)
	}
	c := &Client{endpoints: eps, hc: o.HTTPClient, timeout: o.RequestTimeout, attempts: o.Attempts, backoff: o.Backoff}
	if c.hc == nil {
		c.hc = &http.Client{}
	}
	if c.timeout == 0 {
		c.timeout = DefaultRequestTimeout
	}
	if c.attempts == 0 {
		c.attempts = DefaultAttempts
	}
	if c.backoff == 0 {
		c.backoff = DefaultBackoff
	}
	if c.timeout < 0 || c.attempts < 1 || c.attempts > 10 || c.backoff < 0 {
		return nil, fmt.Errorf("etcdkv: timeout %v, attempts %d, backoff %v out of range", c.timeout, c.attempts, c.backoff)
	}
	return c, nil
}

// i64 decodes the gateway's int64 fields, which proto3 JSON writes as strings.
type i64 int64

// UnmarshalJSON accepts "123" and 123.
func (v *i64) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	if s == "" || s == "null" {
		*v = 0
		return nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return fmt.Errorf("int64 field %s: %w", b, err)
	}
	*v = i64(n)
	return nil
}

type header struct {
	Revision i64 `json:"revision"`
}

type wireKV struct {
	Key            string `json:"key"`
	Value          string `json:"value"`
	CreateRevision i64    `json:"create_revision"`
	ModRevision    i64    `json:"mod_revision"`
	Lease          i64    `json:"lease"`
}

type leaseResp struct {
	ID  i64 `json:"ID"`
	TTL i64 `json:"TTL"`
}

type rangeResp struct {
	Header header   `json:"header"`
	KVs    []wireKV `json:"kvs"`
}

type txnResp struct {
	Header    header `json:"header"`
	Succeeded bool   `json:"succeeded"`
	Responses []struct {
		ResponseRange *rangeResp `json:"response_range"`
	} `json:"responses"`
}

type gatewayError struct {
	Error   string `json:"error"`
	Message string `json:"message"`
	Code    int    `json:"code"`
}

// b64 encodes a key or value as the gateway expects bytes fields.
func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

// toKV decodes one wire key.
func toKV(w wireKV) (kv.KeyValue, error) {
	k, err := base64.StdEncoding.DecodeString(w.Key)
	if err != nil {
		return kv.KeyValue{}, fmt.Errorf("etcdkv: key encoding: %w", err)
	}
	v, err := base64.StdEncoding.DecodeString(w.Value)
	if err != nil {
		return kv.KeyValue{}, fmt.Errorf("etcdkv: value encoding: %w", err)
	}
	return kv.KeyValue{Key: string(k), Value: string(v), CreateRevision: int64(w.CreateRevision),
		ModRevision: int64(w.ModRevision), Lease: kv.LeaseID(w.Lease)}, nil
}

// call posts one request with retries and decodes the first JSON object of the answer.
func (c *Client) call(ctx context.Context, path string, req, resp any) error {
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("etcdkv %s: encode: %w", path, err)
	}
	start := int(c.next.Load())
	var last error
	delay := c.backoff
	for attempt := range c.attempts {
		if attempt > 0 {
			t := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				t.Stop()
				return fmt.Errorf("etcdkv %s: %w: %w (last error: %w)", path, kv.ErrUnavailable, ctx.Err(), last)
			case <-t.C:
			}
			delay *= 2
		}
		ep := c.endpoints[(start+attempt)%len(c.endpoints)]
		retry, err := c.once(ctx, ep+path, body, resp)
		if err == nil {
			return nil
		}
		if !retry {
			return err
		}
		last = err
		c.next.Store(uint32((start + attempt + 1) % len(c.endpoints)))
	}
	return fmt.Errorf("etcdkv %s: %w after %d attempts: %w", path, kv.ErrUnavailable, c.attempts, last)
}

// once sends one HTTP request; it reports whether a failure is worth retrying.
func (c *Client) once(ctx context.Context, u string, body []byte, resp any) (bool, error) {
	rctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	hr, err := http.NewRequestWithContext(rctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return false, fmt.Errorf("etcdkv: request: %w", err)
	}
	hr.Header.Set("Content-Type", "application/json")
	res, err := c.hc.Do(hr)
	if err != nil {
		return ctx.Err() == nil, fmt.Errorf("etcdkv %s: %w", u, err)
	}
	defer func() { _ = res.Body.Close() }() // read-only body; a close error changes nothing
	r := io.LimitReader(res.Body, maxResponseBytes)
	if res.StatusCode != http.StatusOK {
		var ge gatewayError
		raw, _ := io.ReadAll(r) // best effort: the status code already says it failed
		_ = json.Unmarshal(raw, &ge)
		msg := ge.Message
		if msg == "" {
			msg = ge.Error
		}
		if strings.Contains(msg, "lease not found") {
			return false, fmt.Errorf("etcdkv %s: %w", u, kv.ErrLeaseNotFound)
		}
		return res.StatusCode >= 500, fmt.Errorf("etcdkv %s: HTTP %d: %s", u, res.StatusCode, truncate(msg, 200))
	}
	if err := json.NewDecoder(r).Decode(resp); err != nil {
		return true, fmt.Errorf("etcdkv %s: decode: %w", u, err)
	}
	return false, nil
}

// truncate shortens an error message from the server.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// ttlSeconds rounds a TTL up to whole seconds, as etcd counts them.
func ttlSeconds(ttl time.Duration) int64 {
	return int64(math.Ceil(ttl.Seconds()))
}

// Grant creates a lease; etcd counts TTLs in seconds and may raise a short one to its minimum.
func (c *Client) Grant(ctx context.Context, ttl time.Duration) (kv.LeaseID, time.Duration, error) {
	if ttl <= 0 {
		return 0, 0, fmt.Errorf("grant: ttl %v must be positive", ttl)
	}
	var r leaseResp
	if err := c.call(ctx, "/v3/lease/grant", map[string]string{"TTL": strconv.FormatInt(ttlSeconds(ttl), 10)}, &r); err != nil {
		return 0, 0, err
	}
	if r.ID == 0 || r.TTL <= 0 {
		return 0, 0, fmt.Errorf("grant: etcd returned id %d ttl %d", r.ID, r.TTL)
	}
	return kv.LeaseID(r.ID), time.Duration(r.TTL) * time.Second, nil
}

// KeepAlive renews a lease with one keep-alive message.
func (c *Client) KeepAlive(ctx context.Context, id kv.LeaseID) (time.Duration, error) {
	var r struct {
		Result *leaseResp    `json:"result"`
		Error  *gatewayError `json:"error"`
	}
	if err := c.call(ctx, "/v3/lease/keepalive", map[string]string{"ID": strconv.FormatInt(int64(id), 10)}, &r); err != nil {
		return 0, err
	}
	if r.Error != nil {
		if strings.Contains(r.Error.Message, "lease not found") {
			return 0, fmt.Errorf("keepalive %d: %w", id, kv.ErrLeaseNotFound)
		}
		return 0, fmt.Errorf("keepalive %d: %s", id, truncate(r.Error.Message, 200))
	}
	if r.Result == nil || r.Result.TTL <= 0 {
		return 0, fmt.Errorf("keepalive %d: %w", id, kv.ErrLeaseNotFound)
	}
	return time.Duration(r.Result.TTL) * time.Second, nil
}

// Revoke ends a lease and deletes its keys.
func (c *Client) Revoke(ctx context.Context, id kv.LeaseID) error {
	var r struct{}
	if err := c.call(ctx, "/v3/lease/revoke", map[string]string{"ID": strconv.FormatInt(int64(id), 10)}, &r); err != nil {
		return fmt.Errorf("revoke %d: %w", id, err)
	}
	return nil
}

// CreateIfAbsent runs a transaction: if the key's create revision is 0, put it on the lease, else read it.
func (c *Client) CreateIfAbsent(ctx context.Context, key, value string, id kv.LeaseID) (bool, kv.KeyValue, error) {
	if err := kv.ValidateKV(key, value); err != nil {
		return false, kv.KeyValue{}, fmt.Errorf("create: %w", err)
	}
	lease := strconv.FormatInt(int64(id), 10)
	req := map[string]any{
		"compare": []map[string]string{{"key": b64(key), "target": "CREATE", "result": "EQUAL", "create_revision": "0"}},
		"success": []map[string]any{{"request_put": map[string]string{"key": b64(key), "value": b64(value), "lease": lease}}},
		"failure": []map[string]any{{"request_range": map[string]string{"key": b64(key)}}},
	}
	var r txnResp
	if err := c.call(ctx, "/v3/kv/txn", req, &r); err != nil {
		return false, kv.KeyValue{}, fmt.Errorf("create %q: %w", key, err)
	}
	if r.Succeeded {
		rev := int64(r.Header.Revision)
		if rev <= 0 {
			return false, kv.KeyValue{}, fmt.Errorf("create %q: etcd returned revision %d", key, rev)
		}
		return true, kv.KeyValue{Key: key, Value: value, CreateRevision: rev, ModRevision: rev, Lease: id}, nil
	}
	if len(r.Responses) != 1 || r.Responses[0].ResponseRange == nil || len(r.Responses[0].ResponseRange.KVs) != 1 {
		return false, kv.KeyValue{}, fmt.Errorf("create %q: compare failed but no current value returned", key)
	}
	cur, err := toKV(r.Responses[0].ResponseRange.KVs[0])
	return false, cur, err
}

// DeleteIfLease deletes the key in a transaction that compares its lease.
func (c *Client) DeleteIfLease(ctx context.Context, key string, id kv.LeaseID) (bool, error) {
	req := map[string]any{
		"compare": []map[string]string{{"key": b64(key), "target": "LEASE", "result": "EQUAL", "lease": strconv.FormatInt(int64(id), 10)}},
		"success": []map[string]any{{"request_delete_range": map[string]string{"key": b64(key)}}},
	}
	var r txnResp
	if err := c.call(ctx, "/v3/kv/txn", req, &r); err != nil {
		return false, fmt.Errorf("delete %q: %w", key, err)
	}
	return r.Succeeded, nil
}

// Get reads one key (linearizable: the gateway's range is not serializable by default).
func (c *Client) Get(ctx context.Context, key string) (kv.KeyValue, bool, error) {
	var r rangeResp
	if err := c.call(ctx, "/v3/kv/range", map[string]string{"key": b64(key)}, &r); err != nil {
		return kv.KeyValue{}, false, fmt.Errorf("get %q: %w", key, err)
	}
	switch len(r.KVs) {
	case 0:
		return kv.KeyValue{}, false, nil
	case 1:
		e, err := toKV(r.KVs[0])
		return e, err == nil, err
	default:
		return kv.KeyValue{}, false, errors.New("get: more than one key returned for a single-key range")
	}
}
