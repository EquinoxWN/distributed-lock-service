// Package api serves locks over HTTP for clients in any language.
//
// The service keeps no state of its own: every call goes to the store, so several instances can
// serve the same store. The caller renews its lease with POST /v1/leases/{id}/keepalive at about a
// third of the TTL; if it stops, the lease expires in the store and the lock is freed.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/EquinoxWN/distributed-lock-service/internal/kv"
	"github.com/EquinoxWN/distributed-lock-service/internal/lock"
)

const (
	maxBody   = 4 << 10
	opTimeout = 5 * time.Second
)

// Server handles the HTTP API.
type Server struct {
	backend kv.Backend
	prefix  string
	log     *slog.Logger
}

// New returns a server over a store; log may be nil.
func New(b kv.Backend, log *slog.Logger) *Server {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Server{backend: b, prefix: lock.DefaultPrefix, log: log}
}

// Handler returns the routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/locks/{name}", s.acquire)
	mux.HandleFunc("GET /v1/locks/{name}", s.get)
	mux.HandleFunc("DELETE /v1/locks/{name}", s.release)
	mux.HandleFunc("POST /v1/leases/{id}/keepalive", s.keepAlive)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return mux
}

// AcquireRequest is the body of POST /v1/locks/{name}.
type AcquireRequest struct {
	Owner string `json:"owner"`
	TTLMs int64  `json:"ttl_ms"`
}

// AcquireResponse is returned when the lock is taken.
type AcquireResponse struct {
	Name  string     `json:"name"`
	Owner string     `json:"owner"`
	Token int64      `json:"token"`
	Lease kv.LeaseID `json:"lease_id,string"`
	TTLMs int64      `json:"ttl_ms"`
}

type errorBody struct {
	Error   string       `json:"error"`
	Message string       `json:"message"`
	Holder  *lock.Holder `json:"holder,omitempty"`
}

// writeJSON writes one JSON answer.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// The status line is sent; an encode error here means the client went away.
	_ = json.NewEncoder(w).Encode(v)
}

// fail maps an error to a status and a stable error code.
func (s *Server) fail(w http.ResponseWriter, r *http.Request, err error) {
	var held *lock.HeldError
	switch {
	case errors.As(err, &held):
		writeJSON(w, http.StatusConflict, errorBody{Error: "held", Message: err.Error(), Holder: &held.Holder})
	case errors.Is(err, lock.ErrInvalid):
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid", Message: err.Error()})
	case errors.Is(err, kv.ErrLeaseNotFound):
		writeJSON(w, http.StatusNotFound, errorBody{Error: "lease_not_found", Message: "lease expired, revoked or unknown"})
	case errors.Is(err, kv.ErrUnavailable), errors.Is(err, context.DeadlineExceeded):
		s.log.Warn("store unavailable", "path", r.URL.Path, "err", err)
		writeJSON(w, http.StatusServiceUnavailable, errorBody{Error: "unavailable", Message: "lock store unavailable, retry with backoff"})
	default:
		s.log.Error("request failed", "path", r.URL.Path, "err", err)
		writeJSON(w, http.StatusInternalServerError, errorBody{Error: "internal", Message: "internal error"})
	}
}

// decode reads a small JSON body with no unknown fields.
func decode(r *http.Request, w http.ResponseWriter, v any) error {
	if ct := r.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		return fmt.Errorf("%w: Content-Type must be application/json", lock.ErrInvalid)
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("%w: body: %w", lock.ErrInvalid, err)
	}
	if dec.More() {
		return fmt.Errorf("%w: body: trailing data", lock.ErrInvalid)
	}
	return nil
}

// leaseParam parses a lease id from a path or query value.
func leaseParam(v string) (kv.LeaseID, error) {
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%w: lease id %q", lock.ErrInvalid, v)
	}
	return kv.LeaseID(n), nil
}

// acquire grants a lease and creates the lock key on it, or answers 409 with the holder.
func (s *Server) acquire(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var req AcquireRequest
	if err := decode(r, w, &req); err != nil {
		s.fail(w, r, err)
		return
	}
	ttl := time.Duration(req.TTLMs) * time.Millisecond
	if req.TTLMs <= 0 || req.TTLMs > int64(lock.MaxTTL/time.Millisecond) {
		ttl = 0 // rejected by Validate with the allowed range
	}
	if err := lock.Validate(name, req.Owner, ttl); err != nil {
		s.fail(w, r, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), opTimeout)
	defer cancel()
	id, granted, err := s.backend.Grant(ctx, ttl)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	ok, cur, err := s.backend.CreateIfAbsent(ctx, s.prefix+name, req.Owner, id)
	if err == nil && (ok || cur.Lease == id) {
		writeJSON(w, http.StatusOK, AcquireResponse{Name: name, Owner: req.Owner, Token: cur.CreateRevision, Lease: id, TTLMs: granted.Milliseconds()})
		return
	}
	if rerr := s.backend.Revoke(context.WithoutCancel(ctx), id); rerr != nil && !errors.Is(rerr, kv.ErrLeaseNotFound) {
		err = errors.Join(err, fmt.Errorf("revoke unused lease %d: %w", id, rerr))
	}
	if err == nil {
		err = &lock.HeldError{Holder: lock.Holder{Owner: cur.Value, Token: cur.CreateRevision, Lease: cur.Lease}}
	}
	s.fail(w, r, err)
}

// get returns the current holder.
func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := lock.Validate(name, "x", lock.MinTTL); err != nil {
		s.fail(w, r, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), opTimeout)
	defer cancel()
	cur, ok, err := s.backend.Get(ctx, s.prefix+name)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if !ok {
		writeJSON(w, http.StatusNotFound, errorBody{Error: "not_held", Message: "nobody holds this lock"})
		return
	}
	writeJSON(w, http.StatusOK, lock.Holder{Owner: cur.Value, Token: cur.CreateRevision, Lease: cur.Lease})
}

// release deletes the key if the given lease holds it and revokes the lease; repeating it is harmless.
func (s *Server) release(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := lock.Validate(name, "x", lock.MinTTL); err != nil {
		s.fail(w, r, err)
		return
	}
	id, err := leaseParam(r.URL.Query().Get("lease_id"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), opTimeout)
	defer cancel()
	if _, err := s.backend.DeleteIfLease(ctx, s.prefix+name, id); err != nil {
		s.fail(w, r, err)
		return
	}
	if err := s.backend.Revoke(ctx, id); err != nil && !errors.Is(err, kv.ErrLeaseNotFound) {
		s.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// keepAlive renews a lease and returns its TTL.
func (s *Server) keepAlive(w http.ResponseWriter, r *http.Request) {
	id, err := leaseParam(r.PathValue("id"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), opTimeout)
	defer cancel()
	ttl, err := s.backend.KeepAlive(ctx, id)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"lease_id": strconv.FormatInt(int64(id), 10), "ttl_ms": ttl.Milliseconds()})
}
