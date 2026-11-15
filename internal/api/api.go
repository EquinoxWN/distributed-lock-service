// Package api serves locks over HTTP for clients in any language.
//
// The service keeps no state of its own: every call goes to the store, so several instances can
// serve the same store. Acquiring returns a random owner secret once; the caller renews with
// POST /v1/locks/{name}/keepalive at about a third of the TTL and releases with
// DELETE /v1/locks/{name}, both with the secret in the Lock-Secret header. Lease ids are not
// accepted as proof of ownership. If the caller stops renewing, the lease expires and the lock is freed.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/EquinoxWN/distributed-lock-service/internal/kv"
	"github.com/EquinoxWN/distributed-lock-service/internal/lock"
)

// SecretHeader carries the owner secret on keep-alive and release.
const SecretHeader = "Lock-Secret"

const (
	maxBody   = 4 << 10
	opTimeout = 5 * time.Second
)

// Server handles the HTTP API.
type Server struct {
	locks *lock.Client
	log   *slog.Logger
}

// New returns a server over a store; log may be nil.
func New(b kv.Backend, log *slog.Logger) (*Server, error) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	c, err := lock.New(lock.Config{Backend: b, OpTimeout: opTimeout})
	if err != nil {
		return nil, err
	}
	return &Server{locks: c, log: log}, nil
}

// Handler returns the routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/locks/{name}", s.acquire)
	mux.HandleFunc("GET /v1/locks/{name}", s.get)
	mux.HandleFunc("DELETE /v1/locks/{name}", s.release)
	mux.HandleFunc("POST /v1/locks/{name}/keepalive", s.keepAlive)
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

// AcquireResponse is returned when the lock is taken; Secret is shown only here.
type AcquireResponse struct {
	Name   string     `json:"name"`
	Owner  string     `json:"owner"`
	Token  int64      `json:"token"`
	Lease  kv.LeaseID `json:"lease_id,string"`
	TTLMs  int64      `json:"ttl_ms"`
	Secret string     `json:"secret"`
}

type errorBody struct {
	Error   string       `json:"error"`
	Message string       `json:"message"`
	Holder  *lock.Holder `json:"holder,omitempty"`
}

// writeJSON writes one JSON answer.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	// The status line is sent; an encode error here means the client went away.
	_ = json.NewEncoder(w).Encode(v)
}

// fail maps an error to a status and a stable error code; logs never include request headers.
func (s *Server) fail(w http.ResponseWriter, r *http.Request, err error) {
	var held *lock.HeldError
	switch {
	case errors.As(err, &held):
		writeJSON(w, http.StatusConflict, errorBody{Error: "held", Message: err.Error(), Holder: &held.Holder})
	case errors.Is(err, lock.ErrInvalid):
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid", Message: err.Error()})
	case errors.Is(err, lock.ErrSecretRequired):
		writeJSON(w, http.StatusUnauthorized, errorBody{Error: "secret_required", Message: "send the owner secret in the " + SecretHeader + " header"})
	case errors.Is(err, lock.ErrForbidden):
		writeJSON(w, http.StatusForbidden, errorBody{Error: "forbidden", Message: "owner secret does not match the lock holder"})
	case errors.Is(err, kv.ErrLeaseNotFound):
		writeJSON(w, http.StatusNotFound, errorBody{Error: "not_held", Message: "lock not held: lease expired, revoked or released"})
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

// acquire takes the lock and returns its secret once, or answers 409 with the holder.
func (s *Server) acquire(w http.ResponseWriter, r *http.Request) {
	var req AcquireRequest
	if err := decode(r, w, &req); err != nil {
		s.fail(w, r, err)
		return
	}
	ttl := time.Duration(req.TTLMs) * time.Millisecond
	if req.TTLMs <= 0 || req.TTLMs > int64(lock.MaxTTL/time.Millisecond) {
		ttl = 0 // rejected by Validate with the allowed range
	}
	g, err := s.locks.Grant(r.Context(), r.PathValue("name"), req.Owner, ttl)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, AcquireResponse{Name: g.Name, Owner: g.Owner, Token: g.Token, Lease: g.Lease, TTLMs: g.TTL.Milliseconds(), Secret: g.Secret})
}

// get returns the current holder (never its secret).
func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	h, ok, err := s.locks.Get(r.Context(), r.PathValue("name"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if !ok {
		writeJSON(w, http.StatusNotFound, errorBody{Error: "not_held", Message: "nobody holds this lock"})
		return
	}
	writeJSON(w, http.StatusOK, h)
}

// release frees the lock if the secret is the holder's; a lock nobody holds answers 204 too.
func (s *Server) release(w http.ResponseWriter, r *http.Request) {
	if err := s.locks.Release(r.Context(), r.PathValue("name"), r.Header.Get(SecretHeader)); err != nil {
		s.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// keepAlive renews the holder's lease if the secret matches and returns its TTL.
func (s *Server) keepAlive(w http.ResponseWriter, r *http.Request) {
	ttl, err := s.locks.KeepAlive(r.Context(), r.PathValue("name"), r.Header.Get(SecretHeader))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ttl_ms": ttl.Milliseconds()})
}
