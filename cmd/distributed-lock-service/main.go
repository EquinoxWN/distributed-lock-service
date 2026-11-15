// Command distributed-lock-service serves leased locks with fencing tokens over HTTP, or runs the local demo.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/EquinoxWN/distributed-lock-service/internal/api"
	"github.com/EquinoxWN/distributed-lock-service/internal/etcdkv"
	"github.com/EquinoxWN/distributed-lock-service/internal/kv"
	"github.com/EquinoxWN/distributed-lock-service/internal/memkv"
)

const usage = `usage:
  distributed-lock-service serve [-addr 127.0.0.1:8080] [-etcd http://127.0.0.1:2379,...]
  distributed-lock-service demo  [-clients 8] [-rounds 25] [-ttl 300ms]`

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// run dispatches the subcommand.
func run(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New(usage)
	}
	switch args[0] {
	case "serve":
		return serve(args[1:], out)
	case "demo":
		cfg, err := parseDemo(args[1:])
		if err != nil {
			return err
		}
		return demo(cfg, out)
	default:
		return fmt.Errorf("unknown command %q\n%s", args[0], usage)
	}
}

// backend builds the store: etcd when endpoints are given, else the in-process store.
func backend(endpoints string) (kv.Backend, string, error) {
	if endpoints == "" {
		return memkv.New(nil), "in-process store (single instance, state lost on exit)", nil
	}
	c, err := etcdkv.New(etcdkv.Options{Endpoints: strings.Split(endpoints, ",")})
	if err != nil {
		return nil, "", err
	}
	return c, "etcd " + endpoints, nil
}

// utcTime writes log timestamps in UTC.
func utcTime(_ []string, a slog.Attr) slog.Attr {
	if a.Key == slog.TimeKey && a.Value.Kind() == slog.KindTime {
		a.Value = slog.TimeValue(a.Value.Time().UTC())
	}
	return a
}

// serve runs the HTTP API until interrupted.
func serve(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(out)
	addr := fs.String("addr", "127.0.0.1:8080", "listen address")
	etcd := fs.String("etcd", "", "comma-separated etcd endpoints; empty uses the in-process store")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected arguments %v", fs.Args())
	}
	b, desc, err := backend(*etcd)
	if err != nil {
		return err
	}
	log := slog.New(slog.NewJSONHandler(out, &slog.HandlerOptions{ReplaceAttr: utcTime}))
	handler, err := api.New(b, log)
	if err != nil {
		return err
	}
	srv := &http.Server{Addr: *addr, Handler: handler.Handler(), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	log.Info("serving", "addr", ln.Addr().String(), "store", desc)
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(sctx)
}
