# Contributing

Thanks for helping. This is a Go project; issues and pull requests are welcome.

## Set up and check your change

Needs Go 1.26 or newer (CI uses 1.26.10). There are no third-party Go modules. Caches stay in
git-ignored folders inside the repo.

```bash
make setup      # go mod download (nothing to fetch today)
make lint       # gofmt and go vet (also with the etcd build tag)
make test       # all local tests with the race detector (needs cgo; without a C compiler: go test ./...)
make audit      # known-vulnerability check (as in CI)
make demo       # the four lock scenarios on the in-process store
make test-etcd  # integration tests; needs an etcd cluster and DLS_ETCD_ENDPOINTS (CI runs three members)
```

A pull request is ready when `make lint` and `make test` pass and CI is green.

## Guidelines

- Keep each change focused; explain the problem it solves in the pull request.
- Add or update a test for every behaviour change, and update `docs/results/` when numbers change.
- A new store goes behind `kv.Backend` and must pass `internal/kv/kvtest` (the backend contract).
- Significant design changes need an ADR in `docs/adr/` (copy `0001` for the format).
- Keep function comments to one short phrase; let names and tests explain the rest.
- Report security problems privately, as described in [SECURITY.md](SECURITY.md).

By contributing you agree that your work is released under the [MIT License](LICENSE).
