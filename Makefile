.PHONY: setup lint test test-etcd bench demo serve audit ci

setup:
	go mod download

lint:
	@files="$$(gofmt -l cmd internal)"; test -z "$$files" || (echo "$$files"; echo "run gofmt -w"; exit 1)
	go vet ./...
	go vet -tags etcd ./...

test:
	go test -race -count=1 ./...

# Needs a running etcd cluster (CI starts three members): DLS_ETCD_ENDPOINTS=http://127.0.0.1:2379,...
test-etcd:
	go test -tags etcd -count=1 -v -run TestEtcd ./internal/etcdkv

# The numbers in the README: in-process store, 8 clients x 25 rounds, TTL 300 ms.
bench:
	go run ./cmd/distributed-lock-service demo

demo: bench

serve:
	go run ./cmd/distributed-lock-service serve

# Known vulnerabilities in called code (Go 1.26.11 as in CI); any finding fails.
audit:
	bash scripts/vulncheck.sh

ci: setup lint test
