#!/usr/bin/env bash
# Runs govulncheck (Go 1.26.11 as in CI); any finding fails.
set -euo pipefail
if out=$(go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 ./... 2>&1); then
  echo "$out" | tail -3
  echo "vulncheck: no findings"
  exit 0
fi
echo "$out" | grep -E '^Vulnerability #|Found in|Fixed in|Your code is affected' || echo "$out"
echo "::error::govulncheck reported findings"
exit 1
