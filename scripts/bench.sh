#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
out="${1:-benchdata/benchmark_$(date +%Y%m%d_%H%M%S).txt}"
mkdir -p "$(dirname "$out")"
echo "Writing: $out" >&2
go test ./radixdb/... -bench=. -benchmem -count=5 -timeout=30m 2>&1 | tee "$out"
