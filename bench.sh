#!/usr/bin/env bash
# bench.sh — build and run pod5lib benchmarks against a real POD5 file.
#
# Usage:
#   ./bench.sh <file.pod5> [first_n_for_write]
#
# first_n_for_write defaults to 50000.  Set to 0 to copy the whole file.
#
# Outputs a summary table to stdout.  Raw JSON from each run goes to stderr
# (redirect with 2>/dev/null to suppress).

set -euo pipefail

FILE=${1:?usage: ./bench.sh <file.pod5> [first_n]}
FIRST_N=${2:-50000}
REPO_DIR="$(cd "$(dirname "$0")" && pwd)"

# ── build ──────────────────────────────────────────────────────────────────────
echo "Building (release)..." >&2
cargo build --release --example bench_pod5 --manifest-path "$REPO_DIR/Cargo.toml" 2>&1 | grep -v "^$" >&2
BIN="$REPO_DIR/target/release/examples/bench_pod5"

echo "" >&2

# ── sequential read ────────────────────────────────────────────────────────────
echo "Running sequential read..." >&2
SEQ_JSON=$("$BIN" sequential "$FILE" 2>/dev/null)
SEQ_MS=$(echo "$SEQ_JSON"   | python3 -c "import sys,json; d=json.load(sys.stdin); print(f\"{d['wall_ms']:.0f}\")")
SEQ_RSS=$(echo "$SEQ_JSON"  | python3 -c "import sys,json; d=json.load(sys.stdin); print(f\"{d['peak_rss_kb']//1024}\")")
SEQ_N=$(echo "$SEQ_JSON"    | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['n_reads'])")

# ── write (first_n reads) ──────────────────────────────────────────────────────
WRITE_CAP=$FIRST_N
[[ "$FIRST_N" -eq 0 ]] && WRITE_CAP=$SEQ_N
echo "Running write (first $WRITE_CAP reads)..." >&2
WR_JSON=$("$BIN" write "$FILE" 64 "$WRITE_CAP" 2>/dev/null)
WR_MS=$(echo "$WR_JSON"    | python3 -c "import sys,json; d=json.load(sys.stdin); print(f\"{d['wall_ms']:.0f}\")")
WR_RSS=$(echo "$WR_JSON"   | python3 -c "import sys,json; d=json.load(sys.stdin); print(f\"{d['peak_rss_kb']//1024}\")")
WR_N=$(echo "$WR_JSON"     | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['n_reads'])")
WR_MB=$(echo "$WR_JSON"    | python3 -c "import sys,json; d=json.load(sys.stdin); print(f\"{(d.get('bytes_written') or 0)/1e6:.1f}\")")

# ── summary ────────────────────────────────────────────────────────────────────
FILE_MB=$(python3 -c "import os; print(f\"{os.path.getsize('$FILE')/1e6:.0f}\")")
CORES=$(nproc 2>/dev/null || python3 -c "import os; print(os.cpu_count())")

echo ""
echo "=== pod5lib-rs benchmark ============================================="
echo "  file      : $FILE  (${FILE_MB} MB)"
echo "  reads     : $SEQ_N total"
echo "  CPU cores : $CORES"
echo "---------------------------------------------------------------------"
printf "  %-22s  %7s ms  %6s MB RSS\n" "sequential read ($SEQ_N reads)" "$SEQ_MS" "$SEQ_RSS"
printf "  %-22s  %7s ms  %6s MB RSS  →  ${WR_MB} MB written\n" "write (first $WR_N reads)" "$WR_MS" "$WR_RSS"
echo "====================================================================="
