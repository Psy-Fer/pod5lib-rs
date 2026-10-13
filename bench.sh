#!/usr/bin/env bash
# bench.sh — benchmark pod5lib-rs vs Python pod5, sequential read and streaming write.
#
# Usage:
#   ./bench.sh <file.pod5> [first_n_for_write] [par_threads]
#
# first_n_for_write defaults to 50000.  Set to 0 to copy the whole file.
# par_threads defaults to 16.
# Python results are skipped (shown as "n/a") if pod5 is not importable.
#
# A virtualenv is created at .venv/ on first run and pod5 is installed into it.
# To use a specific pod5 version, create the venv yourself first:
#   python3 -m venv .venv && .venv/bin/pip install "pod5==x.y.z"

set -euo pipefail

FILE=${1:?usage: ./bench.sh <file.pod5> [first_n] [par_threads]}
FIRST_N=${2:-50000}
PAR_THREADS=${3:-16}
REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV="$REPO_DIR/.venv"

# ── Python virtualenv ──────────────────────────────────────────────────────────
if [[ ! -x "$VENV/bin/python3" ]]; then
    echo "Creating virtualenv at $VENV..." >&2
    python3 -m venv "$VENV" >&2
fi
if ! "$VENV/bin/python3" -c "import pod5" 2>/dev/null; then
    echo "Installing pod5 into virtualenv..." >&2
    "$VENV/bin/pip" install --quiet pod5 >&2
fi
PYTHON="$VENV/bin/python3"

# ── build Rust binary ──────────────────────────────────────────────────────────
echo "Building (release)..." >&2
cargo build --release --example bench_pod5 --manifest-path "$REPO_DIR/Cargo.toml" 2>&1 | grep -v "^$" >&2
BIN="$REPO_DIR/target/release/examples/bench_pod5"

# ── write Python bench helper to a temp file ───────────────────────────────────
PYFILE=$(mktemp /tmp/pod5_bench_XXXXXX.py)
trap 'rm -f "$PYFILE"' EXIT

cat > "$PYFILE" << 'PYEOF'
import sys, time, resource, tempfile, os, itertools

mode   = sys.argv[1]
path   = sys.argv[2]
first_n = int(sys.argv[3]) if len(sys.argv) > 3 else None

try:
    import pod5
except ImportError:
    print("UNAVAILABLE")
    sys.exit(0)

def rss_mb():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024

if mode == "sequential":
    t0 = time.perf_counter()
    n = 0
    with pod5.Reader(path) as r:
        for read in r.reads():
            _ = read.signal
            n += 1
    ms = (time.perf_counter() - t0) * 1000
    print(f"{ms:.0f} {rss_mb()} {n}")

elif mode == "write":
    out = os.path.join(tempfile.gettempdir(), "pod5_bench_py_out.pod5")
    # pod5.Writer refuses to overwrite an existing file
    if os.path.exists(out):
        os.remove(out)
    cap = first_n
    t0 = time.perf_counter()
    with pod5.Reader(path) as reader:
        with pod5.Writer(out) as writer:
            for read in itertools.islice(reader.reads(), cap):
                writer.add_read(read.to_read())
    ms = (time.perf_counter() - t0) * 1000
    n = cap if cap is not None else 0
    bytes_written = os.path.getsize(out)
    os.remove(out)
    print(f"{ms:.0f} {rss_mb()} {n} {bytes_written}")
PYEOF

echo "" >&2

# ── helpers ────────────────────────────────────────────────────────────────────
jq_py() { "$PYTHON" -c "import sys,json; d=json.load(sys.stdin); print($1)"; }

ratio() {
    # ratio <a_ms> <b_ms> → "1.23×" or "0.89×"
    "$PYTHON" -c "a,b=float('$1'),float('$2'); print(f'{a/b:.2f}×') if b>0 else print('n/a')"
}

fmt_ratio() {
    # colour-code: faster=green, slower=red (only when running in a terminal)
    local r="$1"
    if [[ -t 1 ]]; then
        "$PYTHON" -c "
r = float('$r'.rstrip('×'))
col = '\033[32m' if r < 1.0 else '\033[31m'
print(f'{col}{r:.2f}×\033[0m')
"
    else
        echo "$r"
    fi
}

# ── Rust sequential read ───────────────────────────────────────────────────────
echo "Running Rust sequential read..." >&2
RS_SEQ_JSON=$("$BIN" sequential "$FILE" 2>/dev/null)
RS_SEQ_MS=$(echo "$RS_SEQ_JSON"  | jq_py "f\"{d['wall_ms']:.0f}\"")
RS_SEQ_RSS=$(echo "$RS_SEQ_JSON" | jq_py "f\"{d['peak_rss_kb']//1024}\"")
SEQ_N=$(echo "$RS_SEQ_JSON"      | jq_py "d['n_reads']")

# ── Rust parallel sequential read ─────────────────────────────────────────────
echo "Running Rust par-sequential (threads=$PAR_THREADS)..." >&2
RS_PAR_JSON=$("$BIN" par-sequential "$FILE" "$PAR_THREADS" 2>/dev/null)
RS_PAR_MS=$(echo "$RS_PAR_JSON"  | jq_py "f\"{d['wall_ms']:.0f}\"")
RS_PAR_RSS=$(echo "$RS_PAR_JSON" | jq_py "f\"{d['peak_rss_kb']//1024}\"")

# ── Python sequential read ─────────────────────────────────────────────────────
echo "Running Python sequential read..." >&2
PY_SEQ_OUT=$("$PYTHON" "$PYFILE" sequential "$FILE" 2>/dev/null)
if [[ "$PY_SEQ_OUT" == "UNAVAILABLE" ]]; then
    PY_SEQ_MS="n/a"; PY_SEQ_RSS="n/a"
else
    PY_SEQ_MS=$(echo "$PY_SEQ_OUT"  | awk '{print $1}')
    PY_SEQ_RSS=$(echo "$PY_SEQ_OUT" | awk '{print $2}')
fi

# ── Rust write ─────────────────────────────────────────────────────────────────
WRITE_CAP=$FIRST_N
[[ "$FIRST_N" -eq 0 ]] && WRITE_CAP=$SEQ_N
echo "Running Rust write (first $WRITE_CAP reads)..." >&2
RS_WR_JSON=$("$BIN" write "$FILE" 64 "$WRITE_CAP" 2>/dev/null)
RS_WR_MS=$(echo "$RS_WR_JSON"  | jq_py "f\"{d['wall_ms']:.0f}\"")
RS_WR_RSS=$(echo "$RS_WR_JSON" | jq_py "f\"{d['peak_rss_kb']//1024}\"")
WR_N=$(echo "$RS_WR_JSON"      | jq_py "d['n_reads']")
WR_MB=$(echo "$RS_WR_JSON"     | jq_py "f\"{(d.get('bytes_written') or 0)/1e6:.1f}\"")

# ── Python write ───────────────────────────────────────────────────────────────
echo "Running Python write (first $WRITE_CAP reads)..." >&2
PY_WR_OUT=$("$PYTHON" "$PYFILE" write "$FILE" "$WRITE_CAP" 2>/dev/null)
if [[ "$PY_WR_OUT" == "UNAVAILABLE" ]]; then
    PY_WR_MS="n/a"; PY_WR_RSS="n/a"
else
    PY_WR_MS=$(echo "$PY_WR_OUT"  | awk '{print $1}')
    PY_WR_RSS=$(echo "$PY_WR_OUT" | awk '{print $2}')
fi

# ── compute ratios ─────────────────────────────────────────────────────────────
if [[ "$PY_SEQ_MS" != "n/a" ]]; then
    SEQ_SPEED_RATIO=$(ratio "$RS_SEQ_MS" "$PY_SEQ_MS")
    SEQ_RSS_RATIO=$(ratio "$RS_SEQ_RSS" "$PY_SEQ_RSS")
    PAR_SPEED_RATIO=$(ratio "$RS_PAR_MS" "$PY_SEQ_MS")
    PAR_RSS_RATIO=$(ratio "$RS_PAR_RSS" "$PY_SEQ_RSS")
else
    SEQ_SPEED_RATIO="n/a"; SEQ_RSS_RATIO="n/a"
    PAR_SPEED_RATIO="n/a"; PAR_RSS_RATIO="n/a"
fi
PAR_VS_SEQ=$(ratio "$RS_PAR_MS" "$RS_SEQ_MS")

if [[ "$PY_WR_MS" != "n/a" ]]; then
    WR_SPEED_RATIO=$(ratio "$RS_WR_MS" "$PY_WR_MS")
    WR_RSS_RATIO=$(ratio "$RS_WR_RSS" "$PY_WR_RSS")
else
    WR_SPEED_RATIO="n/a"; WR_RSS_RATIO="n/a"
fi

# ── summary table ──────────────────────────────────────────────────────────────
FILE_MB=$("$PYTHON" -c "import os; print(f\"{os.path.getsize('$FILE')/1e6:.0f}\")")
CORES=$(nproc 2>/dev/null || "$PYTHON" -c "import os; print(os.cpu_count())")

echo ""
echo "=== pod5lib-rs vs Python pod5 ========================================"
echo "  file      : $FILE  (${FILE_MB} MB)"
echo "  reads     : $SEQ_N total"
echo "  CPU cores : $CORES"
echo "----------------------------------------------------------------------"
printf "  %-42s  %s\n" "" "time (ms)   RSS (MB)   Rust/Python"
echo "----------------------------------------------------------------------"
printf "  %-42s  %8s ms  %6s MB\n" \
    "Rust  sequential read ($SEQ_N reads)" "$RS_SEQ_MS" "$RS_SEQ_RSS"
printf "  %-42s  %8s ms  %6s MB   speed %s  RAM %s\n" \
    "Rust  par-sequential (${PAR_THREADS} threads)" "$RS_PAR_MS" "$RS_PAR_RSS" \
    "$PAR_VS_SEQ" "—"
printf "  %-42s  %8s ms  %6s MB   speed %s  RAM %s\n" \
    "Python sequential read" "$PY_SEQ_MS" "$PY_SEQ_RSS" "$SEQ_SPEED_RATIO" "$SEQ_RSS_RATIO"
echo ""
printf "  %-42s  %8s ms  %6s MB   → ${WR_MB} MB written\n" \
    "Rust  write (first $WR_N reads)" "$RS_WR_MS" "$RS_WR_RSS"
printf "  %-42s  %8s ms  %6s MB   speed %s  RAM %s\n" \
    "Python write (first $WR_N reads)" "$PY_WR_MS" "$PY_WR_RSS" "$WR_SPEED_RATIO" "$WR_RSS_RATIO"
echo "======================================================================"
echo "  (Rust/Python ratio <1.0× means Rust is faster)"
