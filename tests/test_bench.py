#!/usr/bin/env python3
"""
Performance benchmark: pod5-rs (Rust) vs Python pod5 library.

Measures wall time and peak RSS for:
  - Sequential scan: iterate all reads in file order, decompress pA signal
  - Random access:   all reads in shuffled order, look up by read ID

Each Python run is isolated in its own subprocess for clean RSS measurement.
The Rust binary reports its own peak RSS via /proc/self/status VmHWM.

Requirements:
    pip install pod5

Usage (standalone):
    python tests/test_bench.py [path/to/file.pod5]

Usage (pytest):
    pytest tests/test_bench.py -s
"""

import json
import pathlib
import random
import subprocess
import sys
import textwrap

ROOT = pathlib.Path(__file__).parent.parent

# ── File discovery ─────────────────────────────────────────────────────────────


def find_test_file() -> pathlib.Path | None:
    for candidate in [
        ROOT / "tests/data/sample.pod5",
        ROOT / "examples/small.pod5",
    ]:
        if candidate.exists():
            return candidate
    return None


# ── Rust side ─────────────────────────────────────────────────────────────────


def _cargo_run_bench(mode: str, pod5_path: pathlib.Path, stdin_text: str | None = None) -> dict:
    """Build (once, cached) and run bench_pod5 in the given mode; return parsed JSON."""
    cmd = [
        "cargo", "run", "--example", "bench_pod5",
        "--release", "--quiet",
        "--", mode, str(pod5_path),
    ]
    result = subprocess.run(
        cmd,
        input=stdin_text,
        capture_output=True,
        text=True,
        cwd=ROOT,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"bench_pod5 {mode} failed\n"
            f"--- stdout ---\n{result.stdout}\n"
            f"--- stderr ---\n{result.stderr}"
        )
    return json.loads(result.stdout)


def rust_sequential(pod5_path: pathlib.Path) -> dict:
    return _cargo_run_bench("sequential", pod5_path)


def rust_random(pod5_path: pathlib.Path, ids: list[str]) -> dict:
    """Pass shuffled IDs via stdin to avoid argument-length limits."""
    return _cargo_run_bench("random", pod5_path, stdin_text="\n".join(ids) + "\n")


# ── Python side (each run isolated in its own subprocess) ─────────────────────

_PYTHON_SEQ_SCRIPT = textwrap.dedent("""\
    import json, resource, sys, time, warnings
    try:
        import pod5
    except ImportError:
        print(json.dumps({"error": "pod5 not installed"}))
        sys.exit(1)

    warnings.filterwarnings("ignore", category=DeprecationWarning)
    pod5_path = sys.argv[1]

    t0 = time.perf_counter()
    n = 0
    read_ids = []
    with pod5.Reader(pod5_path) as reader:
        for rec in reader.reads():
            read_ids.append(str(rec.read_id))
            _ = rec.signal_pa
            n += 1

    wall_ms = (time.perf_counter() - t0) * 1000
    peak_rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

    print(json.dumps({
        "mode": "sequential",
        "n_reads": n,
        "wall_ms": wall_ms,
        "peak_rss_kb": peak_rss_kb,
        "read_ids": read_ids,
    }))
""")

_PYTHON_RANDOM_SCRIPT = textwrap.dedent("""\
    import json, resource, sys, time, warnings
    try:
        import pod5
    except ImportError:
        print(json.dumps({"error": "pod5 not installed"}))
        sys.exit(1)

    warnings.filterwarnings("ignore", category=DeprecationWarning)
    pod5_path, ids_path = sys.argv[1], sys.argv[2]

    with open(ids_path) as fh:
        ids = [l.strip() for l in fh if l.strip()]

    t0 = time.perf_counter()
    n = 0
    with pod5.Reader(pod5_path) as reader:
        # selection= returns reads in file order but filtered to the requested IDs.
        # This is the fastest Python path for fetching a subset by read ID.
        for rec in reader.reads(selection=ids):
            _ = rec.signal_pa
            n += 1

    wall_ms = (time.perf_counter() - t0) * 1000
    peak_rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

    print(json.dumps({
        "mode": "random",
        "n_reads": n,
        "wall_ms": wall_ms,
        "peak_rss_kb": peak_rss_kb,
    }))
""")


def _run_python_script(script: str, extra_args: list[str]) -> dict:
    result = subprocess.run(
        [sys.executable, "-c", script, *extra_args],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Python benchmark script failed\n"
            f"--- stdout ---\n{result.stdout}\n"
            f"--- stderr ---\n{result.stderr}"
        )
    data = json.loads(result.stdout)
    if "error" in data:
        raise RuntimeError(data["error"])
    return data


def python_sequential(pod5_path: pathlib.Path) -> dict:
    return _run_python_script(_PYTHON_SEQ_SCRIPT, [str(pod5_path)])


def python_random(pod5_path: pathlib.Path, ids: list[str]) -> dict:
    import tempfile, os

    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as fh:
        fh.write("\n".join(ids) + "\n")
        tmp = fh.name
    try:
        return _run_python_script(_PYTHON_RANDOM_SCRIPT, [str(pod5_path), tmp])
    finally:
        os.unlink(tmp)


# ── Reporting ─────────────────────────────────────────────────────────────────


def _row(label: str, rust_val: str, py_val: str, ratio: str) -> str:
    return f"  {label:<22s}  {rust_val:>10s}  {py_val:>10s}  {ratio:>9s}"


def print_table(r: dict, p: dict, title: str) -> None:
    n = r["n_reads"]
    r_ms, p_ms = r["wall_ms"], p["wall_ms"]
    r_mb = r["peak_rss_kb"] / 1024
    p_mb = p["peak_rss_kb"] / 1024
    speedup = p_ms / r_ms if r_ms > 0 else float("inf")
    mem_ratio = p_mb / r_mb if r_mb > 0 else float("inf")

    header = f"  {title}  ({n:,} reads)"
    bar = "─" * 62
    print(f"\n{bar}")
    print(header)
    print(bar)
    print(_row("", "Rust", "Python", "ratio"))
    print(_row("Wall time (ms)", f"{r_ms:,.1f}", f"{p_ms:,.1f}", f"{speedup:.2f}x"))
    print(_row("Peak RSS (MB)", f"{r_mb:,.1f}", f"{p_mb:,.1f}", f"{mem_ratio:.2f}x"))
    print(bar)


# ── Runner ─────────────────────────────────────────────────────────────────────


def run_bench(pod5_path: pathlib.Path) -> None:
    print(f"\nBenchmark: {pod5_path}")
    print("Building Rust bench binary (release, first run may take ~30 s)…")

    # ── Rust sequential (also collects all read IDs) ───────────────────────
    print("\n[1/4] Rust   sequential…", end="", flush=True)
    r_seq = rust_sequential(pod5_path)
    print(f"  {r_seq['n_reads']:,} reads, {r_seq['wall_ms']:,.0f} ms")

    all_ids = r_seq["read_ids"]
    shuffled = all_ids.copy()
    random.shuffle(shuffled)

    # ── Rust random ────────────────────────────────────────────────────────
    print(f"[2/4] Rust   random ({len(shuffled):,} IDs, shuffled)…", end="", flush=True)
    r_rnd = rust_random(pod5_path, shuffled)
    print(f"  {r_rnd['n_reads']:,} reads, {r_rnd['wall_ms']:,.0f} ms")

    # ── Python sequential ──────────────────────────────────────────────────
    print("[3/4] Python sequential…", end="", flush=True)
    p_seq = python_sequential(pod5_path)
    print(f"  {p_seq['n_reads']:,} reads, {p_seq['wall_ms']:,.0f} ms")

    # ── Python random ──────────────────────────────────────────────────────
    print(f"[4/4] Python random ({len(shuffled):,} IDs, shuffled)…", end="", flush=True)
    p_rnd = python_random(pod5_path, shuffled)
    print(f"  {p_rnd['n_reads']:,} reads, {p_rnd['wall_ms']:,.0f} ms")

    # ── Results ────────────────────────────────────────────────────────────
    print_table(r_seq, p_seq, "Sequential scan (all reads, file order)")
    print_table(r_rnd, p_rnd, "Random access  (all reads, shuffled)")

    print()
    print("Notes:")
    print("  * ratio = Python / Rust:  >1.0 → Rust wins,  <1.0 → Python wins")
    print("  * Rust RSS  = VmHWM from /proc/self/status (peak for entire process lifetime)")
    print("  * Python RSS = ru_maxrss (peak for entire process lifetime)")
    print("  * Both include: file open, index build, signal decompression")
    print("  * Python random uses reader.reads(selection=...) — filtered file-order scan")
    print("  * Rust random uses get_read_by_id()            — O(1) HashMap + in-memory signal")


# ── pytest integration ─────────────────────────────────────────────────────────


def test_bench_vs_python() -> None:
    """pytest discovers this automatically."""
    pod5_path = find_test_file()
    if pod5_path is None:
        try:
            import pytest
            pytest.skip("no POD5 test file")
        except ImportError:
            pass
        return
    try:
        import pod5  # noqa: F401
    except ImportError:
        try:
            import pytest
            pytest.skip("Python pod5 library not installed (pip install pod5)")
        except ImportError:
            pass
        return
    run_bench(pod5_path)


# ── Standalone entry point ─────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) > 1:
        path = pathlib.Path(sys.argv[1])
    else:
        path = find_test_file()

    if path is None:
        print(
            "ERROR: no POD5 file found.\n"
            "  Add tests/data/sample.pod5 or examples/small.pod5,\n"
            "  or pass the path explicitly:  python tests/test_bench.py <file.pod5>",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        run_bench(path)
    except RuntimeError as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        sys.exit(1)
