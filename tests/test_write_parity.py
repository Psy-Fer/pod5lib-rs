#!/usr/bin/env python3
"""
Parity test: write a POD5 file with Rust, then read it with Python pod5.

Verifies that the Rust-written file is correctly structured and field values
match those from the original source file.

Requirements:
    pip install pod5 numpy

Usage (standalone):
    python tests/test_write_parity.py [path/to/source.pod5]

Usage (pytest):
    pytest tests/test_write_parity.py -s
"""

import math
import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).parent.parent
OUT_FILE = ROOT / "tests/data/rust_written.pod5"

N_READS = 10
# Full ADC signal is compared for the first FULL_SIGNAL_READS reads; the rest get
# a checksum comparison (sum of all samples) to keep the test fast on large files.
FULL_SIGNAL_READS = 5


def find_test_file() -> pathlib.Path | None:
    for candidate in [
        ROOT / "tests/data/sample.pod5",
        ROOT / "examples/small.pod5",
        ROOT / "tests/fixtures/multi_fast5_zip_v7.pod5",
    ]:
        if candidate.exists():
            return candidate
    return None


def run_rust_writer(src: pathlib.Path, out: pathlib.Path, n: int) -> None:
    result = subprocess.run(
        [
            "cargo", "run", "--example", "write_pod5",
            "--quiet",
            "--", str(src), str(out), str(n),
        ],
        capture_output=True,
        text=True,
        cwd=ROOT,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"write_pod5 example failed\n"
            f"--- stdout ---\n{result.stdout}\n"
            f"--- stderr ---\n{result.stderr}"
        )


def approx_eq(a: float, b: float, rel=1e-5, abs_tol=1e-7) -> bool:
    if math.isnan(a) and math.isnan(b):
        return True
    if math.isnan(a) or math.isnan(b):
        return False
    return abs(a - b) <= max(rel * max(abs(a), abs(b)), abs_tol)


def collect_reads(pod5, path: pathlib.Path, limit: int | None = None) -> dict:
    """Every read field Python pod5 exposes, plus the full run info, keyed by read ID."""
    reads = {}
    with pod5.Reader(str(path)) as reader:
        for i, rec in enumerate(reader.reads()):
            if limit is not None and i >= limit:
                break
            sig = rec.signal
            ri = rec.run_info
            fields = {
                "read_number": rec.read_number,
                "start_sample": rec.start_sample,
                "num_samples": rec.num_samples,
                "channel": rec.pore.channel,
                "well": rec.pore.well,
                "pore_type": rec.pore.pore_type,
                "calib_offset": float(rec.calibration.offset),
                "calib_scale": float(rec.calibration.scale),
                "median_before": float(rec.median_before),
                "end_reason": rec.end_reason.reason.name.lower(),
                "end_reason_forced": bool(rec.end_reason.forced),
                "num_minknow_events": rec.num_minknow_events,
                "tracked_shift": float(rec.tracked_scaling.shift),
                "tracked_scale": float(rec.tracked_scaling.scale),
                "predicted_shift": float(rec.predicted_scaling.shift),
                "predicted_scale": float(rec.predicted_scaling.scale),
                "num_reads_since_mux_change": rec.num_reads_since_mux_change,
                "time_since_mux_change": float(rec.time_since_mux_change),
                "signal_len": int(len(sig)),
                "signal_sum": int(sig.astype("int64").sum()),
                "signal_first16": sig[:16].tolist(),
                "signal_last16": sig[max(0, len(sig) - 16):].tolist(),
                "signal_full": sig.tolist() if i < FULL_SIGNAL_READS else None,
            }
            # Added in later pod5 releases (V4: open_pore_level, V5: the other two).
            for name in ("open_pore_level", "expected_open_pore_level", "selected_read_level"):
                if hasattr(rec, name):
                    fields[name] = float(getattr(rec, name))
            for name, value in vars(ri).items():
                if isinstance(value, float):
                    value = float(value)
                elif hasattr(value, "items"):
                    value = dict(value)
                fields[f"run_info.{name}"] = value
            reads[str(rec.read_id)] = fields
    return reads


def first_difference(a: list, b: list) -> str:
    """Describe where two sample lists first differ, including a length difference."""
    for i, (x, y) in enumerate(zip(a, b)):
        if x != y:
            return f"sample {i}: src={x} written={y}"
    return f"lengths differ: src={len(a)} written={len(b)}"


def run_parity(src: pathlib.Path) -> None:
    import warnings
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    import pod5

    n = min(N_READS, _count_reads_python(src))

    print(f"\nSource:   {src}  ({n} reads)")
    print("Building Rust writer (first run may take ~30 s)…")
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    run_rust_writer(src, OUT_FILE, n)
    print(f"Output:   {OUT_FILE}")

    src_reads = collect_reads(pod5, src, limit=n)
    written_reads = collect_reads(pod5, OUT_FILE)

    assert len(written_reads) == n, (
        f"Expected {n} reads in output, got {len(written_reads)}"
    )
    assert set(written_reads.keys()) == set(src_reads.keys()), (
        "Read ID sets differ between source and Rust-written file"
    )

    fails = []
    for rid, s in src_reads.items():
        w = written_reads[rid]
        for field, sv in s.items():
            wv = w[field]
            if field == "signal_full":
                if sv is not None and sv != wv:
                    fails.append(f"  {rid[:8]}... {field}: {first_difference(sv, wv)}")
            elif isinstance(sv, float):
                if not approx_eq(sv, wv):
                    fails.append(f"  {rid[:8]}... {field}: src={sv:.6g} written={wv:.6g}")
            elif sv != wv:
                fails.append(f"  {rid[:8]}... {field}: src={sv!r} written={wv!r}")

    if fails:
        print(f"\nPARITY FAILURES ({len(fails)}):")
        for f in fails:
            print(f)
        raise AssertionError(f"{len(fails)} parity failures (see above)")

    print(f"\nAll {n} reads match between source and Rust-written file.")
    print(f"Fields checked: {', '.join(next(iter(src_reads.values())))}")


def _count_reads_python(src: pathlib.Path) -> int:
    try:
        import pod5
        with pod5.Reader(str(src)) as reader:
            return sum(1 for _ in reader.reads())
    except Exception:
        return N_READS


# ── pytest integration ─────────────────────────────────────────────────────────


def test_write_parity() -> None:
    """pytest discovers this automatically."""
    src = find_test_file()
    if src is None:
        try:
            import pytest
            pytest.skip("no POD5 test file (add tests/data/sample.pod5)")
        except ImportError:
            return
    try:
        import pod5  # noqa: F401
    except ImportError:
        try:
            import pytest
            pytest.skip("Python pod5 library not installed (pip install pod5)")
        except ImportError:
            return
    run_parity(src)


# ── Standalone entry point ─────────────────────────────────────────────────────

if __name__ == "__main__":
    src = pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else find_test_file()
    if src is None:
        print(
            "ERROR: no POD5 file found.\n"
            "  Add tests/data/sample.pod5 or pass explicitly:\n"
            "  python tests/test_write_parity.py <source.pod5>",
            file=sys.stderr,
        )
        sys.exit(1)
    try:
        run_parity(src)
    except (RuntimeError, AssertionError) as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        sys.exit(1)
