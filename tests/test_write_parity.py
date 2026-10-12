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

    # Collect source reads with Python pod5
    src_reads = {}
    with pod5.Reader(str(src)) as reader:
        for i, rec in enumerate(reader.reads()):
            if i >= n:
                break
            sig = rec.signal
            src_reads[str(rec.read_id)] = {
                "read_number": rec.read_number,
                "num_samples": rec.num_samples,
                "channel": rec.pore.channel,
                "well": rec.pore.well,
                "pore_type": rec.pore.pore_type,
                "calib_offset": rec.calibration.offset,
                "calib_scale": rec.calibration.scale,
                "median_before": rec.median_before,
                "end_reason": rec.end_reason.reason.name.lower(),
                "end_reason_forced": bool(rec.end_reason.forced),
                "run_info_acq_id": rec.run_info.acquisition_id,
                "signal_len": int(len(sig)),
                "signal_sum": int(sig.astype("int64").sum()),
                "signal_first16": sig[:16].tolist(),
                "signal_last16": sig[max(0, len(sig) - 16):].tolist(),
                "signal_full": sig.tolist() if i < FULL_SIGNAL_READS else None,
            }

    # Collect written reads with Python pod5
    written_reads = {}
    with pod5.Reader(str(OUT_FILE)) as reader:
        for i, rec in enumerate(reader.reads()):
            sig = rec.signal
            written_reads[str(rec.read_id)] = {
                "read_number": rec.read_number,
                "num_samples": rec.num_samples,
                "channel": rec.pore.channel,
                "well": rec.pore.well,
                "pore_type": rec.pore.pore_type,
                "calib_offset": rec.calibration.offset,
                "calib_scale": rec.calibration.scale,
                "median_before": rec.median_before,
                "end_reason": rec.end_reason.reason.name.lower(),
                "end_reason_forced": bool(rec.end_reason.forced),
                "run_info_acq_id": rec.run_info.acquisition_id,
                "signal_len": int(len(sig)),
                "signal_sum": int(sig.astype("int64").sum()),
                "signal_first16": sig[:16].tolist(),
                "signal_last16": sig[max(0, len(sig) - 16):].tolist(),
                "signal_full": sig.tolist() if i < FULL_SIGNAL_READS else None,
            }

    assert len(written_reads) == n, (
        f"Expected {n} reads in output, got {len(written_reads)}"
    )
    assert set(written_reads.keys()) == set(src_reads.keys()), (
        "Read ID sets differ between source and Rust-written file"
    )

    fails = []
    for rid, s in src_reads.items():
        w = written_reads[rid]
        for field in ("read_number", "num_samples", "channel", "well",
                      "pore_type", "end_reason", "end_reason_forced",
                      "run_info_acq_id", "signal_len", "signal_sum",
                      "signal_first16", "signal_last16"):
            if s[field] != w[field]:
                fails.append(f"  {rid[:8]}... {field}: src={s[field]!r} written={w[field]!r}")
        for field in ("calib_offset", "calib_scale", "median_before"):
            if not approx_eq(s[field], w[field]):
                fails.append(
                    f"  {rid[:8]}... {field}: src={s[field]:.6g} written={w[field]:.6g}"
                )
        if s["signal_full"] is not None:
            if s["signal_full"] != w["signal_full"]:
                first_bad = next(
                    i for i, (a, b) in enumerate(zip(s["signal_full"], w["signal_full"])) if a != b
                )
                fails.append(
                    f"  {rid[:8]}... signal_full mismatch at sample {first_bad}: "
                    f"src={s['signal_full'][first_bad]} written={w['signal_full'][first_bad]}"
                )

    if fails:
        print(f"\nPARITY FAILURES ({len(fails)}):")
        for f in fails:
            print(f)
        raise AssertionError(f"{len(fails)} parity failures (see above)")

    print(f"\nAll {n} reads match between source and Rust-written file.")
    print("Fields checked: read_id, read_number, num_samples, channel, well, pore_type,")
    print("                calib_offset, calib_scale, median_before, end_reason,")
    print(f"                end_reason_forced, run_info.acquisition_id, signal_len,")
    print(f"                signal_sum, signal_first16, signal_last16,")
    print(f"                signal_full (first {FULL_SIGNAL_READS} reads)")


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
