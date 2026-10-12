#!/usr/bin/env python3
"""
Parity test: compare pod5-rs (Rust) output field-by-field against the Python pod5 library.

Requirements:
    pip install pod5

Usage (standalone):
    python tests/test_parity.py [path/to/file.pod5]

Usage (pytest):
    pytest tests/test_parity.py          # auto-discovers test file
    pytest tests/test_parity.py -s       # show progress output

The Rust dump binary is built automatically via `cargo run --example dump_pod5`.
"""

import datetime
import json
import math
import pathlib
import subprocess
import sys
import warnings

ROOT = pathlib.Path(__file__).parent.parent

# ── File discovery ─────────────────────────────────────────────────────────────


def find_test_file() -> pathlib.Path | None:
    for candidate in [
        ROOT / "tests/data/sample.pod5",
        ROOT / "examples/small.pod5",
        ROOT / "tests/fixtures/multi_fast5_zip_v7.pod5",
    ]:
        if candidate.exists():
            return candidate
    return None


# ── Rust side ─────────────────────────────────────────────────────────────────


def build_and_run_rust(pod5_path: pathlib.Path) -> dict:
    """Build the dump_pod5 example and run it; return the parsed JSON."""
    result = subprocess.run(
        [
            "cargo",
            "run",
            "--example",
            "dump_pod5",
            "--quiet",
            "--",
            str(pod5_path),
        ],
        capture_output=True,
        text=True,
        cwd=ROOT,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            "dump_pod5 failed\n"
            f"--- stdout ---\n{result.stdout}\n"
            f"--- stderr ---\n{result.stderr}"
        )
    return json.loads(result.stdout)


# ── Python side ───────────────────────────────────────────────────────────────


def _dt_to_ms(dt: datetime.datetime) -> int:
    """Convert a datetime (timezone-aware or naïve UTC) to milliseconds since epoch."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    return int((dt - epoch).total_seconds() * 1000)


def _finite_or_null(v) -> float | None:
    """Convert NaN / ±Inf to None; otherwise return the value as Python float."""
    try:
        fv = float(v)
    except (TypeError, ValueError):
        return None
    return None if not math.isfinite(fv) else fv


def read_with_python(pod5_path: pathlib.Path) -> dict:
    """Read the POD5 file with the Python pod5 library; return a comparable dict."""
    try:
        import pod5
    except ImportError:
        raise ImportError(
            "Python 'pod5' library is required for the parity test.\n"
            "Install it with:  pip install pod5"
        )

    reads = []
    run_infos_seen: dict[str, dict] = {}

    # The `deprecated` library emits DeprecationWarning at the call site, so we must
    # suppress all DeprecationWarnings here, not just from the pod5 module.
    # Affected fields: tracked_scaling, predicted_scaling, num_reads_since_mux_change,
    # time_since_mux_change — deprecated in pod5 ≥ 0.4.0 but still present in files.
    warnings.filterwarnings("ignore", category=DeprecationWarning)

    with pod5.Reader(pod5_path) as reader:
        for rec in reader.reads():
            signal = rec.signal  # numpy int16 array
            sig_len = int(len(signal))
            first16 = signal[:16].tolist()
            last16 = signal[max(0, sig_len - 16) :].tolist()
            sig_sum = int(signal.astype("int64").sum())

            pa = rec.signal_pa  # numpy float32 array
            pa_first16 = pa[:16].tolist()
            pa_last16 = pa[max(0, sig_len - 16) :].tolist()

            ts = rec.tracked_scaling
            ps = rec.predicted_scaling

            # open_pore_level may not exist in all pod5 versions / files
            opl = _finite_or_null(getattr(rec, "open_pore_level", float("nan")))
            # V5 fields only exist in pod5 >= 0.3.44
            eopl = _finite_or_null(getattr(rec, "expected_open_pore_level", float("nan")))
            srl = _finite_or_null(getattr(rec, "selected_read_level", float("nan")))

            ri = rec.run_info
            # Full signal for the first FULL_SIGNAL_READS reads (matches dump_pod5.rs constant).
            FULL_SIGNAL_READS = 5
            full_adc = signal.tolist() if len(reads) < FULL_SIGNAL_READS else None
            full_pa = pa.tolist() if len(reads) < FULL_SIGNAL_READS else None

            reads.append(
                {
                    "read_id": str(rec.read_id),
                    "read_number": int(rec.read_number),
                    "pore_channel": int(rec.pore.channel),
                    "pore_well": int(rec.pore.well),
                    "pore_type": str(rec.pore.pore_type),
                    "calibration_offset": float(rec.calibration.offset),
                    "calibration_scale": float(rec.calibration.scale),
                    # Python EndReasonEnum.name gives uppercase ("SIGNAL_POSITIVE");
                    # lower() to match our Rust as_str() which returns "signal_positive".
                    "end_reason": rec.end_reason.reason.name.lower(),
                    "end_reason_forced": bool(rec.end_reason.forced),
                    # median_before and time_since_mux_change can be NaN in some files.
                    "median_before": _finite_or_null(rec.median_before),
                    "num_samples": int(rec.num_samples),
                    "start_sample": int(rec.start_sample),
                    "tracked_scaling_shift": _finite_or_null(ts.shift),
                    "tracked_scaling_scale": _finite_or_null(ts.scale),
                    "predicted_scaling_shift": _finite_or_null(ps.shift),
                    "predicted_scaling_scale": _finite_or_null(ps.scale),
                    "num_minknow_events": int(rec.num_minknow_events),
                    "num_reads_since_mux_change": int(rec.num_reads_since_mux_change),
                    "time_since_mux_change": _finite_or_null(rec.time_since_mux_change),
                    "open_pore_level": opl,
                    "expected_open_pore_level": eopl,
                    "selected_read_level": srl,
                    "run_info_acquisition_id": ri.acquisition_id,
                    "signal_len": sig_len,
                    "signal_first16": first16,
                    "signal_last16": last16,
                    "signal_sum": sig_sum,
                    "signal_pa_first16": pa_first16,
                    "signal_pa_last16": pa_last16,
                    "signal_full": full_adc,
                    "signal_pa_full": full_pa,
                }
            )

            if ri.acquisition_id not in run_infos_seen:
                run_infos_seen[ri.acquisition_id] = {
                    "acquisition_id": ri.acquisition_id,
                    "acquisition_start_time_ms": _dt_to_ms(ri.acquisition_start_time),
                    "adc_max": int(ri.adc_max),
                    "adc_min": int(ri.adc_min),
                    "experiment_name": ri.experiment_name,
                    "flow_cell_id": ri.flow_cell_id,
                    "flow_cell_product_code": ri.flow_cell_product_code,
                    "protocol_name": ri.protocol_name,
                    "protocol_run_id": ri.protocol_run_id,
                    "protocol_start_time_ms": _dt_to_ms(ri.protocol_start_time),
                    "sample_id": ri.sample_id,
                    "sample_rate": int(ri.sample_rate),
                    "sequencing_kit": ri.sequencing_kit,
                    "sequencer_position": ri.sequencer_position,
                    "sequencer_position_type": ri.sequencer_position_type,
                    "software": ri.software,
                    "system_name": ri.system_name,
                    "system_type": ri.system_type,
                    "context_tags": dict(ri.context_tags),
                    "tracking_id": dict(ri.tracking_id),
                }

    return {
        "reads": reads,
        "run_infos": list(run_infos_seen.values()),
    }


# ── Comparison helpers ────────────────────────────────────────────────────────

# Relative tolerance for f32 fields.  f32 has ~7 significant digits; 1e-5
# relative is well within that and covers any f32↔f64 promotion noise.
_F32_RTOL = 1e-5


def _close(rv, pv, rtol=_F32_RTOL, label="") -> None:
    if pv is None or rv is None:
        assert rv is None and pv is None, f"{label}: one is null — rust={rv}, python={pv}"
        return
    diff = abs(float(rv) - float(pv))
    scale = max(1.0, abs(float(pv)))
    assert diff <= rtol * scale, (
        f"{label}: rust={rv} python={pv} diff={diff:.3e} (rtol={rtol})"
    )


def _close_list(rl, pl, rtol=_F32_RTOL, label="") -> None:
    assert len(rl) == len(pl), f"{label}: length rust={len(rl)} python={len(pl)}"
    for j, (rv, pv) in enumerate(zip(rl, pl)):
        _close(rv, pv, rtol=rtol, label=f"{label}[{j}]")


# ── Read comparison ───────────────────────────────────────────────────────────


def compare_reads(rust_reads: list, py_reads: list) -> None:
    assert len(rust_reads) == len(py_reads), (
        f"read count: rust={len(rust_reads)} python={len(py_reads)}"
    )

    for i, (r, p) in enumerate(zip(rust_reads, py_reads)):
        prefix = f"read[{i}] {r.get('read_id', '?')}"

        # ── Exact integer / string / boolean fields ──
        for field in (
            "read_id",
            "read_number",
            "pore_channel",
            "pore_well",
            "pore_type",
            "end_reason",
            "end_reason_forced",
            "num_samples",
            "start_sample",
            "num_minknow_events",
            "num_reads_since_mux_change",
            "run_info_acquisition_id",
            "signal_len",
            "signal_first16",
            "signal_last16",
            "signal_sum",
        ):
            assert r[field] == p[field], (
                f"{prefix}: {field}: rust={r[field]!r} != python={p[field]!r}"
            )

        # ── Float fields with tolerance ──
        for field in ("calibration_offset", "calibration_scale"):
            _close(r[field], p[field], label=f"{prefix}.{field}")

        # ── Optional floats (None == NaN/unavailable) ──
        for field in (
            "median_before",
            "time_since_mux_change",
            "tracked_scaling_shift",
            "tracked_scaling_scale",
            "predicted_scaling_shift",
            "predicted_scaling_scale",
            "open_pore_level",
            "expected_open_pore_level",
            "selected_read_level",
        ):
            _close(r[field], p[field], label=f"{prefix}.{field}")

        # ── pA signal samples ──
        _close_list(r["signal_pa_first16"], p["signal_pa_first16"],
                    label=f"{prefix}.signal_pa_first16")
        _close_list(r["signal_pa_last16"], p["signal_pa_last16"],
                    label=f"{prefix}.signal_pa_last16")

        # ── Full signal for the first FULL_SIGNAL_READS reads ──
        if r["signal_full"] is not None:
            assert p["signal_full"] is not None, f"{prefix}: expected full signal in python"
            assert r["signal_full"] == p["signal_full"], (
                f"{prefix}: full ADC signal mismatch at "
                + next(
                    str(j)
                    for j, (rv, pv) in enumerate(zip(r["signal_full"], p["signal_full"]))
                    if rv != pv
                )
            )
            _close_list(
                r["signal_pa_full"], p["signal_pa_full"],
                label=f"{prefix}.signal_pa_full",
            )


# ── RunInfo comparison ────────────────────────────────────────────────────────


def compare_run_infos(rust_ris: list, py_ris: list) -> None:
    rust_by_id = {ri["acquisition_id"]: ri for ri in rust_ris}
    py_by_id = {ri["acquisition_id"]: ri for ri in py_ris}

    assert set(rust_by_id) == set(py_by_id), (
        f"RunInfo acquisition_ids differ:\n"
        f"  rust  = {sorted(rust_by_id)}\n"
        f"  python= {sorted(py_by_id)}"
    )

    for acq_id in py_by_id:
        r, p = rust_by_id[acq_id], py_by_id[acq_id]
        prefix = f"run_info[{acq_id[:8]}...]"

        for field in (
            "acquisition_id",
            "adc_max",
            "adc_min",
            "sample_rate",
            "experiment_name",
            "flow_cell_id",
            "flow_cell_product_code",
            "protocol_name",
            "protocol_run_id",
            "sample_id",
            "sequencing_kit",
            "sequencer_position",
            "sequencer_position_type",
            "software",
            "system_name",
            "system_type",
            "context_tags",
            "tracking_id",
        ):
            assert r[field] == p[field], (
                f"{prefix}: {field}:\n  rust  ={r[field]!r}\n  python={p[field]!r}"
            )

        # Timestamps: Arrow stores exact ms; allow ±1 ms for float rounding
        for field in ("acquisition_start_time_ms", "protocol_start_time_ms"):
            assert abs(r[field] - p[field]) <= 1, (
                f"{prefix}: {field}: rust={r[field]} python={p[field]}"
            )


# ── Top-level runner ──────────────────────────────────────────────────────────


def run_parity_test(pod5_path: pathlib.Path) -> None:
    print(f"\nParity test: {pod5_path}")

    print("  [1/3] Building Rust dump binary and running...")
    rust = build_and_run_rust(pod5_path)
    n_reads = len(rust["reads"])
    n_ris = len(rust["run_infos"])
    print(f"         {n_reads} reads, {n_ris} run_info(s)")

    print("  [2/3] Reading with Python pod5 library...")
    py = read_with_python(pod5_path)

    print("  [3/3] Comparing fields...")
    compare_reads(rust["reads"], py["reads"])
    compare_run_infos(rust["run_infos"], py["run_infos"])

    print(f"  PASS — {n_reads} reads, {n_ris} run_infos, all fields match")


# ── pytest integration ────────────────────────────────────────────────────────


def test_parity_against_python() -> None:
    """pytest discovers this function automatically."""
    pod5_path = find_test_file()
    if pod5_path is None:
        try:
            import pytest

            pytest.skip("no POD5 test file (add tests/data/sample.pod5 or examples/small.pod5)")
        except ImportError:
            pass
        return
    run_parity_test(pod5_path)


# ── Standalone entry point ────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) > 1:
        path = pathlib.Path(sys.argv[1])
    else:
        path = find_test_file()

    if path is None:
        print(
            "ERROR: no POD5 file found.\n"
            "  Add tests/data/sample.pod5 or examples/small.pod5,\n"
            "  or pass the path explicitly:  python tests/test_parity.py <file.pod5>",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        run_parity_test(path)
    except AssertionError as e:
        print(f"\nFAIL: {e}", file=sys.stderr)
        sys.exit(1)
