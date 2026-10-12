"""
Check a POD5 file built from scratch by pod5lib (examples/write_from_scratch.rs)
is read back correctly by the Python pod5 library.

Usage:
    python tests/test_scratch_parity.py
"""

import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).parent.parent
OUT_FILE = ROOT / "tests/data/rust_scratch.pod5"

# Must match examples/write_from_scratch.rs
READ_IDS = [f"{d}1111111-2222-4333-8444-555555555555" for d in "123456"]
SIGNAL_LENS = [1000, 2000, 3000, 4000, 250_000, 0]
END_REASONS = ["signal_positive", "mux_change", "signal_negative", "unblock_mux_change", "paused", "api_request"]
CHUNK = 102_400  # upstream's default max_signal_chunk_size


def main() -> None:
    import warnings

    warnings.filterwarnings("ignore", category=DeprecationWarning)
    import numpy as np
    import pod5

    subprocess.run(
        ["cargo", "run", "--quiet", "--example", "write_from_scratch", "--", str(OUT_FILE)],
        cwd=ROOT,
        check=True,
    )

    # pod5 < 0.3.48 predates `channel_32bit` and sees the 16-bit `channel`, which
    # V7 sets to 0 for channels above 65535.
    knows_channel_32bit = tuple(int(x) for x in pod5.__version__.split(".")[:3]) >= (0, 3, 48)

    fails = []

    def check(label, got, want):
        if got != want:
            fails.append(f"  {label}: got {got!r}, want {want!r}")

    with pod5.Reader(str(OUT_FILE)) as reader:
        reads = {str(r.read_id): r for r in reader.reads()}
        check("read ids", sorted(reads), READ_IDS)
        for i, rid in enumerate(READ_IDS):
            r = reads.get(rid)
            if r is None:
                continue
            n = SIGNAL_LENS[i]
            want_signal = (np.arange(n) * 7 + i) % 1024 - 512
            want_channel = 100 + i if i != 3 else (70000 if knows_channel_32bit else 0)
            check(f"{rid[:8]} channel", r.pore.channel, want_channel)
            check(f"{rid[:8]} read_number", r.read_number, 1000 + i)
            check(f"{rid[:8]} start_sample", r.start_sample, i * 100_000)
            check(f"{rid[:8]} median_before", r.median_before, 200.0 + i)
            reason = pod5.EndReasonEnum[END_REASONS[i].upper()]
            check(f"{rid[:8]} end_reason", r.end_reason.reason, reason)
            check(f"{rid[:8]} end_reason_forced", r.end_reason.forced, pod5.EndReason.from_reason_with_default_forced(reason).forced)
            # The dictionary index must equal the enum value, as in upstream files.
            check(f"{rid[:8]} end_reason_index", r.end_reason_index, reason.value)
            check(f"{rid[:8]} signal rows", len(r.signal_rows), -(-n // CHUNK))
            check(f"{rid[:8]} num_samples", r.num_samples, n)
            check(f"{rid[:8]} signal", r.signal.tolist(), want_signal.tolist())
            check(f"{rid[:8]} tracked_scaling", (r.tracked_scaling.shift, r.tracked_scaling.scale), (1.5, 2.5))
            check(f"{rid[:8]} run_info.acquisition_id", r.run_info.acquisition_id, "scratch-acquisition-0001")
            check(f"{rid[:8]} run_info.sample_rate", r.run_info.sample_rate, 5000)
            check(f"{rid[:8]} run_info.flow_cell_id", r.run_info.flow_cell_id, "PAW00000")
            check(f"{rid[:8]} run_info.context_tags", dict(r.run_info.context_tags), {"sample_frequency": "5000"})
            if hasattr(r, "expected_open_pore_level"):  # pod5 >= 0.3.44
                check(f"{rid[:8]} expected_open_pore_level", r.expected_open_pore_level, 230.0)

    if fails:
        print(f"SCRATCH PARITY FAILURES ({len(fails)}):")
        print("\n".join(fails))
        sys.exit(1)
    print(f"All {len(READ_IDS)} from-scratch reads read back correctly by pod5 {pod5.__version__}.")


if __name__ == "__main__":
    main()
