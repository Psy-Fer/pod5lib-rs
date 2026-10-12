"""
Regenerate tests/fixtures/all_fields_v7.pod5 with upstream pod5 (validated: 0.3.49).

    python tests/fixtures/make_all_fields_fixture.py

Takes 4 reads from multi_fast5_zip_v7.pod5 (one spans two 102,400-sample signal
rows) and gives every field a distinct, non-default value, so parity tests compare
real data rather than NaN against NaN: channels around the 16-bit limit, V5 levels,
scaling pairs, mux fields, end reasons with and without `forced`, and two run infos.
The last read keeps NaN for every optional float.
"""

import dataclasses
import pathlib

import pod5
from pod5.pod5_types import ShiftScalePair

HERE = pathlib.Path(__file__).parent
SRC = HERE / "multi_fast5_zip_v7.pod5"
OUT = HERE / "all_fields_v7.pod5"

READ_IDS = [
    "00919556-e519-4960-8aa5-c2dfa020980c",  #   9,885 samples
    "008ed3dc-86c2-452f-b107-6877a473d177",  #  14,510 samples
    "009dc9bd-c5f4-487b-ba4c-b9ce7e3a711e",  #  15,643 samples
    "0000173c-bf67-44e7-9a9c-1ad0bc728e74",  # 123,627 samples: two signal rows
]
CHANNELS = [1, 65535, 65536, 70000]
END_REASONS = [
    pod5.EndReason(pod5.EndReasonEnum.SIGNAL_POSITIVE, False),
    pod5.EndReason(pod5.EndReasonEnum.MUX_CHANGE, True),
    pod5.EndReason(pod5.EndReasonEnum.DATA_SERVICE_UNBLOCK_MUX_CHANGE, True),
    pod5.EndReason(pod5.EndReasonEnum.PAUSED, False),
]
NAN = float("nan")


def main() -> None:
    with pod5.Reader(SRC) as reader:
        reads = {str(r.read_id): r.to_read() for r in reader.reads()}

    base_run_info = reads[READ_IDS[0]].run_info
    run_infos = [
        base_run_info,
        dataclasses.replace(
            base_run_info,
            acquisition_id="all-fields-second-acquisition",
            acquisition_start_time=base_run_info.acquisition_start_time.replace(year=2024),
            adc_max=2047,
            adc_min=-2048,
            context_tags={"sample_frequency": "5000", "experiment_type": "genomic_dna"},
            experiment_name="all_fields_fixture",
            flow_cell_id="PAW99999",
            flow_cell_product_code="FLO-PRO114M",
            protocol_name="sequencing/sequencing_PRO114_DNA_e8_2_400K",
            protocol_run_id="all-fields-protocol-run",
            sample_id="all_fields_sample",
            sample_rate=5000,
            sequencing_kit="sqk-lsk114",
            sequencer_position="1A",
            sequencer_position_type="PromethION",
            software="MinKNOW 24.11 (fixture)",
            system_name="fixture-host",
            system_type="PromethION",
            tracking_id={"run_id": "all-fields-run", "device_type": "promethion"},
        ),
    ]

    with pod5.Writer(OUT) as writer:
        for i, rid in enumerate(READ_IDS):
            read = reads[rid]
            last = i == len(READ_IDS) - 1
            writer.add_read(
                dataclasses.replace(
                    read,
                    pore=pod5.Pore(channel=CHANNELS[i], well=1 + i, pore_type=f"pore_type_{i % 2}"),
                    calibration=pod5.Calibration(offset=-240.0 - i, scale=0.15 + i / 100),
                    read_number=1000 + i,
                    start_sample=10_000_000 * (i + 1),
                    median_before=NAN if last else 200.5 + i,
                    end_reason=END_REASONS[i],
                    run_info=run_infos[i % 2],
                    num_minknow_events=100 + i,
                    tracked_scaling=ShiftScalePair(NAN, NAN) if last else ShiftScalePair(80.0 + i, 12.5 + i),
                    predicted_scaling=ShiftScalePair(NAN, NAN) if last else ShiftScalePair(81.0 + i, 13.5 + i),
                    num_reads_since_mux_change=7 + i,
                    time_since_mux_change=1.25 + i,
                    open_pore_level=NAN if last else 230.0 + i,
                    expected_open_pore_level=NAN if last else 231.0 + i,
                    selected_read_level=NAN if last else 90.0 + i,
                )
            )
    print(f"wrote {OUT} with pod5 {pod5.__version__}")


if __name__ == "__main__":
    main()
