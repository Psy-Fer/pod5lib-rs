/// Write a POD5 file from reads built in code (no source POD5 file), as a
/// converter from another format would. Also used by tests/test_scratch_parity.py.
///
/// Usage:  cargo run --example write_from_scratch [out.pod5]
///
/// Default: out = tests/data/rust_scratch.pod5
///
/// Read i (0-based) gets channel 100 + i (read 3 uses 70000, which only fits in
/// `channel_32bit`), read_number 1000 + i, and signal `(j * 7 + i) % 1024 - 512`
/// for j in 0..(1000 * (i + 1)).
use std::path::PathBuf;
use std::sync::Arc;

use pod5lib::{
    Calibration, EndReason, EndReasonKind, Pore, ReadRecord, RunInfo, ShiftScalePair, Writer,
    uuid_from_str,
};

const READ_IDS: [&str; 4] = [
    "11111111-2222-4333-8444-555555555555",
    "21111111-2222-4333-8444-555555555555",
    "31111111-2222-4333-8444-555555555555",
    "41111111-2222-4333-8444-555555555555",
];

fn main() {
    let out = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("tests/data/rust_scratch.pod5"));
    if let Some(dir) = out.parent() {
        std::fs::create_dir_all(dir).expect("create output directory");
    }

    let run_info = Arc::new(RunInfo {
        acquisition_id: "scratch-acquisition-0001".into(),
        acquisition_start_time_ms: 1_700_000_000_000,
        adc_min: -4096,
        adc_max: 4095,
        sample_rate: 5000,
        experiment_name: "from_scratch".into(),
        flow_cell_id: "PAW00000".into(),
        sequencing_kit: "SQK-LSK114".into(),
        context_tags: [("sample_frequency".into(), "5000".into())].into(),
        tracking_id: [("run_id".into(), "scratch".into())].into(),
        ..Default::default()
    });

    let mut writer = Writer::new();
    for (i, id) in READ_IDS.iter().enumerate() {
        let channel = if i == 3 { 70_000 } else { 100 + i as u32 };
        let mut read = ReadRecord::new(
            uuid_from_str(id).expect("valid read id"),
            Pore {
                channel,
                well: 1,
                pore_type: "not_set".into(),
            },
            Calibration {
                offset: -243.0,
                scale: 0.1462,
            },
            1000 + i as u32,
            (i as u64) * 100_000,
            200.0 + i as f32,
            EndReason {
                reason: EndReasonKind::SignalPositive,
                forced: false,
            },
            Arc::clone(&run_info),
        );
        read.tracked_scaling = ShiftScalePair {
            shift: 1.5,
            scale: 2.5,
        };
        read.expected_open_pore_level = 230.0;

        let n = 1000 * (i + 1);
        let signal: Vec<i16> = (0..n).map(|j| ((j * 7 + i) % 1024) as i16 - 512).collect();
        writer.add_read(read, signal);
    }

    writer.write_file(&out).expect("write");
    println!("Wrote {} reads to {}", READ_IDS.len(), out.display());
}
