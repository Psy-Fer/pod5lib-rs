//! Dump all POD5 read metadata and signal summaries to JSON on stdout.
//!
//! Used by `tests/test_parity.py` to verify field-by-field parity with the
//! Python `pod5` library.
//!
//!   cargo run --example dump_pod5 --quiet -- <file.pod5>

use pod5::Reader;
use serde::Serialize;
use std::collections::BTreeMap;

// ── Output schema ─────────────────────────────────────────────────────────────

#[derive(Serialize)]
struct Dump {
    pod5_version: String,
    file_identifier: String,
    software: String,
    reads: Vec<ReadDump>,
    run_infos: Vec<RunInfoDump>,
}

#[derive(Serialize)]
struct ReadDump {
    read_id: String,
    read_number: u32,
    pore_channel: u16,
    pore_well: u8,
    pore_type: String,
    calibration_offset: f32,
    calibration_scale: f32,
    end_reason: String,
    end_reason_forced: bool,
    /// `null` when NaN (not recorded for this read).
    median_before: Option<f32>,
    num_samples: u64,
    start_sample: u64,
    /// `null` when unavailable (NaN in the file).
    tracked_scaling_shift: Option<f32>,
    tracked_scaling_scale: Option<f32>,
    predicted_scaling_shift: Option<f32>,
    predicted_scaling_scale: Option<f32>,
    num_minknow_events: u64,
    num_reads_since_mux_change: u32,
    /// `null` when NaN (not recorded for this read).
    time_since_mux_change: Option<f32>,
    /// `null` when the optional open_pore_level column is absent.
    open_pore_level: Option<f32>,
    run_info_acquisition_id: String,
    /// Decompressed ADC signal length (== num_samples).
    signal_len: usize,
    /// First up-to-16 raw ADC samples.
    signal_first16: Vec<i16>,
    /// Last up-to-16 raw ADC samples (in order).
    signal_last16: Vec<i16>,
    /// Sum of all ADC samples (i64 to avoid overflow).
    signal_sum: i64,
    /// First up-to-16 calibrated pA samples.
    signal_pa_first16: Vec<f32>,
    /// Last up-to-16 calibrated pA samples (in order).
    signal_pa_last16: Vec<f32>,
    /// Full raw ADC signal — populated for the first FULL_SIGNAL_READS reads, null elsewhere.
    signal_full: Option<Vec<i16>>,
    /// Full calibrated pA signal — populated for the first FULL_SIGNAL_READS reads, null elsewhere.
    signal_pa_full: Option<Vec<f32>>,
}

#[derive(Serialize)]
struct RunInfoDump {
    acquisition_id: String,
    acquisition_start_time_ms: i64,
    adc_max: i16,
    adc_min: i16,
    experiment_name: String,
    flow_cell_id: String,
    flow_cell_product_code: String,
    protocol_name: String,
    protocol_run_id: String,
    protocol_start_time_ms: i64,
    sample_id: String,
    sample_rate: u16,
    sequencing_kit: String,
    sequencer_position: String,
    sequencer_position_type: String,
    software: String,
    system_name: String,
    system_type: String,
    /// Sorted for deterministic JSON output.
    context_tags: BTreeMap<String, String>,
    tracking_id: BTreeMap<String, String>,
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Returns `Some(v)` for finite floats, `None` for NaN / ±Inf.
/// serde_json panics on NaN in a non-Option f32, so every nullable float
/// in the schema must go through this conversion.
fn finite(v: f32) -> Option<f32> {
    v.is_finite().then_some(v)
}

/// Number of reads for which the full decompressed signal is included in the JSON output.
const FULL_SIGNAL_READS: usize = 5;

fn tail16(v: &[i16]) -> Vec<i16> {
    v[v.len().saturating_sub(16)..].to_vec()
}

fn tail16_f32(v: &[f32]) -> Vec<f32> {
    v[v.len().saturating_sub(16)..].to_vec()
}

// ── Main ──────────────────────────────────────────────────────────────────────

fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: dump_pod5 <file.pod5>");

    let reader = Reader::open(&path).expect("open failed");

    let total = reader.len();
    eprintln!("dump_pod5: {total} reads in {path}");

    let reads: Vec<ReadDump> = reader
        .reads()
        .iter()
        .enumerate()
        .map(|(idx, read)| {
            if idx > 0 && idx % 1000 == 0 {
                eprintln!("  ... {idx}/{total}");
            }

            let signal = read.signal().expect("signal decompression failed");
            let sig_len = signal.len();
            let first16: Vec<i16> = signal.iter().take(16).copied().collect();
            let last16 = tail16(&signal);
            let sig_sum: i64 = signal.iter().map(|&s| s as i64).sum();

            let pa = read.signal_pa().expect("signal_pa failed");
            let pa_first16: Vec<f32> = pa.iter().take(16).copied().collect();
            let pa_last16 = tail16_f32(&pa);

            ReadDump {
                read_id: read.read_id_str(),
                read_number: read.read_number,
                pore_channel: read.pore.channel,
                pore_well: read.pore.well,
                pore_type: read.pore.pore_type.clone(),
                calibration_offset: read.calibration.offset,
                calibration_scale: read.calibration.scale,
                end_reason: read.end_reason.reason.as_str().to_owned(),
                end_reason_forced: read.end_reason.forced,
                median_before: finite(read.median_before),
                num_samples: read.num_samples,
                start_sample: read.start_sample,
                tracked_scaling_shift: finite(read.tracked_scaling.shift),
                tracked_scaling_scale: finite(read.tracked_scaling.scale),
                predicted_scaling_shift: finite(read.predicted_scaling.shift),
                predicted_scaling_scale: finite(read.predicted_scaling.scale),
                num_minknow_events: read.num_minknow_events,
                num_reads_since_mux_change: read.num_reads_since_mux_change,
                time_since_mux_change: finite(read.time_since_mux_change),
                open_pore_level: finite(read.open_pore_level),
                run_info_acquisition_id: read.run_info.acquisition_id.clone(),
                signal_len: sig_len,
                signal_first16: first16,
                signal_last16: last16,
                signal_sum: sig_sum,
                signal_pa_first16: pa_first16,
                signal_pa_last16: pa_last16,
                signal_full: (idx < FULL_SIGNAL_READS).then_some(signal),
                signal_pa_full: (idx < FULL_SIGNAL_READS).then_some(pa),
            }
        })
        .collect();

    let run_infos: Vec<RunInfoDump> = reader
        .run_infos()
        .iter()
        .map(|ri| RunInfoDump {
            acquisition_id: ri.acquisition_id.clone(),
            acquisition_start_time_ms: ri.acquisition_start_time_ms,
            adc_max: ri.adc_max,
            adc_min: ri.adc_min,
            experiment_name: ri.experiment_name.clone(),
            flow_cell_id: ri.flow_cell_id.clone(),
            flow_cell_product_code: ri.flow_cell_product_code.clone(),
            protocol_name: ri.protocol_name.clone(),
            protocol_run_id: ri.protocol_run_id.clone(),
            protocol_start_time_ms: ri.protocol_start_time_ms,
            sample_id: ri.sample_id.clone(),
            sample_rate: ri.sample_rate,
            sequencing_kit: ri.sequencing_kit.clone(),
            sequencer_position: ri.sequencer_position.clone(),
            sequencer_position_type: ri.sequencer_position_type.clone(),
            software: ri.software.clone(),
            system_name: ri.system_name.clone(),
            system_type: ri.system_type.clone(),
            context_tags: ri
                .context_tags
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            tracking_id: ri
                .tracking_id
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        })
        .collect();

    let dump = Dump {
        pod5_version: reader.pod5_version().to_owned(),
        file_identifier: reader.file_identifier().to_owned(),
        software: reader.software().to_owned(),
        reads,
        run_infos,
    };

    println!(
        "{}",
        serde_json::to_string_pretty(&dump).expect("JSON serialization failed")
    );
    eprintln!("dump_pod5: done");
}
