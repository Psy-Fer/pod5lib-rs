/// Arrow IPC table readers for the three POD5 tables.
///
/// Each table is a Feather v2 / Arrow IPC File embedded in the POD5 container.
/// We read the whole section into memory then drive the arrow-ipc FileReader.
use std::collections::HashMap;
use std::io::{Cursor, Read, Seek, SeekFrom};
use std::sync::Arc;

use arrow::array::types::Int16Type;
use arrow::array::{
    Array, BooleanArray, DictionaryArray, FixedSizeBinaryArray, Float32Array, Int16Array,
    ListArray, MapArray, StringArray, TimestampMillisecondArray, UInt8Array, UInt16Array,
    UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;
use arrow::ipc::reader::FileReader;
use arrow::record_batch::RecordBatch;

use crate::Result;
use crate::error::Error;
use crate::signal::{SignalRowRef, SignalTable};
use crate::types::{
    Calibration, EndReason, EndReasonKind, Pore, ReadRecord, RunInfo, ShiftScalePair,
};

// ── helpers ───────────────────────────────────────────────────────────────────

/// Read an embedded Arrow IPC section from `file` into memory, then drive the
/// arrow-ipc `FileReader` over it to collect all record batches.
pub fn read_ipc_section<R: Read + Seek>(
    reader: &mut R,
    offset: u64,
    length: u64,
) -> Result<Vec<RecordBatch>> {
    let mut buf = vec![0u8; length as usize];
    reader.seek(SeekFrom::Start(offset))?;
    reader.read_exact(&mut buf)?;

    let cursor = Cursor::new(buf);
    let ipc = FileReader::try_new(cursor, None)?;
    let mut batches = Vec::new();
    for batch in ipc {
        batches.push(batch?);
    }
    Ok(batches)
}

// ── column accessors ─────────────────────────────────────────────────────────

macro_rules! col {
    ($batch:expr, $name:expr, $T:ty) => {{
        let c = $batch
            .column_by_name($name)
            .ok_or_else(|| Error::MissingColumn($name.into()))?;
        c.as_any()
            .downcast_ref::<$T>()
            .ok_or_else(|| Error::TypeMismatch($name.into()))
    }};
}

/// Get a `DictionaryArray<Int16Type>` column's string value at row `i`.
fn dict_str<'a>(batch: &'a RecordBatch, name: &str, i: usize) -> Result<&'a str> {
    let col = batch
        .column_by_name(name)
        .ok_or_else(|| Error::MissingColumn(name.into()))?;

    // Dictionary<Int16, Utf8>
    let dict = col
        .as_any()
        .downcast_ref::<DictionaryArray<Int16Type>>()
        .ok_or_else(|| Error::TypeMismatch(name.into()))?;
    let values = dict
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::TypeMismatch(name.into()))?;
    let key = dict.keys().value(i) as usize;
    Ok(values.value(key))
}

/// Extract a `map(utf8, utf8)` column at row `i` as `HashMap`.
fn map_to_hashmap(batch: &RecordBatch, name: &str, i: usize) -> Result<HashMap<String, String>> {
    let col = batch
        .column_by_name(name)
        .ok_or_else(|| Error::MissingColumn(name.into()))?;
    let map_arr = col
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| Error::TypeMismatch(name.into()))?;
    let entry = map_arr.value(i);
    let keys = entry
        .column_by_name("key")
        .ok_or_else(|| Error::MissingColumn("key".into()))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::TypeMismatch("key".into()))?;
    let vals = entry
        .column_by_name("value")
        .ok_or_else(|| Error::MissingColumn("value".into()))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::TypeMismatch("value".into()))?;
    let mut out = HashMap::with_capacity(keys.len());
    for j in 0..keys.len() {
        out.insert(keys.value(j).to_owned(), vals.value(j).to_owned());
    }
    Ok(out)
}

// ── RunInfo table ─────────────────────────────────────────────────────────────

pub fn parse_run_info_batches(batches: &[RecordBatch]) -> Result<Vec<Arc<RunInfo>>> {
    let mut out = Vec::new();
    for batch in batches {
        let n = batch.num_rows();
        let acq_id = col!(batch, "acquisition_id", StringArray)?;
        let acq_start = col!(batch, "acquisition_start_time", TimestampMillisecondArray)?;
        let adc_max = col!(batch, "adc_max", Int16Array)?;
        let adc_min = col!(batch, "adc_min", Int16Array)?;
        let exp_name = col!(batch, "experiment_name", StringArray)?;
        let fc_id = col!(batch, "flow_cell_id", StringArray)?;
        let fc_code = col!(batch, "flow_cell_product_code", StringArray)?;
        let proto_name = col!(batch, "protocol_name", StringArray)?;
        let proto_run = col!(batch, "protocol_run_id", StringArray)?;
        let proto_start = col!(batch, "protocol_start_time", TimestampMillisecondArray)?;
        let sample_id = col!(batch, "sample_id", StringArray)?;
        let sample_rate = col!(batch, "sample_rate", UInt16Array)?;
        let seq_kit = col!(batch, "sequencing_kit", StringArray)?;
        let seq_pos = col!(batch, "sequencer_position", StringArray)?;
        let seq_pos_type = col!(batch, "sequencer_position_type", StringArray)?;
        let software = col!(batch, "software", StringArray)?;
        let sys_name = col!(batch, "system_name", StringArray)?;
        let sys_type = col!(batch, "system_type", StringArray)?;

        for i in 0..n {
            let context_tags = map_to_hashmap(batch, "context_tags", i)?;
            let tracking_id = map_to_hashmap(batch, "tracking_id", i)?;
            out.push(Arc::new(RunInfo {
                acquisition_id: acq_id.value(i).to_owned(),
                acquisition_start_time_ms: acq_start.value(i),
                adc_max: adc_max.value(i),
                adc_min: adc_min.value(i),
                context_tags,
                experiment_name: exp_name.value(i).to_owned(),
                flow_cell_id: fc_id.value(i).to_owned(),
                flow_cell_product_code: fc_code.value(i).to_owned(),
                protocol_name: proto_name.value(i).to_owned(),
                protocol_run_id: proto_run.value(i).to_owned(),
                protocol_start_time_ms: proto_start.value(i),
                sample_id: sample_id.value(i).to_owned(),
                sample_rate: sample_rate.value(i),
                sequencing_kit: seq_kit.value(i).to_owned(),
                sequencer_position: seq_pos.value(i).to_owned(),
                sequencer_position_type: seq_pos_type.value(i).to_owned(),
                software: software.value(i).to_owned(),
                system_name: sys_name.value(i).to_owned(),
                system_type: sys_type.value(i).to_owned(),
                tracking_id,
            }));
        }
    }
    Ok(out)
}

// ── Signal table ─────────────────────────────────────────────────────────────

/// Build a `SignalTable` from Arrow IPC record batches.
///
/// Takes ownership of the batches so `SignalTable` can borrow from their buffers
/// at decompression time — no byte copying.
///
/// The `signal` column may be:
///   - `LargeBinary` with extension `minknow.vbz`  → compressed
///   - `LargeList<Int16>`                          → uncompressed
pub fn parse_signal_batches(batches: Vec<RecordBatch>) -> Result<SignalTable> {
    let mut index = Vec::new();

    for (batch_idx, batch) in batches.iter().enumerate() {
        let n = batch.num_rows();
        let samples_col = col!(batch, "samples", UInt32Array)?;

        let signal_col = batch
            .column_by_name("signal")
            .ok_or_else(|| Error::MissingColumn("signal".into()))?;

        let compressed = !matches!(signal_col.data_type(), DataType::LargeList(_));

        for i in 0..n {
            index.push(SignalRowRef {
                batch_idx: batch_idx as u32,
                row_in_batch: i as u32,
                num_samples: samples_col.value(i),
                compressed,
            });
        }
    }

    Ok(SignalTable { batches, index })
}

// ── Reads table ───────────────────────────────────────────────────────────────

/// Parse all Reads-table batches into `ReadRecord` structs.
///
/// `run_infos` must already be built from the RunInfo table; the `run_info`
/// dictionary value (an acquisition_id string) is matched against `run_infos`.
pub fn parse_reads_batches(
    batches: &[RecordBatch],
    run_infos: &[Arc<RunInfo>],
    signal_table: Arc<SignalTable>,
) -> Result<Vec<ReadRecord>> {
    let run_info_by_id: HashMap<&str, Arc<RunInfo>> = run_infos
        .iter()
        .map(|ri| (ri.acquisition_id.as_str(), Arc::clone(ri)))
        .collect();

    let mut out = Vec::new();
    for batch in batches {
        let n = batch.num_rows();

        // Fixed columns
        let read_id_col = col!(batch, "read_id", FixedSizeBinaryArray)?;
        let read_number_col = col!(batch, "read_number", UInt32Array)?;
        let channel_col = col!(batch, "channel", UInt16Array)?;
        let well_col = col!(batch, "well", UInt8Array)?;
        let calib_offset_col = col!(batch, "calibration_offset", Float32Array)?;
        let calib_scale_col = col!(batch, "calibration_scale", Float32Array)?;
        let median_before_col = col!(batch, "median_before", Float32Array)?;
        let num_samples_col = col!(batch, "num_samples", UInt64Array)?;
        let start_col = col!(batch, "start", UInt64Array)?;
        let end_reason_forced_col = col!(batch, "end_reason_forced", BooleanArray)?;
        let num_events_col = col!(batch, "num_minknow_events", UInt64Array)?;
        // Deprecated in newer MinKNOW versions; may be absent in older files.
        let num_reads_mux_col = batch
            .column_by_name("num_reads_since_mux_change")
            .and_then(|c| c.as_any().downcast_ref::<UInt32Array>());
        let time_mux_col = batch
            .column_by_name("time_since_mux_change")
            .and_then(|c| c.as_any().downcast_ref::<Float32Array>());

        // Optional v4 column
        let open_pore_col = batch
            .column_by_name("open_pore_level")
            .and_then(|c| c.as_any().downcast_ref::<Float32Array>());

        // Optional scaling columns
        let tr_scale_col = batch
            .column_by_name("tracked_scaling_scale")
            .and_then(|c| c.as_any().downcast_ref::<Float32Array>());
        let tr_shift_col = batch
            .column_by_name("tracked_scaling_shift")
            .and_then(|c| c.as_any().downcast_ref::<Float32Array>());
        let pr_scale_col = batch
            .column_by_name("predicted_scaling_scale")
            .and_then(|c| c.as_any().downcast_ref::<Float32Array>());
        let pr_shift_col = batch
            .column_by_name("predicted_scaling_shift")
            .and_then(|c| c.as_any().downcast_ref::<Float32Array>());

        // signal column: List<UInt64>
        let signal_list_col = batch
            .column_by_name("signal")
            .ok_or_else(|| Error::MissingColumn("signal".into()))?;
        let signal_list = signal_list_col
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| Error::TypeMismatch("signal".into()))?;

        for i in 0..n {
            // read_id: 16 bytes
            let rid_bytes = read_id_col.value(i);
            let mut read_id = [0u8; 16];
            read_id.copy_from_slice(rid_bytes);

            // signal row indices
            let start_off = signal_list.value_offsets()[i] as usize;
            let end_off = signal_list.value_offsets()[i + 1] as usize;
            let inner = signal_list
                .values()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| Error::TypeMismatch("signal (inner)".into()))?;
            let signal_row_indices: Vec<u64> =
                (start_off..end_off).map(|j| inner.value(j)).collect();

            // pore
            let pore_type = dict_str(batch, "pore_type", i)?.to_owned();
            let pore = Pore {
                channel: channel_col.value(i),
                well: well_col.value(i),
                pore_type,
            };

            // calibration
            let calibration = Calibration {
                offset: calib_offset_col.value(i),
                scale: calib_scale_col.value(i),
            };

            // end reason
            let reason_str = dict_str(batch, "end_reason", i)?;
            let end_reason = EndReason {
                reason: EndReasonKind::from_reason_str(reason_str),
                forced: end_reason_forced_col.value(i),
            };

            // run_info: match dictionary value (acquisition_id string) against RunInfo table
            let acq_id = dict_str(batch, "run_info", i)?;
            let run_info = run_info_by_id
                .get(acq_id)
                .cloned()
                .ok_or_else(|| Error::MissingRunInfo(acq_id.to_string()))?;

            // scaling pairs
            let tracked_scaling = match (tr_scale_col, tr_shift_col) {
                (Some(sc), Some(sh)) => ShiftScalePair {
                    scale: sc.value(i),
                    shift: sh.value(i),
                },
                _ => ShiftScalePair::UNAVAILABLE,
            };
            let predicted_scaling = match (pr_scale_col, pr_shift_col) {
                (Some(sc), Some(sh)) => ShiftScalePair {
                    scale: sc.value(i),
                    shift: sh.value(i),
                },
                _ => ShiftScalePair::UNAVAILABLE,
            };

            out.push(ReadRecord {
                read_id,
                read_number: read_number_col.value(i),
                pore,
                calibration,
                end_reason,
                run_info,
                median_before: if median_before_col.is_null(i) {
                    f32::NAN
                } else {
                    median_before_col.value(i)
                },
                num_samples: num_samples_col.value(i),
                start_sample: start_col.value(i),
                tracked_scaling,
                predicted_scaling,
                num_minknow_events: num_events_col.value(i),
                num_reads_since_mux_change: num_reads_mux_col.map_or(0, |c| c.value(i)),
                time_since_mux_change: time_mux_col.map_or(0.0, |c| c.value(i)),
                open_pore_level: open_pore_col.map_or(f32::NAN, |c| {
                    if c.is_null(i) { f32::NAN } else { c.value(i) }
                }),
                signal_row_indices,
                signal_table: Arc::clone(&signal_table),
            });
        }
    }
    Ok(out)
}
