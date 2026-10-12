/// Arrow IPC table readers for the three POD5 tables.
///
/// Each table is a Feather v2 / Arrow IPC File embedded in the POD5 container.
/// We read the whole section into memory then drive the arrow-ipc FileReader.
use std::collections::HashMap;
use std::fs::File;
use std::io::{Cursor, Read, Seek, SeekFrom};
use std::sync::{Arc, Mutex};

use arrow::array::types::Int16Type;
use arrow::array::{
    Array, BooleanArray, DictionaryArray, FixedSizeBinaryArray, Float32Array, Int16Array,
    ListArray, MapArray, StringArray, TimestampMillisecondArray, UInt8Array, UInt16Array,
    UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::ipc::reader::FileReader;
use arrow::record_batch::RecordBatch;

use crate::Result;
use crate::error::Error;
use crate::signal::{LazySignalRow, SignalRowRef, SignalTable};
use crate::types::{
    Calibration, EndReason, EndReasonKind, Pore, ReadRecord, RunInfo, ShiftScalePair,
};

// ── helpers ───────────────────────────────────────────────────────────────────

/// Read an embedded Arrow IPC section from `file` into memory, then drive the
/// arrow-ipc `FileReader` over it to collect the schema and all record batches.
pub(crate) fn read_ipc_section<R: Read + Seek>(
    reader: &mut R,
    offset: u64,
    length: u64,
) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let mut buf = vec![0u8; length as usize];
    reader.seek(SeekFrom::Start(offset))?;
    reader.read_exact(&mut buf)?;

    let cursor = Cursor::new(buf);
    let ipc = FileReader::try_new(cursor, None)?;
    let schema = ipc.schema();
    let mut batches = Vec::new();
    for batch in ipc {
        batches.push(batch?);
    }
    Ok((schema, batches))
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

pub(crate) fn parse_run_info_batches(batches: &[RecordBatch]) -> Result<Vec<Arc<RunInfo>>> {
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
/// at decompression time (no byte copying).
///
/// The `signal` column may be:
///   - `LargeBinary` with extension `minknow.vbz`  → compressed
///   - `LargeList<Int16>`                          → uncompressed
pub(crate) fn parse_signal_batches(batches: Vec<RecordBatch>) -> Result<SignalTable> {
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

    Ok(SignalTable::Eager { batches, index })
}

/// Build a lazy `SignalTable` by reading only row metadata from the Arrow IPC section.
///
/// Unlike `parse_signal_batches`, this function does NOT load the signal blob data
/// into memory. It reads the Arrow IPC footer and per-batch message headers to extract
/// the file offsets and lengths of each VBZ blob, storing them as `LazySignalRow`
/// entries. Signal bytes are read from `file` on demand when `samples_for_rows` is called.
///
/// `section_offset` and `section_len` are the absolute byte position and length of the
/// Signal IPC section within the .pod5 file (as reported by the FlatBuffers footer).
pub(crate) fn parse_signal_section_lazy(
    file: Arc<Mutex<File>>,
    section_offset: u64,
    section_len: u64,
) -> crate::Result<SignalTable> {
    // ── 1. Read Arrow IPC footer from end of section ──────────────────────────
    // Arrow IPC file footer layout (last 10 bytes of section):
    //   [4 bytes] footer_length (i32 LE, unpadded flatbuffer byte count)
    //   [6 bytes] "ARROW1" magic
    const ARROW_FOOTER_SUFFIX: usize = 10;
    const ARROW_MAGIC: &[u8; 6] = b"ARROW1";

    let mut guard = file
        .lock()
        .map_err(|_| Error::Vbz("signal file mutex poisoned".into()))?;

    let suffix_pos = section_offset + section_len - ARROW_FOOTER_SUFFIX as u64;
    guard.seek(SeekFrom::Start(suffix_pos))?;
    let mut suffix = [0u8; ARROW_FOOTER_SUFFIX];
    guard.read_exact(&mut suffix)?;

    if &suffix[4..10] != ARROW_MAGIC {
        return Err(Error::FlatBuffers(
            "Arrow IPC magic 'ARROW1' not found at end of Signal section".into(),
        ));
    }

    let arrow_footer_len = i32::from_le_bytes(suffix[0..4].try_into().unwrap()) as u64;
    if arrow_footer_len == 0 {
        // Empty signal table
        drop(guard);
        return Ok(SignalTable::Lazy {
            file,
            rows: Vec::new(),
        });
    }

    let arrow_footer_start =
        section_offset + section_len - ARROW_FOOTER_SUFFIX as u64 - arrow_footer_len;
    guard.seek(SeekFrom::Start(arrow_footer_start))?;
    let mut footer_bytes = vec![0u8; arrow_footer_len as usize];
    guard.read_exact(&mut footer_bytes)?;

    let arrow_footer = arrow::ipc::root_as_footer(&footer_bytes)
        .map_err(|e| Error::FlatBuffers(format!("Arrow IPC footer parse error: {e}")))?;

    // ── 2. Determine VBZ vs uncompressed from schema ──────────────────────────
    // VBZ: field 1 is LargeBinary (type_type != LargeList)
    // Uncompressed: field 1 is LargeList
    let is_vbz = arrow_footer
        .schema()
        .and_then(|s| s.fields())
        .map(|fields| {
            // Signal table schema: field 0 = read_id (FixedSizeBinary), field 1 = signal
            fields.len() <= 1 || fields.get(1).type_type() != arrow::ipc::Type::LargeList
        })
        .unwrap_or(true); // default to VBZ

    // ── 3. Parse each RecordBatch block ───────────────────────────────────────
    let record_batches = match arrow_footer.recordBatches() {
        Some(rb) => rb,
        None => {
            drop(guard);
            return Ok(SignalTable::Lazy {
                file,
                rows: Vec::new(),
            });
        }
    };

    let mut rows: Vec<LazySignalRow> = Vec::new();

    for block in record_batches {
        // Arrow IPC block offsets are relative to the section start, NOT absolute.
        let abs_msg_start = section_offset + block.offset() as u64;
        let meta_len = block.metaDataLength() as u64;
        let body_start = abs_msg_start + meta_len;

        // Read the message header (continuation marker + size + flatbuffer)
        guard.seek(SeekFrom::Start(abs_msg_start))?;
        let mut meta_bytes = vec![0u8; meta_len as usize];
        guard.read_exact(&mut meta_bytes)?;

        // Arrow IPC message header layout:
        //   [4 bytes] continuation marker (0xFFFFFFFF)
        //   [4 bytes] metadata size (i32 LE)
        //   [N bytes] flatbuffer message
        //   [padding] to 8-byte boundary
        if meta_bytes.len() < 8 {
            return Err(Error::FlatBuffers(
                "Arrow IPC message header too short".into(),
            ));
        }
        let msg = arrow::ipc::root_as_message(&meta_bytes[8..])
            .map_err(|e| Error::FlatBuffers(format!("Arrow IPC message parse error: {e}")))?;

        let rb = msg
            .header_as_record_batch()
            .ok_or_else(|| Error::FlatBuffers("Arrow IPC message is not a RecordBatch".into()))?;

        let n_rows = rb.length() as usize;
        if n_rows == 0 {
            continue;
        }

        let buffers = rb
            .buffers()
            .ok_or_else(|| Error::FlatBuffers("RecordBatch has no buffers".into()))?;

        if is_vbz {
            // VBZ (LargeBinary) buffer layout for Signal table schema:
            //   FixedSizeBinary(16): buffers[0] validity, buffers[1] data
            //   LargeBinary:         buffers[2] validity, buffers[3] int64 offsets, buffers[4] blob data
            //   UInt32:              buffers[5] validity, buffers[6] data
            if buffers.len() < 7 {
                return Err(Error::FlatBuffers(format!(
                    "VBZ signal batch has only {} buffers (expected ≥7)",
                    buffers.len()
                )));
            }

            // Read int64 offsets buffer (buffers[3]): (n_rows+1) × 8 bytes
            let offsets_buf = buffers.get(3);
            let samples_buf = buffers.get(6);
            let data_buf = buffers.get(4);

            let offsets_abs = body_start + offsets_buf.offset() as u64;
            let samples_abs = body_start + samples_buf.offset() as u64;
            let data_abs = body_start + data_buf.offset() as u64;

            // Read the int64 offsets
            guard.seek(SeekFrom::Start(offsets_abs))?;
            let mut offsets_bytes = vec![0u8; (n_rows + 1) * 8];
            guard.read_exact(&mut offsets_bytes)?;
            let int64_offsets: Vec<i64> = offsets_bytes
                .chunks_exact(8)
                .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
                .collect();

            // Read the UInt32 num_samples
            guard.seek(SeekFrom::Start(samples_abs))?;
            let mut samples_bytes = vec![0u8; n_rows * 4];
            guard.read_exact(&mut samples_bytes)?;
            let num_samples_vec: Vec<u32> = samples_bytes
                .chunks_exact(4)
                .map(|c| u32::from_le_bytes(c.try_into().unwrap()))
                .collect();

            for i in 0..n_rows {
                let file_offset = data_abs + int64_offsets[i] as u64;
                let compressed_len = (int64_offsets[i + 1] - int64_offsets[i]) as u32;
                let num_samples = num_samples_vec[i];
                rows.push(LazySignalRow::Compressed {
                    file_offset,
                    compressed_len,
                    num_samples,
                });
            }
        } else {
            // Uncompressed (LargeList<Int16>) buffer layout:
            //   FixedSizeBinary(16): buffers[0] validity, buffers[1] data
            //   LargeList<Int16>:    buffers[2] validity, buffers[3] list offsets, buffers[4] child validity, buffers[5] child data
            //   UInt32:              buffers[6] validity, buffers[7] data
            if buffers.len() < 6 {
                return Err(Error::FlatBuffers(format!(
                    "Uncompressed signal batch has only {} buffers (expected ≥6)",
                    buffers.len()
                )));
            }

            let list_offsets_buf = buffers.get(3);
            let child_data_buf = buffers.get(5);

            let list_offsets_abs = body_start + list_offsets_buf.offset() as u64;
            let child_data_abs = body_start + child_data_buf.offset() as u64;

            // Read the list int64 offsets
            guard.seek(SeekFrom::Start(list_offsets_abs))?;
            let mut offsets_bytes = vec![0u8; (n_rows + 1) * 8];
            guard.read_exact(&mut offsets_bytes)?;
            let list_offsets: Vec<i64> = offsets_bytes
                .chunks_exact(8)
                .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
                .collect();

            // Read the child Int16 data (all at once for this batch)
            let total_samples = list_offsets[n_rows] as usize;
            guard.seek(SeekFrom::Start(child_data_abs))?;
            let mut child_bytes = vec![0u8; total_samples * 2];
            guard.read_exact(&mut child_bytes)?;
            let child_i16: Vec<i16> = child_bytes
                .chunks_exact(2)
                .map(|c| i16::from_le_bytes(c.try_into().unwrap()))
                .collect();

            for i in 0..n_rows {
                let start = list_offsets[i] as usize;
                let end = list_offsets[i + 1] as usize;
                let samples: Box<[i16]> = child_i16[start..end].to_vec().into_boxed_slice();
                rows.push(LazySignalRow::Uncompressed(samples));
            }
        }
    }

    drop(guard);
    Ok(SignalTable::Lazy { file, rows })
}

// ── Reads table ───────────────────────────────────────────────────────────────

/// Detect the Reads-table schema version from its columns, as upstream pod5 does.
///
/// - V4 adds `open_pore_level`
/// - V5 adds `expected_open_pore_level` and `selected_read_level`
/// - V6 widens `channel` to `UInt32`
/// - V7 restores `channel` to `UInt16` and adds `channel_32bit` (`UInt32`)
///
/// Tables older than V4 report 3.
pub(crate) fn reads_table_version(schema: &Schema) -> u8 {
    let has = |name: &str, dt: &DataType| {
        schema
            .field_with_name(name)
            .is_ok_and(|f| f.data_type() == dt)
    };
    if has("channel_32bit", &DataType::UInt32) {
        7
    } else if has("channel", &DataType::UInt32) {
        6
    } else if has("expected_open_pore_level", &DataType::Float32)
        && has("selected_read_level", &DataType::Float32)
    {
        5
    } else if has("open_pore_level", &DataType::Float32) {
        4
    } else {
        3
    }
}

/// Channel number at row `i`, accepting the V3–V5/V7 `UInt16` layout, the V6
/// `UInt32` layout, and preferring the V7 `channel_32bit` column when present.
enum ChannelCol<'a> {
    U16(&'a UInt16Array),
    U32(&'a UInt32Array),
}

impl<'a> ChannelCol<'a> {
    fn from_batch(batch: &'a RecordBatch) -> Result<Self> {
        if let Some(c) = batch
            .column_by_name("channel_32bit")
            .and_then(|c| c.as_any().downcast_ref::<UInt32Array>())
        {
            return Ok(Self::U32(c));
        }
        let c = batch
            .column_by_name("channel")
            .ok_or_else(|| Error::MissingColumn("channel".into()))?;
        if let Some(c) = c.as_any().downcast_ref::<UInt16Array>() {
            Ok(Self::U16(c))
        } else if let Some(c) = c.as_any().downcast_ref::<UInt32Array>() {
            Ok(Self::U32(c))
        } else {
            Err(Error::TypeMismatch("channel".into()))
        }
    }

    fn value(&self, i: usize) -> u32 {
        match self {
            Self::U16(c) => c.value(i) as u32,
            Self::U32(c) => c.value(i),
        }
    }
}

/// Value of an optional nullable `Float32` column at row `i`; NaN if absent or null.
fn opt_f32(col: Option<&Float32Array>, i: usize) -> f32 {
    col.map_or(
        f32::NAN,
        |c| if c.is_null(i) { f32::NAN } else { c.value(i) },
    )
}

/// Parse all Reads-table batches into `ReadRecord` structs.
///
/// `run_infos` must already be built from the RunInfo table; the `run_info`
/// dictionary value (an acquisition_id string) is matched against `run_infos`.
pub(crate) fn parse_reads_batches(
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
        let channel_col = ChannelCol::from_batch(batch)?;
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

        // Optional v4/v5 columns
        let open_pore_col = batch
            .column_by_name("open_pore_level")
            .and_then(|c| c.as_any().downcast_ref::<Float32Array>());
        let expected_open_pore_col = batch
            .column_by_name("expected_open_pore_level")
            .and_then(|c| c.as_any().downcast_ref::<Float32Array>());
        let selected_read_level_col = batch
            .column_by_name("selected_read_level")
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
                open_pore_level: opt_f32(open_pore_col, i),
                expected_open_pore_level: opt_f32(expected_open_pore_col, i),
                selected_read_level: opt_f32(selected_read_level_col, i),
                signal_row_indices,
                signal_table: Arc::clone(&signal_table),
            });
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;

    fn reads_schema(extra: &[(&str, DataType)], channel: DataType) -> Schema {
        let mut fields = vec![
            Field::new("read_id", DataType::FixedSizeBinary(16), false),
            Field::new("channel", channel, false),
        ];
        fields.extend(extra.iter().map(|(n, t)| Field::new(*n, t.clone(), true)));
        Schema::new(fields)
    }

    #[test]
    fn detects_reads_table_versions() {
        let v4 = [("open_pore_level", DataType::Float32)];
        let v5 = [
            ("open_pore_level", DataType::Float32),
            ("expected_open_pore_level", DataType::Float32),
            ("selected_read_level", DataType::Float32),
        ];
        let v7 = [
            ("open_pore_level", DataType::Float32),
            ("expected_open_pore_level", DataType::Float32),
            ("selected_read_level", DataType::Float32),
            ("channel_32bit", DataType::UInt32),
        ];
        assert_eq!(reads_table_version(&reads_schema(&[], DataType::UInt16)), 3);
        assert_eq!(reads_table_version(&reads_schema(&v4, DataType::UInt16)), 4);
        assert_eq!(reads_table_version(&reads_schema(&v5, DataType::UInt16)), 5);
        assert_eq!(reads_table_version(&reads_schema(&v5, DataType::UInt32)), 6);
        assert_eq!(reads_table_version(&reads_schema(&v7, DataType::UInt16)), 7);
    }

    fn batch(cols: Vec<(&str, Arc<dyn Array>)>) -> RecordBatch {
        RecordBatch::try_from_iter(cols).unwrap()
    }

    #[test]
    fn channel_u16() {
        let b = batch(vec![("channel", Arc::new(UInt16Array::from(vec![7u16])))]);
        assert_eq!(ChannelCol::from_batch(&b).unwrap().value(0), 7);
    }

    #[test]
    fn channel_v6_u32() {
        let b = batch(vec![(
            "channel",
            Arc::new(UInt32Array::from(vec![70_000u32])),
        )]);
        assert_eq!(ChannelCol::from_batch(&b).unwrap().value(0), 70_000);
    }

    #[test]
    fn channel_v7_prefers_channel_32bit() {
        let b = batch(vec![
            ("channel", Arc::new(UInt16Array::from(vec![0u16]))),
            (
                "channel_32bit",
                Arc::new(UInt32Array::from(vec![70_000u32])),
            ),
        ]);
        assert_eq!(ChannelCol::from_batch(&b).unwrap().value(0), 70_000);
    }

    #[test]
    fn channel_wrong_type_errors() {
        let b = batch(vec![("channel", Arc::new(UInt8Array::from(vec![1u8])))]);
        assert!(matches!(
            ChannelCol::from_batch(&b),
            Err(Error::TypeMismatch(_))
        ));
    }
}
