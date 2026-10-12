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

    check_ipc_blocks(&buf)?;
    // arrow-ipc can panic on malformed input instead of returning an error;
    // treat that as a malformed file.
    std::panic::catch_unwind(move || -> Result<(SchemaRef, Vec<RecordBatch>)> {
        let ipc = FileReader::try_new(Cursor::new(buf), None)?;
        let schema = ipc.schema();
        let batches = ipc.collect::<std::result::Result<Vec<_>, _>>()?;
        Ok((schema, batches))
    })
    .unwrap_or_else(|_| Err(malformed("Arrow IPC table could not be decoded")))
}

/// arrow-ipc sizes its read buffers from the block table in the Arrow IPC footer
/// without checking them against the data, so a corrupt length becomes a huge
/// allocation. Check every block lies inside the section before handing it over.
fn check_ipc_blocks(buf: &[u8]) -> Result<()> {
    // Trailer: [i32 footer length]["ARROW1"]
    let n = buf.len();
    if n < 10 || &buf[n - 6..] != b"ARROW1" {
        return Err(malformed("Arrow IPC magic not found at end of table"));
    }
    let footer_start = usize::try_from(i32::from_le_bytes(buf[n - 10..n - 6].try_into().unwrap()))
        .ok()
        .and_then(|len| (n - 10).checked_sub(len))
        .ok_or_else(|| malformed("invalid Arrow footer length"))?;
    let footer = arrow::ipc::root_as_footer(&buf[footer_start..n - 10])
        .map_err(|e| Error::FlatBuffers(format!("Arrow IPC footer parse error: {e}")))?;
    let blocks = footer.dictionaries().into_iter().flatten();
    for block in blocks.chain(footer.recordBatches().into_iter().flatten()) {
        let end = add_offset(0, block.offset())
            .and_then(|start| add_offset(start, block.metaDataLength() as i64))
            .and_then(|end| add_offset(end, block.bodyLength()))?;
        if end > footer_start as u64 {
            return Err(malformed("Arrow IPC block extends past its table"));
        }
    }
    Ok(())
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
    let key = usize::try_from(dict.keys().value(i))
        .ok()
        .filter(|&k| k < values.len())
        .ok_or_else(|| malformed(&format!("dictionary key out of range in '{name}'")))?;
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

/// Bounds-checked reads from one embedded section of the file. Offsets come from
/// the file itself, so every range is checked to lie inside the section.
struct Section<'f> {
    file: &'f mut File,
    start: u64,
    len: u64,
}

impl Section<'_> {
    /// Read `len` bytes at offset `rel` from the section start.
    fn read(&mut self, rel: u64, len: u64) -> Result<Vec<u8>> {
        if rel.checked_add(len).is_none_or(|end| end > self.len) {
            return Err(malformed("Signal table offset outside its section"));
        }
        self.file.seek(SeekFrom::Start(self.start + rel))?;
        let mut buf = vec![0u8; len as usize];
        self.file.read_exact(&mut buf)?;
        Ok(buf)
    }
}

fn malformed(what: &str) -> Error {
    Error::Malformed(what.to_string())
}

/// `a + b` for file offsets, failing on overflow or negative inputs.
fn add_offset(a: u64, b: i64) -> Result<u64> {
    u64::try_from(b)
        .ok()
        .and_then(|b| a.checked_add(b))
        .ok_or_else(|| malformed("invalid Signal table offset"))
}

/// Build a `SignalTable` for `Reader::open`, reading only row metadata.
///
/// For the layout upstream writes (VBZ `signal`, columns `read_id, signal, samples`,
/// no Arrow body compression) this reads the Arrow IPC footer and per-batch message
/// headers to find each VBZ blob's position, and returns a lazy table that reads
/// blobs from `file` on demand. Any other layout falls back to loading the section
/// with [`parse_signal_batches`].
///
/// `section_offset` and `section_len` are the absolute byte position and length of the
/// Signal IPC section (already checked to lie inside the file). The schema's
/// `MINKNOW:file_identifier` is checked against `file_identifier`.
pub(crate) fn parse_signal_section_lazy(
    file: Arc<Mutex<File>>,
    section_offset: u64,
    section_len: u64,
    file_identifier: &str,
) -> Result<SignalTable> {
    let rows = {
        let mut guard = file.lock().unwrap_or_else(|e| e.into_inner());
        let mut section = Section {
            file: &mut guard,
            start: section_offset,
            len: section_len,
        };
        match lazy_signal_rows(&mut section, file_identifier)? {
            Some(rows) => rows,
            None => {
                let (schema, batches) = read_ipc_section(&mut *guard, section_offset, section_len)?;
                check_file_identifier("Signal", &schema, file_identifier)?;
                return parse_signal_batches(batches);
            }
        }
    };
    Ok(SignalTable::Lazy { file, rows })
}

/// Row metadata for the lazy fast path, or `None` if the section's layout needs
/// the general (eager) parser.
fn lazy_signal_rows(
    section: &mut Section<'_>,
    file_identifier: &str,
) -> Result<Option<Vec<LazySignalRow>>> {
    // Arrow IPC file trailer: [i32 footer length]["ARROW1"]
    const SUFFIX: u64 = 10;
    if section.len < SUFFIX {
        return Err(malformed("Signal table section too short"));
    }
    let suffix = section.read(section.len - SUFFIX, SUFFIX)?;
    if &suffix[4..10] != b"ARROW1" {
        return Err(malformed(
            "Arrow IPC magic not found at end of Signal table",
        ));
    }
    let footer_len = u64::try_from(i32::from_le_bytes(suffix[0..4].try_into().unwrap()))
        .map_err(|_| malformed("negative Arrow footer length"))?;
    let footer_start = (section.len - SUFFIX)
        .checked_sub(footer_len)
        .ok_or_else(|| malformed("Arrow footer length exceeds Signal table"))?;
    let footer_bytes = section.read(footer_start, footer_len)?;
    let footer = arrow::ipc::root_as_footer(&footer_bytes)
        .map_err(|e| Error::FlatBuffers(format!("Arrow IPC footer parse error: {e}")))?;

    // Only the layout upstream writes takes the fast path.
    let Some(schema) = footer.schema() else {
        return Ok(None);
    };
    let schema_id = schema.custom_metadata().and_then(|kvs| {
        kvs.iter()
            .find(|kv| kv.key() == Some("MINKNOW:file_identifier"))
            .and_then(|kv| kv.value())
    });
    check_file_identifier_value("Signal", schema_id, file_identifier)?;
    let expected = [
        ("read_id", arrow::ipc::Type::FixedSizeBinary),
        ("signal", arrow::ipc::Type::LargeBinary),
        ("samples", arrow::ipc::Type::Int),
    ];
    let layout_ok = schema.fields().is_some_and(|fields| {
        fields.len() == expected.len()
            && fields
                .iter()
                .zip(expected)
                .all(|(f, (name, ty))| f.name() == Some(name) && f.type_type() == ty)
    });
    if !layout_ok {
        return Ok(None);
    }

    let mut rows = Vec::new();
    for block in footer.recordBatches().into_iter().flatten() {
        let msg_start = add_offset(0, block.offset())?;
        let meta_len = u64::try_from(block.metaDataLength())
            .map_err(|_| malformed("negative Arrow metadata length"))?;
        let body_start = add_offset(msg_start, meta_len as i64)?;
        let body_len = u64::try_from(block.bodyLength())
            .map_err(|_| malformed("negative Arrow body length"))?;

        // Message header: [0xFFFFFFFF][i32 size][flatbuffer Message][padding]
        let meta = section.read(msg_start, meta_len)?;
        if meta.len() < 8 {
            return Err(malformed("Arrow IPC message header too short"));
        }
        let msg = arrow::ipc::root_as_message(&meta[8..])
            .map_err(|e| Error::FlatBuffers(format!("Arrow IPC message parse error: {e}")))?;
        let rb = msg
            .header_as_record_batch()
            .ok_or_else(|| malformed("Arrow IPC message is not a record batch"))?;
        if rb.compression().is_some() {
            return Ok(None);
        }
        let n_rows =
            usize::try_from(rb.length()).map_err(|_| malformed("negative Signal batch length"))?;
        if n_rows == 0 {
            continue;
        }
        // Buffers: read_id [0] validity, [1] data; signal [2] validity, [3] i64
        // offsets, [4] blob data; samples [5] validity, [6] data.
        let buffers = rb
            .buffers()
            .filter(|b| b.len() == 7)
            .ok_or_else(|| malformed("unexpected Signal batch buffer count"))?;
        let body_buffer = |i: usize, min_len: u64| -> Result<(u64, u64)> {
            let b = buffers.get(i);
            let (off, len) = (add_offset(0, b.offset())?, add_offset(0, b.length())?);
            if off.checked_add(len).is_none_or(|end| end > body_len) || len < min_len {
                return Err(malformed("Signal batch buffer outside its message body"));
            }
            Ok((body_start + off, len))
        };
        let n = n_rows as u64;
        let offsets_len = n
            .checked_add(1)
            .and_then(|k| k.checked_mul(8))
            .ok_or_else(|| malformed("Signal batch too long"))?;
        let (offsets_pos, _) = body_buffer(3, offsets_len)?;
        let (data_pos, data_len) = body_buffer(4, 0)?;
        let (samples_pos, _) = body_buffer(6, n * 4)?;

        let offsets: Vec<i64> = section
            .read(offsets_pos, offsets_len)?
            .chunks_exact(8)
            .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
            .collect();
        let samples = section.read(samples_pos, n * 4)?;
        for (i, w) in offsets.windows(2).enumerate() {
            let (start, end) = (w[0], w[1]);
            if start < 0 || end < start || end as u64 > data_len {
                return Err(malformed("invalid Signal blob offsets"));
            }
            rows.push(LazySignalRow {
                file_offset: section.start + data_pos + start as u64,
                compressed_len: u32::try_from(end - start)
                    .map_err(|_| malformed("Signal blob too large"))?,
                num_samples: u32::from_le_bytes(samples[i * 4..i * 4 + 4].try_into().unwrap()),
            });
        }
    }
    Ok(Some(rows))
}

/// Error if a table's `MINKNOW:file_identifier` schema metadata disagrees with the footer.
pub(crate) fn check_file_identifier(table: &str, schema: &Schema, footer_id: &str) -> Result<()> {
    check_file_identifier_value(
        table,
        schema
            .metadata()
            .get("MINKNOW:file_identifier")
            .map(String::as_str),
        footer_id,
    )
}

fn check_file_identifier_value(
    table: &str,
    schema_id: Option<&str>,
    footer_id: &str,
) -> Result<()> {
    match schema_id {
        Some(id) if id != footer_id => Err(Error::FileIdentifierMismatch(
            table.to_string(),
            id.to_string(),
            footer_id.to_string(),
        )),
        _ => Ok(()),
    }
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
        if read_id_col.value_length() != 16 {
            return Err(Error::TypeMismatch("read_id".into()));
        }
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
