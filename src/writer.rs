/// POD5 file writer — buffered ([`Writer`]) and streaming ([`StreamingWriter`]).
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;

use arrow::array::builder::{
    BooleanBuilder, FixedSizeBinaryBuilder, Float32Builder, Int16Builder, LargeBinaryBuilder,
    ListBuilder, MapBuilder, MapFieldNames, StringBuilder, TimestampMillisecondBuilder,
    UInt8Builder, UInt16Builder, UInt32Builder, UInt64Builder,
};
use arrow::array::types::Int16Type;
use arrow::array::{Array, DictionaryArray, Int16Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::writer::{FileWriter, IpcWriteOptions};
use arrow::record_batch::RecordBatch;

use crate::Result;
use crate::footer;
use crate::format::{FOOTER_MAGIC, SECTION_MARKER_LEN, SIGNATURE, align8};
use crate::signal::compress_vbz;
use crate::types::{ReadRecord, RunInfo};

// ── Compact per-read buffer ───────────────────────────────────────────────────

/// All per-read data needed to build the three IPC sections.
///
/// Signal is VBZ-compressed eagerly; no `Arc<SignalTable>` or raw `Vec<i16>`
/// is retained after construction.
struct BufferedRead {
    read_id: [u8; 16],
    read_number: u32,
    channel: u16,
    well: u8,
    pore_type: String,
    calib_offset: f32,
    calib_scale: f32,
    median_before: f32,
    end_reason: &'static str,
    end_reason_forced: bool,
    num_minknow_events: u64,
    num_reads_since_mux_change: u32,
    num_samples: u64,
    open_pore_level: f32,
    predicted_scaling_scale: f32,
    predicted_scaling_shift: f32,
    ri_idx: usize,
    start_sample: u64,
    time_since_mux_change: f32,
    tracked_scaling_scale: f32,
    tracked_scaling_shift: f32,
    vbz: Vec<u8>,
    vbz_num_samples: u32,
}

impl BufferedRead {
    fn from_read(record: &ReadRecord, signal: &[i16], ri_idx: usize) -> Result<Self> {
        let vbz = compress_vbz(signal)?;
        Ok(Self {
            read_id: record.read_id,
            read_number: record.read_number,
            channel: record.pore.channel,
            well: record.pore.well,
            pore_type: record.pore.pore_type.clone(),
            calib_offset: record.calibration.offset,
            calib_scale: record.calibration.scale,
            median_before: record.median_before,
            end_reason: record.end_reason.reason.as_str(),
            end_reason_forced: record.end_reason.forced,
            num_minknow_events: record.num_minknow_events,
            num_reads_since_mux_change: record.num_reads_since_mux_change,
            num_samples: record.num_samples,
            open_pore_level: record.open_pore_level,
            predicted_scaling_scale: record.predicted_scaling.scale,
            predicted_scaling_shift: record.predicted_scaling.shift,
            ri_idx,
            start_sample: record.start_sample,
            time_since_mux_change: record.time_since_mux_change,
            tracked_scaling_scale: record.tracked_scaling.scale,
            tracked_scaling_shift: record.tracked_scaling.shift,
            vbz,
            vbz_num_samples: signal.len() as u32,
        })
    }
}

// ── Writer ────────────────────────────────────────────────────────────────────

/// Accumulate reads in memory and write a POD5 file in one shot.
///
/// Prefer [`StreamingWriter`] when processing large files where holding all
/// reads in memory simultaneously is undesirable.
///
/// ```no_run
/// use pod5::{Reader, Writer};
///
/// let reader = Reader::open("input.pod5").unwrap();
/// let mut writer = Writer::new();
/// writer.file_identifier = reader.file_identifier().to_string();
/// writer.pod5_version = reader.pod5_version().to_string();
/// for read in reader.reads_iter().take(10) {
///     let signal = read.signal().unwrap();
///     writer.add_read(read.clone(), signal);
/// }
/// writer.write_file("output.pod5").unwrap();
/// ```
#[derive(Debug, Default)]
pub struct Writer {
    /// file_identifier stored in the footer (should be a UUID string).
    pub file_identifier: String,
    /// Software string recorded in the footer.
    pub software: String,
    /// POD5 spec version to record.
    pub pod5_version: String,
    reads: Vec<WriterRead>,
}

#[derive(Debug)]
struct WriterRead {
    record: ReadRecord,
    signal: Vec<i16>,
}

impl Writer {
    /// Create a new empty writer.
    ///
    /// `software` and `pod5_version` are pre-filled from `Cargo.toml`;
    /// set `file_identifier` before calling [`Writer::write_file`].
    pub fn new() -> Self {
        Self {
            file_identifier: String::new(),
            software: concat!(env!("CARGO_PKG_NAME"), " ", env!("CARGO_PKG_VERSION")).to_string(),
            pod5_version: env!("CARGO_PKG_VERSION").to_string(),
            reads: Vec::new(),
        }
    }

    /// Add a read with its decompressed ADC signal.
    pub fn add_read(&mut self, record: ReadRecord, signal: Vec<i16>) {
        self.reads.push(WriterRead { record, signal });
    }

    /// Write to a file path.
    pub fn write_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let file = File::create(path)?;
        let mut out = BufWriter::new(file);
        self.write_to(&mut out)
    }

    /// Write to any `Write` sink (useful for testing with `Vec<u8>`).
    pub fn write_to<W: Write>(&self, out: &mut W) -> Result<()> {
        let mut sw = StreamingWriter::new(out);
        sw.file_identifier = self.file_identifier.clone();
        sw.software = self.software.clone();
        sw.pod5_version = self.pod5_version.clone();
        for wr in &self.reads {
            sw.write_read(&wr.record, &wr.signal)?;
        }
        sw.finish()
    }
}

// ── StreamingWriter ───────────────────────────────────────────────────────────

/// Memory-efficient POD5 writer that VBZ-compresses each read's signal
/// immediately on [`Self::write_read`], rather than accumulating raw `i16` samples.
///
/// The decompressed signal slice is not retained after each call to
/// [`Self::write_read`], making this suitable for writing large files without holding
/// all signal data in memory at once.
///
/// ```no_run
/// use pod5::{Reader, StreamingWriter};
///
/// let reader = Reader::open("input.pod5").unwrap();
/// let mut writer = StreamingWriter::create("output.pod5").unwrap();
/// writer.file_identifier = reader.file_identifier().to_string();
/// writer.pod5_version = reader.pod5_version().to_string();
/// for read in reader.reads_iter() {
///     let signal = read.signal().unwrap();
///     writer.write_read(read, &signal).unwrap();
/// }
/// writer.finish().unwrap();
/// ```
pub struct StreamingWriter<W: Write> {
    /// file_identifier stored in the footer (should be a UUID string).
    pub file_identifier: String,
    /// Software string recorded in the footer.
    pub software: String,
    /// POD5 spec version to record.
    pub pod5_version: String,
    sink: W,
    run_infos: Vec<Arc<RunInfo>>,
    ri_id_to_idx: HashMap<String, usize>,
    reads: Vec<BufferedRead>,
}

impl StreamingWriter<BufWriter<File>> {
    /// Create a `StreamingWriter` writing to a new file at `path`.
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::create(path)?;
        Ok(Self::new(BufWriter::new(file)))
    }
}

impl<W: Write> StreamingWriter<W> {
    /// Create a `StreamingWriter` writing to `sink`.
    pub fn new(sink: W) -> Self {
        Self {
            file_identifier: String::new(),
            software: concat!(env!("CARGO_PKG_NAME"), " ", env!("CARGO_PKG_VERSION")).to_string(),
            pod5_version: env!("CARGO_PKG_VERSION").to_string(),
            sink,
            run_infos: Vec::new(),
            ri_id_to_idx: HashMap::new(),
            reads: Vec::new(),
        }
    }

    /// VBZ-compress `signal` and buffer the read's metadata.
    ///
    /// `signal` is not retained after this call returns.
    pub fn write_read(&mut self, record: &ReadRecord, signal: &[i16]) -> Result<()> {
        let ri_idx = match self
            .ri_id_to_idx
            .get(record.run_info.acquisition_id.as_str())
        {
            Some(&idx) => idx,
            None => {
                let idx = self.run_infos.len();
                self.ri_id_to_idx
                    .insert(record.run_info.acquisition_id.clone(), idx);
                self.run_infos.push(Arc::clone(&record.run_info));
                idx
            }
        };
        self.reads
            .push(BufferedRead::from_read(record, signal, ri_idx)?);
        Ok(())
    }

    /// Finalise and write the complete POD5 file to the sink.
    pub fn finish(mut self) -> Result<()> {
        let section_marker = [0u8; SECTION_MARKER_LEN];

        let ipc_meta = HashMap::from([
            (
                "MINKNOW:file_identifier".to_string(),
                self.file_identifier.clone(),
            ),
            ("MINKNOW:software".to_string(), self.software.clone()),
            (
                "MINKNOW:pod5_version".to_string(),
                self.pod5_version.clone(),
            ),
        ]);

        let runinfo_ipc = build_run_info_ipc(&self.run_infos, ipc_meta.clone())?;
        let signal_ipc = build_signal_ipc(&self.reads, ipc_meta.clone())?;
        let reads_ipc = build_reads_ipc(&self.reads, &self.run_infos, ipc_meta)?;

        let out = &mut self.sink;
        out.write_all(&SIGNATURE)?;
        out.write_all(&section_marker)?;
        let mut pos: u64 = (8 + SECTION_MARKER_LEN) as u64;

        let runinfo_off = pos;
        let runinfo_len = runinfo_ipc.len() as i64;
        write_section(out, &runinfo_ipc, &section_marker, &mut pos)?;

        let signal_off = pos;
        let signal_len = signal_ipc.len() as i64;
        write_section(out, &signal_ipc, &section_marker, &mut pos)?;

        let reads_off = pos;
        let reads_len = reads_ipc.len() as i64;
        write_section(out, &reads_ipc, &section_marker, &mut pos)?;

        let contents = [
            (runinfo_off as i64, runinfo_len, 4i16),
            (signal_off as i64, signal_len, 1i16),
            (reads_off as i64, reads_len, 0i16),
        ];
        let footer_fb = footer::build_footer(
            &self.file_identifier,
            &self.software,
            &self.pod5_version,
            &contents,
        );
        let footer_len = footer_fb.len() as i64;
        let padded = align8(footer_len as u64);
        let footer_pad = (padded - footer_len as u64) as usize;

        out.write_all(&FOOTER_MAGIC)?;
        out.write_all(&footer_fb)?;
        out.write_all(&vec![0u8; footer_pad])?;
        out.write_all(&footer_len.to_le_bytes())?;
        out.write_all(&section_marker)?;
        out.write_all(&SIGNATURE)?;

        Ok(())
    }
}

// ── File writing helper ───────────────────────────────────────────────────────

/// Write one IPC blob: data, 8-byte-aligned padding, then section marker.
/// Updates `pos` to point past the section marker.
fn write_section<W: Write>(
    out: &mut W,
    ipc: &[u8],
    marker: &[u8; SECTION_MARKER_LEN],
    pos: &mut u64,
) -> Result<()> {
    out.write_all(ipc)?;
    *pos += ipc.len() as u64;
    let pad = (align8(*pos) - *pos) as usize;
    out.write_all(&vec![0u8; pad])?;
    *pos += pad as u64;
    out.write_all(marker)?;
    *pos += SECTION_MARKER_LEN as u64;
    Ok(())
}

// ── IPC section builders ──────────────────────────────────────────────────────

fn write_ipc(schema: &Schema, batches: &[RecordBatch]) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    // preserve_dict_id=false: writer assigns unique IDs from schema position rather
    // than reusing the dict_id=0 default from DictionaryArray::try_new, which would
    // cause a "dictionary replacement" error when multiple dict columns coexist.
    let opts = IpcWriteOptions::default().with_preserve_dict_id(false);
    let mut writer = FileWriter::try_new_with_options(&mut buf, schema, opts)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    Ok(buf)
}

fn map_field_names() -> MapFieldNames {
    MapFieldNames {
        entry: "entries".to_string(),
        key: "key".to_string(),
        value: "value".to_string(),
    }
}

fn build_run_info_ipc(
    run_infos: &[Arc<RunInfo>],
    meta: HashMap<String, String>,
) -> Result<Vec<u8>> {
    let map_type = DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Utf8, true),
                ]
                .into(),
            ),
            false,
        )),
        false,
    );

    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("acquisition_id", DataType::Utf8, false),
            Field::new(
                "acquisition_start_time",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                false,
            ),
            Field::new("adc_max", DataType::Int16, false),
            Field::new("adc_min", DataType::Int16, false),
            Field::new("context_tags", map_type.clone(), false),
            Field::new("experiment_name", DataType::Utf8, false),
            Field::new("flow_cell_id", DataType::Utf8, false),
            Field::new("flow_cell_product_code", DataType::Utf8, false),
            Field::new("protocol_name", DataType::Utf8, false),
            Field::new("protocol_run_id", DataType::Utf8, false),
            Field::new(
                "protocol_start_time",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                false,
            ),
            Field::new("sample_id", DataType::Utf8, false),
            Field::new("sample_rate", DataType::UInt16, false),
            Field::new("sequencing_kit", DataType::Utf8, false),
            Field::new("sequencer_position", DataType::Utf8, false),
            Field::new("sequencer_position_type", DataType::Utf8, false),
            Field::new("software", DataType::Utf8, false),
            Field::new("system_name", DataType::Utf8, false),
            Field::new("system_type", DataType::Utf8, false),
            Field::new("tracking_id", map_type, false),
        ],
        meta,
    ));

    let mut acq_id = StringBuilder::new();
    let mut acq_start = TimestampMillisecondBuilder::new().with_timezone("UTC");
    let mut adc_max = Int16Builder::new();
    let mut adc_min = Int16Builder::new();
    let mut context_tags = MapBuilder::new(
        Some(map_field_names()),
        StringBuilder::new(),
        StringBuilder::new(),
    );
    let mut exp_name = StringBuilder::new();
    let mut fc_id = StringBuilder::new();
    let mut fc_code = StringBuilder::new();
    let mut proto_name = StringBuilder::new();
    let mut proto_run = StringBuilder::new();
    let mut proto_start = TimestampMillisecondBuilder::new().with_timezone("UTC");
    let mut sample_id = StringBuilder::new();
    let mut sample_rate = UInt16Builder::new();
    let mut seq_kit = StringBuilder::new();
    let mut seq_pos = StringBuilder::new();
    let mut seq_pos_type = StringBuilder::new();
    let mut software = StringBuilder::new();
    let mut sys_name = StringBuilder::new();
    let mut sys_type = StringBuilder::new();
    let mut tracking_id = MapBuilder::new(
        Some(map_field_names()),
        StringBuilder::new(),
        StringBuilder::new(),
    );

    for ri in run_infos {
        acq_id.append_value(&ri.acquisition_id);
        acq_start.append_value(ri.acquisition_start_time_ms);
        adc_max.append_value(ri.adc_max);
        adc_min.append_value(ri.adc_min);
        for (k, v) in &ri.context_tags {
            context_tags.keys().append_value(k);
            context_tags.values().append_value(v);
        }
        context_tags.append(true)?;
        exp_name.append_value(&ri.experiment_name);
        fc_id.append_value(&ri.flow_cell_id);
        fc_code.append_value(&ri.flow_cell_product_code);
        proto_name.append_value(&ri.protocol_name);
        proto_run.append_value(&ri.protocol_run_id);
        proto_start.append_value(ri.protocol_start_time_ms);
        sample_id.append_value(&ri.sample_id);
        sample_rate.append_value(ri.sample_rate);
        seq_kit.append_value(&ri.sequencing_kit);
        seq_pos.append_value(&ri.sequencer_position);
        seq_pos_type.append_value(&ri.sequencer_position_type);
        software.append_value(&ri.software);
        sys_name.append_value(&ri.system_name);
        sys_type.append_value(&ri.system_type);
        for (k, v) in &ri.tracking_id {
            tracking_id.keys().append_value(k);
            tracking_id.values().append_value(v);
        }
        tracking_id.append(true)?;
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(acq_id.finish()),
            Arc::new(acq_start.finish()),
            Arc::new(adc_max.finish()),
            Arc::new(adc_min.finish()),
            Arc::new(context_tags.finish()),
            Arc::new(exp_name.finish()),
            Arc::new(fc_id.finish()),
            Arc::new(fc_code.finish()),
            Arc::new(proto_name.finish()),
            Arc::new(proto_run.finish()),
            Arc::new(proto_start.finish()),
            Arc::new(sample_id.finish()),
            Arc::new(sample_rate.finish()),
            Arc::new(seq_kit.finish()),
            Arc::new(seq_pos.finish()),
            Arc::new(seq_pos_type.finish()),
            Arc::new(software.finish()),
            Arc::new(sys_name.finish()),
            Arc::new(sys_type.finish()),
            Arc::new(tracking_id.finish()),
        ],
    )?;

    write_ipc(&schema, &[batch])
}

fn build_signal_ipc(reads: &[BufferedRead], meta: HashMap<String, String>) -> Result<Vec<u8>> {
    let uuid_meta = HashMap::from([
        (
            "ARROW:extension:name".to_string(),
            "minknow.uuid".to_string(),
        ),
        ("ARROW:extension:metadata".to_string(), String::new()),
    ]);
    let vbz_meta = HashMap::from([
        (
            "ARROW:extension:name".to_string(),
            "minknow.vbz".to_string(),
        ),
        ("ARROW:extension:metadata".to_string(), String::new()),
    ]);
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("read_id", DataType::FixedSizeBinary(16), false).with_metadata(uuid_meta),
            Field::new("signal", DataType::LargeBinary, false).with_metadata(vbz_meta),
            Field::new("samples", DataType::UInt32, false),
        ],
        meta,
    ));

    let mut read_id_col = FixedSizeBinaryBuilder::new(16);
    let mut signal_col = LargeBinaryBuilder::new();
    let mut samples_col = UInt32Builder::new();

    for br in reads {
        read_id_col.append_value(br.read_id)?;
        signal_col.append_value(&br.vbz);
        samples_col.append_value(br.vbz_num_samples);
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(read_id_col.finish()),
            Arc::new(signal_col.finish()),
            Arc::new(samples_col.finish()),
        ],
    )?;

    write_ipc(&schema, &[batch])
}

/// Build a Dictionary<Int16, Utf8> array by deduplicating string values in insertion order.
fn build_str_dict(values: &[&str]) -> Arc<dyn Array> {
    let mut unique: Vec<&str> = Vec::new();
    let mut val_to_key: HashMap<&str, i16> = HashMap::new();
    let mut keys: Vec<i16> = Vec::with_capacity(values.len());
    for &v in values {
        let k = if let Some(&k) = val_to_key.get(v) {
            k
        } else {
            let k = unique.len() as i16;
            val_to_key.insert(v, k);
            unique.push(v);
            k
        };
        keys.push(k);
    }
    Arc::new(
        DictionaryArray::<Int16Type>::try_new(
            Int16Array::from(keys),
            Arc::new(StringArray::from(unique)),
        )
        .expect("valid dict array"),
    )
}

fn dict_type() -> DataType {
    DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8))
}

fn build_reads_ipc(
    reads: &[BufferedRead],
    run_infos: &[Arc<RunInfo>],
    meta: HashMap<String, String>,
) -> Result<Vec<u8>> {
    let uuid_meta = HashMap::from([
        (
            "ARROW:extension:name".to_string(),
            "minknow.uuid".to_string(),
        ),
        ("ARROW:extension:metadata".to_string(), String::new()),
    ]);
    let signal_item = Arc::new(Field::new("item", DataType::UInt64, true));
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("read_id", DataType::FixedSizeBinary(16), false).with_metadata(uuid_meta),
            Field::new("read_number", DataType::UInt32, false),
            Field::new("channel", DataType::UInt16, false),
            Field::new("well", DataType::UInt8, false),
            Field::new("pore_type", dict_type(), false),
            Field::new("calibration_offset", DataType::Float32, false),
            Field::new("calibration_scale", DataType::Float32, false),
            Field::new("median_before", DataType::Float32, false),
            Field::new("end_reason", dict_type(), false),
            Field::new("end_reason_forced", DataType::Boolean, false),
            Field::new("num_minknow_events", DataType::UInt64, false),
            Field::new("num_reads_since_mux_change", DataType::UInt32, false),
            Field::new("num_samples", DataType::UInt64, false),
            Field::new("open_pore_level", DataType::Float32, false),
            Field::new("predicted_scaling_scale", DataType::Float32, false),
            Field::new("predicted_scaling_shift", DataType::Float32, false),
            Field::new("run_info", dict_type(), false),
            Field::new("start", DataType::UInt64, false),
            Field::new("time_since_mux_change", DataType::Float32, false),
            Field::new("tracked_scaling_scale", DataType::Float32, false),
            Field::new("tracked_scaling_shift", DataType::Float32, false),
            Field::new("signal", DataType::List(signal_item), false),
        ],
        meta,
    ));

    let mut read_id_col = FixedSizeBinaryBuilder::new(16);
    let mut read_number_col = UInt32Builder::new();
    let mut channel_col = UInt16Builder::new();
    let mut well_col = UInt8Builder::new();
    let mut calib_offset_col = Float32Builder::new();
    let mut calib_scale_col = Float32Builder::new();
    let mut median_before_col = Float32Builder::new();
    let mut end_reason_forced_col = BooleanBuilder::new();
    let mut num_events_col = UInt64Builder::new();
    let mut num_reads_mux_col = UInt32Builder::new();
    let mut num_samples_col = UInt64Builder::new();
    let mut open_pore_col = Float32Builder::new();
    let mut pr_scale_col = Float32Builder::new();
    let mut pr_shift_col = Float32Builder::new();
    let mut start_col = UInt64Builder::new();
    let mut time_mux_col = Float32Builder::new();
    let mut tr_scale_col = Float32Builder::new();
    let mut tr_shift_col = Float32Builder::new();
    let mut signal_col = ListBuilder::new(UInt64Builder::new());

    let mut pore_type_vals: Vec<&str> = Vec::with_capacity(reads.len());
    let mut end_reason_vals: Vec<&str> = Vec::with_capacity(reads.len());
    let mut run_info_keys: Vec<i16> = Vec::with_capacity(reads.len());

    for (signal_row_idx, br) in reads.iter().enumerate() {
        read_id_col.append_value(br.read_id)?;
        read_number_col.append_value(br.read_number);
        channel_col.append_value(br.channel);
        well_col.append_value(br.well);
        pore_type_vals.push(&br.pore_type);
        calib_offset_col.append_value(br.calib_offset);
        calib_scale_col.append_value(br.calib_scale);
        median_before_col.append_value(br.median_before);
        end_reason_vals.push(br.end_reason);
        end_reason_forced_col.append_value(br.end_reason_forced);
        num_events_col.append_value(br.num_minknow_events);
        num_reads_mux_col.append_value(br.num_reads_since_mux_change);
        num_samples_col.append_value(br.num_samples);
        open_pore_col.append_value(br.open_pore_level);
        pr_scale_col.append_value(br.predicted_scaling_scale);
        pr_shift_col.append_value(br.predicted_scaling_shift);
        run_info_keys.push(br.ri_idx as i16);
        start_col.append_value(br.start_sample);
        time_mux_col.append_value(br.time_since_mux_change);
        tr_scale_col.append_value(br.tracked_scaling_scale);
        tr_shift_col.append_value(br.tracked_scaling_shift);

        signal_col.values().append_value(signal_row_idx as u64);
        signal_col.append(true);
    }

    let pore_type_col = build_str_dict(&pore_type_vals);
    let end_reason_col = build_str_dict(&end_reason_vals);

    // run_info dictionary: keys = ri_idx per read, values = acquisition IDs in table order
    let ri_vals: Vec<&str> = run_infos
        .iter()
        .map(|r| r.acquisition_id.as_str())
        .collect();
    let run_info_col: Arc<dyn Array> = Arc::new(
        DictionaryArray::<Int16Type>::try_new(
            Int16Array::from(run_info_keys),
            Arc::new(StringArray::from(ri_vals)),
        )
        .expect("valid run_info dict"),
    );

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(read_id_col.finish()),
            Arc::new(read_number_col.finish()),
            Arc::new(channel_col.finish()),
            Arc::new(well_col.finish()),
            pore_type_col,
            Arc::new(calib_offset_col.finish()),
            Arc::new(calib_scale_col.finish()),
            Arc::new(median_before_col.finish()),
            end_reason_col,
            Arc::new(end_reason_forced_col.finish()),
            Arc::new(num_events_col.finish()),
            Arc::new(num_reads_mux_col.finish()),
            Arc::new(num_samples_col.finish()),
            Arc::new(open_pore_col.finish()),
            Arc::new(pr_scale_col.finish()),
            Arc::new(pr_shift_col.finish()),
            run_info_col,
            Arc::new(start_col.finish()),
            Arc::new(time_mux_col.finish()),
            Arc::new(tr_scale_col.finish()),
            Arc::new(tr_shift_col.finish()),
            Arc::new(signal_col.finish()),
        ],
    )?;

    write_ipc(&schema, &[batch])
}
