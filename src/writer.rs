/// POD5 file writer: buffered ([`Writer`]) and streaming ([`StreamingWriter`]).
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::{Arc, mpsc};
use std::thread;

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

use rayon::prelude::*;

use crate::error::Error;
use crate::footer;
use crate::format::{
    FOOTER_MAGIC, MIN_POD5_VERSION, SECTION_MARKER_LEN, SIGNATURE, align8, parse_pod5_version,
};
use crate::signal::compress_vbz;
use crate::types::{ReadRecord, RunInfo};
use crate::{POD5_COMPAT_VERSION, Result};
use uuid::Uuid;

/// Number of in-flight reads buffered between caller and writer thread.
/// Bounds peak RSS to roughly CHANNEL_DEPTH × avg-VBZ-bytes-per-read.
const CHANNEL_DEPTH: usize = 64;

/// Default number of reads accumulated per rayon compression burst and Arrow signal batch.
/// Matches `CHANNEL_DEPTH` so one burst fills the channel exactly, maximising pipeline overlap.
/// Tune upward for throughput at the cost of RSS; below 16 rayon under-utilises the CPU pool.
pub const DEFAULT_SIGNAL_BATCH_SIZE: usize = 64;

// ── Per-read metadata (no signal bytes) ──────────────────────────────────────

struct ReadMeta {
    read_id: [u8; 16],
    read_number: u32,
    channel: u32,
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
    expected_open_pore_level: f32,
    selected_read_level: f32,
    predicted_scaling_scale: f32,
    predicted_scaling_shift: f32,
    ri_idx: usize,
    start_sample: u64,
    time_since_mux_change: f32,
    tracked_scaling_scale: f32,
    tracked_scaling_shift: f32,
}

fn read_meta_from_record(record: &ReadRecord, ri_idx: usize) -> ReadMeta {
    ReadMeta {
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
        expected_open_pore_level: record.expected_open_pore_level,
        selected_read_level: record.selected_read_level,
        predicted_scaling_scale: record.predicted_scaling.scale,
        predicted_scaling_shift: record.predicted_scaling.shift,
        ri_idx,
        start_sample: record.start_sample,
        time_since_mux_change: record.time_since_mux_change,
        tracked_scaling_scale: record.tracked_scaling.scale,
        tracked_scaling_shift: record.tracked_scaling.shift,
    }
}

// ── Message sent from caller thread to writer thread ─────────────────────────

struct WriterMsg {
    read_id: [u8; 16],
    read_number: u32,
    channel: u32,
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
    expected_open_pore_level: f32,
    selected_read_level: f32,
    predicted_scaling_scale: f32,
    predicted_scaling_shift: f32,
    run_info: Arc<RunInfo>,
    start_sample: u64,
    time_since_mux_change: f32,
    tracked_scaling_scale: f32,
    tracked_scaling_shift: f32,
    vbz: Vec<u8>,
    vbz_num_samples: u32,
}

// ── Writer configuration ──────────────────────────────────────────────────────

struct WriterConfig {
    file_identifier: String,
    software: String,
    pod5_version: String,
    signal_batch_size: usize,
}

// ── StreamingWriter state machine ─────────────────────────────────────────────

enum WriterState {
    Idle(BufWriter<File>),
    Running {
        tx: mpsc::SyncSender<WriterMsg>,
        thread: thread::JoinHandle<Result<()>>,
    },
    /// Transient during `start()`; also set if the state is consumed and not restored.
    Poisoned,
}

// ── Writer ────────────────────────────────────────────────────────────────────

/// Accumulate reads in memory and write a POD5 file in one shot.
///
/// Prefer [`StreamingWriter`] when processing large files where holding all
/// reads in memory simultaneously is undesirable.
///
/// ```no_run
/// use pod5lib::{Reader, Writer};
///
/// let reader = Reader::open("input.pod5").unwrap();
/// let mut writer = Writer::new();
/// for read in reader.reads_iter().take(10) {
///     let signal = read.signal().unwrap();
///     writer.add_read(read.clone(), signal);
/// }
/// writer.write_file("output.pod5").unwrap();
/// ```
#[derive(Debug)]
pub struct Writer {
    /// UUID identifying the file, stored in the footer and schema metadata.
    ///
    /// Defaults to a fresh random (v4) UUID, as upstream `pod5` generates for every
    /// new file. Any UUID form accepted by the `uuid` crate may be set; it is written
    /// in canonical lowercase hyphenated form. Writing fails with
    /// [`Error::InvalidFileIdentifier`] if it is not a UUID.
    pub file_identifier: String,
    /// Software string recorded in the footer and schema metadata; free text, not
    /// interpreted by readers. Defaults to `"pod5lib-rs <crate version> (pod5 <compat version>)"`.
    pub software: String,
    /// `pod5_version` recorded in the footer and schema metadata. Defaults to
    /// [`POD5_COMPAT_VERSION`]. Upstream parses this, so it must be `X.Y.Z` and at
    /// least `0.0.38`; writing fails with [`Error::InvalidPod5Version`] otherwise.
    /// Don't copy it from a source file: the written Reads table is always V7.
    pub pod5_version: String,
    /// Number of reads per Arrow signal batch. Tune for write throughput vs. peak RSS.
    pub signal_batch_size: usize,
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
    /// `file_identifier`, `software` and `pod5_version` are pre-filled (see the field docs).
    pub fn new() -> Self {
        Self {
            file_identifier: Uuid::new_v4().to_string(),
            software: default_software(),
            pod5_version: POD5_COMPAT_VERSION.to_string(),
            signal_batch_size: DEFAULT_SIGNAL_BATCH_SIZE,
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
        validate_pod5_version(&self.pod5_version)?;
        let cfg = WriterConfig {
            file_identifier: canonical_file_identifier(&self.file_identifier)?,
            software: self.software.clone(),
            pod5_version: self.pod5_version.clone(),
            signal_batch_size: self.signal_batch_size,
        };
        write_pod5_to_sink(out, &self.reads, &cfg)
    }
}

impl Default for Writer {
    fn default() -> Self {
        Self::new()
    }
}

// ── StreamingWriter ───────────────────────────────────────────────────────────

/// Memory-efficient, threaded POD5 writer.
///
/// VBZ compression runs on the calling thread; a background writer thread
/// owns the `FileWriter` and flushes completed batches to disk.  A bounded
/// channel limits in-flight reads so peak RSS stays
/// near-constant regardless of file size.
///
/// ```no_run
/// use pod5lib::{Reader, StreamingWriter};
///
/// let reader = Reader::open("input.pod5").unwrap();
/// let mut writer = StreamingWriter::create("output.pod5").unwrap();
/// for read in reader.reads_iter() {
///     let signal = read.signal().unwrap();
///     writer.write_read(read, &signal).unwrap();
/// }
/// writer.finish().unwrap();
/// ```
pub struct StreamingWriter {
    /// UUID identifying the file, stored in the footer and schema metadata.
    ///
    /// Defaults to a fresh random (v4) UUID, as upstream `pod5` generates for every
    /// new file. Any UUID form accepted by the `uuid` crate may be set; it is written
    /// in canonical lowercase hyphenated form. Writing fails with
    /// [`Error::InvalidFileIdentifier`] if it is not a UUID.
    pub file_identifier: String,
    /// Software string recorded in the footer and schema metadata; free text, not
    /// interpreted by readers. Defaults to `"pod5lib-rs <crate version> (pod5 <compat version>)"`.
    pub software: String,
    /// `pod5_version` recorded in the footer and schema metadata. Defaults to
    /// [`POD5_COMPAT_VERSION`]. Upstream parses this, so it must be `X.Y.Z` and at
    /// least `0.0.38`; writing fails with [`Error::InvalidPod5Version`] otherwise.
    /// Don't copy it from a source file: the written Reads table is always V7.
    pub pod5_version: String,
    /// Number of reads per Arrow signal batch. Tune for write throughput vs. peak RSS.
    pub signal_batch_size: usize,
    channel_depth: usize,
    raw_buf: Vec<(ReadRecord, Vec<i16>)>,
    state: WriterState,
}

impl StreamingWriter {
    /// Open `path` for writing and return a new `StreamingWriter`.
    ///
    /// `file_identifier`, `software` and `pod5_version` are pre-filled; change them
    /// before the first call to [`Self::write_read`].  The background writer thread is
    /// started lazily on the first write.
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::create(path)?;
        Ok(Self {
            file_identifier: Uuid::new_v4().to_string(),
            software: default_software(),
            pod5_version: POD5_COMPAT_VERSION.to_string(),
            signal_batch_size: DEFAULT_SIGNAL_BATCH_SIZE,
            channel_depth: CHANNEL_DEPTH,
            raw_buf: Vec::with_capacity(DEFAULT_SIGNAL_BATCH_SIZE),
            state: WriterState::Idle(BufWriter::new(file)),
        })
    }

    /// Lazily start the writer thread, capturing the current config.
    fn start(&mut self) -> Result<()> {
        validate_pod5_version(&self.pod5_version)?;
        let cfg = WriterConfig {
            file_identifier: canonical_file_identifier(&self.file_identifier)?,
            software: self.software.clone(),
            pod5_version: self.pod5_version.clone(),
            signal_batch_size: self.signal_batch_size,
        };
        let out = match std::mem::replace(&mut self.state, WriterState::Poisoned) {
            WriterState::Idle(f) => f,
            _ => return Err(Error::WriterThreadDied),
        };
        let (tx, rx) = mpsc::sync_channel(self.channel_depth);
        let thread = thread::spawn(move || run_writer_thread(out, cfg, rx));
        self.state = WriterState::Running { tx, thread };
        Ok(())
    }

    /// Buffer `record` and `signal`; when the buffer hits `signal_batch_size`, rayon-compress
    /// all buffered signals in parallel and send them to the background writer thread.
    ///
    /// Blocks on channel send if the writer thread falls behind (backpressure).
    pub fn write_read(&mut self, record: &ReadRecord, signal: &[i16]) -> Result<()> {
        self.raw_buf.push((record.clone(), signal.to_vec()));
        if self.raw_buf.len() >= self.signal_batch_size {
            self.flush_compress_buf()?;
        }
        Ok(())
    }

    /// Parallel-compress the raw buffer, then drain it into the writer thread channel.
    fn flush_compress_buf(&mut self) -> Result<()> {
        if self.raw_buf.is_empty() {
            return Ok(());
        }
        if matches!(self.state, WriterState::Idle(_)) {
            self.start()?;
        }

        // Take ownership of the buffer; rayon compresses all signals in parallel.
        let buf = std::mem::take(&mut self.raw_buf);
        let compressed: Vec<Result<Vec<u8>>> =
            buf.par_iter().map(|(_, sig)| compress_vbz(sig)).collect();

        let tx = match &self.state {
            WriterState::Running { tx, .. } => tx.clone(),
            _ => return Err(Error::WriterThreadDied),
        };

        for ((record, signal), vbz_result) in buf.into_iter().zip(compressed) {
            let vbz = vbz_result?;
            let msg = WriterMsg {
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
                expected_open_pore_level: record.expected_open_pore_level,
                selected_read_level: record.selected_read_level,
                predicted_scaling_scale: record.predicted_scaling.scale,
                predicted_scaling_shift: record.predicted_scaling.shift,
                run_info: Arc::clone(&record.run_info),
                start_sample: record.start_sample,
                time_since_mux_change: record.time_since_mux_change,
                tracked_scaling_scale: record.tracked_scaling.scale,
                tracked_scaling_shift: record.tracked_scaling.shift,
                vbz,
                vbz_num_samples: signal.len() as u32,
            };
            tx.send(msg).map_err(|_| Error::WriterThreadDied)?;
        }
        Ok(())
    }

    /// Finalise the file: flush any buffered reads, signal EOF to the writer thread,
    /// and wait for it to finish writing the RunInfo table, Reads table, and footer.
    pub fn finish(mut self) -> Result<()> {
        self.flush_compress_buf()?;
        // Handle the empty-file case: start the thread with an empty channel.
        if matches!(self.state, WriterState::Idle(_)) {
            self.start()?;
        }
        match self.state {
            WriterState::Running { tx, thread } => {
                drop(tx); // closing the sender signals EOF to the receiver
                thread.join().map_err(|_| Error::WriterThreadDied)?
            }
            _ => Err(Error::WriterThreadDied),
        }
    }
}

// ── Writer thread ─────────────────────────────────────────────────────────────

/// Core POD5 write loop executed in the background thread.
///
/// `FileWriter` lives here as a local variable; the borrow checker is
/// satisfied because it borrows `out` for only the duration of its scope block,
/// after which `out` is free to use again.
fn run_writer_thread(
    mut out: BufWriter<File>,
    cfg: WriterConfig,
    rx: mpsc::Receiver<WriterMsg>,
) -> Result<()> {
    let ipc_meta = make_ipc_meta(&cfg);
    let section_marker = [0u8; SECTION_MARKER_LEN];

    out.write_all(&SIGNATURE)?;
    out.write_all(&section_marker)?;
    let mut pos: u64 = (8 + SECTION_MARKER_LEN) as u64;

    // ── Signal section ────────────────────────────────────────────────────────
    // FileWriter is a stack-local here, not stored in a struct.  This is what
    // lets us borrow `out` through ByteCounter without fighting the borrow checker.
    let signal_off = pos;
    let signal_len: i64;
    let mut reads: Vec<ReadMeta> = Vec::new();
    let mut run_infos: Vec<Arc<RunInfo>> = Vec::new();
    let mut ri_id_to_idx: HashMap<String, usize> = HashMap::new();

    let sig_schema = signal_schema(&ipc_meta);
    {
        let opts = IpcWriteOptions::default().with_preserve_dict_id(false);
        let mut counter = ByteCounter {
            inner: &mut out,
            n: 0,
        };
        let mut sig_writer = FileWriter::try_new_with_options(&mut counter, &sig_schema, opts)?;

        let batch_size = cfg.signal_batch_size;
        let mut batch_buf: Vec<WriterMsg> = Vec::with_capacity(batch_size);

        for msg in rx {
            batch_buf.push(msg);
            if batch_buf.len() >= batch_size {
                let batch = drain_signal_batch(
                    &mut batch_buf,
                    &sig_schema,
                    &mut run_infos,
                    &mut ri_id_to_idx,
                    &mut reads,
                )?;
                sig_writer.write(&batch)?;
            }
        }
        if !batch_buf.is_empty() {
            let batch = drain_signal_batch(
                &mut batch_buf,
                &sig_schema,
                &mut run_infos,
                &mut ri_id_to_idx,
                &mut reads,
            )?;
            sig_writer.write(&batch)?;
        }

        sig_writer.finish()?;
        drop(sig_writer); // releases borrow on counter
        signal_len = counter.n as i64;
    } // counter drops, releasing borrow on out

    pos += signal_len as u64;
    write_pad_and_marker(&mut out, &section_marker, &mut pos)?;

    // ── RunInfo section ───────────────────────────────────────────────────────
    let runinfo_ipc = build_run_info_ipc(&run_infos, ipc_meta.clone())?;
    let runinfo_off = pos;
    let runinfo_len = runinfo_ipc.len() as i64;
    write_section(&mut out, &runinfo_ipc, &section_marker, &mut pos)?;

    // ── Reads section ─────────────────────────────────────────────────────────
    let reads_ipc = build_reads_ipc(&reads, &run_infos, ipc_meta)?;
    let reads_off = pos;
    let reads_len = reads_ipc.len() as i64;
    write_section(&mut out, &reads_ipc, &section_marker, &mut pos)?;

    write_footer(
        &mut out,
        &cfg,
        &section_marker,
        signal_off,
        signal_len,
        runinfo_off,
        runinfo_len,
        reads_off,
        reads_len,
    )
}

// ── Synchronous write path (used by Writer::write_to) ────────────────────────

fn write_pod5_to_sink<W: Write>(
    out: &mut W,
    writes: &[WriterRead],
    cfg: &WriterConfig,
) -> Result<()> {
    let ipc_meta = make_ipc_meta(cfg);
    let section_marker = [0u8; SECTION_MARKER_LEN];

    out.write_all(&SIGNATURE)?;
    out.write_all(&section_marker)?;
    let mut pos: u64 = (8 + SECTION_MARKER_LEN) as u64;

    // ── Signal section ────────────────────────────────────────────────────────
    let signal_off = pos;
    let signal_len: i64;
    let mut reads: Vec<ReadMeta> = Vec::with_capacity(writes.len());
    let mut run_infos: Vec<Arc<RunInfo>> = Vec::new();
    let mut ri_id_to_idx: HashMap<String, usize> = HashMap::new();

    let sig_schema = signal_schema(&ipc_meta);
    {
        let opts = IpcWriteOptions::default().with_preserve_dict_id(false);
        let mut counter = ByteCounter { inner: out, n: 0 };
        let mut sig_writer = FileWriter::try_new_with_options(&mut counter, &sig_schema, opts)?;

        // Parallel-compress all signals up front, then write Arrow batches sequentially.
        let compressed: Vec<Vec<u8>> = writes
            .par_iter()
            .map(|wr| compress_vbz(&wr.signal))
            .collect::<Result<Vec<_>>>()?;

        for (chunk, vbz_chunk) in writes
            .chunks(cfg.signal_batch_size)
            .zip(compressed.chunks(cfg.signal_batch_size))
        {
            let mut read_id_col = FixedSizeBinaryBuilder::new(16);
            let mut signal_col = LargeBinaryBuilder::new();
            let mut samples_col = UInt32Builder::new();
            for (wr, vbz) in chunk.iter().zip(vbz_chunk) {
                let ri_idx = dedup_run_info(&wr.record.run_info, &mut run_infos, &mut ri_id_to_idx);
                read_id_col.append_value(wr.record.read_id)?;
                signal_col.append_value(vbz);
                samples_col.append_value(wr.signal.len() as u32);
                reads.push(read_meta_from_record(&wr.record, ri_idx));
            }
            let batch = RecordBatch::try_new(
                sig_schema.clone(),
                vec![
                    Arc::new(read_id_col.finish()),
                    Arc::new(signal_col.finish()),
                    Arc::new(samples_col.finish()),
                ],
            )?;
            sig_writer.write(&batch)?;
        }

        sig_writer.finish()?;
        drop(sig_writer);
        signal_len = counter.n as i64;
    }

    pos += signal_len as u64;
    write_pad_and_marker(out, &section_marker, &mut pos)?;

    // ── RunInfo section ───────────────────────────────────────────────────────
    let runinfo_ipc = build_run_info_ipc(&run_infos, ipc_meta.clone())?;
    let runinfo_off = pos;
    let runinfo_len = runinfo_ipc.len() as i64;
    write_section(out, &runinfo_ipc, &section_marker, &mut pos)?;

    // ── Reads section ─────────────────────────────────────────────────────────
    let reads_ipc = build_reads_ipc(&reads, &run_infos, ipc_meta)?;
    let reads_off = pos;
    let reads_len = reads_ipc.len() as i64;
    write_section(out, &reads_ipc, &section_marker, &mut pos)?;

    write_footer(
        out,
        cfg,
        &section_marker,
        signal_off,
        signal_len,
        runinfo_off,
        runinfo_len,
        reads_off,
        reads_len,
    )
}

// ── Shared write helpers ──────────────────────────────────────────────────────

fn default_software() -> String {
    format!(
        "pod5lib-rs {} (pod5 {POD5_COMPAT_VERSION})",
        env!("CARGO_PKG_VERSION")
    )
}

/// Parse `file_identifier` as a UUID and return its canonical hyphenated form,
/// which upstream `pod5` requires.
fn canonical_file_identifier(id: &str) -> Result<String> {
    Uuid::try_parse(id)
        .map(|u| u.hyphenated().to_string())
        .map_err(|_| Error::InvalidFileIdentifier(id.to_string()))
}

/// Reject `pod5_version` values that upstream `pod5` would refuse to open
/// (not `X.Y.Z` of u16 parts) or would wrongly migrate (below 0.0.38).
fn validate_pod5_version(v: &str) -> Result<()> {
    match parse_pod5_version(v) {
        Some(ver) if ver >= MIN_POD5_VERSION => Ok(()),
        _ => Err(Error::InvalidPod5Version(v.to_string())),
    }
}

fn make_ipc_meta(cfg: &WriterConfig) -> HashMap<String, String> {
    HashMap::from([
        (
            "MINKNOW:file_identifier".to_string(),
            cfg.file_identifier.clone(),
        ),
        ("MINKNOW:software".to_string(), cfg.software.clone()),
        ("MINKNOW:pod5_version".to_string(), cfg.pod5_version.clone()),
    ])
}

fn signal_schema(meta: &HashMap<String, String>) -> Arc<Schema> {
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
    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("read_id", DataType::FixedSizeBinary(16), false).with_metadata(uuid_meta),
            Field::new("signal", DataType::LargeBinary, false).with_metadata(vbz_meta),
            Field::new("samples", DataType::UInt32, false),
        ],
        meta.clone(),
    ))
}

fn build_signal_batch_multi(msgs: &[WriterMsg], schema: &Arc<Schema>) -> Result<RecordBatch> {
    let mut read_id_col = FixedSizeBinaryBuilder::new(16);
    let mut signal_col = LargeBinaryBuilder::new();
    let mut samples_col = UInt32Builder::new();
    for msg in msgs {
        read_id_col.append_value(msg.read_id)?;
        signal_col.append_value(&msg.vbz);
        samples_col.append_value(msg.vbz_num_samples);
    }
    Ok(RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(read_id_col.finish()),
            Arc::new(signal_col.finish()),
            Arc::new(samples_col.finish()),
        ],
    )?)
}

/// Build a signal batch from `buf`, write metadata to `reads`, then drain `buf`.
fn drain_signal_batch(
    buf: &mut Vec<WriterMsg>,
    schema: &Arc<Schema>,
    run_infos: &mut Vec<Arc<RunInfo>>,
    ri_id_to_idx: &mut HashMap<String, usize>,
    reads: &mut Vec<ReadMeta>,
) -> Result<RecordBatch> {
    let batch = build_signal_batch_multi(buf, schema)?;
    for msg in buf.drain(..) {
        let ri_idx = dedup_run_info(&msg.run_info, run_infos, ri_id_to_idx);
        reads.push(ReadMeta {
            read_id: msg.read_id,
            read_number: msg.read_number,
            channel: msg.channel,
            well: msg.well,
            pore_type: msg.pore_type,
            calib_offset: msg.calib_offset,
            calib_scale: msg.calib_scale,
            median_before: msg.median_before,
            end_reason: msg.end_reason,
            end_reason_forced: msg.end_reason_forced,
            num_minknow_events: msg.num_minknow_events,
            num_reads_since_mux_change: msg.num_reads_since_mux_change,
            num_samples: msg.num_samples,
            open_pore_level: msg.open_pore_level,
            expected_open_pore_level: msg.expected_open_pore_level,
            selected_read_level: msg.selected_read_level,
            predicted_scaling_scale: msg.predicted_scaling_scale,
            predicted_scaling_shift: msg.predicted_scaling_shift,
            ri_idx,
            start_sample: msg.start_sample,
            time_since_mux_change: msg.time_since_mux_change,
            tracked_scaling_scale: msg.tracked_scaling_scale,
            tracked_scaling_shift: msg.tracked_scaling_shift,
        });
    }
    Ok(batch)
}

/// Add `run_info` to `run_infos` if not yet seen; return its index.
fn dedup_run_info(
    run_info: &Arc<RunInfo>,
    run_infos: &mut Vec<Arc<RunInfo>>,
    ri_id_to_idx: &mut HashMap<String, usize>,
) -> usize {
    if let Some(&idx) = ri_id_to_idx.get(run_info.acquisition_id.as_str()) {
        return idx;
    }
    let idx = run_infos.len();
    ri_id_to_idx.insert(run_info.acquisition_id.clone(), idx);
    run_infos.push(Arc::clone(run_info));
    idx
}

/// Write `ipc` bytes, pad to 8-byte alignment, then write `marker`.
fn write_section<W: Write>(
    out: &mut W,
    ipc: &[u8],
    marker: &[u8; SECTION_MARKER_LEN],
    pos: &mut u64,
) -> Result<()> {
    out.write_all(ipc)?;
    *pos += ipc.len() as u64;
    write_pad_and_marker(out, marker, pos)
}

/// Pad `*pos` to 8-byte alignment then write `marker`.
fn write_pad_and_marker<W: Write>(
    out: &mut W,
    marker: &[u8; SECTION_MARKER_LEN],
    pos: &mut u64,
) -> Result<()> {
    let pad = (align8(*pos) - *pos) as usize;
    if pad > 0 {
        out.write_all(&vec![0u8; pad])?;
        *pos += pad as u64;
    }
    out.write_all(marker)?;
    *pos += SECTION_MARKER_LEN as u64;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn write_footer<W: Write>(
    out: &mut W,
    cfg: &WriterConfig,
    section_marker: &[u8; SECTION_MARKER_LEN],
    signal_off: u64,
    signal_len: i64,
    runinfo_off: u64,
    runinfo_len: i64,
    reads_off: u64,
    reads_len: i64,
) -> Result<()> {
    let contents = [
        (runinfo_off as i64, runinfo_len, 4i16),
        (signal_off as i64, signal_len, 1i16),
        (reads_off as i64, reads_len, 0i16),
    ];
    let footer_fb = footer::build_footer(
        &cfg.file_identifier,
        &cfg.software,
        &cfg.pod5_version,
        &contents,
    );
    let footer_len = footer_fb.len() as i64;
    let footer_pad = (align8(footer_len as u64) - footer_len as u64) as usize;

    out.write_all(&FOOTER_MAGIC)?;
    out.write_all(&footer_fb)?;
    if footer_pad > 0 {
        out.write_all(&vec![0u8; footer_pad])?;
    }
    out.write_all(&footer_len.to_le_bytes())?;
    out.write_all(section_marker)?;
    out.write_all(&SIGNATURE)?;
    Ok(())
}

// ── Byte-counting Write wrapper ───────────────────────────────────────────────

struct ByteCounter<'a, W: Write> {
    inner: &'a mut W,
    n: u64,
}

impl<W: Write> Write for ByteCounter<'_, W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let k = self.inner.write(buf)?;
        self.n += k as u64;
        Ok(k)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
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
    reads: &[ReadMeta],
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
            // Reads table V5 fields
            Field::new("expected_open_pore_level", DataType::Float32, false),
            Field::new("selected_read_level", DataType::Float32, false),
            // Reads table V7: `channel` holds 0 for channels that do not fit in 16 bits
            Field::new("channel_32bit", DataType::UInt32, false),
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
    let mut expected_open_pore_col = Float32Builder::new();
    let mut selected_read_level_col = Float32Builder::new();
    let mut channel_32bit_col = UInt32Builder::new();
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

    for (signal_row_idx, rm) in reads.iter().enumerate() {
        read_id_col.append_value(rm.read_id)?;
        read_number_col.append_value(rm.read_number);
        channel_col.append_value(u16::try_from(rm.channel).unwrap_or(0));
        channel_32bit_col.append_value(rm.channel);
        well_col.append_value(rm.well);
        pore_type_vals.push(&rm.pore_type);
        calib_offset_col.append_value(rm.calib_offset);
        calib_scale_col.append_value(rm.calib_scale);
        median_before_col.append_value(rm.median_before);
        end_reason_vals.push(rm.end_reason);
        end_reason_forced_col.append_value(rm.end_reason_forced);
        num_events_col.append_value(rm.num_minknow_events);
        num_reads_mux_col.append_value(rm.num_reads_since_mux_change);
        num_samples_col.append_value(rm.num_samples);
        open_pore_col.append_value(rm.open_pore_level);
        expected_open_pore_col.append_value(rm.expected_open_pore_level);
        selected_read_level_col.append_value(rm.selected_read_level);
        pr_scale_col.append_value(rm.predicted_scaling_scale);
        pr_shift_col.append_value(rm.predicted_scaling_shift);
        run_info_keys.push(rm.ri_idx as i16);
        start_col.append_value(rm.start_sample);
        time_mux_col.append_value(rm.time_since_mux_change);
        tr_scale_col.append_value(rm.tracked_scaling_scale);
        tr_shift_col.append_value(rm.tracked_scaling_shift);

        signal_col.values().append_value(signal_row_idx as u64);
        signal_col.append(true);
    }

    let pore_type_col = build_str_dict(&pore_type_vals);
    let end_reason_col = build_str_dict(&end_reason_vals);

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
            Arc::new(expected_open_pore_col.finish()),
            Arc::new(selected_read_level_col.finish()),
            Arc::new(channel_32bit_col.finish()),
        ],
    )?;

    write_ipc(&schema, &[batch])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_pod5_version_is_valid() {
        validate_pod5_version(POD5_COMPAT_VERSION).unwrap();
    }

    #[test]
    fn default_file_identifier_is_random_v4() {
        let a = Writer::new().file_identifier;
        let b = Writer::new().file_identifier;
        assert_ne!(a, b);
        assert_eq!(Uuid::parse_str(&a).unwrap().get_version_num(), 4);
        assert_eq!(canonical_file_identifier(&a).unwrap(), a);
    }

    #[test]
    fn file_identifier_canonicalised() {
        assert_eq!(
            canonical_file_identifier("{A1B2C3D4-0506-0708-090A-0B0C0D0E0F10}").unwrap(),
            "a1b2c3d4-0506-0708-090a-0b0c0d0e0f10"
        );
        assert_eq!(
            canonical_file_identifier("a1b2c3d405060708090a0b0c0d0e0f10").unwrap(),
            "a1b2c3d4-0506-0708-090a-0b0c0d0e0f10"
        );
        for bad in ["", "not-a-uuid", "a1b2c3d4-0506-0708-090a-0b0c0d0e0f1"] {
            assert!(
                matches!(
                    canonical_file_identifier(bad),
                    Err(Error::InvalidFileIdentifier(_))
                ),
                "{bad}"
            );
        }
    }

    #[test]
    fn pod5_version_validation() {
        for ok in ["0.0.38", "0.3.49", "1.0.0", "65535.0.0"] {
            assert!(validate_pod5_version(ok).is_ok(), "{ok}");
        }
        for bad in [
            "",
            "0.1",
            "1.0.0.0",
            "0.0.35",
            "0.2.0-rc1",
            "65536.0.0",
            "1..0",
            "+1.0.0",
            " 1.0.0",
        ] {
            assert!(
                matches!(
                    validate_pod5_version(bad),
                    Err(Error::InvalidPod5Version(_))
                ),
                "{bad}"
            );
        }
    }
}
