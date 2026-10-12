/// POD5 file writer: buffered ([`Writer`]) and streaming ([`StreamingWriter`]).
///
/// Both feed the same [`FileCore`], which lays out the file:
/// signature, Signal table (streamed in batches), RunInfo table, Reads table, footer.
use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::ops::Range;
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
use crate::types::{EndReasonKind, KNOWN_END_REASONS, ReadRecord, RunInfo};
use crate::{POD5_COMPAT_VERSION, Result};
use uuid::Uuid;

/// Maximum number of compressed reads in flight between caller and writer thread.
/// Bounds peak RSS to roughly CHANNEL_DEPTH × avg-VBZ-bytes-per-read.
const CHANNEL_DEPTH: usize = 64;

/// Default number of reads compressed together in one parallel burst, and of signal
/// rows per Arrow batch. Tune upward for throughput at the cost of RSS.
pub const DEFAULT_SIGNAL_BATCH_SIZE: usize = 64;

/// Samples per Signal-table row; longer reads span several rows. Matches upstream
/// `pod5`'s default `max_signal_chunk_size`.
const SIGNAL_CHUNK_SIZE: usize = 102_400;

// ── Compressed reads and per-read metadata ───────────────────────────────────

/// A read whose signal has been split into rows and VBZ-compressed.
struct CompressedRead {
    record: ReadRecord,
    /// (VBZ blob, sample count) per Signal-table row; empty for an empty signal.
    chunks: Vec<(Vec<u8>, u32)>,
    num_samples: u64,
}

fn compress_read(record: ReadRecord, signal: &[i16]) -> Result<CompressedRead> {
    let chunks = signal
        .chunks(SIGNAL_CHUNK_SIZE)
        .map(|c| Ok((compress_vbz(c)?, c.len() as u32)))
        .collect::<Result<Vec<_>>>()?;
    Ok(CompressedRead {
        record,
        chunks,
        num_samples: signal.len() as u64,
    })
}

/// Reads-table row: everything needed from a record, without its signal.
struct ReadMeta {
    read_id: [u8; 16],
    read_number: u32,
    channel: u32,
    well: u8,
    pore_type: String,
    calib_offset: f32,
    calib_scale: f32,
    median_before: f32,
    end_reason: EndReasonKind,
    end_reason_forced: bool,
    num_minknow_events: u64,
    num_reads_since_mux_change: u32,
    num_samples: u64,
    open_pore_level: f32,
    expected_open_pore_level: f32,
    selected_read_level: f32,
    predicted_scaling_scale: f32,
    predicted_scaling_shift: f32,
    ri_idx: i16,
    signal_rows: Range<u64>,
    start_sample: u64,
    time_since_mux_change: f32,
    tracked_scaling_scale: f32,
    tracked_scaling_shift: f32,
}

impl ReadMeta {
    fn new(r: &ReadRecord, num_samples: u64, ri_idx: i16, signal_rows: Range<u64>) -> Self {
        Self {
            read_id: r.read_id,
            read_number: r.read_number,
            channel: r.pore.channel,
            well: r.pore.well,
            pore_type: r.pore.pore_type.clone(),
            calib_offset: r.calibration.offset,
            calib_scale: r.calibration.scale,
            median_before: r.median_before,
            end_reason: r.end_reason.reason.clone(),
            end_reason_forced: r.end_reason.forced,
            num_minknow_events: r.num_minknow_events,
            num_reads_since_mux_change: r.num_reads_since_mux_change,
            num_samples,
            open_pore_level: r.open_pore_level,
            expected_open_pore_level: r.expected_open_pore_level,
            selected_read_level: r.selected_read_level,
            predicted_scaling_scale: r.predicted_scaling.scale,
            predicted_scaling_shift: r.predicted_scaling.shift,
            ri_idx,
            signal_rows,
            start_sample: r.start_sample,
            time_since_mux_change: r.time_since_mux_change,
            tracked_scaling_scale: r.tracked_scaling.scale,
            tracked_scaling_shift: r.tracked_scaling.shift,
        }
    }
}

// ── Writer configuration ──────────────────────────────────────────────────────

struct WriterConfig {
    file_identifier: String,
    software: String,
    pod5_version: String,
    signal_batch_size: usize,
}

impl WriterConfig {
    /// Validate the user-settable fields and canonicalise `file_identifier`.
    fn new(
        file_identifier: &str,
        software: &str,
        pod5_version: &str,
        signal_batch_size: usize,
    ) -> Result<Self> {
        validate_pod5_version(pod5_version)?;
        Ok(Self {
            file_identifier: canonical_file_identifier(file_identifier)?,
            software: software.to_string(),
            pod5_version: pod5_version.to_string(),
            signal_batch_size: signal_batch_size.max(1),
        })
    }
}

// ── File core (shared by both writers) ───────────────────────────────────────

/// Writes one POD5 file to `W`. Signal rows are written as reads arrive; read
/// metadata is kept until [`FileCore::finish`] writes the RunInfo and Reads tables.
struct FileCore<W: Write> {
    cfg: WriterConfig,
    ipc_meta: HashMap<String, String>,
    section_marker: [u8; SECTION_MARKER_LEN],
    signal_off: u64,
    sig_schema: Arc<Schema>,
    sig: FileWriter<ByteCounter<W>>,
    /// Signal rows not yet written as an Arrow batch.
    pending: Vec<([u8; 16], Vec<u8>, u32)>,
    next_row: u64,
    reads: Vec<ReadMeta>,
    run_infos: Vec<Arc<RunInfo>>,
    ri_id_to_idx: HashMap<String, i16>,
}

impl<W: Write> FileCore<W> {
    fn new(out: W, cfg: WriterConfig) -> Result<Self> {
        let ipc_meta = make_ipc_meta(&cfg);
        // Upstream uses a random UUID as the marker between sections.
        let section_marker = Uuid::new_v4().into_bytes();
        let mut counter = ByteCounter { inner: out, n: 0 };
        counter.write_all(&SIGNATURE)?;
        counter.write_all(&section_marker)?;
        let signal_off = counter.n;
        let sig_schema = signal_schema(&ipc_meta);
        let sig = FileWriter::try_new_with_options(counter, &sig_schema, ipc_write_options())?;
        Ok(Self {
            cfg,
            ipc_meta,
            section_marker,
            signal_off,
            sig_schema,
            sig,
            pending: Vec::new(),
            next_row: 0,
            reads: Vec::new(),
            run_infos: Vec::new(),
            ri_id_to_idx: HashMap::new(),
        })
    }

    fn add(&mut self, read: CompressedRead) -> Result<()> {
        let ri_idx = self.run_info_index(&read.record.run_info)?;
        let first_row = self.next_row;
        for (blob, samples) in read.chunks {
            self.pending.push((read.record.read_id, blob, samples));
            self.next_row += 1;
            if self.pending.len() >= self.cfg.signal_batch_size {
                self.write_pending_rows()?;
            }
        }
        self.reads.push(ReadMeta::new(
            &read.record,
            read.num_samples,
            ri_idx,
            first_row..self.next_row,
        ));
        Ok(())
    }

    /// Dictionary key of `run_info`, adding it on first sight.
    fn run_info_index(&mut self, run_info: &Arc<RunInfo>) -> Result<i16> {
        if let Some(&idx) = self.ri_id_to_idx.get(&run_info.acquisition_id) {
            return Ok(idx);
        }
        let idx = i16::try_from(self.run_infos.len())
            .map_err(|_| Error::TooManyDictionaryValues("run_info"))?;
        self.ri_id_to_idx
            .insert(run_info.acquisition_id.clone(), idx);
        self.run_infos.push(Arc::clone(run_info));
        Ok(idx)
    }

    fn write_pending_rows(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let mut read_id_col = FixedSizeBinaryBuilder::new(16);
        let mut signal_col = LargeBinaryBuilder::new();
        let mut samples_col = UInt32Builder::new();
        for (read_id, blob, samples) in self.pending.drain(..) {
            read_id_col.append_value(read_id)?;
            signal_col.append_value(blob);
            samples_col.append_value(samples);
        }
        let batch = RecordBatch::try_new(
            self.sig_schema.clone(),
            vec![
                Arc::new(read_id_col.finish()),
                Arc::new(signal_col.finish()),
                Arc::new(samples_col.finish()),
            ],
        )?;
        self.sig.write(&batch)?;
        Ok(())
    }

    /// Write the remaining sections and the footer, flush, and return the sink.
    fn finish(mut self) -> Result<W> {
        self.write_pending_rows()?;
        self.sig.finish()?;
        let mut out = self.sig.into_inner()?;
        let signal_len = out.n - self.signal_off;
        write_pad_and_marker(&mut out, &self.section_marker)?;

        let runinfo_off = out.n;
        let runinfo_ipc = build_run_info_ipc(&self.run_infos, self.ipc_meta.clone())?;
        write_section(&mut out, &runinfo_ipc, &self.section_marker)?;

        let reads_off = out.n;
        let reads_ipc = build_reads_ipc(&self.reads, &self.run_infos, self.ipc_meta)?;
        write_section(&mut out, &reads_ipc, &self.section_marker)?;

        let contents = [
            (runinfo_off as i64, runinfo_ipc.len() as i64, 4i16),
            (self.signal_off as i64, signal_len as i64, 1i16),
            (reads_off as i64, reads_ipc.len() as i64, 0i16),
        ];
        write_footer(&mut out, &self.cfg, &self.section_marker, &contents)?;
        out.flush()?;
        Ok(out.inner)
    }
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
    /// Reads compressed together per parallel burst, and signal rows per Arrow
    /// batch. Larger is faster but uses more memory. 0 is treated as 1.
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
    ///
    /// The written `num_samples` is `signal.len()`; `record.num_samples` is ignored.
    pub fn add_read(&mut self, record: ReadRecord, signal: Vec<i16>) {
        self.reads.push(WriterRead { record, signal });
    }

    /// Write to a file path, creating or truncating it.
    ///
    /// The writer's fields are validated before the file is touched.
    pub fn write_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let cfg = self.config()?;
        let out = BufWriter::new(File::create(path)?);
        self.write_with(out, cfg)?;
        Ok(())
    }

    /// Write to any `Write` sink (useful for testing with `Vec<u8>`).
    pub fn write_to<W: Write>(&self, out: &mut W) -> Result<()> {
        let cfg = self.config()?;
        self.write_with(out, cfg)?;
        Ok(())
    }

    fn config(&self) -> Result<WriterConfig> {
        WriterConfig::new(
            &self.file_identifier,
            &self.software,
            &self.pod5_version,
            self.signal_batch_size,
        )
    }

    fn write_with<W: Write>(&self, out: W, cfg: WriterConfig) -> Result<W> {
        let burst = cfg.signal_batch_size;
        let mut core = FileCore::new(out, cfg)?;
        for burst_reads in self.reads.chunks(burst) {
            let compressed = burst_reads
                .par_iter()
                .map(|wr| compress_read(wr.record.clone(), &wr.signal))
                .collect::<Result<Vec<_>>>()?;
            for read in compressed {
                core.add(read)?;
            }
        }
        core.finish()
    }
}

impl Default for Writer {
    fn default() -> Self {
        Self::new()
    }
}

// ── StreamingWriter ───────────────────────────────────────────────────────────

/// Message from the caller to the writer thread.
enum WriterMsg {
    Read(Box<CompressedRead>),
    /// Write the remaining sections and footer. If the channel closes without this
    /// (the `StreamingWriter` was dropped), the thread abandons the file unfinished.
    Finish,
}

enum WriterState {
    Idle(BufWriter<File>),
    Running {
        tx: mpsc::SyncSender<WriterMsg>,
        thread: thread::JoinHandle<Result<()>>,
    },
    /// Finished, failed, or transient during a state change.
    Done,
}

/// Memory-efficient, threaded POD5 writer.
///
/// Reads are buffered in groups of `signal_batch_size`, VBZ-compressed in parallel on
/// the rayon pool, and handed to a background thread that writes them to disk. A
/// bounded channel applies backpressure, so peak memory stays roughly constant
/// regardless of file size.
///
/// [`finish`](Self::finish) must be called to complete the file. A writer dropped
/// without it (for example when an error is returned early with `?`) abandons the
/// file without a footer, so it cannot be mistaken for a complete file.
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
    /// Reads compressed together per parallel burst, and signal rows per Arrow
    /// batch. Larger is faster but uses more memory. 0 is treated as 1.
    pub signal_batch_size: usize,
    channel_depth: usize,
    raw_buf: Vec<(ReadRecord, Vec<i16>)>,
    state: WriterState,
}

impl fmt::Debug for StreamingWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = match self.state {
            WriterState::Idle(_) => "idle",
            WriterState::Running { .. } => "running",
            WriterState::Done => "done",
        };
        f.debug_struct("StreamingWriter")
            .field("file_identifier", &self.file_identifier)
            .field("software", &self.software)
            .field("pod5_version", &self.pod5_version)
            .field("signal_batch_size", &self.signal_batch_size)
            .field("buffered_reads", &self.raw_buf.len())
            .field("state", &state)
            .finish()
    }
}

impl StreamingWriter {
    /// Create (or truncate) `path` and return a new `StreamingWriter`.
    ///
    /// `file_identifier`, `software` and `pod5_version` are pre-filled; change them
    /// before the first call to [`Self::write_read`]. They are validated when the
    /// background writer thread starts: at the first batch flush, or in
    /// [`Self::finish`] for small files.
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
        let cfg = WriterConfig::new(
            &self.file_identifier,
            &self.software,
            &self.pod5_version,
            self.signal_batch_size,
        )?;
        let out = match std::mem::replace(&mut self.state, WriterState::Done) {
            WriterState::Idle(f) => f,
            _ => return Err(Error::WriterThreadDied),
        };
        let (tx, rx) = mpsc::sync_channel(self.channel_depth);
        let thread = thread::spawn(move || run_writer_thread(out, cfg, rx));
        self.state = WriterState::Running { tx, thread };
        Ok(())
    }

    /// Buffer `record` and `signal`; when the buffer reaches `signal_batch_size`,
    /// compress all buffered signals in parallel and send them to the writer thread.
    ///
    /// The written `num_samples` is `signal.len()`; `record.num_samples` is ignored.
    /// Blocks if the writer thread falls behind (backpressure).
    pub fn write_read(&mut self, record: &ReadRecord, signal: &[i16]) -> Result<()> {
        self.raw_buf.push((record.clone(), signal.to_vec()));
        if self.raw_buf.len() >= self.signal_batch_size.max(1) {
            self.flush_compress_buf()?;
        }
        Ok(())
    }

    /// Parallel-compress the raw buffer, then hand it to the writer thread.
    fn flush_compress_buf(&mut self) -> Result<()> {
        if self.raw_buf.is_empty() {
            return Ok(());
        }
        if matches!(self.state, WriterState::Idle(_)) {
            self.start()?;
        }
        let compressed = std::mem::take(&mut self.raw_buf)
            .into_par_iter()
            .map(|(record, signal)| compress_read(record, &signal))
            .collect::<Result<Vec<_>>>()?;
        for read in compressed {
            self.send(WriterMsg::Read(Box::new(read)))?;
        }
        Ok(())
    }

    /// Send to the writer thread; if it has stopped, return the error it stopped with.
    fn send(&mut self, msg: WriterMsg) -> Result<()> {
        let sent = match &self.state {
            WriterState::Running { tx, .. } => tx.send(msg).is_ok(),
            _ => false,
        };
        if sent {
            Ok(())
        } else {
            Err(self.join_thread())
        }
    }

    /// Join a writer thread that stopped early and return why it stopped.
    fn join_thread(&mut self) -> Error {
        match std::mem::replace(&mut self.state, WriterState::Done) {
            WriterState::Running { tx, thread } => {
                drop(tx);
                match thread.join() {
                    Ok(Err(e)) => e,
                    _ => Error::WriterThreadDied,
                }
            }
            _ => Error::WriterThreadDied,
        }
    }

    /// Finalise the file: flush buffered reads, then wait for the writer thread to
    /// write the RunInfo table, Reads table and footer.
    pub fn finish(mut self) -> Result<()> {
        self.flush_compress_buf()?;
        if matches!(self.state, WriterState::Idle(_)) {
            self.start()?; // empty file
        }
        self.send(WriterMsg::Finish)?;
        match std::mem::replace(&mut self.state, WriterState::Done) {
            WriterState::Running { tx, thread } => {
                drop(tx);
                thread.join().map_err(|_| Error::WriterThreadDied)?
            }
            _ => Err(Error::WriterThreadDied),
        }
    }
}

impl Drop for StreamingWriter {
    /// Abandon an unfinished file: the writer thread stops without writing the
    /// footer, so the output is not a valid POD5 file.
    fn drop(&mut self) {
        if let WriterState::Running { tx, thread } =
            std::mem::replace(&mut self.state, WriterState::Done)
        {
            drop(tx);
            let _ = thread.join();
        }
    }
}

/// Writer-thread loop: add reads until `Finish` (complete the file) or until the
/// channel closes without it (abandon the file).
fn run_writer_thread(
    out: BufWriter<File>,
    cfg: WriterConfig,
    rx: mpsc::Receiver<WriterMsg>,
) -> Result<()> {
    let mut core = FileCore::new(out, cfg)?;
    loop {
        match rx.recv() {
            Ok(WriterMsg::Read(read)) => core.add(*read)?,
            Ok(WriterMsg::Finish) => {
                core.finish()?;
                return Ok(());
            }
            Err(mpsc::RecvError) => return Ok(()),
        }
    }
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

/// preserve_dict_id=false: the writer assigns unique IDs from schema position rather
/// than reusing the dict_id=0 default from DictionaryArray::try_new, which would
/// cause a "dictionary replacement" error when multiple dict columns coexist.
fn ipc_write_options() -> IpcWriteOptions {
    IpcWriteOptions::default().with_preserve_dict_id(false)
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

/// Write `ipc` bytes, pad to 8-byte alignment, then write `marker`.
fn write_section<W: Write>(
    out: &mut ByteCounter<W>,
    ipc: &[u8],
    marker: &[u8; SECTION_MARKER_LEN],
) -> Result<()> {
    out.write_all(ipc)?;
    write_pad_and_marker(out, marker)
}

/// Pad to 8-byte alignment then write `marker`.
fn write_pad_and_marker<W: Write>(
    out: &mut ByteCounter<W>,
    marker: &[u8; SECTION_MARKER_LEN],
) -> Result<()> {
    let pad = (align8(out.n) - out.n) as usize;
    out.write_all(&[0u8; 8][..pad])?;
    out.write_all(marker)?;
    Ok(())
}

/// `contents`: `(offset, length, content_type)` for each embedded table.
fn write_footer<W: Write>(
    out: &mut W,
    cfg: &WriterConfig,
    section_marker: &[u8; SECTION_MARKER_LEN],
    contents: &[(i64, i64, i16)],
) -> Result<()> {
    let footer_fb = footer::build_footer(
        &cfg.file_identifier,
        &cfg.software,
        &cfg.pod5_version,
        contents,
    );
    // build_footer pads to a multiple of 8, so no padding is needed here.
    out.write_all(&FOOTER_MAGIC)?;
    out.write_all(&footer_fb)?;
    out.write_all(&(footer_fb.len() as i64).to_le_bytes())?;
    out.write_all(section_marker)?;
    out.write_all(&SIGNATURE)?;
    Ok(())
}

// ── Byte-counting Write wrapper ───────────────────────────────────────────────

/// Tracks the absolute file position of everything written through it.
struct ByteCounter<W: Write> {
    inner: W,
    n: u64,
}

impl<W: Write> Write for ByteCounter<W> {
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
    let mut writer = FileWriter::try_new_with_options(&mut buf, schema, ipc_write_options())?;
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
/// Dictionary-encode `values`. The dictionary starts with `preset` (in order),
/// followed by other values in first-seen order.
fn build_str_dict<'a>(
    values: &[&'a str],
    preset: &[&'a str],
    column: &'static str,
) -> Result<Arc<dyn Array>> {
    let mut unique: Vec<&str> = Vec::new();
    let mut val_to_key: HashMap<&str, i16> = HashMap::new();
    let mut key_for = |v: &'a str| -> Result<i16> {
        if let Some(&k) = val_to_key.get(v) {
            return Ok(k);
        }
        let k = i16::try_from(unique.len()).map_err(|_| Error::TooManyDictionaryValues(column))?;
        val_to_key.insert(v, k);
        unique.push(v);
        Ok(k)
    };
    for &v in preset {
        key_for(v)?;
    }
    let keys = values
        .iter()
        .map(|&v| key_for(v))
        .collect::<Result<Vec<i16>>>()?;
    Ok(Arc::new(DictionaryArray::<Int16Type>::try_new(
        Int16Array::from(keys),
        Arc::new(StringArray::from(unique)),
    )?))
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

    for rm in reads {
        read_id_col.append_value(rm.read_id)?;
        read_number_col.append_value(rm.read_number);
        channel_col.append_value(u16::try_from(rm.channel).unwrap_or(0));
        channel_32bit_col.append_value(rm.channel);
        well_col.append_value(rm.well);
        pore_type_vals.push(&rm.pore_type);
        calib_offset_col.append_value(rm.calib_offset);
        calib_scale_col.append_value(rm.calib_scale);
        median_before_col.append_value(rm.median_before);
        end_reason_vals.push(rm.end_reason.as_str());
        end_reason_forced_col.append_value(rm.end_reason_forced);
        num_events_col.append_value(rm.num_minknow_events);
        num_reads_mux_col.append_value(rm.num_reads_since_mux_change);
        num_samples_col.append_value(rm.num_samples);
        open_pore_col.append_value(rm.open_pore_level);
        expected_open_pore_col.append_value(rm.expected_open_pore_level);
        selected_read_level_col.append_value(rm.selected_read_level);
        pr_scale_col.append_value(rm.predicted_scaling_scale);
        pr_shift_col.append_value(rm.predicted_scaling_shift);
        run_info_keys.push(rm.ri_idx);
        start_col.append_value(rm.start_sample);
        time_mux_col.append_value(rm.time_since_mux_change);
        tr_scale_col.append_value(rm.tracked_scaling_scale);
        tr_shift_col.append_value(rm.tracked_scaling_shift);

        signal_col.values().extend(rm.signal_rows.clone().map(Some));
        signal_col.append(true);
    }

    let pore_type_col = build_str_dict(&pore_type_vals, &[], "pore_type")?;
    // Like upstream, the end_reason dictionary always starts with every known reason
    // in enum order, so a key equals its `pod5.EndReasonEnum` value.
    let end_reason_col = build_str_dict(&end_reason_vals, &KNOWN_END_REASONS, "end_reason")?;

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
    fn signal_rows_follow_chunking() {
        use crate::types::{Calibration, EndReason, EndReasonKind, Pore};
        let run_info = Arc::new(RunInfo {
            acquisition_id: "acq".into(),
            ..Default::default()
        });
        let mk = |b: u8| {
            ReadRecord::new(
                [b; 16],
                Pore {
                    channel: 1,
                    well: 1,
                    pore_type: "p".into(),
                },
                Calibration {
                    offset: 0.0,
                    scale: 1.0,
                },
                1,
                0,
                0.0,
                EndReason::from_reason_with_default_forced(EndReasonKind::Unknown),
                Arc::clone(&run_info),
            )
        };
        let mut w = Writer::new();
        w.add_read(mk(1), vec![1; 250_000]); // rows 0, 1, 2
        w.add_read(mk(2), Vec::new()); // no rows
        w.add_read(mk(3), vec![1; SIGNAL_CHUNK_SIZE]); // row 3
        let mut buf = Vec::new();
        w.write_to(&mut buf).unwrap();
        let r = crate::Reader::from_reader(&mut std::io::Cursor::new(buf)).unwrap();
        let rows: Vec<Vec<u64>> = r
            .reads()
            .iter()
            .map(|x| x.signal_row_indices.clone())
            .collect();
        assert_eq!(rows, vec![vec![0, 1, 2], vec![], vec![3]]);
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
