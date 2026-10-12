/// High-level POD5 file reader.
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::footer::{self, ContentType, Footer};
use crate::format::{
    FOOTER_LEN_FIELD_FROM_END, FOOTER_MAGIC, MIN_POD5_VERSION, SECTION_MARKER_LEN, SIGNATURE,
    align8, parse_pod5_version,
};
use crate::tables;
use crate::types::{ReadRecord, RunInfo, uuid_from_str};
use crate::{Error, Result};

// ── Reader ────────────────────────────────────────────────────────────────────

/// Open and read a POD5 file.
///
/// Read metadata (Reads + RunInfo tables) is loaded eagerly on `open`; signal data
/// is decompressed lazily when [`ReadRecord::signal`] / [`ReadRecord::signal_pa`] is
/// called, reading directly from the file.
///
/// ```no_run
/// use pod5lib::Reader;
///
/// let reader = Reader::open("sample.pod5").unwrap();
///
/// // Iterate in file order (streaming, one record at a time):
/// for read in reader.reads_iter() {
///     println!("{}: {} samples", read.read_id_str(), read.num_samples);
/// }
///
/// // Random access by UUID string (O(1)):
/// if let Some(read) = reader.get_read_by_id("a1b2c3d4-0506-0708-090a-0b0c0d0e0f10") {
///     let adc = read.signal().unwrap();
/// }
/// ```
#[derive(Debug)]
pub struct Reader {
    reads: Vec<ReadRecord>,
    /// UUID bytes → index into `reads`; built alongside `reads` for O(1) lookup.
    reads_index: HashMap<[u8; 16], usize>,
    run_infos: Vec<Arc<RunInfo>>,
    pod5_version: String,
    file_identifier: String,
    software: String,
    reads_table_version: u8,
}

// ── Private helpers ───────────────────────────────────────────────────────────

/// Partially-parsed result of reading the POD5 container structure.
/// Does NOT include the signal table; callers build that separately.
struct ParsedTables {
    footer: Footer,
    run_info_batches: Vec<arrow::record_batch::RecordBatch>,
    reads_batches: Vec<arrow::record_batch::RecordBatch>,
    reads_table_version: u8,
    signal_off: u64,
    signal_len: u64,
}

impl Reader {
    /// Parse the POD5 container from `reader`: verify signatures, locate the FlatBuffers
    /// footer, then load the RunInfo and Reads Arrow IPC sections into memory.
    /// Returns the parsed batches and the byte range of the Signal section so callers
    /// can build the signal table lazily or eagerly.
    fn parse_tables<R: Read + Seek>(reader: &mut R) -> Result<ParsedTables> {
        // ── 1. Verify opening signature ───────────────────────────────────
        let mut sig = [0u8; 8];
        reader.read_exact(&mut sig)?;
        if sig != SIGNATURE {
            return Err(Error::InvalidSignature);
        }

        // ── 2. Read section marker ────────────────────────────────────────
        let mut section_marker = [0u8; SECTION_MARKER_LEN];
        reader.read_exact(&mut section_marker)?;

        // ── 3. Locate footer from end of file ─────────────────────────────
        let file_size = reader.seek(SeekFrom::End(0))?;

        // Verify closing signature
        reader.seek(SeekFrom::Start(file_size - 8))?;
        reader.read_exact(&mut sig)?;
        if sig != SIGNATURE {
            return Err(Error::InvalidSignature);
        }

        // Verify final section marker matches
        let mut final_marker = [0u8; SECTION_MARKER_LEN];
        reader.seek(SeekFrom::Start(file_size - 8 - SECTION_MARKER_LEN as u64))?;
        reader.read_exact(&mut final_marker)?;
        if final_marker != section_marker {
            return Err(Error::InconsistentSectionMarker);
        }

        // Read footer_length (int64 LE, unpadded FlatBuffer byte count)
        reader.seek(SeekFrom::Start(file_size - FOOTER_LEN_FIELD_FROM_END))?;
        let mut len_buf = [0u8; 8];
        reader.read_exact(&mut len_buf)?;
        let footer_len = i64::from_le_bytes(len_buf) as u64;

        // Footer FlatBuffer starts at: file_size - FOOTER_LEN_FIELD_FROM_END - padded_len
        let padded = align8(footer_len);
        let footer_fb_start = file_size - FOOTER_LEN_FIELD_FROM_END - padded;

        // Verify "FOOTER\0\0" magic (8 bytes immediately before the FlatBuffer)
        let mut magic = [0u8; 8];
        reader.seek(SeekFrom::Start(footer_fb_start - 8))?;
        reader.read_exact(&mut magic)?;
        if magic != FOOTER_MAGIC {
            return Err(Error::FooterMagicNotFound);
        }

        // Read FlatBuffer footer bytes
        reader.seek(SeekFrom::Start(footer_fb_start))?;
        let mut footer_bytes = vec![0u8; footer_len as usize];
        reader.read_exact(&mut footer_bytes)?;

        // ── 4. Parse FlatBuffers footer ───────────────────────────────────
        let footer = footer::parse(&footer_bytes)?;

        // Pre-0.0.38 files use a legacy layout that upstream migrates on open.
        // Unparseable versions are let through; the table parsers decide.
        if parse_pod5_version(&footer.pod5_version).is_some_and(|v| v < MIN_POD5_VERSION) {
            return Err(Error::LegacyPod5Version(footer.pod5_version));
        }

        // ── 5. Locate required tables ─────────────────────────────────────
        let find = |ct: ContentType| -> Option<(u64, u64)> {
            footer
                .contents
                .iter()
                .find(|f| f.content_type == ct)
                .map(|f| (f.offset as u64, f.length as u64))
        };
        let (reads_off, reads_len) =
            find(ContentType::ReadsTable).ok_or(Error::MissingTable("ReadsTable"))?;
        let (signal_off, signal_len) =
            find(ContentType::SignalTable).ok_or(Error::MissingTable("SignalTable"))?;
        let (runinfo_off, runinfo_len) =
            find(ContentType::RunInfoTable).ok_or(Error::MissingTable("RunInfoTable"))?;

        // ── 6. Load RunInfo and Reads Arrow IPC sections ──────────────────
        let (_, run_info_batches) = tables::read_ipc_section(reader, runinfo_off, runinfo_len)?;
        let (reads_schema, reads_batches) = tables::read_ipc_section(reader, reads_off, reads_len)?;

        // Cross-validate MINKNOW:file_identifier between footer and RunInfo/Reads schemas.
        for (table_name, batches) in [("RunInfo", &run_info_batches), ("Reads", &reads_batches)] {
            if let Some(batch) = batches.first()
                && let Some(schema_id) = batch.schema().metadata().get("MINKNOW:file_identifier")
                && schema_id != &footer.file_identifier
            {
                return Err(Error::FileIdentifierMismatch(
                    table_name.to_string(),
                    schema_id.clone(),
                    footer.file_identifier.clone(),
                ));
            }
        }

        Ok(ParsedTables {
            footer,
            run_info_batches,
            reads_batches,
            reads_table_version: tables::reads_table_version(&reads_schema),
            signal_off,
            signal_len,
        })
    }

    /// Assemble a `Reader` from parsed table batches and a pre-built signal table.
    fn finish(pt: ParsedTables, signal_table: Arc<crate::signal::SignalTable>) -> Result<Self> {
        let run_infos = tables::parse_run_info_batches(&pt.run_info_batches)?;
        let reads = tables::parse_reads_batches(&pt.reads_batches, &run_infos, signal_table)?;

        let reads_index: HashMap<[u8; 16], usize> = reads
            .iter()
            .enumerate()
            .map(|(i, r)| (r.read_id, i))
            .collect();

        Ok(Self {
            reads,
            reads_index,
            run_infos,
            pod5_version: pt.footer.pod5_version,
            file_identifier: pt.footer.file_identifier,
            software: pt.footer.software,
            reads_table_version: pt.reads_table_version,
        })
    }

    /// Open a POD5 file at `path`.
    ///
    /// Loads the Reads and RunInfo tables eagerly; signal blobs are read from the
    /// file on demand when `ReadRecord::signal` / `ReadRecord::signal_pa` is called.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let mut file = File::open(path)?;
        let pt = Self::parse_tables(&mut file)?;
        let (signal_off, signal_len) = (pt.signal_off, pt.signal_len);
        let file = Arc::new(Mutex::new(file));
        let signal_table = Arc::new(tables::parse_signal_section_lazy(
            Arc::clone(&file),
            signal_off,
            signal_len,
        )?);
        Self::finish(pt, signal_table)
    }

    /// Build a `Reader` from any `Read + Seek` source (useful for testing).
    ///
    /// Loads all three tables (Reads, RunInfo, Signal) eagerly into memory.
    pub fn from_reader<R: Read + Seek>(reader: &mut R) -> Result<Self> {
        let pt = Self::parse_tables(reader)?;
        let (_, signal_batches) = tables::read_ipc_section(reader, pt.signal_off, pt.signal_len)?;

        // Cross-validate Signal table file_identifier for the eager path.
        if let Some(batch) = signal_batches.first()
            && let Some(schema_id) = batch.schema().metadata().get("MINKNOW:file_identifier")
            && schema_id != &pt.footer.file_identifier
        {
            return Err(Error::FileIdentifierMismatch(
                "Signal".to_string(),
                schema_id.clone(),
                pt.footer.file_identifier.clone(),
            ));
        }

        let signal_table = Arc::new(tables::parse_signal_batches(signal_batches)?);
        Self::finish(pt, signal_table)
    }

    // ── Public accessors ──────────────────────────────────────────────────

    /// All reads as a slice (eagerly loaded).
    ///
    /// Prefer [`reads_iter`](Self::reads_iter) when processing reads one at a time
    /// or [`get_read_by_id`](Self::get_read_by_id) for single-record lookup.
    pub fn reads(&self) -> &[ReadRecord] {
        &self.reads
    }

    /// Iterate over reads in file order.
    ///
    /// Yields one `&ReadRecord` at a time without allocating a new collection.
    /// Use [`par_reads_iter`](Self::par_reads_iter) to decompress signal in parallel.
    pub fn reads_iter(&self) -> impl Iterator<Item = &ReadRecord> {
        self.reads.iter()
    }

    /// Parallel variant of [`reads_iter`](Self::reads_iter): decompresses signal for
    /// batches of reads concurrently, using `threads` worker threads.
    ///
    /// Reads are yielded in file order. For file-backed (lazy) signal tables, I/O is
    /// still sequential (all blobs in a batch are read under a single lock acquisition),
    /// and only the decompression step runs in parallel. For in-memory (eager) tables,
    /// decompression is fully parallel.
    ///
    /// The internal batch size is `max(threads * 4, 16)`, bounding peak RSS to roughly
    /// `batch_size × avg_signal_bytes` beyond the baseline metadata footprint.
    ///
    /// `threads = 0` lets rayon choose the thread count (typically `nproc`).
    pub fn par_reads_iter(&self, threads: usize) -> ParReadsIter<'_> {
        let n = if threads == 0 {
            rayon::current_num_threads()
        } else {
            threads
        };
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(n)
            .build()
            .expect("failed to build rayon thread pool");
        let batch_size = (n * 4).max(16);
        ParReadsIter {
            reads: &self.reads,
            pool: Arc::new(pool),
            batch_size,
            next_fetch: 0,
            batch_start: 0,
            batch_signals: Vec::new(),
            batch_pos: 0,
        }
    }

    /// Look up a single read by its UUID string (e.g. `"a1b2c3d4-0506-0708-090a-0b0c0d0e0f10"`).
    ///
    /// Returns `None` if no read with that ID exists. O(1) via an internal HashMap index.
    pub fn get_read_by_id(&self, id: &str) -> Option<&ReadRecord> {
        let bytes = uuid_from_str(id)?;
        self.reads_index.get(&bytes).map(|&i| &self.reads[i])
    }

    /// Return an iterator of reads matching `id` (0 or 1 items since UUIDs are unique).
    ///
    /// Uses the internal HashMap index for O(1) lookup.
    pub fn reads_by_id<'a>(&'a self, id: &'a str) -> impl Iterator<Item = &'a ReadRecord> + 'a {
        self.get_read_by_id(id).into_iter()
    }

    /// All run-info entries in this file (typically one per acquisition).
    pub fn run_infos(&self) -> &[Arc<RunInfo>] {
        &self.run_infos
    }

    /// Number of reads in the file.
    pub fn len(&self) -> usize {
        self.reads.len()
    }

    /// Returns `true` if the file contains no reads.
    pub fn is_empty(&self) -> bool {
        self.reads.is_empty()
    }

    /// POD5 specification version stored in the file footer (e.g. `"1.0.0"`).
    pub fn pod5_version(&self) -> &str {
        &self.pod5_version
    }

    /// File-level UUID identifier (matches across all three Arrow tables).
    pub fn file_identifier(&self) -> &str {
        &self.file_identifier
    }

    /// Software string recorded in the footer when the file was created.
    pub fn software(&self) -> &str {
        &self.software
    }

    /// Reads-table schema version actually stored in the file (3–7), detected from
    /// its columns. Equivalent to Python `pod5.Reader.physical_read_table_version`.
    ///
    /// Reads are always presented with the latest fields regardless of this value;
    /// fields missing from older tables are NaN.
    pub fn reads_table_version(&self) -> u8 {
        self.reads_table_version
    }
}

// ── ParReadsIter ──────────────────────────────────────────────────────────────

/// Parallel reads iterator returned by [`Reader::par_reads_iter`].
///
/// Yields `Result<(&ReadRecord, Vec<i16>)>` in file order, decompressing signal
/// for a batch of reads concurrently before surfacing individual results.
pub struct ParReadsIter<'a> {
    reads: &'a [ReadRecord],
    pool: Arc<rayon::ThreadPool>,
    batch_size: usize,
    /// Next global read index to start an I/O batch from.
    next_fetch: usize,
    /// Global index of the first read in `batch_signals`.
    batch_start: usize,
    batch_signals: Vec<Vec<i16>>,
    batch_pos: usize,
}

impl<'a> Iterator for ParReadsIter<'a> {
    type Item = Result<(&'a ReadRecord, Vec<i16>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.batch_pos >= self.batch_signals.len() {
            if self.next_fetch >= self.reads.len() {
                return None;
            }
            let end = (self.next_fetch + self.batch_size).min(self.reads.len());
            let chunk = &self.reads[self.next_fetch..end];

            let row_indices: Vec<&[u64]> = chunk
                .iter()
                .map(|r| r.signal_row_indices.as_slice())
                .collect();

            let signal_table = &*chunk[0].signal_table;
            match signal_table.batch_decompress(&row_indices, &self.pool) {
                Ok(signals) => {
                    self.batch_start = self.next_fetch;
                    self.next_fetch = end;
                    self.batch_signals = signals;
                    self.batch_pos = 0;
                }
                Err(e) => {
                    self.next_fetch = end;
                    self.batch_signals.clear();
                    self.batch_pos = 0;
                    return Some(Err(e));
                }
            }
        }

        let read = &self.reads[self.batch_start + self.batch_pos];
        let signal = std::mem::take(&mut self.batch_signals[self.batch_pos]);
        self.batch_pos += 1;
        Some(Ok((read, signal)))
    }
}
