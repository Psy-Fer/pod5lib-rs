/// High-level POD5 file reader — public entry point.
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

use crate::footer::{self, ContentType};
use crate::format::{
    FOOTER_LEN_FIELD_FROM_END, FOOTER_MAGIC, SECTION_MARKER_LEN, SIGNATURE, align8,
};
use crate::tables;
use crate::types::{ReadRecord, RunInfo};
use crate::{Error, Result};

// ── Reader ────────────────────────────────────────────────────────────────────

/// Open and read a POD5 file.
///
/// Read metadata is loaded eagerly on `open`; signal data is decompressed
/// lazily when [`ReadRecord::signal`] / [`ReadRecord::signal_pa`] is called.
///
/// ```no_run
/// use pod5::Reader;
///
/// let reader = Reader::open("sample.pod5").unwrap();
///
/// // Iterate in file order (streaming, one record at a time):
/// for read in reader.reads_iter() {
///     println!("{} — {} samples", read.read_id_str(), read.num_samples);
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
}

impl Reader {
    /// Open a POD5 file at `path` and load its tables.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)?;
        let mut br = BufReader::new(file);
        Self::from_reader(&mut br)
    }

    /// Build a `Reader` from any `Read + Seek` source (useful for testing).
    pub fn from_reader<R: Read + Seek>(reader: &mut R) -> Result<Self> {
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

        // ── 6. Load each Arrow IPC section ────────────────────────────────
        let run_info_batches = tables::read_ipc_section(reader, runinfo_off, runinfo_len)?;
        let run_infos = tables::parse_run_info_batches(&run_info_batches)?;

        let signal_batches = tables::read_ipc_section(reader, signal_off, signal_len)?;
        let signal_table = Arc::new(tables::parse_signal_batches(signal_batches)?);

        let reads_batches = tables::read_ipc_section(reader, reads_off, reads_len)?;
        let reads = tables::parse_reads_batches(&reads_batches, &run_infos, signal_table)?;

        let reads_index: HashMap<[u8; 16], usize> = reads
            .iter()
            .enumerate()
            .map(|(i, r)| (r.read_id, i))
            .collect();

        Ok(Self {
            reads,
            reads_index,
            run_infos,
            pod5_version: footer.pod5_version,
            file_identifier: footer.file_identifier,
            software: footer.software,
        })
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
    pub fn reads_iter(&self) -> impl Iterator<Item = &ReadRecord> {
        self.reads.iter()
    }

    /// Look up a single read by its UUID string (e.g. `"a1b2c3d4-0506-0708-090a-0b0c0d0e0f10"`).
    ///
    /// Returns `None` if no read with that ID exists. O(1) — uses an internal HashMap index.
    pub fn get_read_by_id(&self, id: &str) -> Option<&ReadRecord> {
        let bytes = parse_uuid_str(id)?;
        self.reads_index.get(&bytes).map(|&i| &self.reads[i])
    }

    /// Return an iterator of reads matching `id` (0 or 1 items since UUIDs are unique).
    ///
    /// Uses the internal HashMap index — O(1) lookup.
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
}

// ── UUID string parser ────────────────────────────────────────────────────────

/// Parse a canonical UUID string (`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`) into 16 bytes.
/// Returns `None` on any format error.
fn parse_uuid_str(s: &str) -> Option<[u8; 16]> {
    let s = s.as_bytes();
    if s.len() != 36 || s[8] != b'-' || s[13] != b'-' || s[18] != b'-' || s[23] != b'-' {
        return None;
    }
    let hex: Vec<u8> = s.iter().filter(|&&b| b != b'-').copied().collect();
    if hex.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for (i, chunk) in hex.chunks(2).enumerate() {
        let hi = from_hex(chunk[0])?;
        let lo = from_hex(chunk[1])?;
        out[i] = (hi << 4) | lo;
    }
    Some(out)
}

fn from_hex(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}
