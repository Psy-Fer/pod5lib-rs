/// High-level POD5 file reader — public entry point.
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
/// Reads and tables are loaded eagerly on `open`; signal data is decompressed
/// lazily when [`ReadRecord::signal`] / [`ReadRecord::signal_pa`] is called.
///
/// ```no_run
/// use pod5::Reader;
///
/// let reader = Reader::open("sample.pod5").unwrap();
/// for read in reader.reads() {
///     println!("{} samples", read.num_samples);
/// }
/// ```
#[derive(Debug)]
pub struct Reader {
    reads: Vec<ReadRecord>,
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
        let signal_table = Arc::new(tables::parse_signal_batches(&signal_batches)?);

        let reads_batches = tables::read_ipc_section(reader, reads_off, reads_len)?;
        let reads = tables::parse_reads_batches(&reads_batches, &run_infos, signal_table)?;

        Ok(Self {
            reads,
            run_infos,
            pod5_version: footer.pod5_version,
            file_identifier: footer.file_identifier,
            software: footer.software,
        })
    }

    // ── Public accessors ──────────────────────────────────────────────────

    /// Iterate over all reads in file order.
    pub fn reads(&self) -> &[ReadRecord] {
        &self.reads
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

    /// Return an iterator that yields `ReadRecord` references by read-id UUID string.
    pub fn reads_by_id<'a>(&'a self, id: &'a str) -> impl Iterator<Item = &'a ReadRecord> + 'a {
        self.reads.iter().filter(move |r| r.read_id_str() == id)
    }
}
