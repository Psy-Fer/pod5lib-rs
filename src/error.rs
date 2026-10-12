/// All errors that can be produced by this library.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Underlying I/O error (file open, read, seek).
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// The 8-byte opening or closing signature does not match the expected POD5 magic bytes.
    #[error("invalid POD5 signature")]
    InvalidSignature,

    /// Two section markers in the file differ; the file is likely truncated or corrupt.
    #[error("section markers are not consistent")]
    InconsistentSectionMarker,

    /// The 8-byte `"FOOTER\0\0"` magic was not found at the expected position.
    #[error("footer magic 'FOOTER\\0\\0' not found")]
    FooterMagicNotFound,

    /// The FlatBuffers footer could not be parsed.
    #[error("FlatBuffers parse error: {0}")]
    FlatBuffers(String),

    /// An Arrow IPC operation failed (schema mismatch, truncated batch, etc.).
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// A required Arrow IPC section (Reads, Signal, or RunInfo) was absent from the footer.
    #[error("required table missing from footer: {0}")]
    MissingTable(&'static str),

    /// A column expected by the parser was not present in the Arrow batch.
    #[error("column '{0}' not found in batch")]
    MissingColumn(String),

    /// A column had a different Arrow type than the parser expected.
    #[error("unexpected Arrow type for column '{0}'")]
    TypeMismatch(String),

    /// VBZ decompression of a signal chunk failed.
    #[error("VBZ decompression error: {0}")]
    Vbz(String),

    /// A string column contained invalid UTF-8.
    #[error("invalid UTF-8: {0}")]
    Utf8(#[from] std::str::Utf8Error),

    /// A signal row index stored in the Reads table points beyond the end of the Signal table.
    #[error("signal row index {0} out of range (table has {1} rows)")]
    SignalRowOutOfRange(u64, usize),

    /// The `MINKNOW:file_identifier` in an Arrow table's schema metadata does not match
    /// the value recorded in the FlatBuffers footer.
    #[error("file_identifier mismatch: {0} table schema has '{1}', footer has '{2}'")]
    FileIdentifierMismatch(String, String, String),

    /// The `run_info` dictionary column references an `acquisition_id` not present in the
    /// RunInfo table.
    #[error("run_info column references unknown acquisition_id: '{0}'")]
    MissingRunInfo(String),

    /// The writer's `pod5_version` is not a version upstream `pod5` accepts: it must be
    /// `X.Y.Z` with each part ≤ 65535, and at least `0.0.38` (older values trigger an
    /// on-open migration of legacy formats).
    #[error("invalid pod5_version '{0}': expected X.Y.Z, at least 0.0.38")]
    InvalidPod5Version(String),

    /// The writer's `file_identifier` is not a UUID; upstream `pod5` refuses to open such files.
    #[error("invalid file_identifier '{0}': expected a UUID")]
    InvalidFileIdentifier(String),

    /// The background writer thread exited before the file was complete.
    #[error("writer thread exited unexpectedly")]
    WriterThreadDied,
}
