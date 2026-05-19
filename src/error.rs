/// All errors that can be produced by this library.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid POD5 signature")]
    InvalidSignature,

    #[error("section markers are not consistent")]
    InconsistentSectionMarker,

    #[error("footer magic 'FOOTER\\0\\0' not found")]
    FooterMagicNotFound,

    #[error("FlatBuffers parse error: {0}")]
    FlatBuffers(String),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("required table missing from footer: {0}")]
    MissingTable(&'static str),

    #[error("column '{0}' not found in batch")]
    MissingColumn(String),

    #[error("unexpected Arrow type for column '{0}'")]
    TypeMismatch(String),

    #[error("VBZ decompression error: {0}")]
    Vbz(String),

    #[error("invalid UTF-8: {0}")]
    Utf8(#[from] std::str::Utf8Error),

    #[error("signal row index {0} out of range (table has {1} rows)")]
    SignalRowOutOfRange(u64, usize),
}
