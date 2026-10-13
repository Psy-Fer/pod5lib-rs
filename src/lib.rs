//! Rust library for reading and writing [POD5](https://pod5-file-format.readthedocs.io/en/latest/)
//! nanopore sequencing files, the successor to FAST5 from Oxford Nanopore Technologies.
//! Files are interchangeable with ONT's own `pod5` library (validated against
//! [`POD5_COMPAT_VERSION`]): any file written by `pod5` 0.0.38 or later can be read,
//! and files are written as `pod5` 0.3.48+ writes them.
//!
//! Files are treated as untrusted input: a corrupt or truncated file returns an
//! [`Error`], never a panic or an allocation sized by corrupt data.
//!
//! POD5 files embed three [Apache Arrow IPC](https://arrow.apache.org/docs/format/IPC.html)
//! tables (Reads, Signal, RunInfo) in a binary container with a
//! [FlatBuffers](https://flatbuffers.dev/) footer. Signal data is compressed with VBZ
//! (delta + zigzag + SVB16 + zstd).
//!
//! ## Reading
//!
//! ```no_run
//! use pod5lib::Reader;
//!
//! let reader = Reader::open("sample.pod5").unwrap();
//! println!("{} reads", reader.len());
//!
//! for read in reader.reads_iter() {
//!     let adc = read.signal().unwrap();      // raw i16 ADC samples
//!     let pa  = read.signal_pa().unwrap();   // calibrated picoamps (f32)
//!     println!("{}: {} samples", read.read_id_str(), adc.len());
//! }
//!
//! // Random access by UUID string (O(1))
//! if let Some(read) = reader.get_read_by_id("a1b2c3d4-0506-0708-090a-0b0c0d0e0f10") {
//!     let _ = read.signal_pa().unwrap();
//! }
//! ```
//!
//! ## Writing
//!
//! Use [`Writer`] for small files or when all reads are already in memory.
//! Use [`StreamingWriter`] for large files: it buffers only `signal_batch_size` reads
//! at a time, compresses them in parallel and writes them from a background thread,
//! so memory use stays roughly constant. [`StreamingWriter::finish`] must be called
//! to complete the file.
//!
//! To write data that did not come from a POD5 file, build reads with
//! [`ReadRecord::new`].
//!
//! ```no_run
//! use pod5lib::{Reader, StreamingWriter};
//!
//! let src = Reader::open("input.pod5").unwrap();
//! let mut writer = StreamingWriter::create("output.pod5").unwrap();
//!
//! for read in src.reads_iter() {
//!     let signal = read.signal().unwrap();
//!     writer.write_read(read, &signal).unwrap();
//! }
//! writer.finish().unwrap();
//! ```
#![deny(missing_docs)]
#![forbid(unsafe_code)]

mod error;
mod footer;
mod format;
mod reader;
mod signal;
mod tables;
mod types;
mod writer;

pub use error::Error;
pub use reader::{ParReadsIter, Reader};
pub use types::{
    Calibration, EndReason, EndReasonKind, Pore, ReadRecord, RunInfo, ShiftScalePair,
    uuid_from_str, uuid_to_str,
};
pub use writer::{DEFAULT_SIGNAL_BATCH_SIZE, StreamingWriter, Writer};

/// Version of the upstream Python/C++ `pod5` library this crate is validated against.
///
/// Writers record it as the file's `pod5_version` (which upstream parses to decide
/// on migrations) and in the default `software` string.
pub const POD5_COMPAT_VERSION: &str = "0.3.49";

/// Crate-wide `Result` type, equivalent to `std::result::Result<T, `[`Error`]`>`.
pub type Result<T> = std::result::Result<T, Error>;
