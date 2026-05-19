//! Pure Rust library for reading (and eventually writing) POD5 nanopore sequencing files.
//!
//! ## Quick start
//!
//! ```no_run
//! use pod5::Reader;
//!
//! let reader = Reader::open("sample.pod5").unwrap();
//! println!("{} reads", reader.len());
//!
//! // Stream reads one at a time:
//! for read in reader.reads_iter() {
//!     println!("read {} — {} samples", read.read_id_str(), read.num_samples);
//!     let adc = read.signal().unwrap();
//!     let pa  = read.signal_pa().unwrap();
//! }
//!
//! // Random access by UUID string (O(1)):
//! if let Some(read) = reader.get_read_by_id("a1b2c3d4-0506-0708-090a-0b0c0d0e0f10") {
//!     let _ = read.signal_pa().unwrap();
//! }
//! ```
pub mod error;
pub mod reader;
pub mod types;

mod footer;
mod format;
mod signal;
mod tables;

pub use error::Error;
pub use reader::Reader;
pub use types::{
    Calibration, EndReason, EndReasonKind, Pore, ReadRecord, RunInfo, ShiftScalePair, uuid_to_str,
};

pub type Result<T> = std::result::Result<T, Error>;
