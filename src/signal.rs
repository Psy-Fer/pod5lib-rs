/// VBZ signal decompression and the signal table (eager or lazy).
///
/// VBZ compression pipeline (encoding order):
///   int16 ADC samples
///   → delta encode (differences between adjacent samples)
///   → zigzag encode (signed i16 → unsigned u16, small absolute values → small codes)
///   → SVB16 encode (StreamVByte for 16-bit: 1-2 bytes/value, 1-bit control stream)
///   → zstd compress
///
/// On-disk blob layout:
///   [zstd-compressed SVB stream...]  (no separate length header; Arrow `samples` column carries N)
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::sync::{Arc, Mutex};

use arrow::array::{Array, Int16Array, LargeBinaryArray, LargeListArray};
use arrow::record_batch::RecordBatch;
use rayon::prelude::*;

use crate::{Error, Result};

// ── Signal table ─────────────────────────────────────────────────────────────

/// Signal store — either lazy (file-backed, reads signal on demand) or eager (in-memory).
///
/// Signal data is decompressed lazily when `samples_for_rows` is called; opening a file
/// only loads metadata (Reads + RunInfo tables).
#[derive(Debug)]
pub(crate) enum SignalTable {
    /// File-backed lazy variant: signal blobs are read from disk on demand.
    Lazy {
        file: Arc<Mutex<File>>,
        rows: Vec<LazySignalRow>,
    },
    /// In-memory eager variant: Arrow IPC batches kept alive for zero-copy borrow.
    Eager {
        /// Arrow IPC batches kept alive so we can borrow signal bytes from them.
        batches: Vec<RecordBatch>,
        /// One entry per signal row in the file (across all batches).
        index: Vec<SignalRowRef>,
    },
}

/// One row in the lazy signal table.
#[derive(Debug)]
pub(crate) enum LazySignalRow {
    /// VBZ-compressed blob stored at a specific file position.
    Compressed {
        /// Absolute byte position of the compressed blob within the .pod5 file.
        file_offset: u64,
        /// Length of the compressed blob in bytes.
        compressed_len: u32,
        /// Number of ADC samples after decompression.
        num_samples: u32,
    },
    /// Eagerly materialized uncompressed samples (legacy LargeList<Int16> format).
    Uncompressed(Box<[i16]>),
}

/// Lightweight pointer into one row of one Arrow batch (used by Eager variant).
#[derive(Debug, Clone, Copy)]
pub(crate) struct SignalRowRef {
    pub batch_idx: u32,
    pub row_in_batch: u32,
    pub num_samples: u32,
    pub compressed: bool,
}

impl SignalTable {
    /// Decompress and concatenate signal rows identified by `indices`.
    pub fn samples_for_rows(&self, indices: &[u64]) -> Result<Vec<i16>> {
        match self {
            SignalTable::Lazy { file, rows } => {
                let total: usize = indices
                    .iter()
                    .map(|&i| match rows.get(i as usize) {
                        Some(LazySignalRow::Compressed { num_samples, .. }) => {
                            *num_samples as usize
                        }
                        Some(LazySignalRow::Uncompressed(s)) => s.len(),
                        None => 0,
                    })
                    .sum();
                let mut out = Vec::with_capacity(total);

                let mut guard = file
                    .lock()
                    .map_err(|_| Error::Vbz("signal file mutex poisoned".into()))?;

                for &idx in indices {
                    let row = rows
                        .get(idx as usize)
                        .ok_or(Error::SignalRowOutOfRange(idx, rows.len()))?;

                    match row {
                        LazySignalRow::Compressed {
                            file_offset,
                            compressed_len,
                            num_samples,
                        } => {
                            guard.seek(SeekFrom::Start(*file_offset))?;
                            let mut blob = vec![0u8; *compressed_len as usize];
                            guard.read_exact(&mut blob)?;
                            let mut chunk = decompress_vbz(&blob, *num_samples as usize)?;
                            out.append(&mut chunk);
                        }
                        LazySignalRow::Uncompressed(samples) => {
                            out.extend_from_slice(samples);
                        }
                    }
                }
                Ok(out)
            }

            SignalTable::Eager { batches, index } => {
                let total: usize = indices
                    .iter()
                    .map(|&i| index.get(i as usize).map_or(0, |r| r.num_samples as usize))
                    .sum();
                let mut out = Vec::with_capacity(total);

                for &idx in indices {
                    let row = index
                        .get(idx as usize)
                        .ok_or(Error::SignalRowOutOfRange(idx, index.len()))?;

                    let batch = &batches[row.batch_idx as usize];
                    let col = batch
                        .column_by_name("signal")
                        .ok_or_else(|| Error::MissingColumn("signal".into()))?;
                    let i = row.row_in_batch as usize;

                    if row.compressed {
                        let arr = col
                            .as_any()
                            .downcast_ref::<LargeBinaryArray>()
                            .ok_or_else(|| Error::TypeMismatch("signal".into()))?;
                        // arr.value(i) is a &[u8] borrowed from Arrow's Arc-backed buffer — no copy.
                        let mut chunk = decompress_vbz(arr.value(i), row.num_samples as usize)?;
                        out.append(&mut chunk);
                    } else {
                        let list =
                            col.as_any()
                                .downcast_ref::<LargeListArray>()
                                .ok_or_else(|| {
                                    Error::TypeMismatch("signal (uncompressed list)".into())
                                })?;
                        let arr = list.value(i);
                        let int16 = arr.as_any().downcast_ref::<Int16Array>().ok_or_else(|| {
                            Error::TypeMismatch("signal (uncompressed int16)".into())
                        })?;
                        for j in 0..int16.len() {
                            out.push(int16.value(j));
                        }
                    }
                }
                Ok(out)
            }
        }
    }

    /// Decompress signal for a batch of reads in parallel using `pool`.
    ///
    /// For `Lazy` tables: all blobs are read from disk inside **one** mutex acquisition
    /// (sequential I/O), then released before parallel decompression begins.
    /// For `Eager` tables: Arrow buffers are lock-free, so decompression is fully parallel.
    ///
    /// Returns one `Vec<i16>` per element of `batch_row_indices`, in order.
    pub(crate) fn batch_decompress(
        &self,
        batch_row_indices: &[&[u64]],
        pool: &rayon::ThreadPool,
    ) -> Result<Vec<Vec<i16>>> {
        match self {
            SignalTable::Lazy { file, rows } => {
                // I/O pass: read all compressed blobs under a single lock acquisition.
                let all_blobs: Vec<Vec<RawSignalBlob>> = {
                    let mut guard = file
                        .lock()
                        .map_err(|_| Error::Vbz("signal file mutex poisoned".into()))?;
                    let mut result = Vec::with_capacity(batch_row_indices.len());
                    for indices in batch_row_indices {
                        let mut read_blobs = Vec::with_capacity(indices.len());
                        for &idx in *indices {
                            let row = rows
                                .get(idx as usize)
                                .ok_or(Error::SignalRowOutOfRange(idx, rows.len()))?;
                            match row {
                                LazySignalRow::Compressed {
                                    file_offset,
                                    compressed_len,
                                    num_samples,
                                } => {
                                    guard.seek(SeekFrom::Start(*file_offset))?;
                                    let mut blob = vec![0u8; *compressed_len as usize];
                                    guard.read_exact(&mut blob)?;
                                    read_blobs.push(RawSignalBlob::Compressed(blob, *num_samples));
                                }
                                LazySignalRow::Uncompressed(s) => {
                                    read_blobs.push(RawSignalBlob::Uncompressed(s.clone()));
                                }
                            }
                        }
                        result.push(read_blobs);
                    }
                    result
                };
                // Decompress pass: fully parallel, lock already released.
                pool.install(|| {
                    all_blobs
                        .par_iter()
                        .map(|blobs| decompress_raw_blobs(blobs))
                        .collect()
                })
            }
            SignalTable::Eager { .. } => {
                // No mutex: Arrow data is Arc-backed, safe to read from multiple threads.
                pool.install(|| {
                    batch_row_indices
                        .par_iter()
                        .map(|indices| self.samples_for_rows(indices))
                        .collect()
                })
            }
        }
    }
}

// ── Raw blob type (parallel read pipeline) ────────────────────────────────────

/// Intermediate representation used by the parallel read path: a compressed or
/// uncompressed signal blob for one signal-table row, gathered before decompression.
enum RawSignalBlob {
    Compressed(Vec<u8>, u32),
    Uncompressed(Box<[i16]>),
}

/// Decompress (or copy) a single read's raw blobs into a flat `i16` sample vector.
fn decompress_raw_blobs(blobs: &[RawSignalBlob]) -> Result<Vec<i16>> {
    let total: usize = blobs
        .iter()
        .map(|b| match b {
            RawSignalBlob::Compressed(_, n) => *n as usize,
            RawSignalBlob::Uncompressed(s) => s.len(),
        })
        .sum();
    let mut out = Vec::with_capacity(total);
    for blob in blobs {
        match blob {
            RawSignalBlob::Compressed(data, n) => {
                let mut chunk = decompress_vbz(data, *n as usize)?;
                out.append(&mut chunk);
            }
            RawSignalBlob::Uncompressed(s) => out.extend_from_slice(s),
        }
    }
    Ok(out)
}

// ── VBZ compression ───────────────────────────────────────────────────────────

/// Compress raw ADC i16 samples to a VBZ blob.
///
/// Pipeline: delta → zigzag → SVB16 → zstd
pub(crate) fn compress_vbz(samples: &[i16]) -> crate::Result<Vec<u8>> {
    if samples.is_empty() {
        return Ok(Vec::new());
    }
    let mut svb_buf = Vec::new();
    svb::encode_vbz_into(samples, &mut svb_buf);
    zstd::bulk::compress(&svb_buf, 0).map_err(|e| crate::Error::Vbz(format!("zstd compress: {e}")))
}

// ── VBZ decompression ────────────────────────────────────────────────────────

/// Decompress one VBZ blob to `num_samples` raw ADC values.
///
/// The blob is a raw zstd frame; after zstd decompression the payload is
/// SVB16 + zigzag-delta encoded i16 values.
pub(crate) fn decompress_vbz(data: &[u8], num_samples: usize) -> Result<Vec<i16>> {
    if num_samples == 0 {
        return Ok(Vec::new());
    }
    let svb_cap = num_samples / 8 + 1 + num_samples * 2;
    let svb_data =
        zstd::bulk::decompress(data, svb_cap).map_err(|e| Error::Vbz(format!("zstd: {e}")))?;
    let mut out = Vec::with_capacity(num_samples);
    svb::decode_vbz_fused_into(&svb_data, num_samples, &mut out)
        .map_err(|e| Error::Vbz(format!("fused decode: {e}")))?;
    Ok(out)
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int16Builder, LargeListBuilder, UInt32Builder};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use svb::u16::Svb16;

    // ── Helpers ───────────────────────────────────────────────────────

    /// Build an uncompressed `SignalTable` (Eager variant) from slices of i16 samples.
    /// Each slice becomes one signal row via a `LargeList<Int16>` Arrow batch.
    fn uncompressed_table(rows: &[&[i16]]) -> SignalTable {
        let item_field = Arc::new(Field::new("item", DataType::Int16, true));
        let mut list_builder = LargeListBuilder::new(Int16Builder::new());
        let mut samples_builder = UInt32Builder::new();

        let mut index = Vec::new();

        for (row_in_batch, &samples) in rows.iter().enumerate() {
            for &s in samples {
                list_builder.values().append_value(s);
            }
            list_builder.append(true);
            samples_builder.append_value(samples.len() as u32);

            index.push(SignalRowRef {
                batch_idx: 0,
                row_in_batch: row_in_batch as u32,
                num_samples: samples.len() as u32,
                compressed: false,
            });
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("signal", DataType::LargeList(item_field), false),
            Field::new("samples", DataType::UInt32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(list_builder.finish()) as ArrayRef,
                Arc::new(samples_builder.finish()) as ArrayRef,
            ],
        )
        .unwrap();

        SignalTable::Eager {
            batches: vec![batch],
            index,
        }
    }

    // ── Zigzag (via svb crate) ────────────────────────────────────────────

    #[test]
    fn zigzag_round_trip() {
        for v in [-32768i16, -1, 0, 1, 127, 32767] {
            let enc = svb::zigzag::encode::<i16>(&[v]);
            let dec = svb::zigzag::decode::<i16>(&enc);
            assert_eq!(dec[0], v, "round-trip failed for {v}");
        }
    }

    // ── SVB16 (via svb crate) — verifies ONT wire-format compatibility ────

    #[test]
    fn svb16_single_byte_values() {
        // 4 values all ≤255 → ctrl byte 0x00, then 4 data bytes
        let data: Vec<u8> = vec![0x00, 0, 1, 2, 3];
        let out = Svb16.decode(&data, 4).unwrap();
        assert_eq!(out, vec![0u16, 1, 2, 3]);
    }

    #[test]
    fn svb16_two_byte_values() {
        // 1 value that needs 2 bytes → ctrl bit=1 → ctrl byte 0x01, data = 256 LE
        let data: Vec<u8> = vec![0x01, 0x00, 0x01];
        let out = Svb16.decode(&data, 1).unwrap();
        assert_eq!(out, vec![256u16]);
    }

    #[test]
    fn svb16_mixed() {
        // val=5 (1 byte, bit=0), val=300 (2 bytes, bit=1) → ctrl=0x02
        let data: Vec<u8> = vec![0x02, 5, 0x2C, 0x01];
        let out = Svb16.decode(&data, 2).unwrap();
        assert_eq!(out, vec![5u16, 300u16]);
    }

    // ── VBZ compress/decompress ───────────────────────────────────────────

    #[test]
    fn svb16_encode_decode_round_trip() {
        let values: Vec<u16> = vec![0, 1, 255, 256, 300, 1000, 65535];
        let encoded = Svb16.encode(&values);
        let decoded = Svb16.decode(&encoded, values.len()).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn compress_vbz_empty() {
        let result = compress_vbz(&[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn compress_decompress_round_trip() {
        let samples: Vec<i16> = vec![100, 105, 98, 200, -50, 0, 32767, -32768];
        let blob = compress_vbz(&samples).unwrap();
        let recovered = decompress_vbz(&blob, samples.len()).unwrap();
        assert_eq!(recovered, samples);
    }

    #[test]
    fn compress_decompress_flat_signal() {
        let samples: Vec<i16> = vec![512; 1000];
        let blob = compress_vbz(&samples).unwrap();
        let recovered = decompress_vbz(&blob, samples.len()).unwrap();
        assert_eq!(recovered, samples);
    }

    #[test]
    fn decompress_vbz_zero_samples() {
        let result = decompress_vbz(&[], 0).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn decompress_vbz_bad_input() {
        let result = decompress_vbz(b"not-a-zstd-frame", 1);
        assert!(matches!(result, Err(crate::Error::Vbz(_))));
    }

    // ── SignalTable ───────────────────────────────────────────────────────

    #[test]
    fn signal_table_row_out_of_range() {
        let table = SignalTable::Eager {
            batches: vec![],
            index: vec![],
        };
        let err = table.samples_for_rows(&[0]).unwrap_err();
        assert!(matches!(err, crate::Error::SignalRowOutOfRange(0, 0)));
    }

    #[test]
    fn signal_table_uncompressed_path() {
        let table = uncompressed_table(&[&[1i16, 5i16]]);
        let result = table.samples_for_rows(&[0]).unwrap();
        assert_eq!(result, vec![1i16, 5i16]);
    }

    #[test]
    fn signal_table_multi_row_concat() {
        // Two rows: [1, 2] and [3]
        let table = uncompressed_table(&[&[1i16, 2i16], &[3i16]]);
        // indices [1, 0] → row 1 then row 0
        let result = table.samples_for_rows(&[1, 0]).unwrap();
        assert_eq!(result, vec![3i16, 1i16, 2i16]);
    }

    #[test]
    fn lazy_row_out_of_range() {
        // Build a Lazy SignalTable with no rows using a real temp file.
        let dir = std::env::temp_dir();
        let path = dir.join("pod5_signal_test_empty.tmp");
        std::fs::write(&path, b"\0").unwrap();
        let f = File::open(&path).unwrap();
        let _ = std::fs::remove_file(&path);
        let table = SignalTable::Lazy {
            file: Arc::new(Mutex::new(f)),
            rows: vec![],
        };
        let err = table.samples_for_rows(&[0]).unwrap_err();
        assert!(matches!(err, crate::Error::SignalRowOutOfRange(0, 0)));
    }

    #[test]
    fn lazy_uncompressed_row() {
        let dir = std::env::temp_dir();
        let path = dir.join("pod5_signal_test_uncomp.tmp");
        std::fs::write(&path, b"\0").unwrap();
        let f = File::open(&path).unwrap();
        let _ = std::fs::remove_file(&path);
        let samples: Box<[i16]> = vec![10i16, 20, 30].into_boxed_slice();
        let table = SignalTable::Lazy {
            file: Arc::new(Mutex::new(f)),
            rows: vec![LazySignalRow::Uncompressed(samples)],
        };
        let result = table.samples_for_rows(&[0]).unwrap();
        assert_eq!(result, vec![10i16, 20, 30]);
    }
}
