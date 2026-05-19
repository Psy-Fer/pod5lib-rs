/// VBZ signal decompression and the in-memory signal table.
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
use arrow::array::{Array, Int16Array, LargeBinaryArray, LargeListArray};
use arrow::record_batch::RecordBatch;

use crate::{Error, Result};

// ── Signal table ─────────────────────────────────────────────────────────────

/// In-memory signal store.
///
/// Holds Arrow `RecordBatch` objects from the Signal IPC section — no byte copying.
/// Compressed blobs are borrowed directly from Arrow's `Arc`-backed buffers at
/// decompression time.
#[derive(Debug)]
pub struct SignalTable {
    /// Arrow IPC batches kept alive so we can borrow signal bytes from them.
    pub(crate) batches: Vec<RecordBatch>,
    /// One entry per signal row in the file (across all batches).
    pub(crate) index: Vec<SignalRowRef>,
}

/// Lightweight pointer into one row of one Arrow batch.
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
        let total: usize = indices
            .iter()
            .map(|&i| {
                self.index
                    .get(i as usize)
                    .map_or(0, |r| r.num_samples as usize)
            })
            .sum();
        let mut out = Vec::with_capacity(total);

        for &idx in indices {
            let row = self
                .index
                .get(idx as usize)
                .ok_or(Error::SignalRowOutOfRange(idx, self.index.len()))?;

            let batch = &self.batches[row.batch_idx as usize];
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
                let list = col
                    .as_any()
                    .downcast_ref::<LargeListArray>()
                    .ok_or_else(|| Error::TypeMismatch("signal (uncompressed list)".into()))?;
                let arr = list.value(i);
                let int16 = arr
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .ok_or_else(|| Error::TypeMismatch("signal (uncompressed int16)".into()))?;
                for j in 0..int16.len() {
                    out.push(int16.value(j));
                }
            }
        }
        Ok(out)
    }
}

// ── VBZ compression ───────────────────────────────────────────────────────────

/// Compress raw ADC i16 samples to a VBZ blob.
///
/// Pipeline (reverse of decompression): delta → zigzag → SVB16 → zstd
pub fn compress_vbz(samples: &[i16]) -> crate::Result<Vec<u8>> {
    if samples.is_empty() {
        return Ok(Vec::new());
    }

    let mut prev: i16 = 0;
    let zigzag: Vec<u16> = samples
        .iter()
        .map(|&s| {
            let delta = s.wrapping_sub(prev);
            prev = s;
            ((delta << 1) ^ (delta >> 15)) as u16
        })
        .collect();

    let svb = encode_svb16(&zigzag);
    zstd::bulk::compress(&svb, 0).map_err(|e| crate::Error::Vbz(format!("zstd compress: {e}")))
}

/// Encode u16 values as SVB16 (1-bit-per-value control stream, LSB-first).
///
/// bit=0 → write 1 data byte (value fits in u8)
/// bit=1 → write 2 data bytes LE (value requires u16)
pub(crate) fn encode_svb16(values: &[u16]) -> Vec<u8> {
    if values.is_empty() {
        return Vec::new();
    }
    let n = values.len();
    let ctrl_len = n.div_ceil(8);
    let mut ctrl = vec![0u8; ctrl_len];
    let mut data: Vec<u8> = Vec::with_capacity(n * 2);

    for (i, &v) in values.iter().enumerate() {
        if v <= 0xFF {
            data.push(v as u8);
        } else {
            ctrl[i / 8] |= 1 << (i % 8);
            data.extend_from_slice(&v.to_le_bytes());
        }
    }

    let mut out = ctrl;
    out.extend_from_slice(&data);
    out
}

// ── VBZ decompression ────────────────────────────────────────────────────────

/// Decompress one VBZ blob to `num_samples` raw ADC values.
///
/// The blob is a raw zstd frame; after zstd decompression the payload is
/// SVB16 + zigzag-delta encoded i16 values.
pub fn decompress_vbz(data: &[u8], num_samples: usize) -> Result<Vec<i16>> {
    if num_samples == 0 {
        return Ok(Vec::new());
    }

    // SVB16 output: ceil(N/8) control bytes + at most N*2 data bytes
    let svb_cap = num_samples / 8 + 1 + num_samples * 2;

    let svb_data =
        zstd::bulk::decompress(data, svb_cap).map_err(|e| Error::Vbz(format!("zstd: {e}")))?;

    let zigzag = decode_svb16(&svb_data, num_samples)?;

    let mut samples = Vec::with_capacity(num_samples);
    let mut prev: i16 = 0;
    for z in zigzag {
        let delta = zigzag_decode(z);
        prev = prev.wrapping_add(delta);
        samples.push(prev);
    }

    samples.truncate(num_samples);
    Ok(samples)
}

// ── SVB16 ────────────────────────────────────────────────────────────────────

/// Decode a StreamVByte-16 stream of `n` unsigned 16-bit integers.
///
/// VBZ uses **1 bit per value** packed LSB-first into control bytes (8 per byte):
///   bit=0 → next 1 byte  encodes the value as u8
///   bit=1 → next 2 bytes encode the value as u16 LE
///
/// Layout: `ceil(n/8)` control bytes, followed by the variable-length data bytes.
fn decode_svb16(data: &[u8], n: usize) -> Result<Vec<u16>> {
    if n == 0 {
        return Ok(Vec::new());
    }
    let ctrl_len = n.div_ceil(8);
    if data.len() < ctrl_len {
        return Err(Error::Vbz("SVB16: data shorter than control stream".into()));
    }

    let ctrl = &data[..ctrl_len];
    let mut pos = ctrl_len;
    let mut out = Vec::with_capacity(n);

    for i in 0..n {
        let bit = (ctrl[i / 8] >> (i % 8)) & 1;
        let val = if bit == 0 {
            if pos >= data.len() {
                return Err(Error::Vbz("SVB16: data overrun (1-byte)".into()));
            }
            let v = data[pos] as u16;
            pos += 1;
            v
        } else {
            if pos + 1 >= data.len() {
                return Err(Error::Vbz("SVB16: data overrun (2-byte)".into()));
            }
            let v = u16::from_le_bytes([data[pos], data[pos + 1]]);
            pos += 2;
            v
        };
        out.push(val);
    }

    Ok(out)
}

// ── Zigzag ───────────────────────────────────────────────────────────────────

/// Decode a zigzag-encoded u16 back to a signed i16 delta.
///
/// Encoding: `(x << 1) ^ (x >> 15)`  — maps 0→0, -1→1, 1→2, -2→3, …
#[inline]
fn zigzag_decode(n: u16) -> i16 {
    ((n >> 1) as i16) ^ (-((n & 1) as i16))
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int16Builder, LargeListBuilder, UInt32Builder};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    // ── Helpers ───────────────────────────────────────────────────────────

    /// Build an uncompressed `SignalTable` from slices of i16 samples.
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

        SignalTable {
            batches: vec![batch],
            index,
        }
    }

    // ── Zigzag ────────────────────────────────────────────────────────────

    #[test]
    fn zigzag_round_trip() {
        for v in [-32768i16, -1, 0, 1, 127, 32767] {
            let encoded = ((v << 1) ^ (v >> 15)) as u16;
            assert_eq!(zigzag_decode(encoded), v, "round-trip failed for {v}");
        }
    }

    // ── SVB16 ────────────────────────────────────────────────────────────

    #[test]
    fn svb16_single_byte_values() {
        // 4 values, all fit in 1 byte → bits = 00000000 → ctrl = 0x00
        let data: Vec<u8> = vec![0x00, 0, 1, 2, 3];
        let out = decode_svb16(&data, 4).unwrap();
        assert_eq!(out, vec![0u16, 1, 2, 3]);
    }

    #[test]
    fn svb16_two_byte_values() {
        // 1 value, needs 2 bytes → bit=1 → ctrl byte = 0b00000001 = 0x01
        let data: Vec<u8> = vec![0x01, 0x00, 0x01]; // ctrl, then 256 LE
        let out = decode_svb16(&data, 1).unwrap();
        assert_eq!(out, vec![256u16]);
    }

    #[test]
    fn svb16_mixed() {
        // val=5 (1 byte, bit=0), val=300 (2 bytes, bit=1)
        // ctrl byte = 0b00000010 = 0x02
        let data: Vec<u8> = vec![0x02, 5, 0x2C, 0x01];
        let out = decode_svb16(&data, 2).unwrap();
        assert_eq!(out, vec![5u16, 300u16]);
    }

    // ── VBZ ──────────────────────────────────────────────────────────────

    // ── SVB16 encode ─────────────────────────────────────────────────────

    #[test]
    fn svb16_encode_decode_round_trip() {
        let values: Vec<u16> = vec![0, 1, 255, 256, 300, 1000, 65535];
        let encoded = encode_svb16(&values);
        let decoded = decode_svb16(&encoded, values.len()).unwrap();
        assert_eq!(decoded, values);
    }

    // ── VBZ compress/decompress ───────────────────────────────────────────

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
        let table = SignalTable {
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
}
