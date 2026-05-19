/// VBZ signal decompression and the in-memory signal table.
///
/// VBZ compression pipeline (encoding order):
///   int16 ADC samples
///   → delta encode (differences between adjacent samples)
///   → zigzag encode (signed i16 → unsigned u16, small absolute values → small codes)
///   → SVB16 encode (StreamVByte for 16-bit: 1-2 bytes/value, 2-bit control stream)
///   → zstd compress
///
/// On-disk blob layout:
///   [original_size: u32 LE]  ← byte count of original int16 data (= N * 2)
///   [zstd-compressed SVB stream...]
use crate::{Error, Result};

// ── Signal table ─────────────────────────────────────────────────────────────

/// Lightweight in-memory store of all signal rows loaded from the Signal table.
/// Each row is either VBZ-compressed bytes or raw int16 bytes.
#[derive(Debug)]
pub struct SignalTable {
    pub(crate) rows: Vec<SignalRow>,
}

#[derive(Debug)]
pub(crate) struct SignalRow {
    /// Raw bytes: either VBZ-compressed or uncompressed `[i16]` little-endian.
    pub data: Vec<u8>,
    pub num_samples: u32,
    pub compressed: bool,
}

impl SignalTable {
    /// Decompress and concatenate signal rows identified by `indices`.
    pub fn samples_for_rows(&self, indices: &[u64]) -> Result<Vec<i16>> {
        let total: usize = indices
            .iter()
            .map(|&i| {
                self.rows
                    .get(i as usize)
                    .map_or(0, |r| r.num_samples as usize)
            })
            .sum();
        let mut out = Vec::with_capacity(total);
        for &idx in indices {
            let row = self
                .rows
                .get(idx as usize)
                .ok_or(Error::SignalRowOutOfRange(idx, self.rows.len()))?;
            if row.compressed {
                let mut chunk = decompress_vbz(&row.data, row.num_samples as usize)?;
                out.append(&mut chunk);
            } else {
                // Uncompressed: raw little-endian i16 bytes
                for chunk in row.data.chunks_exact(2) {
                    out.push(i16::from_le_bytes([chunk[0], chunk[1]]));
                }
            }
        }
        Ok(out)
    }
}

// ── VBZ decompression ────────────────────────────────────────────────────────

/// Decompress one VBZ blob to `num_samples` raw ADC values.
///
/// The blob is a raw zstd frame (magic `0xFD 0x2F 0xB5 0x28`); there is no
/// separate length header.  After zstd decompression the payload is a
/// SVB16 + zigzag-delta encoded stream of exactly `num_samples` int16 values.
pub fn decompress_vbz(data: &[u8], num_samples: usize) -> Result<Vec<i16>> {
    if num_samples == 0 {
        return Ok(Vec::new());
    }

    // SVB16 output: ceil(N/4) control bytes + at most N*2 data bytes ≈ N*2.25
    let svb_cap = num_samples / 4 + 2 + num_samples * 2;

    let svb_data =
        zstd::bulk::decompress(data, svb_cap).map_err(|e| Error::Vbz(format!("zstd: {e}")))?;

    // SVB16 decode → zigzag values
    let zigzag = decode_svb16(&svb_data, num_samples)?;

    // Zigzag + delta decode → original ADC samples
    let mut samples = Vec::with_capacity(num_samples);
    let mut prev: i16 = 0;
    for z in zigzag {
        let delta = zigzag_decode(z);
        prev = prev.wrapping_add(delta);
        samples.push(prev);
    }

    // Truncate to exactly what the Signal table says (SVB may pad to 4-value boundary)
    samples.truncate(num_samples);

    Ok(samples)
}

// ── SVB16 ────────────────────────────────────────────────────────────────────

/// Decode a StreamVByte-16 stream of `n` unsigned 16-bit integers.
///
/// VBZ uses **1 bit per value** packed LSB-first into control bytes (8 per byte):
///   bit=0 → next 1 byte   encodes the value as u8 (value 0-255)
///   bit=1 → next 2 bytes  encode the value as u16 LE (value 0-65535)
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

    #[test]
    fn zigzag_round_trip() {
        for v in [-32768i16, -1, 0, 1, 127, 32767] {
            let encoded = ((v << 1) ^ (v >> 15)) as u16;
            assert_eq!(zigzag_decode(encoded), v, "round-trip failed for {v}");
        }
    }

    #[test]
    fn svb16_single_byte_values() {
        // 4 values, all fit in 1 byte → bits = 0000 → ctrl byte = 0x00
        // 1 ctrl byte (ceil(4/8)=1), then 4 data bytes
        let data: Vec<u8> = vec![0x00, 0, 1, 2, 3];
        let out = decode_svb16(&data, 4).unwrap();
        assert_eq!(out, vec![0u16, 1, 2, 3]);
    }

    #[test]
    fn svb16_two_byte_values() {
        // 1 value, needs 2 bytes → bit=1 → ctrl byte = 0b00000001 = 0x01
        // 1 ctrl byte, then 2 data bytes for value 256 = [0x00, 0x01] LE
        let data: Vec<u8> = vec![0x01, 0x00, 0x01];
        let out = decode_svb16(&data, 1).unwrap();
        assert_eq!(out, vec![256u16]);
    }

    #[test]
    fn svb16_mixed() {
        // 2 values: first=5 (1 byte, bit=0), second=300 (2 bytes, bit=1)
        // ctrl byte = 0b00000010 = 0x02 (bit 0=0 for first, bit 1=1 for second)
        let data: Vec<u8> = vec![0x02, 5, 0x2C, 0x01]; // ctrl, then 5, then 300 LE
        let out = decode_svb16(&data, 2).unwrap();
        assert_eq!(out, vec![5u16, 300u16]);
    }
}
