/// Diagnostic: inspect zstd-decompressed SVB stream size and first bytes
/// to determine the exact SVB bit layout used in VBZ.
///
///   cargo run --example inspect_signal
use std::fs::File;
use std::io::{BufReader, Cursor, Read, Seek, SeekFrom};

use arrow::array::{LargeBinaryArray, UInt32Array};
use arrow::ipc::reader::FileReader;

const SIG: [u8; 8] = [0x8B, b'P', b'O', b'D', 0x0D, 0x0A, 0x1A, 0x0A];

fn main() {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "examples/small.pod5".into());

    let file = File::open(&path).expect("open");
    let mut br = BufReader::new(file);

    let mut sig = [0u8; 8];
    br.read_exact(&mut sig).unwrap();
    assert_eq!(sig, SIG);

    let file_size = br.seek(SeekFrom::End(0)).unwrap();
    br.seek(SeekFrom::Start(file_size - 32)).unwrap();
    let mut len_buf = [0u8; 8];
    br.read_exact(&mut len_buf).unwrap();
    let footer_len = i64::from_le_bytes(len_buf) as u64;
    let padded = (footer_len + 7) & !7;
    let footer_fb_start = file_size - 32 - padded;
    br.seek(SeekFrom::Start(footer_fb_start)).unwrap();
    let mut footer_bytes = vec![0u8; footer_len as usize];
    br.read_exact(&mut footer_bytes).unwrap();

    let (signal_off, signal_len) = find_signal_table(&footer_bytes).expect("signal table");

    let mut buf = vec![0u8; signal_len as usize];
    br.seek(SeekFrom::Start(signal_off)).unwrap();
    br.read_exact(&mut buf).unwrap();

    let cursor = Cursor::new(buf);
    let mut ipc = FileReader::try_new(cursor, None).expect("IPC");

    let batch = ipc.next().unwrap().expect("batch");
    let samples_col = batch
        .column_by_name("samples")
        .and_then(|c| c.as_any().downcast_ref::<UInt32Array>())
        .expect("samples");
    let signal_col = batch
        .column_by_name("signal")
        .and_then(|c| c.as_any().downcast_ref::<LargeBinaryArray>())
        .expect("signal LargeBinary");

    for row in 0..5.min(batch.num_rows()) {
        let compressed = signal_col.value(row);
        let n = samples_col.value(row) as usize;
        let svb_cap = n * 3 + 64;
        let svb = zstd::bulk::decompress(compressed, svb_cap).expect("zstd");

        println!(
            "row {row}: n={n} compressed={} svb_decompressed={}",
            compressed.len(),
            svb.len()
        );
        println!("  svb first 32B: {:02x?}", &svb[..32.min(svb.len())]);

        // Try 1-bit/8-per-byte layout
        let ctrl_1bit = n.div_ceil(8);
        println!(
            "  if 1-bit (8/byte): ctrl_bytes={ctrl_1bit}  first ctrl byte: {:08b}",
            svb[0]
        );

        // Try 2-bit/4-per-byte layout
        let ctrl_2bit = n.div_ceil(4);
        println!(
            "  if 2-bit (4/byte): ctrl_bytes={ctrl_2bit}  first ctrl byte: {:08b}",
            svb[0]
        );

        // Try decoding with 1-bit layout
        match try_decode_1bit(&svb, n) {
            Ok(vals) => println!(
                "  1-bit decode: OK ({} values), first 5: {:?}",
                vals.len(),
                &vals[..5.min(vals.len())]
            ),
            Err(e) => println!("  1-bit decode: FAIL ({e})"),
        }

        // Try decoding with 2-bit layout
        match try_decode_2bit(&svb, n) {
            Ok(vals) => println!(
                "  2-bit decode: OK ({} values), first 5: {:?}",
                vals.len(),
                &vals[..5.min(vals.len())]
            ),
            Err(e) => println!("  2-bit decode: FAIL ({e})"),
        }
        println!();
    }
}

fn try_decode_1bit(data: &[u8], n: usize) -> Result<Vec<i16>, String> {
    let ctrl_len = n.div_ceil(8);
    if data.len() < ctrl_len {
        return Err("too short for ctrl".into());
    }
    let ctrl = &data[..ctrl_len];
    let mut pos = ctrl_len;
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let bit = (ctrl[i / 8] >> (i % 8)) & 1;
        if bit == 0 {
            if pos >= data.len() {
                return Err(format!("overrun 1B at i={i}"));
            }
            out.push(zigzag(data[pos] as u16));
            pos += 1;
        } else {
            if pos + 1 >= data.len() {
                return Err(format!("overrun 2B at i={i}"));
            }
            let v = u16::from_le_bytes([data[pos], data[pos + 1]]);
            out.push(zigzag(v));
            pos += 2;
        }
    }
    // delta decode
    let mut prev = 0i16;
    let result: Vec<i16> = out
        .iter()
        .map(|&d| {
            prev = prev.wrapping_add(d);
            prev
        })
        .collect();
    Ok(result)
}

fn try_decode_2bit(data: &[u8], n: usize) -> Result<Vec<i16>, String> {
    let ctrl_len = n.div_ceil(4);
    if data.len() < ctrl_len {
        return Err("too short for ctrl".into());
    }
    let ctrl = &data[..ctrl_len];
    let mut pos = ctrl_len;
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let code = (ctrl[i / 4] >> ((i % 4) * 2)) & 3;
        let len = code as usize + 1;
        if pos + len > data.len() {
            return Err(format!("overrun {}B at i={i}", len));
        }
        let v = if len == 1 {
            data[pos] as u16
        } else if len == 2 {
            u16::from_le_bytes([data[pos], data[pos + 1]])
        } else {
            return Err(format!("unexpected code {code}"));
        };
        out.push(zigzag(v));
        pos += len;
    }
    let mut prev = 0i16;
    let result: Vec<i16> = out
        .iter()
        .map(|&d| {
            prev = prev.wrapping_add(d);
            prev
        })
        .collect();
    Ok(result)
}

fn zigzag(n: u16) -> i16 {
    ((n >> 1) as i16) ^ (-((n & 1) as i16))
}

// ── Minimal footer parser ─────────────────────────────────────────────────────

fn find_signal_table(buf: &[u8]) -> Option<(u64, u64)> {
    let root = u32_le(buf, 0) as usize;
    let contents_pos = fb_field(buf, root, 3)?;
    let v = u32_le(buf, contents_pos) as usize;
    let vec_start = contents_pos + v;
    let count = u32_le(buf, vec_start) as usize;
    for i in 0..count {
        let ref_pos = vec_start + 4 + i * 4;
        let tbl = ref_pos + u32_le(buf, ref_pos) as usize;
        let ct_pos = fb_field(buf, tbl, 3)?;
        if i16_le(buf, ct_pos) == 1 {
            let off_pos = fb_field(buf, tbl, 0)?;
            let len_pos = fb_field(buf, tbl, 1)?;
            return Some((i64_le(buf, off_pos) as u64, i64_le(buf, len_pos) as u64));
        }
    }
    None
}

fn fb_field(buf: &[u8], table: usize, id: usize) -> Option<usize> {
    let vtable = (table as i64 - i32_le(buf, table) as i64) as usize;
    let vt_size = u16_le(buf, vtable) as usize;
    let entry = vtable + 4 + id * 2;
    if entry + 2 > vtable + vt_size {
        return None;
    }
    let offset = u16_le(buf, entry) as usize;
    if offset == 0 {
        None
    } else {
        Some(table + offset)
    }
}

fn u16_le(buf: &[u8], pos: usize) -> u16 {
    u16::from_le_bytes(buf[pos..pos + 2].try_into().unwrap())
}
fn u32_le(buf: &[u8], pos: usize) -> u32 {
    u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap())
}
fn i16_le(buf: &[u8], pos: usize) -> i16 {
    i16::from_le_bytes(buf[pos..pos + 2].try_into().unwrap())
}
fn i32_le(buf: &[u8], pos: usize) -> i32 {
    i32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap())
}
fn i64_le(buf: &[u8], pos: usize) -> i64 {
    i64::from_le_bytes(buf[pos..pos + 8].try_into().unwrap())
}
