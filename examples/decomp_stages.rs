//! VBZ decode pipeline breakdown: 3-stage vs fused single-pass.
//!
//!   cargo run --example decomp_stages --release -- file.pod5

use pod5lib::Reader;
use rayon::prelude::*;
use std::hint::black_box;
use std::time::Instant;
use svb::u16::Svb16;

fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: decomp_stages <file.pod5>");
    let reader = Reader::open(&path).expect("open");

    eprint!("Decompressing all reads into RAM...");
    let samples: Vec<Vec<i16>> = reader.reads_iter().map(|r| r.signal().unwrap()).collect();
    let total_samples: usize = samples.iter().map(|s| s.len()).sum();
    let ns: Vec<usize> = samples.iter().map(|s| s.len()).collect();
    eprintln!(
        " {} reads, {}M samples",
        samples.len(),
        total_samples / 1_000_000
    );

    eprint!("Building VBZ blobs...");
    let blobs: Vec<Vec<u8>> = samples
        .par_iter()
        .map(|s| {
            let mut buf = Vec::new();
            svb::encode_vbz_into(s, &mut buf);
            zstd::bulk::compress(&buf, 0).unwrap()
        })
        .collect();
    eprintln!(
        " {:.1} MB",
        blobs.iter().map(|b| b.len()).sum::<usize>() as f64 / 1e6
    );

    let svb_cap = |n: usize| n / 8 + 1 + n * 2;

    // zstd baseline
    let t = Instant::now();
    for (blob, &n) in blobs.iter().zip(&ns) {
        black_box(zstd::bulk::decompress(blob, svb_cap(n)).unwrap());
    }
    let zstd_ms = t.elapsed().as_secs_f64() * 1000.0;

    // 3-stage: SVB16 → zigzag → delta
    let t = Instant::now();
    for (blob, &n) in blobs.iter().zip(&ns) {
        let svb_data = zstd::bulk::decompress(blob, svb_cap(n)).unwrap();
        let zigzag = Svb16.decode(&svb_data, n).unwrap();
        let deltas = svb::zigzag::decode::<i16>(&zigzag);
        black_box(svb::delta::decode(&deltas));
    }
    let staged_ms = t.elapsed().as_secs_f64() * 1000.0;

    // fused single-pass
    let t = Instant::now();
    for (blob, &n) in blobs.iter().zip(&ns) {
        let svb_data = zstd::bulk::decompress(blob, svb_cap(n)).unwrap();
        let mut out = Vec::with_capacity(n);
        svb::decode_vbz_fused_into(&svb_data, n, &mut out).unwrap();
        black_box(out);
    }
    let fused_ms = t.elapsed().as_secs_f64() * 1000.0;

    println!(
        "\n{} reads, {}M samples",
        samples.len(),
        total_samples / 1_000_000
    );
    println!("  zstd only (floor) : {:6.0} ms", zstd_ms);
    println!(
        "  3-stage           : {:6.0} ms  ({:.2}× floor)",
        staged_ms,
        staged_ms / zstd_ms
    );
    println!(
        "  fused             : {:6.0} ms  ({:.2}× floor)  [{:.2}× vs 3-stage]",
        fused_ms,
        fused_ms / zstd_ms,
        staged_ms / fused_ms
    );
}
