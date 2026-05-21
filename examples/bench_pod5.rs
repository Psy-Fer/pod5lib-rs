//! Performance benchmark: sequential scan, random access, and streaming write.
//!
//! Outputs a single JSON object to stdout; progress/warnings go to stderr.
//! Designed to be driven by `tests/test_bench.py`.
//!
//! Modes:
//!   sequential  <file.pod5>
//!       Iterate all reads in file order, decompress pA signal for each.
//!       Writes read IDs to stdout JSON so the caller can build the random list.
//!
//!   random  <file.pod5>
//!       Read UUID strings from stdin (one per line), look up each with
//!       get_read_by_id(), decompress pA signal.
//!
//!   write  <file.pod5>
//!       Read every read from <file.pod5>, write each to a temp file using
//!       StreamingWriter (VBZ-compresses on the fly).  Measures end-to-end
//!       throughput including source decompression and output compression.
//!
//!   cargo run --example bench_pod5 --release --quiet -- sequential file.pod5
//!   printf "id1\nid2\n" | cargo run --example bench_pod5 --release --quiet -- random file.pod5
//!   cargo run --example bench_pod5 --release --quiet -- write file.pod5

use pod5lib::{Reader, StreamingWriter};
use serde::Serialize;
use std::io::{self, BufRead};
use std::time::Instant;

#[derive(Serialize)]
struct BenchResult {
    mode: String,
    n_reads: usize,
    wall_ms: f64,
    peak_rss_kb: u64,
    /// Populated only in sequential mode — the caller uses these to build the random list.
    #[serde(skip_serializing_if = "Option::is_none")]
    read_ids: Option<Vec<String>>,
    /// Populated only in write mode.
    #[serde(skip_serializing_if = "Option::is_none")]
    bytes_written: Option<u64>,
}

/// Peak RSS (kB) from /proc/self/status VmHWM (high-water mark).
fn peak_rss_kb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmHWM:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|v| v.parse().ok())
        })
        .unwrap_or(0)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("usage: bench_pod5 sequential <file.pod5>");
        eprintln!("       bench_pod5 random     <file.pod5>   # IDs on stdin");
        std::process::exit(1);
    }
    let mode = args[1].as_str();
    let path = &args[2];

    let result = match mode {
        "sequential" => run_sequential(path),
        "random" => run_random(path),
        "write" => run_write(path),
        _ => {
            eprintln!("unknown mode: {mode}");
            std::process::exit(1);
        }
    };

    println!(
        "{}",
        serde_json::to_string(&result).expect("JSON serialization failed")
    );
}

fn run_sequential(path: &str) -> BenchResult {
    let t0 = Instant::now();
    let reader = Reader::open(path).expect("open failed");
    let total = reader.len();
    eprintln!("bench_pod5 sequential: {total} reads in {path}");

    let mut read_ids = Vec::with_capacity(total);
    let mut n = 0usize;

    for (i, read) in reader.reads_iter().enumerate() {
        read_ids.push(read.read_id_str());
        let _pa = read.signal_pa().expect("signal_pa failed");
        n += 1;
        if i > 0 && i % 1000 == 0 {
            eprintln!("  ... {i}/{total}");
        }
    }

    BenchResult {
        mode: "sequential".into(),
        n_reads: n,
        wall_ms: t0.elapsed().as_secs_f64() * 1000.0,
        peak_rss_kb: peak_rss_kb(),
        read_ids: Some(read_ids),
        bytes_written: None,
    }
}

fn run_write(path: &str) -> BenchResult {
    let out = std::env::temp_dir().join("bench_pod5_out.pod5");
    let reader = Reader::open(path).expect("open failed");
    let total = reader.len();
    eprintln!(
        "bench_pod5 write: {total} reads in {path} → {}",
        out.display()
    );

    let t0 = Instant::now();
    let mut writer = StreamingWriter::create(out.to_str().unwrap()).expect("create writer failed");
    writer.file_identifier = reader.file_identifier().to_string();

    let mut n = 0usize;
    for (i, read) in reader.reads_iter().enumerate() {
        let signal = read.signal().expect("signal failed");
        writer.write_read(read, &signal).expect("write_read failed");
        n += 1;
        if i > 0 && i % 1000 == 0 {
            eprintln!("  ... {i}/{total}");
        }
    }
    writer.finish().expect("finish failed");

    let bytes = std::fs::metadata(&out).map(|m| m.len()).unwrap_or(0);
    BenchResult {
        mode: "write".into(),
        n_reads: n,
        wall_ms: t0.elapsed().as_secs_f64() * 1000.0,
        peak_rss_kb: peak_rss_kb(),
        read_ids: None,
        bytes_written: Some(bytes),
    }
}

fn run_random(path: &str) -> BenchResult {
    let ids: Vec<String> = io::stdin()
        .lock()
        .lines()
        .map(|l| l.expect("stdin read failed"))
        .filter(|l| !l.is_empty())
        .collect();
    eprintln!("bench_pod5 random: {} IDs, file={path}", ids.len());

    let t0 = Instant::now();
    let reader = Reader::open(path).expect("open failed");
    let mut n = 0usize;
    let mut missing = 0usize;

    for (i, id) in ids.iter().enumerate() {
        match reader.get_read_by_id(id) {
            Some(read) => {
                let _pa = read.signal_pa().expect("signal_pa failed");
                n += 1;
            }
            None => missing += 1,
        }
        if i > 0 && i % 1000 == 0 {
            eprintln!("  ... {}/{}", i, ids.len());
        }
    }

    if missing > 0 {
        eprintln!("WARNING: {missing} IDs not found");
    }

    BenchResult {
        mode: "random".into(),
        n_reads: n,
        wall_ms: t0.elapsed().as_secs_f64() * 1000.0,
        peak_rss_kb: peak_rss_kb(),
        read_ids: None,
        bytes_written: None,
    }
}
