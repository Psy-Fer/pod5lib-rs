//! Performance benchmark: sequential scan and random access by read ID.
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
//!   cargo run --example bench_pod5 --release --quiet -- sequential file.pod5
//!   printf "id1\nid2\n" | cargo run --example bench_pod5 --release --quiet -- random file.pod5

use pod5::Reader;
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
    }
}
