/// Write the first N reads from a source POD5 file to a new POD5 file.
///
/// Usage:  cargo run --example write_pod5 [src.pod5 [out.pod5 [N]]]
///
/// Defaults: src = tests/data/sample.pod5 or examples/small.pod5
///           out = tests/data/rust_written.pod5
///           N   = 10
use std::path::PathBuf;

use pod5lib::{Reader, Writer};

fn find_test_file() -> Option<PathBuf> {
    for candidate in &["tests/data/sample.pod5", "examples/small.pod5"] {
        let p = PathBuf::from(candidate);
        if p.exists() {
            return Some(p);
        }
    }
    None
}

fn main() {
    let mut args = std::env::args().skip(1);
    let src: PathBuf = args
        .next()
        .map(PathBuf::from)
        .or_else(find_test_file)
        .unwrap_or_else(|| {
            eprintln!(
                "No POD5 source file found.\n\
                 Place tests/data/sample.pod5 or pass a path explicitly."
            );
            std::process::exit(1);
        });

    let out: PathBuf = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("tests/data/rust_written.pod5"));

    let n_take: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(10);

    let reader = Reader::open(&src).unwrap_or_else(|e| {
        eprintln!("Cannot open {}: {e}", src.display());
        std::process::exit(1);
    });

    let mut writer = Writer::new();
    writer.file_identifier = reader.file_identifier().to_string();
    writer.software = "pod5-rs write_pod5 example".to_string();
    writer.pod5_version = reader.pod5_version().to_string();

    let n = n_take.min(reader.len());
    for read in reader.reads_iter().take(n) {
        let signal = read.signal().unwrap_or_else(|e| {
            eprintln!("Cannot decompress signal for {}: {e}", read.read_id_str());
            std::process::exit(1);
        });
        writer.add_read(read.clone(), signal);
    }

    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    writer.write_file(&out).unwrap_or_else(|e| {
        eprintln!("Write failed: {e}");
        std::process::exit(1);
    });

    println!("Wrote {n} reads → {}", out.display());

    // Self-verify: read back with our reader and compare read IDs.
    let verify = Reader::open(&out).unwrap_or_else(|e| {
        eprintln!("Self-verify open failed: {e}");
        std::process::exit(1);
    });
    assert_eq!(verify.len(), n, "read-back count mismatch");
    for (orig, written) in reader.reads_iter().take(n).zip(verify.reads_iter()) {
        assert_eq!(
            orig.read_id_str(),
            written.read_id_str(),
            "read ID mismatch"
        );
    }
    println!("Self-verify OK: {n} read IDs match.");
}
