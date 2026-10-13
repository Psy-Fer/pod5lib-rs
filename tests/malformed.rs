//! Corrupted and truncated POD5 files must produce `Err`, never a panic or an
//! allocation sized by corrupt data. Deterministic: single-byte corruptions of a
//! small file, truncations, and corruptions of an upstream fixture.
//!
//! The allocation tracker below is per binary, so the tests here run serially and
//! nothing else lives in this file.
use std::alloc::{GlobalAlloc, Layout, System};
use std::io::Cursor;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use pod5lib::{Calibration, EndReason, EndReasonKind, Pore, ReadRecord, Reader, RunInfo, Writer};

/// Records the largest single allocation, to catch sizes taken from corrupt data.
struct TrackMax;
static MAX_ALLOC: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for TrackMax {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        MAX_ALLOC.fetch_max(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        MAX_ALLOC.fetch_max(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        MAX_ALLOC.fetch_max(new_size, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: TrackMax = TrackMax;

/// No legitimate read of these small files allocates anywhere near this.
const ALLOC_LIMIT: usize = 256 << 20;

/// Open `data` both ways (eager and file-backed lazy) and decode every signal,
/// optionally through `par_reads_iter` as well.
fn exercise(data: &[u8], tmp: &std::path::Path, parallel: bool) {
    let decode = |r: Reader| {
        for read in r.reads_iter() {
            let _ = read.signal();
        }
        if parallel {
            let _ = r.par_reads_iter(2).count();
        }
    };
    if let Ok(r) = Reader::from_reader(&mut Cursor::new(data)) {
        decode(r);
    }
    std::fs::write(tmp, data).unwrap();
    if let Ok(r) = Reader::open(tmp) {
        decode(r);
    }
}

fn small_file() -> Vec<u8> {
    let run_info = Arc::new(RunInfo {
        acquisition_id: "acq".into(),
        adc_min: -4096,
        adc_max: 4095,
        sample_rate: 5000,
        ..Default::default()
    });
    let mut w = Writer::new();
    for (i, n) in [300usize, 0, 700].into_iter().enumerate() {
        let read = ReadRecord::new(
            [i as u8 + 1; 16],
            Pore {
                channel: 1 + i as u32,
                well: 1,
                pore_type: "p".into(),
            },
            Calibration {
                offset: -240.0,
                scale: 0.15,
            },
            i as u32,
            0,
            200.0,
            EndReason::from_reason_with_default_forced(EndReasonKind::SignalPositive),
            Arc::clone(&run_info),
        );
        w.add_read(read, (0..n).map(|j| (j * 37 % 900) as i16 - 450).collect());
    }
    let mut buf = Vec::new();
    w.write_to(&mut buf).unwrap();
    buf
}

/// How many corruptions to try: the default test samples, the exhaustive one
/// (`#[ignore]`, run in CI in release mode) tries everything.
struct Coverage {
    /// Corrupt every byte within this many bytes of either end of the small file...
    ends: usize,
    /// ...and every `stride`-th byte in between.
    stride: usize,
    xors: &'static [u8],
    truncate_step: usize,
    /// Positions to corrupt in the upstream fixture (its last 512 bytes come first).
    fixture_cases: usize,
}

/// Both tests share the allocation tracker, so they must not overlap.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn malformed_files_return_errors() {
    run(Coverage {
        ends: 1024,
        stride: 13,
        xors: &[0xFF],
        truncate_step: 61,
        fixture_cases: 64,
    });
}

#[test]
#[ignore = "slow; CI runs it with --release"]
fn malformed_files_return_errors_exhaustive() {
    run(Coverage {
        ends: usize::MAX,
        stride: 1,
        xors: &[0x01, 0x80, 0xFF],
        truncate_step: 7,
        fixture_cases: 712,
    });
}

fn run(cov: Coverage) {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    std::panic::set_hook(Box::new(|_| {})); // failures are reported below instead
    let tmp = std::env::temp_dir().join(format!("pod5lib-malformed-{}.pod5", std::process::id()));
    let small = small_file();
    // The fixture is not in the published crate; skip those cases without it.
    let fixture = std::fs::read(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/multi_fast5_zip_v7.pod5"),
    )
    .unwrap_or_default();

    let mut cases: Vec<(String, Vec<u8>)> = Vec::new();
    let n = small.len();
    for pos in
        (0..n).filter(|&p| p < cov.ends || p >= n.saturating_sub(cov.ends) || p % cov.stride == 0)
    {
        for &x in cov.xors {
            let mut d = small.clone();
            d[pos] ^= x;
            cases.push((format!("small[{pos}] ^= {x:#04x}"), d));
        }
    }
    for len in (0..small.len()).step_by(cov.truncate_step) {
        cases.push((format!("small truncated to {len}"), small[..len].to_vec()));
    }
    // Upstream fixture: every byte of the footer region, plus spread-out positions.
    let n = fixture.len();
    let mut positions: Vec<usize> = (n.saturating_sub(512)..n).collect();
    let mut seed = 0x9E37_79B9_7F4A_7C15u64;
    for _ in 0..if n == 0 { 0 } else { 200 } {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        positions.push((seed >> 33) as usize % n);
    }
    for pos in positions.into_iter().take(cov.fixture_cases) {
        let mut d = fixture.clone();
        d[pos] ^= 0xFF;
        cases.push((format!("fixture[{pos}] ^= 0xff"), d));
    }

    let mut failures = Vec::new();
    for (i, (name, data)) in cases.iter().enumerate() {
        // par_reads_iter builds a thread pool per call, so exercise it on a subset.
        let parallel = i % 16 == 0 || name.starts_with("fixture");
        if std::env::var_os("POD5_MALFORMED_TRACE").is_some() {
            eprintln!("case: {name}");
        }
        MAX_ALLOC.store(0, Ordering::Relaxed);
        let outcome = catch_unwind(AssertUnwindSafe(|| exercise(data, &tmp, parallel)));
        let max = MAX_ALLOC.load(Ordering::Relaxed);
        if outcome.is_err() {
            failures.push(format!("{name}: panicked"));
        } else if max > ALLOC_LIMIT {
            failures.push(format!("{name}: allocated {max} bytes"));
        }
    }
    let _ = std::fs::remove_file(&tmp);
    let _ = std::panic::take_hook();
    assert!(
        failures.is_empty(),
        "{} of {} cases failed:\n{}",
        failures.len(),
        cases.len(),
        failures
            .iter()
            .take(20)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
}
