/// Integration tests for the POD5 reader.
///
/// File-backed tests look for a POD5 file in this order:
///   1. tests/data/sample.pod5
///   2. examples/small.pod5
///
/// If neither exists the tests are silently skipped.
use std::path::PathBuf;

fn sample_pod5() -> Option<PathBuf> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let candidates = [
        root.join("tests/data/sample.pod5"),
        root.join("examples/small.pod5"),
    ];
    candidates.into_iter().find(|p| p.exists())
}

// ── Format / unit tests (no file needed) ─────────────────────────────────────

#[test]
fn uuid_to_str_known_value() {
    // Python: uuid.UUID(bytes_le=bytes([1,2,3,4, 5,6, 7,8, 9,10, 11,12,13,14,15,16])).hex
    // Expected: "04030201-0605-0807-090a-0b0c0d0e0f10"
    let bytes: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let s = pod5::uuid_to_str(&bytes);
    assert_eq!(s, "04030201-0605-0807-090a-0b0c0d0e0f10");
}

#[test]
fn calibration_to_pa() {
    let cal = pod5::Calibration {
        offset: 0.0,
        scale: 0.1,
    };
    let pa = cal.to_pa(&[0i16, 10, -10, 100]);
    assert!((pa[1] - 1.0f32).abs() < 1e-5);
    assert!((pa[2] - (-1.0f32)).abs() < 1e-5);
}

// ── File-backed tests (skipped when test file absent) ─────────────────────────

#[test]
fn open_sample_file() {
    let Some(path) = sample_pod5() else {
        return;
    };
    let reader = pod5::Reader::open(&path).expect("open failed");
    assert!(!reader.is_empty(), "expected at least one read");
    assert!(
        !reader.run_infos().is_empty(),
        "expected at least one RunInfo"
    );
}

#[test]
fn read_fields_present() {
    let Some(path) = sample_pod5() else {
        return;
    };
    let reader = pod5::Reader::open(&path).expect("open failed");
    let read = reader.reads().first().expect("no reads");
    // read_id must be non-zero
    assert_ne!(read.read_id, [0u8; 16]);
    // num_samples must match actual signal length
    let signal = read.signal().expect("signal decompression failed");
    assert_eq!(signal.len(), read.num_samples as usize);
}

#[test]
fn signal_pa_length_matches() {
    let Some(path) = sample_pod5() else {
        return;
    };
    let reader = pod5::Reader::open(&path).expect("open failed");
    let read = reader.reads().first().expect("no reads");
    let pa = read.signal_pa().expect("signal_pa failed");
    assert_eq!(pa.len(), read.num_samples as usize);
    // sanity: picoamp values should be finite
    assert!(pa.iter().all(|v| v.is_finite()), "NaN/Inf in pA signal");
}

#[test]
fn run_info_sample_rate_nonzero() {
    let Some(path) = sample_pod5() else {
        return;
    };
    let reader = pod5::Reader::open(&path).expect("open failed");
    for ri in reader.run_infos() {
        assert!(ri.sample_rate > 0);
    }
}
