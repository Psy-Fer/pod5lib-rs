/// Integration tests for the POD5 reader.
///
/// File-backed tests look for a POD5 file in this order:
///   1. tests/data/sample.pod5
///   2. examples/small.pod5
///
/// If neither exists the tests are silently skipped.
use std::io::Cursor;
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
    // Python: str(uuid.UUID(bytes=bytes([1,2,3,4, 5,6, 7,8, 9,10, 11,12,13,14,15,16])))
    // == "01020304-0506-0708-090a-0b0c0d0e0f10"
    let bytes: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let s = pod5lib::uuid_to_str(&bytes);
    assert_eq!(s, "01020304-0506-0708-090a-0b0c0d0e0f10");
}

#[test]
fn calibration_to_pa() {
    // Formula: pA = (adc + offset) * scale
    // offset is stored negative (e.g. -224 means subtract 224 from ADC).
    let cal = pod5lib::Calibration {
        offset: -224.0,
        scale: 0.1,
    };
    let pa = cal.to_pa(&[224i16, 234, 214]);
    // (224 + -224) * 0.1 = 0.0
    assert!((pa[0] - 0.0f32).abs() < 1e-5);
    // (234 + -224) * 0.1 = 1.0
    assert!((pa[1] - 1.0f32).abs() < 1e-5);
    // (214 + -224) * 0.1 = -1.0
    assert!((pa[2] - (-1.0f32)).abs() < 1e-5);
}

// ── File-backed tests (skipped when test file absent) ─────────────────────────

#[test]
fn open_sample_file() {
    let Some(path) = sample_pod5() else {
        return;
    };
    let reader = pod5lib::Reader::open(&path).expect("open failed");
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
    let reader = pod5lib::Reader::open(&path).expect("open failed");
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
    let reader = pod5lib::Reader::open(&path).expect("open failed");
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
    let reader = pod5lib::Reader::open(&path).expect("open failed");
    for ri in reader.run_infos() {
        assert!(ri.sample_rate > 0);
    }
}

#[test]
fn reader_accessors_nonempty() {
    let Some(path) = sample_pod5() else {
        return;
    };
    let reader = pod5lib::Reader::open(&path).expect("open failed");
    assert!(!reader.pod5_version().is_empty());
    assert!(!reader.file_identifier().is_empty());
    assert!(!reader.software().is_empty());
    assert_eq!(reader.len(), reader.reads().len());
    assert!(!reader.is_empty());
}

#[test]
fn reads_by_id_finds_first_read() {
    let Some(path) = sample_pod5() else {
        return;
    };
    let reader = pod5lib::Reader::open(&path).expect("open failed");
    let first = reader.reads().first().expect("no reads");
    let id = first.read_id_str();
    let found: Vec<_> = reader.reads_by_id(&id).collect();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].read_id, first.read_id);
}

#[test]
fn reads_by_id_not_found() {
    let Some(path) = sample_pod5() else {
        return;
    };
    let reader = pod5lib::Reader::open(&path).expect("open failed");
    let found: Vec<_> = reader
        .reads_by_id("00000000-0000-0000-0000-000000000000")
        .collect();
    assert!(found.is_empty());
}

#[test]
fn reads_iter_yields_all_reads() {
    let Some(path) = sample_pod5() else {
        return;
    };
    let reader = pod5lib::Reader::open(&path).expect("open failed");
    let count = reader.reads_iter().count();
    assert_eq!(count, reader.len());
}

#[test]
fn reads_iter_matches_reads_slice() {
    let Some(path) = sample_pod5() else {
        return;
    };
    let reader = pod5lib::Reader::open(&path).expect("open failed");
    for (iter_read, slice_read) in reader.reads_iter().zip(reader.reads()) {
        assert_eq!(iter_read.read_id, slice_read.read_id);
    }
}

#[test]
fn get_read_by_id_finds_every_read() {
    let Some(path) = sample_pod5() else {
        return;
    };
    let reader = pod5lib::Reader::open(&path).expect("open failed");
    for read in reader.reads() {
        let id = read.read_id_str();
        let found = reader
            .get_read_by_id(&id)
            .expect("get_read_by_id returned None");
        assert_eq!(found.read_id, read.read_id);
    }
}

#[test]
fn get_read_by_id_not_found() {
    let Some(path) = sample_pod5() else {
        return;
    };
    let reader = pod5lib::Reader::open(&path).expect("open failed");
    assert!(
        reader
            .get_read_by_id("00000000-0000-0000-0000-000000000000")
            .is_none()
    );
}

#[test]
fn get_read_by_id_bad_format_returns_none() {
    let Some(path) = sample_pod5() else {
        return;
    };
    let reader = pod5lib::Reader::open(&path).expect("open failed");
    assert!(reader.get_read_by_id("not-a-uuid").is_none());
    assert!(reader.get_read_by_id("").is_none());
    assert!(
        reader
            .get_read_by_id("gggggggg-0000-0000-0000-000000000000")
            .is_none()
    );
}

// ── Writer round-trip tests ───────────────────────────────────────────────────

#[test]
fn writer_round_trip_read_ids() {
    let Some(path) = sample_pod5() else {
        return;
    };
    let src = pod5lib::Reader::open(&path).expect("open source");
    let n = 3.min(src.len());

    let mut writer = pod5lib::Writer::new();
    writer.file_identifier = src.file_identifier().to_string();
    writer.pod5_version = src.pod5_version().to_string();
    for read in src.reads_iter().take(n) {
        let signal = read.signal().expect("decompress");
        writer.add_read(read.clone(), signal);
    }

    let mut buf = Vec::new();
    writer.write_to(&mut buf).expect("write_to failed");

    let mut cursor = Cursor::new(buf);
    let dst = pod5lib::Reader::from_reader(&mut cursor).expect("read back failed");

    assert_eq!(dst.len(), n, "read count mismatch");
    for (orig, written) in src.reads_iter().take(n).zip(dst.reads_iter()) {
        assert_eq!(
            orig.read_id_str(),
            written.read_id_str(),
            "read_id mismatch"
        );
    }
}

#[test]
fn writer_round_trip_signal() {
    let Some(path) = sample_pod5() else {
        return;
    };
    let src = pod5lib::Reader::open(&path).expect("open source");
    let n = 3.min(src.len());

    let mut writer = pod5lib::Writer::new();
    writer.file_identifier = src.file_identifier().to_string();
    writer.pod5_version = src.pod5_version().to_string();
    let orig_signals: Vec<Vec<i16>> = src
        .reads_iter()
        .take(n)
        .map(|r| r.signal().expect("decompress"))
        .collect();
    for (read, signal) in src.reads_iter().take(n).zip(orig_signals.iter()) {
        writer.add_read(read.clone(), signal.clone());
    }

    let mut buf = Vec::new();
    writer.write_to(&mut buf).expect("write_to failed");

    let mut cursor = Cursor::new(buf);
    let dst = pod5lib::Reader::from_reader(&mut cursor).expect("read back failed");

    for (orig_sig, written) in orig_signals.iter().zip(dst.reads_iter()) {
        let written_sig = written.signal().expect("decompress written");
        assert_eq!(orig_sig, &written_sig, "signal mismatch");
    }
}

#[test]
fn writer_empty_file() {
    let writer = pod5lib::Writer::new();
    let mut buf = Vec::new();
    writer.write_to(&mut buf).expect("write_to failed");

    let mut cursor = Cursor::new(buf);
    let dst = pod5lib::Reader::from_reader(&mut cursor).expect("read back failed");
    assert!(dst.is_empty());
    assert_eq!(
        dst.reads_table_version(),
        7,
        "writer should emit Reads table V7"
    );
}

#[test]
fn writer_round_trip_v7_fields() {
    let Some(path) = sample_pod5() else {
        return;
    };
    let src = pod5lib::Reader::open(&path).expect("open source");
    let Some(orig) = src.reads_iter().next() else {
        return;
    };

    // A channel that only fits in `channel_32bit`, plus explicit V5 values.
    let mut read = orig.clone();
    read.pore.channel = 70_000;
    read.expected_open_pore_level = 210.5;
    read.selected_read_level = 85.25;

    let mut writer = pod5lib::Writer::new();
    writer.file_identifier = src.file_identifier().to_string();
    writer.add_read(read, orig.signal().expect("decompress"));
    let mut buf = Vec::new();
    writer.write_to(&mut buf).expect("write_to failed");

    let dst = pod5lib::Reader::from_reader(&mut Cursor::new(buf)).expect("read back failed");
    assert_eq!(dst.reads_table_version(), 7);
    let written = &dst.reads()[0];
    assert_eq!(written.pore.channel, 70_000);
    assert_eq!(written.expected_open_pore_level, 210.5);
    assert_eq!(written.selected_read_level, 85.25);
}

// ── Reader error-path tests (no file needed) ──────────────────────────────────

const POD5_SIG: [u8; 8] = [0x8B, b'P', b'O', b'D', 0x0D, 0x0A, 0x1A, 0x0A];
const FOOTER_MAGIC: [u8; 8] = [b'F', b'O', b'O', b'T', b'E', b'R', 0x00, 0x00];

#[test]
fn reader_invalid_opening_signature() {
    let mut cursor = Cursor::new(b"WRONGSIG");
    let err = pod5lib::Reader::from_reader(&mut cursor).unwrap_err();
    assert!(
        matches!(err, pod5lib::Error::InvalidSignature),
        "expected InvalidSignature, got {err:?}"
    );
}

/// File layout for error-path tests (64 bytes):
///   [0..8]   opening SIG
///   [8..24]  section_marker_a
///   [24..32] footer_magic_field  (position = file_size - 32 - align8(footer_len) - 8 = 32 - 0 - 8 = 24)
///   [32..40] footer_len = 0
///   [40..56] section_marker_b   (final marker, read from file_size - 24 = 40)
///   [56..64] closing SIG
fn build_error_test_buf(marker_a: [u8; 16], marker_b: [u8; 16], magic: [u8; 8]) -> Vec<u8> {
    let mut buf = vec![0u8; 64];
    buf[0..8].copy_from_slice(&POD5_SIG);
    buf[8..24].copy_from_slice(&marker_a);
    buf[24..32].copy_from_slice(&magic);
    // buf[32..40] stays 0 → footer_len = 0
    buf[40..56].copy_from_slice(&marker_b);
    buf[56..64].copy_from_slice(&POD5_SIG);
    buf
}

#[test]
fn reader_section_marker_mismatch() {
    let buf = build_error_test_buf([0xAA; 16], [0xBB; 16], FOOTER_MAGIC);
    let mut cursor = Cursor::new(buf);
    let err = pod5lib::Reader::from_reader(&mut cursor).unwrap_err();
    assert!(
        matches!(err, pod5lib::Error::InconsistentSectionMarker),
        "expected InconsistentSectionMarker, got {err:?}"
    );
}

#[test]
fn reader_footer_magic_not_found() {
    let buf = build_error_test_buf([0xCC; 16], [0xCC; 16], [0u8; 8]);
    let mut cursor = Cursor::new(buf);
    let err = pod5lib::Reader::from_reader(&mut cursor).unwrap_err();
    assert!(
        matches!(err, pod5lib::Error::FooterMagicNotFound),
        "expected FooterMagicNotFound, got {err:?}"
    );
}
