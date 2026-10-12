/// Integration tests for the POD5 reader.
///
/// File-backed tests look for a POD5 file in this order:
///   1. tests/data/sample.pod5
///   2. examples/small.pod5
///   3. tests/fixtures/multi_fast5_zip_v7.pod5 (committed, so these always run in CI)
use std::io::Cursor;
use std::path::PathBuf;

fn sample_pod5() -> Option<PathBuf> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let candidates = [
        root.join("tests/data/sample.pod5"),
        root.join("examples/small.pod5"),
        root.join("tests/fixtures/multi_fast5_zip_v7.pod5"),
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
    assert_eq!(dst.pod5_version(), pod5lib::POD5_COMPAT_VERSION);
    assert!(
        dst.software().starts_with("pod5lib-rs "),
        "{}",
        dst.software()
    );
}

#[test]
fn writer_rejects_invalid_file_identifier() {
    let mut writer = pod5lib::Writer::new();
    writer.file_identifier = "not-a-uuid".to_string();
    let err = writer.write_to(&mut Vec::new()).unwrap_err();
    assert!(
        matches!(err, pod5lib::Error::InvalidFileIdentifier(_)),
        "{err}"
    );
}

#[test]
fn writer_rejects_invalid_pod5_version() {
    let mut writer = pod5lib::Writer::new();
    writer.pod5_version = "0.1".to_string();
    let err = writer.write_to(&mut Vec::new()).unwrap_err();
    assert!(
        matches!(err, pod5lib::Error::InvalidPod5Version(_)),
        "{err}"
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

// ── Reads built from scratch (no source file) ─────────────────────────────────

fn scratch_run_info() -> std::sync::Arc<pod5lib::RunInfo> {
    std::sync::Arc::new(pod5lib::RunInfo {
        acquisition_id: "a08e850aaa44c8b56765eee10b386fc3e516a62b".into(),
        acquisition_start_time_ms: 1_700_000_000_000,
        adc_min: -4096,
        adc_max: 4095,
        sample_rate: 5000,
        flow_cell_id: "PAW00000".into(),
        sequencing_kit: "SQK-LSK114".into(),
        context_tags: [("sample_frequency".to_string(), "5000".to_string())].into(),
        ..Default::default()
    })
}

/// Two reads with deliberately wrong `num_samples` (the writer must use the
/// signal length) and one channel that only fits in `channel_32bit`.
fn scratch_reads() -> Vec<(pod5lib::ReadRecord, Vec<i16>)> {
    use pod5lib::{Calibration, EndReason, EndReasonKind, Pore, ReadRecord, uuid_from_str};
    let run_info = scratch_run_info();
    let make = |id: &str, channel: u32, n: usize| {
        let mut r = ReadRecord::new(
            uuid_from_str(id).unwrap(),
            Pore {
                channel,
                well: 2,
                pore_type: "not_set".into(),
            },
            Calibration {
                offset: -243.0,
                scale: 0.1462,
            },
            channel * 10,
            channel as u64 * 1000,
            201.5,
            EndReason {
                reason: EndReasonKind::SignalPositive,
                forced: false,
            },
            run_info.clone(),
        );
        r.num_samples = 999_999;
        r.tracked_scaling = pod5lib::ShiftScalePair {
            shift: 1.5,
            scale: 2.5,
        };
        let signal: Vec<i16> = (0..n).map(|i| (i % 2000) as i16 - 1000).collect();
        (r, signal)
    };
    vec![
        make("0000173c-bf67-44e7-9a9c-1ad0bc728e74", 7, 12_345),
        make("002fde30-9e23-4125-9eae-d112c18a81a7", 70_000, 3),
    ]
}

fn check_scratch_reads(dst: &pod5lib::Reader) {
    let expected = scratch_reads();
    assert_eq!(dst.len(), expected.len());
    assert_eq!(dst.run_infos().len(), 1);
    let ri = &dst.run_infos()[0];
    assert_eq!(ri.flow_cell_id, "PAW00000");
    assert_eq!(ri.sample_rate, 5000);
    assert_eq!(ri.context_tags["sample_frequency"], "5000");
    for (orig, signal) in &expected {
        let r = dst
            .get_read_by_id(&orig.read_id_str())
            .expect("read present");
        assert_eq!(r.pore, orig.pore);
        assert_eq!(r.read_number, orig.read_number);
        assert_eq!(r.start_sample, orig.start_sample);
        assert_eq!(r.median_before, orig.median_before);
        assert_eq!(r.calibration, orig.calibration);
        assert_eq!(r.end_reason, orig.end_reason);
        assert_eq!(r.tracked_scaling, orig.tracked_scaling);
        assert!(!r.predicted_scaling.is_available());
        assert!(r.open_pore_level.is_nan() && r.selected_read_level.is_nan());
        assert_eq!(
            r.num_samples,
            signal.len() as u64,
            "num_samples from signal"
        );
        assert_eq!(&r.signal().unwrap(), signal);
    }
}

#[test]
fn read_record_new_defaults() {
    let (r, _) = scratch_reads().remove(0);
    assert!(r.signal().unwrap().is_empty());
    assert_eq!(r.num_minknow_events, 0);
    assert_eq!(r.time_since_mux_change, 0.0);
    assert!(r.expected_open_pore_level.is_nan());
}

#[test]
fn writer_from_scratch() {
    let mut writer = pod5lib::Writer::new();
    for (r, s) in scratch_reads() {
        writer.add_read(r, s);
    }
    let mut buf = Vec::new();
    writer.write_to(&mut buf).expect("write");
    check_scratch_reads(&pod5lib::Reader::from_reader(&mut Cursor::new(buf)).unwrap());
}

#[test]
fn streaming_writer_from_scratch() {
    let path = std::env::temp_dir().join(format!("pod5lib-scratch-{}.pod5", std::process::id()));
    let mut writer = pod5lib::StreamingWriter::create(&path).expect("create");
    for (r, s) in scratch_reads() {
        writer.write_read(&r, &s).expect("write_read");
    }
    writer.finish().expect("finish");
    let dst = pod5lib::Reader::open(&path).expect("open");
    check_scratch_reads(&dst);
    drop(dst);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn uuid_from_str_round_trip() {
    let id = "0000173c-bf67-44e7-9a9c-1ad0bc728e74";
    let bytes = pod5lib::uuid_from_str(id).unwrap();
    assert_eq!(pod5lib::uuid_to_str(&bytes), id);
    assert_eq!(
        pod5lib::uuid_from_str("0000173CBF6744E79A9C1AD0BC728E74"),
        Some(bytes)
    );
    assert_eq!(pod5lib::uuid_from_str("not-a-uuid"), None);
}

// ── Writer behaviour ──────────────────────────────────────────────────────────

fn scratch_read(id: &str, signal_len: usize) -> (pod5lib::ReadRecord, Vec<i16>) {
    let (mut r, _) = scratch_reads().remove(0);
    r.read_id = pod5lib::uuid_from_str(id).unwrap();
    let signal = (0..signal_len).map(|i| (i % 4000) as i16 - 2000).collect();
    (r, signal)
}

#[test]
fn writer_long_and_empty_signals_round_trip() {
    // 250_000 samples spans three 102_400-sample signal rows; the empty read has none.
    let reads = [
        scratch_read("11111111-0000-4000-8000-000000000001", 250_000),
        scratch_read("11111111-0000-4000-8000-000000000002", 0),
        scratch_read("11111111-0000-4000-8000-000000000003", 102_400),
    ];
    for streaming in [false, true] {
        let dst = if streaming {
            let path =
                std::env::temp_dir().join(format!("pod5lib-chunks-{}.pod5", std::process::id()));
            let mut w = pod5lib::StreamingWriter::create(&path).unwrap();
            w.signal_batch_size = 2;
            for (r, s) in &reads {
                w.write_read(r, s).unwrap();
            }
            w.finish().unwrap();
            let dst = pod5lib::Reader::from_reader(&mut std::fs::File::open(&path).unwrap());
            let _ = std::fs::remove_file(&path);
            dst.unwrap()
        } else {
            let mut w = pod5lib::Writer::new();
            w.signal_batch_size = 2;
            for (r, s) in &reads {
                w.add_read(r.clone(), s.clone());
            }
            let mut buf = Vec::new();
            w.write_to(&mut buf).unwrap();
            pod5lib::Reader::from_reader(&mut Cursor::new(buf)).unwrap()
        };
        for (r, s) in &reads {
            let got = dst.get_read_by_id(&r.read_id_str()).unwrap();
            assert_eq!(got.num_samples, s.len() as u64, "streaming={streaming}");
            assert_eq!(&got.signal().unwrap(), s, "streaming={streaming}");
        }
    }
}

#[test]
fn writer_keeps_unrecognised_end_reason() {
    let (mut r, s) = scratch_read("11111111-0000-4000-8000-000000000004", 10);
    r.end_reason.reason = pod5lib::EndReasonKind::from_reason_str("some_future_reason");
    let mut w = pod5lib::Writer::new();
    w.add_read(r, s);
    let mut buf = Vec::new();
    w.write_to(&mut buf).unwrap();
    let dst = pod5lib::Reader::from_reader(&mut Cursor::new(buf)).unwrap();
    assert_eq!(
        dst.reads()[0].end_reason.reason,
        pod5lib::EndReasonKind::Other("some_future_reason".into())
    );
}

#[test]
fn streaming_writer_dropped_without_finish_leaves_invalid_file() {
    let path = std::env::temp_dir().join(format!("pod5lib-drop-{}.pod5", std::process::id()));
    {
        let mut w = pod5lib::StreamingWriter::create(&path).unwrap();
        w.signal_batch_size = 1; // start the writer thread on the first read
        for (r, s) in scratch_reads() {
            w.write_read(&r, &s).unwrap();
        }
        // dropped here without finish()
    }
    assert!(
        pod5lib::Reader::open(&path).is_err(),
        "abandoned file must not open"
    );
    let _ = std::fs::remove_file(&path);
}

#[cfg(target_os = "linux")]
#[test]
fn streaming_writer_reports_io_error() {
    // /dev/full fails every write with ENOSPC; the real error must surface, not
    // WriterThreadDied. (It may arrive wrapped by arrow, which did the write.)
    let mut w = pod5lib::StreamingWriter::create("/dev/full").unwrap();
    w.signal_batch_size = 1;
    let mut result = Ok(());
    for (r, s) in scratch_reads() {
        result = result.and_then(|_| w.write_read(&r, &s));
    }
    let err = result.and_then(|_| w.finish()).unwrap_err();
    assert!(err.to_string().contains("os error 28"), "{err}");
}

#[test]
fn write_file_validates_before_truncating() {
    let path = std::env::temp_dir().join(format!("pod5lib-keep-{}.pod5", std::process::id()));
    std::fs::write(&path, b"existing contents").unwrap();
    let mut w = pod5lib::Writer::new();
    w.pod5_version = "bad".into();
    assert!(w.write_file(&path).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), b"existing contents");
    let _ = std::fs::remove_file(&path);
}

#[test]
fn writer_signal_batch_size_zero_is_treated_as_one() {
    let mut w = pod5lib::Writer::new();
    w.signal_batch_size = 0;
    for (r, s) in scratch_reads() {
        w.add_read(r, s);
    }
    let mut buf = Vec::new();
    w.write_to(&mut buf).unwrap();
    check_scratch_reads(&pod5lib::Reader::from_reader(&mut Cursor::new(buf)).unwrap());
}

#[test]
fn writer_rejects_too_many_pore_types() {
    let (template, _) = scratch_reads().remove(0);
    let mut w = pod5lib::Writer::new();
    // Int16 keys allow 32_768 distinct values (0..=32_767); one more must fail.
    for i in 0..=32_768u32 {
        let mut r = template.clone();
        r.read_id[..4].copy_from_slice(&i.to_be_bytes());
        r.pore.pore_type = format!("pore_{i}");
        w.add_read(r, Vec::new());
    }
    let err = w.write_to(&mut Vec::new()).unwrap_err();
    assert!(
        matches!(err, pod5lib::Error::TooManyDictionaryValues("pore_type")),
        "{err}"
    );
}
