/// Tests against upstream pod5 fixtures in tests/fixtures/ (see README.md there).
///
/// Every fixture holds the same 10 reads, stored under a different Reads-table
/// version, so one reference table (values from Python `pod5` 0.3.49) checks all.
use std::io::Cursor;
use std::path::PathBuf;

use pod5lib::{Error, Reader, Writer};

fn fixture(version: u8) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(format!("tests/fixtures/multi_fast5_zip_v{version}.pod5"))
}

const SUPPORTED: [u8; 5] = [3, 4, 5, 6, 7];
const ACQUISITION_ID: &str = "a08e850aaa44c8b56765eee10b386fc3e516a62b";

struct Expected {
    read_id: &'static str,
    read_number: u32,
    channel: u32,
    well: u8,
    num_samples: u64,
    signal_sum: i64,
    first: i16,
    last: i16,
}

#[rustfmt::skip]
const EXPECTED: [Expected; 10] = [
    Expected { read_id: "0000173c-bf67-44e7-9a9c-1ad0bc728e74", read_number: 1093, channel: 109, well: 4, num_samples: 123627, signal_sum: 41634316, first: 371, last: 313 },
    Expected { read_id: "002fde30-9e23-4125-9eae-d112c18a81a7", read_number: 75, channel: 463, well: 2, num_samples: 37440, signal_sum: 12730452, first: 632, last: 373 },
    Expected { read_id: "006d1319-2877-4b34-85df-34de7250a47b", read_number: 1053, channel: 489, well: 4, num_samples: 337876, signal_sum: 126761880, first: 571, last: 710 },
    Expected { read_id: "00728efb-2120-4224-87d8-580fbb0bd4b2", read_number: 657, channel: 147, well: 2, num_samples: 161547, signal_sum: 64194513, first: 431, last: 507 },
    Expected { read_id: "007cc97e-6de2-4ff6-a0fd-1c1eca816425", read_number: 1625, channel: 126, well: 2, num_samples: 505057, signal_sum: 188470651, first: 679, last: 433 },
    Expected { read_id: "008468c3-e477-46c4-a6e2-7d021a4ebf0b", read_number: 411, channel: 2, well: 2, num_samples: 206976, signal_sum: 95221038, first: 530, last: 523 },
    Expected { read_id: "008ed3dc-86c2-452f-b107-6877a473d177", read_number: 513, channel: 474, well: 4, num_samples: 14510, signal_sum: 5187950, first: 583, last: 338 },
    Expected { read_id: "00919556-e519-4960-8aa5-c2dfa020980c", read_number: 56, channel: 199, well: 4, num_samples: 9885, signal_sum: 3849682, first: 531, last: 416 },
    Expected { read_id: "00925f34-6baf-47fc-b40c-22591e27fb5c", read_number: 930, channel: 53, well: 4, num_samples: 136370, signal_sum: 43419625, first: 434, last: 366 },
    Expected { read_id: "009dc9bd-c5f4-487b-ba4c-b9ce7e3a711e", read_number: 195, channel: 452, well: 2, num_samples: 15643, signal_sum: 4846109, first: 546, last: 286 },
];

fn check_reads(reader: &Reader, label: &str) {
    assert_eq!(reader.len(), EXPECTED.len(), "{label}: read count");
    for e in &EXPECTED {
        let r = reader
            .get_read_by_id(e.read_id)
            .unwrap_or_else(|| panic!("{label}: read {} missing", e.read_id));
        assert_eq!(r.read_number, e.read_number, "{label} {}", e.read_id);
        assert_eq!(r.pore.channel, e.channel, "{label} {}", e.read_id);
        assert_eq!(r.pore.well, e.well, "{label} {}", e.read_id);
        assert_eq!(r.num_samples, e.num_samples, "{label} {}", e.read_id);
        assert_eq!(r.run_info.acquisition_id, ACQUISITION_ID, "{label}");
        assert_eq!(r.run_info.sample_rate, 4000, "{label}");

        let signal = r.signal().expect("decompress");
        assert_eq!(signal.len() as u64, e.num_samples, "{label} {}", e.read_id);
        let sum: i64 = signal.iter().map(|&s| s as i64).sum();
        assert_eq!(sum, e.signal_sum, "{label} {}: signal sum", e.read_id);
        assert_eq!(signal[0], e.first, "{label} {}", e.read_id);
        assert_eq!(*signal.last().unwrap(), e.last, "{label} {}", e.read_id);
    }
}

#[test]
fn reads_every_supported_version() {
    for v in SUPPORTED {
        let reader = Reader::open(fixture(v)).unwrap_or_else(|e| panic!("open v{v}: {e}"));
        assert_eq!(reader.reads_table_version(), v, "detected version");
        check_reads(&reader, &format!("v{v}"));
    }
}

#[test]
fn rewrite_every_supported_version_as_v7() {
    for v in SUPPORTED {
        let src = Reader::open(fixture(v)).expect("open");
        let mut writer = Writer::new();
        for read in src.reads_iter() {
            writer.add_read(read.clone(), read.signal().expect("decompress"));
        }
        let mut buf = Vec::new();
        writer.write_to(&mut buf).expect("write");

        let dst = Reader::from_reader(&mut Cursor::new(buf)).expect("read back");
        assert_eq!(dst.reads_table_version(), 7);
        check_reads(&dst, &format!("v{v} rewritten"));
    }
}

#[test]
fn rejects_legacy_version() {
    let err = Reader::open(fixture(2)).unwrap_err();
    assert!(
        matches!(&err, Error::LegacyPod5Version(v) if v == "0.0.32"),
        "{err}"
    );
}

#[test]
fn reads_uncompressed_signal() {
    // Signal stored as LargeList<Int16> instead of VBZ: Reader::open falls back to
    // loading the Signal table eagerly.
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/multi_fast5_zip_v7_uncompressed.pod5");
    let lazy = Reader::open(&path).expect("open");
    let eager = Reader::from_reader(&mut std::fs::File::open(&path).unwrap()).expect("from_reader");
    for reader in [lazy, eager] {
        assert_eq!(reader.len(), 3);
        for r in reader.reads_iter() {
            let e = EXPECTED
                .iter()
                .find(|e| e.read_id == r.read_id_str())
                .expect("known read");
            let signal = r.signal().expect("signal");
            assert_eq!(signal.len() as u64, e.num_samples);
            assert_eq!(signal.iter().map(|&s| s as i64).sum::<i64>(), e.signal_sum);
            assert_eq!((signal[0], *signal.last().unwrap()), (e.first, e.last));
        }
        let par: Vec<_> = reader.par_reads_iter(2).collect::<Result<_, _>>().unwrap();
        assert_eq!(par.len(), 3);
    }
}

#[test]
fn reads_all_fields_fixture() {
    // Written by upstream pod5 0.3.49 (tests/fixtures/make_all_fields_fixture.py):
    // every field has a distinct value; the last read keeps NaN optional floats.
    use pod5lib::{EndReasonKind, ShiftScalePair};
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/all_fields_v7.pod5");
    let ids = [
        "00919556-e519-4960-8aa5-c2dfa020980c",
        "008ed3dc-86c2-452f-b107-6877a473d177",
        "009dc9bd-c5f4-487b-ba4c-b9ce7e3a711e",
        "0000173c-bf67-44e7-9a9c-1ad0bc728e74",
    ];
    let channels = [1u32, 65535, 65536, 70000];
    let reasons = [
        (EndReasonKind::SignalPositive, false),
        (EndReasonKind::MuxChange, true),
        (EndReasonKind::DataServiceUnblockMuxChange, true),
        (EndReasonKind::Paused, false),
    ];
    let lazy = Reader::open(&path).expect("open");
    let eager = Reader::from_reader(&mut std::fs::File::open(&path).unwrap()).unwrap();
    for reader in [lazy, eager] {
        assert_eq!(reader.reads_table_version(), 7);
        assert_eq!(reader.len(), 4);
        assert_eq!(reader.run_infos().len(), 2);
        for (i, id) in ids.iter().enumerate() {
            let r = reader.get_read_by_id(id).expect("read");
            let f = i as f32;
            let last = i == 3;
            let nan_or = |v: f32| if last { f32::NAN } else { v };
            let same = |a: f32, b: f32| (a.is_nan() && b.is_nan()) || a == b;

            assert_eq!(r.pore.channel, channels[i], "{id}");
            assert_eq!(r.pore.well, 1 + i as u8);
            assert_eq!(r.pore.pore_type, format!("pore_type_{}", i % 2));
            assert_eq!(r.calibration.offset, -240.0 - f);
            assert!((r.calibration.scale - (0.15 + f / 100.0)).abs() < 1e-6);
            assert_eq!(r.read_number, 1000 + i as u32);
            assert_eq!(r.start_sample, 10_000_000 * (i as u64 + 1));
            assert!(same(r.median_before, nan_or(200.5 + f)), "{id}");
            assert_eq!(
                (r.end_reason.reason.clone(), r.end_reason.forced),
                reasons[i]
            );
            assert_eq!(r.num_minknow_events, 100 + i as u64);
            let pair = |shift, scale| ShiftScalePair {
                shift: nan_or(shift),
                scale: nan_or(scale),
            };
            assert_eq!(r.tracked_scaling, pair(80.0 + f, 12.5 + f));
            assert_eq!(r.predicted_scaling, pair(81.0 + f, 13.5 + f));
            assert_eq!(r.num_reads_since_mux_change, 7 + i as u32);
            assert_eq!(r.time_since_mux_change, 1.25 + f);
            assert!(same(r.open_pore_level, nan_or(230.0 + f)), "{id}");
            assert!(same(r.expected_open_pore_level, nan_or(231.0 + f)), "{id}");
            assert!(same(r.selected_read_level, nan_or(90.0 + f)), "{id}");

            let ri = &r.run_info;
            if i % 2 == 0 {
                assert_eq!(ri.acquisition_id, ACQUISITION_ID);
                assert_eq!(ri.sample_rate, 4000);
            } else {
                assert_eq!(ri.acquisition_id, "all-fields-second-acquisition");
                assert_eq!(
                    (ri.adc_min, ri.adc_max, ri.sample_rate),
                    (-2048, 2047, 5000)
                );
                assert_eq!(ri.flow_cell_product_code, "FLO-PRO114M");
                assert_eq!(ri.sequencer_position_type, "PromethION");
                assert_eq!(ri.context_tags["experiment_type"], "genomic_dna");
                assert_eq!(ri.tracking_id["device_type"], "promethion");
            }

            let e = EXPECTED.iter().find(|e| e.read_id == *id).unwrap();
            let signal = r.signal().unwrap();
            assert_eq!(signal.len() as u64, e.num_samples);
            assert_eq!(signal.iter().map(|&s| s as i64).sum::<i64>(), e.signal_sum);
        }
    }
}

/// Every public field of two reads (and their run infos) is the same.
fn assert_same_read(a: &pod5lib::ReadRecord, b: &pod5lib::ReadRecord) {
    let id = a.read_id_str();
    let same = |x: f32, y: f32| (x.is_nan() && y.is_nan()) || x == y;
    assert_eq!(a.read_id, b.read_id);
    assert_eq!(a.read_number, b.read_number, "{id}");
    assert_eq!(a.pore, b.pore, "{id}");
    assert_eq!(a.calibration, b.calibration, "{id}");
    assert_eq!(a.end_reason, b.end_reason, "{id}");
    assert!(same(a.median_before, b.median_before), "{id}");
    assert_eq!(a.num_samples, b.num_samples, "{id}");
    assert_eq!(a.start_sample, b.start_sample, "{id}");
    assert_eq!(a.tracked_scaling, b.tracked_scaling, "{id}");
    assert_eq!(a.predicted_scaling, b.predicted_scaling, "{id}");
    assert_eq!(a.num_minknow_events, b.num_minknow_events, "{id}");
    assert_eq!(
        a.num_reads_since_mux_change, b.num_reads_since_mux_change,
        "{id}"
    );
    assert!(
        same(a.time_since_mux_change, b.time_since_mux_change),
        "{id}"
    );
    assert!(same(a.open_pore_level, b.open_pore_level), "{id}");
    assert!(
        same(a.expected_open_pore_level, b.expected_open_pore_level),
        "{id}"
    );
    assert!(same(a.selected_read_level, b.selected_read_level), "{id}");
    let (ra, rb) = (&a.run_info, &b.run_info);
    assert_eq!(ra.acquisition_id, rb.acquisition_id);
    assert_eq!(ra.acquisition_start_time_ms, rb.acquisition_start_time_ms);
    assert_eq!((ra.adc_min, ra.adc_max), (rb.adc_min, rb.adc_max));
    assert_eq!(ra.context_tags, rb.context_tags);
    assert_eq!(ra.experiment_name, rb.experiment_name);
    assert_eq!(ra.flow_cell_id, rb.flow_cell_id);
    assert_eq!(ra.flow_cell_product_code, rb.flow_cell_product_code);
    assert_eq!(ra.protocol_name, rb.protocol_name);
    assert_eq!(ra.protocol_run_id, rb.protocol_run_id);
    assert_eq!(ra.protocol_start_time_ms, rb.protocol_start_time_ms);
    assert_eq!(ra.sample_id, rb.sample_id);
    assert_eq!(ra.sample_rate, rb.sample_rate);
    assert_eq!(ra.sequencing_kit, rb.sequencing_kit);
    assert_eq!(ra.sequencer_position, rb.sequencer_position);
    assert_eq!(ra.sequencer_position_type, rb.sequencer_position_type);
    assert_eq!(ra.software, rb.software);
    assert_eq!(ra.system_name, rb.system_name);
    assert_eq!(ra.system_type, rb.system_type);
    assert_eq!(ra.tracking_id, rb.tracking_id);
    assert_eq!(a.signal().unwrap(), b.signal().unwrap(), "{id}");
}

#[test]
fn rewrite_all_fields_fixture_with_both_writers() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/all_fields_v7.pod5");
    let src = Reader::open(&path).unwrap();

    let mut buffered = Writer::new();
    for r in src.reads_iter() {
        buffered.add_read(r.clone(), r.signal().unwrap());
    }
    let mut buf = Vec::new();
    buffered.write_to(&mut buf).unwrap();

    let tmp = std::env::temp_dir().join(format!("pod5lib-allfields-{}.pod5", std::process::id()));
    let mut streaming = pod5lib::StreamingWriter::create(&tmp).unwrap();
    streaming.signal_batch_size = 1;
    for r in src.reads_iter() {
        streaming.write_read(r, &r.signal().unwrap()).unwrap();
    }
    streaming.finish().unwrap();

    let outputs = [
        Reader::from_reader(&mut Cursor::new(buf)).unwrap(),
        Reader::open(&tmp).unwrap(),
    ];
    for dst in &outputs {
        assert_eq!(dst.len(), src.len());
        assert_eq!(dst.run_infos().len(), 2);
        for a in src.reads_iter() {
            assert_same_read(a, dst.get_read_by_id(&a.read_id_str()).unwrap());
        }
    }
    drop(outputs);
    let _ = std::fs::remove_file(&tmp);
}
