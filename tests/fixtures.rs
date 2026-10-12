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
