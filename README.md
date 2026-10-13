# pod5lib

[![Crates.io](https://img.shields.io/crates/v/pod5lib.svg)](https://crates.io/crates/pod5lib)
[![docs.rs](https://docs.rs/pod5lib/badge.svg)](https://docs.rs/pod5lib)
[![CI](https://github.com/Psy-Fer/pod5lib-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/Psy-Fer/pod5lib-rs/actions/workflows/ci.yml)
[![License: MPL-2.0](https://img.shields.io/badge/license-MPL--2.0-blue.svg)](https://opensource.org/licenses/MPL-2.0)
[![Rust 1.87+](https://img.shields.io/badge/rust-1.87%2B-orange.svg)](https://www.rust-lang.org)

Rust library for reading and writing [POD5](https://pod5-file-format.readthedocs.io/en/latest/) nanopore sequencing files, the successor to FAST5 from Oxford Nanopore Technologies. Files are interchangeable with ONT's own `pod5` library.

```toml
[dependencies]
pod5lib = "0.2"
```

Upgrading from 0.1? See the [changelog](CHANGELOG.md#upgrading-from-01). Files written by 0.1 cannot be opened by current versions of Python `pod5`; read and rewrite them with 0.2.

## Features

- Read POD5 files: iterate reads, random access by read ID, parallel signal decompression
- Write POD5 files compatible with the Python `pod5` library, buffered (`Writer`) or streaming (`StreamingWriter`)
- Build reads from scratch with `ReadRecord::new` (e.g. converting from FAST5/BLOW5)
- Reads every POD5 written since pod5 0.0.38 (Reads table V3–V7); writes V7
- Corrupt or truncated files return errors rather than panicking or exhausting memory
- No `unsafe` code in this crate; SVB16 encoding and decoding use the SIMD [`svb`](https://crates.io/crates/svb) crate, and zstd uses the `zstd` crate (which builds the C zstd library)

## Quick start

### Reading

```rust
use pod5lib::Reader;

let reader = Reader::open("sample.pod5")?;
println!("{} reads", reader.len());

for read in reader.reads_iter() {
    let adc = read.signal()?;          // raw i16 ADC samples (VBZ decompressed)
    let pa  = read.signal_pa()?;       // calibrated picoamps (f32)
    println!("{}: {} samples", read.read_id_str(), adc.len());
}

// Random access by UUID string (O(1))
if let Some(read) = reader.get_read_by_id("a1b2c3d4-0506-0708-090a-0b0c0d0e0f10") {
    println!("found: {} samples", read.num_samples);
}
```

### Writing (buffered)

```rust
use pod5lib::{Reader, Writer};

let src = Reader::open("input.pod5")?;
let mut writer = Writer::new();

for read in src.reads_iter().take(10) {
    let signal = read.signal()?;
    writer.add_read(read.clone(), signal);
}
writer.write_file("output.pod5")?;
```

### Writing (streaming, lower memory use)

```rust
use pod5lib::{Reader, StreamingWriter};

let src = Reader::open("input.pod5")?;
let mut writer = StreamingWriter::create("output.pod5")?;

for read in src.reads_iter() {
    let signal = read.signal()?;
    writer.write_read(read, &signal)?;
}
writer.finish()?;   // required: a writer dropped without finish() leaves an invalid file
```

`StreamingWriter` buffers `signal_batch_size` reads (default 64), compresses them in parallel, and hands them to a background thread that writes them out, so memory use stays roughly constant however large the file. `Writer` keeps every read in memory until `write_file`. Prefer `StreamingWriter` for large files.

## Performance

Measured with pod5lib **0.2.0** on a 500,000-read, 24.7 GB POD5 file on a 64-core server, against the Python `pod5` library.

| Operation | Rust | Python | Speed |
|---|---|---|---|
| Read (sequential) | 49.2 s, 315 MB RSS | 51.4 s, 23,713 MB RSS | parity |
| Read (parallel, 16 threads) | **26.5 s**, 370 MB RSS | 51.4 s, 23,713 MB RSS | **1.9× faster** |
| Write (50k reads) | **10.3 s**, 397 MB RSS | 20.3 s, 2,540 MB RSS | **2.0× faster** |

Timings are from a second run, with the file in the page cache.

Python's read RSS is not decompressed signal piling up: Python `pod5` memory-maps the file, and RSS counts the mapped file pages it has touched, which the kernel can reclaim. Iterating every signal in a 733 MB file with Python `pod5` 0.3.49 ends with 787 MB RSS, of which 746 MB is the mapped file and 41 MB is the process's own memory. pod5lib reads signal with ordinary file reads, so the same pages sit in the page cache but not in its RSS. The RSS figures are therefore not a like-for-like memory comparison.

`Reader::par_reads_iter(threads)` decompresses signal in parallel: blobs are gathered under a single I/O lock acquisition per batch, then decompressed concurrently on a rayon thread pool of the given size.

## Compatibility

Validated against Python `pod5` **0.3.49** (`pod5lib::POD5_COMPAT_VERSION`) and **0.3.39**; CI checks both, field by field, in each direction.

- **Reading:** any POD5 file written by `pod5` 0.0.38 or later (Reads table V3–V7). Older files fail with `Error::LegacyPod5Version`; convert them with upstream `pod5 update`.
- **Writing:** pod5lib writes Reads table V7, as `pod5` 0.3.48+ does. Older `pod5` versions can still read these files, but releases before 0.3.48 see channels above 65535 as 0, and releases before 0.3.44 do not see `expected_open_pore_level` / `selected_read_level`.

The POD5 format is specified and maintained by Oxford Nanopore Technologies at [github.com/nanoporetech/pod5-file-format](https://github.com/nanoporetech/pod5-file-format).

## POD5 format

POD5 files contain three [Arrow IPC](https://arrow.apache.org/docs/format/IPC.html) tables embedded in a binary container with a [FlatBuffers](https://flatbuffers.dev/) footer:

| Table | Key columns |
|---|---|
| Reads | `read_id`, `signal` (row indices into Signal table), calibration, pore, end reason, run_info |
| Signal | `read_id`, `signal` (VBZ-compressed blob), `samples` |
| RunInfo | `acquisition_id`, `sample_rate`, timestamps, context tags, tracking ID |

Signal is compressed with VBZ: delta-encode → zigzag-encode → SVB16 → zstd.

## Dependencies

| Crate | Purpose |
|---|---|
| `arrow` (ipc feature) | Arrow IPC reading and writing |
| `zstd` | zstd compression/decompression for VBZ signal |
| `svb` | SIMD SVB16 encode/decode for VBZ signal |
| `rayon` | Parallel signal compression and decompression |
| `uuid` | Random `file_identifier` generation and UUID parsing |
| `thiserror` | Error derive |

## Acknowledgements

The POD5 format is designed and developed by [Oxford Nanopore Technologies](https://nanoporetech.com/).
The reference implementation and format specification are available at
[github.com/nanoporetech/pod5-file-format](https://github.com/nanoporetech/pod5-file-format)
under the Mozilla Public License v2.0.

`pod5lib-rs` is an independent clean-room Rust implementation of the format and is not
affiliated with or endorsed by Oxford Nanopore Technologies.

## AI assistance

This library was developed with AI assistance (Claude). Architecture decisions, wire-compatibility validation, and algorithm choices are the author's own; AI tooling served as an accelerator over existing skill.

## License

This project is licensed under the [Mozilla Public License v2.0](LICENSE), consistent
with the ONT reference implementation.

## Development

```bash
cargo fmt                  # format
cargo ci-fmt               # format check (CI)
cargo ci-clippy            # clippy -D warnings (CI)
cargo ci-test              # all tests including doc-tests (CI)
```

Tests run against the upstream fixtures in `tests/fixtures/`. To also run the integration tests against your own file:

```bash
mkdir -p tests/data
cp your_file.pod5 tests/data/sample.pod5
cargo ci-test
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the Python interop checks.
