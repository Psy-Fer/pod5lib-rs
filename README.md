# pod5lib

[![Crates.io](https://img.shields.io/crates/v/pod5lib.svg)](https://crates.io/crates/pod5lib)
[![docs.rs](https://docs.rs/pod5lib/badge.svg)](https://docs.rs/pod5lib)
[![CI](https://github.com/Psy-Fer/pod5lib-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/Psy-Fer/pod5lib-rs/actions/workflows/ci.yml)
[![License: MPL-2.0](https://img.shields.io/badge/license-MPL--2.0-blue.svg)](https://opensource.org/licenses/MPL-2.0)
[![Rust 1.87+](https://img.shields.io/badge/rust-1.87%2B-orange.svg)](https://www.rust-lang.org)

Pure Rust library for reading and writing [POD5](https://pod5-file-format.readthedocs.io/en/latest/) nanopore sequencing files, the successor to FAST5 from Oxford Nanopore Technologies.

## Features

- Read POD5 files: iterate reads, random-access by UUID, decompress VBZ signal
- Write POD5 files compatible with the Python `pod5` library
- Streaming writer (`StreamingWriter`) for memory-efficient writing of large files
- No unsafe code; pure Rust VBZ decompression (delta + zigzag + SVB16 + zstd)

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
    writer.write_read(read, &signal)?;   // signal compressed immediately, not held
}
writer.finish()?;
```

`StreamingWriter` VBZ-compresses each read's signal on `write_read` and releases it immediately, rather than accumulating raw samples for the whole file. Prefer it for large files.

## Performance

Benchmarked on a 500,000-read / 24.7 GB POD5 file, 64-core server, compared against the Python `pod5` library.

| Operation | Rust | Python | Ratio |
|---|---|---|---|
| Read (sequential) | 54.6 s, **300 MB** RSS | 58.0 s, 23,769 MB RSS | speed parity, **79× less RAM** |
| Read (parallel, 16 threads) | **31.1 s**, **359 MB** RSS | 58.0 s, 23,769 MB RSS | **1.9× faster**, **66× less RAM** |
| Write (50k reads) | **12.7 s**, **525 MB** RSS | 21.7 s, 2,531 MB RSS | **1.7× faster**, **5× less RAM** |

Python's RSS for the sequential read case (~24 GB) matches the uncompressed file size. The library accumulates decompressed signal arrays in memory throughout iteration. Rust decompresses each read's signal on demand and discards it immediately, keeping RSS constant at ~300 MB regardless of file size.

`Reader::par_reads_iter(threads)` decompresses signal in parallel: blobs are gathered under a single I/O lock acquisition per batch, then decompressed concurrently with a rayon thread pool of the specified size. `StreamingWriter` similarly compresses signal in parallel rayon batches before streaming to a background writer thread.

## Compatibility

Tested against Python `pod5` **0.3.39** and **0.3.49**. Files produced by either library are fully readable by the other. POD5 files with `pod5_version` strings in the `0.3.x` series are fully supported.

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

Integration tests against a real POD5 file:

```bash
mkdir -p tests/data
cp your_file.pod5 tests/data/sample.pod5
cargo ci-test
```

Tests that require `tests/data/sample.pod5` are silently skipped when the file is absent.
