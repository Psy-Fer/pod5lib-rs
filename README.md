# pod5lib

Pure Rust library for reading and writing [POD5](https://pod5-file-format.readthedocs.io/en/latest/) nanopore sequencing files — Oxford Nanopore Technologies' successor to FAST5.

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
    println!("{} — {} samples", read.read_id_str(), adc.len());
}

// Random-access by UUID string — O(1)
if let Some(read) = reader.get_read_by_id("a1b2c3d4-0506-0708-090a-0b0c0d0e0f10") {
    println!("found: {} samples", read.num_samples);
}
```

### Writing (buffered)

```rust
use pod5lib::{Reader, Writer};

let src = Reader::open("input.pod5")?;
let mut writer = Writer::new();
writer.file_identifier = src.file_identifier().to_string();

for read in src.reads_iter().take(10) {
    let signal = read.signal()?;
    writer.add_read(read.clone(), signal);
}
writer.write_file("output.pod5")?;
```

### Writing (streaming — lower memory use)

```rust
use pod5lib::{Reader, StreamingWriter};

let src = Reader::open("input.pod5")?;
let mut writer = StreamingWriter::create("output.pod5")?;
writer.file_identifier = src.file_identifier().to_string();

for read in src.reads_iter() {
    let signal = read.signal()?;
    writer.write_read(read, &signal)?;   // signal compressed immediately, not held
}
writer.finish()?;
```

`StreamingWriter` VBZ-compresses each read's signal on `write_read` and releases it immediately, rather than accumulating raw samples for the whole file. Prefer it for large files.

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
