# pod5

Pure Rust library for reading and writing [POD5](https://pod5-file-format.readthedocs.io/en/latest/)
nanopore sequencing files — Oxford Nanopore Technologies' successor to FAST5.

## Features

- **Read** POD5 files: iterate over reads, random-access by UUID, decompress VBZ-encoded signal
- **Write** POD5 files compatible with the Python `pod5` library
- **Streaming writer** (`StreamingWriter`) for memory-efficient writing of large files —
  signal is compressed immediately per read, not accumulated
- No unsafe code; pure Rust VBZ decompression (delta + zigzag + SVB16 + zstd)

## Performance

Benchmarked on a 4,282-read / 733 MB POD5 file (sequential scan, `--release`), compared against the Python `pod5` library.

| Operation | Rust | Python | Ratio |
|---|---|---|---|
| Read (sequential) | ~1,360 ms, **31 MB** RSS | ~1,370 ms, 797 MB RSS | speed parity, **26× less RAM** |
| Write (copy) | ~2,240 ms, **305 MB** RSS | ~4,035 ms, 885 MB RSS | **1.8× faster**, **3× less RAM** |

`StreamingWriter` compresses signal in parallel rayon batches and streams compressed blobs to a background writer thread, keeping peak RSS bounded for arbitrarily large files.

## Installation

Add to `Cargo.toml`:

```toml
[dependencies]
pod5 = { git = "https://github.com/Psy-Fer/pod5lib-rs" }
```

## Quick example

```rust,no_run
use pod5::Reader;

let reader = Reader::open("sample.pod5")?;
println!("{} reads", reader.len());

for read in reader.reads_iter() {
    let adc = read.signal()?;        // raw i16 ADC samples
    let pa  = read.signal_pa()?;     // calibrated picoamps
    println!("{} — {} samples", read.read_id_str(), adc.len());
}
# Ok::<(), pod5::Error>(())
```

## API overview

| Type | Purpose |
|---|---|
| [`Reader`](https://docs.rs/pod5) | Open and query a POD5 file |
| [`ReadRecord`](https://docs.rs/pod5) | Metadata and lazy signal for one read |
| [`RunInfo`](https://docs.rs/pod5) | Per-acquisition run context |
| [`Writer`](https://docs.rs/pod5) | Buffered writer (all reads in memory) |
| [`StreamingWriter`](https://docs.rs/pod5) | Streaming writer (one read at a time) |
