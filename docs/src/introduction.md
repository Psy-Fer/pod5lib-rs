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

Benchmarked on a 500,000-read / 24.7 GB POD5 file, 64-core server, compared against the Python `pod5` library.

| Operation | Rust | Python | Ratio |
|---|---|---|---|
| Read — sequential | 54.6 s, **300 MB** RSS | 58.0 s, 23,769 MB RSS | speed parity, **79× less RAM** |
| Read — parallel (16 threads) | **31.1 s**, **359 MB** RSS | 58.0 s, 23,769 MB RSS | **1.9× faster**, **66× less RAM** |
| Write (50k reads) | **12.7 s**, **525 MB** RSS | 21.7 s, 2,531 MB RSS | **1.7× faster**, **5× less RAM** |

Python accumulates decompressed signal in RAM throughout iteration (~24 GB for a 24 GB file). Rust decompresses each read's signal on demand and discards it immediately, keeping RSS constant at ~300 MB regardless of file size.

[`Reader::par_reads_iter(threads)`](https://docs.rs/pod5lib) decompresses signal in parallel using a rayon thread pool of the specified size. [`StreamingWriter`](https://docs.rs/pod5lib) compresses signal in parallel rayon batches and streams to a background writer thread.

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
