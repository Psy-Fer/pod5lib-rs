# pod5lib

Rust library for reading and writing [POD5](https://pod5-file-format.readthedocs.io/en/latest/)
nanopore sequencing files, the successor to FAST5 from Oxford Nanopore Technologies.
Files are interchangeable with ONT's own `pod5` library.

## Features

- **Read** POD5 files: iterate reads, random access by read ID, parallel signal decompression
- **Write** POD5 files compatible with the Python `pod5` library, buffered (`Writer`) or streaming (`StreamingWriter`)
- **Build reads from scratch** with `ReadRecord::new`, e.g. when converting from FAST5/BLOW5
- Reads every POD5 written since pod5 0.0.38 (Reads table V3–V7); writes V7
- Corrupt or truncated files return errors rather than panicking or exhausting memory
- No `unsafe` code in this crate

## Installation

Add to `Cargo.toml`:

```toml
[dependencies]
pod5lib = "0.2"
```

Upgrading from 0.1? See the
[changelog](https://github.com/Psy-Fer/pod5lib-rs/blob/main/CHANGELOG.md#upgrading-from-01).
Files written by 0.1 cannot be opened by current versions of Python `pod5`; read and
rewrite them with 0.2.

## Quick example

```rust,no_run
use pod5lib::Reader;

let reader = Reader::open("sample.pod5")?;
println!("{} reads", reader.len());

for read in reader.reads_iter() {
    let adc = read.signal()?;        // raw i16 ADC samples
    let pa  = read.signal_pa()?;     // calibrated picoamps
    println!("{}: {} samples", read.read_id_str(), adc.len());
}
# Ok::<(), pod5lib::Error>(())
```

## API overview

| Type | Purpose |
|---|---|
| [`Reader`](https://docs.rs/pod5lib/latest/pod5lib/struct.Reader.html) | Open and query a POD5 file |
| [`ReadRecord`](https://docs.rs/pod5lib/latest/pod5lib/struct.ReadRecord.html) | Metadata and on-demand signal for one read |
| [`RunInfo`](https://docs.rs/pod5lib/latest/pod5lib/struct.RunInfo.html) | Per-acquisition run context |
| [`Writer`](https://docs.rs/pod5lib/latest/pod5lib/struct.Writer.html) | Buffered writer (all reads in memory) |
| [`StreamingWriter`](https://docs.rs/pod5lib/latest/pod5lib/struct.StreamingWriter.html) | Streaming writer (bounded memory) |

## Performance

Measured with pod5lib 0.1.0 on a 500,000-read, 24.7 GB POD5 file on a 64-core server,
against the Python `pod5` library.

| Operation | Rust | Python | Speed |
|---|---|---|---|
| Read (sequential) | 54.6 s, 300 MB RSS | 58.0 s, 23,769 MB RSS | parity |
| Read (parallel, 16 threads) | **31.1 s**, 359 MB RSS | 58.0 s, 23,769 MB RSS | **1.9× faster** |
| Write (50k reads) | **12.7 s**, 525 MB RSS | 21.7 s, 2,531 MB RSS | **1.7× faster** |

Python's read RSS is mostly the memory-mapped file, not decompressed signal: Python
`pod5` maps the file and RSS counts the mapped pages it has touched, which the kernel
can reclaim. pod5lib uses ordinary file reads, so the same pages are in the page cache
but not in its RSS. The RSS columns are therefore not a like-for-like memory comparison.
