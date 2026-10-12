# Writing

Two writers are available. Both produce files fully compatible with the Python `pod5` library.

## Writer (buffered)

`Writer` accumulates all reads in memory before writing the file.
Use it when the full dataset fits comfortably in RAM.

```rust,no_run
use pod5lib::{Reader, Writer};

let src = Reader::open("input.pod5")?;
let mut writer = Writer::new();
writer.file_identifier = src.file_identifier().to_string();

for read in src.reads_iter().take(10) {
    let signal = read.signal()?;
    writer.add_read(read.clone(), signal);
}

writer.write_file("output.pod5")?;
# Ok::<(), pod5lib::Error>(())
```

## StreamingWriter (memory-efficient)

`StreamingWriter` VBZ-compresses each read's signal immediately on `write_read`, then
releases the raw samples. It never holds the full decompressed signal for the whole file.
Prefer this for large files.

```rust,no_run
use pod5lib::{Reader, StreamingWriter};

let src = Reader::open("input.pod5")?;
let mut writer = StreamingWriter::create("output.pod5")?;
writer.file_identifier = src.file_identifier().to_string();

for read in src.reads_iter() {
    let signal = read.signal()?;
    writer.write_read(read, &signal)?;   // compressed immediately, not held
}

writer.finish()?;
# Ok::<(), pod5lib::Error>(())
```

> `finish()` must be called to flush the Arrow IPC sections and write the FlatBuffers footer.
> Dropping the writer without calling `finish()` will leave an incomplete file.

## Fields

Both writers expose the same public fields:

| Field | Default | Description |
|---|---|---|
| `file_identifier` | `""` | UUID string to embed in the file (copy from source file if rewriting) |
| `software` | `"pod5lib-rs <version> (pod5 <compat>)"` | Free-text software string written to the footer and Arrow schema metadata; not interpreted by readers (Python: `Reader.writing_software`) |
| `pod5_version` | `POD5_COMPAT_VERSION` | Upstream `pod5` version written to the footer and schema metadata. Upstream parses it: must be `X.Y.Z` and ≥ `0.0.38`, otherwise writing fails with `Error::InvalidPod5Version`. Don't copy it from a source file; the written Reads table is always V7 |

## Round-trip example

```rust,no_run
use pod5lib::{Reader, StreamingWriter};

let src = Reader::open("input.pod5")?;
let mut writer = StreamingWriter::create("output.pod5")?;
writer.file_identifier = src.file_identifier().to_string();

for read in src.reads_iter() {
    writer.write_read(read, &read.signal()?)?;
}

writer.finish()?;
# Ok::<(), pod5lib::Error>(())
```
