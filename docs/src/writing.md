# Writing

Two writers are available. Both produce files fully compatible with the Python `pod5` library.

## Writer (buffered)

`Writer` accumulates all reads in memory before writing the file.
Use it when the full dataset fits comfortably in RAM.

```rust,no_run
use pod5lib::{Reader, Writer};

let src = Reader::open("input.pod5")?;
let mut writer = Writer::new();

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

for read in src.reads_iter() {
    let signal = read.signal()?;
    writer.write_read(read, &signal)?;   // compressed immediately, not held
}

writer.finish()?;
# Ok::<(), pod5lib::Error>(())
```

> `finish()` must be called to flush the Arrow IPC sections and write the FlatBuffers footer.
> Dropping the writer without calling `finish()` will leave an incomplete file.

## Building reads from scratch

To write data that did not come from a POD5 file (for example when converting from
FAST5 or BLOW5), build each read with `ReadRecord::new`. It takes the same required
fields as Python `pod5.Read`; the remaining fields take Python's defaults and can be
set afterwards. The writer sets `num_samples` from the signal you pass it.

```rust,no_run
use std::sync::Arc;
use pod5lib::{
    Calibration, EndReason, EndReasonKind, Pore, ReadRecord, RunInfo, StreamingWriter,
    uuid_from_str,
};

let run_info = Arc::new(RunInfo {
    acquisition_id: "a08e850aaa44c8b56765eee10b386fc3e516a62b".into(),
    adc_min: -4096,
    adc_max: 4095,
    sample_rate: 5000,
    flow_cell_id: "PAW00000".into(),
    ..Default::default()
});

let mut writer = StreamingWriter::create("output.pod5")?;
let mut read = ReadRecord::new(
    uuid_from_str("0000173c-bf67-44e7-9a9c-1ad0bc728e74").unwrap(),
    Pore { channel: 109, well: 1, pore_type: "not_set".into() },
    Calibration { offset: -243.0, scale: 0.1462 },
    1093,      // read_number
    4_096_000, // start_sample
    201.5,     // median_before
    EndReason { reason: EndReasonKind::SignalPositive, forced: false },
    Arc::clone(&run_info),
);
read.num_minknow_events = 42; // optional fields are public

let signal: Vec<i16> = vec![512, 498, 503, 520];
writer.write_read(&read, &signal)?;
writer.finish()?;
# Ok::<(), pod5lib::Error>(())
```

Share one `Arc<RunInfo>` between all reads from the same acquisition; the writer
stores each distinct `acquisition_id` once. A full example is in
`examples/write_from_scratch.rs`.

## Fields

Both writers expose the same public fields:

| Field | Default | Description |
|---|---|---|
| `file_identifier` | random v4 UUID | File UUID written to the footer and schema metadata. Upstream gives every new file a fresh one; any UUID form may be set, and it is written in canonical hyphenated form (`Error::InvalidFileIdentifier` if it is not a UUID) |
| `software` | `"pod5lib-rs <version> (pod5 <compat>)"` | Free-text software string written to the footer and Arrow schema metadata; not interpreted by readers (Python: `Reader.writing_software`) |
| `pod5_version` | `POD5_COMPAT_VERSION` | Upstream `pod5` version written to the footer and schema metadata. Upstream parses it: must be `X.Y.Z` and ≥ `0.0.38`, otherwise writing fails with `Error::InvalidPod5Version`. Don't copy it from a source file; the written Reads table is always V7 |

## Round-trip example

```rust,no_run
use pod5lib::{Reader, StreamingWriter};

let src = Reader::open("input.pod5")?;
let mut writer = StreamingWriter::create("output.pod5")?;

for read in src.reads_iter() {
    writer.write_read(read, &read.signal()?)?;
}

writer.finish()?;
# Ok::<(), pod5lib::Error>(())
```
