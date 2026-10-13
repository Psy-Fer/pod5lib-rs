# Reading

## Opening a file

```rust,no_run
use pod5lib::Reader;

let reader = Reader::open("sample.pod5")?;
println!("{} reads", reader.len());
println!("written by pod5 {}", reader.pod5_version());
println!("Reads table V{}", reader.reads_table_version());
println!("file ID: {}", reader.file_identifier());
# Ok::<(), pod5lib::Error>(())
```

`Reader::open` loads the Reads and RunInfo tables and builds an index for O(1) lookup
by read ID. Signal stays on disk: each call to `read.signal()` or `read.signal_pa()` reads
and decompresses that read's signal. `Reader::from_reader` reads from any `Read + Seek`
source and loads the Signal table into memory as well.

Any POD5 file written by pod5 0.0.38 or later (Reads table V3–V7) can be read. Fields a
file's version lacks (for example `expected_open_pore_level` before V5) are NaN. Corrupt
or truncated files return an error such as `Error::Malformed`.

## Iterating over reads

```rust,no_run
use pod5lib::Reader;

let reader = Reader::open("sample.pod5")?;

for read in reader.reads_iter() {
    // Metadata is available immediately
    println!(
        "{} ch={} well={} samples={}",
        read.read_id_str(),
        read.pore.channel,
        read.pore.well,
        read.num_samples,
    );

    // Signal is decompressed on demand
    let adc: Vec<i16> = read.signal()?;
    let pa:  Vec<f32> = read.signal_pa()?;
}
# Ok::<(), pod5lib::Error>(())
```

## Random access by UUID

Lookup by UUID string is O(1) via a pre-built `HashMap`.

```rust,no_run
use pod5lib::Reader;

let reader = Reader::open("sample.pod5")?;

if let Some(read) = reader.get_read_by_id("a1b2c3d4-0506-0708-090a-0b0c0d0e0f10") {
    let pa = read.signal_pa()?;
    println!("{} samples at {} Hz", pa.len(), read.run_info.sample_rate);
}
# Ok::<(), pod5lib::Error>(())
```

## Parallel decompression

`par_reads_iter(threads)` yields `(read, signal)` pairs in file order, decompressing
batches of reads on a thread pool:

```rust,no_run
use pod5lib::Reader;

let reader = Reader::open("sample.pod5")?;
for item in reader.par_reads_iter(8) {
    let (read, signal) = item?;
    println!("{}: {} samples", read.read_id_str(), signal.len());
}
# Ok::<(), pod5lib::Error>(())
```

## Signal calibration

Raw ADC values are converted to picoamps using per-read calibration parameters:

```text
pA = (adc + calibration.offset) * calibration.scale
```

`read.signal_pa()` applies this automatically. To apply it manually:

```rust,no_run
# use pod5lib::Reader;
# let reader = Reader::open("sample.pod5")?;
# let read = reader.reads_iter().next().unwrap();
let adc = read.signal()?;
let pa  = read.calibration.to_pa(&adc);
# Ok::<(), pod5lib::Error>(())
```

## Run info

Each read links to a `RunInfo` entry (typically one per file) containing acquisition metadata:

```rust,no_run
# use pod5lib::Reader;
# let reader = Reader::open("sample.pod5")?;
# let read = reader.reads_iter().next().unwrap();
let ri = &read.run_info;
println!("sample_rate={} flow_cell={}", ri.sample_rate, ri.flow_cell_id);
println!("experiment={} kit={}", ri.experiment_name, ri.sequencing_kit);
# Ok::<(), pod5lib::Error>(())
```

`reader.run_infos()` returns all `RunInfo` entries in the file if you need them independently.
