# Reading

## Opening a file

```rust,no_run
use pod5::Reader;

let reader = Reader::open("sample.pod5")?;
println!("{} reads", reader.len());
println!("POD5 version: {}", reader.pod5_version());
println!("File ID: {}", reader.file_identifier());
# Ok::<(), pod5::Error>(())
```

`Reader::open` loads all three Arrow tables into memory and builds an index for O(1) UUID lookup.
Signal data is not decompressed until you call `read.signal()` or `read.signal_pa()`.

## Iterating over reads

```rust,no_run
use pod5::Reader;

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
# Ok::<(), pod5::Error>(())
```

## Random access by UUID

Lookup by UUID string is O(1) via a pre-built `HashMap`.

```rust,no_run
use pod5::Reader;

let reader = Reader::open("sample.pod5")?;

if let Some(read) = reader.get_read_by_id("a1b2c3d4-0506-0708-090a-0b0c0d0e0f10") {
    let pa = read.signal_pa()?;
    println!("{} samples at {} Hz", pa.len(), read.run_info.sample_rate);
}
# Ok::<(), pod5::Error>(())
```

## Signal calibration

Raw ADC values are converted to picoamps using per-read calibration parameters:

```
pA = (adc + calibration.offset) * calibration.scale
```

`read.signal_pa()` applies this automatically. To apply it manually:

```rust,no_run
# use pod5::Reader;
# let reader = Reader::open("sample.pod5")?;
# let read = reader.reads_iter().next().unwrap();
let adc = read.signal()?;
let pa  = read.calibration.to_pa(&adc);
# Ok::<(), pod5::Error>(())
```

## Run info

Each read links to a `RunInfo` entry (typically one per file) containing acquisition metadata:

```rust,no_run
# use pod5::Reader;
# let reader = Reader::open("sample.pod5")?;
# let read = reader.reads_iter().next().unwrap();
let ri = &read.run_info;
println!("sample_rate={} flow_cell={}", ri.sample_rate, ri.flow_cell_id);
println!("experiment={} kit={}", ri.experiment_name, ri.sequencing_kit);
# Ok::<(), pod5::Error>(())
```

`reader.run_infos()` returns all `RunInfo` entries in the file if you need them independently.
