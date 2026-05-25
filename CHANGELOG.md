# Changelog

All notable changes to pod5lib will be documented here.

## [0.1.0] - 2026-05-23

Initial release.

### Features

- **Reading** - sequential iteration and O(1) random access by UUID string
- **Parallel reading** - `Reader::par_reads_iter(threads)`: batches compressed blobs in a single I/O pass, then decompresses concurrently with a rayon thread pool
- **Writing** - `Writer` (buffered, all reads in memory) and `StreamingWriter` (one read at a time, bounded RSS)
- **VBZ codec** - fused single-pass SVB16 + zigzag + delta decode/encode with SIMD acceleration via [`svb`](https://crates.io/crates/svb) (SSSE3 on x86-64, NEON on aarch64, scalar fallback)
- **Calibration** - `ReadRecord::signal_pa()` converts raw ADC samples to picoamps
- **Full metadata** - `RunInfo`, `Pore`, `Calibration`, `EndReason`, scaling pairs

### Compatibility

- Tested against Python `pod5` **0.3.39**; files produced by either library are fully readable by the other
- POD5 format: files with `pod5_version` strings in the `0.3.x` series are fully supported
- Rust minimum version: **1.87**
- Platforms: Linux x86-64, Linux ARM64, Windows x86-64

### Performance (500k reads, 24.7 GB file, 64-core server)

| Operation | pod5lib-rs | Python pod5 |
|---|---|---|
| Sequential read | 54.6 s, 300 MB RSS | 58.0 s, 23,769 MB RSS |
| Parallel read (16 threads) | 31.1 s, 359 MB RSS | n/a |
| Write (50k reads) | 12.7 s, 525 MB RSS | 21.7 s, 2,531 MB RSS |
