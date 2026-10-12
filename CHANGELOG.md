# Changelog

All notable changes to pod5lib will be documented here.

## [Unreleased]

Brings pod5lib in line with Python `pod5` **0.3.49** (Reads table V7).

### Fixed

- The crate did not build on its stated minimum Rust version (1.87): it used `let` chains, stable only from 1.88. CI now builds and tests on 1.87.
- Writers now take `num_samples` from the signal they are given rather than from the record, so the Reads and Signal tables cannot disagree.
- **Files written by pod5lib could not be opened by Python `pod5` ≥ 0.3.47** ("Invalid footer found in file"). The FlatBuffers footer is now padded to a multiple of 8 bytes and its `int64` fields are 8-byte aligned, matching upstream's builder.
- Files with a V6 Reads table (`channel` as `UInt32`, written by `pod5` 0.3.46–0.3.47) failed to open with a type mismatch.

### Added

- `ReadRecord::new`: build reads from scratch (e.g. when converting from FAST5/BLOW5) with the same required fields as Python `pod5.Read`; other fields take Python's defaults. See `examples/write_from_scratch.rs`.
- `uuid_from_str`: parse a read ID string into the 16 bytes stored in POD5 (inverse of `uuid_to_str`).
- `RunInfo` implements `Default`, for struct update syntax.
- `Error::LegacyPod5Version`: files from `pod5` older than 0.0.38 (Reads table V0–V2) now fail with a clear error suggesting `pod5 update`, instead of a misleading "RunInfoTable missing".
- Test fixtures from ONT's pod5 repository (`tests/fixtures/`, Reads table V2–V7, MPL-2.0) with per-version tests; file-backed integration tests now run in CI instead of being skipped.
- CI job checking interop with Python `pod5` 0.3.49 and 0.3.39 (read parity and write parity on every fixture).
- `POD5_COMPAT_VERSION` (`"0.3.49"`): the upstream `pod5` version this crate is validated against.
- `Error::InvalidFileIdentifier`: writers reject a `file_identifier` that is not a UUID (upstream refuses to open such files). Any UUID form is accepted and written in canonical hyphenated form.
- `Error::InvalidPod5Version`: writers now reject a `pod5_version` that upstream would refuse to open (not `X.Y.Z`) or would wrongly migrate (below `0.0.38`).
- `ReadRecord::expected_open_pore_level` and `ReadRecord::selected_read_level` (Reads table V5), NaN when absent.
- `Reader::reads_table_version()`: the Reads-table schema version stored in the file (3–7), equivalent to Python `Reader.physical_read_table_version`.

### Changed

- Writer defaults: `file_identifier` is now a fresh random (v4) UUID, as upstream generates for every new file (was empty, which produced files upstream could not open). New dependency: `uuid`.
- Writer defaults: `pod5_version` is now `POD5_COMPAT_VERSION` (was the crate version), and `software` is `"pod5lib-rs <version> (pod5 <compat version>)"` (was `"pod5lib <version>"`).
- **Breaking:** `Pore::channel` is now `u32`. It is read from `channel_32bit` when present, so channels above 65535 are preserved.
- Writers now emit a V7 Reads table: adds `expected_open_pore_level`, `selected_read_level` and `channel_32bit`, while `channel` stays `UInt16` (0 when it does not fit in 16 bits). Older `pod5` versions still read these files and ignore the new columns.

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
