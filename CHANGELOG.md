# Changelog

All notable changes to pod5lib will be documented here.

## [0.2.0] - unreleased

Brings pod5lib in line with Python `pod5` **0.3.49** (Reads table V7), fixes files that
current `pod5` could not open, and hardens reading against corrupt files. Contains
breaking changes; see [Upgrading from 0.1](#upgrading-from-01).

### Upgrading from 0.1

- **Rewrite files written by 0.1.** Python `pod5` 0.3.47 and later cannot open them
  ("Invalid footer found in file"). pod5lib 0.2 still reads them: open each with
  `Reader` and write it again with `StreamingWriter`. Files written with 0.1's default
  `Writer::new()` also had an empty `file_identifier`; 0.2 generates one.
- `Pore::channel` is now `u32`: remove `as u16` conversions.
- `Error` and `EndReasonKind` are `#[non_exhaustive]`: add a `_ =>` arm when matching them.
- `EndReasonKind::Other` holds a `String`, and `EndReasonKind` is no longer `Copy`:
  clone it where you copied it before.
- `Error::Arrow` no longer exposes arrow's error type; use `std::error::Error::source`
  to reach it.
- Call `StreamingWriter::finish()` explicitly. A writer dropped without it now leaves
  an invalid file instead of a valid one missing its last reads.
- Don't copy `file_identifier` or `pod5_version` from a source file into a writer: the
  defaults are now correct, and writers reject values upstream cannot read.

### Breaking changes

- `Pore::channel` is `u32`. It is read from `channel_32bit` when present, so channels above 65535 are preserved.
- `Error` and `EndReasonKind` are `#[non_exhaustive]`; future variants will not be breaking changes.
- `Error::Arrow` holds the arrow error type-erased (available through `std::error::Error::source`), so `arrow` is no longer part of the public API and can be upgraded in a patch release.
- `EndReasonKind::Other` holds the unrecognised reason string (was an always-zero `u8`), and writers write it back unchanged (was `"other"`, which Python `pod5` cannot parse). Reading an unrecognised reason prints a one-time warning per string asking for it to be reported. `EndReasonKind` is no longer `Copy`, and `as_str` borrows from it.
- A `StreamingWriter` dropped without `finish()` abandons the file without a footer, so it cannot be opened. Before, the background thread completed it as a valid file that silently lacked the last buffered reads.
- Writers reject a `file_identifier` that is not a UUID (`Error::InvalidFileIdentifier`) and a `pod5_version` that upstream would refuse or wrongly migrate (`Error::InvalidPod5Version`).

### Fixed

- **Files written by pod5lib could not be opened by Python `pod5` ≥ 0.3.47** ("Invalid footer found in file"). The FlatBuffers footer is now padded to a multiple of 8 bytes and its `int64` fields are 8-byte aligned, matching upstream's builder.
- **Corrupt or truncated files could crash the calling program.** A single corrupted byte could cause a panic or an attempted allocation of hundreds of gigabytes, which aborts the process. Reading is now hardened against untrusted input: the footer parser is bounds-checked; footer, table and blob offsets and lengths are validated against the file before anything is allocated; sample counts are checked against the compressed data; Arrow IPC block tables are validated before `arrow` reads them; and panics inside `arrow` or the SVB16 decoder become errors (`Error::Malformed` or an existing variant).
- Upgraded `svb` from 0.1 to 0.3.1. Its SIMD decoder in 0.1 did not check that a stream's control bits matched the data present: a corrupt signal blob could make it read past a stack buffer (returning wrong samples) or panic. 0.3.1 returns an error instead.
- Files with a V6 Reads table (`channel` as `UInt32`, written by `pod5` 0.3.46–0.3.47) failed to open with a type mismatch.
- **`end_reason` dictionary order:** writers now emit all ten known reasons in `pod5.EndReasonEnum` order, as upstream does, so each read's dictionary key equals its enum value. Before, the dictionary was built in first-seen order, so anything using the raw key (such as Python `ReadRecord.end_reason_index`) got the wrong reason.
- Writers with the default settings produced files with an empty `file_identifier`, which upstream `pod5` cannot open.
- `StreamingWriter::finish()` reported `WriterThreadDied` instead of the actual error (such as a full disk) when the writer thread failed.
- Writers did not flush explicitly, so an error writing the end of the file could be lost.
- `Writer::write_file` emptied an existing output file before validating the writer's settings.
- Signals longer than 4,294,967,295 samples were silently truncated; more than 32,768 distinct pore types or run infos produced a corrupt dictionary (now `Error::TooManyDictionaryValues`).
- Writers took `num_samples` from the record rather than from the signal they were given, so the Reads and Signal tables could disagree.
- `signal_batch_size = 0` panicked; it is now treated as 1.
- `Reader::open` (lazy signal) now checks the Signal table's `file_identifier` against the footer, as `Reader::from_reader` already did.
- `Reader::open` handles any valid Signal table layout: the fast path for upstream's standard layout (VBZ signal, columns `read_id, signal, samples`, no Arrow body compression) is unchanged, and anything else is loaded with the general parser instead of being misread.
- A panic while decoding signal no longer leaves a lazily-opened `Reader` unusable (its file lock was poisoned). Signal is now decompressed after releasing the file lock.
- Files from `pod5` older than 0.0.38 (Reads table V0–V2) failed with a misleading "RunInfoTable missing"; they now fail with `Error::LegacyPod5Version`, suggesting `pod5 update`.
- The crate did not build on its stated minimum Rust version (1.87): it used `let` chains, stable only from 1.88. CI now builds and tests on 1.87.

### Added

- `ReadRecord::new`: build reads from scratch (e.g. when converting from FAST5/BLOW5) with the same required fields as Python `pod5.Read`; other fields take Python's defaults. See `examples/write_from_scratch.rs`.
- `ReadRecord::expected_open_pore_level` and `ReadRecord::selected_read_level` (Reads table V5), NaN when absent.
- `Reader::reads_table_version()`: the Reads-table schema version stored in the file (3–7), equivalent to Python `Reader.physical_read_table_version`.
- `EndReason::from_reason_with_default_forced` and `EndReasonKind::default_forced`, matching Python `pod5`.
- `uuid_from_str`: parse a read ID string into the 16 bytes stored in POD5 (inverse of `uuid_to_str`).
- `RunInfo` implements `Default`, for struct update syntax.
- `POD5_COMPAT_VERSION` (`"0.3.49"`): the upstream `pod5` version this crate is validated against.
- Errors: `Malformed`, `LegacyPod5Version`, `InvalidFileIdentifier`, `InvalidPod5Version`, `TooManyDictionaryValues`.
- `StreamingWriter` and `ParReadsIter` implement `Debug`.
- `#![forbid(unsafe_code)]`.

### Changed

- Writers emit a V7 Reads table: adds `expected_open_pore_level`, `selected_read_level` and `channel_32bit`, while `channel` stays `UInt16` (0 when it does not fit in 16 bits). Older `pod5` versions still read these files and ignore the new columns.
- Signal is written in rows of up to 102,400 samples, as upstream does (was one row per read). Reads of any length can be written, and Python's per-row APIs (`signal_rows`, `signal_for_chunk`) match upstream-written files.
- Writer defaults: `file_identifier` is a fresh random (v4) UUID per file, as upstream generates; `pod5_version` is `POD5_COMPAT_VERSION` (was the crate version); `software` is `"pod5lib-rs <version> (pod5 <compat version>)"` (was `"pod5lib <version>"`).
- Both writers share one implementation. `Writer` compresses `signal_batch_size` reads at a time rather than holding every compressed read in memory; in a 4,282-read benchmark, `StreamingWriter` peak memory dropped from 322 MB to 145 MB with no change in speed.
- `ShiftScalePair` equality follows Python: two all-NaN pairs (such as `UNAVAILABLE`) are equal.
- The section marker between tables is a random UUID per file, as upstream writes (was all zeros).
- New dependency: `uuid`. `svb` 0.1 → 0.3.1.

### Testing

- Test fixtures from ONT's pod5 repository (`tests/fixtures/`, Reads table V2–V7, MPL-2.0) with per-version tests; file-backed integration tests now run in CI instead of being skipped.
- `tests/fixtures/all_fields_v7.pod5`, written by upstream pod5 0.3.49 with a distinct real value in every field (channels 65535–70000, V5 levels, scaling pairs, mux fields, two run infos), plus the script that generates it, and an uncompressed-signal fixture.
- CI checks interop with Python `pod5` 0.3.49 and 0.3.39 in both directions, field by field (46 fields per read, including the full run info).
- `tests/malformed.rs` checks thousands of corrupted and truncated files on every run; CI checks every single-byte corruption and truncation of a small file in release mode.
- CI builds and tests on the minimum supported Rust version.

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
