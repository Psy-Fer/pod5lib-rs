# Changelog

All notable changes to pod5lib will be documented here.

## [Unreleased]

Brings pod5lib in line with Python `pod5` **0.3.49** (Reads table V7).

### Fixed

- **Corrupt or truncated files could crash the calling program.** A single corrupted byte could cause a panic or an attempted allocation of hundreds of gigabytes, which aborts the process. Reading is now hardened against untrusted input: the footer parser is bounds-checked; footer, table and blob offsets and lengths are validated against the file before anything is allocated; sample counts are checked against the compressed data; Arrow IPC block tables are validated before `arrow` reads them; and panics inside `arrow` or the SVB16 decoder become errors. These all return the new `Error::Malformed` or an existing error variant. `tests/malformed.rs` checks thousands of corruptions on every run, and CI checks every single-byte corruption and truncation of a small file.
- `Reader::open` (lazy signal) now checks the Signal table's `file_identifier` against the footer, as `Reader::from_reader` already did.
- `Reader::open` now handles any valid Signal table layout: the fast path for upstream's standard layout (VBZ signal, columns `read_id, signal, samples`, no Arrow body compression) is unchanged, and anything else is loaded with the general parser instead of being misread.
- A panic while decoding signal no longer leaves a lazily-opened `Reader` unusable (its file lock was poisoned). Signal is now decompressed after releasing the file lock.
- **`end_reason` dictionary order:** writers now emit all ten known reasons in `pod5.EndReasonEnum` order, as upstream does, so each read's dictionary key equals its enum value. Before, the dictionary was built in first-seen order, so anything using the raw key (such as Python `ReadRecord.end_reason_index`) got the wrong reason.
- `StreamingWriter::finish()` reported `WriterThreadDied` instead of the actual error (such as a full disk) when the writer thread failed.
- Writers did not flush explicitly, so an error writing the end of the file could be lost.
- `Writer::write_file` emptied an existing output file before validating the writer's settings.
- Signals longer than 4,294,967,295 samples were silently truncated; more than 32,768 distinct pore types or run infos produced a corrupt dictionary (now `Error::TooManyDictionaryValues`).
- `signal_batch_size = 0` panicked; it is now treated as 1.
- The crate did not build on its stated minimum Rust version (1.87): it used `let` chains, stable only from 1.88. CI now builds and tests on 1.87.
- Writers now take `num_samples` from the signal they are given rather than from the record, so the Reads and Signal tables cannot disagree.
- **Files written by pod5lib could not be opened by Python `pod5` ≥ 0.3.47** ("Invalid footer found in file"). The FlatBuffers footer is now padded to a multiple of 8 bytes and its `int64` fields are 8-byte aligned, matching upstream's builder.
- Files with a V6 Reads table (`channel` as `UInt32`, written by `pod5` 0.3.46–0.3.47) failed to open with a type mismatch.

### Added

- `Error::Malformed` for corrupt or truncated files.
- `EndReason::from_reason_with_default_forced` and `EndReasonKind::default_forced`, matching Python `pod5`.
- `Error::TooManyDictionaryValues`.
- `StreamingWriter` implements `Debug`.
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

- **Breaking:** `Error` and `EndReasonKind` are `#[non_exhaustive]`; `match` on them needs a wildcard arm. Future variants will not be breaking changes.
- **Breaking:** `Error::Arrow` holds the arrow error type-erased (available through `std::error::Error::source`), so `arrow` is no longer part of the public API and can be upgraded in a patch release.
- **Breaking:** `EndReasonKind::Other` now holds the unrecognised reason string (was an always-zero `u8`), and writers write it back unchanged (was `"other"`, which Python `pod5` cannot parse). Reading an unrecognised reason prints a one-time warning per string asking for it to be reported. `EndReasonKind` is no longer `Copy`, and `as_str` borrows from it.
- **Breaking:** a `StreamingWriter` dropped without `finish()` now abandons the file without a footer, so it cannot be opened. Before, the background thread completed it as a valid file that silently lacked the last buffered reads.
- Signal is written in rows of up to 102,400 samples, as upstream does (was one row per read). Reads of any length can be written, and Python's per-row APIs (`signal_rows`, `signal_for_chunk`) match upstream-written files.
- Both writers share one implementation. `Writer` compresses `signal_batch_size` reads at a time rather than holding every compressed read in memory; in a 4,282-read benchmark, `StreamingWriter` peak memory dropped from 322 MB to 145 MB with no change in speed.
- `ShiftScalePair` equality follows Python: two all-NaN pairs (such as `UNAVAILABLE`) are equal.
- The section marker between tables is a random UUID per file, as upstream writes (was all zeros).
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
