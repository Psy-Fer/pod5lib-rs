# POD5 Format

POD5 is a binary container embedding three [Apache Arrow IPC](https://arrow.apache.org/docs/format/IPC.html)
tables and a [FlatBuffers](https://flatbuffers.dev/) footer.

## Binary layout

```text
[  8 bytes ] opening signature  0x8B 'P' 'O' 'D' 0x0D 0x0A 0x1A 0x0A
[ 16 bytes ] section marker (random UUID per file, same value repeated throughout)
[  N bytes ] Arrow IPC file #1  (padded to 8-byte boundary)
[ 16 bytes ] section marker
[ ... ]      Arrow IPC file #2
[ 16 bytes ] section marker
[ ... ]      Arrow IPC file #3
[ 16 bytes ] section marker
[  8 bytes ] footer magic  "FOOTER\0\0"
[  F bytes ] FlatBuffers FileFooter  (F is a multiple of 8)
[  8 bytes ] footer_length (i64 LE, = F)
[ 16 bytes ] section marker
[  8 bytes ] closing signature (identical to opening)
```

The reader navigates from the **end of file**: reads `footer_length` at
`file_size − 32`, takes the FlatBuffer as the `footer_length` bytes before that,
verifies the magic before it, then parses the FlatBuffer to find the byte offsets of
the three Arrow sections. Upstream relies on `footer_length` being a multiple of 8,
which a standard FlatBuffers builder guarantees; pod5lib 0.1 wrote unpadded lengths,
which is why current `pod5` cannot open its files. Every offset and length read from
the file is checked against the file size before use.

## Three Arrow IPC tables

The FlatBuffers `Footer.contents` vector lists the byte offset and length of each section:

### Reads table

| Column | Arrow type | Description |
|---|---|---|
| `read_id` | `FixedSizeBinary(16)` | UUID in RFC 4122 big-endian byte order |
| `signal` | `List<UInt64>` | Row indices into the Signal table |
| `read_number` | `UInt32` | Read number on the channel: increasing, not necessarily consecutive |
| `channel` | `UInt16` (`UInt32` in V6) | Channel number; 0 in V7 when it does not fit in 16 bits |
| `well` | `UInt8` | Well number |
| `start` | `UInt64` | Samples recorded on the channel before the read started (`ReadRecord::start_sample`) |
| `median_before` | `Float32` | Open-channel level before the read (pA); nullable |
| `num_samples` | `UInt64` | Total ADC samples |
| `calibration_offset` | `Float32` | ADC zero-point offset |
| `calibration_scale` | `Float32` | Picoamps per ADC count |
| `pore_type` | `Dictionary<Int16, Utf8>` | Pore type string |
| `end_reason` | `Dictionary<Int16, Utf8>` | End-reason string. The dictionary lists all ten known reasons in `pod5.EndReasonEnum` order, so a key equals its enum value |
| `end_reason_forced` | `Bool` | Whether termination was forced |
| `run_info` | `Dictionary<Int16, Utf8>` | `acquisition_id` string |
| `num_minknow_events` | `UInt64` | MinKNOW event count |
| `tracked_scaling_shift/scale` | `Float32` | Real-time scaling; NaN if absent |
| `predicted_scaling_shift/scale` | `Float32` | Predicted scaling; NaN if absent |
| `num_reads_since_mux_change` | `UInt32` | (deprecated; optional) |
| `time_since_mux_change` | `Float32` | (deprecated; optional) |
| `open_pore_level` | `Float32` | Open-pore level (pA); nullable |
| `expected_open_pore_level` | `Float32` | Expected open-pore level (pA); V5+, NaN if absent |
| `selected_read_level` | `Float32` | Selected read level (pA); V5+, NaN if absent |
| `channel_32bit` | `UInt32` | Full channel number; V7+, preferred over `channel` when present |

The Reads-table version is inferred from its columns, as upstream pod5 does
([`Reader::reads_table_version`](https://docs.rs/pod5lib/latest/pod5lib/struct.Reader.html#method.reads_table_version)):
V4 adds `open_pore_level`; V5 adds `expected_open_pore_level` and `selected_read_level`;
V6 widens `channel` to `UInt32`; V7 restores `channel` to `UInt16` and adds `channel_32bit`.
The reader accepts V3–V7. The writer always emits V7. Files written by `pod5` before 0.0.38
(V0–V2) use an older layout; reading them fails with `Error::LegacyPod5Version`.

### Signal table

| Column | Arrow type | Description |
|---|---|---|
| `read_id` | `FixedSizeBinary(16)` | UUID of the owning read |
| `signal` | `LargeBinary` | VBZ-compressed chunk (extension: `minknow.vbz`), or `LargeList<Int16>` for uncompressed signal |
| `samples` | `UInt32` | Number of ADC samples in this chunk |

A read's signal is split into rows of up to 102,400 samples (upstream's default chunk
size; an empty signal has no rows). The `signal` column in the Reads table lists the row
indices that belong to each read.

### RunInfo table

| Column | Arrow type | Description |
|---|---|---|
| `acquisition_id` | `Utf8` | Primary key, matched by the Reads `run_info` dictionary |
| `sample_rate` | `UInt16` | ADC sampling rate (Hz) |
| `adc_max` / `adc_min` | `Int16` | ADC output range |
| `context_tags` | `Map<Utf8, Utf8>` | Experiment configuration key/value pairs |
| `tracking_id` | `Map<Utf8, Utf8>` | MinKNOW tracking key/value pairs |
| `acquisition_start_time` | `Timestamp(ms, "UTC")` | `RunInfo::acquisition_start_time_ms` |
| `protocol_start_time` | `Timestamp(ms, "UTC")` | `RunInfo::protocol_start_time_ms` |
| `experiment_name` | `Utf8` | |
| `flow_cell_id` / `flow_cell_product_code` | `Utf8` | |
| `protocol_name` / `protocol_run_id` | `Utf8` | |
| `sample_id` | `Utf8` | |
| `sequencing_kit` | `Utf8` | |
| `sequencer_position` / `sequencer_position_type` | `Utf8` | |
| `software` | `Utf8` | |
| `system_name` / `system_type` | `Utf8` | |

## UUID byte order

POD5 stores UUIDs as standard RFC 4122 **big-endian** bytes, identical to
Python `uuid.UUID(bytes=data)`. `uuid_to_str` reads the 16 bytes in order to produce
the canonical `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` string.

## Arrow schema metadata

Each table's Arrow schema carries `MINKNOW:*` metadata keys:

| Key | Value |
|---|---|
| `MINKNOW:file_identifier` | File-level UUID (cross-validated with FlatBuffers footer) |
| `MINKNOW:software` | Free-text software string (set by the writer) |
| `MINKNOW:pod5_version` | Writing `pod5` library version, `X.Y.Z`; upstream rejects files where it does not parse |
