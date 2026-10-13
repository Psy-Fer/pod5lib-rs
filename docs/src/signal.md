# Signal & VBZ

POD5 signal data is compressed with **VBZ**, a four-stage codec designed for 16-bit ADC
signal that exhibits short-range correlation.

## Compression pipeline

```text
raw i16 ADC samples
  ──▶  delta encode       (1st-order differences; small values dominate)
  ──▶  zigzag encode      (signed i16 → unsigned u16; small |values| → small codes)
  ──▶  SVB16 encode       (StreamVByte-16: 1 or 2 bytes/value, 1-bit control stream)
  ──▶  zstd               (outer entropy coding)
```

Decompression applies the stages in reverse.

## Decompression walkthrough

Given a VBZ blob and its uncompressed sample count `N` (from the `samples` Arrow column):

### 1. zstd decompress

Decompress the blob to recover the SVB16-encoded stream. Its length must lie between
`⌈N/8⌉ + N` and `⌈N/8⌉ + 2N` bytes; pod5lib checks this against the size declared in
the zstd frame before decompressing, because `N` comes from the file and could be corrupt.

### 2. SVB16 decode

The SVB16 stream begins with `⌈N/8⌉` **control bytes**, followed by the data bytes.

Each control byte encodes 8 values, one bit per value (LSB-first):
- `0` → the next data byte is a 1-byte value (`u8`, zero-extended to `u16`)
- `1` → the next two data bytes are a 2-byte LE value (`u16`)

```text
control byte layout (8 values, LSB first):
  bit 0 → value 0   (0 = 1 byte, 1 = 2 bytes)
  bit 1 → value 1
  ...
  bit 7 → value 7
```

### 3. Zigzag decode

Convert each `u16` back to a signed `i16` delta:

```text
delta = (n >> 1) ^ -(n & 1)
```

This maps `0→0, 1→-1, 2→1, 3→-2, 4→2, …`, inverting the zigzag encoding.

### 4. Prefix sum

Cumulative-sum the deltas to recover the original ADC samples:

```text
sample[0] = delta[0]
sample[i] = sample[i-1] + delta[i]
```

Steps 2–4 run as one fused SIMD pass in the [`svb`](https://crates.io/crates/svb) crate
(SSSE3 on x86-64, NEON on aarch64, scalar fallback), which returns an error if the control
bits ask for more data than the stream holds.

## Multi-chunk reads

A read's signal is stored across Signal table rows of up to 102,400 samples each.
`ReadRecord::signal()` decompresses and concatenates its rows in order, returning the full
`Vec<i16>`. Writers split signal the same way.

## Calibration

After decompression, convert ADC samples to picoamps:

```text
pA[i] = (adc[i] + calibration.offset) * calibration.scale
```

where `calibration.scale = adc_range / digitisation` and `digitisation = adc_max − adc_min + 1`.

`ReadRecord::signal_pa()` combines decompression and calibration in one call.
