# Contributing to pod5lib

## Before you start

pod5lib is a clean-room Rust implementation of the [POD5 file format](https://github.com/nanoporetech/pod5-file-format) specified and developed by Oxford Nanopore Technologies. Any contribution must maintain wire compatibility with the Python `pod5` library. Files written by one must be readable by the other.

## Development setup

```bash
# Build
cargo build

# Format, lint, test (mirrors CI)
cargo ci-fmt
cargo ci-clippy
cargo ci-test

# Integration tests also run against your own file if present
# (otherwise they use the committed tests/fixtures/multi_fast5_zip_v7.pod5)
mkdir -p tests/data
cp your_file.pod5 tests/data/sample.pod5
cargo ci-test

# Python interop (also run in CI against pod5 0.3.49 and 0.3.39)
pip install pod5==0.3.49
python tests/test_parity.py tests/fixtures/multi_fast5_zip_v7.pod5        # Rust vs Python reader
python tests/test_write_parity.py tests/fixtures/multi_fast5_zip_v7.pod5  # Rust writer -> Python reader
```

`tests/fixtures/` holds ONT's upstream test files, one per Reads-table version (V2–V7);
see its README for provenance.

## What to work on

Check the issue tracker for open issues. Good starting points are labelled `good first issue`.

Before opening a PR for a new feature, open an issue first to discuss scope, particularly anything that touches the on-disk format or the public API, since those have compatibility implications.

## Compatibility requirement

The reference for correctness is the Python `pod5` library. Before submitting a change that touches reading or writing:

1. Test against a real POD5 file produced by Python `pod5` (not just synthetic data)
2. Verify files written by pod5lib-rs are readable by Python `pod5`
3. Note the Python `pod5` version you tested against in the PR description

The currently validated versions are **0.3.39** and **0.3.49**; the `python-interop` CI job checks both. When validating a new release, update `POD5_COMPAT_VERSION` in `src/lib.rs` and the CI matrix. If you test against a newer version and it still works, update `CHANGELOG.md` and the compatibility note in `README.md`.

## Reporting bugs

If you find a file that pod5lib-rs reads incorrectly or refuses to open:

- Confirm the file is valid by opening it with Python `pod5` first
- Provide the smallest reproducible example you can (a truncated or synthetic file is fine)
- Include the output of `pod5 inspect reads your_file.pod5` alongside the Rust error

Do not report upstream ONT bugs based on pod5lib-rs behaviour alone. Rule out a reimplementation issue first.

## Pull requests

- Keep PRs focused; one logical change per PR
- Add or update tests for any behaviour change
- Run `cargo ci-fmt && cargo ci-clippy && cargo ci-test` locally before pushing
- Update `CHANGELOG.md` under an `[Unreleased]` heading

## License

By contributing you agree that your changes will be licensed under the [Mozilla Public License 2.0](LICENSE), consistent with the rest of the project and the ONT reference implementation.
