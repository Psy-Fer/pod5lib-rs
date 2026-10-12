//! POD5 file-level binary format constants.

/// Opening and closing 8-byte file signature (mirrors PNG design to catch transport corruption).
pub(crate) const SIGNATURE: [u8; 8] = [0x8B, b'P', b'O', b'D', 0x0D, 0x0A, 0x1A, 0x0A];

/// 16-byte UUID section marker that separates every embedded section.
/// All markers in a file are identical; used for recovery scanning.
pub(crate) const SECTION_MARKER_LEN: usize = 16;

/// 8-byte magic that precedes the FlatBuffers footer blob.
pub(crate) const FOOTER_MAGIC: [u8; 8] = [b'F', b'O', b'O', b'T', b'E', b'R', 0x00, 0x00];

/// Bytes from the end of file to the start of the 8-byte footer-length field:
///   closing_sig(8) + final_section_marker(16) + footer_len_field(8) = 32
pub(crate) const FOOTER_LEN_FIELD_FROM_END: u64 = 32;

/// Oldest `pod5_version` with the current container layout (Reads table V3).
/// Upstream `pod5` physically migrates older files on open; we reject them.
pub(crate) const MIN_POD5_VERSION: (u16, u16, u16) = (0, 0, 38);

/// Parse a `pod5_version` string the way upstream does: exactly `X.Y.Z`, each a u16.
pub(crate) fn parse_pod5_version(v: &str) -> Option<(u16, u16, u16)> {
    let mut parts = v.split('.').map(|p| {
        (!p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
            .then(|| p.parse::<u16>().ok())
            .flatten()
    });
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(Some(a)), Some(Some(b)), Some(Some(c)), None) => Some((a, b, c)),
        _ => None,
    }
}

/// Align `n` up to the next multiple of 8.
#[inline]
pub(crate) fn align8(n: u64) -> u64 {
    (n + 7) & !7
}

#[cfg(test)]
mod tests {
    use super::{align8, parse_pod5_version};

    #[test]
    fn parse_pod5_versions() {
        assert_eq!(parse_pod5_version("0.3.49"), Some((0, 3, 49)));
        assert_eq!(parse_pod5_version("65535.0.0"), Some((65535, 0, 0)));
        for bad in [
            "",
            "0.1",
            "1.0.0.0",
            "0.2.0-rc1",
            "65536.0.0",
            "1..0",
            "+1.0.0",
            " 1.0.0",
        ] {
            assert_eq!(parse_pod5_version(bad), None, "{bad}");
        }
    }

    #[test]
    fn align8_already_aligned() {
        assert_eq!(align8(0), 0);
        assert_eq!(align8(8), 8);
        assert_eq!(align8(16), 16);
        assert_eq!(align8(1024), 1024);
    }

    #[test]
    fn align8_rounds_up() {
        assert_eq!(align8(1), 8);
        assert_eq!(align8(7), 8);
        assert_eq!(align8(9), 16);
        assert_eq!(align8(15), 16);
        assert_eq!(align8(17), 24);
    }
}
