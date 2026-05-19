//! POD5 file-level binary format constants.

/// Opening and closing 8-byte file signature (mirrors PNG design to catch transport corruption).
pub const SIGNATURE: [u8; 8] = [0x8B, b'P', b'O', b'D', 0x0D, 0x0A, 0x1A, 0x0A];

/// 16-byte UUID section marker that separates every embedded section.
/// All markers in a file are identical; used for recovery scanning.
pub const SECTION_MARKER_LEN: usize = 16;

/// 8-byte magic that precedes the FlatBuffers footer blob.
pub const FOOTER_MAGIC: [u8; 8] = [b'F', b'O', b'O', b'T', b'E', b'R', 0x00, 0x00];

/// Bytes from the end of file to the start of the 8-byte footer-length field:
///   closing_sig(8) + final_section_marker(16) + footer_len_field(8) = 32
pub const FOOTER_LEN_FIELD_FROM_END: u64 = 32;

/// Align `n` up to the next multiple of 8.
#[inline]
pub fn align8(n: u64) -> u64 {
    (n + 7) & !7
}

#[cfg(test)]
mod tests {
    use super::align8;

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
