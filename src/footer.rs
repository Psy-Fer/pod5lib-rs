/// Hand-written minimal FlatBuffers reader for the POD5 footer schema.
///
/// POD5 FlatBuffers schema (footer.fbs):
///
/// ```flatbuffers
/// enum ContentType : short { ReadsTable=0, SignalTable=1, ReadIdIndex=2,
///                            OtherIndex=3, RunInfoTable=4 }
/// enum Format      : short { FeatherV2=0 }
///
/// table EmbeddedFile {
///     offset:       int64;          // field 0 – byte offset in the POD5 file
///     length:       int64;          // field 1 – byte length (excl. padding)
///     format:       Format;         // field 2
///     content_type: ContentType;    // field 3
/// }
///
/// table Footer {
///     file_identifier: string;          // field 0
///     software:        string;          // field 1
///     pod5_version:    string;          // field 2
///     contents:        [EmbeddedFile];  // field 3
/// }
/// root_type Footer;
/// ```
use crate::{Error, Result};

// ── FlatBuffers low-level primitives ─────────────────────────────────────────

struct Fb<'a> {
    buf: &'a [u8],
}

fn bad(what: &str) -> Error {
    Error::FlatBuffers(format!("malformed footer: {what}"))
}

/// Every accessor is bounds-checked: the footer comes from an untrusted file.
impl<'a> Fb<'a> {
    fn bytes<const N: usize>(&self, pos: usize) -> Result<[u8; N]> {
        pos.checked_add(N)
            .and_then(|end| self.buf.get(pos..end))
            .map(|b| b.try_into().unwrap())
            .ok_or_else(|| bad("offset out of bounds"))
    }
    fn u16_at(&self, pos: usize) -> Result<u16> {
        self.bytes(pos).map(u16::from_le_bytes)
    }
    fn u32_at(&self, pos: usize) -> Result<u32> {
        self.bytes(pos).map(u32::from_le_bytes)
    }
    fn i32_at(&self, pos: usize) -> Result<i32> {
        self.bytes(pos).map(i32::from_le_bytes)
    }
    fn i64_at(&self, pos: usize) -> Result<i64> {
        self.bytes(pos).map(i64::from_le_bytes)
    }
    fn i16_at(&self, pos: usize) -> Result<i16> {
        self.bytes(pos).map(i16::from_le_bytes)
    }

    /// Dereference a forward `uoffset_t`: returns the target absolute position.
    fn follow_u(&self, pos: usize) -> Result<usize> {
        pos.checked_add(self.u32_at(pos)? as usize)
            .filter(|&p| p < self.buf.len())
            .ok_or_else(|| bad("offset out of bounds"))
    }

    /// Absolute position of the root table.
    fn root(&self) -> Result<usize> {
        self.follow_u(0)
    }

    /// Absolute position of a field in a table, or `None` if absent/default.
    fn field(&self, table: usize, id: usize) -> Result<Option<usize>> {
        // soffset at start of table points backwards to the vtable
        let soffset = self.i32_at(table)? as i64;
        let vtable = usize::try_from(table as i64 - soffset).map_err(|_| bad("bad vtable"))?;
        let vt_size = self.u16_at(vtable)? as usize;
        let entry = vtable + 4 + id * 2;
        if entry + 2 > vtable + vt_size {
            return Ok(None);
        }
        match self.u16_at(entry)? as usize {
            0 => Ok(None),
            offset => Ok(Some(table + offset)),
        }
    }

    /// Dereference a string field (field_pos → uoffset → length-prefixed UTF-8).
    fn string_at(&self, field_pos: usize) -> Result<&'a str> {
        let s = self.follow_u(field_pos)?;
        let len = self.u32_at(s)? as usize;
        let bytes = (s + 4)
            .checked_add(len)
            .and_then(|end| self.buf.get(s + 4..end))
            .ok_or_else(|| bad("string out of bounds"))?;
        std::str::from_utf8(bytes).map_err(Error::Utf8)
    }

    /// Dereference a vector field → `(vector_start, element_count)`. The count is
    /// checked against the buffer so it cannot drive a huge allocation.
    fn vector_at(&self, field_pos: usize) -> Result<(usize, usize)> {
        let v = self.follow_u(field_pos)?;
        let count = self.u32_at(v)? as usize;
        if count > (self.buf.len() - v - 4) / 4 {
            return Err(bad("vector length out of bounds"));
        }
        Ok((v, count))
    }

    /// Table at index `i` inside a vector of tables.
    fn vector_table(&self, vec_start: usize, i: usize) -> Result<usize> {
        self.follow_u(vec_start + 4 + i * 4)
    }

    /// Optional string field of `table`; empty if absent.
    fn opt_string(&self, table: usize, id: usize) -> Result<String> {
        Ok(match self.field(table, id)? {
            Some(p) => self.string_at(p)?.to_owned(),
            None => String::new(),
        })
    }
}

// ── Domain types ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ContentType {
    ReadsTable,
    SignalTable,
    RunInfoTable,
    /// Index or unknown variant; value is preserved.
    Other(i16),
}

impl ContentType {
    fn from_i16(v: i16) -> Self {
        match v {
            0 => Self::ReadsTable,
            1 => Self::SignalTable,
            4 => Self::RunInfoTable,
            n => Self::Other(n),
        }
    }
}

#[derive(Debug)]
pub(crate) struct EmbeddedFile {
    /// Absolute byte offset in the POD5 file.
    pub offset: i64,
    /// Byte length of the embedded Arrow IPC file, excluding alignment padding.
    pub length: i64,
    pub content_type: ContentType,
}

#[derive(Debug)]
pub(crate) struct Footer {
    pub file_identifier: String,
    pub pod5_version: String,
    pub software: String,
    pub contents: Vec<EmbeddedFile>,
}

// ── Parser ────────────────────────────────────────────────────────────────────

pub(crate) fn parse(data: &[u8]) -> Result<Footer> {
    if data.len() < 4 {
        return Err(Error::FlatBuffers("buffer too short".into()));
    }
    let fb = Fb { buf: data };
    let root = fb.root()?;

    let file_identifier = fb.opt_string(root, 0)?;
    let software = fb.opt_string(root, 1)?;
    let pod5_version = fb.opt_string(root, 2)?;

    let mut contents = Vec::new();
    if let Some(contents_pos) = fb.field(root, 3)? {
        let (vec_start, count) = fb.vector_at(contents_pos)?;
        for i in 0..count {
            let tbl = fb.vector_table(vec_start, i)?;
            let get_i64 =
                |id| -> Result<i64> { fb.field(tbl, id)?.map_or(Ok(0), |p| fb.i64_at(p)) };
            let content_type = fb.field(tbl, 3)?.map_or(Ok(0), |p| fb.i16_at(p))?;
            contents.push(EmbeddedFile {
                offset: get_i64(0)?,
                length: get_i64(1)?,
                content_type: ContentType::from_i16(content_type),
            });
        }
    }

    Ok(Footer {
        file_identifier,
        pod5_version,
        software,
        contents,
    })
}

// ── Builder ───────────────────────────────────────────────────────────────────

/// Serialize a POD5 FlatBuffers footer to bytes.
///
/// `contents`: slice of `(offset, length, content_type)` triples where
/// content_type is the raw i16 value (0=ReadsTable, 1=SignalTable, 4=RunInfoTable).
///
/// The layout follows the standard FlatBuffers builder convention: vtables are
/// placed immediately BEFORE their tables (positive soffset) so that the
/// FlatBuffers verifier accepts the buffer without modification.
///
/// Like a real FlatBuffers builder, the `i64` fields are 8-byte aligned and the
/// total size is a multiple of 8. Upstream pod5 relies on the latter: it reads the
/// footer as the `footer_length` bytes immediately before the length field.
///
/// Layout (N = contents.len(), P = 0 or 4 bytes of alignment padding):
/// ```text
/// [0..4]      root uoffset = 16  (Footer table at pos 16)
/// [4..16]     Footer vtable (12 bytes)
/// [16..36]    Footer table  (soffset=12, then 4 field uoffsets)
/// [36..40+4N] contents vector: count + N element uoffsets
/// [40+4N..52+4N]  EmbeddedFile vtable (shared, 12 bytes)
/// [52+4N+P..]     N EmbeddedFile tables (24 bytes each; i64 fields 8-byte aligned)
/// [..]        string data, then zero padding to a multiple of 8
/// ```
pub(crate) fn build_footer(
    file_identifier: &str,
    software: &str,
    pod5_version: &str,
    contents: &[(i64, i64, i16)],
) -> Vec<u8> {
    let n = contents.len() as u32;

    // Fixed positions
    let footer_vtable: u32 = 4; // Footer vtable at pos 4
    let footer_table: u32 = 16; // Footer table at pos 16
    let contents_vec: u32 = 36; // contents vector at pos 36
    let ef_vtable: u32 = 40 + 4 * n; // EmbeddedFile vtable
    // First EmbeddedFile table; its i64 fields sit at +4 and +12, so the table
    // itself must start at 4 (mod 8).
    let ef_pad: u32 = if (52 + 4 * n) % 8 == 4 { 0 } else { 4 };
    let ef_start: u32 = 52 + 4 * n + ef_pad;
    let str_start: u32 = ef_start + 24 * n; // first string

    // FlatBuffers strings: 4-byte length prefix + bytes + 1 null terminator,
    // padded to a 4-byte boundary so the next string is also 4-byte aligned.
    let flat_str_size = |s: &str| -> u32 { (4 + s.len() as u32 + 1 + 3) & !3 };

    let str0 = str_start;
    let str1 = str0 + flat_str_size(file_identifier);
    let str2 = str1 + flat_str_size(software);

    let mut buf = Vec::new();

    // [0..4] root uoffset → Footer table
    buf.extend_from_slice(&footer_table.to_le_bytes());

    // [4..16] Footer vtable
    buf.extend_from_slice(&12u16.to_le_bytes()); // vt_size
    buf.extend_from_slice(&20u16.to_le_bytes()); // obj_size
    buf.extend_from_slice(&4u16.to_le_bytes()); // field 0 at table+4
    buf.extend_from_slice(&8u16.to_le_bytes()); // field 1 at table+8
    buf.extend_from_slice(&12u16.to_le_bytes()); // field 2 at table+12
    buf.extend_from_slice(&16u16.to_le_bytes()); // field 3 at table+16

    // [16..36] Footer table: soffset (positive, vtable is before table) + 4 uoffsets
    let footer_soffset = (footer_table - footer_vtable) as i32; // 16 - 4 = 12
    buf.extend_from_slice(&footer_soffset.to_le_bytes());
    // field uoffsets: stored at footer_table+4, +8, +12, +16
    buf.extend_from_slice(&(str0 - (footer_table + 4)).to_le_bytes()); // field 0
    buf.extend_from_slice(&(str1 - (footer_table + 8)).to_le_bytes()); // field 1
    buf.extend_from_slice(&(str2 - (footer_table + 12)).to_le_bytes()); // field 2
    buf.extend_from_slice(&(contents_vec - (footer_table + 16)).to_le_bytes()); // field 3

    // [36..40+4N] contents vector: count + N element uoffsets
    buf.extend_from_slice(&n.to_le_bytes());
    for i in 0..n {
        // element i stored at pos 40+4i; EF table i at ef_start+i*24
        let elem_pos = 40 + 4 * i;
        let target = ef_start + i * 24;
        buf.extend_from_slice(&(target - elem_pos).to_le_bytes());
    }

    // [40+4N..52+4N] EmbeddedFile vtable (shared by all EF tables)
    // EF table fields: i64 offset@+4, i64 length@+12, i16 format@+20, i16 content_type@+22
    buf.extend_from_slice(&12u16.to_le_bytes()); // vt_size
    buf.extend_from_slice(&24u16.to_le_bytes()); // obj_size
    buf.extend_from_slice(&4u16.to_le_bytes()); // field 0: i64 offset
    buf.extend_from_slice(&12u16.to_le_bytes()); // field 1: i64 length
    buf.extend_from_slice(&20u16.to_le_bytes()); // field 2: i16 format
    buf.extend_from_slice(&22u16.to_le_bytes()); // field 3: i16 content_type
    buf.extend(std::iter::repeat_n(0u8, ef_pad as usize));

    // [52+4N..] EmbeddedFile tables (24 bytes each)
    for (i, &(offset, length, content_type)) in contents.iter().enumerate() {
        // EF table i at ef_start + i*24; EF vtable at ef_vtable
        // soffset = table_pos - vtable_pos (must be positive: table is after vtable)
        let table_pos = ef_start + i as u32 * 24;
        let soffset = (table_pos - ef_vtable) as i32;
        buf.extend_from_slice(&soffset.to_le_bytes());
        buf.extend_from_slice(&offset.to_le_bytes());
        buf.extend_from_slice(&length.to_le_bytes());
        buf.extend_from_slice(&0i16.to_le_bytes()); // format = FeatherV2
        buf.extend_from_slice(&content_type.to_le_bytes());
    }

    // Strings: u32 length + UTF-8 bytes + null terminator + alignment padding.
    for s in [file_identifier, software, pod5_version] {
        buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
        buf.extend_from_slice(s.as_bytes());
        buf.push(0); // null terminator required by FlatBuffers verifier
        let written = 4 + s.len() + 1;
        let pad = ((written + 3) & !3) - written;
        buf.extend(std::iter::repeat_n(0u8, pad));
    }

    buf.resize(buf.len().next_multiple_of(8), 0);
    buf
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal valid FlatBuffer for a Footer with three string fields
    /// and no contents vector.
    ///
    /// Buffer layout:
    ///   [0..4]   root_uoffset = 4 (root table at pos 4)
    ///   [4..8]   soffset (i32) = 4 - 20 = -16 → vtable at pos 20
    ///   [8..12]  uoffset → file_id string   (field 0)
    ///   [12..16] uoffset → software string  (field 1)
    ///   [16..20] uoffset → version string   (field 2)
    ///   [20..32] vtable: vt_size=12, obj_size=16, offsets=[4,8,12,0]
    ///   [32..]   string data (u32 length prefix, then bytes, no null)
    fn build_footer_fb(file_id: &str, software: &str, version: &str) -> Vec<u8> {
        const TABLE_POS: usize = 4;
        const VTABLE_POS: usize = 20;
        const STRING_START: usize = 32;

        let file_id_pos = STRING_START;
        let software_pos = file_id_pos + 4 + file_id.len();
        let version_pos = software_pos + 4 + software.len();

        let mut buf = Vec::new();

        // root_uoffset
        buf.extend_from_slice(&(TABLE_POS as u32).to_le_bytes());
        // soffset: table_pos - vtable_pos = 4 - 20 = -16
        buf.extend_from_slice(&(TABLE_POS as i32 - VTABLE_POS as i32).to_le_bytes());
        // uoffset for field 0 (stored at pos 8)
        buf.extend_from_slice(&((file_id_pos - 8) as u32).to_le_bytes());
        // uoffset for field 1 (stored at pos 12)
        buf.extend_from_slice(&((software_pos - 12) as u32).to_le_bytes());
        // uoffset for field 2 (stored at pos 16)
        buf.extend_from_slice(&((version_pos - 16) as u32).to_le_bytes());
        // buf is now 20 bytes

        // vtable (12 bytes) at pos 20
        buf.extend_from_slice(&12u16.to_le_bytes()); // vt_size
        buf.extend_from_slice(&16u16.to_le_bytes()); // obj_size (pos 4..20 = 16 bytes)
        buf.extend_from_slice(&4u16.to_le_bytes()); // field 0 offset from table: 8-4=4
        buf.extend_from_slice(&8u16.to_le_bytes()); // field 1: 12-4=8
        buf.extend_from_slice(&12u16.to_le_bytes()); // field 2: 16-4=12
        buf.extend_from_slice(&0u16.to_le_bytes()); // field 3 absent
        // buf is now 32 bytes

        // strings
        buf.extend_from_slice(&(file_id.len() as u32).to_le_bytes());
        buf.extend_from_slice(file_id.as_bytes());
        buf.extend_from_slice(&(software.len() as u32).to_le_bytes());
        buf.extend_from_slice(software.as_bytes());
        buf.extend_from_slice(&(version.len() as u32).to_le_bytes());
        buf.extend_from_slice(version.as_bytes());

        buf
    }

    #[test]
    fn parse_too_short() {
        let err = parse(&[1, 2, 3]).unwrap_err();
        assert!(matches!(err, crate::Error::FlatBuffers(_)));
    }

    #[test]
    fn parse_minimal_footer() {
        let fb = build_footer_fb("my-uuid", "test-software", "1.2.3");
        let footer = parse(&fb).expect("parse failed");
        assert_eq!(footer.file_identifier, "my-uuid");
        assert_eq!(footer.software, "test-software");
        assert_eq!(footer.pod5_version, "1.2.3");
        assert!(footer.contents.is_empty());
    }

    #[test]
    fn build_parse_round_trip_with_contents() {
        let contents = vec![
            (1024i64, 512i64, 4i16),  // RunInfoTable
            (2048i64, 1024i64, 1i16), // SignalTable
            (4096i64, 2048i64, 0i16), // ReadsTable
        ];
        let fb = build_footer("file-uuid", "pod5-rs/0.1", "1.0.0", &contents);
        let footer = parse(&fb).expect("round-trip parse failed");

        assert_eq!(footer.file_identifier, "file-uuid");
        assert_eq!(footer.software, "pod5-rs/0.1");
        assert_eq!(footer.pod5_version, "1.0.0");
        assert_eq!(footer.contents.len(), 3);

        assert_eq!(footer.contents[0].offset, 1024);
        assert_eq!(footer.contents[0].length, 512);
        assert_eq!(footer.contents[0].content_type, ContentType::RunInfoTable);

        assert_eq!(footer.contents[1].offset, 2048);
        assert_eq!(footer.contents[1].length, 1024);
        assert_eq!(footer.contents[1].content_type, ContentType::SignalTable);

        assert_eq!(footer.contents[2].offset, 4096);
        assert_eq!(footer.contents[2].length, 2048);
        assert_eq!(footer.contents[2].content_type, ContentType::ReadsTable);
    }

    #[test]
    fn build_footer_alignment() {
        for n in 0..5 {
            let contents: Vec<_> = (0..n).map(|i| (i * 8, 8, 0i16)).collect();
            let fb = build_footer("file-uuid", "sw", "0.3.48", &contents);
            assert_eq!(fb.len() % 8, 0, "footer size not 8-aligned for n={n}");
            // Follow each contents element's uoffset (at 40 + 4i) to its table;
            // the i64 fields are at table+4 and table+12.
            for i in 0..n as usize {
                let elem = 40 + 4 * i;
                let table =
                    elem + u32::from_le_bytes(fb[elem..elem + 4].try_into().unwrap()) as usize;
                assert_eq!((table + 4) % 8, 0, "misaligned i64 for n={n}, i={i}");
            }
            assert_eq!(parse(&fb).unwrap().contents.len(), n as usize);
        }
    }

    #[test]
    fn build_parse_empty_contents() {
        let fb = build_footer("id", "sw", "ver", &[]);
        let footer = parse(&fb).expect("empty contents parse failed");
        assert_eq!(footer.file_identifier, "id");
        assert!(footer.contents.is_empty());
    }

    #[test]
    fn content_type_mapping() {
        assert_eq!(ContentType::from_i16(0), ContentType::ReadsTable);
        assert_eq!(ContentType::from_i16(1), ContentType::SignalTable);
        assert_eq!(ContentType::from_i16(4), ContentType::RunInfoTable);
        assert_eq!(ContentType::from_i16(99), ContentType::Other(99));
        assert_eq!(ContentType::from_i16(-1), ContentType::Other(-1));
    }
}
