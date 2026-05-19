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

impl<'a> Fb<'a> {
    #[inline]
    fn u16_at(&self, pos: usize) -> u16 {
        u16::from_le_bytes(self.buf[pos..pos + 2].try_into().unwrap())
    }
    #[inline]
    fn u32_at(&self, pos: usize) -> u32 {
        u32::from_le_bytes(self.buf[pos..pos + 4].try_into().unwrap())
    }
    #[inline]
    fn i32_at(&self, pos: usize) -> i32 {
        i32::from_le_bytes(self.buf[pos..pos + 4].try_into().unwrap())
    }
    #[inline]
    fn i64_at(&self, pos: usize) -> i64 {
        i64::from_le_bytes(self.buf[pos..pos + 8].try_into().unwrap())
    }
    #[inline]
    fn i16_at(&self, pos: usize) -> i16 {
        i16::from_le_bytes(self.buf[pos..pos + 2].try_into().unwrap())
    }

    /// Dereference a forward `uoffset_t`: returns the target absolute position.
    #[inline]
    fn follow_u(&self, pos: usize) -> usize {
        pos + self.u32_at(pos) as usize
    }

    /// Absolute position of the root table.
    fn root(&self) -> usize {
        self.u32_at(0) as usize
    }

    /// Absolute position of a field in a table, or `None` if absent/default.
    fn field(&self, table: usize, id: usize) -> Option<usize> {
        // soffset at start of table points backwards to the vtable
        let soffset = self.i32_at(table);
        let vtable = (table as i64 - soffset as i64) as usize;
        let vt_size = self.u16_at(vtable) as usize;
        let entry = vtable + 4 + id * 2;
        if entry + 2 > vtable + vt_size {
            return None;
        }
        let offset = self.u16_at(entry) as usize;
        if offset == 0 {
            None
        } else {
            Some(table + offset)
        }
    }

    /// Dereference a string field (field_pos → uoffset → length-prefixed UTF-8).
    fn string_at(&self, field_pos: usize) -> Result<&'a str> {
        let s = self.follow_u(field_pos);
        let len = self.u32_at(s) as usize;
        std::str::from_utf8(&self.buf[s + 4..s + 4 + len]).map_err(Error::Utf8)
    }

    /// Dereference a vector field → `(vector_start, element_count)`.
    fn vector_at(&self, field_pos: usize) -> (usize, usize) {
        let v = self.follow_u(field_pos);
        (v, self.u32_at(v) as usize)
    }

    /// Table at index `i` inside a vector of tables.
    fn vector_table(&self, vec_start: usize, i: usize) -> usize {
        let ref_pos = vec_start + 4 + i * 4;
        self.follow_u(ref_pos)
    }
}

// ── Domain types ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentType {
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
pub struct EmbeddedFile {
    /// Absolute byte offset in the POD5 file.
    pub offset: i64,
    /// Byte length of the embedded Arrow IPC file, excluding alignment padding.
    pub length: i64,
    pub content_type: ContentType,
}

#[derive(Debug)]
pub struct Footer {
    pub file_identifier: String,
    pub pod5_version: String,
    pub software: String,
    pub contents: Vec<EmbeddedFile>,
}

// ── Parser ────────────────────────────────────────────────────────────────────

pub fn parse(data: &[u8]) -> Result<Footer> {
    if data.len() < 4 {
        return Err(Error::FlatBuffers("buffer too short".into()));
    }
    let fb = Fb { buf: data };
    let root = fb.root();

    let file_identifier = fb
        .field(root, 0)
        .map(|p| fb.string_at(p))
        .transpose()?
        .unwrap_or("")
        .to_owned();

    let software = fb
        .field(root, 1)
        .map(|p| fb.string_at(p))
        .transpose()?
        .unwrap_or("")
        .to_owned();

    let pod5_version = fb
        .field(root, 2)
        .map(|p| fb.string_at(p))
        .transpose()?
        .unwrap_or("")
        .to_owned();

    let contents = match fb.field(root, 3) {
        None => Vec::new(),
        Some(contents_pos) => {
            let (vec_start, count) = fb.vector_at(contents_pos);
            (0..count)
                .map(|i| {
                    let tbl = fb.vector_table(vec_start, i);
                    let offset = fb.field(tbl, 0).map(|p| fb.i64_at(p)).unwrap_or(0);
                    let length = fb.field(tbl, 1).map(|p| fb.i64_at(p)).unwrap_or(0);
                    let content_type =
                        ContentType::from_i16(fb.field(tbl, 3).map(|p| fb.i16_at(p)).unwrap_or(0));
                    EmbeddedFile {
                        offset,
                        length,
                        content_type,
                    }
                })
                .collect()
        }
    };

    Ok(Footer {
        file_identifier,
        pod5_version,
        software,
        contents,
    })
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
    fn content_type_mapping() {
        assert_eq!(ContentType::from_i16(0), ContentType::ReadsTable);
        assert_eq!(ContentType::from_i16(1), ContentType::SignalTable);
        assert_eq!(ContentType::from_i16(4), ContentType::RunInfoTable);
        assert_eq!(ContentType::from_i16(99), ContentType::Other(99));
        assert_eq!(ContentType::from_i16(-1), ContentType::Other(-1));
    }
}
