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
