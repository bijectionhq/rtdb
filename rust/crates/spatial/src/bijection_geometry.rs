use std::borrow::Cow;

use geometry::{GeometryWork, Gv, Scalar, ValueView};
use rtdb_types::Result;

use crate::kernel::{geometry_error, invalid};

#[derive(Clone, Copy, Default)]
struct Span {
    start: u32,
    next: u32,
    children: u32,
}

pub(super) struct Decoded<'a> {
    bytes: &'a [u8],
    spans: Spans,
}

impl Decoded<'_> {
    pub(super) fn value(&self) -> Value<'_> {
        Value { bytes: self.bytes, raw: self.bytes, spans: self.spans.as_slice(), index: 0 }
    }
}

enum Spans {
    Inline { entries: [Span; 8], len: usize },
    Heap(Vec<Span>),
}

impl Spans {
    fn as_slice(&self) -> &[Span] {
        match self {
            Self::Inline { entries, len } => &entries[..*len],
            Self::Heap(entries) => entries,
        }
    }

    fn push(&mut self, span: Span) {
        match self {
            Self::Inline { entries, len } if *len < entries.len() => {
                entries[*len] = span;
                *len += 1;
            }
            Self::Inline { entries, .. } => {
                let mut heap = Vec::with_capacity(entries.len() + 1);
                heap.extend_from_slice(entries);
                heap.push(span);
                *self = Self::Heap(heap);
            }
            Self::Heap(entries) => {
                if entries.capacity() == entries.len() {
                    entries.reserve_exact((entries.len() / 2).max(1));
                }
                entries.push(span);
            }
        }
    }

    fn set(&mut self, index: usize, span: Span) {
        match self {
            Self::Inline { entries, .. } => entries[index] = span,
            Self::Heap(entries) => entries[index] = span,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct Value<'a> {
    bytes: &'a [u8],
    raw: &'a [u8],
    spans: &'a [Span],
    index: usize,
}

pub(super) fn decode<'a>(bytes: &'a [u8], work: &mut GeometryWork) -> Result<Decoded<'a>> {
    let mut decoded = Decoded { bytes, spans: Spans::Inline { entries: [Span::default(); 8], len: 0 } };
    let mut tail = bytes;
    decoded.validate(&mut tail, 0, 0, work)?;
    if !tail.is_empty() {
        return Err(invalid("Bijection geometry has trailing bytes"));
    }
    Ok(decoded)
}

fn take<'a>(bytes: &mut &'a [u8], length: usize) -> Result<&'a [u8]> {
    let (value, tail) = (*bytes).split_at_checked(length).ok_or_else(truncated)?;
    *bytes = tail;
    Ok(value)
}

fn truncated() -> rtdb_types::Anomaly {
    invalid("Bijection geometry is truncated")
}

fn text<'a>(bytes: &mut &'a [u8]) -> Result<Cow<'a, str>> {
    let mut escaped = None::<Vec<u8>>;
    loop {
        let end = bytes.iter().position(|&byte| byte == 0).ok_or_else(truncated)?;
        let chunk = take(bytes, end)?;
        match take(bytes, 2)? {
            [0, 1] => {
                let invalid_utf8 = |_| invalid("Bijection geometry text must be UTF-8");
                return if let Some(mut decoded) = escaped {
                    decoded.extend_from_slice(chunk);
                    String::from_utf8(decoded).map(Cow::Owned)
                        .map_err(|_| invalid("Bijection geometry text must be UTF-8"))
                } else {
                    std::str::from_utf8(chunk).map(Cow::Borrowed).map_err(invalid_utf8)
                };
            }
            [0, 0xff] => {
                let decoded = escaped.get_or_insert_with(Vec::new);
                decoded.extend_from_slice(chunk);
                decoded.push(0);
            }
            _ => return Err(invalid("Invalid Bijection geometry text delimiter")),
        }
    }
}

fn validate_text<'a>(bytes: &mut &'a [u8], work: &mut GeometryWork) -> Result<&'a [u8]> {
    let start = *bytes;
    let mut valid_utf8 = true;
    loop {
        let end = bytes
            .iter()
            .position(|&byte| byte == 0)
            .ok_or_else(truncated)?;
        work.charge(end + 2).map_err(geometry_error)?;
        work.allocate(end + 1).map_err(geometry_error)?;
        let chunk = take(bytes, end)?;
        // An escaped NUL separates complete UTF-8 sequences.
        valid_utf8 &= std::str::from_utf8(chunk).is_ok();
        match take(bytes, 2)? {
            [0, 1] => break,
            [0, 0xff] => (),
            _ => return Err(invalid("Invalid Bijection geometry text delimiter")),
        }
    }
    if !valid_utf8 {
        return Err(invalid("Bijection geometry text must be UTF-8"));
    }
    Ok(&start[..start.len() - bytes.len() - 2])
}

fn float(bytes: &[u8]) -> f64 {
    let encoded = u64::from_be_bytes(bytes.try_into().expect("eight bytes"));
    f64::from_bits(if encoded < (1 << 63) { !encoded } else { encoded ^ (1 << 63) })
}

impl<'a> Decoded<'a> {
    fn offset(&self, tail: &[u8]) -> Result<u32> {
        u32::try_from(self.bytes.len() - tail.len()).map_err(|_| crate::postings::work_limit())
    }

    fn validate(&mut self, bytes: &mut &'a [u8], start: u32, depth: usize, work: &mut GeometryWork) -> Result<()> {
        if depth > 64 {
            return Err(invalid("Bijection geometry is too deeply nested"));
        }
        work.charge(1).map_err(geometry_error)?;
        // The existing per-value allowance includes span storage and its growth.
        work.allocate(size_of::<Gv>()).map_err(geometry_error)?;
        let index = self.spans.as_slice().len();
        self.spans.push(Span { start, next: 0, children: 0 });
        let mut children = 0;
        match take(bytes, 1)?[0] {
            2 => (),
            3 => {
                if take(bytes, 1)?[0] > 1 {
                    return Err(invalid("Invalid Bijection geometry boolean"));
                }
            }
            4 => { take(bytes, 8)?; }
            5 => {
                let value = float(take(bytes, 8)?);
                if value.is_nan() && value.to_bits() != 0x7ff8_0000_0000_0000 {
                    return Err(invalid("Invalid Bijection geometry float"));
                }
            }
            9 => { validate_text(bytes, work)?; }
            12 => {
                while !bytes.starts_with(&[0, 0]) {
                    let start = self.offset(bytes)?;
                    self.validate(bytes, start, depth + 1, work)?;
                    children += 1;
                }
                take(bytes, 2)?;
            }
            13 => {
                let mut previous = None;
                while !bytes.starts_with(&[0, 0]) {
                    let start = self.offset(bytes)?;
                    let key = validate_text(bytes, work)?;
                    if previous.is_some_and(|previous| previous >= key) {
                        return Err(invalid("Bijection geometry fields must be strictly ordered"));
                    }
                    work.allocate(size_of::<String>()).map_err(geometry_error)?;
                    previous = Some(key);
                    self.validate(bytes, start, depth + 1, work)?;
                    children += 1;
                }
                take(bytes, 2)?;
            }
            _ => return Err(invalid("Unsupported canonical Bijection geometry value")),
        }
        let next = u32::try_from(self.spans.as_slice().len()).map_err(|_| crate::postings::work_limit())?;
        self.spans.set(index, Span { start, next, children });
        Ok(())
    }
}

impl<'a> Value<'a> {
    fn children(self) -> impl ExactSizeIterator<Item = Self> {
        let mut next = self.index + 1;
        (0..self.spans[self.index].children as usize).map(move |_| {
            let index = next;
            let span = self.spans[index];
            next = span.next as usize;
            Self { raw: &self.bytes[span.start as usize..], index, ..self }
        })
    }
}

impl<'a> ValueView<'a> for Value<'a> {
    fn scalar(self) -> Scalar {
        match self.raw[0] {
            2 => Scalar::Null,
            3 => Scalar::Bool(self.raw[1] == 1),
            5 => Scalar::Float(float(&self.raw[1..9])),
            _ => Scalar::Other,
        }
    }

    fn text(self) -> Option<Cow<'a, str>> {
        let mut rest = self.raw.strip_prefix(&[9])?;
        Some(text(&mut rest).expect("validated text"))
    }

    fn array(self) -> Option<impl ExactSizeIterator<Item = Self>> {
        (self.raw[0] == 12).then(|| self.children())
    }

    fn object(self) -> Option<impl Iterator<Item = (Cow<'a, str>, Self)>> {
        (self.raw[0] == 13).then(|| self.children().map(|mut child| {
            let key = text(&mut child.raw).expect("validated key");
            (key, child)
        }))
    }
}
