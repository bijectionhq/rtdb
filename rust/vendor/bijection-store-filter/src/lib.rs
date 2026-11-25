//! Value filters a read pushes down to the store (RTDB-PLAN, owner decision 1).
//!
//! The kernel defines what a filter means, here and only here; a store runs
//! this same code next to the data. A filtered read returns exactly the
//! entries an unfiltered read of the same ranges and version returns whose
//! whole value [`matches`](ValueFilter::matches), and the contract suite checks
//! that equality. A filter narrows what a page carries, never what the read
//! covered: the read set of a filtered read is still its ranges.
//!
//! Comparisons are over bytes in their original order, which is the order of
//! the canonical value encoding (`value_order::try_compare_bytes`): a field the
//! kernel stores in that encoding compares here exactly as queries compare it.
//!
//! This file depends on `std` alone, so the store can carry it verbatim. RTDB
//! vendors it as `rust/vendor/bijection-store-filter/src/lib.rs`; change it here
//! and copy it there in the paired RTDB commit.

use std::cmp::Ordering;

/// The most nodes one filter may have.
pub const MAX_NODES: usize = 1024;
/// The deepest one filter may nest.
pub const MAX_DEPTH: usize = 32;

/// Which bytes of a value a predicate looks at.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Field {
    /// The whole value.
    Whole,
    /// The bytes `[start, start + len)`, clipped to the value's length, as
    /// a slice projection reads them.
    Slice { start: u32, len: u32 },
}

impl Field {
    pub fn of<'a>(&self, value: &'a [u8]) -> &'a [u8] {
        match *self {
            Self::Whole => value,
            Self::Slice { start, len } => {
                let start = (start as usize).min(value.len());
                let end = start.saturating_add(len as usize).min(value.len());
                &value[start..end]
            }
        }
    }
}

/// A comparison of a field with an operand, in original byte order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Comparison {
    Equal,
    NotEqual,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
}

impl Comparison {
    fn holds(self, order: Ordering) -> bool {
        match self {
            Self::Equal => order == Ordering::Equal,
            Self::NotEqual => order != Ordering::Equal,
            Self::Less => order == Ordering::Less,
            Self::LessOrEqual => order != Ordering::Greater,
            Self::Greater => order == Ordering::Greater,
            Self::GreaterOrEqual => order != Ordering::Less,
        }
    }

    fn tag(self) -> u8 {
        match self {
            Self::Equal => 0,
            Self::NotEqual => 1,
            Self::Less => 2,
            Self::LessOrEqual => 3,
            Self::Greater => 4,
            Self::GreaterOrEqual => 5,
        }
    }

    fn from_tag(tag: u8) -> Option<Self> {
        Some(match tag {
            0 => Self::Equal,
            1 => Self::NotEqual,
            2 => Self::Less,
            3 => Self::LessOrEqual,
            4 => Self::Greater,
            5 => Self::GreaterOrEqual,
            _ => return None,
        })
    }
}

/// A predicate over one whole stored value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ValueFilter {
    Compare { field: Field, comparison: Comparison, operand: Vec<u8> },
    Prefix { field: Field, prefix: Vec<u8> },
    /// Every child holds; an empty list holds.
    All(Vec<ValueFilter>),
    /// Some child holds; an empty list does not.
    Any(Vec<ValueFilter>),
    Not(Box<ValueFilter>),
}

impl ValueFilter {
    /// Whether `value` passes. This is the filter's whole meaning.
    pub fn matches(&self, value: &[u8]) -> bool {
        match self {
            Self::Compare { field, comparison, operand } => comparison.holds(field.of(value).cmp(operand.as_slice())),
            Self::Prefix { field, prefix } => field.of(value).starts_with(prefix),
            Self::All(children) => children.iter().all(|child| child.matches(value)),
            Self::Any(children) => children.iter().any(|child| child.matches(value)),
            Self::Not(child) => !child.matches(value),
        }
    }

    /// The filter's wire form. [`decode`](Self::decode) is its inverse.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode_into(&mut out);
        out
    }

    fn encode_into(&self, out: &mut Vec<u8>) {
        fn field(out: &mut Vec<u8>, field: &Field) {
            match *field {
                Field::Whole => out.push(0),
                Field::Slice { start, len } => {
                    out.push(1);
                    out.extend_from_slice(&start.to_be_bytes());
                    out.extend_from_slice(&len.to_be_bytes());
                }
            }
        }
        fn bytes(out: &mut Vec<u8>, bytes: &[u8]) {
            out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            out.extend_from_slice(bytes);
        }
        match self {
            Self::Compare { field: f, comparison, operand } => {
                out.push(1);
                field(out, f);
                out.push(comparison.tag());
                bytes(out, operand);
            }
            Self::Prefix { field: f, prefix } => {
                out.push(2);
                field(out, f);
                bytes(out, prefix);
            }
            Self::All(children) | Self::Any(children) => {
                out.push(if matches!(self, Self::All(_)) { 3 } else { 4 });
                out.extend_from_slice(&(children.len() as u32).to_be_bytes());
                for child in children {
                    child.encode_into(out);
                }
            }
            Self::Not(child) => {
                out.push(5);
                child.encode_into(out);
            }
        }
    }

    /// Decode one filter. Refuses trailing bytes, more than [`MAX_NODES`]
    /// nodes and nesting deeper than [`MAX_DEPTH`].
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut decoder = Decoder { bytes, nodes: 0 };
        let filter = decoder.filter(0)?;
        if decoder.bytes.is_empty() { Ok(filter) } else { Err(DecodeError("trailing bytes after a value filter")) }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecodeError(pub &'static str);

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid value filter: {}", self.0)
    }
}

impl std::error::Error for DecodeError {}

struct Decoder<'a> {
    bytes: &'a [u8],
    nodes: usize,
}

impl Decoder<'_> {
    fn take(&mut self, len: usize) -> Result<&[u8], DecodeError> {
        if self.bytes.len() < len {
            return Err(DecodeError("truncated"));
        }
        let (head, tail) = self.bytes.split_at(len);
        self.bytes = tail;
        Ok(head)
    }

    fn byte(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }

    fn word(&mut self) -> Result<u32, DecodeError> {
        let word = self.take(4)?;
        Ok(u32::from_be_bytes([word[0], word[1], word[2], word[3]]))
    }

    fn bytes(&mut self) -> Result<Vec<u8>, DecodeError> {
        let len = self.word()? as usize;
        Ok(self.take(len)?.to_vec())
    }

    fn field(&mut self) -> Result<Field, DecodeError> {
        match self.byte()? {
            0 => Ok(Field::Whole),
            1 => Ok(Field::Slice { start: self.word()?, len: self.word()? }),
            _ => Err(DecodeError("unknown field")),
        }
    }

    fn filter(&mut self, depth: usize) -> Result<ValueFilter, DecodeError> {
        if depth > MAX_DEPTH {
            return Err(DecodeError("nested too deeply"));
        }
        self.nodes += 1;
        if self.nodes > MAX_NODES {
            return Err(DecodeError("too many nodes"));
        }
        match self.byte()? {
            1 => {
                let field = self.field()?;
                let comparison = Comparison::from_tag(self.byte()?).ok_or(DecodeError("unknown comparison"))?;
                Ok(ValueFilter::Compare { field, comparison, operand: self.bytes()? })
            }
            2 => Ok(ValueFilter::Prefix { field: self.field()?, prefix: self.bytes()? }),
            tag @ (3 | 4) => {
                let count = self.word()? as usize;
                if count > MAX_NODES {
                    return Err(DecodeError("too many nodes"));
                }
                let children = (0..count).map(|_| self.filter(depth + 1)).collect::<Result<Vec<_>, _>>()?;
                Ok(if tag == 3 { ValueFilter::All(children) } else { ValueFilter::Any(children) })
            }
            5 => Ok(ValueFilter::Not(Box::new(self.filter(depth + 1)?))),
            _ => Err(DecodeError("unknown node")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn first(comparison: Comparison, operand: &[u8]) -> ValueFilter {
        ValueFilter::Compare { field: Field::Slice { start: 0, len: 1 }, comparison, operand: operand.to_vec() }
    }

    #[test]
    fn comparisons_follow_original_byte_order() {
        assert!(first(Comparison::Equal, b"a").matches(b"abc"));
        assert!(first(Comparison::Less, b"b").matches(b"abc"));
        assert!(!first(Comparison::Greater, b"b").matches(b"abc"));
        assert!(first(Comparison::GreaterOrEqual, b"a").matches(b"a"));
        // A clipped field is shorter, so it orders before a longer operand.
        assert!(first(Comparison::Less, b"a").matches(b""));
        let prefix = ValueFilter::Prefix { field: Field::Whole, prefix: b"ab".to_vec() };
        assert!(prefix.matches(b"abc") && !prefix.matches(b"ac"));
        assert!(ValueFilter::All(Vec::new()).matches(b"") && !ValueFilter::Any(Vec::new()).matches(b""));
        assert!(ValueFilter::Not(Box::new(prefix)).matches(b"ac"));
    }

    #[test]
    fn the_wire_form_round_trips_and_is_bounded() {
        let filter = ValueFilter::Any(vec![
            ValueFilter::All(vec![first(Comparison::NotEqual, b"\0"), ValueFilter::Prefix { field: Field::Whole, prefix: b"x".to_vec() }]),
            ValueFilter::Not(Box::new(first(Comparison::LessOrEqual, b"\x05"))),
        ]);
        assert_eq!(ValueFilter::decode(&filter.encode()), Ok(filter.clone()));
        let mut trailing = filter.encode();
        trailing.push(0);
        assert!(ValueFilter::decode(&trailing).is_err());
        assert!(ValueFilter::decode(&filter.encode()[..3]).is_err());
        let mut deep = first(Comparison::Equal, b"a");
        for _ in 0..=MAX_DEPTH {
            deep = ValueFilter::Not(Box::new(deep));
        }
        assert!(ValueFilter::decode(&deep.encode()).is_err());
    }
}
