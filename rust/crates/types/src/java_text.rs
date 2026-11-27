//! Exact Java string units and retained text storage.
//! Encoding and object identity belong to the consuming boundary.

use std::{alloc::Layout, fmt, ops::Range, sync::{Arc, atomic::AtomicUsize}};
use crate::{Anomaly, MemoryBudget, MemoryReservation, Result};

mod iter;
pub use iter::{JavaCodePoints, JavaUtf8Bytes, Utf16Units};

/// An immutable text value whose clones share storage and its retained charge.
/// Equality compares UTF16 units across backing representations; sharing confers no Java object identity.
/// Charged constructors retain their budget through the final clone's destruction.
/// Debug output escapes individual units and is not a serialized or Java-rendered string.
#[derive(Clone)]
pub struct JavaString(Backing);

#[derive(Clone)]
enum Backing {
    // Storage must be destroyed before the final reservation handle.
    Tracked { storage: Arc<TextStorage>, reservation: Arc<MemoryReservation> },
    Untracked(Arc<TextStorage>),
}

struct TextStorage { text: Text, retained_bytes: usize }
enum Text { Utf8 { value: Box<str>, units: usize }, Utf16(Box<[u16]>) }

#[derive(Clone, Copy)]
enum Source<'a> { Utf8(&'a str), Utf16(&'a [u16]) }

/// An allocation-free borrowed range, indexed in UTF16 units.
/// Ranges may split supplementary pairs and retain unmatched surrogates exactly.
#[derive(Clone, Copy)]
pub struct JavaText<'a> { source: Source<'a>, start: usize, end: usize }

impl JavaString {
    /// Copies UTF8 after admitting its boxed backing, shared owner and bookkeeping.
    /// Rejects more than `i32::MAX` UTF16 units, unaddressable storage or budget exhaustion.
    /// The caller keeps the source's own charge until this copy completes.
    pub fn copy_utf8(source: &str, memory: &Arc<MemoryBudget>) -> Result<Self> {
        Self::copy(Source::Utf8(source), Some(memory))
    }

    /// Copies every supplied unit, including unmatched surrogates, after finite admission.
    /// Uses the same size, budget and source-lifetime contract as `copy_utf8`.
    pub fn copy_utf16(source: &[u16], memory: &Arc<MemoryBudget>) -> Result<Self> {
        Self::copy(Source::Utf16(source), Some(memory))
    }

    /// Copies exact UTF16 units after the caller admits `allocation_bound_utf16(source)` bytes.
    /// Calls `admit` once with that byte bound before copying storage; size errors precede that call and authority errors propagate unchanged.
    /// The admitted bytes must cover this copy in addition to the source and every other live allocation.
    /// Rejects a returned reservation smaller than the bound; larger reservations remain retained in full through the final clone.
    /// The authority is not retained, and the source's own charge must remain held until the copy completes.
    pub fn copy_utf16_with_admission(source: &[u16], admit: impl FnOnce(usize) -> Result<Arc<MemoryReservation>>) -> Result<Self> {
        let source = Source::Utf16(source);
        let (units, retained_bytes) = source.allocation()?;
        let reservation = admit(retained_bytes)?;
        if reservation.bytes() < retained_bytes {
            return Err(Anomaly::incorrect("rtdb.types/java-text-admission", "Caller returned an insufficient Java text reservation"));
        }
        Ok(Self::copy_storage(source, units, retained_bytes, Some(reservation)))
    }

    /// Copies without a reservation for explicitly untracked programmatic contexts.
    /// The native unit-count and storage-size checks still apply.
    pub fn copy_utf8_untracked(source: &str) -> Result<Self> { Self::copy(Source::Utf8(source), None) }

    /// Copies exact units without a reservation for explicitly untracked programmatic contexts.
    /// Rejects more than `i32::MAX` units or unaddressable storage.
    pub fn copy_utf16_untracked(source: &[u16]) -> Result<Self> { Self::copy(Source::Utf16(source), None) }

    /// Borrows the complete value in constant time without copying or encoding it.
    pub fn view(&self) -> JavaText<'_> {
        match &self.storage().text {
            Text::Utf8 { value, units } => JavaText { source: Source::Utf8(value), start: 0, end: *units },
            Text::Utf16(value) => JavaText::from_utf16(value),
        }
    }

    /// Returns the full reservation required by `copy_utf8`, including an empty value's owner.
    /// Counts UTF16 units in linear time and returns the constructor's size errors.
    pub fn allocation_bound_utf8(source: &str) -> Result<usize> { Source::Utf8(source).allocation().map(|(_, bytes)| bytes) }

    /// Returns the full reservation required by `copy_utf16`, or its size error, without allocating.
    pub fn allocation_bound_utf16(source: &[u16]) -> Result<usize> { Source::Utf16(source).allocation().map(|(_, bytes)| bytes) }

    /// Returns the entire tracked reservation, shared by every clone, including any caller-supplied excess admission.
    /// Untracked backing returns its calculated copy bound, including bookkeeping and excluding containing nodes.
    /// Acquires no additional charge.
    pub fn retained_bytes(&self) -> usize {
        match &self.0 {
            Backing::Tracked { reservation, .. } => reservation.bytes(),
            Backing::Untracked(storage) => storage.retained_bytes,
        }
    }

    fn storage(&self) -> &TextStorage {
        match &self.0 { Backing::Tracked { storage, .. } | Backing::Untracked(storage) => storage }
    }

    fn copy(source: Source<'_>, memory: Option<&Arc<MemoryBudget>>) -> Result<Self> {
        let (units, retained_bytes) = source.allocation()?;
        let reservation = memory.map(|memory| memory.reserve(retained_bytes)).transpose()?;
        Ok(Self::copy_storage(source, units, retained_bytes, reservation))
    }

    fn copy_storage(source: Source<'_>, units: usize, retained_bytes: usize, reservation: Option<Arc<MemoryReservation>>) -> Self {
        let text = match source {
            Source::Utf8(value) => Text::Utf8 { value: Box::from(value), units },
            Source::Utf16(value) => Text::Utf16(Box::from(value)),
        };
        let storage = Arc::new(TextStorage { text, retained_bytes });
        Self(match reservation {
            Some(reservation) => Backing::Tracked { storage, reservation },
            None => Backing::Untracked(storage),
        })
    }
}

impl Source<'_> {
    fn allocation(self) -> Result<(usize, usize)> {
        let (units, layout) = match self {
            Self::Utf8(value) => (value.encode_utf16().count(), Layout::array::<u8>(value.len())),
            Self::Utf16(value) => (value.len(), Layout::array::<u16>(value.len())),
        };
        Ok((units, allocation_bound(units, layout.map_err(|_| size_error())?)?))
    }
}

fn allocation_bound(units: usize, backing: Layout) -> Result<usize> {
    if units > i32::MAX as usize { return Err(size_error()); }
    // Rust 1.94.1 alloc::sync::ArcInner: two atomic counts followed by the aligned payload.
    let owner = Layout::new::<[AtomicUsize; 2]>().extend(Layout::new::<TextStorage>())
        .map_err(|_| size_error())?.0.pad_to_align();
    backing.size().checked_add(owner.size()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)
}

fn size_error() -> Anomaly { Anomaly::incorrect("rtdb.types/java-text-size", "Java text exceeds addressable storage").with_kind(crate::AnomalyKind::ProgramLimitExceeded) }

impl<'a> JavaText<'a> {
    /// Counts UTF16 units in linear time, borrowing the original UTF8 bytes without allocation.
    pub fn from_utf8(source: &'a str) -> Self { Self { source: Source::Utf8(source), start: 0, end: source.encode_utf16().count() } }

    /// Borrows every supplied unit in constant time without validating surrogate pairs.
    pub fn from_utf16(source: &'a [u16]) -> Self { Self { source: Source::Utf16(source), start: 0, end: source.len() } }

    /// Returns this range's UTF16 unit count in constant time.
    pub fn len_units(self) -> usize { self.end - self.start }

    pub fn is_empty(self) -> bool { self.start == self.end }

    /// Selects a half-open unit range relative to this view in constant time.
    /// Reversed or out-of-bounds ranges return `None`; splitting a pair is valid.
    pub fn slice_units(self, range: Range<usize>) -> Option<Self> {
        if range.start > range.end || range.end > self.len_units() { return None; }
        Some(Self { source: self.source, start: self.start + range.start, end: self.start + range.end })
    }

    /// Iterates exact units; starting within UTF8 backing scans its preceding units once.
    /// Iteration then proceeds sequentially with constant state and no allocation.
    pub fn units(self) -> Utf16Units<'a> { Utf16Units::new(self.source, self.start, self.end) }

    /// Combines adjacent high/low pairs within this range and yields unmatched units as `u32`.
    /// Uses constant state; values in the surrogate range remain representable.
    pub fn code_points(self) -> JavaCodePoints<'a> { JavaCodePoints::new(self.units()) }

    /// Explicitly applies Java String's UTF8 encoding: valid pairs combine; each unmatched unit emits `?`.
    /// Uses constant state and no owned output.
    pub fn java_utf8_bytes(self) -> JavaUtf8Bytes<'a> { JavaUtf8Bytes::new(self.code_points()) }

    /// Counts the explicit Java UTF8 encoding without allocation; an unrepresentable byte count is an error.
    pub fn java_utf8_len(self) -> Result<usize> {
        self.java_utf8_bytes().try_fold(0usize, |bytes, _| bytes.checked_add(1).ok_or_else(size_error))
    }

    /// Computes Java String.hashCode's wrapping 31-fold over UTF16 units, without caching or allocation.
    pub fn java_hash_code(self) -> i32 { self.units().fold(0i32, |hash, unit| hash.wrapping_mul(31).wrapping_add(i32::from(unit))) }

    /// Borrows a substring only from UTF8 backing when both unit boundaries are scalar boundaries.
    /// Scans the UTF8 prefix in linear time; UTF16 backing and pair-splitting boundaries return `None`.
    pub fn utf8_source(self) -> Option<&'a str> {
        let Source::Utf8(source) = self.source else { return None; };
        Some(&source[byte_offset(source, self.start)?..byte_offset(source, self.end)?])
    }
}

fn byte_offset(source: &str, target: usize) -> Option<usize> {
    let mut units = 0;
    for (offset, ch) in source.char_indices() {
        if units == target { return Some(offset); }
        units += ch.len_utf16();
        if units > target { return None; }
    }
    (units == target).then_some(source.len())
}

impl PartialEq for JavaString { fn eq(&self, other: &Self) -> bool { self.view().units().eq(other.view().units()) } }
impl Eq for JavaString {}

impl fmt::Debug for JavaString { fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { debug_text("JavaString", self.view(), f) } }
impl fmt::Debug for JavaText<'_> { fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { debug_text("JavaText", *self, f) } }

fn debug_text(name: &str, value: JavaText<'_>, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{name}(\"")?;
    for unit in value.units() { write!(f, "\\u{unit:04x}")?; }
    f.write_str("\")")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_unit_and_layout_limits_are_checked_without_large_allocations() {
        let byte = Layout::new::<u8>();
        assert!(allocation_bound(i32::MAX as usize, byte).is_ok());
        assert_eq!(allocation_bound(i32::MAX as usize + 1, byte).unwrap_err().code, "rtdb.types/java-text-size");
        assert!(Layout::array::<u16>(usize::MAX).is_err());
        assert!(Layout::array::<u8>(isize::MAX as usize + 1).is_err());
    }
}
