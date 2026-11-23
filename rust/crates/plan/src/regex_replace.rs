use std::{fmt, sync::Arc};
use rtdb_types::{Anomaly,LogicalType as L,MemoryBudget,MemoryReservation,Result,ScalarType as S,Value};
use ::serde::{Deserialize,Serialize};

mod view;
mod storage;
mod builder;
mod serde;
#[cfg(test)]
mod storage_tests;
pub use builder::RegexFormBuilder;
pub(crate) use builder::FormAllocation;
pub use view::{RegexFormView, RegexFormKind, RegexForms, RegexFormIter, RegexFormPairs, RegexFormPairIter,
    RegexLiteralView, RegexLiteralKind, RegexLiterals, RegexLiteralIter, RegexLiteralPairs, RegexLiteralPairIter,
    RegexFields, RegexFieldIter, RegexLeaf};

/// One immutable canonical UTF8 raw metadata root, independent of scalar binding.
/// Construct with the fallible builder; Clone shares its complete backing and paired lease.
/// Generic deserialization builds an untracked backing; nested format callbacks inherit their recursion boundary.
/// Alternate Debug with newline fill likewise uses borrowed native recursive callbacks.
#[derive(Clone)]
pub struct RegexForm(Backing);
#[derive(Clone)]
enum Backing {
    InlineNull,
    Tracked { storage: Arc<storage::FormStorage>, _reservation: Arc<MemoryReservation> },
    Untracked(Arc<storage::FormStorage>),
}

impl Default for RegexForm { fn default() -> Self { Self(Backing::InlineNull) } }
impl PartialEq for RegexForm {
    fn eq(&self, other: &Self) -> bool {
        match (self.storage(), other.storage()) {
            (Some(left), Some(right)) => left.structural_eq(right),
            (None, None) => true,
            (None, Some(_)) => matches!(other.view().kind(), RegexFormKind::Literal(value) if matches!(value.kind(), RegexLiteralKind::Leaf(leaf) if matches!(leaf.value().as_ref(), Value::Null))),
            (Some(_), None) => other == self,
        }
    }
}
impl fmt::Debug for RegexForm { fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { storage::debug(self, f) } }

impl RegexForm {
    /// Borrows raw syntax without evaluating metadata or copying its owned storage.
    pub fn view(&self) -> RegexFormView<'_> {
        RegexFormView(match self.storage() { Some(storage) => view::RawSource::Stored(storage, storage::FormId(0)), None => view::RawSource::Null })
    }

    fn storage(&self) -> Option<&storage::FormStorage> { match &self.0 { Backing::InlineNull => None, Backing::Tracked { storage, .. } | Backing::Untracked(storage) => Some(storage) } }

    /// Makes an independent canonical copy in the destination budget, retaining source and destination charges during copying.
    /// Size, budget, shape and native allocation errors leave the source unchanged.
    pub fn copy_in(source: RegexFormView<'_>, memory: &Arc<MemoryBudget>) -> Result<Self> { FormAllocation::Tracked(memory).copy(source) }
    /// Makes an explicitly untracked independent compatibility copy; native allocation and size failures propagate.
    pub fn copy_untracked(source: RegexFormView<'_>) -> Result<Self> { FormAllocation::Untracked.copy(source) }

    fn truthy(&self) -> bool {
        !matches!(self.view().kind(), RegexFormKind::Literal(value) if matches!(value.kind(), RegexLiteralKind::Leaf(leaf) if matches!(leaf.value().as_ref(), Value::Null | Value::Bool(false))))
    }
    /// Bounds the complete retained backing, including spare capacity and unreachable charged tombstones.
    /// Sharing a backing does not discount this conservative retained-copy bound.
    pub fn retained_bytes(&self) -> Result<usize> {
        storage::add(std::mem::size_of::<Self>(), self.storage().map_or(0, |storage| storage.retained))
    }

    fn work_bound(&self) -> Result<usize> { match self.storage() { Some(storage) => storage.work_bound(), None => storage::add(std::mem::size_of::<storage::RawRow>(), storage::add(std::mem::size_of::<storage::LiteralRow>(), std::mem::size_of::<storage::Leaf34>())?) } }
}

/// Raw metadata for one source-expression replacement.
/// Rust Default fills all fields with nil; generic deserialization requires all five fields.
/// Start and occurrence use Clojure truthiness when an applicable source leg is admitted.
#[derive(Clone,Debug,Default,PartialEq,Serialize,Deserialize)]
pub struct RegexReplacement {
    pub pattern: RegexForm,
    pub replacement: RegexForm,
    pub start: RegexForm,
    pub occurrence: RegexForm,
    pub flags: RegexForm,
}

impl RegexReplacement {
    /// Copies the five fields in declaration order, admitting independent backing for each field that needs storage.
    /// Completed fields stay charged until destruction if a later copy fails; successful roots retain leases through later clones.
    pub fn copy_in(&self, memory: &Arc<MemoryBudget>) -> Result<Self> { self.copy_with(FormAllocation::Tracked(memory)) }
    /// Copies the five fields with explicitly untracked storage for each allocated backing.
    pub fn copy_untracked(&self) -> Result<Self> { self.copy_with(FormAllocation::Untracked) }
    fn copy_with(&self, allocation: FormAllocation<'_>) -> Result<Self> {
        Ok(Self { pattern: allocation.copy(self.pattern.view())?, replacement: allocation.copy(self.replacement.view())?, start: allocation.copy(self.start.view())?, occurrence: allocation.copy(self.occurrence.view())?, flags: allocation.copy(self.flags.view())? })
    }
    /// Admits every source type leg without compiling a pattern or consuming a row.
    /// Null and empty source types leave all metadata uninterpreted; non-null sources must be UTF8.
    pub fn output_type(&self,input: &L) -> Result<L> {
        let mut output = L::Nothing;
        for leg in input.legs() {
            output = output.join(&match leg {
                L::Null => L::Null,
                L::Scalar(S::Utf8) => L::Scalar(S::Utf8),
                _ => return Err(Anomaly::incorrect("xtdb.expression/function-type-mismatch","regexp_replace requires a UTF8 source")),
            });
        }
        if input.legs().contains(&L::Scalar(S::Utf8)) { self.admit_metadata()?; }
        Ok(output)
    }

    /// Retains a provisional output type without admitting the source or metadata.
    /// A non-null COALESCE input can remove this expression before emitted-type admission.
    pub fn declared_type(&self,input: &L) -> L {
        input.legs().into_iter().fold(L::Nothing,|output,leg|output.join(&if leg == L::Null { L::Null } else { L::Scalar(S::Utf8) }))
    }

    fn admit_metadata(&self) -> Result<()> {
        self.replacement_text()?;
        if self.start.truthy() || self.occurrence.truthy() {
            return Err(Anomaly::unsupported("xtdb.expression/unsupported","regexp_replace does not yet support `start` or `n` arguments"));
        }
        self.flag_text()?;
        Ok(())
    }

    /// Returns the literal replacement consumed during metadata admission.
    pub fn replacement_text(&self) -> Result<&str> {
        match self.replacement.view().kind() {
            RegexFormKind::Literal(value) => match value.kind() { RegexLiteralKind::Text(text) => Ok(text), _ => Err(metadata_error("REGEXP_REPLACE replacement is not a string")) },
            _ => Err(metadata_error("REGEXP_REPLACE replacement is not a string")),
        }
    }

    /// Reduces raw seqable flags to the supported Java flags; sequence elements are not evaluated.
    /// Nil is empty, map entries contribute no character flags, and non-seqable metadata fails admission.
    pub fn flag_text(&self) -> Result<String> {
        let mut flags = [false;3];
        let mut flag = |ch| match ch { 'i' => flags[0] = true,'s' => flags[1] = true,'m' => flags[2] = true,_ => () };
        match self.flags.view().kind() {
            RegexFormKind::Map(_) => (),
            RegexFormKind::Literal(value) => match value.kind() {
                RegexLiteralKind::Leaf(leaf) if matches!(leaf.value().as_ref(), Value::Null) => (),
                RegexLiteralKind::Text(text) => { for ch in text.chars() { flag(ch); } }
                _ => return Err(metadata_error("REGEXP_REPLACE flags are not seqable")),
            },
            RegexFormKind::List(values) | RegexFormKind::Vector(values) | RegexFormKind::Set(values) => {
                for value in values.iter() { if let RegexFormKind::Character(ch) = value.kind() { flag(ch); } }
            }
            _ => return Err(metadata_error("REGEXP_REPLACE flags are not seqable")),
        }
        Ok(['i','s','m'].into_iter().zip(flags).filter_map(|(ch,present)|present.then_some(ch)).collect())
    }

    /// Bounds the retained recipe independently of its source expression and compiled invocation.
    pub fn retained_bytes(&self) -> Result<usize> {
        [&self.pattern,&self.replacement,&self.start,&self.occurrence,&self.flags].into_iter()
            .try_fold(std::mem::size_of::<Self>(),|bytes,value|bytes.checked_add(value.retained_bytes()?).ok_or_else(size_error))
    }

    /// Returns the reachable logical recipe-byte estimate used by conservative plan work bounds.
    /// Spare capacities, shared leases and unreachable tombstones do not change this checked estimate.
    /// This measurement does not acquire a memory reservation.
    pub fn work_bound(&self) -> Result<usize> {
        [&self.pattern,&self.replacement,&self.start,&self.occurrence,&self.flags].into_iter()
            .try_fold(std::mem::size_of::<Self>(), |bytes, form| storage::add(bytes, form.work_bound()?))
    }
}

fn metadata_error(message: &str) -> Anomaly { Anomaly::fault("xtdb.error/unknown",message) }
fn size_error() -> Anomaly { Anomaly::incorrect("rtdb.plan/regex-metadata-size","Regex metadata exceeds addressable memory") }
