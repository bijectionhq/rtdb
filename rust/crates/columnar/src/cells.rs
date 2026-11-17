mod getters;
mod descriptor;
mod materialize;
mod native_scalar;
mod write;
mod open;
mod scalar;
mod storage;
mod view;

use std::sync::Arc;
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, Field};
use rtdb_types::{Anomaly, LogicalType, Result, Value};
use crate::{MemoryReservation, OwnedColumn, Selection};
use storage::NodeLayout;
pub use storage::{PhysicalLayout, RetainedBuffer};
pub use descriptor::{ContainerKind, DeclarationRef, RetainedArrowField, UnencodedBottom, UnionEncoding, VectorDescriptor, VectorTypeFailure, VectorTypeResult};
use descriptor::Declaration;

/// Controls primitive validity checks for an individual physical read.
/// Logical object reads always retain their own null guard.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NullChecks { Enabled, Disabled }

/// Primitive reader failure before a caller chooses its host-exception conversion boundary.
/// AbsentPrimitiveReceiver is a message-less NullPointerException producer, not an already-converted anomaly.
/// Kotlin and Clojure boundaries apply different null-message policies; no implicit reverse conversion is provided.
#[derive(Debug)]
pub enum VectorReadError { Anomaly(Anomaly), AbsentPrimitiveReceiver }

/// Result of a forwarding primitive/getBytes read or a concrete numeric wrapper.
pub type VectorReadResult<T> = std::result::Result<T, VectorReadError>;

impl From<Anomaly> for VectorReadError { fn from(error: Anomaly) -> Self { Self::Anomaly(error) } }
impl std::fmt::Display for VectorReadError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self { Self::Anomaly(error) => std::fmt::Display::fmt(error, formatter), Self::AbsentPrimitiveReceiver => formatter.write_str("java.lang.NullPointerException") }
    }
}
impl std::error::Error for VectorReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> { match self { Self::Anomaly(error) => Some(error), Self::AbsentPrimitiveReceiver => None } }
}

/// Immutable physical vector with shared buffer, schema and selection reservations.
/// Clones and child views retain their owners; selection does not copy payload buffers.
/// Fixed-width primitive byte addresses use source i32 products after logical row admission.
#[derive(Clone, Debug)]
pub struct ExecutionVector { view: ReaderView, address: Address }

#[derive(Clone, Debug)]
enum ReaderView {
    Direct(Arc<PhysicalNode>),
    UnionLeg { union: Arc<PhysicalNode>, type_id: i8, inner: Arc<ExecutionVector>, _reservation: Arc<MemoryReservation> },
}

#[derive(Debug)]
struct PhysicalNode {
    declaration: Declaration, descriptor: Arc<VectorDescriptor>, length: usize,
    storage: NodeLayout, owner: Arc<NodeOwner>,
}
impl PhysicalNode {
    fn declaration(&self) -> DeclarationRef<'_> {
        match &self.declaration { Declaration::Arrow(field) => DeclarationRef::Arrow(field), Declaration::Native => DeclarationRef::Native(&self.descriptor) }
    }
    fn ingress_arrow(&self) -> Result<&Field> { self.declaration().arrow().ok_or_else(layout) }
    fn scalar_field(&self) -> Result<&Field> { self.descriptor.scalar_field().ok_or_else(layout) }
}
#[derive(Debug)]
struct NodeOwner { backing: BackingOwner, metadata: Arc<MemoryReservation> }
#[derive(Debug)]
enum BackingOwner { Arrow { _array: ArrayRef, reservation: Arc<MemoryReservation> }, Native }

#[derive(Clone, Debug)]
enum Address {
    Identity { len: usize },
    Range { offset: usize, len: usize },
    Indices { selection: Arc<Indices>, offset: usize, len: usize },
}
#[derive(Debug)]
struct Indices { rows: Vec<PhysicalIndex>, _reservation: Arc<MemoryReservation> }
#[derive(Clone, Copy, Debug)]
struct PhysicalIndex(i32);
impl PhysicalIndex {
    fn from_position(index: usize) -> Result<Self> { i32::try_from(index).map(Self).map_err(|_| index_error()) }
    fn position(self) -> Result<usize> { usize::try_from(self.0).map_err(|_| index_error()) }
}
impl Address {
    fn len(&self) -> usize { match self { Self::Identity { len } | Self::Range { len, .. } | Self::Indices { len, .. } => *len } }
    fn row(&self, index: usize) -> Result<PhysicalIndex> { self.translate(PhysicalIndex::from_position(index)?) }
    fn translate(&self, index: PhysicalIndex) -> Result<PhysicalIndex> {
        match self {
            Self::Identity { .. } => Ok(index),
            Self::Range { offset, .. } => Ok(PhysicalIndex((*offset as i32).wrapping_add(index.0))),
            Self::Indices { selection, offset, len } => {
                let index = index.position()?;
                if index >= *len { return Err(index_error()); }
                selection.rows.get(offset + index).copied().ok_or_else(index_error)
            }
        }
    }
}

impl ExecutionVector {
    /// Opens admitted Arrow under its retained budget, ignoring nonnullable validity without changing caller arrays.
    /// Layout, timezone, 128-level schema depth, i32 vector length and budget validation precede publication; HALF is unsupported.
    pub fn open(field: Arc<Field>, column: OwnedColumn) -> Result<Self> { open::open(field, column) }
    /// Adopts raw Arrow before execution nullability normalization; the reservation must cover its full array memory size.
    /// Caller-retained array clones require their reservation retained separately; validation matches `open`.
    pub fn with_reservation(field: Arc<Field>, array: ArrayRef, reservation: Arc<MemoryReservation>) -> Result<Self> { open::open_from_parts(field, array, reservation) }
    /// Adopts a native container with retained buffers and complete direct child vectors.
    /// Length and schema-depth checks precede metadata reservation; layout validation follows it, and failure drops the adopted inputs.
    /// Absent union discriminators are retained without constructing invalid Arrow arrays.
    pub fn from_physical(field: Arc<Field>, layout: PhysicalLayout, memory: &Arc<crate::MemoryBudget>) -> Result<Self> { storage::native(field, layout, memory) }
    /// Borrows canonical native metadata, including signed integer widths, ZoneId spelling and ordered container children.
    pub fn descriptor(&self) -> &VectorDescriptor { &self.node().descriptor }
    /// Projects canonical metadata into checked SDK fields under a retained lease.
    /// Native union shapes outside the SDK discriminator domain fail without changing this vector.
    pub fn arrow_field(&self) -> Result<RetainedArrowField> { self.descriptor().arrow_field() }
    /// Borrows original Arrow provenance, or the authoritative native descriptor when no Arrow declaration exists.
    pub fn ingress_field(&self) -> DeclarationRef<'_> { self.node().declaration() }
    /// Borrows logical metadata; native unsupported run-end value types return their typed source failure.
    /// Field projection and concrete reads do not implicitly request this metadata.
    pub fn logical_type(&self) -> VectorTypeResult<&LogicalType> { self.descriptor().logical_type() }
    /// Reports the concrete reader's nullability, separately from physical Field nullability.
    pub fn reader_nullable(&self) -> bool { self.descriptor().reader_nullable() }
    pub fn len(&self) -> usize { self.address.len() }
    pub fn is_empty(&self) -> bool { self.len() == 0 }
    /// Borrows a checked selected position without allocating.
    pub fn cell(&self, index: usize) -> Result<CellRef<'_>> {
        if index >= self.len() { return Err(index_error()); }
        Ok(CellRef { vector: self, index })
    }
    /// Retains a checked cell's buffers, metadata and reader view without materialization.
    pub fn owned_cell(&self, index: usize) -> Result<OwnedCell> {
        self.cell(index)?;
        Ok(OwnedCell { vector: self.clone(), index })
    }
    fn node(&self) -> &PhysicalNode { match &self.view { ReaderView::Direct(node) => node, ReaderView::UnionLeg { inner, .. } => inner.node() } }
    fn budget(&self) -> &Arc<crate::MemoryBudget> { self.node().owner.metadata.budget() }
    fn direct(node: Arc<PhysicalNode>) -> Self { let len = node.length; Self { view: ReaderView::Direct(node), address: Address::Identity { len } } }
    fn into_direct(self) -> Result<Arc<PhysicalNode>> {
        match (self.view, self.address) {
            (ReaderView::Direct(node), Address::Identity { len }) if len == node.length => Ok(node),
            _ => Err(Anomaly::incorrect("rtdb/physical-child-view", "Physical container children must be complete direct vectors")),
        }
    }
    fn resolve(&self, index: PhysicalIndex) -> Result<ResolvedAccess<'_>> {
        let row = self.address.translate(index)?;
        match &self.view {
            ReaderView::Direct(node) => resolve_node(node, row),
            ReaderView::UnionLeg { union, inner, .. } => inner.resolve(union.union_offset_index(row)?),
        }
    }
    fn null(&self, index: PhysicalIndex) -> Result<bool> {
        let row = self.address.translate(index)?;
        match &self.view {
            ReaderView::Direct(node) => node.null(row),
            ReaderView::UnionLeg { union, type_id, inner, .. } => Ok(union.union_id(row)? != *type_id || inner.null(union.union_offset_index(row)?)?),
        }
    }
    fn leg_name(&self, index: PhysicalIndex) -> Result<Option<&str>> {
        fn name(node: &PhysicalNode, row: PhysicalIndex) -> Result<Option<&str>> {
            match &node.storage {
                NodeLayout::Union { .. } => Ok(node.union_leg(row)?.map(|child| child.descriptor.name())),
                NodeLayout::RunEnd { values, .. } => name(values, node.run_index(row)?),
                _ => Err(unsupported("getLeg")),
            }
        }
        let row = self.address.translate(index)?;
        match &self.view {
            ReaderView::Direct(node) => name(node, row),
            ReaderView::UnionLeg { union, inner, .. } => inner.leg_name(union.union_offset_index(row)?),
        }
    }
}

/// Borrowed selected cell; primitive errors identify the resolved physical getter index.
#[derive(Clone, Copy, Debug)]
pub struct CellRef<'a> { vector: &'a ExecutionVector, index: usize }
/// Retained selected cell sharing its complete buffer, metadata and reader-view owners.
#[derive(Clone, Debug)]
pub struct OwnedCell { vector: ExecutionVector, index: usize }
impl OwnedCell { pub fn as_ref(&self) -> CellRef<'_> { CellRef { vector: &self.vector, index: self.index } } }
/// Materialized logical value whose reservation remains attached through borrowed access.
#[derive(Debug)]
pub struct OwnedValue { value: Value, _reservation: Arc<MemoryReservation> }
impl OwnedValue { pub fn value(&self) -> &Value { &self.value } }

struct ResolvedCell<'a> { node: &'a PhysicalNode, row: PhysicalIndex }
struct PayloadCell<'a> { node: &'a PhysicalNode, row: usize }
impl<'a> ResolvedCell<'a> {
    fn payload(&self) -> Result<PayloadCell<'a>> { Ok(PayloadCell { node: self.node, row: self.node.checked_position(self.row)? }) }
}
enum ResolvedAccess<'a> { Leaf(ResolvedCell<'a>), AbsentUnion { type_id: i8, row: PhysicalIndex } }
fn resolve_node(mut node: &PhysicalNode, mut row: PhysicalIndex) -> Result<ResolvedAccess<'_>> {
    loop {
        match &node.storage {
            NodeLayout::Union { .. } => match node.union_child(row)? {
                Some((child, offset)) => { node = child; row = offset; }
                None => return Ok(ResolvedAccess::AbsentUnion { type_id: node.union_id(row)?, row }),
            },
            NodeLayout::RunEnd { values, .. } => { row = node.run_index(row)?; node = values; }
            _ => return Ok(ResolvedAccess::Leaf(ResolvedCell { node, row })),
        }
    }
}
impl<'a> CellRef<'a> {
    /// Returns actual selected leaf metadata, including nullable leaf identity; an absent union leg is an error.
    pub fn leaf_descriptor(&self) -> Result<&'a VectorDescriptor> { Ok(&self.resolve()?.node.descriptor) }
    /// Returns a checked source leaf row after all indirection; absent legs and addresses outside that leaf return errors.
    /// Concrete getter byte-stride arithmetic is applied after this logical row admission.
    pub fn getter_index(&self) -> Result<usize> { Ok(self.resolve()?.payload()?.row) }
    /// Applies reader nullness, including named-leg discriminator masks and absent union rows.
    pub fn is_null(&self) -> Result<bool> { self.vector.null(PhysicalIndex::from_position(self.index)?) }
    /// Reads the direct union's signed discriminator; named-leg/run-end readers reject getTypeId.
    pub fn type_id(&self) -> Result<i8> { match &self.vector.view { ReaderView::Direct(node) => node.union_id(self.vector.address.row(self.index)?), _ => Err(unsupported("getTypeId")) } }
    /// Resolves a selected union name, or None for an absent leg; non-union leaves reject getLeg.
    /// Run-end and named-leg readers delegate to their inner reader.
    pub fn leg_name(&self) -> Result<Option<&'a str>> { self.vector.leg_name(PhysicalIndex::from_position(self.index)?) }
    /// Materializes logical Value with reserved result/scratch, applying logical null guards.
    /// This adapter does not construct TaggedValue or the full JVM object surface; bottom and absent union reads fail.
    pub fn object(&self) -> Result<OwnedValue> { materialize::object(self) }
    /// Resolves physical indirection to a retained Struct child without masking it by parent nullness.
    /// Raw access does not grant the wrapper's reference vectorFor method.
    pub fn raw_struct_field(&self, name: &str) -> Result<Option<OwnedCell>> {
        let cell = self.resolve()?;
        let NodeLayout::Struct { children, .. } = &cell.node.storage else { return Err(unsupported("raw_struct_field")); };
        children.iter().find(|child| child.descriptor.name() == name).map(|child| ExecutionVector::direct(Arc::clone(child)).owned_cell(cell.node.checked_position(cell.row)?)).transpose()
    }
    /// Returns retained raw elements, including null-parent payload, after physical indirection.
    /// This is distinct from listElements admission on union/run-end readers.
    pub fn raw_list_elements(&self) -> Result<ExecutionVector> {
        let cell = self.resolve()?;
        if !matches!(&cell.node.storage, NodeLayout::List { .. } | NodeLayout::FixedList { .. }) { return Err(unsupported("raw_list_elements")); }
        let (child, start, end) = cell.node.elements(cell.node.checked_position(cell.row)?)?;
        ExecutionVector::direct(Arc::clone(child)).select(&Selection::Range { offset: start, length: end - start })
    }
    fn resolve(&self) -> Result<ResolvedCell<'a>> {
        match self.vector.resolve(PhysicalIndex::from_position(self.index)?)? { ResolvedAccess::Leaf(cell) => Ok(cell), ResolvedAccess::AbsentUnion { type_id, row } => Err(absent(type_id, row)) }
    }
    fn resolve_primitive(&self) -> VectorReadResult<ResolvedCell<'a>> {
        match self.vector.resolve(PhysicalIndex::from_position(self.index)?)? {
            ResolvedAccess::Leaf(cell) => Ok(cell),
            ResolvedAccess::AbsentUnion { .. } => Err(VectorReadError::AbsentPrimitiveReceiver),
        }
    }
}

fn downcast<T: Array + 'static>(array: &dyn Array) -> Result<&T> { array.as_any().downcast_ref().ok_or_else(layout) }
fn vector_length(length: usize) -> Result<()> { if length > i32::MAX as usize { Err(Anomaly::unsupported("rtdb/vector-length", "Execution vector length exceeds the signed 32-bit index domain")) } else { Ok(()) } }
fn index_bytes(length: usize) -> Result<usize> { length.checked_mul(std::mem::size_of::<usize>() * 2).and_then(|n| n.checked_add(256)).ok_or_else(allocation) }
fn index_error() -> Anomaly { Anomaly::incorrect("rtdb/row-index", "Cell index exceeds vector length") }
fn layout() -> Anomaly { Anomaly::incorrect("rtdb/invalid-arrow", "Execution field and physical layout disagree") }
fn allocation() -> Anomaly { Anomaly::incorrect("rtdb/allocation-overflow", "Execution vector allocation bound exceeds usize") }
fn unsupported(method: &str) -> Anomaly { Anomaly::unsupported("rtdb/unsupported-vector-reader", format!("Vector does not expose {method}")) }
fn absent(_type_id: i8, row: PhysicalIndex) -> Anomaly { Anomaly::fault("xtdb.error/unknown", format!("no leg for index {}", row.0)) }
