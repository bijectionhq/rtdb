mod getters;
mod open;

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, FixedSizeListArray, ListArray, MapArray, RunArray, UnionArray, types::{Int16Type, Int32Type, Int64Type}};
use arrow_schema::{DataType, Field};
use rtdb_types::{Anomaly, LogicalType, Result, Value};

use crate::{MemoryReservation, OwnedColumn, Selection};

/// Controls primitive validity checks for an individual physical read.
/// Logical object reads always retain their own null guard.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NullChecks { Enabled, Disabled }

/// Immutable physical vector with shared buffer, schema and selection reservations.
/// Clones and child views retain their owners; no payload buffers are copied by selection.
#[derive(Clone, Debug)]
pub struct ExecutionVector {
    node: Arc<OpenedNode>,
    address: Address,
}

#[derive(Debug)]
struct OpenedNode {
    field: Arc<Field>,
    logical: LogicalType,
    array: ArrayRef,
    children: Vec<Arc<OpenedNode>>,
    owner: Arc<OpenedOwner>,
}

#[derive(Debug)]
struct OpenedOwner {
    _array: ArrayRef,
    reservation: Arc<MemoryReservation>,
    _metadata: Arc<MemoryReservation>,
}

#[derive(Clone, Debug)]
enum Address {
    Range { offset: usize, len: usize },
    Indices { selection: Arc<Indices>, offset: usize, len: usize },
}

#[derive(Debug)]
struct Indices { rows: Vec<usize>, _reservation: Arc<MemoryReservation> }

impl Address {
    fn len(&self) -> usize {
        match self { Self::Range { len, .. } | Self::Indices { len, .. } => *len }
    }

    fn row(&self, index: usize) -> usize {
        match self {
            Self::Range { offset, .. } => offset + index,
            Self::Indices { selection, offset, .. } => selection.rows[offset + index],
        }
    }

    fn slice(&self, start: usize, length: usize) -> Self {
        match self {
            Self::Range { offset, .. } => Self::Range { offset: offset + start, len: length },
            Self::Indices { selection, offset, .. } => Self::Indices { selection: Arc::clone(selection), offset: offset + start, len: length },
        }
    }
}

impl ExecutionVector {
    /// Opens an admitted Arrow layout using the column's budget.
    /// Declared nonnullable nodes ignore incoming validity, sharing all original payload buffers.
    /// Schemas are limited to 128 levels and vectors to the signed 32-bit index domain.
    /// Mismatched or unsupported layouts, HALF and insufficient budget return errors.
    /// The caller's field and Arrow arrays remain unchanged.
    pub fn open(field: Arc<Field>, column: OwnedColumn) -> Result<Self> { open::open(field, column) }

    /// Adopts a pre-reserved Arrow allocation before execution nullability normalization.
    /// The reservation must cover `array.get_array_memory_size()`.
    /// Callers retaining independent input-array clones must retain their reservation separately.
    /// Raw arrays are retained privately; only normalized, validated views are published.
    /// Layout, schema-depth, signed-vector length and budget errors match `open`.
    pub fn with_reservation(field: Arc<Field>, array: ArrayRef, reservation: Arc<MemoryReservation>) -> Result<Self> {
        open::open_from_parts(field, array, reservation)
    }

    /// Borrows the opened physical declaration; selections leave it unchanged.
    pub fn field(&self) -> &Field { &self.node.field }
    /// Borrows the execution type, with unsigned widths interpreted as signed and logical union legs normalized.
    pub fn logical_type(&self) -> &LogicalType { &self.node.logical }
    pub fn len(&self) -> usize { self.address.len() }
    pub fn is_empty(&self) -> bool { self.len() == 0 }

    /// Normalizes indices to this opened vector's source rows before publishing a retained view.
    /// Ranges share any existing index owner; index selections reserve their storage before allocation.
    /// Invalid rows, signed-vector length overflow and budget exhaustion return errors.
    pub fn select(&self, selection: &Selection) -> Result<Self> {
        selection.validate(self.len())?;
        let address = match selection {
            Selection::All => self.address.clone(),
            Selection::Range { offset, length } => self.address.slice(*offset, *length),
            Selection::Indices(indices) => {
                vector_length(indices.len())?;
                let bytes = indices.len().checked_mul(std::mem::size_of::<usize>()).and_then(|n| n.checked_add(128)).ok_or_else(allocation)?;
                let reservation = self.node.owner.reservation.budget().reserve(bytes)?;
                let rows = indices.iter().map(|index| self.address.row(*index as usize)).collect();
                Address::Indices { selection: Arc::new(Indices { rows, _reservation: reservation }), offset: 0, len: indices.len() }
            }
        };
        Ok(Self { node: Arc::clone(&self.node), address })
    }

    /// Borrows a selected position without allocating; an out-of-range position returns an error.
    pub fn cell(&self, index: usize) -> Result<CellRef<'_>> {
        if index >= self.len() { return Err(index_error()); }
        Ok(CellRef { vector: self, index })
    }

    /// Retains the vector and selection at a checked position without materializing its value.
    pub fn owned_cell(&self, index: usize) -> Result<OwnedCell> {
        self.cell(index)?;
        Ok(OwnedCell { vector: self.clone(), index })
    }

    /// Implements the plain Struct vector's child lookup, retaining the parent's selection.
    /// Missing fields return `None`; other vector classes, including run-end encoding, reject this method.
    /// A null parent does not mask its child vector.
    pub fn struct_child(&self, name: &str) -> Result<Option<Self>> {
        let DataType::Struct(fields) = self.field().data_type() else { return Err(unsupported("vectorFor")); };
        Ok(fields.iter().position(|field| field.name() == name).map(|index| Self {
            node: Arc::clone(&self.node.children[index]), address: self.address.clone(),
        }))
    }

    /// Returns the complete element vector for plain list/map containers and their extensions.
    /// Parent selections do not select elements; use each cell's offsets or `raw_list_elements` for that.
    /// Run-end and union vectors reject this reference reader method.
    pub fn list_elements(&self) -> Result<Self> {
        match self.field().data_type() {
            DataType::List(_) | DataType::FixedSizeList(_, _) | DataType::Map(_, _) => Ok(Self::direct(Arc::clone(&self.node.children[0]))),
            _ => Err(unsupported("listElements")),
        }
    }

    fn direct(node: Arc<OpenedNode>) -> Self {
        let len = node.array.len();
        Self { node, address: Address::Range { offset: 0, len } }
    }
}

/// Borrowed selected cell; primitive methods resolve the actual union leg and physical run first.
/// Getter failures report the resolved leaf index, not the outer selection position.
#[derive(Clone, Copy, Debug)]
pub struct CellRef<'a> { vector: &'a ExecutionVector, index: usize }

/// Retained selected cell sharing its complete buffer, metadata and index owners.
#[derive(Clone, Debug)]
pub struct OwnedCell { vector: ExecutionVector, index: usize }

impl OwnedCell {
    /// Borrows the retained selected position without materializing or reserving a value.
    pub fn as_ref(&self) -> CellRef<'_> { CellRef { vector: &self.vector, index: self.index } }
}

/// Materialized logical value and its allocation reservation.
/// Borrowing cannot detach its allocation charge; dropping the final owner releases the charge.
#[derive(Debug)]
pub struct OwnedValue { value: Value, _reservation: Arc<MemoryReservation> }

impl OwnedValue {
    /// Borrows the logical value while this owner retains its decoding reservation.
    pub fn value(&self) -> &Value { &self.value }
}

struct ResolvedCell<'a> { node: &'a OpenedNode, row: usize }

impl<'a> CellRef<'a> {
    /// Returns the selected physical leaf metadata, including nullable leaf identity.
    pub fn leaf_field(&self) -> Result<&'a Field> { Ok(&self.resolve()?.node.field) }

    /// Returns the physical leaf row after selection, union offsets and run-end resolution.
    pub fn getter_index(&self) -> Result<usize> { Ok(self.resolve()?.row) }

    /// Applies opened-vector nullability at the selected leaf; bottom and Null vectors report null.
    pub fn is_null(&self) -> Result<bool> {
        let cell = self.resolve()?;
        Ok(cell.node.field.data_type() == &DataType::Null || cell.node.array.is_null(cell.row))
    }

    /// Materializes a logical execution value after reserving its result and decoding scratch.
    /// Null payload bits are never exposed by this operation, regardless of primitive check policy.
    /// Bottom, malformed logical objects and budget exhaustion return errors.
    pub fn object(&self) -> Result<OwnedValue> {
        let cell = self.resolve()?;
        let bytes = crate::estimate_read_value_bytes(&cell.node.field, cell.node.array.as_ref(), cell.row)?;
        let reservation = cell.node.owner.reservation.budget().reserve(bytes)?;
        let value = crate::read_execution_value(&cell.node.field, cell.node.array.as_ref(), cell.row)?;
        Ok(OwnedValue { value, _reservation: reservation })
    }

    /// Resolves physical indirection and returns a retained Struct child at the same leaf row.
    /// This raw layout operation also works through run-end/union indirection; it does not grant their `vectorFor` method.
    /// Missing names return `None`; a non-Struct leaf returns an error, and parent nullness does not mask child storage.
    pub fn raw_struct_field(&self, name: &str) -> Result<Option<OwnedCell>> {
        let cell = self.resolve()?;
        let DataType::Struct(fields) = cell.node.field.data_type() else { return Err(unsupported("raw_struct_field")); };
        fields.iter().position(|field| field.name() == name).map(|index| {
            ExecutionVector::direct(Arc::clone(&cell.node.children[index])).owned_cell(cell.row)
        }).transpose()
    }

    /// Returns the retained physical element range even when the parent validity bit is null.
    /// This raw layout operation resolves run-end/union indirection and is distinct from their reader-method admission.
    /// Non-list leaves and invalid offset ranges return errors.
    pub fn raw_list_elements(&self) -> Result<ExecutionVector> {
        let cell = self.resolve()?;
        let (start, end) = match cell.node.field.data_type() {
            DataType::List(_) => {
                let array = downcast::<ListArray>(cell.node.array.as_ref())?;
                (array.value_offsets()[cell.row] as usize, array.value_offsets()[cell.row + 1] as usize)
            }
            DataType::FixedSizeList(_, length) => {
                let length = *length as usize;
                let start = cell.row.checked_mul(length).ok_or_else(allocation)?;
                (start, start.checked_add(length).ok_or_else(allocation)?)
            }
            DataType::Map(_, _) => {
                let array = downcast::<MapArray>(cell.node.array.as_ref())?;
                (array.value_offsets()[cell.row] as usize, array.value_offsets()[cell.row + 1] as usize)
            }
            _ => return Err(unsupported("raw_list_elements")),
        };
        let length = end.checked_sub(start).ok_or_else(layout)?;
        ExecutionVector::direct(Arc::clone(&cell.node.children[0])).select(&Selection::Range { offset: start, length })
    }

    fn resolve(&self) -> Result<ResolvedCell<'a>> {
        let mut node = self.vector.node.as_ref();
        let mut row = self.vector.address.row(self.index);
        loop {
            match node.field.data_type() {
                DataType::Union(fields, _) => {
                    let array = downcast::<UnionArray>(node.array.as_ref())?;
                    let id = array.type_id(row);
                    let index = fields.iter().position(|(candidate, _)| candidate == id).ok_or_else(layout)?;
                    row = array.value_offset(row);
                    node = &node.children[index];
                }
                DataType::RunEndEncoded(ends, _) => {
                    row = match ends.data_type() {
                        DataType::Int16 => downcast::<RunArray<Int16Type>>(node.array.as_ref())?.get_physical_index(row),
                        DataType::Int32 => downcast::<RunArray<Int32Type>>(node.array.as_ref())?.get_physical_index(row),
                        DataType::Int64 => downcast::<RunArray<Int64Type>>(node.array.as_ref())?.get_physical_index(row),
                        _ => return Err(layout()),
                    };
                    node = &node.children[0];
                }
                _ => return Ok(ResolvedCell { node, row }),
            }
            if row >= node.array.len() { return Err(index_error()); }
        }
    }
}

fn downcast<T: Array + 'static>(array: &dyn Array) -> Result<&T> { array.as_any().downcast_ref().ok_or_else(layout) }
fn vector_length(length: usize) -> Result<()> {
    if length > i32::MAX as usize { Err(Anomaly::unsupported("rtdb/vector-length", "Execution vector length exceeds the signed 32-bit index domain")) } else { Ok(()) }
}
fn index_error() -> Anomaly { Anomaly::incorrect("rtdb/row-index", "Cell index exceeds vector length") }
fn layout() -> Anomaly { Anomaly::incorrect("rtdb/invalid-arrow", "Execution field and physical array layout disagree") }
fn allocation() -> Anomaly { Anomaly::incorrect("rtdb/allocation-overflow", "Execution vector allocation bound exceeds usize") }
fn unsupported(method: &str) -> Anomaly { Anomaly::unsupported("rtdb/unsupported-vector-reader", format!("Vector does not expose {method}")) }
