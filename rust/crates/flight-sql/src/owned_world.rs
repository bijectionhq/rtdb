use std::{ops::Range, sync::Arc};

use arrow_array::{Array, ArrayRef, BinaryArray, FixedSizeListArray, Float32Array, Float64Array, GenericByteArray, ListArray, NullArray, RecordBatch, StringArray,
    builder::{Float32Builder, Float64Builder}, cast::AsArray, types::{BinaryType, ByteArrayType, Float32Type, Float64Type, Utf8Type}};
use arrow_buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer, bit_util};
use arrow_schema::{DataType, Field};
use futures::future::BoxFuture;
use rtdb_columnar::{ArrowCell, MemoryReservation};
use rtdb_node::{OwnedChangeEdge, Database, NativeSession, Node, OwnedCountRequest, OwnedRangeCount, OwnedRangePage, OwnedRangeRequest, OwnedWorldWitness};
use rtdb_types::{EntityId, Iid, Interval, ListKind, LogicalType, OwnedRelation, OwnedWorldAssertion, Result, ScalarType, StructFields, TimeUnit, Timestamp, TransactionKey, Value, WorldClockMode};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// The placed host's closed checkpoint-world authority. Flight supplies the
/// authenticated native session and its already admitted database attachment.
pub trait OwnedWorldHost: std::fmt::Debug + Send + Sync {
    fn prepare_world_overlay<'a>(
        &'a self, call: WorldCall<'a>, cut: rtdb_node::OwnedForkCut, child: Uuid, retain_until: Timestamp, writable: bool,
    ) -> BoxFuture<'a, Result<PreparedWorldOverlay>>;

    fn prepare_replay_parent<'a>(
        &'a self, call: WorldCall<'a>, binding: [u8; 32], original: Option<TransactionKey>,
    ) -> BoxFuture<'a, Result<Option<PreparedReplayParent>>>;

    fn finish_replay_parents<'a>(
        &'a self, call: WorldCall<'a>, binding: [u8; 32],
    ) -> BoxFuture<'a, Result<()>>;

    fn retain_replayed_world<'a>(
        &'a self, call: WorldCall<'a>, binding: [u8; 32], world: Uuid,
    ) -> BoxFuture<'a, Result<RetainedReplayWorld>>;

    fn open_world_read<'a>(&'a self, call: WorldCall<'a>, world: Uuid) -> BoxFuture<'a, Result<Box<dyn OwnedWorldRead>>>;
    fn fork_world_checked<'a>(&'a self, call: WorldCall<'a>, capture: Uuid, suffix: Option<Uuid>, until: Timestamp, fork: rtdb_node::OwnedWorldFork) -> BoxFuture<'a, Result<(Uuid, TransactionKey)>>;
    fn commit_world_batch<'a>(
        &'a self, call: WorldCall<'a>, sections: Vec<rtdb_node::OwnedWorldSection>,
        root: rtdb_node::OwnedRootSection, observed: rtdb_node::OwnedWorldObservations, fingerprint: [u8; 32],
        controls: Vec<rtdb_node::HostedControlWrite>, deadline: Timestamp,
    ) -> BoxFuture<'a, Result<TransactionKey>>;

    fn root_rank_authority(&self, call: WorldCall<'_>) -> Result<Arc<dyn rtdb_storage::sealing::RootRankAuthority>>;

    /// The placed deployment's assertion-family vault (dev/doc/rtdb-assertion-families.md).
    fn assertion_family<'a>(
        &'a self, call: WorldCall<'a>, request: crate::FamilyRequest,
    ) -> BoxFuture<'a, Result<crate::FamilyReply>>;

    fn supports_retired_original_family(&self) -> bool { false }

    fn retired_family_fence<'a>(
        &'a self, _call: WorldCall<'a>, _request: &'a crate::RetiredFamilyRequest,
    ) -> BoxFuture<'a, Result<bytes::Bytes>> {
        Box::pin(async { Err(rtdb_types::OwnedWorld::unavailable()) })
    }
}

/// The independently archived overlay position and its bounded preparation lease.
#[derive(serde::Serialize)]
pub struct PreparedWorldOverlay {
    pub child: [u8; 16],
    pub world: [u8; 16],
    pub parent_pin: [u8; 16],
    pub capture: [u8; 16],
    pub suffix: [u8; 16],
    pub manifest: String,
    pub basis: TransactionKey,
    pub publication: TransactionKey,
    pub logical: Timestamp,
    pub until: Timestamp,
}

/// An independent destination archive at an Original replayed Root cut.
#[derive(serde::Serialize)]
pub struct PreparedReplayParent {
    pub capture: [u8; 16],
    pub suffix: Option<[u8; 16]>,
    pub manifest: String,
    pub basis: TransactionKey,
    pub logical: Timestamp,
}

/// The Directory retention awaiting the held writer's local readmission.
#[derive(serde::Serialize)]
pub struct RetainedReplayWorld {
    pub pin: [u8; 16],
    pub ancestors: Vec<rtdb_types::OwnedWorldAncestor>,
    pub capture: [u8; 16],
    pub suffix: Option<[u8; 16]>,
    pub manifest: String,
    pub basis: TransactionKey,
    pub until: Timestamp,
}

pub struct WorldCall<'a> {
    pub node: &'a Node,
    pub attachment: &'a Database,
    pub session: &'a mut NativeSession,
    pub cancellation: &'a CancellationToken,
}

pub struct WorldPointResult {
    pub present: Timestamp,
    pub row: Option<WorldPointRow>,
    pub until: Option<Timestamp>,
    pub shadowed: bool,
    pub witness: Option<OwnedWorldWitness>,
    pub frozen_winner: Option<FrozenWorldWinner>,
}

/// The original event and accepted Held reading of a frozen overlay winner.
#[derive(Clone, Copy)]
pub struct FrozenWorldWinner {
    pub author: Iid,
    pub publication: TransactionKey,
    pub event_ordinal: u64,
    pub reading: Timestamp,
}

pub struct WorldPointRow {
    pub batch: RecordBatch,
    pub revision: Timestamp,
    pub assertion: Option<OwnedWorldAssertion>,
    pub reservation: Arc<MemoryReservation>,
}


impl WorldPointRow {
    #[doc(hidden)]
    pub fn document_bytes(batch: &RecordBatch) -> Result<usize> {
        let base = Self::document_header_bytes(batch)?;
        Self::cells(batch).try_fold(base, |bytes, cell| {
            bytes.checked_add(PointCell::prepare(cell)?.heap_bytes()).ok_or_else(rtdb_types::OwnedWorld::unavailable)
        })
    }

    pub(crate) fn cells(batch: &RecordBatch) -> impl Iterator<Item = ArrowCell<'_>> {
        batch.schema_ref().fields().iter().zip(batch.columns())
            .map(|(field, array)| ArrowCell { field, array: array.as_ref(), row: 0 })
    }

    pub(crate) fn document_header_bytes(batch: &RecordBatch) -> Result<usize> {
        let shape = batch.schema_ref().fields().iter().map(|field| Ok((field.name().clone(), Value::Null)))
            .collect::<Result<StructFields<_>>>()?;
        Value::document_retained_bytes(&shape)
    }
}

pub(crate) struct PointCell<'a> {
    logical: LogicalType,
    bytes: usize,
    storage: PointStorage<'a>,
}

enum PointStorage<'a> {
    Empty,
    EmptySequence,
    Slice(ArrowCell<'a>),
    Compact { cell: ArrowCell<'a>, validate_uri: bool },
    FloatList { values: &'a dyn Array, rows: Range<usize> },
    ByteList { values: &'a dyn Array, rows: Range<usize>, payload_bytes: usize },
    Struct(Vec<PointCell<'a>>),
    Decoded(Value),
    Read(ArrowCell<'a>),
}

impl<'a> PointCell<'a> {
    pub(crate) fn prepare(cell: ArrowCell<'a>) -> Result<Self> {
        let selected = cell.physical()?;
        let logical = selected.logical_type()?;
        let storage = match &logical {
            LogicalType::Null => PointStorage::Empty,
            LogicalType::Scalar(scalar) => match rtdb_columnar::to_field(cell.field.name(), &logical) {
                Ok(field) if selected.field.data_type() == field.data_type() && selected.field.metadata() == field.metadata() =>
                    PointStorage::Slice(selected),
                Ok(field) if selected.field.data_type() == field.data_type()
                    && cell.logical_position(None).is_ok_and(|(_, ty)| required_point_type(&ty) == &logical) =>
                    PointStorage::Compact { cell: selected, validate_uri: *scalar == ScalarType::Uri },
                _ => PointStorage::Read(cell),
            },
            LogicalType::List { .. } | LogicalType::Struct(_) => {
                if std::ptr::eq(cell.field, selected.field)
                    || cell.logical_position(None).is_ok_and(|(_, ty)| required_point_type(&ty) == &logical)
                {
                    if let Some(nested) = Self::try_concrete(selected, logical)? { return Ok(nested); }
                }
                return Self::decoded(rtdb_columnar::read_value(cell.field, cell.array, cell.row)?);
            }
            _ => PointStorage::Read(cell),
        };
        Ok(Self { bytes: scalar_point_bytes(selected, &logical), logical, storage })
    }

    pub(crate) fn heap_bytes(&self) -> usize { self.bytes }

    fn decoded(value: Value) -> Result<Self> {
        let bytes = value.retained_bytes()?.checked_sub(size_of::<Value>()).ok_or_else(rtdb_types::OwnedWorld::unavailable)?;
        Ok(Self { logical: value.logical_type(), bytes, storage: PointStorage::Decoded(value) })
    }

    fn try_nested(cell: ArrowCell<'a>, declared: &LogicalType) -> Result<Option<Self>> {
        if *declared == LogicalType::Nothing {
            return Ok(Some(Self { logical: LogicalType::Nothing, bytes: 0, storage: PointStorage::Empty }));
        }
        let Ok((selected, interpreted)) = cell.logical_position(Some(declared)) else { return Ok(None); };
        let Ok(logical) = selected.logical_type() else { return Ok(None); };
        if logical != LogicalType::Null && required_point_type(&interpreted) != &logical { return Ok(None); }
        Self::try_concrete(selected, logical)
    }

    fn try_concrete(selected: ArrowCell<'a>, logical: LogicalType) -> Result<Option<Self>> {
        let (logical, bytes, storage) = match &logical {
            LogicalType::Null => (logical, 0, PointStorage::Empty),
            LogicalType::Scalar(scalar) => {
                if !rtdb_columnar::to_field(selected.field.name(), &logical)
                    .is_ok_and(|field| field.data_type() == selected.field.data_type())
                {
                    return Ok(None);
                }
                if *scalar == ScalarType::Uri {
                    rtdb_types::UriParts::parse(selected.array.as_string::<i32>().value(selected.row))?;
                }
                let bytes = scalar_point_bytes(selected, &logical);
                (logical, bytes, PointStorage::Compact { cell: selected, validate_uri: false })
            }
            LogicalType::Struct(types) => {
                let Some(children) = selected.structure().filter(|children| children.len() == types.len()) else { return Ok(None); };
                let mut shape = StructFields::new();
                let mut fields = StructFields::new();
                let mut output = Vec::new();
                let mut bytes = 0usize;
                for child in children {
                    let name = child.field.name();
                    let Some(declared) = types.get(name) else { return Ok(None); };
                    let planned = match Self::try_nested(child, declared)? {
                        Some(planned) => planned,
                        None => Self::decoded(rtdb_columnar::read_value_as(declared, child.field, child.array, child.row)?)?,
                    };
                    bytes = bytes.checked_add(planned.heap_bytes()).ok_or_else(rtdb_types::OwnedWorld::unavailable)?;
                    shape.insert(name.clone(), Value::Null);
                    fields.insert(name.clone(), planned.logical.clone());
                    output.push(planned);
                }
                let bytes = bytes.checked_add(Value::document_retained_bytes(&shape)?).ok_or_else(rtdb_types::OwnedWorld::unavailable)?;
                (LogicalType::Struct(fields), bytes, PointStorage::Struct(output))
            }
            LogicalType::List { kind: kind @ (ListKind::Set | ListKind::Map { .. }), .. }
                if selected.sequence_range().is_some_and(|(_, _, rows)| rows.is_empty()) =>
            {
                let element = if matches!(kind, ListKind::Set) {
                    LogicalType::Nothing
                } else {
                    LogicalType::Struct(StructFields::from([
                        ("key".into(), LogicalType::Nothing), ("value".into(), LogicalType::Nothing),
                    ]))
                };
                (LogicalType::List { kind: kind.clone(), element: Box::new(element) }, 0, PointStorage::EmptySequence)
            }
            LogicalType::List { kind: ListKind::TimestampTzRange, element } => {
                let endpoint = LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) });
                let Some((_, values, rows)) = selected.sequence_range() else { return Ok(None); };
                if **element != endpoint || rows.len() != 2 || rows.clone().any(|row| values.is_null(row))
                    || !rtdb_columnar::to_field(selected.field.name(), &logical).is_ok_and(|field| {
                        field.data_type() == selected.field.data_type() && field.metadata() == selected.field.metadata()
                    })
                {
                    return Ok(None);
                }
                (logical, 0, PointStorage::Compact { cell: selected, validate_uri: false })
            }
            LogicalType::List { kind: kind @ (ListKind::List | ListKind::FixedSizeList(_)), element } => {
                let Some((item, values, rows)) = selected.sequence_range() else { return Ok(None); };
                if rows.is_empty() || !item.metadata().is_empty() {
                    return Ok(None);
                }
                let element = match element.as_ref() { LogicalType::Maybe(inner) => inner.as_ref(), ty => ty };
                let plain_list = *kind == ListKind::List && selected.field.metadata().is_empty();
                let (header, width) = match (element, item.data_type()) {
                    (LogicalType::Scalar(ScalarType::F32), DataType::Float32) => (size_of::<Float32Array>(), 4),
                    (LogicalType::Scalar(ScalarType::F64), DataType::Float64) => (size_of::<Float64Array>(), 8),
                    (LogicalType::Scalar(ScalarType::Utf8), DataType::Utf8) if plain_list =>
                        return Ok(Self::try_byte_list::<Utf8Type>(values, rows, element)),
                    (LogicalType::Scalar(ScalarType::Binary), DataType::Binary) if plain_list =>
                        return Ok(Self::try_byte_list::<BinaryType>(values, rows, element)),
                    _ => return Ok(None),
                };
                let nulls = values.nulls().map_or(0, |nulls| nulls.slice(rows.start, rows.len()).null_count());
                // Point admission charges the validity capacity allocated by copy_float_components.
                let validity_bytes = if nulls == 0 { 0 } else {
                    bit_util::round_upto_multiple_of_64(bit_util::ceil(rows.len(), 8))
                };
                let bytes = rows.len().checked_mul(width)
                    .and_then(|bytes| header.checked_add(bytes)?.checked_add(validity_bytes))
                    .ok_or_else(rtdb_types::OwnedWorld::unavailable)?;
                let observed = if nulls == 0 { element.clone() }
                    else if nulls == rows.len() { LogicalType::Null }
                    else { element.clone().maybe() };
                let logical = LogicalType::List { kind: kind.clone(), element: Box::new(observed) };
                (logical, bytes, PointStorage::FloatList { values, rows })
            }
            _ => return Ok(None),
        };
        Ok(Some(Self { logical, bytes, storage }))
    }

    fn try_byte_list<T: ByteArrayType<Offset = i32>>(
        values: &'a dyn Array, rows: Range<usize>, element: &LogicalType,
    ) -> Option<Self> {
        let values = values.as_bytes_opt::<T>()?;
        let nulls = values.nulls().map(|nulls| nulls.slice(rows.start, rows.len()));
        let null_count = nulls.as_ref().map_or(0, NullBuffer::null_count);
        if null_count == rows.len() { return None; }
        let offsets = &values.value_offsets()[rows.start..=rows.end];
        let (element, payload_bytes, validity_bytes) = match nulls.filter(|_| null_count != 0) {
            None => (element.clone(), (offsets[rows.len()] - offsets[0]) as usize, 0),
            Some(nulls) => (
                element.clone().maybe(),
                nulls.valid_slices().map(|(start, end)| (offsets[end] - offsets[start]) as usize).sum(),
                bit_util::ceil(rows.len(), 64) * size_of::<u64>(),
            ),
        };
        // Point admission charges the canonical scalar-list field and the capacities of Arrow's byte interleave.
        let bytes = size_of::<GenericByteArray<T>>()
            .checked_add(Field::new("value", T::DATA_TYPE, null_count != 0).size())?
            .checked_add(size_of_val(offsets))?
            .checked_add(payload_bytes)?
            .checked_add(validity_bytes)?;
        Some(Self {
            logical: LogicalType::list(element),
            bytes,
            storage: PointStorage::ByteList { values, rows, payload_bytes },
        })
    }

    pub(crate) fn write(self, name: &str) -> Result<(Field, ArrayRef)> {
        if let PointStorage::Read(cell) = &self.storage {
            let value = rtdb_columnar::read_value(cell.field, cell.array, cell.row)?;
            let logical = value.logical_type();
            return Ok((rtdb_columnar::to_field(name, &logical)?, rtdb_columnar::build_array(&logical, &[value])?));
        }
        let field = rtdb_columnar::to_field(name, &self.logical)?;
        let array = self.write_as(&field)?;
        Ok((field, array))
    }

    fn write_as(self, field: &Field) -> Result<ArrayRef> {
        Ok(match self.storage {
            PointStorage::Empty => Arc::new(NullArray::new(1)),
            PointStorage::EmptySequence => {
                let offsets = ScalarBuffer::from(vec![0_i32, 0]);
                let data = arrow_array::new_empty_array(field.data_type()).to_data().into_builder().len(1)
                    .buffers(vec![offsets.into_inner()]).build().map_err(point_arrow)?;
                arrow_array::make_array(data)
            }
            PointStorage::Slice(cell) => {
                cell.validate_decimal_precision()?;
                let array = cell.array.slice(cell.row, 1);
                if array.nulls().is_some() {
                    let data = array.to_data().into_builder().nulls(None).build().map_err(point_arrow)?;
                    arrow_array::make_array(data)
                } else { array }
            }
            PointStorage::Compact { cell, validate_uri } => {
                if validate_uri { rtdb_types::UriParts::parse(cell.array.as_string::<i32>().value(cell.row))?; }
                cell.validate_decimal_precision()?;
                match field.data_type() {
                    DataType::Utf8 => Arc::new(StringArray::from(vec![Some(cell.array.as_string::<i32>().value(cell.row))])),
                    DataType::Binary => Arc::new(BinaryArray::from(vec![Some(cell.array.as_binary::<i32>().value(cell.row))])),
                    _ => rtdb_columnar::copy_selected_cells(&[cell.array], &[(0, cell.row)])?,
                }
            }
            PointStorage::FloatList { values, rows } => compact_float_list(field, values, rows)?,
            PointStorage::ByteList { values, rows, payload_bytes } => match values.data_type() {
                DataType::Utf8 => compact_byte_list(field, values.as_string::<i32>(), rows, payload_bytes)?,
                DataType::Binary => compact_byte_list(field, values.as_binary::<i32>(), rows, payload_bytes)?,
                _ => unreachable!("Prepared point byte list"),
            },
            PointStorage::Struct(children) => {
                let DataType::Struct(fields) = field.data_type() else { unreachable!("Prepared point struct") };
                let arrays = children.into_iter().zip(fields.iter()).map(|(child, field)| child.write_as(field))
                    .collect::<Result<Vec<_>>>()?;
                rtdb_columnar::build_struct_array(fields.clone(), &arrays, 1, None)?
            }
            PointStorage::Decoded(value) => rtdb_columnar::build_array(&self.logical, &[value])?,
            PointStorage::Read(_) => unreachable!("Deferred top-level point read"),
        })
    }
}

fn required_point_type(logical: &LogicalType) -> &LogicalType {
    match logical { LogicalType::Maybe(inner) => inner.as_ref(), ty => ty }
}

fn scalar_point_bytes(cell: ArrowCell<'_>, logical: &LogicalType) -> usize {
    let ArrowCell { array, row, .. } = cell;
    match logical {
        LogicalType::Scalar(ScalarType::Utf8 | ScalarType::Keyword | ScalarType::Uri) =>
            array.as_string::<i32>().value(row).len(),
        LogicalType::Scalar(ScalarType::Binary | ScalarType::Transit) =>
            array.as_binary::<i32>().value(row).len(),
        LogicalType::Scalar(ScalarType::FixedSizeBinary(_)) => array.as_fixed_size_binary().value(row).len(),
        LogicalType::Scalar(ScalarType::Timestamp { timezone, .. }) => timezone.as_ref().map_or(0, String::len),
        LogicalType::Scalar(ScalarType::Decimal { bit_width, .. }) => usize::from(bit_width / 8),
        _ => 0,
    }
}

fn compact_float_list(field: &Field, values: &dyn Array, rows: Range<usize>) -> Result<ArrayRef> {
    let (item, width) = match field.data_type() {
        DataType::List(item) => (item, None),
        DataType::FixedSizeList(item, width) => (item, Some(*width)),
        _ => unreachable!("Prepared point float list"),
    };
    let length = rows.len();
    let array: ArrayRef = match item.data_type() {
        DataType::Null => Arc::new(NullArray::new(length)),
        DataType::Float32 => {
            let mut output = Float32Builder::with_capacity(length);
            let values = values.as_primitive::<Float32Type>();
            if item.is_nullable() {
                output.extend(values.slice(rows.start, length).iter());
            } else {
                output.append_slice(&values.values()[rows]);
            }
            Arc::new(output.finish())
        }
        DataType::Float64 => {
            let mut output = Float64Builder::with_capacity(length);
            let values = values.as_primitive::<Float64Type>();
            if item.is_nullable() {
                output.extend(values.slice(rows.start, length).iter());
            } else {
                output.append_slice(&values.values()[rows]);
            }
            Arc::new(output.finish())
        }
        _ => unreachable!("Prepared point float components"),
    };
    Ok(match width {
        Some(width) => Arc::new(FixedSizeListArray::try_new(Arc::clone(item), width, array, None).map_err(point_arrow)?),
        None => {
            let end = i32::try_from(length).expect("An Arrow list has i32 offsets");
            let offsets = if item.is_nullable() {
                // Reply admission includes the offset capacity retained by the detached list writer.
                let mut offsets = vec![0];
                offsets.push(end);
                offsets
            } else {
                vec![0, end]
            };
            let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));
            Arc::new(ListArray::try_new(Arc::clone(item), offsets, array, None).map_err(point_arrow)?)
        }
    })
}

fn compact_byte_list<T: ByteArrayType<Offset = i32>>(
    field: &Field, values: &GenericByteArray<T>, rows: Range<usize>, payload_bytes: usize,
) -> Result<ArrayRef> {
    let DataType::List(item) = field.data_type() else { unreachable!("Prepared point byte list") };
    let length = rows.len();
    let nulls = item.is_nullable().then(|| {
        NullBuffer::from(BooleanBuffer::collect_bool(length, |index| values.is_valid(rows.start + index)))
    });
    let mut offsets = Vec::with_capacity(length + 1);
    let mut payload = Vec::with_capacity(payload_bytes);
    offsets.push(0);
    for (index, row) in rows.enumerate() {
        if nulls.as_ref().is_none_or(|nulls| nulls.is_valid(index)) {
            payload.extend_from_slice(values.value(row).as_ref());
        }
        offsets.push(i32::try_from(payload.len()).expect("Selected values fit the source i32 offsets"));
    }
    let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));
    let child: ArrayRef = Arc::new(GenericByteArray::<T>::try_new(offsets, payload.into(), nulls).map_err(point_arrow)?);
    // The encoder grant includes the parent offset capacity retained by the packed list writer.
    let mut offsets = vec![0];
    offsets.push(i32::try_from(length).expect("An Arrow list has i32 offsets"));
    rtdb_columnar::build_list_array(Arc::clone(item), offsets.into(), &child, None)
}

fn point_arrow(error: arrow_schema::ArrowError) -> rtdb_types::Anomaly {
    rtdb_types::Anomaly::incorrect("rtdb/invalid-arrow", error.to_string())
}

#[cfg(test)]
#[test]
fn held_point_plain_byte_lists_prepare_borrowed_child_spans() {
    let text = Arc::new(StringArray::from(vec!["skipped", "taken"])) as ArrayRef;
    let binary = Arc::new(BinaryArray::from_iter_values([b"skipped".as_slice(), b"taken"])) as ArrayRef;
    for (scalar, child) in [(ScalarType::Utf8, text), (ScalarType::Binary, binary)] {
        let field = rtdb_columnar::to_field("items", &LogicalType::list(LogicalType::Scalar(scalar))).unwrap();
        let DataType::List(item) = field.data_type() else { unreachable!() };
        let offsets = OffsetBuffer::new(vec![0, 1, 2].into());
        let array = ListArray::new(Arc::clone(item), offsets, Arc::clone(&child), None);
        let plan = PointCell::prepare(ArrowCell { field: &field, array: &array, row: 1 }).unwrap();
        let PointStorage::ByteList { values, rows, payload_bytes } = plan.storage else {
            panic!("Expected a borrowed byte-list plan");
        };
        assert!(std::ptr::addr_eq(values, child.as_ref()));
        assert_eq!(rows, 1..2);
        assert_eq!(payload_bytes, 5);
    }
}

#[cfg(test)]
#[test]
fn held_point_nullable_float_lists_keep_projected_validity_bits_and_the_original_charge() {
    use arrow_buffer::NullBuffer;
    use arrow_ipc::writer::IpcWriteOptions;
    use arrow_schema::Schema;

    let range_bits = 513;
    let f32_type = LogicalType::list(LogicalType::Scalar(ScalarType::F32).maybe());
    let f32_field = rtdb_columnar::to_field("items", &f32_type).unwrap();
    let DataType::List(item) = f32_field.data_type() else { unreachable!() };
    let mut floats = vec![123.0_f32; 1 + range_bits];
    floats[1] = f32::from_bits(0x7fc0_0042);
    floats[2] = -0.0;
    let mut validity = vec![false; floats.len()];
    validity[..3].fill(true);
    let values = Float32Array::new(floats.into(), Some(NullBuffer::from(validity)));
    let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 1 + range_bits as i32]));
    let f32_list = ListArray::try_new(Arc::clone(item), offsets, Arc::new(values), None).unwrap().slice(1, 1);
    let f32_present = ListArray::try_new(Arc::clone(item), OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
        Arc::new(Float32Array::from(vec![-0.0])), None).unwrap();

    let f64_type = LogicalType::List {
        kind: ListKind::FixedSizeList(2),
        element: Box::new(LogicalType::Scalar(ScalarType::F64).maybe()),
    };
    let f64_field = rtdb_columnar::to_field("fixed", &f64_type).unwrap();
    let DataType::FixedSizeList(item, width) = f64_field.data_type() else { unreachable!() };
    let values = Float64Array::new(vec![1.0, 2.0, 30.0, 40.0, -0.0, 60.0].into(),
        Some(NullBuffer::from(vec![true, true, false, false, true, false])));
    let f64_list = FixedSizeListArray::try_new(Arc::clone(item), *width, Arc::new(values), None).unwrap();
    let input = [
        (f32_field.clone(), Arc::new(f32_list) as ArrayRef),
        (f32_field, Arc::new(f32_present) as ArrayRef),
        (f64_field.clone(), Arc::new(f64_list.slice(1, 1)) as ArrayRef),
        (f64_field, Arc::new(f64_list.slice(2, 1)) as ArrayRef),
    ];
    for (field, array) in input {
        let cell = ArrowCell { field: &field, array: array.as_ref(), row: 0 };
        let decoded = rtdb_columnar::read_value(&field, array.as_ref(), 0).unwrap();
        let observed = decoded.logical_type();
        let plan = PointCell::prepare(cell).unwrap();
        assert!(matches!(&plan.storage, PointStorage::FloatList { .. }));
        assert_eq!(plan.heap_bytes(), decoded.retained_bytes().unwrap() - size_of::<Value>());
        let (output_field, output) = plan.write(field.name()).unwrap();
        assert_eq!(output_field, rtdb_columnar::to_field(field.name(), &observed).unwrap());
        let expected = rtdb_columnar::build_array(&observed, &[decoded]).unwrap();
        let schema = Arc::new(Schema::new(vec![output_field]));
        let ipc = |array| {
            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array]).unwrap();
            rtdb_format::ipc::write_arrow_stream(&schema, &[batch], IpcWriteOptions::default()).unwrap()
        };
        if let LogicalType::List { element, .. } = &observed
            && **element == LogicalType::Scalar(ScalarType::F32)
        {
            let list = output.as_any().downcast_ref::<ListArray>().unwrap();
            assert_eq!(list.offsets().inner().inner().capacity(), 2 * size_of::<i32>());
        } else {
            assert_eq!(output.get_array_memory_size(), expected.get_array_memory_size());
        }
        assert_eq!(ipc(output), ipc(expected));
    }
}


pub struct WorldIdentityPage {
    pub keys: Vec<EntityId>,
    pub more: bool,
    pub reservations: Vec<Arc<MemoryReservation>>,
}

/// A held overlay reply keeps its candidate grants through encoding.
pub enum WorldIdentityPageOutput {
    Rows(WorldIdentityPage),
    Local(rtdb_node::OwnedIdentityPageOutput),
}

pub struct WorldParentVersion {
    pub sequence: u64,
    pub system: Timestamp,
    pub timeline: Vec<rtdb_columnar::OwnedBatch>,
}

pub struct WorldParentVersions {
    pub floor: Timestamp,
    pub next_revision: u64,
    pub done: bool,
    pub scanned: usize,
    pub versions: Vec<WorldParentVersion>,
}

/// A verified live cut and its already admitted archived parent.
pub trait OwnedWorldRead: Send + Sync {
    fn retained_child_cut<'a>(
        &'a self, scope: rtdb_types::PlacedAttemptScope, publication: TransactionKey,
    ) -> BoxFuture<'a, Result<rtdb_node::OwnedForkCut>>;
    fn present(&self) -> Timestamp;
    fn ancestors(&self) -> Vec<(rtdb_types::Iid, Timestamp)>;
    fn ancestor(&self) -> Option<(rtdb_types::Iid, Timestamp)>;
    /// Local logical reading and its ordered physical revision.
    fn application_publication(&self) -> Option<(Timestamp, Timestamp)>;
    fn application_clock(&self) -> Option<Timestamp> {
        self.application_publication().map(|(reading, _)| reading)
    }
    fn initial_clock(&self) -> Timestamp;
    fn clock_mode(&self) -> WorldClockMode;
    fn parent_basis(&self) -> TransactionKey;
    fn ranked_search<'a>(
        &'a self, partitions: Vec<(OwnedRelation, OwnedRelation)>, query: String, offset: usize, limit: usize,
        valid: Timestamp, staged: Option<rtdb_node::TextShadows<'a>>,
    ) -> BoxFuture<'a, Result<(rtdb_node::OwnedSearchBatch, Vec<OwnedWorldWitness>)>>;
    fn ranked_search_at<'a>(
        &'a self, partitions: Vec<(OwnedRelation, OwnedRelation)>, query: String, offset: usize, limit: usize,
        valid: Timestamp, system: Timestamp, parent_only: bool,
    ) -> BoxFuture<'a, Result<(rtdb_node::OwnedSearchBatch, Vec<OwnedWorldWitness>)>>;
    /// Exact cosine neighbours of the effective world view (`NearestHeldWorld.v1`).
    fn nearest<'a>(
        &'a self, partitions: Vec<rtdb_node::nearest::NearestPartition>, query: rtdb_node::nearest::NearestQuery,
        valid: Timestamp, staged: Option<rtdb_node::nearest::HeldVectorOverlay>,
    ) -> BoxFuture<'a, Result<(rtdb_node::OwnedSearchBatch, Vec<OwnedWorldWitness>)>>;
    fn nearest_at<'a>(
        &'a self, partitions: Vec<rtdb_node::nearest::NearestPartition>, query: rtdb_node::nearest::NearestQuery,
        valid: Timestamp, system: Timestamp, parent_only: bool,
    ) -> BoxFuture<'a, Result<(rtdb_node::OwnedSearchBatch, Vec<OwnedWorldWitness>)>>;
    fn nearest_parent_at<'a>(
        &'a self, partitions: Vec<rtdb_node::nearest::NearestPartition>, query: rtdb_node::nearest::NearestQuery,
        valid: Timestamp, system: Timestamp,
    ) -> BoxFuture<'a, Result<(rtdb_node::OwnedSearchBatch, Vec<OwnedWorldWitness>)>>;
    fn range<'a>(
        &'a self, relation: OwnedRelation, path: OwnedRelation, request: OwnedRangeRequest<'a>, valid: Timestamp,
    ) -> BoxFuture<'a, Result<(OwnedRangePage, OwnedWorldWitness)>>;
    fn count<'a>(
        &'a self, relation: OwnedRelation, path: OwnedRelation, request: OwnedCountRequest<'a>,
    ) -> BoxFuture<'a, Result<(u64, OwnedWorldWitness)>>;
    fn measure<'a>(
        &'a self, relation: OwnedRelation, path: OwnedRelation, request: OwnedCountRequest<'a>,
    ) -> BoxFuture<'a, Result<(OwnedRangeCount, OwnedWorldWitness)>>;
    fn shared_column_census<'a>(
        &'a self, scope: rtdb_types::PlacedAttemptScope, valid: Timestamp,
        root_prefix: rtdb_types::SharedColumnCarrierPrefix<'a>, world_prefix: &'a str,
    ) -> BoxFuture<'a, Result<(WorldIdentityPage, [OwnedWorldWitness; 2])>>;
    fn parent_identity_page_at<'a>(
        &'a self, relation: &'a OwnedRelation, valid: Timestamp, after: &'a rtdb_types::OwnedIdentityScan<'a>, limit: usize, system: Timestamp,
    ) -> BoxFuture<'a, Result<WorldIdentityPage>>;
    fn parent_schedule_at<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid: Interval, system: Timestamp,
    ) -> BoxFuture<'a, Result<(Vec<(Interval, Timestamp)>, OwnedWorldWitness)>>;
    fn ranked_search_parent_at<'a>(
        &'a self, partitions: Vec<(OwnedRelation, OwnedRelation)>, query: String, offset: usize, limit: usize,
        valid: Timestamp, system: Timestamp,
    ) -> BoxFuture<'a, Result<(rtdb_node::OwnedSearchBatch, Vec<OwnedWorldWitness>)>>;
    fn parent_range_at<'a>(
        &'a self, relation: OwnedRelation, path: OwnedRelation, request: OwnedRangeRequest<'a>,
        valid: Timestamp, system: Timestamp,
    ) -> BoxFuture<'a, Result<(OwnedRangePage, OwnedWorldWitness)>>;
    fn parent_count_at<'a>(
        &'a self, relation: OwnedRelation, path: OwnedRelation, request: OwnedCountRequest<'a>, system: Timestamp,
    ) -> BoxFuture<'a, Result<(u64, OwnedWorldWitness)>>;
    fn overlay_range<'a>(
        &'a self, relation: OwnedRelation, path: OwnedRelation, request: OwnedRangeRequest<'a>, valid: Timestamp,
    ) -> BoxFuture<'a, Result<(OwnedRangePage, OwnedWorldWitness)>>;
    fn overlay_range_at<'a>(
        &'a self, relation: OwnedRelation, path: OwnedRelation, request: OwnedRangeRequest<'a>,
        valid: Timestamp, system: Timestamp,
    ) -> BoxFuture<'a, Result<OwnedRangePage>>;
    fn parent_pin(&self) -> rtdb_types::Iid;
    fn until(&self) -> Timestamp;
    fn evidence_until(&self) -> Timestamp;
    fn check_relation(&self, relation: &OwnedRelation) -> Result<()>;
    fn check_parent_point(&self, relation: &OwnedRelation, until: Option<Timestamp>) -> Result<()>;
    fn check(&self) -> Result<()>;
    fn close(&self);
    fn changes<'a>(&'a self, relations: &'a [OwnedRelation], after: OwnedChangeEdge, through: OwnedChangeEdge, limit: usize, descending: bool)
        -> BoxFuture<'a, Result<rtdb_node::OwnedMergedChanges>>;
    fn valid_during<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid: Interval, parent_only: bool,
    ) -> BoxFuture<'a, Result<(Vec<(Interval, Timestamp)>, Option<OwnedWorldWitness>)>>;
    fn valid_during_at<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid: Interval, system: Timestamp,
    ) -> BoxFuture<'a, Result<Vec<(Interval, Timestamp)>>>;
    fn identity_page<'a>(
        &'a self, relation: &'a OwnedRelation, valid: Timestamp, after: &'a rtdb_types::OwnedIdentityScan<'a>, limit: usize,
    ) -> BoxFuture<'a, Result<WorldIdentityPage>>;
    fn identity_page_at<'a>(
        &'a self, relation: &'a OwnedRelation, valid: Timestamp, after: &'a rtdb_types::OwnedIdentityScan<'a>, limit: usize,
        system: Timestamp, parent_only: bool,
    ) -> BoxFuture<'a, Result<WorldIdentityPage>>;
    fn overlay_identities<'a>(
        &'a self, relation: &'a OwnedRelation, valid: Timestamp, after: &'a rtdb_types::OwnedIdentityScan<'a>, limit: usize,
    ) -> BoxFuture<'a, Result<WorldIdentityPage>>;
    fn overlay_identities_output<'a>(
        &'a self, relation: &'a OwnedRelation, valid: Timestamp, after: &'a rtdb_types::OwnedIdentityScan<'a>, limit: usize,
    ) -> BoxFuture<'a, Result<WorldIdentityPageOutput>> {
        Box::pin(async move { self.overlay_identities(relation, valid, after, limit).await.map(WorldIdentityPageOutput::Rows) })
    }
    fn overlay_identities_at<'a>(
        &'a self, relation: &'a OwnedRelation, valid: Timestamp, after: &'a rtdb_types::OwnedIdentityScan<'a>,
        limit: usize, system: Timestamp,
    ) -> BoxFuture<'a, Result<WorldIdentityPage>>;
    fn overlay_identities_at_output<'a>(
        &'a self, relation: &'a OwnedRelation, valid: Timestamp, after: &'a rtdb_types::OwnedIdentityScan<'a>,
        limit: usize, system: Timestamp,
    ) -> BoxFuture<'a, Result<WorldIdentityPageOutput>> {
        Box::pin(async move { self.overlay_identities_at(relation, valid, after, limit, system).await.map(WorldIdentityPageOutput::Rows) })
    }
    fn parent_touch_images_at<'a>(
        &'a self, relation: &'a OwnedRelation, path: &'a OwnedRelation, id: &'a EntityId,
        selected: Timestamp, before: Timestamp, group: Timestamp, valid: Timestamp, images: bool,
    ) -> BoxFuture<'a, Result<[WorldPointResult; 2]>>;
    fn parent_changes_at<'a>(
        &'a self, relation: &'a OwnedRelation, selected: Timestamp, after: OwnedChangeEdge,
        through: OwnedChangeEdge, limit: usize, reverse: bool,
    ) -> BoxFuture<'a, Result<(rtdb_node::OwnedMergedChanges, OwnedWorldWitness)>>;
    fn ancestor_changes<'a>(
        &'a self, relation: &'a OwnedRelation, selected: Timestamp, after: OwnedChangeEdge,
        through: OwnedChangeEdge, limit: usize, reverse: bool,
    ) -> BoxFuture<'a, Result<(rtdb_node::OwnedMergedChanges, OwnedWorldWitness)>>;
    fn parent_versions_at<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, selected: Timestamp,
        request: rtdb_node::OwnedVersionRequest,
    ) -> BoxFuture<'a, Result<(WorldParentVersions, OwnedWorldWitness)>>;
    fn version<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId,
        from: Timestamp, through: Timestamp, group: Timestamp,
    ) -> BoxFuture<'a, Result<(Timestamp, Vec<(Interval, WorldPointResult)>)>>;
    fn path_touch<'a>(
        &'a self, relation: &'a OwnedRelation, path: &'a OwnedRelation, id: &'a EntityId,
        before: Timestamp, group: Timestamp, valid: Timestamp, images: bool,
    ) -> BoxFuture<'a, Result<[WorldPointResult; 2]>>;
    fn select_accepted_scope<'a>(
        &'a self, relations: &'a [OwnedRelation], requested: Timestamp,
    ) -> BoxFuture<'a, Result<Option<(TransactionKey, Timestamp)>>>;
    fn select_parent_as_of<'a>(
        &'a self, relations: &'a [OwnedRelation], requested: Timestamp,
    ) -> BoxFuture<'a, Result<Option<Timestamp>>>;
    fn parent_point_at<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid: Timestamp, system: Timestamp,
        original_scope: Option<rtdb_types::PlacedAttemptScope>,
    ) -> BoxFuture<'a, Result<WorldPointResult>>;
    fn preflight_accepted_scope<'a>(
        &'a self, relations: &'a [OwnedRelation], system: Timestamp,
    ) -> BoxFuture<'a, Result<(TransactionKey, Timestamp)>>;
    fn accepted_point<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid: Timestamp,
        system: Timestamp, original_scope: Option<rtdb_types::PlacedAttemptScope>,
    ) -> BoxFuture<'a, Result<WorldPointResult>>;
    fn read_point<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid_at: Timestamp, observed: bool,
    ) -> BoxFuture<'a, Result<WorldPointResult>>;
    fn read_ancestor_root_point<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid: Timestamp,
    ) -> BoxFuture<'a, Result<WorldPointResult>>;
    fn read_parent_point<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid_at: Timestamp, observed: bool,
        original_scope: Option<rtdb_types::PlacedAttemptScope>,
    ) -> BoxFuture<'a, Result<WorldPointResult>>;
}
