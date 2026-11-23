use arrow_array::{
    Array, RecordBatch, ListArray, StructArray,
    cast::AsArray,
    types::{
        Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type, Int16Type, Int32Type,
        Int64Type,
    },
};
use arrow_schema::{DataType, Field, UnionMode};
use rtdb_types::{Anomaly, ListKind, LogicalType, Result, StructFields};

#[derive(Clone, Copy)]
pub struct ArrowCell<'a> {
    pub field: &'a Field,
    pub array: &'a dyn Array,
    pub row: usize,
}

impl<'a> ArrowCell<'a> {
    pub fn named(batch: &'a RecordBatch, name: &str, row: usize) -> Option<Self> {
        let (index, field) = batch.schema_ref().column_with_name(name)?;
        Some(Self {
            field,
            array: batch.column(index).as_ref(),
            row,
        })
    }

    pub fn int64(self) -> Option<i64> {
        let Self { field, array, row } = self.physical().ok()?;
        if field.data_type() != &DataType::Int64
            || field.metadata().contains_key(crate::EXTENSION_NAME_KEY)
            || array.is_null(row)
        {
            return None;
        }
        Some(array.as_primitive::<Int64Type>().value(row))
    }

    pub fn list(self) -> Option<impl ExactSizeIterator<Item = Self> + 'a> {
        if self.sequence_kind() != Some(ListKind::List) {
            return None;
        }
        self.sequence()
    }

    pub fn signed_integer(self) -> Option<i64> {
        use arrow_array::{cast::AsArray, types::*};
        use arrow_schema::DataType;

        if self.array.is_null(self.row) || !self.field.metadata().is_empty() {
            return None;
        }
        Some(match self.array.data_type() {
            DataType::Int8 => i64::from(self.array.as_primitive::<Int8Type>().value(self.row)),
            DataType::Int16 => i64::from(self.array.as_primitive::<Int16Type>().value(self.row)),
            DataType::Int32 => i64::from(self.array.as_primitive::<Int32Type>().value(self.row)),
            DataType::Int64 => self.array.as_primitive::<Int64Type>().value(self.row),
            DataType::UInt8 => {
                i64::from(self.array.as_primitive::<UInt8Type>().value(self.row) as i8)
            }
            DataType::UInt16 => {
                i64::from(self.array.as_primitive::<UInt16Type>().value(self.row) as i16)
            }
            DataType::UInt32 => {
                i64::from(self.array.as_primitive::<UInt32Type>().value(self.row) as i32)
            }
            DataType::UInt64 => self.array.as_primitive::<UInt64Type>().value(self.row) as i64,
            _ => return None,
        })
    }

    /// Bounds the metadata allocated by `observed_type` without charging the selected payload bytes.
    pub fn observed_type_bytes(self) -> Result<usize> {
        fn add_child(bytes: usize, child: ArrowCell<'_>) -> Result<usize> {
            bytes
                .checked_add(child.observed_type_bytes()?)
                .ok_or_else(layout)
        }

        let cell = self.physical()?;
        let bytes = cell
            .field
            .size()
            .checked_mul(16)
            .and_then(|bytes| bytes.checked_add(256))
            .ok_or_else(layout)?;
        if let Some(mut children) = cell.structure() {
            children.try_fold(bytes, add_child)
        } else if let Some(mut children) = cell.sequence() {
            children.try_fold(bytes, |bytes, child| {
                if child.is_undefined()? {
                    return Err(Anomaly::fault(
                        "xtdb/zero-leg-read",
                        "The bottom type has no values to read",
                    ));
                }
                add_child(bytes, child)
            })
        } else {
            Ok(bytes)
        }
    }

    /// Derives the type of this value using its selected children and union legs.
    pub fn observed_type(self) -> Result<LogicalType> {
        let cell = self.physical()?;
        let logical = cell.logical_type()?;
        Ok(match logical {
            LogicalType::Struct(_) => {
                let mut fields = StructFields::new();
                for child in cell.structure().ok_or_else(layout)? {
                    if fields
                        .insert(child.field.name().clone(), child.observed_type()?)
                        .is_some()
                    {
                        return Err(layout());
                    }
                }
                LogicalType::Struct(fields)
            }
            LogicalType::List {
                kind: ListKind::TimestampTzRange,
                ..
            } => {
                let element = LogicalType::Scalar(rtdb_types::ScalarType::Timestamp {
                    unit: rtdb_types::TimeUnit::Microsecond,
                    timezone: Some("UTC".into()),
                });
                LogicalType::List {
                    kind: ListKind::TimestampTzRange,
                    element: Box::new(element),
                }
            }
            LogicalType::List {
                kind: ListKind::Map { sorted },
                ..
            } => {
                let mut keys = LogicalType::Nothing;
                let mut values = LogicalType::Nothing;
                for entry in cell.sequence().ok_or_else(layout)? {
                    let mut fields = entry.structure().ok_or_else(layout)?;
                    let key = fields.next().ok_or_else(layout)?;
                    let value = fields.next().ok_or_else(layout)?;
                    keys = keys.join(&key.observed_type()?);
                    values = values.join(&value.observed_type()?);
                }
                let element = LogicalType::Struct(StructFields::from([
                    ("key".into(), keys),
                    ("value".into(), values),
                ]));
                LogicalType::List {
                    kind: ListKind::Map { sorted },
                    element: Box::new(element),
                }
            }
            LogicalType::List { kind, .. } => {
                let types = cell
                    .sequence()
                    .ok_or_else(layout)?
                    .map(Self::observed_type)
                    .collect::<Result<Vec<_>>>()?;
                LogicalType::List {
                    kind,
                    element: Box::new(LogicalType::join_all(&types)),
                }
            }
            logical => logical,
        })
    }

    pub fn physical(mut self) -> Result<Self> {
        loop {
            let Self { field, array, row } = self;
            if field.data_type() != array.data_type() || row >= array.len() {
                return Err(layout());
            }
            self = match field.data_type() {
                DataType::Union(fields, UnionMode::Dense) => {
                    let union = array.as_union();
                    let id = union.type_id(row);
                    let (_, field) = fields
                        .iter()
                        .find(|(tag, _)| *tag == id)
                        .ok_or_else(layout)?;
                    Self {
                        field,
                        array: union.child(id).as_ref(),
                        row: union.value_offset(row),
                    }
                }
                DataType::RunEndEncoded(ends, field) => {
                    macro_rules! run {
                        ($ty:ty) => {{
                            let run = array.as_run::<$ty>();
                            Self {
                                field,
                                array: run.values().as_ref(),
                                row: run.get_physical_index(row),
                            }
                        }};
                    }
                    match ends.data_type() {
                        DataType::Int16 => run!(Int16Type),
                        DataType::Int32 => run!(Int32Type),
                        DataType::Int64 => run!(Int64Type),
                        _ => return Err(layout()),
                    }
                }
                _ => return Ok(self),
            };
        }
    }

    pub fn logical_type(self) -> Result<LogicalType> {
        let cell = self.physical()?;
        if cell.is_undefined()? {
            return Ok(LogicalType::Nothing);
        }
        if cell.is_null()? {
            return Ok(LogicalType::Null);
        }
        crate::from_field(&cell.field.clone().with_nullable(false))
    }

    pub fn utf8(self) -> Option<&'a str> {
        let Self { field, array, row } = self.physical().ok()?;
        if field.data_type() != &DataType::Utf8
            || field.metadata().contains_key(crate::EXTENSION_NAME_KEY)
            || array.is_null(row)
        {
            return None;
        }
        Some(array.as_string::<i32>().value(row))
    }

    pub fn binary(self) -> Option<&'a [u8]> {
        let Self { field, array, row } = self.physical().ok()?;
        if field.data_type() != &DataType::Binary
            || field.metadata().contains_key(crate::EXTENSION_NAME_KEY)
            || array.is_null(row)
        {
            return None;
        }
        Some(array.as_binary::<i32>().value(row))
    }

    pub fn decimal_coefficient(self) -> Result<Option<(crate::DecimalParameters, [u8; 32])>> {
        let Self { field, array, row } = self.physical()?;
        let Some(parameters) = crate::decimal_field_parameters(field)? else {
            return Ok(None);
        };
        if array.is_null(row) {
            return Ok(None);
        }
        let mut coefficient = [0; 32];
        match field.data_type() {
            DataType::Decimal32(..) => coefficient[28..].copy_from_slice(
                &array
                    .as_primitive::<Decimal32Type>()
                    .value(row)
                    .to_be_bytes(),
            ),
            DataType::Decimal64(..) => coefficient[24..].copy_from_slice(
                &array
                    .as_primitive::<Decimal64Type>()
                    .value(row)
                    .to_be_bytes(),
            ),
            DataType::Decimal128(..) => coefficient[16..].copy_from_slice(
                &array
                    .as_primitive::<Decimal128Type>()
                    .value(row)
                    .to_be_bytes(),
            ),
            DataType::Decimal256(..) => {
                coefficient = array
                    .as_primitive::<Decimal256Type>()
                    .value(row)
                    .to_be_bytes()
            }
            _ => return Err(layout()),
        }
        Ok(Some((parameters, coefficient)))
    }

    pub fn is_undefined(self) -> Result<bool> {
        let cell = self.physical()?;
        Ok(cell.field.data_type() == &DataType::Null && !cell.field.is_nullable())
    }

    /// Borrows the selected children of a physical list, fixed-size list, or map.
    pub fn sequence(self) -> Option<impl ExactSizeIterator<Item = Self> + 'a> {
        let (field, array, range) = self.sequence_range()?;
        Some(range.map(move |row| Self { field, array, row }))
    }

    pub fn sequence_range(self) -> Option<(&'a Field, &'a dyn Array, std::ops::Range<usize>)> {
        let Self { field, array, row } = self.physical().ok()?;
        if array.is_null(row) {
            return None;
        }
        let (element, values, from, to): (&Field, &dyn Array, usize, usize) =
            match field.data_type() {
                DataType::List(element) => {
                    let list = array.as_any().downcast_ref::<ListArray>()?;
                    let offsets = list.value_offsets();
                    (
                        element,
                        list.values().as_ref(),
                        offsets[row] as usize,
                        offsets[row + 1] as usize,
                    )
                }
                DataType::FixedSizeList(element, width) => {
                    let list = array.as_fixed_size_list();
                    let from = list.value_offset(row) as usize;
                    (
                        element,
                        list.values().as_ref(),
                        from,
                        from.checked_add(usize::try_from(*width).ok()?)?,
                    )
                }
                DataType::Map(element, _) => {
                    let map = array.as_map();
                    let offsets = map.value_offsets();
                    (
                        element,
                        map.entries(),
                        offsets[row] as usize,
                        offsets[row + 1] as usize,
                    )
                }
                _ => return None,
            };
        (from <= to && to <= values.len()).then_some((element, values, from..to))
    }

    /// Counts the selected physical payload using only the active children.
    pub fn payload_bytes(self) -> Result<usize> {
        let cell = self.physical()?;
        if cell.is_null()? {
            return Ok(0);
        }
        let Self { field, array, row } = cell;
        let size = match field.data_type() {
            DataType::Utf8 => array.as_string::<i32>().value(row).len(),
            DataType::Binary => array.as_binary::<i32>().value(row).len(),
            DataType::FixedSizeBinary(width) => usize::try_from(*width).map_err(|_| layout())?,
            DataType::Struct(_) => {
                cell.structure()
                    .ok_or_else(layout)?
                    .try_fold(0usize, |total, child| {
                        total
                            .checked_add(child.field.name().len() + 64)
                            .and_then(|size| size.checked_add(child.payload_bytes().ok()?))
                            .ok_or_else(layout)
                    })?
            }
            DataType::List(_) | DataType::FixedSizeList(..) | DataType::Map(..) => cell
                .sequence()
                .ok_or_else(layout)?
                .try_fold(0usize, |total, child| {
                    total
                        .checked_add(child.payload_bytes()?)
                        .and_then(|size| size.checked_add(32))
                        .ok_or_else(layout)
                })?,
            DataType::Timestamp(_, timezone) => 32 + timezone.as_ref().map_or(0, |zone| zone.len()),
            _ => 32,
        };
        Ok(size)
    }

    pub fn sequence_kind(self) -> Option<ListKind> {
        let cell = self.physical().ok()?;
        Some(
            match (cell.field.data_type(), cell.field.extension_type_name()) {
                (DataType::List(_), Some("set")) => ListKind::Set,
                (DataType::List(_), None) => ListKind::List,
                (DataType::FixedSizeList(_, 2), Some("xt/tstz-range")) => {
                    ListKind::TimestampTzRange
                }
                (DataType::FixedSizeList(_, width), None) => ListKind::FixedSizeList(*width),
                (DataType::Map(_, sorted), None) => ListKind::Map { sorted: *sorted },
                _ => return None,
            },
        )
    }

    pub fn structure(self) -> Option<impl ExactSizeIterator<Item = Self> + 'a> {
        let Self { field, array, row } = self.physical().ok()?;
        let DataType::Struct(fields) = field.data_type() else {
            return None;
        };
        if field.metadata().contains_key(crate::EXTENSION_NAME_KEY) || array.is_null(row) {
            return None;
        }
        let structure = array.as_any().downcast_ref::<StructArray>()?;
        Some(
            fields
                .iter()
                .zip(structure.columns())
                .map(move |(field, array)| Self {
                    field,
                    array: array.as_ref(),
                    row,
                }),
        )
    }

    pub fn is_null(self) -> Result<bool> {
        let Self { field, array, row } = self.physical()?;
        Ok(field.data_type() == &DataType::Null || array.is_null(row))
    }
}

fn layout() -> Anomaly {
    Anomaly::fault(
        "rtdb/arrow-cell",
        "Cell and field disagree with their Arrow layout",
    )
}
