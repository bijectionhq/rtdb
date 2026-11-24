use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, FixedSizeBinaryArray, UnionArray,
    builder::{BinaryBuilder, FixedSizeBinaryBuilder, Int64Builder, StringBuilder},
    cast::AsArray,
    types::{Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type},
};
use arrow_buffer::ScalarBuffer;
use arrow_schema::{DataType, Field, UnionMode};
use rtdb_types::{Anomaly, EntityId, Iid, Keyword, LogicalType, Result, ScalarType};

use crate::{from_field, to_field};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum EntityIdRef<'a> {
    String(&'a str),
    Keyword(&'a str),
    Integer(i64),
    Uuid([u8; 16]),
    Binary(&'a [u8]),
}

impl<'a> EntityIdRef<'a> {
    pub fn from_handle_bytes(handle: &'a [u8]) -> Result<Self> {
        let invalid = || Anomaly::incorrect("rtdb.owned/invalid-handle", "Invalid complete typed identity");
        let (tag, payload) = handle.split_first().ok_or_else(invalid)?;
        match tag {
            0 => Ok(Self::String(std::str::from_utf8(payload).map_err(|_| invalid())?)),
            1 => Ok(Self::Keyword(std::str::from_utf8(payload).map_err(|_| invalid())?)),
            2 => Ok(Self::Integer((u64::from_be_bytes(payload.try_into().map_err(|_| invalid())?) ^ (1 << 63)) as i64)),
            3 => Ok(Self::Uuid(payload.try_into().map_err(|_| invalid())?)),
            4 => Ok(Self::Binary(payload)),
            _ => Err(invalid()),
        }
    }

    pub fn iid(self) -> Iid {
        match self {
            Self::String(value) => EntityId::string_iid(value),
            Self::Keyword(value) => EntityId::keyword_iid(value),
            Self::Integer(value) => EntityId::Integer(value).iid(),
            Self::Uuid(value) => Iid(value),
            Self::Binary(value) => EntityId::binary_iid(value),
        }
    }

    pub fn owned_payload_bytes(self) -> usize {
        match self {
            Self::String(value) | Self::Keyword(value) => value.len(),
            Self::Binary(value) => value.len(),
            Self::Integer(_) | Self::Uuid(_) => 0,
        }
    }

    pub fn into_owned(self) -> EntityId {
        match self {
            Self::String(text) => EntityId::String(text.to_owned()),
            Self::Keyword(name) => EntityId::Keyword(Keyword::from_name(name.to_owned())),
            Self::Integer(value) => EntityId::Integer(value),
            Self::Uuid(value) => EntityId::Uuid(value),
            Self::Binary(value) => EntityId::Binary(value.to_vec()),
        }
    }
}

impl<'a> From<&'a EntityId> for EntityIdRef<'a> {
    fn from(id: &'a EntityId) -> Self {
        match id {
            EntityId::String(text) => Self::String(text),
            EntityId::Keyword(keyword) => Self::Keyword(keyword.as_str()),
            EntityId::Integer(value) => Self::Integer(*value),
            EntityId::Uuid(value) => Self::Uuid(*value),
            EntityId::Binary(value) => Self::Binary(value),
        }
    }
}

/// Borrows a complete entity ID from its active Arrow leg.
pub fn read_entity_id<'a>(field: &Field, array: &'a dyn Array, row: usize) -> Result<EntityIdRef<'a>> {
    read_id(field, array, row, false)
}

pub fn read_owned_entity_id<'a>(field: &Field, array: &'a dyn Array, row: usize) -> Result<EntityIdRef<'a>> {
    read_id(field, array, row, true)
}

fn read_id<'a>(field: &Field, array: &'a dyn Array, row: usize, owned: bool) -> Result<EntityIdRef<'a>> {
    if row >= array.len() || field.data_type() != array.data_type() { return Err(invalid()); }
    match field.data_type() {
        DataType::Union(fields, UnionMode::Dense) => {
            let union = array.as_union();
            let id = union.type_id(row);
            let (_, child) = fields.iter().find(|(tag, _)| *tag == id).ok_or_else(invalid)?;
            read_id(child, union.child(id).as_ref(), union.value_offset(row), owned)
        }
        DataType::RunEndEncoded(ends, child) => {
            macro_rules! read_run {
                ($ty:ty) => {{
                    let runs = array.as_run::<$ty>();
                    read_id(child, runs.values().as_ref(), runs.get_physical_index(row), owned)
                }};
            }
            match ends.data_type() {
                DataType::Int16 => read_run!(Int16Type),
                DataType::Int32 => read_run!(Int32Type),
                DataType::Int64 => read_run!(Int64Type),
                _ => Err(invalid()),
            }
        }
        _ if array.is_null(row) => Err(invalid()),
        _ => {
            let logical = from_field(field)?;
            borrow_id_scalar(&logical, array, row, false, owned).ok_or_else(invalid)
        }
    }
}

pub(crate) fn borrow_id_scalar<'a>(
    logical: &LogicalType, array: &'a dyn Array, row: usize, execution: bool, owned: bool,
) -> Option<EntityIdRef<'a>> {
    use ScalarType::*;
    let logical = match logical { LogicalType::Maybe(inner) => inner.as_ref(), ty => ty };
    let LogicalType::Scalar(scalar) = logical else { return None; };
    Some(match scalar {
        Utf8 => EntityIdRef::String(array.as_string_opt::<i32>()?.value(row)),
        Keyword => EntityIdRef::Keyword(array.as_string_opt::<i32>()?.value(row)),
        I8 => EntityIdRef::Integer(i64::from(array.as_primitive_opt::<Int8Type>()?.value(row))),
        I16 => EntityIdRef::Integer(i64::from(array.as_primitive_opt::<Int16Type>()?.value(row))),
        I32 => EntityIdRef::Integer(i64::from(array.as_primitive_opt::<Int32Type>()?.value(row))),
        I64 => EntityIdRef::Integer(array.as_primitive_opt::<Int64Type>()?.value(row)),
        U8 if execution => EntityIdRef::Integer(i64::from(array.as_primitive_opt::<UInt8Type>()?.value(row) as i8)),
        U16 if execution => EntityIdRef::Integer(i64::from(array.as_primitive_opt::<UInt16Type>()?.value(row) as i16)),
        U32 if execution => EntityIdRef::Integer(i64::from(array.as_primitive_opt::<UInt32Type>()?.value(row) as i32)),
        U64 if execution => EntityIdRef::Integer(array.as_primitive_opt::<UInt64Type>()?.value(row) as i64),
        Binary if owned => EntityIdRef::Binary(array.as_binary_opt::<i32>()?.value(row)),
        Uuid => EntityIdRef::Uuid(array.as_any().downcast_ref::<FixedSizeBinaryArray>()?.value(row).try_into().ok()?),
        _ => return None,
    })
}

pub fn entity_id_type() -> LogicalType {
    LogicalType::from_legs([ScalarType::Utf8, ScalarType::Keyword, ScalarType::I64, ScalarType::Uuid, ScalarType::Binary].map(LogicalType::Scalar))
}

/// Builds the dense Arrow union used for a batch of complete entity identifiers.
pub fn entity_id_array<'a>(ids: impl IntoIterator<Item = EntityIdRef<'a>>) -> Result<ArrayRef> {
    try_entity_id_array(ids.into_iter().map(Ok))
}

/// Builds the dense identity union from fallibly read cells in row order.
pub fn try_entity_id_array<'a>(ids: impl IntoIterator<Item = Result<EntityIdRef<'a>>>) -> Result<ArrayRef> {
    let field = to_field("_id", &entity_id_type())?;
    let DataType::Union(fields, UnionMode::Dense) = field.data_type() else { unreachable!() };
    let types = fields.iter().map(|(tag, field)| Ok((tag, from_field(field)?))).collect::<Result<Vec<_>>>()?;
    let tag = |scalar| {
        let logical = LogicalType::Scalar(scalar);
        types.iter().find(|(_, ty)| ty == &logical).map(|(tag, _)| *tag).ok_or_else(invalid)
    };
    let (strings, keywords, integers, uuids) = (tag(ScalarType::Utf8)?, tag(ScalarType::Keyword)?, tag(ScalarType::I64)?, tag(ScalarType::Uuid)?);
    let binaries = tag(ScalarType::Binary)?;
    let ids = ids.into_iter();
    let capacity = ids.size_hint().0;
    let mut text = StringBuilder::with_capacity(capacity, 0);
    let mut keyword = StringBuilder::with_capacity(capacity, 0);
    let mut integer = Int64Builder::with_capacity(capacity);
    let mut uuid = FixedSizeBinaryBuilder::with_capacity(capacity, 16);
    let mut binary = BinaryBuilder::with_capacity(capacity, 0);
    let mut type_ids = Vec::with_capacity(capacity);
    let mut offsets = Vec::with_capacity(capacity);
    let mut counts = [0i32; 5];
    for id in ids {
        let (tag, count) = match id? {
            EntityIdRef::String(value) => { text.append_value(value); (strings, &mut counts[0]) }
            EntityIdRef::Keyword(value) => { keyword.append_value(value); (keywords, &mut counts[1]) }
            EntityIdRef::Integer(value) => { integer.append_value(value); (integers, &mut counts[2]) }
            EntityIdRef::Uuid(value) => { uuid.append_value(value).map_err(crate::arrow_error)?; (uuids, &mut counts[3]) }
            EntityIdRef::Binary(value) => { binary.append_value(value); (binaries, &mut counts[4]) }
        };
        type_ids.push(tag);
        offsets.push(*count);
        *count = count.checked_add(1).ok_or_else(invalid)?;
    }
    let text = Arc::new(text.finish()) as ArrayRef;
    let keyword = Arc::new(keyword.finish()) as ArrayRef;
    let integer = Arc::new(integer.finish()) as ArrayRef;
    let uuid = Arc::new(uuid.finish()) as ArrayRef;
    let binary = Arc::new(binary.finish()) as ArrayRef;
    let children = fields.iter().map(|(tag, _)| Arc::clone(
        if tag == strings { &text } else if tag == keywords { &keyword } else if tag == integers { &integer } else if tag == uuids { &uuid } else { &binary }
    )).collect();
    Ok(Arc::new(UnionArray::try_new(fields.clone(), ScalarBuffer::from(type_ids), Some(ScalarBuffer::from(offsets)), children).map_err(crate::arrow_error)?))
}

pub(crate) fn invalid() -> Anomaly {
    Anomaly::incorrect("xtdb/invalid-id", "An ID must be a string, keyword, signed integer, or UUID")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_dense_arrow_id_union_retains_every_id_kind_in_request_order() {
        let ids = [
            EntityIdRef::Integer(3), EntityIdRef::String("3"), EntityIdRef::Keyword("app/id"),
            EntityIdRef::Uuid([3; 16]), EntityIdRef::String("more"),
        ];
        let array = entity_id_array(ids).unwrap();
        let field = to_field("_id", &entity_id_type()).unwrap();
        let actual = (0..ids.len()).map(|row| read_entity_id(&field, array.as_ref(), row).unwrap()).collect::<Vec<_>>();
        assert_eq!(actual, ids);
    }

    #[test]
    fn rebuilding_arrow_identity_cells_preserves_typed_ids_in_row_order() -> Result<()> {
        use rtdb_types::Value;

        let uuid = [0xf3; 16];
        let values = [
            Value::Utf8("places/東京".into()),
            Value::Keyword(Keyword::from_name("places/東京")),
            Value::I32(-3),
            Value::Uuid(uuid),
            Value::I32(7),
        ];
        let logical = LogicalType::from_legs(values.iter().map(Value::logical_type));
        let source = to_field("_id", &logical)?;
        let target = to_field("_id", &entity_id_type())?;
        let input = crate::build_array(&logical, &values)?;
        assert!(crate::relayout_arrow_column(Arc::clone(&input), &source, &target).is_err());

        let cells = (0..input.len()).map(|row| read_entity_id(&source, input.as_ref(), row));
        let output = try_entity_id_array(cells.clone())?;
        assert_eq!(output.data_type(), target.data_type());
        let expected = [
            EntityIdRef::String("places/東京"),
            EntityIdRef::Keyword("places/東京"),
            EntityIdRef::Integer(-3),
            EntityIdRef::Uuid(uuid),
            EntityIdRef::Integer(7),
        ];
        assert_eq!(output.len(), expected.len());
        for (row, id) in expected.into_iter().enumerate() {
            assert_eq!(read_entity_id(&target, output.as_ref(), row)?, id);
        }
        let invalid_cell = read_entity_id(&source, input.as_ref(), input.len());
        assert_eq!(try_entity_id_array(cells.chain([invalid_cell])).unwrap_err(), invalid());
        Ok(())
    }

    #[test]
    fn borrowed_identity_hash_matches_the_owned_identity() {
        for id in [
            EntityIdRef::String("spot"),
            EntityIdRef::Keyword("places/spot"),
            EntityIdRef::Integer(i64::MIN),
            EntityIdRef::Uuid([4; 16]),
        ] {
            assert_eq!(id.iid(), id.into_owned().iid());
        }
    }

    #[test]
    fn entity_ids_borrow_from_sliced_runs_with_raw_and_execution_semantics() {
        use rtdb_types::{RunEndWidth, Value};

        for value in [Value::U64(u64::MAX), Value::Utf8("東京".into())] {
            let logical = LogicalType::RunEndEncoded {
                width: RunEndWidth::I32, value: Box::new(value.logical_type().maybe()),
            };
            let array = crate::build_array(&logical, &[value.clone(), value, Value::Null]).unwrap();
            let array = array.slice(1, 2);
            let field = to_field("_id", &logical).unwrap();
            let reader = crate::ColumnReader::new(&field).unwrap();
            let expected = reader.read(array.as_ref(), 0).unwrap().entity_id();
            let actual = reader.read_entity_id(array.as_ref(), 0).map(|id| id.unwrap().into_owned());
            assert_eq!(actual, expected);
            let expected = crate::read_execution_value(&field, array.as_ref(), 0).unwrap().entity_id().unwrap();
            let actual = crate::read_execution_entity_id_as(None, &field, array.as_ref(), 0).unwrap().unwrap();
            assert_eq!(actual.into_owned(), expected);
            assert_eq!(actual.iid(), expected.iid());
            assert_eq!(reader.read_entity_id(array.as_ref(), 1).unwrap(), None);
        }
    }
}
