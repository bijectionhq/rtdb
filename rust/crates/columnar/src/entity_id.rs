use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, FixedSizeBinaryArray, UnionArray,
    builder::{FixedSizeBinaryBuilder, Int64Builder, StringBuilder},
    cast::AsArray,
    types::{Int8Type, Int16Type, Int32Type, Int64Type},
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
            _ => Err(invalid()),
        }
    }

    pub fn iid(self) -> Iid {
        match self {
            Self::String(value) => EntityId::string_iid(value),
            Self::Keyword(value) => EntityId::keyword_iid(value),
            Self::Integer(value) => EntityId::Integer(value).iid(),
            Self::Uuid(value) => Iid(value),
        }
    }

    pub fn owned_payload_bytes(self) -> usize {
        match self {
            Self::String(value) | Self::Keyword(value) => value.len(),
            Self::Integer(_) | Self::Uuid(_) => 0,
        }
    }

    pub fn into_owned(self) -> EntityId {
        match self {
            Self::String(text) => EntityId::String(text.to_owned()),
            Self::Keyword(name) => EntityId::Keyword(Keyword::from_name(name.to_owned())),
            Self::Integer(value) => EntityId::Integer(value),
            Self::Uuid(value) => EntityId::Uuid(value),
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
        }
    }
}

/// Borrows a complete entity ID from its active Arrow leg.
pub fn read_entity_id<'a>(field: &Field, array: &'a dyn Array, row: usize) -> Result<EntityIdRef<'a>> {
    if row >= array.len() || field.data_type() != array.data_type() { return Err(invalid()); }
    match field.data_type() {
        DataType::Union(fields, UnionMode::Dense) => {
            let union = array.as_union();
            let id = union.type_id(row);
            let (_, child) = fields.iter().find(|(tag, _)| *tag == id).ok_or_else(invalid)?;
            read_entity_id(child, union.child(id).as_ref(), union.value_offset(row))
        }
        DataType::RunEndEncoded(ends, child) => {
            macro_rules! read_run {
                ($ty:ty) => {{
                    let runs = array.as_run::<$ty>();
                    read_entity_id(child, runs.values().as_ref(), runs.get_physical_index(row))
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
            let logical = match &logical { LogicalType::Maybe(inner) => inner.as_ref(), ty => ty };
            Ok(match logical {
                LogicalType::Scalar(ScalarType::Utf8) => EntityIdRef::String(array.as_string::<i32>().value(row)),
                LogicalType::Scalar(ScalarType::Keyword) => EntityIdRef::Keyword(array.as_string::<i32>().value(row)),
                LogicalType::Scalar(ScalarType::I8) => EntityIdRef::Integer(i64::from(array.as_primitive::<Int8Type>().value(row))),
                LogicalType::Scalar(ScalarType::I16) => EntityIdRef::Integer(i64::from(array.as_primitive::<Int16Type>().value(row))),
                LogicalType::Scalar(ScalarType::I32) => EntityIdRef::Integer(i64::from(array.as_primitive::<Int32Type>().value(row))),
                LogicalType::Scalar(ScalarType::I64) => EntityIdRef::Integer(array.as_primitive::<Int64Type>().value(row)),
                LogicalType::Scalar(ScalarType::Uuid) => EntityIdRef::Uuid(
                    array.as_any().downcast_ref::<FixedSizeBinaryArray>().ok_or_else(invalid)?.value(row).try_into().map_err(|_| invalid())?,
                ),
                _ => return Err(invalid()),
            })
        }
    }
}

pub fn entity_id_type() -> LogicalType {
    LogicalType::from_legs([ScalarType::Utf8, ScalarType::Keyword, ScalarType::I64, ScalarType::Uuid].map(LogicalType::Scalar))
}

/// Builds the dense Arrow union used for a batch of complete entity identifiers.
pub fn entity_id_array<'a>(ids: impl IntoIterator<Item = EntityIdRef<'a>>) -> Result<ArrayRef> {
    let field = to_field("_id", &entity_id_type())?;
    let DataType::Union(fields, UnionMode::Dense) = field.data_type() else { unreachable!() };
    let types = fields.iter().map(|(tag, field)| Ok((tag, from_field(field)?))).collect::<Result<Vec<_>>>()?;
    let tag = |scalar| {
        let logical = LogicalType::Scalar(scalar);
        types.iter().find(|(_, ty)| ty == &logical).map(|(tag, _)| *tag).ok_or_else(invalid)
    };
    let (strings, keywords, integers, uuids) = (tag(ScalarType::Utf8)?, tag(ScalarType::Keyword)?, tag(ScalarType::I64)?, tag(ScalarType::Uuid)?);
    let ids = ids.into_iter();
    let capacity = ids.size_hint().0;
    let mut text = StringBuilder::with_capacity(capacity, 0);
    let mut keyword = StringBuilder::with_capacity(capacity, 0);
    let mut integer = Int64Builder::with_capacity(capacity);
    let mut uuid = FixedSizeBinaryBuilder::with_capacity(capacity, 16);
    let mut type_ids = Vec::with_capacity(capacity);
    let mut offsets = Vec::with_capacity(capacity);
    let mut counts = [0i32; 4];
    for id in ids {
        let (tag, count) = match id {
            EntityIdRef::String(value) => { text.append_value(value); (strings, &mut counts[0]) }
            EntityIdRef::Keyword(value) => { keyword.append_value(value); (keywords, &mut counts[1]) }
            EntityIdRef::Integer(value) => { integer.append_value(value); (integers, &mut counts[2]) }
            EntityIdRef::Uuid(value) => { uuid.append_value(value).map_err(crate::arrow_error)?; (uuids, &mut counts[3]) }
        };
        type_ids.push(tag);
        offsets.push(*count);
        *count = count.checked_add(1).ok_or_else(invalid)?;
    }
    let text = Arc::new(text.finish()) as ArrayRef;
    let keyword = Arc::new(keyword.finish()) as ArrayRef;
    let integer = Arc::new(integer.finish()) as ArrayRef;
    let uuid = Arc::new(uuid.finish()) as ArrayRef;
    let children = fields.iter().map(|(tag, _)| Arc::clone(
        if tag == strings { &text } else if tag == keywords { &keyword } else if tag == integers { &integer } else { &uuid }
    )).collect();
    Ok(Arc::new(UnionArray::try_new(fields.clone(), ScalarBuffer::from(type_ids), Some(ScalarBuffer::from(offsets)), children).map_err(crate::arrow_error)?))
}

fn invalid() -> Anomaly {
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
}
