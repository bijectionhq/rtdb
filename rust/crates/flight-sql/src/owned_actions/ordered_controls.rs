use std::{borrow::Cow, sync::Arc};

use arrow_array::{Array, BooleanArray, RecordBatch};
use arrow_schema::{DataType, Field, TimeUnit};
use rtdb_columnar::{ArrowCell, MemoryReservation, read_value};
use rtdb_node::OwnedKeyBound;
use rtdb_types::{StructFields, Value};
use tonic::Status;

use crate::{error::{invalid, status}, service::Service, upload};

pub(super) enum OrderedControls<'a> {
    Arrow(&'a RecordBatch),
    Values(StructFields<Value>),
}

impl<'a> OrderedControls<'a> {
    pub(super) fn read(batch: &'a RecordBatch, service: &Service) -> Result<(Self, Arc<MemoryReservation>), Status> {
        if batch.schema_ref().fields().iter().all(|field| plain_control(field)) {
            let charge = upload::reserve_document_decode(batch, service.memory())?;
            for field in batch.schema_ref().fields() {
                rtdb_columnar::from_field(field).map_err(status)?;
            }
            return Ok((Self::Arrow(batch), charge));
        }
        let (mut rows, charge) = upload::domain_documents(batch, service)?;
        Ok((Self::Values(rows.pop().expect("validated one row")), charge))
    }

    pub(super) fn contains(&self, name: &str) -> bool {
        match self {
            Self::Arrow(batch) => batch.schema_ref().index_of(name).is_ok(),
            Self::Values(row) => row.get(name).is_some(),
        }
    }

    pub(super) fn text(&self, name: &str) -> Option<&str> {
        match self {
            Self::Arrow(batch) => ArrowCell::named(batch, name, 0)?.utf8(),
            Self::Values(row) => match row.get(name)? {
                Value::Utf8(value) => Some(value),
                _ => None,
            },
        }
    }

    pub(super) fn integer(&self, name: &str) -> Option<i64> {
        match self {
            Self::Arrow(batch) => ArrowCell::named(batch, name, 0)?.int64(),
            Self::Values(row) => match row.get(name)? {
                Value::I64(value) => Some(*value),
                _ => None,
            },
        }
    }

    pub(super) fn boolean(&self, name: &str) -> Option<bool> {
        match self {
            Self::Arrow(batch) => {
                let cell = ArrowCell::named(batch, name, 0)?;
                let values = cell.array.as_any().downcast_ref::<BooleanArray>()?;
                (!values.is_null(cell.row)).then(|| values.value(cell.row))
            }
            Self::Values(row) => match row.get(name)? {
                Value::Bool(value) => Some(*value),
                _ => None,
            },
        }
    }

    pub(super) fn bound(&self, name: &str) -> Result<Option<OwnedKeyBound<'_>>, Status> {
        let wrong_type = || invalid("Owned full-key range endpoints must be structs or null");
        match self {
            Self::Arrow(batch) => {
                let index = batch.schema_ref().index_of(name).map_err(|_| wrong_type())?;
                let field = &batch.schema_ref().fields()[index];
                let array = batch.column(index);
                let cell = ArrowCell { field, array: array.as_ref(), row: 0 };
                if cell.is_null().map_err(status)? { return Ok(None); }
                cell.structure().ok_or_else(wrong_type)?;
                Ok(Some(OwnedKeyBound::Arrow { field, array, row: 0 }))
            }
            Self::Values(row) => match row.get(name) {
                Some(Value::Null) => Ok(None),
                Some(Value::Struct(fields)) => Ok(Some(OwnedKeyBound::Values(fields))),
                _ => Err(wrong_type()),
            },
        }
    }

    pub(super) fn leading(&self, name: &str) -> Result<Option<Cow<'_, Value>>, Status> {
        let value = match self {
            Self::Arrow(batch) => ArrowCell::named(batch, name, 0).map(|cell| {
                read_value(cell.field, cell.array, cell.row).map(Cow::Owned)
            }).transpose().map_err(status)?,
            Self::Values(row) => row.get(name).map(Cow::Borrowed),
        };
        Ok(value.filter(|value| !matches!(value.as_ref(), Value::Null | Value::Undefined)))
    }
}

fn plain_control(field: &Field) -> bool {
    match field.data_type() {
        DataType::Struct(fields) => !field.metadata().contains_key(rtdb_columnar::EXTENSION_NAME_KEY)
            && fields.iter().all(|field| plain_scalar(field)),
        _ => plain_scalar(field),
    }
}

fn plain_scalar(field: &Field) -> bool {
    if let Some(extension) = field.metadata().get(rtdb_columnar::EXTENSION_NAME_KEY) {
        return extension == "uuid" && field.data_type() == &DataType::FixedSizeBinary(16);
    }
    match field.data_type() {
        DataType::Null => field.is_nullable(),
        DataType::Boolean | DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
        | DataType::Float32 | DataType::Float64 | DataType::Utf8 | DataType::Binary
        | DataType::Date32 | DataType::Date64 | DataType::Timestamp(TimeUnit::Microsecond, Some(_))
        | DataType::Decimal128(..) => true,
        _ => false,
    }
}
