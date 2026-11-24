use std::{borrow::Cow, sync::Arc};

use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch};
use arrow_schema::{DataType, Field, FieldRef, TimeUnit, UnionMode};
use rtdb_columnar::{
    ArrowCell, MemoryBudget, MemoryReservation, from_field, read_value, read_value_as,
};
use rtdb_node::OwnedKeyBound;
use rtdb_types::{LogicalType, Value};
use tonic::Status;

use crate::{
    error::{invalid, status},
    service::Service,
    upload,
};

pub(super) struct OrderedControls<'a> {
    batch: &'a RecordBatch,
    decoded: Vec<(usize, Value)>,
}

enum Control<'a> {
    Arrow(&'a FieldRef, &'a ArrayRef),
    Decoded(&'a Value),
}

fn arrow_cell<'a>(field: &'a Field, array: &'a ArrayRef) -> ArrowCell<'a> {
    ArrowCell {
        field,
        array: array.as_ref(),
        row: 0,
    }
}

impl<'a> OrderedControls<'a> {
    pub(super) fn read(
        batch: &'a RecordBatch,
        service: &Service,
    ) -> Result<(Self, Arc<MemoryReservation>), Status> {
        Self::admit(batch, service.memory())
    }

    fn admit(
        batch: &'a RecordBatch,
        memory: &Arc<MemoryBudget>,
    ) -> Result<(Self, Arc<MemoryReservation>), Status> {
        let charge = upload::reserve_document_decode(batch, memory)?;
        let fields = batch.schema_ref().fields();
        for field in fields {
            from_field(field).map_err(status)?;
        }
        let mut decoded = Vec::new();
        for (index, field) in fields.iter().enumerate() {
            // The document path preserves the LocalDate diagnostic for an invalid _id.
            if plain_control(field)
                && (!has_date_identity(field) || fields.iter().all(|field| plain_control(field)))
            {
                continue;
            }
            let cell = arrow_cell(field, batch.column(index));
            let logical = from_field(field).map_err(status)?;
            if !exact_encoded_scalar(cell, &logical) {
                let value = read_value_as(&logical, field, cell.array, cell.row).map_err(status)?;
                decoded.push((index, value));
            }
        }
        Ok((Self { batch, decoded }, charge))
    }

    pub(super) fn contains(&self, name: &str) -> bool {
        self.batch.schema_ref().index_of(name).is_ok()
    }

    fn control(&self, name: &str) -> Option<Control<'_>> {
        let index = self.batch.schema_ref().index_of(name).ok()?;
        if let Some((_, value)) = self.decoded.iter().find(|(column, _)| *column == index) {
            return Some(Control::Decoded(value));
        }
        Some(Control::Arrow(
            &self.batch.schema_ref().fields()[index],
            self.batch.column(index),
        ))
    }

    pub(super) fn text(&self, name: &str) -> Option<&str> {
        match self.control(name)? {
            Control::Arrow(field, array) => arrow_cell(field, array).utf8(),
            Control::Decoded(Value::Utf8(value)) => Some(value),
            _ => None,
        }
    }

    pub(super) fn integer(&self, name: &str) -> Option<i64> {
        match self.control(name)? {
            Control::Arrow(field, array) => arrow_cell(field, array).int64(),
            Control::Decoded(Value::I64(value)) => Some(*value),
            _ => None,
        }
    }

    pub(super) fn boolean(&self, name: &str) -> Option<bool> {
        match self.control(name)? {
            Control::Arrow(field, array) => {
                let cell = arrow_cell(field, array).physical().ok()?;
                let values = cell.array.as_any().downcast_ref::<BooleanArray>()?;
                (!values.is_null(cell.row)).then(|| values.value(cell.row))
            }
            Control::Decoded(Value::Bool(value)) => Some(*value),
            _ => None,
        }
    }

    pub(super) fn bound(&self, name: &str) -> Result<Option<OwnedKeyBound<'_>>, Status> {
        let wrong_type = || invalid("Owned full-key range endpoints must be structs or null");
        match self.control(name) {
            Some(Control::Arrow(field, array)) => {
                let cell = arrow_cell(field, array);
                if cell.is_null().map_err(status)? {
                    return Ok(None);
                }
                cell.structure().ok_or_else(wrong_type)?;
                Ok(Some(OwnedKeyBound::Arrow {
                    field,
                    array,
                    row: 0,
                }))
            }
            Some(Control::Decoded(Value::Null)) => Ok(None),
            Some(Control::Decoded(Value::Struct(fields))) => {
                Ok(Some(OwnedKeyBound::Values(fields)))
            }
            _ => Err(wrong_type()),
        }
    }

    pub(super) fn leading(&self, name: &str) -> Result<Option<Cow<'_, Value>>, Status> {
        let value = match self.control(name) {
            Some(Control::Arrow(field, array)) => Some(Cow::Owned(
                read_value(field, array.as_ref(), 0).map_err(status)?,
            )),
            Some(Control::Decoded(value)) => Some(Cow::Borrowed(value)),
            None => None,
        };
        Ok(value.filter(|value| !matches!(value.as_ref(), Value::Null | Value::Undefined)))
    }
}

fn exact_encoded_scalar(cell: ArrowCell<'_>, logical: &LogicalType) -> bool {
    if !matches!(
        cell.field.data_type(),
        DataType::Union(_, UnionMode::Dense) | DataType::RunEndEncoded(..)
    ) {
        return false;
    }
    let Ok((selected, logical)) = cell.logical_position(Some(logical)) else {
        return false;
    };
    plain_scalar(selected.field)
        && from_field(selected.field).is_ok_and(|declared| declared == logical)
}

fn has_date_identity(field: &Field) -> bool {
    let DataType::Struct(fields) = field.data_type() else {
        return false;
    };
    fields.iter().any(|field| {
        field.name() == "_id" && matches!(field.data_type(), DataType::Date32 | DataType::Date64)
    })
}

fn plain_control(field: &Field) -> bool {
    match field.data_type() {
        DataType::Struct(fields) => {
            !field
                .metadata()
                .contains_key(rtdb_columnar::EXTENSION_NAME_KEY)
                && fields.iter().all(|field| plain_scalar(field))
        }
        _ => plain_scalar(field),
    }
}

fn plain_scalar(field: &Field) -> bool {
    if let Some(extension) = field.metadata().get(rtdb_columnar::EXTENSION_NAME_KEY) {
        return extension == "uuid" && field.data_type() == &DataType::FixedSizeBinary(16);
    }
    match field.data_type() {
        DataType::Null => field.is_nullable(),
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::Binary
        | DataType::Date32
        | DataType::Date64
        | DataType::Timestamp(TimeUnit::Microsecond, Some(_))
        | DataType::Decimal128(..) => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{
        Int16Array, Int32Array, RunArray, StringArray, StructArray, types::Int32Type,
    };

    use super::*;

    #[test]
    fn mixed_controls_keep_plain_bounds_and_selected_scalars_arrow_backed() {
        let path = Arc::new(StringArray::from(vec!["by-key"]));
        let upper = StructArray::new(
            vec![Field::new("_id", DataType::Utf8, false)].into(),
            vec![path.clone()],
            None,
        );
        let directions = RunArray::<Int32Type>::try_new(
            &Int32Array::from(vec![1]),
            &BooleanArray::from(vec![false]),
        )
        .unwrap();
        let keywords = StructArray::new(
            vec![
                Field::new("_id", DataType::Utf8, false).with_metadata(
                    [(
                        rtdb_columnar::EXTENSION_NAME_KEY.into(),
                        "xt/clj-keyword".into(),
                    )]
                    .into(),
                ),
            ]
            .into(),
            vec![path.clone()],
            None,
        );
        let batch = RecordBatch::try_from_iter([
            ("path", path.clone() as ArrayRef),
            ("lower", Arc::new(keywords)),
            ("upper", Arc::new(upper)),
            ("descending", Arc::new(directions)),
            ("limit", Arc::new(Int16Array::from(vec![2]))),
        ])
        .unwrap();
        let (controls, _charge) =
            OrderedControls::admit(&batch, &MemoryBudget::new(1 << 20)).unwrap();

        assert!(std::ptr::eq(controls.text("path").unwrap(), path.value(0)));
        assert_eq!(controls.boolean("descending"), Some(false));
        assert_eq!(controls.integer("limit"), None);
        assert_eq!(controls.decoded.len(), 1);
        assert!(matches!(
            controls.bound("upper").unwrap(),
            Some(OwnedKeyBound::Arrow { .. })
        ));
        let Some(OwnedKeyBound::Values(lower)) = controls.bound("lower").unwrap() else {
            panic!("expected a decoded bound");
        };
        assert!(matches!(lower.get("_id"), Some(Value::Keyword(_))));
    }

    #[test]
    fn a_nullable_endpoint_is_distinct_from_a_missing_control() {
        let batch = RecordBatch::try_from_iter([(
            "lower",
            Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
        )])
        .unwrap();
        let (controls, _charge) =
            OrderedControls::admit(&batch, &MemoryBudget::new(1 << 20)).unwrap();

        assert!(controls.contains("lower"));
        assert!(controls.bound("lower").unwrap().is_none());
        assert!(controls.leading("lower").unwrap().is_none());
        assert!(!controls.contains("upper"));
        assert!(controls.bound("upper").is_err());
    }
}
