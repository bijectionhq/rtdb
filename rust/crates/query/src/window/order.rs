use std::{mem::size_of, sync::Arc};

use arrow_schema::{DataType, Field};
use rtdb_columnar::{EXTENSION_NAME_KEY, MemoryBudget, MemoryReservation};
use rtdb_plan::{ColumnId, Expr, NamedExpr, RelationNode, SortExpr};
use rtdb_types::{LogicalType, ScalarType, Value};

use crate::{
    columnar::{Column, ColumnBatch},
    runtime_schema::EmittedRelation,
};

pub(crate) struct InputOrder {
    keys: Vec<(usize, PhysicalKey)>,
    _reservation: Arc<MemoryReservation>,
}

impl InputOrder {
    pub(crate) fn bind(
        input: &Arc<EmittedRelation>,
        partition_by: &[ColumnId],
        order_by: &[SortExpr],
        memory: &Arc<MemoryBudget>,
    ) -> Option<Self> {
        let width = partition_by.len().checked_add(order_by.len())?;
        if width == 0 {
            return None;
        }
        let bytes = width
            .checked_mul(size_of::<(usize, PhysicalKey)>() + size_of::<ColumnId>())?
            .checked_add(256)?;
        let reservation = memory.try_reserve(bytes)?;
        let mut columns = Vec::with_capacity(width);
        columns.extend_from_slice(partition_by);
        for order in order_by {
            columns.push(column(&order.expr)?);
        }
        let mut keys = Vec::with_capacity(width);
        for (index, &id) in columns.iter().enumerate() {
            let position = input.schema().position(id)?;
            let layout = PhysicalKey::bind(&input.schema().fields()[position].data_type)?;
            if index < partition_by.len() && matches!(layout, PhysicalKey::F32 | PhysicalKey::F64) {
                return None;
            }
            keys.push((position, layout));
        }

        let mut source = Arc::clone(input);
        loop {
            let node = source.relation().node();
            let (projections, extends) = match node {
                RelationNode::Project { expressions, .. }
                | RelationNode::Rename { expressions, .. } => (expressions, false),
                RelationNode::Extend { expressions, .. } => (expressions, true),
                RelationNode::Sort {
                    order_by: provided, ..
                }
                | RelationNode::SortLimit {
                    order_by: provided,
                    offset: None,
                    fetch: None,
                    ..
                } => {
                    let prefix = provided.get(..width)?;
                    for (provided, id) in prefix.iter().zip(columns) {
                        if column(&provided.expr)? != id {
                            return None;
                        }
                    }
                    // Partition order is first-seen order, already established by the input sort.
                    if !prefix[partition_by.len()..].iter().zip(order_by).all(
                        |(provided, required)| {
                            provided.descending == required.descending
                                && provided.nulls_first == required.nulls_first
                        },
                    ) {
                        return None;
                    }
                    return Some(Self {
                        keys,
                        _reservation: reservation,
                    });
                }
                _ => return None,
            };
            let child = source.child(0).ok()?;
            for id in &mut columns {
                if !extends || child.schema().get(*id).is_none() {
                    *id = projection_source(projections, *id)?;
                }
            }
            source = child;
        }
    }

    pub(super) fn admits(&self, batch: &ColumnBatch) -> bool {
        self.keys
            .iter()
            .all(|&(position, key)| match &batch.columns[position] {
                Column::Arrow {
                    array,
                    field,
                    logical,
                } => {
                    array.data_type() == field.data_type()
                        && key.admits_field(field, logical.as_ref())
                }
                Column::Selected(selected) => {
                    selected.source_layouts().all(|(array, field, logical)| {
                        array.data_type() == field.data_type() && key.admits_field(field, logical)
                    })
                }
                Column::Values(values) => values.iter().all(|value| key.admits_value(value)),
            })
    }
}

fn column(expr: &Expr) -> Option<ColumnId> {
    match expr {
        Expr::Column { id, .. } => Some(*id),
        _ => None,
    }
}

fn projection_source(projections: &[NamedExpr], id: ColumnId) -> Option<ColumnId> {
    column(
        &projections
            .iter()
            .find(|projection| projection.field.id == id)?
            .expr,
    )
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PhysicalKey {
    Null,
    I8,
    I16,
    I32,
    I64,
    F32,
    F64,
    Bool,
    Utf8,
}

impl PhysicalKey {
    fn bind(logical: &LogicalType) -> Option<Self> {
        Some(match logical {
            LogicalType::Maybe(value) => return Self::bind(value),
            LogicalType::Null => Self::Null,
            LogicalType::Scalar(ScalarType::I8) => Self::I8,
            LogicalType::Scalar(ScalarType::I16) => Self::I16,
            LogicalType::Scalar(ScalarType::I32) => Self::I32,
            LogicalType::Scalar(ScalarType::I64) => Self::I64,
            LogicalType::Scalar(ScalarType::F32) => Self::F32,
            LogicalType::Scalar(ScalarType::F64) => Self::F64,
            LogicalType::Scalar(ScalarType::Bool) => Self::Bool,
            LogicalType::Scalar(ScalarType::Utf8) => Self::Utf8,
            _ => return None,
        })
    }

    fn admits_field(self, field: &Field, logical: Option<&LogicalType>) -> bool {
        if field.metadata().contains_key(EXTENSION_NAME_KEY) {
            return false;
        }
        if field.data_type() == &DataType::Null {
            return field.is_nullable() && logical != Some(&LogicalType::Nothing);
        }
        if logical.is_some_and(|logical| Self::bind(logical) != Some(self)) {
            return false;
        }
        matches!(
            (self, field.data_type()),
            (Self::I8, DataType::Int8)
                | (Self::I16, DataType::Int16)
                | (Self::I32, DataType::Int32)
                | (Self::I64, DataType::Int64)
                | (Self::F32, DataType::Float32)
                | (Self::F64, DataType::Float64)
                | (Self::Bool, DataType::Boolean)
                | (Self::Utf8, DataType::Utf8)
        )
    }

    fn admits_value(self, value: &Value) -> bool {
        matches!(
            (self, value),
            (_, Value::Null)
                | (Self::I8, Value::I8(_))
                | (Self::I16, Value::I16(_))
                | (Self::I32, Value::I32(_))
                | (Self::I64, Value::I64(_))
                | (Self::F32, Value::F32(_))
                | (Self::F64, Value::F64(_))
                | (Self::Bool, Value::Bool(_))
                | (Self::Utf8, Value::Utf8(_))
        )
    }
}
