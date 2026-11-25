use std::{borrow::Cow, sync::Arc};

use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, ListArray,
    builder::{ArrayBuilder, NullBuilder, PrimitiveBuilder, StringBuilder},
    cast::AsArray,
    types::{ArrowPrimitiveType, Float32Type, Float64Type, Int32Type, Int64Type},
};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_schema::{DataType, Field};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_types::Result;

use super::{
    column::{fill_integer_nulls, float_array_text_layout, vector_components, vector_width},
    float, float32, invalid, visit_array,
};

pub enum WalArrayRow<'a> {
    List(WalListRow<'a>),
    Text(&'a str),
}

pub struct WalListRow<'a> {
    values: ListValues<'a>,
    slots: usize,
    present: bool,
    text_bytes: usize,
    _scratch: Arc<MemoryReservation>,
}

enum ListValues<'a> {
    Null(usize),
    Int32(Vec<Option<i32>>),
    Int64(Vec<Option<i64>>),
    Float32(Vec<Option<f32>>),
    Float64(Vec<Option<f64>>),
    Utf8(Vec<Option<Cow<'a, str>>>),
    Vector(Vec<f32>),
}

impl ListValues<'_> {
    fn layout(&self) -> (DataType, usize, bool) {
        fn describe<T>(ty: DataType, values: &[Option<T>]) -> (DataType, usize, bool) {
            (ty, values.len(), values.iter().any(Option::is_none))
        }
        match self {
            Self::Null(count) => (DataType::Null, *count, *count != 0),
            Self::Int32(values) => describe(DataType::Int32, values),
            Self::Int64(values) => describe(DataType::Int64, values),
            Self::Float32(values) => describe(DataType::Float32, values),
            Self::Float64(values) => describe(DataType::Float64, values),
            Self::Utf8(values) => describe(DataType::Utf8, values),
            Self::Vector(values) => (DataType::Float32, values.len(), false),
        }
    }
}

impl WalListRow<'_> {
    pub fn builder_bytes(&self) -> Result<usize> {
        let width = match &self.values {
            ListValues::Null(_) => return Ok(0),
            ListValues::Int32(_)
            | ListValues::Float32(_)
            | ListValues::Utf8(_)
            | ListValues::Vector(_) => 4,
            ListValues::Int64(_) | ListValues::Float64(_) => 8,
        };
        self.slots
            .checked_mul(width)
            .and_then(|size| size.checked_add(self.slots.div_ceil(8)))
            .and_then(|size| size.checked_add(self.text_bytes))
            .ok_or_else(overflow)
    }
}

/// Parses one WAL array or vector while its input and component scratch are available.
pub fn read_wal_array<'a>(
    name: &str,
    oid: u32,
    vector_typmod: Option<i32>,
    text: Option<&'a str>,
    memory: &Arc<MemoryBudget>,
) -> Result<Option<(Field, WalArrayRow<'a>)>> {
    let dimension = vector_typmod.filter(|&typmod| typmod != -1);
    let (values, scratch) = if vector_typmod.is_some() {
        let width = dimension.map(vector_width).transpose()?;
        let components = text
            .map(|text| vector_components(text, width))
            .transpose()?;
        let count = text.map_or(0, |text| width.unwrap_or_else(|| component_bound(text)));
        let scratch = memory.reserve(
            count
                .checked_mul(std::mem::size_of::<f32>())
                .ok_or_else(overflow)?,
        )?;
        let mut values = Vec::with_capacity(count);
        if let Some(components) = components {
            for component in components {
                values.push(component?);
            }
        }
        (ListValues::Vector(values), scratch)
    } else {
        match oid {
            1007 if text.is_some() => stage_array(
                text,
                memory,
                |item| item.parse().map_err(invalid),
                ListValues::Int32,
                false,
            )?,
            1016 if text.is_some() => stage_array(
                text,
                memory,
                |item| item.parse().map_err(invalid),
                ListValues::Int64,
                false,
            )?,
            1009 if text.is_some() => stage_array(text, memory, Ok, ListValues::Utf8, false)?,
            1021 | 1022 => {
                if let Some(text) = text.filter(|text| float_array_text_layout(text)) {
                    i32::try_from(text.len()).map_err(invalid)?;
                    return Ok(Some((
                        Field::new(name, DataType::Utf8, false),
                        WalArrayRow::Text(text),
                    )));
                }
                if oid == 1021 {
                    stage_array(
                        text,
                        memory,
                        |item| float32(&item),
                        ListValues::Float32,
                        true,
                    )?
                } else {
                    stage_array(text, memory, |item| float(&item), ListValues::Float64, true)?
                }
            }
            _ => return Ok(None),
        }
    };
    let (ty, slots, nullable) = values.layout();
    let text_bytes = match &values {
        ListValues::Utf8(values) => values.iter().flatten().try_fold(0usize, |size, item| {
            size.checked_add(item.len()).ok_or_else(overflow)
        })?,
        _ => 0,
    };
    i32::try_from(text_bytes).map_err(invalid)?;
    let item = Arc::new(Field::new("$data$", ty, nullable));
    let ty = if let Some(width) = dimension {
        DataType::FixedSizeList(item, width)
    } else {
        i32::try_from(slots).map_err(invalid)?;
        DataType::List(item)
    };
    let present = text.is_some();
    let slots = dimension.map_or(slots, |width| width as usize);
    let row = WalListRow {
        values,
        slots,
        present,
        text_bytes,
        _scratch: scratch,
    };
    Ok(Some((
        Field::new(name, ty, !present),
        WalArrayRow::List(row),
    )))
}

fn stage_array<'a, T>(
    text: Option<&'a str>,
    memory: &Arc<MemoryBudget>,
    mut parse: impl FnMut(Cow<'a, str>) -> Result<T>,
    wrap: impl FnOnce(Vec<Option<T>>) -> ListValues<'a>,
    keep_child_type: bool,
) -> Result<(ListValues<'a>, Arc<MemoryReservation>)> {
    let bound = if let Some(text) = text {
        // Quoted commas only widen this bound. Escaped components can also own decoded text.
        component_bound(text)
            .max(4)
            .checked_mul(3 * std::mem::size_of::<Option<T>>())
            .and_then(|size| size.checked_add(text.len().checked_mul(8)?))
            .and_then(|size| size.checked_add(128))
            .ok_or_else(overflow)?
    } else {
        0
    };
    let scratch = memory.reserve(bound)?;
    let mut values = Vec::new();
    if let Some(text) = text {
        visit_array(text, |item| {
            values.push(item.map(&mut parse).transpose()?);
            Ok(())
        })?;
    }
    let values = if !keep_child_type && values.iter().all(Option::is_none) {
        ListValues::Null(values.len())
    } else {
        wrap(values)
    };
    Ok((values, scratch))
}

fn component_bound(text: &str) -> usize {
    text.bytes().filter(|&byte| byte == b',').count() + 1
}

/// Accumulates one field layout after the caller admits the pending column's buffers.
pub struct WalListColumn {
    layout: ListLayout,
    child: ChildBuilder,
    valid: NullBufferBuilder,
}

enum ListLayout {
    Variable(Vec<i32>),
    Fixed(i32),
}

enum ChildBuilder {
    Null(NullBuilder),
    Int32(PrimitiveBuilder<Int32Type>),
    Int64(PrimitiveBuilder<Int64Type>),
    Float32(PrimitiveBuilder<Float32Type>),
    Float64(PrimitiveBuilder<Float64Type>),
    Utf8(StringBuilder),
}

impl WalListColumn {
    pub fn new(field: &Field) -> Option<Self> {
        if !field.metadata().is_empty() {
            return None;
        }
        let (item, layout) = match field.data_type() {
            DataType::List(item) => (item, ListLayout::Variable(vec![0])),
            DataType::FixedSizeList(item, width)
                if *width > 0 && item.data_type() == &DataType::Float32 =>
            {
                (item, ListLayout::Fixed(*width))
            }
            _ => return None,
        };
        if !item.metadata().is_empty() {
            return None;
        }
        let child = match item.data_type() {
            DataType::Null => ChildBuilder::Null(NullBuilder::new()),
            DataType::Int32 => ChildBuilder::Int32(PrimitiveBuilder::with_capacity(0)),
            DataType::Int64 => ChildBuilder::Int64(PrimitiveBuilder::with_capacity(0)),
            DataType::Float32 => ChildBuilder::Float32(PrimitiveBuilder::with_capacity(0)),
            DataType::Float64 => ChildBuilder::Float64(PrimitiveBuilder::with_capacity(0)),
            DataType::Utf8 => ChildBuilder::Utf8(StringBuilder::with_capacity(0, 0)),
            _ => return None,
        };
        Some(Self {
            layout,
            child,
            valid: NullBufferBuilder::new(0),
        })
    }

    pub fn check_row(&self, row: &WalListRow<'_>) -> Result<()> {
        self.check_append(row.slots, row.text_bytes)
    }

    pub fn check_array(&self, array: &dyn Array) -> Result<()> {
        let (child, _) = self.array_child(array);
        let text_bytes = if matches!(&self.child, ChildBuilder::Utf8(_)) {
            let offsets = child.as_string::<i32>().value_offsets();
            (offsets[offsets.len() - 1] - offsets[0]) as usize
        } else {
            0
        };
        self.check_append(child.len(), text_bytes)
    }

    fn check_append(&self, slots: usize, text_bytes: usize) -> Result<()> {
        let end = self
            .child
            .builder()
            .len()
            .checked_add(slots)
            .ok_or_else(overflow)?;
        if matches!(&self.layout, ListLayout::Variable(_)) {
            i32::try_from(end).map_err(invalid)?;
        }
        if let ChildBuilder::Utf8(builder) = &self.child {
            let end = builder
                .values_slice()
                .len()
                .checked_add(text_bytes)
                .ok_or_else(overflow)?;
            i32::try_from(end).map_err(invalid)?;
        }
        Ok(())
    }

    pub fn push(&mut self, row: WalListRow<'_>) {
        match (&mut self.child, row.values) {
            (ChildBuilder::Null(builder), ListValues::Null(slots)) => builder.append_nulls(slots),
            (ChildBuilder::Int32(builder), ListValues::Int32(values)) => {
                append_integers(builder, values)
            }
            (ChildBuilder::Int64(builder), ListValues::Int64(values)) => {
                append_integers(builder, values)
            }
            (ChildBuilder::Float32(builder), ListValues::Float32(values)) => {
                for value in values {
                    builder.append_option(value);
                }
            }
            (ChildBuilder::Float32(builder), ListValues::Vector(values)) => {
                if let (false, ListLayout::Fixed(width)) = (row.present, &self.layout) {
                    builder.append_nulls(*width as usize);
                } else {
                    builder.append_slice(&values);
                }
            }
            (ChildBuilder::Float64(builder), ListValues::Float64(values)) => {
                for value in values {
                    builder.append_option(value);
                }
            }
            (ChildBuilder::Utf8(builder), ListValues::Utf8(values)) => {
                for value in values {
                    builder.append_option(value);
                }
            }
            _ => unreachable!("parsed row matches its pending field"),
        }
        self.finish_row(row.present);
    }

    pub fn push_array(&mut self, array: &dyn Array) {
        let (child, present) = self.array_child(array);
        match &mut self.child {
            ChildBuilder::Null(builder) => builder.append_nulls(child.len()),
            ChildBuilder::Int32(builder) => builder.append_array(child.as_primitive::<Int32Type>()),
            ChildBuilder::Int64(builder) => builder.append_array(child.as_primitive::<Int64Type>()),
            ChildBuilder::Float32(builder) => {
                builder.append_array(child.as_primitive::<Float32Type>())
            }
            ChildBuilder::Float64(builder) => {
                builder.append_array(child.as_primitive::<Float64Type>())
            }
            ChildBuilder::Utf8(builder) => builder
                .append_array(child.as_string::<i32>())
                .expect("checked UTF-8 offsets"),
        }
        self.finish_row(present);
    }

    fn array_child<'a>(&self, array: &'a dyn Array) -> (&'a dyn Array, bool) {
        // Generic rows are constructed as one-row arrays covering their entire child.
        let child = match &self.layout {
            ListLayout::Variable(_) => array.as_list::<i32>().values(),
            ListLayout::Fixed(_) => array.as_fixed_size_list().values(),
        };
        (child.as_ref(), array.is_valid(0))
    }

    fn finish_row(&mut self, present: bool) {
        if let ListLayout::Variable(offsets) = &mut self.layout {
            offsets.push(i32::try_from(self.child.builder().len()).expect("checked list offsets"));
        }
        self.valid.append(present);
    }

    pub fn finish(mut self, field: &Field) -> Result<ArrayRef> {
        let child = self.child.finish();
        let valid = self.valid.finish();
        match (self.layout, field.data_type()) {
            (ListLayout::Variable(offsets), DataType::List(item)) => {
                let array = ListArray::try_new(
                    Arc::clone(item),
                    OffsetBuffer::new(offsets.into()),
                    child,
                    valid,
                )
                .map_err(invalid)?;
                Ok(Arc::new(array))
            }
            (ListLayout::Fixed(width), DataType::FixedSizeList(item, _)) => {
                let array = FixedSizeListArray::try_new(Arc::clone(item), width, child, valid)
                    .map_err(invalid)?;
                Ok(Arc::new(array))
            }
            _ => unreachable!("pending list retains its field layout"),
        }
    }
}

impl ChildBuilder {
    fn builder(&self) -> &dyn ArrayBuilder {
        match self {
            Self::Null(builder) => builder,
            Self::Int32(builder) => builder,
            Self::Int64(builder) => builder,
            Self::Float32(builder) => builder,
            Self::Float64(builder) => builder,
            Self::Utf8(builder) => builder,
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Null(mut builder) => Arc::new(builder.finish()),
            Self::Int32(mut builder) => Arc::new(builder.finish()),
            Self::Int64(mut builder) => Arc::new(builder.finish()),
            Self::Float32(mut builder) => Arc::new(builder.finish()),
            Self::Float64(mut builder) => Arc::new(builder.finish()),
            Self::Utf8(mut builder) => Arc::new(builder.finish()),
        }
    }
}

fn append_integers<T: ArrowPrimitiveType>(
    builder: &mut PrimitiveBuilder<T>,
    values: Vec<Option<T::Native>>,
) {
    let start = builder.len();
    let first = values.iter().flatten().next().copied();
    let mut has_null = false;
    for value in values {
        has_null |= value.is_none();
        builder.append_option(value);
    }
    fill_integer_nulls(builder, start, first.filter(|_| has_null));
}

fn overflow() -> rtdb_types::Anomaly {
    invalid("PostgreSQL WAL array allocation overflow")
}
