use rtdb_types::StructFields;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int32Array, ListArray, RecordBatch, StructArray, UnionArray};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit as ArrowTimeUnit, UnionFields, UnionMode};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_types::{Anomaly, Interval, LogicalType, Result, TimeUnit, Timestamp, Value};

use crate::ipc::ArrowFile;

pub use crate::statistics::{column_metadata, columns_for_event_page};

#[derive(Clone, Debug, PartialEq)]
pub enum MetadataNode {
    Nil,
    IidBranch(Vec<Option<usize>>),
    RecencyBranch(Vec<(Timestamp, usize)>),
    Leaf { data_page_index: usize, columns: LeafColumns },
}

/// A leaf's per-column statistics; a leaf read from a file decodes them each time they are asked for, since a scan
/// visits few of a file's leaves and the statistics, Bloom bitmaps included, dominate what a whole file decodes to.
#[derive(Clone)]
pub struct LeafColumns(Arc<LeafSource>);
enum LeafSource { Decoded(Vec<Value>), Stored { field: Arc<Field>, columns: ArrayRef, offset: usize } }
impl LeafColumns {
    pub fn new(columns: Vec<Value>) -> Self { Self(Arc::new(LeafSource::Decoded(columns))) }
    pub fn values(&self) -> Result<std::borrow::Cow<'_, [Value]>> {
        match self.0.as_ref() {
            LeafSource::Decoded(columns) => Ok(std::borrow::Cow::Borrowed(columns)),
            LeafSource::Stored { field, columns, offset } => match rtdb_columnar::read_value(field, columns.as_ref(), *offset)? {
                Value::List(values) => Ok(std::borrow::Cow::Owned(values.into_values())),
                _ => Err(invalid("Metadata leaf has no columns")),
            },
        }
    }
}
/// A leaf's column statistics borrowed by the scan.
pub struct LeafStatistics<'a>(StatisticsSource<'a>);
enum StatisticsSource<'a> {
    Values(std::borrow::Cow<'a, [Value]>),
    Arrow { records: &'a StructArray, rows: std::ops::Range<usize> },
}

#[derive(Clone, Copy)]
enum StatisticCell<'a> { Value(&'a Value), Arrow(&'a dyn Array, usize) }

#[derive(Clone, Copy)]
pub struct ColumnStatistics<'a>(StatisticCell<'a>);

impl LeafColumns {
    pub fn statistics(&self) -> Result<LeafStatistics<'_>> {
        fn plain(field: &Field) -> bool {
            field.metadata().is_empty() && match field.data_type() {
                DataType::Null | DataType::Boolean | DataType::Int32 | DataType::Int64 | DataType::Float64 | DataType::Utf8 | DataType::Binary => true,
                DataType::List(child) => plain(child),
                DataType::Struct(fields) => fields.iter().all(|child| plain(child)),
                _ => false,
            }
        }
        if let LeafSource::Stored { field, columns, offset } = self.0.as_ref()
            && plain(field)
            && let Some(list) = columns.as_any().downcast_ref::<ListArray>()
            && list.is_valid(*offset)
            && let Some(records) = list.values().as_any().downcast_ref::<StructArray>() {
            let offsets = list.value_offsets();
            let rows = offsets[*offset] as usize..offsets[*offset + 1] as usize;
            return Ok(LeafStatistics(StatisticsSource::Arrow { records, rows }));
        }
        self.values().map(|values| LeafStatistics(StatisticsSource::Values(values)))
    }
}

impl LeafStatistics<'_> {
    fn columns(&self) -> impl Iterator<Item = ColumnStatistics<'_>> {
        let len = match &self.0 { StatisticsSource::Arrow { rows, .. } => rows.len(), StatisticsSource::Values(values) => values.len() };
        (0..len).map(|position| ColumnStatistics(match &self.0 {
            StatisticsSource::Arrow { records, rows } => StatisticCell::Arrow(*records, rows.start + position),
            StatisticsSource::Values(values) => StatisticCell::Value(&values[position]),
        }))
    }

    pub fn named<'a>(&'a self, name: &'a str) -> impl Iterator<Item = ColumnStatistics<'a>> {
        self.columns().filter(move |column| column.name() == Some(name))
    }

    pub fn root_column(&self, name: &str) -> Option<ColumnStatistics<'_>> {
        self.columns().find(|column| column.name() == Some(name) && column.flag("root-col?") == Some(true))
    }
}

impl<'a> StatisticCell<'a> {
    fn field(self, name: &str) -> Option<Self> {
        match self {
            Self::Value(value) => value.field(name).map(Self::Value),
            Self::Arrow(array, row) if array.is_valid(row) => {
                let records = array.as_any().downcast_ref::<StructArray>()?;
                Some(Self::Arrow(records.column_by_name(name)?.as_ref(), row))
            }
            _ => None,
        }
    }
}

impl<'a> ColumnStatistics<'a> {
    fn name(self) -> Option<&'a str> {
        match self.0.field("col-name")? {
            StatisticCell::Value(Value::Utf8(name)) => Some(name),
            StatisticCell::Arrow(array, row) if array.is_valid(row) =>
                array.as_any().downcast_ref::<arrow_array::StringArray>().map(|values| values.value(row)),
            _ => None,
        }
    }

    pub fn flag(self, field: &str) -> Option<bool> {
        match self.0.field(field)? {
            StatisticCell::Value(Value::Bool(value)) => Some(*value),
            StatisticCell::Arrow(array, row) if array.is_valid(row) =>
                array.as_any().downcast_ref::<arrow_array::BooleanArray>().map(|values| values.value(row)),
            _ => None,
        }
    }

    pub fn count(self) -> Option<i64> {
        match self.0.field("count")? {
            StatisticCell::Value(Value::I64(value)) => Some(*value),
            StatisticCell::Arrow(array, row) if array.is_valid(row) =>
                array.as_any().downcast_ref::<arrow_array::Int64Array>().map(|values| values.value(row)),
            _ => None,
        }
    }

    pub fn bound(self, flavour: &str, key: &str) -> Option<f64> {
        match self.0.field(flavour)?.field(key)? {
            StatisticCell::Value(Value::F64(value)) => Some(*value),
            StatisticCell::Arrow(array, row) if array.is_valid(row) =>
                array.as_any().downcast_ref::<arrow_array::Float64Array>().map(|values| values.value(row)),
            _ => None,
        }
    }

    pub fn bloom(self) -> Option<&'a [u8]> {
        match self.0.field("bytes")?.field("bloom")? {
            StatisticCell::Value(Value::Binary(value)) => Some(value),
            StatisticCell::Arrow(array, row) if array.is_valid(row) =>
                array.as_any().downcast_ref::<arrow_array::BinaryArray>().map(|values| values.value(row)),
            _ => None,
        }
    }
}

impl PartialEq for LeafColumns {
    fn eq(&self, other: &Self) -> bool { matches!((self.values(), other.values()), (Ok(left), Ok(right)) if *left == *right) }
}
impl std::fmt::Debug for LeafColumns {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.values() { Ok(columns) => columns.fmt(formatter), Err(error) => write!(formatter, "<undecodable: {error:?}>") }
    }
}

#[derive(Clone, Debug)]
pub struct MetadataFile {
    nodes: Vec<MetadataNode>,
    schema: SchemaRef,
    stored_bytes: usize,
}

pub struct MetadataBuilder {
    nodes: Vec<MetadataNode>,
    column_count: usize,
    page_count: usize,
    row_count: i64,
    temporal: crate::log::proto::TemporalMetadata,
    recency_branches: bool,
    calculate_blooms: bool,
}

fn page_metadata_reservation(page: &RecordBatch) -> Result<usize> {
    let mut bytes = page.get_array_memory_size().checked_mul(4).and_then(|size| size.checked_add(65_536)).ok_or_else(|| invalid("Page metadata reservation overflow"))?;
    let mut arrays: Vec<_> = page.columns().iter().map(|array| array.to_data()).collect();
    // Each array node yields one statistics entry and at most one Bloom bitmap; cells are read in place, not cloned per row.
    while let Some(array) = arrays.pop() {
        bytes = bytes.checked_add(array.len().checked_mul(32).ok_or_else(|| invalid("Page metadata reservation overflow"))?)
            .and_then(|size| size.checked_add(4096)).ok_or_else(|| invalid("Page metadata reservation overflow"))?;
        arrays.extend(array.child_data().iter().cloned());
    }
    Ok(bytes)
}

fn retained_node_bytes(node: &MetadataNode) -> Result<usize> {
    let base = 512;
    match node {
        MetadataNode::Nil => Ok(base),
        MetadataNode::IidBranch(children) => children.capacity().checked_mul(std::mem::size_of::<Option<usize>>()).and_then(|bytes| bytes.checked_add(base)).ok_or_else(metadata_size),
        MetadataNode::RecencyBranch(children) => children.capacity().checked_mul(std::mem::size_of::<(Timestamp, usize)>()).and_then(|bytes| bytes.checked_add(base)).ok_or_else(metadata_size),
        MetadataNode::Leaf { columns, .. } => {
            let LeafSource::Decoded(columns) = columns.0.as_ref() else { return Ok(base) };
            let unused = columns.capacity() - columns.len();
            let bytes = unused.checked_mul(std::mem::size_of::<Value>()).and_then(|bytes| bytes.checked_add(base)).ok_or_else(metadata_size)?;
            columns.iter().try_fold(bytes, |bytes, column| bytes.checked_add(column.retained_bytes()?).ok_or_else(metadata_size))
        }
    }
}

fn metadata_size() -> Anomaly { invalid("Metadata allocation exceeds addressable memory") }

impl MetadataBuilder {
    pub fn new(recency_branches: bool, calculate_blooms: bool) -> Self {
        Self { nodes: Vec::new(), column_count: 0, page_count: 0, row_count: 0,
            temporal: crate::log::proto::TemporalMetadata::default(), recency_branches, calculate_blooms }
    }

    pub fn write_nil(&mut self) -> Result<usize> { self.push(MetadataNode::Nil) }

    pub fn write_iid_branch(&mut self, children: [Option<usize>; 4]) -> Result<usize> {
        for child in children.iter().flatten() { validate_child(*child, self.nodes.len())?; }
        self.push(MetadataNode::IidBranch(children.to_vec()))
    }

    pub fn write_recency_branch(&mut self, children: Vec<(Timestamp, usize)>) -> Result<usize> {
        if !self.recency_branches { return Err(invalid("Recency branches require recency data")); }
        for (index, (upper, child)) in children.iter().enumerate() {
            validate_child(*child, self.nodes.len())?;
            if index > 0 && children[index - 1].0 > *upper { return Err(invalid("Recency ceilings must be nondecreasing")); }
        }
        self.push(MetadataNode::RecencyBranch(children))
    }

    pub fn write_leaf(&mut self, batch: &RecordBatch) -> Result<usize> {
        if batch.schema().index_of("_recency").is_ok() != self.recency_branches { return Err(invalid("Data recency column does not match metadata schema")); }
        crate::data::EventBatch::new(batch.clone())?;
        let times = |name: &str| -> Result<&[i64]> {
            let index = batch.schema().index_of(name).map_err(invalid)?;
            batch.column(index).as_any().downcast_ref::<arrow_array::TimestampMicrosecondArray>().map(|array| array.values().as_ref()).ok_or_else(|| invalid("Event timestamp array type changed"))
        };
        let (valid_from, valid_to, system_from) = (times("_valid_from")?, times("_valid_to")?, times("_system_from")?);
        let mut temporal = self.temporal.clone();
        for ((lower, upper), system) in valid_from.iter().zip(valid_to).zip(system_from) {
            Interval::new(Timestamp(*lower), Timestamp(*upper))?;
            temporal.min_valid_from = Some(temporal.min_valid_from.unwrap_or(i64::MAX).min(*lower));
            temporal.max_valid_from = Some(temporal.max_valid_from.unwrap_or(i64::MIN).max(*lower));
            temporal.min_valid_to = Some(temporal.min_valid_to.unwrap_or(i64::MAX).min(*upper));
            temporal.max_valid_to = Some(temporal.max_valid_to.unwrap_or(i64::MIN).max(*upper));
            temporal.min_system_from = Some(temporal.min_system_from.unwrap_or(i64::MAX).min(*system));
            temporal.max_system_from = Some(temporal.max_system_from.unwrap_or(i64::MIN).max(*system));
        }
        let mut columns = columns_for_event_page(batch, self.calculate_blooms)?;
        let base = i32::try_from(self.column_count).map_err(invalid)?;
        let column_count = self.column_count.checked_add(columns.len()).ok_or_else(|| invalid("Metadata column count overflow"))?;
        i32::try_from(column_count).map_err(invalid)?;
        for column in &mut columns {
            let Value::Struct(fields) = column else { return Err(invalid("Column metadata must be a struct")); };
            for (name, value) in fields {
                match (name.as_str(), value) {
                    ("list" | "set", Value::I32(index)) => *index = index.checked_add(base).ok_or_else(|| invalid("Metadata child index overflow"))?,
                    ("struct", Value::List(children)) if !children.is_empty() => {
                        let indices = children.array().and_then(|array| array.as_any().downcast_ref::<Int32Array>())
                            .filter(|indices| indices.null_count() == 0)
                            .ok_or_else(|| invalid("Struct metadata children must be integers"))?;
                        let mut shifted = arrow_array::builder::Int32Builder::with_capacity(indices.len());
                        for &index in indices.values().iter() {
                            shifted.append_value(index.checked_add(base).ok_or_else(|| invalid("Metadata child index overflow"))?);
                        }
                        let array = shifted.finish();
                        let field = Arc::new(Field::new("item", DataType::Int32, false));
                        *children = rtdb_types::ListValue::from_numeric_arrow(field, Arc::new(array))
                            .expect("metadata uses i32 indices");
                    }
                    _ => {}
                }
            }
        }
        let row_count = self.row_count.checked_add(i64::try_from(batch.num_rows()).map_err(invalid)?).ok_or_else(|| invalid("Metadata row count overflow"))?;
        i32::try_from(self.page_count).map_err(invalid)?;
        let index = self.push(MetadataNode::Leaf { data_page_index: self.page_count, columns: LeafColumns::new(columns) })?;
        self.column_count = column_count;
        self.page_count += 1;
        self.row_count = row_count;
        self.temporal = temporal;
        Ok(index)
    }

    /// The returned reservation covers the retained leaf through serialization.
    pub fn write_leaf_reserved(&mut self, batch: &RecordBatch, memory: &Arc<MemoryBudget>) -> Result<(usize, Arc<MemoryReservation>)> {
        let mut reservation = memory.reserve(page_metadata_reservation(batch)?)?;
        let index = self.write_leaf(batch)?;
        let retained = retained_node_bytes(&self.nodes[index])?;
        let reserved = Arc::get_mut(&mut reservation).expect("metadata construction owns its reservation");
        if retained > reserved.bytes() { reserved.grow_to(retained)?; } else { reserved.shrink_to(retained)?; }
        Ok((index, reservation))
    }

    /// Admits schema construction and serialization for the accumulated metadata.
    pub fn serialization_bound(&self) -> Result<usize> {
        self.nodes.iter().try_fold(65_536usize, |bytes, node| {
            retained_node_bytes(node)?.checked_mul(8).and_then(|size| bytes.checked_add(size)).ok_or_else(metadata_size)
        })
    }

    pub fn finish(self) -> Result<(MetadataFile, crate::log::proto::TrieMetadata)> {
        let metadata = crate::log::proto::TrieMetadata {
            temporal_metadata: (self.row_count > 0).then_some(self.temporal), row_count: Some(self.row_count), iid_bloom: None,
        };
        Ok((MetadataFile::from_nodes(self.nodes, self.recency_branches)?, metadata))
    }

    fn push(&mut self, node: MetadataNode) -> Result<usize> {
        let index = self.nodes.len();
        i32::try_from(index).map_err(invalid)?;
        self.nodes.push(node);
        Ok(index)
    }
}

impl MetadataFile {
    pub fn read(bytes: Arc<[u8]>) -> Result<Self> {
        Self::read_reader(std::io::Cursor::new(bytes))
    }

    pub fn read_reader(reader: impl std::io::Read + std::io::Seek + Send + 'static) -> Result<Self> {
        let mut file = ArrowFile::from_reader(reader, None)?;
        if file.page_count() != 1 { return Err(invalid("Trie metadata must contain exactly one Arrow record batch")); }
        let batch = file.read_page(0)?;
        let index = batch.schema().index_of("nodes").map_err(invalid)?;
        let nodes = batch.column(index).as_any().downcast_ref::<UnionArray>().ok_or_else(|| invalid("Trie metadata nodes must be a union"))?;
        let DataType::Union(fields, _) = nodes.data_type() else { return Err(invalid("Missing metadata union schema")); };
        let mut decoded = Vec::with_capacity(nodes.len());
        for row in 0..nodes.len() {
            let id = nodes.type_id(row);
            let field = fields.iter().find_map(|(type_id, field)| (id == type_id).then_some(field)).ok_or_else(|| invalid("Metadata node type has no declared leg"))?;
            if field.name() == "leaf" {
                decoded.push(stored_leaf(field, nodes.child(id), nodes.value_offset(row))?);
                continue;
            }
            let value = rtdb_columnar::read_value(field, nodes.child(id).as_ref(), nodes.value_offset(row))?;
            decoded.push(match field.name().as_str() {
                "nil" => MetadataNode::Nil,
                "branch-iid" => {
                    let Value::List(children) = value else { return Err(invalid("IID branch is not a list")); };
                    if children.len() != 4 { return Err(invalid("IID branch must have four buckets")); }
                    MetadataNode::IidBranch(children.iter().map(|child| match child.as_ref() {
                        Value::Null => Ok(None),
                        Value::I32(child) => Ok(Some(child_index(*child, row)?)),
                        _ => Err(invalid("IID child reference is not an integer")),
                    }).collect::<Result<_>>()?)
                }
                "branch-recency" => {
                    let Value::List(children) = value else { return Err(invalid("Recency branch is not a list")); };
                    let mut buckets = Vec::with_capacity(children.len());
                    for child in children.into_values() {
                        let Value::Struct(fields) = child else { return Err(invalid("Recency bucket is not a struct")); };
                        let Value::I32(index) = required(&fields, "idx")? else { return Err(invalid("Recency bucket has no child index")); };
                        let Value::Timestamp { value, unit: TimeUnit::Microsecond, .. } = required(&fields, "max-recency")? else { return Err(invalid("Recency bucket has no microsecond ceiling")); };
                        let upper = Timestamp(*value);
                        if buckets.last().is_some_and(|(previous, _)| *previous > upper) { return Err(invalid("Recency ceilings must be nondecreasing")); }
                        buckets.push((upper, child_index(*index, row)?));
                    }
                    MetadataNode::RecencyBranch(buckets)
                }
                other => return Err(invalid(format!("Unknown metadata node: {other}"))),
            });
        }
        let stored_bytes = rtdb_columnar::shared_allocation_size(&batch)?.checked_add(decoded.capacity().checked_mul(std::mem::size_of::<MetadataNode>()).ok_or_else(metadata_size)?).ok_or_else(metadata_size)?;
        Ok(Self { nodes: decoded, schema: batch.schema(), stored_bytes })
    }

    pub fn from_nodes(nodes: Vec<MetadataNode>, recency_branches: bool) -> Result<Self> {
        let mut column_types = Vec::new();
        for (parent, node) in nodes.iter().enumerate() {
            match node {
                MetadataNode::Nil => {}
                MetadataNode::IidBranch(children) => {
                    if children.len() != 4 { return Err(invalid("IID branch must have four buckets")); }
                    for child in children.iter().flatten() { validate_child(*child, parent)?; }
                }
                MetadataNode::RecencyBranch(children) => {
                    if !recency_branches { return Err(invalid("Recency branches require the recency metadata schema")); }
                    for (index, (upper, child)) in children.iter().enumerate() {
                        validate_child(*child, parent)?;
                        if index > 0 && children[index - 1].0 > *upper { return Err(invalid("Recency ceilings must be nondecreasing")); }
                    }
                }
                MetadataNode::Leaf { data_page_index, columns } => {
                    i32::try_from(*data_page_index).map_err(invalid)?;
                    column_types.extend(columns.values()?.iter().map(Value::logical_type));
                }
            }
        }
        let mut columns = vec![Field::new("col-name", DataType::Utf8, false), Field::new("root-col?", DataType::Boolean, false), Field::new("count", DataType::Int64, false)];
        if let LogicalType::Struct(types) = LogicalType::join_all(&column_types) {
            for (name, ty) in types {
                if !columns.iter().any(|field| field.name() == &name) { columns.push(column_stat_field(&name, &ty)?); }
            }
        }
        let list = |name: &str, element: Field| Field::new(name, DataType::List(Arc::new(element)), false);
        let mut legs = vec![Field::new("nil", DataType::Null, true), list("branch-iid", Field::new("$data$", DataType::Int32, true))];
        if recency_branches {
            legs.push(list("branch-recency", Field::new("$data$", DataType::Struct(vec![
                Field::new("max-recency", DataType::Timestamp(ArrowTimeUnit::Microsecond, Some("UTC".into())), false),
                Field::new("idx", DataType::Int32, false),
            ].into()), false)));
        }
        legs.push(Field::new("leaf", DataType::Struct(vec![
            Field::new("data-page-idx", DataType::Int32, false),
            list("columns", Field::new("$data$", DataType::Struct(columns.into()), false)),
        ].into()), false));
        let fields = UnionFields::new(0..legs.len() as i8, legs);
        Ok(Self { nodes, schema: Arc::new(Schema::new(vec![Field::new("nodes", DataType::Union(fields, UnionMode::Dense), false)])), stored_bytes: 0 })
    }

    pub fn write(&self) -> Result<Vec<u8>> {
        let field = self.schema.field_with_name("nodes").map_err(invalid)?;
        let DataType::Union(fields, UnionMode::Dense) = field.data_type() else { return Err(invalid("Metadata schema must use a dense node union")); };
        let mut leg_values: Vec<Vec<Value>> = vec![Vec::new(); fields.len()];
        let mut ids = Vec::with_capacity(self.nodes.len());
        let mut offsets = Vec::with_capacity(self.nodes.len());
        for node in &self.nodes {
            let (name, value) = node_value(node)?;
            let (position, (id, _)) = fields.iter().enumerate().find(|(_, (_, field))| field.name() == name).ok_or_else(|| invalid(format!("Metadata schema has no {name} leg")))?;
            ids.push(id);
            offsets.push(i32::try_from(leg_values[position].len()).map_err(invalid)?);
            leg_values[position].push(value);
        }
        let children = fields.iter().zip(leg_values).map(|((_, field), values)| build_field_values(field, &values)).collect::<Result<Vec<_>>>()?;
        let nodes: ArrayRef = Arc::new(UnionArray::try_new(fields.clone(), ScalarBuffer::from(ids), Some(ScalarBuffer::from(offsets)), children).map_err(invalid)?);
        let batch = RecordBatch::try_new(self.schema.clone(), vec![nodes]).map_err(invalid)?;
        crate::ipc::write_file(&self.schema, &[batch])
    }

    pub fn nodes(&self) -> &[MetadataNode] { &self.nodes }
    /// What a file read from storage retains: its node arrays, which its leaves decode from, and its node index.
    pub fn stored_bytes(&self) -> usize { self.stored_bytes }
    pub fn schema(&self) -> &SchemaRef { &self.schema }
    pub fn root_index(&self) -> Option<usize> { self.nodes.len().checked_sub(1) }

    pub fn leaves(&self) -> Vec<(Vec<u8>, usize)> {
        let mut leaves = Vec::new();
        let mut pending = self.root_index().map(|root| vec![(root, Vec::new())]).unwrap_or_default();
        while let Some((node, path)) = pending.pop() {
            match &self.nodes[node] {
                MetadataNode::Nil => {}
                MetadataNode::Leaf { data_page_index, .. } => leaves.push((path, *data_page_index)),
                MetadataNode::IidBranch(children) => {
                    for (bucket, child) in children.iter().enumerate().rev() {
                        if let Some(child) = child { let mut path = path.clone(); path.push(bucket as u8); pending.push((*child, path)); }
                    }
                }
                MetadataNode::RecencyBranch(children) => for (_, child) in children.iter().rev() { pending.push((*child, path.clone())); },
            }
        }
        leaves
    }
}

fn validate_child(child: usize, parent: usize) -> Result<()> {
    if child >= parent { return Err(invalid("Metadata children must precede their parent")); }
    i32::try_from(child).map_err(invalid)?;
    Ok(())
}

fn column_stat_field(name: &str, ty: &LogicalType) -> Result<Field> {
    let data_type = match name {
        "numbers" | "date-times" | "times-of-day" | "durations" => {
            // As the reference's vectors do, a bound is nullable only once some page lacked it.
            let bounds = match ty { LogicalType::Maybe(inner) => inner.as_ref(), ty => ty };
            let nullable = |bound: &str| match bounds { LogicalType::Struct(children) => children.get(bound).is_none_or(|ty| *ty != LogicalType::Scalar(rtdb_types::ScalarType::F64)), _ => true };
            DataType::Struct(vec![Field::new("min", DataType::Float64, nullable("min")), Field::new("max", DataType::Float64, nullable("max"))].into())
        }
        "bytes" => DataType::Struct(vec![Field::new("bloom", DataType::Binary, true)].into()),
        "list" | "set" => DataType::Int32,
        "struct" => DataType::List(Arc::new(Field::new("$data$", DataType::Int32, false))),
        _ => return rtdb_columnar::to_field(name, ty),
    };
    Ok(Field::new(name, data_type, true))
}

fn node_value(node: &MetadataNode) -> Result<(&'static str, Value)> {
    Ok(match node {
        MetadataNode::Nil => ("nil", Value::Null),
        MetadataNode::IidBranch(children) => ("branch-iid", Value::list(children.iter().map(|child| child.map(|child| i32::try_from(child).map(Value::I32).map_err(invalid)).unwrap_or(Ok(Value::Null))).collect::<Result<_>>()?)),
        MetadataNode::RecencyBranch(children) => ("branch-recency", Value::list(children.iter().map(|(upper, child)| Ok(Value::Struct(StructFields::from([
            ("max-recency".into(), Value::Timestamp { value: upper.0, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) }),
            ("idx".into(), Value::I32(i32::try_from(*child).map_err(invalid)?)),
        ])))).collect::<Result<_>>()?)),
        MetadataNode::Leaf { data_page_index, columns } => ("leaf", Value::Struct(StructFields::from([
            ("data-page-idx".into(), Value::I32(i32::try_from(*data_page_index).map_err(invalid)?)),
            ("columns".into(), Value::list(columns.values()?.into_owned())),
        ]))),
    })
}

fn build_field_values(field: &Field, values: &[Value]) -> Result<ArrayRef> {
    let nulls = values.iter().map(|value| !matches!(value, Value::Null | Value::Undefined)).collect::<Vec<_>>();
    let nulls = nulls.iter().any(|valid| !valid).then(|| NullBuffer::from(nulls));
    match field.data_type() {
        DataType::Struct(fields) => {
            let children = fields.iter().map(|field| {
                let values = values.iter().map(|value| value.field(field.name()).cloned().unwrap_or(Value::Null)).collect::<Vec<_>>();
                build_field_values(field, &values)
            }).collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(StructArray::try_new_with_length(Fields::from(fields.iter().cloned().collect::<Vec<_>>()), children, nulls, values.len()).map_err(invalid)?))
        }
        DataType::List(element) => {
            let mut elements = Vec::new();
            let mut offsets = vec![0i32];
            for value in values {
                match value {
                    Value::List(values) => elements.extend(values.iter().map(std::borrow::Cow::into_owned)),
                    Value::Null | Value::Undefined => {}
                    _ => return Err(invalid("Metadata list field contains a non-list value")),
                }
                offsets.push(i32::try_from(elements.len()).map_err(invalid)?);
            }
            let elements = build_field_values(element, &elements)?;
            Ok(Arc::new(ListArray::try_new(element.clone(), OffsetBuffer::new(ScalarBuffer::from(offsets)), elements, nulls).map_err(invalid)?))
        }
        _ => {
            let ty = rtdb_columnar::from_field(field)?;
            let ty = if nulls.is_some() && ty != LogicalType::Nothing { ty.maybe() } else { ty };
            rtdb_columnar::copy_array(&ty, values)
        }
    }
}

fn child_index(value: i32, parent: usize) -> Result<usize> {
    let child = usize::try_from(value).map_err(invalid)?;
    if child >= parent { return Err(invalid("Metadata children must precede their parent")); }
    Ok(child)
}

fn stored_leaf(field: &Field, leaves: &ArrayRef, offset: usize) -> Result<MetadataNode> {
    let leaves = leaves.as_any().downcast_ref::<StructArray>().ok_or_else(|| invalid("Metadata leaf is not a struct"))?;
    let DataType::Struct(fields) = field.data_type() else { return Err(invalid("Metadata leaf is not a struct")); };
    let child = |name: &str| fields.iter().position(|field| field.name() == name).ok_or_else(|| invalid(format!("Missing metadata field: {name}")));
    let pages = leaves.column(child("data-page-idx")?).as_any().downcast_ref::<arrow_array::Int32Array>().ok_or_else(|| invalid("Metadata leaf has no page index"))?;
    if pages.is_null(offset) { return Err(invalid("Metadata leaf has no page index")); }
    let columns = child("columns")?;
    Ok(MetadataNode::Leaf { data_page_index: usize::try_from(pages.value(offset)).map_err(invalid)?,
        columns: LeafColumns(Arc::new(LeafSource::Stored { field: Arc::clone(&fields[columns]), columns: Arc::clone(leaves.column(columns)), offset })) })
}

fn required<'a>(fields: &'a StructFields<Value>, key: &str) -> Result<&'a Value> {
    fields.get(key).ok_or_else(|| invalid(format!("Missing metadata field: {key}")))
}

fn invalid(error: impl std::fmt::Display) -> Anomaly {
    Anomaly::incorrect("rtdb.format/invalid-trie-metadata", error.to_string())
}
