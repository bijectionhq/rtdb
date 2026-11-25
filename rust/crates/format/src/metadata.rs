use rtdb_types::StructFields;
use std::{collections::HashSet, sync::Arc};

use arrow_array::{
    Array, ArrayRef, Int32Array, ListArray, NullArray, RecordBatch, StructArray,
    TimestampMicrosecondArray, UnionArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit as ArrowTimeUnit, UnionFields, UnionMode};
use rtdb_columnar::{ArrowCell, MemoryBudget, MemoryReservation};
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

#[derive(Clone)]
pub struct LeafColumns(Arc<LeafSource>);
enum LeafSource {
    Decoded(Vec<Value>),
    Page(StructArray),
    Stored { field: Arc<Field>, columns: ArrayRef, offset: usize, statistics: Option<StoredStatistics> },
}

struct StoredStatistics {
    field: Arc<Field>,
    records: ArrayRef,
    rows: std::ops::Range<usize>,
}
impl LeafColumns {
    pub fn new(columns: Vec<Value>) -> Self { Self(Arc::new(LeafSource::Decoded(columns))) }
    pub fn values(&self) -> Result<std::borrow::Cow<'_, [Value]>> {
        match self.0.as_ref() {
            LeafSource::Decoded(columns) => Ok(std::borrow::Cow::Borrowed(columns)),
            LeafSource::Page(page) => {
                let field = Field::new("$data$", page.data_type().clone(), false);
                let reader = rtdb_columnar::ColumnReader::new(&field)?;
                let values = (0..page.len()).map(|row| {
                    let Value::Struct(mut fields) = reader.read(page, row)? else { return Err(invalid("Column metadata must be a struct")); };
                    fields.retain(|_, value| !matches!(value, Value::Null));
                    Ok(Value::Struct(fields))
                }).collect::<Result<_>>()?;
                Ok(std::borrow::Cow::Owned(values))
            }
            LeafSource::Stored { field, columns, offset, .. } => match rtdb_columnar::read_value(field, columns.as_ref(), *offset)? {
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
    Arrow { field: Option<&'a Field>, records: &'a dyn Array, rows: std::ops::Range<usize> },
}

#[derive(Clone, Copy)]
enum StatisticCell<'a> { Value(&'a Value), Arrow(ArrowCell<'a>), Record(&'a StructArray, usize) }

#[derive(Clone, Copy)]
pub struct ColumnStatistics<'a>(StatisticCell<'a>);

impl LeafColumns {
    pub fn statistics(&self) -> Result<LeafStatistics<'_>> {
        match self.0.as_ref() {
            LeafSource::Page(records) => return Ok(LeafStatistics(StatisticsSource::Arrow {
                field: None, records, rows: 0..records.len(),
            })),
            LeafSource::Stored { statistics: Some(stats), .. } => {
                return Ok(LeafStatistics(StatisticsSource::Arrow {
                    field: Some(&stats.field), records: stats.records.as_ref(), rows: stats.rows.clone(),
                }));
            }
            LeafSource::Stored { field, columns, offset, .. } => {
                let cell = ArrowCell { field, array: columns.as_ref(), row: *offset };
                if let Some((field, records, rows)) = cell.sequence_range()
                    && cell.sequence_kind() == Some(rtdb_types::ListKind::List)
                    && records.data_type() == field.data_type()
                    && typed_statistics_layout(field) {
                    return Ok(LeafStatistics(StatisticsSource::Arrow { field: Some(field), records, rows }));
                }
            }
            LeafSource::Decoded(_) => {}
        }
        self.values().map(|values| LeafStatistics(StatisticsSource::Values(values)))
    }
}

impl LeafStatistics<'_> {
    fn columns(&self) -> impl Iterator<Item = ColumnStatistics<'_>> {
        let len = match &self.0 { StatisticsSource::Arrow { rows, .. } => rows.len(), StatisticsSource::Values(values) => values.len() };
        (0..len).map(|position| ColumnStatistics(match &self.0 {
            StatisticsSource::Arrow { field: Some(field), records, rows } => StatisticCell::Arrow(ArrowCell {
                field, array: *records, row: rows.start + position,
            }),
            StatisticsSource::Arrow { field: None, records, rows } => StatisticCell::Record(records.as_any().downcast_ref::<StructArray>().expect("page statistics are a struct"), rows.start + position),
            StatisticsSource::Values(values) => StatisticCell::Value(&values[position]),
        }))
    }

    pub fn named<'a>(&'a self, name: &'a str) -> impl Iterator<Item = ColumnStatistics<'a>> {
        self.columns().filter(move |column| column.name() == Some(name))
    }

    pub fn root_column(&self, name: &str) -> Option<ColumnStatistics<'_>> {
        self.columns().find(|column| column.name() == Some(name) && column.flag("root-col?") == Some(true))
    }

    pub fn has_redacted_assertions(&self) -> bool {
        self.root_column("_system_from").and_then(|column| column.flag("redacted?")) == Some(true)
    }
}

impl<'a> StatisticCell<'a> {
    fn field(self, name: &str) -> Option<Self> {
        match self {
            Self::Value(value) => value.field(name).map(Self::Value),
            Self::Arrow(cell) => cell.structure()?.find(|child| child.field.name() == name).map(Self::Arrow),
            Self::Record(records, row) => {
                let (index, field) = records.fields().iter().enumerate().find(|(_, field)| field.name() == name)?;
                Some(Self::Arrow(ArrowCell { field, array: records.column(index).as_ref(), row }))
            }
        }
    }

    fn primitive<A: Array + 'static>(self) -> Option<(&'a A, usize)> {
        let Self::Arrow(cell) = self else { return None; };
        let ArrowCell { field, array, row } = cell.physical().ok()?;
        if field.metadata().contains_key(rtdb_columnar::EXTENSION_NAME_KEY) || array.is_null(row) { return None; }
        Some((array.as_any().downcast_ref()?, row))
    }
}

impl<'a> ColumnStatistics<'a> {
    fn name(self) -> Option<&'a str> {
        match self.0.field("col-name")? {
            StatisticCell::Value(Value::Utf8(name)) => Some(name),
            cell => cell.primitive::<arrow_array::StringArray>().map(|(values, row)| values.value(row)),
        }
    }

    pub fn flag(self, field: &str) -> Option<bool> {
        match self.0.field(field)? {
            StatisticCell::Value(Value::Bool(value)) => Some(*value),
            cell => cell.primitive::<arrow_array::BooleanArray>().map(|(values, row)| values.value(row)),
        }
    }

    pub fn count(self) -> Option<i64> {
        match self.0.field("count")? {
            StatisticCell::Value(Value::I64(value)) => Some(*value),
            cell => cell.primitive::<arrow_array::Int64Array>().map(|(values, row)| values.value(row)),
        }
    }

    pub fn bound(self, flavour: &str, key: &str) -> Option<f64> {
        match self.0.field(flavour)?.field(key)? {
            StatisticCell::Value(Value::F64(value)) => Some(*value),
            cell => cell.primitive::<arrow_array::Float64Array>().map(|(values, row)| values.value(row)),
        }
    }

    pub fn bloom(self) -> Option<&'a [u8]> {
        match self.0.field("bytes")?.field("bloom")? {
            StatisticCell::Value(Value::Binary(value)) => Some(value),
            cell => cell.primitive::<arrow_array::BinaryArray>().map(|(values, row)| values.value(row)),
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
            let columns = match columns.0.as_ref() {
                LeafSource::Decoded(columns) => columns,
                LeafSource::Page(page) => return page.get_array_memory_size().checked_add(base).ok_or_else(metadata_size),
                LeafSource::Stored { .. } => return Ok(base),
            };
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
        let base = i32::try_from(self.column_count).map_err(invalid)?;
        let columns = crate::statistics::page_statistics(batch, self.calculate_blooms, base)?;
        let column_count = self.column_count.checked_add(columns.len()).ok_or_else(|| invalid("Metadata column count overflow"))?;
        i32::try_from(column_count).map_err(invalid)?;
        let row_count = self.row_count.checked_add(i64::try_from(batch.num_rows()).map_err(invalid)?).ok_or_else(|| invalid("Metadata row count overflow"))?;
        i32::try_from(self.page_count).map_err(invalid)?;
        let index = self.push(MetadataNode::Leaf { data_page_index: self.page_count, columns: LeafColumns(Arc::new(LeafSource::Page(columns))) })?;
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
        Self::read_file(ArrowFile::open(bytes, None)?)
    }

    pub fn read_reader(reader: impl std::io::Read + std::io::Seek + Send + 'static) -> Result<Self> {
        Self::read_file(ArrowFile::from_reader(reader, None)?)
    }

    pub fn read_file(mut file: ArrowFile) -> Result<Self> {
        if file.page_count() != 1 { return Err(invalid("Trie metadata must contain exactly one Arrow record batch")); }
        let batch = file.read_page(0)?;
        let index = batch.schema().index_of("nodes").map_err(invalid)?;
        let nodes = batch.column(index).as_any().downcast_ref::<UnionArray>().ok_or_else(|| invalid("Trie metadata nodes must be a union"))?;
        let DataType::Union(fields, _) = nodes.data_type() else { return Err(invalid("Missing metadata union schema")); };
        let typed_columns = fields.iter().find(|(_, field)| field.name() == "leaf").is_some_and(|(_, leaf)| {
            let DataType::Struct(fields) = leaf.data_type() else { return false; };
            fields.find("columns").is_some_and(|(_, field)| typed_statistics_layout(field))
        });
        let mut decoded = Vec::with_capacity(nodes.len());
        for row in 0..nodes.len() {
            let id = nodes.type_id(row);
            let field = fields.iter().find_map(|(type_id, field)| (id == type_id).then_some(field)).ok_or_else(|| invalid("Metadata node type has no declared leg"))?;
            if field.name() == "leaf" {
                decoded.push(stored_leaf(field, nodes.child(id), nodes.value_offset(row), typed_columns)?);
                continue;
            }
            let cell = ArrowCell { field, array: nodes.child(id).as_ref(), row: nodes.value_offset(row) };
            if let Some(branch) = read_branch(cell, row) {
                decoded.push(branch?);
                continue;
            }
            let value = rtdb_columnar::read_value(cell.field, cell.array, cell.row)?;
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
        let mut page_layouts = HashSet::new();
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
                    match columns.0.as_ref() {
                        LeafSource::Page(page) if page_layouts.insert(page.data_type()) => {
                            let field = Field::new("$data$", page.data_type().clone(), false);
                            column_types.push(rtdb_columnar::from_field(&field)?);
                        }
                        LeafSource::Page(_) => {}
                        _ => column_types.extend(columns.values()?.iter().map(Value::logical_type)),
                    }
                }
            }
        }
        drop(page_layouts);
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
        if self.stored_bytes != 0 { return self.write_imported(); }
        let field = self.schema.field_with_name("nodes").map_err(invalid)?;
        let DataType::Union(fields, UnionMode::Dense) = field.data_type() else { return Err(invalid("Metadata schema must use a dense node union")); };
        let mut leg_lengths = vec![0usize; fields.len()];
        let mut ids = Vec::with_capacity(self.nodes.len());
        let mut offsets = Vec::with_capacity(self.nodes.len());
        let mut iid = Vec::new();
        let mut recency_ends = vec![0i32];
        let mut recency_times = Vec::new();
        let mut recency_indices = Vec::new();
        let mut leaf_pages = Vec::new();
        let mut leaf_ends = vec![0i32];
        let mut leaf_columns = Vec::new();
        for node in &self.nodes {
            let name = match node {
                MetadataNode::Nil => "nil",
                MetadataNode::IidBranch(children) => {
                    for child in children { iid.push(child.map(i32::try_from).transpose().map_err(invalid)?); }
                    "branch-iid"
                }
                MetadataNode::RecencyBranch(children) => {
                    for (ceiling, child) in children {
                        recency_times.push(ceiling.0);
                        recency_indices.push(i32::try_from(*child).map_err(invalid)?);
                    }
                    recency_ends.push(i32::try_from(recency_times.len()).map_err(invalid)?);
                    "branch-recency"
                }
                MetadataNode::Leaf { data_page_index, columns } => {
                    leaf_pages.push(i32::try_from(*data_page_index).map_err(invalid)?);
                    let (_, leaf) = fields.iter().find(|(_, field)| field.name() == "leaf")
                        .ok_or_else(|| invalid("Metadata schema has no leaf leg"))?;
                    let page = leaf_statistics_array(leaf, columns)?;
                    let rows = i64::from(*leaf_ends.last().expect("leaf offsets start at zero")) + i64::try_from(page.len()).map_err(invalid)?;
                    leaf_ends.push(i32::try_from(rows).map_err(invalid)?);
                    leaf_columns.push(page);
                    "leaf"
                }
            };
            let (position, (id, _)) = fields.iter().enumerate().find(|(_, (_, field))| field.name() == name).ok_or_else(|| invalid(format!("Metadata schema has no {name} leg")))?;
            ids.push(id);
            offsets.push(i32::try_from(leg_lengths[position]).map_err(invalid)?);
            leg_lengths[position] += 1;
        }
        let mut iid = Some(iid);
        let mut recency = Some((recency_ends, recency_times, recency_indices));
        let mut leaves = Some((leaf_pages, leaf_ends, leaf_columns));
        let children = fields.iter().zip(leg_lengths).map(|((_, field), len)| match field.name().as_str() {
            "nil" => Ok(Arc::new(NullArray::new(len)) as ArrayRef),
            "branch-iid" => {
                let ends = (0..=len).map(|index| i32::try_from(index * 4).map_err(invalid)).collect::<Result<_>>()?;
                build_list(field, ends, Arc::new(Int32Array::from(iid.take().unwrap())))
            }
            "branch-recency" => {
                let (ends, times, indices) = recency.take().unwrap();
                let child = list_item(field)?;
                let records = build_struct(child, vec![
                    Arc::new(TimestampMicrosecondArray::from(times).with_timezone("UTC")),
                    Arc::new(Int32Array::from(indices)),
                ], None, indices_len(&ends))?;
                build_list(field, ends, records)
            }
            "leaf" => {
                let (pages, ends, columns) = leaves.take().unwrap();
                let DataType::Struct(children) = field.data_type() else { return Err(invalid("Metadata leaf is not a struct")); };
                let columns_field = children.find("columns").ok_or_else(|| invalid("Missing metadata field: columns"))?.1;
                let statistics = if columns.is_empty() { arrow_array::new_empty_array(list_item(columns_field)?.data_type()) } else {
                    let arrays = columns.iter().map(AsRef::as_ref).collect::<Vec<_>>();
                    arrow_select::concat::concat(&arrays).map_err(invalid)?
                };
                build_struct(field, vec![Arc::new(Int32Array::from(pages)), build_list(columns_field, ends, statistics)?], None, len)
            }
            _ => build_field_values(field, &[]),
        }).collect::<Result<Vec<_>>>()?;
        let nodes: ArrayRef = Arc::new(UnionArray::try_new(fields.clone(), ScalarBuffer::from(ids), Some(ScalarBuffer::from(offsets)), children).map_err(invalid)?);
        let batch = RecordBatch::try_new(self.schema.clone(), vec![nodes]).map_err(invalid)?;
        crate::ipc::write_file(&self.schema, &[batch])
    }

    fn write_imported(&self) -> Result<Vec<u8>> {
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

fn read_branch(cell: ArrowCell<'_>, parent: usize) -> Option<Result<MetadataNode>> {
    let kind = cell.field.name().as_str();
    let (item, array, rows) = cell.sequence_range()?;
    if cell.sequence_kind() != Some(rtdb_types::ListKind::List) { return None; }
    match kind {
        "branch-iid" => {
            let children = array.as_any().downcast_ref::<Int32Array>()?;
            if !item.metadata().is_empty() { return None; }
            if rows.len() != 4 { return Some(Err(invalid("IID branch must have four buckets"))); }
            Some(rows.map(|row| if children.is_null(row) { Ok(None) } else { child_index(children.value(row), parent).map(Some) })
                .collect::<Result<_>>().map(MetadataNode::IidBranch))
        }
        "branch-recency" => {
            let buckets = array.as_any().downcast_ref::<StructArray>()?;
            if !item.metadata().is_empty() || buckets.fields().len() != 2 || buckets.null_count() != 0 { return None; }
            let (index, child_field) = buckets.fields().find("idx")?;
            let children = buckets.column(index).as_any().downcast_ref::<Int32Array>()?;
            let (index, time_field) = buckets.fields().find("max-recency")?;
            let ceilings = buckets.column(index).as_any().downcast_ref::<TimestampMicrosecondArray>()?;
            if !child_field.metadata().is_empty() || !time_field.metadata().is_empty() { return None; }
            Some((|| {
                let mut branch = Vec::with_capacity(rows.len());
                for row in rows {
                    if children.is_null(row) { return Err(invalid("Recency bucket has no child index")); }
                    if ceilings.is_null(row) { return Err(invalid("Recency bucket has no microsecond ceiling")); }
                    let upper = Timestamp(ceilings.value(row));
                    if branch.last().is_some_and(|(previous, _)| *previous > upper) { return Err(invalid("Recency ceilings must be nondecreasing")); }
                    branch.push((upper, child_index(children.value(row), parent)?));
                }
                Ok(MetadataNode::RecencyBranch(branch))
            })())
        }
        _ => None,
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

fn list_item(field: &Field) -> Result<&Arc<Field>> {
    match field.data_type() {
        DataType::List(item) => Ok(item),
        _ => Err(invalid("Metadata list field contains a non-list value")),
    }
}

fn indices_len(offsets: &[i32]) -> usize { *offsets.last().expect("list offsets start at zero") as usize }

fn build_list(field: &Field, offsets: Vec<i32>, array: ArrayRef) -> Result<ArrayRef> {
    Ok(Arc::new(ListArray::try_new(list_item(field)?.clone(), OffsetBuffer::new(ScalarBuffer::from(offsets)), array, None).map_err(invalid)?))
}

fn build_struct(field: &Field, arrays: Vec<ArrayRef>, nulls: Option<NullBuffer>, len: usize) -> Result<ArrayRef> {
    let DataType::Struct(fields) = field.data_type() else { return Err(invalid("Column metadata must be a struct")); };
    Ok(Arc::new(StructArray::try_new_with_length(fields.clone(), arrays, nulls, len).map_err(invalid)?))
}

fn leaf_statistics_array(leaf: &Field, columns: &LeafColumns) -> Result<ArrayRef> {
    let DataType::Struct(fields) = leaf.data_type() else { return Err(invalid("Metadata leaf is not a struct")); };
    let columns_field = fields.find("columns").ok_or_else(|| invalid("Missing metadata field: columns"))?.1;
    let field = list_item(columns_field)?;
    match columns.0.as_ref() {
        LeafSource::Page(page) => align_statistics(field, page),
        _ => build_field_values(field, &columns.values()?),
    }
}

fn align_statistics(target: &Field, array: &dyn Array) -> Result<ArrayRef> {
    if target.data_type() == array.data_type() { return Ok(arrow_array::make_array(array.to_data())); }
    let DataType::Struct(fields) = target.data_type() else { return Err(invalid("Column statistics layouts differ")); };
    let source = array.as_any().downcast_ref::<StructArray>().ok_or_else(|| invalid("Column metadata must be a struct"))?;
    let children = fields.iter().map(|field| match source.column_by_name(field.name()) {
        Some(array) => align_statistics(field, array.as_ref()),
        None => Ok(arrow_array::new_null_array(field.data_type(), source.len())),
    }).collect::<Result<Vec<_>>>()?;
    build_struct(target, children, source.nulls().cloned(), source.len())
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

fn stored_leaf(field: &Field, leaves: &ArrayRef, offset: usize, typed_columns: bool) -> Result<MetadataNode> {
    let leaves = leaves.as_any().downcast_ref::<StructArray>().ok_or_else(|| invalid("Metadata leaf is not a struct"))?;
    let DataType::Struct(fields) = field.data_type() else { return Err(invalid("Metadata leaf is not a struct")); };
    let child = |name: &str| fields.iter().position(|field| field.name() == name).ok_or_else(|| invalid(format!("Missing metadata field: {name}")));
    let pages = leaves.column(child("data-page-idx")?).as_any().downcast_ref::<arrow_array::Int32Array>().ok_or_else(|| invalid("Metadata leaf has no page index"))?;
    if pages.is_null(offset) { return Err(invalid("Metadata leaf has no page index")); }
    let columns = child("columns")?;
    Ok(MetadataNode::Leaf { data_page_index: usize::try_from(pages.value(offset)).map_err(invalid)?,
        columns: LeafColumns(Arc::new(LeafSource::Stored {
            field: Arc::clone(&fields[columns]), columns: Arc::clone(leaves.column(columns)), offset,
            statistics: typed_columns.then(|| stored_statistics(&fields[columns], leaves.column(columns).as_ref(), offset)).flatten(),
        })) })
}

fn typed_statistics_layout(field: &Field) -> bool {
    if field.metadata().contains_key(rtdb_columnar::EXTENSION_NAME_KEY) {
        return false;
    }
    match field.data_type() {
        DataType::Struct(fields) => fields.iter().enumerate().all(|(index, field)| {
            fields.iter().take(index).all(|earlier| earlier.name() != field.name()) && typed_statistics_layout(field)
        }),
        DataType::List(item) | DataType::FixedSizeList(item, _) => typed_statistics_layout(item),
        DataType::Map(..) => false,
        DataType::RunEndEncoded(ends, item) =>
            matches!(ends.data_type(), DataType::Int16 | DataType::Int32 | DataType::Int64) && typed_statistics_layout(item),
        DataType::Union(legs, UnionMode::Dense) => legs.iter().all(|(_, leg)| typed_statistics_layout(leg))
            && rtdb_columnar::from_field(field).and_then(|logical| rtdb_columnar::to_field(field.name(), &logical))
                .is_ok_and(|canonical| canonical.data_type() == field.data_type()),
        _ => rtdb_columnar::from_field(field).is_ok(),
    }
}

fn stored_statistics(field: &Field, array: &dyn Array, row: usize) -> Option<StoredStatistics> {
    let DataType::List(item) = field.data_type() else { return None; };
    let list = array.as_any().downcast_ref::<ListArray>()?;
    if list.is_null(row) || field.metadata().contains_key(rtdb_columnar::EXTENSION_NAME_KEY) {
        return None;
    }
    let offsets = list.value_offsets();
    Some(StoredStatistics { field: Arc::clone(item), records: Arc::clone(list.values()),
        rows: offsets[row] as usize..offsets[row + 1] as usize })
}

fn required<'a>(fields: &'a StructFields<Value>, key: &str) -> Result<&'a Value> {
    fields.get(key).ok_or_else(|| invalid(format!("Missing metadata field: {key}")))
}

fn invalid(error: impl std::fmt::Display) -> Anomaly {
    Anomaly::incorrect("rtdb.format/invalid-trie-metadata", error.to_string())
}

#[cfg(test)]
mod metadata_wave_tests {
    use super::*;
    use crate::data::{StoredEvent, StoredOperation};
    use rtdb_types::Iid;

    #[test]
    fn metadata_schema_keeps_first_field_and_union_leg_order_with_repeated_page_layouts() {
        let page = |names: [&str; 2]| {
            let fields: Fields = names.into_iter()
                .map(|name| Field::new(name, DataType::Int32, false)).collect();
            let value: ArrayRef = Arc::new(Int32Array::from(vec![1]));
            let arrays = vec![value; names.len()];
            let records = StructArray::try_new(fields, arrays, None).unwrap();
            LeafColumns(Arc::new(LeafSource::Page(records)))
        };
        let decoded = LeafColumns::new(vec![Value::Struct(StructFields::from([
            ("quantity".into(), Value::F64(1.0)),
            ("decoded-only".into(), Value::Bool(true)),
        ]))]);
        let sources = [
            page(["left-only", "quantity"]),
            page(["right-only", "left-only"]),
            decoded,
            page(["left-only", "quantity"]),
        ];
        let nodes = sources.into_iter().enumerate()
            .map(|(data_page_index, columns)| MetadataNode::Leaf { data_page_index, columns }).collect();
        let file = MetadataFile::from_nodes(nodes, false).unwrap();
        let DataType::Union(legs, _) = file.schema.field(0).data_type() else { panic!("expected node union") };
        let (_, leaf) = legs.iter().find(|(_, field)| field.name() == "leaf").unwrap();
        let DataType::Struct(fields) = leaf.data_type() else { panic!("expected leaf fields") };
        let item = list_item(fields.find("columns").unwrap().1).unwrap();
        let DataType::Struct(fields) = item.data_type() else { panic!("expected statistics fields") };
        let names = fields.iter().map(|field| field.name().as_str()).collect::<Vec<_>>();
        assert_eq!(names, ["col-name", "root-col?", "count", "left-only", "quantity", "right-only", "decoded-only"]);
        assert!(fields.find("left-only").unwrap().1.is_nullable());
        let DataType::Union(legs, _) = fields.find("quantity").unwrap().1.data_type() else { panic!("expected quantity union") };
        let types = legs.iter().map(|(_, leg)| leg.data_type()).collect::<Vec<_>>();
        assert_eq!(types, [&DataType::Int32, &DataType::Null, &DataType::Float64]);
    }

    #[test]
    fn metadata_typed_legs_match_the_legacy_ipc_bytes() {
        for recency in [false, true] {
            for blooms in [false, true] {
                let mut file = MetadataBuilder::new(recency, blooms);
                file.write_nil().unwrap();
                for (index, value) in [Value::F64(-0.0), Value::Utf8("s".repeat(2000)), Value::F64(f64::INFINITY), Value::Bool(true)].into_iter().enumerate() {
                    let row = StructFields::from([
                        ("a".to_owned(), Value::list(vec![Value::I32(9)])),
                        ("b".to_owned(), Value::Struct(StructFields::from([("n".into(), value)]))),
                        ("c".to_owned(), Value::F64(f64::from_bits(0x7ff8_0000_0000_0013))),
                        ("d".to_owned(), Value::Null),
                    ]);
                    let fields = row.iter().map(|(name, val)| (name.clone(), val.logical_type())).collect::<StructFields<_>>();
                    let events = [StoredEvent {
                        position: None,
                        origin: None, commit_position: 0, entity_key: None, iid: Iid([index as u8; 16]),
                        system_from: Timestamp(10), valid_time: Interval::ALL,
                        operation: StoredOperation::Put(row), recency: recency.then_some(Timestamp(100)),
                    }];
                    let page = crate::data::build_event_batch_with_recency(&events, Some(fields), None, recency).unwrap();
                    let leaf = file.write_leaf(&page).unwrap();
                    let MetadataNode::Leaf { columns, .. } = &file.nodes[leaf] else { panic!("expected leaf") };
                    assert!(matches!(columns.0.as_ref(), LeafSource::Page(_)));
                    assert_eq!(columns.statistics().unwrap().root_column("a").unwrap().count(), Some(1));
                }
                let branch = file.write_iid_branch([Some(1), Some(2), Some(3), Some(4)]).unwrap();
                if recency { file.write_recency_branch(vec![(Timestamp(100), branch), (Timestamp::MAX, 0)]).unwrap(); }
                let (file, _) = file.finish().unwrap();
                let old_nodes = file.nodes.iter().map(|node| match node {
                    MetadataNode::Leaf { data_page_index, columns } => MetadataNode::Leaf {
                        data_page_index: *data_page_index, columns: LeafColumns::new(columns.values().unwrap().into_owned()),
                    },
                    node => node.clone(),
                }).collect();
                let legacy = MetadataFile::from_nodes(old_nodes, recency).unwrap();
                assert_eq!(file.schema(), legacy.schema());
                let bytes = file.write().unwrap();
                assert_eq!(bytes, legacy.write_imported().unwrap());
                let loaded = MetadataFile::read(bytes.into()).unwrap();
                assert_eq!(loaded.leaves(), file.leaves());
                let MetadataNode::Leaf { columns, .. } = &loaded.nodes[2] else { panic!("expected leaf") };
                let LeafSource::Stored { statistics: Some(cached), .. } = columns.0.as_ref() else { panic!("expected cached metadata positions") };
                let stats = columns.statistics().unwrap();
                let StatisticsSource::Arrow { records, .. } = &stats.0 else { panic!("expected typed statistics") };
                assert!(std::ptr::eq(*records, cached.records.as_ref()));
                assert_eq!(stats.root_column("b").unwrap().count(), Some(1));
            }
        }

    }

    #[test]
    fn metadata_cached_extended_statistics_borrow_active_bloom_bytes() {
        use rtdb_types::{RunEndWidth, ScalarType};

        let count_type = LogicalType::RunEndEncoded { width: RunEndWidth::I32, value: Box::new(LogicalType::Scalar(ScalarType::I64)) };
        let bitmap = crate::bloom::BloomFilter::default().to_bytes().unwrap();
        let bytes = Arc::new(arrow_array::BinaryArray::from_iter_values([bitmap.as_slice()]));
        let bitmap_pointer = bytes.value(0).as_ptr();
        let statistics = StructArray::try_new(vec![
            Field::new("col-name", DataType::Utf8, false),
            Field::new("root-col?", DataType::Boolean, false),
            rtdb_columnar::to_field("count", &count_type).unwrap(),
            Field::new("bytes", DataType::Struct(vec![Field::new("bloom", DataType::Binary, true)].into()), true),
        ].into(), vec![
            Arc::new(arrow_array::StringArray::from(vec!["name"])),
            Arc::new(arrow_array::BooleanArray::from(vec![true])),
            rtdb_columnar::build_array(&count_type, &[Value::I64(2)]).unwrap(),
            Arc::new(StructArray::try_new(vec![Field::new("bloom", DataType::Binary, true)].into(), vec![bytes], None).unwrap()),
        ], None).unwrap();
        let item = Arc::new(Field::new("$data$", statistics.data_type().clone(), false));
        let array: ArrayRef = Arc::new(ListArray::try_new(item, OffsetBuffer::new(vec![0, 1].into()), Arc::new(statistics), None).unwrap());
        let field = Arc::new(Field::new("columns", array.data_type().clone(), false));
        assert!(typed_statistics_layout(&field));
        let stored = stored_statistics(&field, array.as_ref(), 0).unwrap();
        let columns = LeafColumns(Arc::new(LeafSource::Stored { field, columns: array, offset: 0, statistics: Some(stored) }));
        let stats = columns.statistics().unwrap();
        let column = stats.root_column("name").unwrap();
        assert_eq!(column.count(), Some(2));
        let bloom = column.bloom().unwrap();
        assert_eq!(bloom.as_ptr(), bitmap_pointer);
        assert_eq!(bloom, bitmap.as_slice());
    }

    #[test]
    fn metadata_ipc_in_memory_buffer_outlives_its_reader() {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arrow_array::Int64Array::from(vec![-1, 4, 6]))]).unwrap();
        let bytes: Arc<[u8]> = crate::ipc::write_file(&schema, &[batch]).unwrap().into();
        let range = bytes.as_ptr_range();
        let mut file = ArrowFile::open(Arc::clone(&bytes), None).unwrap();
        let read = file.read_page(0).unwrap();
        let n = read.column(0).as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
        assert!(range.contains(&n.values().as_ptr().cast::<u8>()));
        drop(file);
        drop(bytes);
        assert_eq!(n.values().as_ref(), &[-1, 4, 6]);
    }
}
