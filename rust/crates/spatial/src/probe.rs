use std::{collections::HashMap, sync::Arc};

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, ListArray, StringArray, StructArray, UInt32Array,
    cast::AsArray, types::{Int16Type, Int32Type, Int64Type},
};
use arrow_schema::{DataType, Field, Fields, UnionMode};
use rtdb_columnar::{EXTENSION_NAME_KEY, MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_query::{QueryCursor, SpatialProbeCells, SpatialProbeFallback};
use rtdb_types::{LogicalType, Result};

use crate::read::SelectedHit;

/// The full projection is already admitted by the read. These fields may be
/// represented by their exact clear value size until the caller selects a row.
#[derive(Clone, Copy)]
pub struct Probe<'a> {
    pub fields: &'a [&'a str],
    pub point_column: Option<&'a str>,
    pub certify_point: bool,
}

pub(super) struct Evidence {
    pub(super) omitted: Option<StructArray>,
    pub(super) point_intersects: bool,
    _reservation: Option<Arc<MemoryReservation>>,
}

impl Evidence {
    pub(super) fn new(point_intersects: bool) -> Self {
        Self { omitted: None, point_intersects, _reservation: None }
    }
}

pub(super) struct ProjectionColumns {
    columns: Vec<usize>,
    _reservation: Arc<MemoryReservation>,
}

impl std::ops::Deref for ProjectionColumns {
    type Target = [usize];

    fn deref(&self) -> &[usize] { &self.columns }
}

pub(super) fn summarize(
    hits: &[SelectedHit], columns: &[usize], probe: Probe<'_>, evidence: &mut Evidence,
    memory: &Arc<MemoryBudget>,
) -> Result<ProjectionColumns> {
    let output_reservation = memory.reserve(size_of_val(columns))?;
    if probe.fields.is_empty() {
        return Ok(ProjectionColumns { columns: columns.to_vec(), _reservation: output_reservation });
    }
    let schema = hits[0].source.batch().schema_ref();
    let mut reservation = memory.reserve(0)?;
    let mut omitted = Vec::new();
    let mut output = Vec::with_capacity(columns.len());
    for &index in columns {
        let field = schema.field(index);
        if probe.fields.contains(&field.name().as_str())
            && (evidence.point_intersects || probe.point_column != Some(field.name().as_str()))
        {
            let _measurement = memory.reserve(512 + hits.len() * size_of::<Option<u32>>())?;
            let mut plans = LayoutPlans::new(memory)?;
            let sizes = hits.iter().map(|hit| {
                let batch = hit.source.batch();
                if let Some((plan, cells)) = hit.probe.as_ref().and_then(|probe| probe.plan(index)) {
                    return match cells.cell(hit.row)? {
                        Some((cell, ty)) => plan.layouts.plans.get(&layout_key(cell.field, Some(ty)))?.cell_size(cell.array, cell.row),
                        None => plan.nullable_struct.then_some(None),
                    };
                }
                let field = batch.schema_ref().field(index);
                if field.data_type() == &DataType::Binary {
                    return Measure::compile(field, Shape::Fact).cell_size(batch.column(index).as_ref(), hit.row);
                }
                plans.get(field, None, || Measure::compile(field, Shape::Fact))?.cell_size(batch.column(index).as_ref(), hit.row)
            }).collect::<Option<Vec<_>>>();
            if let Some(sizes) = sizes {
                let retained = Arc::get_mut(&mut reservation).expect("omission evidence owns its reservation");
                let additional = 512 + field.name().len() + hits.len() * size_of::<u32>() + hits.len().div_ceil(8);
                retained.grow_to(retained.bytes() + additional)?;
                omitted.push((
                    Arc::new(Field::new(field.name(), DataType::UInt32, true)),
                    Arc::new(UInt32Array::from(sizes)) as ArrayRef,
                ));
                continue;
            }
        }
        output.push(index);
    }
    if !omitted.is_empty() {
        let (fields, sizes): (Vec<_>, Vec<_>) = omitted.into_iter().unzip();
        let metadata = fields.iter().map(|field| field.size() + size_of::<Arc<Field>>()).sum::<usize>();
        let omitted = StructArray::new(Fields::from(fields), sizes, None);
        let retained = omitted.get_array_memory_size() + metadata;
        Arc::get_mut(&mut reservation).expect("omission evidence owns its reservation").shrink_to(retained)?;
        evidence.omitted = Some(omitted);
        evidence._reservation = Some(reservation);
    }
    Ok(ProjectionColumns { columns: output, _reservation: output_reservation })
}

pub(super) fn gather(hits: &[SelectedHit], columns: &[usize], memory: &Arc<MemoryBudget>) -> Result<Option<OwnedBatch>> {
    use crate::read::gather_selected;

    if hits.iter().all(|hit| hit.probe.is_none()) {
        return gather_selected(hits, columns, memory);
    }
    let mut scratch = memory.reserve(4096 + hits.len() * 256)?;
    let mut source_indices = HashMap::new();
    let mut sources = Vec::<Vec<usize>>::new();
    for (index, hit) in hits.iter().enumerate() {
        let source = *source_indices.entry(Arc::as_ptr(&hit.source).addr()).or_insert_with(|| {
            sources.push(Vec::new());
            sources.len() - 1
        });
        sources[source].push(index);
    }
    let mut projected = hits.iter().map(|_| None).collect::<Vec<_>>();
    for positions in sources {
        let first = &hits[positions[0]];
        let charge = Arc::get_mut(&mut scratch).expect("the probe gather owns its projection scratch");
        charge.grow_to(charge.bytes() + columns.len() * 64)?;
        let (page, compact) = if let Some(fallback) = &first.probe
            && columns.iter().any(|&column| fallback.plan(column).is_some())
        {
            let rows = positions.iter().map(|&position| hits[position].row).collect::<Vec<_>>();
            fallback.project_rows(columns, &rows, memory)?
        } else {
            let batch = first.source.batch().project(columns).map_err(|error| {
                rtdb_types::Anomaly::fault("rtdb.spatial/projection", error.to_string())
            })?;
            (first.source.with_retained_buffers(batch), false)
        };
        let page = Arc::new(page);
        for (row, position) in positions.into_iter().enumerate() {
            let hit = &hits[position];
            projected[position] = Some(SelectedHit {
                source: Arc::clone(&page), probe: None,
                row: if compact { row } else { hit.row }, distance_m: hit.distance_m,
            });
        }
    }
    let projected = projected.into_iter().map(|hit| hit.expect("every hit has a source")).collect::<Vec<_>>();
    gather_selected(&projected, &(0..columns.len()).collect::<Vec<_>>(), memory)
}

const MAX_SIZE: u32 = 32 << 20;
const MAX_DEPTH: usize = 15;

pub(super) async fn next_batch(
    cursor: &mut QueryCursor, probe: Option<Probe<'_>>, geometry: Option<&str>, partition: &[String],
) -> Result<Option<(OwnedBatch, Option<Arc<DeferredPage>>)>> {
    if let Some(probe) = probe.filter(|probe| !probe.fields.is_empty()) {
        let page = cursor.next_spatial_probe_batch(|field, cells, memory| {
            let name = field.name().as_str();
            if !probe.fields.contains(&name) || probe.point_column == Some(name) || geometry == Some(name)
                || partition.iter().any(|column| column.as_str() == name)
                || !matches!(field.data_type(), DataType::Struct(_) | DataType::Union(_, UnionMode::Dense))
            {
                return None;
            }
            DeferredMeasure::compile(field, cells, memory)
        }).await?;
        Ok(page.map(|(batch, fallback)| (batch, fallback.map(Arc::new))))
    } else {
        Ok(cursor.next_batch().await?.map(|batch| (batch, None)))
    }
}

pub(super) type DeferredPage = SpatialProbeFallback<DeferredMeasure>;

pub(super) struct DeferredMeasure {
    layouts: LayoutPlans,
    nullable_struct: bool,
}

impl DeferredMeasure {
    fn compile(target: &Field, cells: SpatialProbeCells<'_>, memory: &Arc<MemoryBudget>) -> Option<Self> {
        let mut layouts = LayoutPlans::new(memory).ok()?;
        for (array, field, logical) in cells.layouts()? {
            let logical = logical?;
            let plan = layouts.get(field, Some(logical), || {
                if cells.preserves_layout(array, field, logical, target) { Measure::compile(field, Shape::Fact) }
                else { Measure::Unsupported }
            })?;
            if !plan.covers_layout() { return None; }
        }
        let nullable_struct = target.is_nullable() && matches!(target.data_type(), DataType::Struct(_));
        Some(Self { layouts, nullable_struct })
    }
}

fn layout_key(field: &Field, logical: Option<&LogicalType>) -> (usize, usize) {
    (std::ptr::from_ref(field).addr(), logical.map_or(0, |logical| std::ptr::from_ref(logical).addr()))
}

struct LayoutPlans {
    plans: HashMap<(usize, usize), Measure>,
    scratch: Arc<MemoryReservation>,
}

impl LayoutPlans {
    fn new(memory: &Arc<MemoryBudget>) -> Result<Self> {
        Ok(Self { plans: HashMap::new(), scratch: memory.reserve(0)? })
    }

    fn get(&mut self, field: &Field, logical: Option<&LogicalType>, compile: impl FnOnce() -> Measure) -> Option<&Measure> {
        let key = layout_key(field, logical);
        use std::collections::hash_map::Entry;
        Some(match self.plans.entry(key) {
            Entry::Occupied(plan) => plan.into_mut(),
            Entry::Vacant(entry) => {
                let scratch = Arc::get_mut(&mut self.scratch).expect("The measurement owns its plans");
                let bound = field.size().checked_mul(8)?.checked_add(1024)?;
                scratch.grow_to(scratch.bytes().checked_add(bound)?).ok()?;
                entry.insert(compile())
            }
        })
    }
}

#[derive(Clone, Copy)]
enum Shape { Fact, Value(usize) }

enum Measure {
    Unsupported,
    UnmeasuredNull,
    Image,
    Wrapper(Box<Measure>),
    Union(Vec<(i8, Measure)>),
    Run(RunWidth, Box<Measure>),
    Null { readable: bool },
    Fixed(u32),
    Text,
    Binary,
    List(Box<Measure>),
    Struct(Vec<Member>),
}

enum RunWidth { I16, I32, I64 }

struct Member {
    value: usize,
    presence: Option<usize>,
    name_bytes: u32,
    measure: Measure,
}

impl Measure {
    fn compile(field: &Field, shape: Shape) -> Self {
        match field.data_type() {
            DataType::Union(fields, UnionMode::Dense) => {
                let mut legs = fields.iter().map(|(id, field)| (id, Self::compile(field, shape))).collect::<Vec<_>>();
                legs.sort_unstable_by_key(|(id, _)| *id);
                return Self::Union(legs);
            }
            DataType::RunEndEncoded(ends, child) => {
                let width = match ends.data_type() {
                    DataType::Int16 => RunWidth::I16,
                    DataType::Int32 => RunWidth::I32,
                    DataType::Int64 => RunWidth::I64,
                    _ => return Self::Unsupported,
                };
                return Self::Run(width, Box::new(Self::compile(child, shape)));
            }
            _ => {}
        }
        if field.metadata().contains_key(EXTENSION_NAME_KEY) { return Self::Unsupported; }
        match (shape, field.data_type()) {
            (Shape::Fact, DataType::Binary) => Self::Image,
            (Shape::Fact, DataType::Null) if field.is_nullable() => Self::UnmeasuredNull,
            (Shape::Fact, DataType::Struct(fields)) if fields.len() == 1 && fields[0].name() == "v2_value"
                && !fields[0].metadata().contains_key(EXTENSION_NAME_KEY) =>
                Self::Wrapper(Box::new(Self::compile(&fields[0], Shape::Value(0)))),
            (Shape::Value(_), DataType::Null) => Self::Null { readable: field.is_nullable() },
            (Shape::Value(_), DataType::Boolean) => Self::Fixed(1),
            (Shape::Value(_), DataType::Int64 | DataType::Float64) => Self::Fixed(9),
            (Shape::Value(_), DataType::Utf8) => Self::Text,
            (Shape::Value(_), DataType::Binary) => Self::Binary,
            (Shape::Value(depth), DataType::List(item)) if depth < MAX_DEPTH =>
                Self::List(Box::new(Self::compile(item, Shape::Value(depth + 1)))),
            (Shape::Value(depth), DataType::Struct(fields)) if depth < MAX_DEPTH && fields.len() <= 2048 =>
                Self::members(fields, depth + 1).map_or(Self::Unsupported, Self::Struct),
            _ => Self::Unsupported,
        }
    }

    fn members(fields: &Fields, depth: usize) -> Option<Vec<Member>> {
        let mut presence = HashMap::new();
        for (position, field) in fields.iter().enumerate() {
            if let Some(name) = field.name().strip_prefix("is_present_") {
                if field.data_type() != &DataType::Boolean || field.metadata().contains_key(EXTENSION_NAME_KEY)
                    || presence.insert(name, position).is_some()
                {
                    return None;
                }
            }
        }
        let count = fields.len() - presence.len();
        if count > 1024 { return None; }
        let mut members = Vec::with_capacity(count);
        let mut previous = None;
        for (value, field) in fields.iter().enumerate() {
            if field.name().starts_with("is_present_") { continue; }
            let name = field.name().strip_prefix("value_")?;
            if !clear_field_name(name.as_bytes()) || previous.is_some_and(|prior| prior >= name) { return None; }
            previous = Some(name);
            members.push(Member {
                value, presence: presence.remove(name), name_bytes: name.len() as u32 + 1,
                measure: Self::compile(field, Shape::Value(depth)),
            });
        }
        presence.is_empty().then_some(members)
    }

    // An unselected leg or a child masked by a null can still affect normal
    // output conversion. Defer it only when the whole layout is covered.
    fn covers_layout(&self) -> bool {
        match self {
            Self::Unsupported | Self::Null { readable: false } => false,
            Self::Union(legs) => legs.iter().all(|(_, leg)| leg.covers_layout()),
            Self::Wrapper(child) | Self::List(child) | Self::Run(_, child) => child.covers_layout(),
            Self::Struct(members) => members.iter().all(|member| member.measure.covers_layout()),
            _ => true,
        }
    }

    fn cell_size(&self, array: &dyn Array, row: usize) -> Option<Option<u32>> {
        let (size, null) = self.measure(array, row)?;
        Some((!null).then_some(size))
    }

    fn measure(&self, array: &dyn Array, row: usize) -> Option<(u32, bool)> {
        if row >= array.len() { return None; }
        match self {
            Self::Union(legs) => {
                let union = array.as_union_opt()?;
                let id = union.type_id(row);
                let index = legs.binary_search_by_key(&id, |(tag, _)| *tag).ok()?;
                return legs[index].1.measure(union.child(id).as_ref(), union.value_offset(row));
            }
            Self::Run(width, child) => {
                macro_rules! measure_run {
                    ($ty:ty) => {{
                        let runs = array.as_run_opt::<$ty>()?;
                        child.measure(runs.values().as_ref(), runs.get_physical_index(row))
                    }};
                }
                return match width {
                    RunWidth::I16 => measure_run!(Int16Type),
                    RunWidth::I32 => measure_run!(Int32Type),
                    RunWidth::I64 => measure_run!(Int64Type),
                };
            }
            _ if array.is_null(row) => return Some((1, true)),
            _ => {}
        }
        let size = match self {
            Self::Unsupported | Self::UnmeasuredNull => return None,
            Self::Null { .. } => return Some((1, true)),
            Self::Image => clear_image_size(array.as_any().downcast_ref::<BinaryArray>()?.value(row))?,
            Self::Wrapper(child) => child.measure(array.as_struct_opt()?.column(0).as_ref(), row)?.0,
            Self::Fixed(size) => *size,
            Self::Text => add(2, u32::try_from(array.as_any().downcast_ref::<StringArray>()?.value(row).len()).ok()?)?,
            Self::Binary => add(2, u32::try_from(array.as_any().downcast_ref::<BinaryArray>()?.value(row).len()).ok()?)?,
            Self::List(child) => {
                let list = array.as_any().downcast_ref::<ListArray>()?;
                let offsets = list.value_offsets();
                let mut entries = usize::try_from(offsets[row]).ok()?..usize::try_from(offsets[row + 1]).ok()?;
                if entries.len() > 8192 { return None; }
                entries.try_fold(2, |size, index| add(size, child.measure(list.values().as_ref(), index)?.0))?
            }
            Self::Struct(members) => {
                let value = array.as_struct_opt()?;
                let mut size = 2;
                for member in members {
                    let (bytes, null) = member.measure.measure(value.column(member.value).as_ref(), row)?;
                    if let Some(position) = member.presence {
                        let flags = value.column(position).as_any().downcast_ref::<BooleanArray>()?;
                        if flags.is_null(row) { return None; }
                        if !flags.value(row) {
                            if !null { return None; }
                            continue;
                        }
                    }
                    size = add(add(size, member.name_bytes)?, bytes)?;
                }
                size
            }
            Self::Union(_) | Self::Run(..) => unreachable!(),
        };
        Some((size, false))
    }
}

fn clear_field_name(key: &[u8]) -> bool {
    key.len() <= 1024 && key.first() != Some(&b'$') && key.iter().all(|byte| (32..127).contains(byte))
}

fn clear_image_size(mut image: &[u8]) -> Option<u32> {
    if image.len() > 3 * MAX_SIZE as usize {
        return None;
    }
    let size = value_size(&mut image, 0)?;
    image.is_empty().then_some(size)
}

fn take<'a>(image: &mut &'a [u8], count: usize) -> Option<&'a [u8]> {
    let (value, rest) = image.split_at_checked(count)?;
    *image = rest;
    Some(value)
}

fn payload_size(image: &mut &[u8], text: bool) -> Option<u32> {
    let mut size = 2;
    loop {
        let end = image.iter().position(|byte| *byte == 0)?;
        let segment = take(image, end)?;
        if text {
            std::str::from_utf8(segment).ok()?;
        }
        size = add(size, u32::try_from(end).ok()?)?;
        match take(image, 2)? {
            [0, 1] => return Some(size),
            [0, 255] => size = add(size, 1)?,
            _ => return None,
        }
    }
}

fn add(left: u32, right: u32) -> Option<u32> {
    left.checked_add(right).filter(|size| *size <= MAX_SIZE)
}

fn value_size(image: &mut &[u8], depth: usize) -> Option<u32> {
    let tag = take(image, 1)?[0];
    match tag {
        2 => Some(1),
        3 if matches!(take(image, 1)?, [0 | 1]) => Some(1),
        4 => { take(image, 8)?; Some(9) }
        5 => {
            let word = u64::from_be_bytes(take(image, 8)?.try_into().ok()?);
            let bits = if word < (1 << 63) { !word } else { word ^ (1 << 63) };
            (!f64::from_bits(bits).is_nan() || bits == 0x7ff8_0000_0000_0000).then_some(9)
        }
        9 | 10 => payload_size(image, tag == 9),
        12 | 13 if depth < MAX_DEPTH => {
            let record = tag == 13;
            let mut previous = None;
            let mut size = 2;
            let mut count = 0;
            while !image.starts_with(&[0, 0]) {
                count += 1;
                if count > if record { 1024 } else { 8192 } {
                    return None;
                }
                if record {
                    let end = image.iter().position(|byte| *byte == 0)?;
                    let key = take(image, end)?;
                    if !clear_field_name(key)
                        || previous.is_some_and(|prior: &[u8]| prior >= key)
                        || take(image, 2)? != [0, 1]
                    {
                        return None;
                    }
                    size = add(size, u32::try_from(key.len()).ok()? + 1)?;
                    previous = Some(key);
                }
                size = add(size, value_size(image, depth + 1)?)?;
            }
            take(image, 2)?;
            Some(size)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_omission_keeps_only_its_size_array_under_a_wide_projection() -> Result<()> {
        let memory = MemoryBudget::new(1 << 20);
        let batch = arrow_array::RecordBatch::try_from_iter((0..32).map(|index| {
            (format!("c_{index}"), Arc::new(BinaryArray::from(vec![&[3, 1][..]])) as ArrayRef)
        })).unwrap();
        let source = Arc::new(OwnedBatch::new(batch, &memory)?);
        let hits = [SelectedHit { source, probe: None, row: 0, distance_m: None }];
        let columns = (0..32).collect::<Vec<_>>();
        let probe = Probe { fields: &["c_0"], point_column: None, certify_point: false };
        let mut evidence = Evidence::new(false);
        let before = memory.used_bytes();
        let selected = summarize(&hits, &columns, probe, &mut evidence, &memory)?;
        assert_eq!(&*selected, &columns[1..]);
        let omitted = evidence.omitted.as_ref().unwrap();
        assert_eq!(omitted.fields()[0].name(), "c_0");
        assert_eq!(omitted.column(0).as_any().downcast_ref::<UInt32Array>().unwrap().value(0), 1);
        drop(selected);
        assert_eq!(memory.used_bytes() - before, evidence._reservation.as_ref().unwrap().bytes());
        assert!(memory.used_bytes() - before < 1024);
        drop(evidence);
        assert_eq!(memory.used_bytes(), before);
        Ok(())
    }

    #[test]
    fn typed_clear_fact_preserves_absent_and_explicit_null() {
        let field = Field::new("v2_value", DataType::Utf8, true);
        let wrapped = StructArray::new(
            vec![Arc::new(field)].into(),
            vec![Arc::new(StringArray::from(vec![Some("text"), None, None]))],
            Some(arrow_buffer::NullBuffer::from(vec![true, true, false])),
        );
        let column = Field::new("c_text", wrapped.data_type().clone(), true);
        let measure = Measure::compile(&column, Shape::Fact);
        assert_eq!(measure.cell_size(&wrapped, 0), Some(Some(6)));
        assert_eq!(measure.cell_size(&wrapped, 1), Some(Some(1)));
        assert_eq!(measure.cell_size(&wrapped, 2), Some(None));
    }

    #[test]
    fn typed_compound_sizes_match_canonical_missing_and_null_members() {
        let object = StructArray::new(
            vec![Field::new("value_note", DataType::Utf8, true), Field::new("is_present_note", DataType::Boolean, false)].into(),
            vec![
                Arc::new(StringArray::from(vec![None, None, Some("x"), Some("hidden")])),
                Arc::new(BooleanArray::from(vec![false, true, true, false])),
            ], None,
        );
        let items = Arc::new(Field::new("item", object.data_type().clone(), false));
        let list = ListArray::new(items, arrow_buffer::OffsetBuffer::new(vec![0, 1, 2, 3, 4].into()), Arc::new(object), None);
        let wrapped = StructArray::new(
            vec![Field::new("v2_value", list.data_type().clone(), true)].into(), vec![Arc::new(list)], None,
        );
        let field = Field::new("c_list", wrapped.data_type().clone(), true);
        let measure = Measure::compile(&field, Shape::Fact);
        for (row, canonical) in [
            &[12, 13, 0, 0, 0, 0][..],
            &[12, 13, b'n', b'o', b't', b'e', 0, 1, 2, 0, 0, 0, 0],
            &[12, 13, b'n', b'o', b't', b'e', 0, 1, 9, b'x', 0, 1, 0, 0, 0, 0],
        ].into_iter().enumerate() {
            assert_eq!(measure.cell_size(&wrapped, row), Some(clear_image_size(canonical)));
        }
        assert_eq!(measure.cell_size(&wrapped, 3), None);
    }

    #[test]
    fn nested_typed_lists_match_the_canonical_depth_limit() {
        let mut values = Arc::new(BooleanArray::from(vec![true])) as ArrayRef;
        for depth in 0..=MAX_DEPTH {
            let wrapped = StructArray::from(vec![(
                Arc::new(Field::new("v2_value", values.data_type().clone(), false)), Arc::clone(&values),
            )]);
            let field = Field::new("c_list", wrapped.data_type().clone(), false);
            let typed = Measure::compile(&field, Shape::Fact).cell_size(&wrapped, 0);
            let mut image = vec![12; depth];
            image.extend_from_slice(&[3, 1]);
            image.resize(image.len() + 2 * depth, 0);
            assert_eq!(typed, Some(clear_image_size(&image)));

            values = Arc::new(ListArray::new(
                Arc::new(Field::new("item", values.data_type().clone(), false)),
                arrow_buffer::OffsetBuffer::new(vec![0, 1].into()), values, None,
            ));
        }
        let wrapped = StructArray::from(vec![(Arc::new(Field::new("v2_value", values.data_type().clone(), false)), values)]);
        let field = Field::new("c_list", wrapped.data_type().clone(), false);
        assert_eq!(Measure::compile(&field, Shape::Fact).cell_size(&wrapped, 0), None);
    }

    #[test]
    fn measures_the_active_typed_fact_union_leg() {
        use arrow_array::{NullArray, UnionArray};
        use arrow_schema::UnionFields;

        let union = UnionArray::try_new(
            UnionFields::try_new(
                [0, 1],
                [Field::new("null", DataType::Null, true), Field::new("utf8", DataType::Utf8, false)],
            ).unwrap(),
            vec![1_i8, 0].into(),
            Some(vec![0_i32, 0].into()),
            vec![Arc::new(NullArray::new(1)), Arc::new(StringArray::from(vec!["text"]))],
        ).unwrap();
        let wrapped = StructArray::new(
            vec![Arc::new(Field::new("v2_value", union.data_type().clone(), false))].into(),
            vec![Arc::new(union)],
            None,
        );
        let column = Field::new("c_text", wrapped.data_type().clone(), true);
        let measure = Measure::compile(&column, Shape::Fact);
        assert_eq!(measure.cell_size(&wrapped, 0), Some(Some(6)));
        assert_eq!(measure.cell_size(&wrapped, 1), Some(Some(1)));
    }

    #[test]
    fn measures_clear_images_and_declines_unadmitted_values() {
        assert_eq!(clear_image_size(&[2]), Some(1));
        assert_eq!(clear_image_size(&[9, b'a', 0, 255, b'b', 0, 1]), Some(5));
        assert_eq!(clear_image_size(&[13, b'a', 0, 1, 12, 2, 3, 0, 0, 0, 0, 0]), Some(8));
        assert_eq!(clear_image_size(&[9, 0xc2, 0, 255, 0x80, 0, 1]), None);
        assert_eq!(clear_image_size(&[13, b'$', 0, 1, 2, 0, 0]), None);
        assert_eq!(clear_image_size(&[3, 2]), None);
        assert_eq!(clear_image_size(&[2, 2]), None);
    }
}
