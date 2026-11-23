use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::{bloom::BloomFilter, transit::{TransitValue, encode_msgpack}};
use rtdb_plan::{BinaryOp, ColumnId, Expr, Field, NamedExpr, Schema};
use rtdb_types::{EntityId, Iid, LogicalType, Result, ScalarType, Value};
use crate::{columnar::ColumnBatch, context::Execution, invalid, scalar_key::ScalarKey};

type IidEntries = BTreeMap<Iid, Arc<MemoryReservation>>;

#[derive(Clone, Copy, PartialEq, Eq)]
enum EntityIdDomain { Integer, String, Keyword, Uuid }

impl EntityIdDomain {
    fn of_type(data_type: &LogicalType) -> Option<Self> {
        match data_type {
            LogicalType::Maybe(inner) => Self::of_type(inner),
            LogicalType::Scalar(ScalarType::I8 | ScalarType::I16 | ScalarType::I32 | ScalarType::I64) => Some(Self::Integer),
            LogicalType::Scalar(ScalarType::Utf8) => Some(Self::String),
            LogicalType::Scalar(ScalarType::Keyword) => Some(Self::Keyword),
            LogicalType::Scalar(ScalarType::Uuid) => Some(Self::Uuid),
            _ => None,
        }
    }

    fn contains(self, value: &Value) -> bool {
        matches!((self, value),
            (Self::Integer, Value::I8(_) | Value::I16(_) | Value::I32(_) | Value::I64(_))
            | (Self::String, Value::Utf8(_))
            | (Self::Keyword, Value::Keyword(_))
            | (Self::Uuid, Value::Uuid(_)))
    }
}

enum IidCandidates {
    Raw(IidEntries),
    Entity(EntityIdDomain, IidEntries),
    UnresolvedEntity,
}

impl IidCandidates {
    fn for_probe(field: &Field) -> Option<Self> {
        match field.name.as_str() {
            "_iid" => Some(Self::Raw(BTreeMap::new())),
            "_id" => Some(EntityIdDomain::of_type(&field.data_type)
                .map_or(Self::UnresolvedEntity, |domain| Self::Entity(domain, BTreeMap::new()))),
            _ => None,
        }
    }

    fn entries_for(&self, field: &Field, canonical_entity_iids: bool) -> Option<&IidEntries> {
        match self {
            Self::Raw(entries) if field.name == "_iid" => Some(entries),
            Self::Entity(domain, entries) if field.name == "_id"
                && (canonical_entity_iids || entries.is_empty())
                && EntityIdDomain::of_type(&field.data_type) == Some(*domain) => Some(entries),
            _ => None,
        }
    }

    fn count_for(&self, field: &Field, canonical_entity_iids: bool) -> Option<usize> {
        if matches!(self, Self::UnresolvedEntity) && field.name == "_id" {
            Some(0)
        } else {
            self.entries_for(field, canonical_entity_iids).map(BTreeMap::len)
        }
    }

    fn insert(&mut self, value: &Value, scope: &rtdb_expr::EvaluationScope, memory: &Arc<MemoryBudget>) -> Result<bool> {
        let (iid, entries) = match self {
            Self::Raw(entries) => {
                let Value::FixedSizeBinary(bytes) = value else { return Ok(false); };
                let Ok(bytes) = bytes.as_slice().try_into() else { return Ok(false); };
                (Iid(bytes), entries)
            }
            Self::Entity(domain, entries) if domain.contains(value) => {
                scope.reserve(value.retained_bytes()?.checked_mul(3).and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?)?;
                (value.entity_id()?.iid(), entries)
            }
            _ => return Ok(false),
        };
        if !entries.contains_key(&iid) {
            entries.insert(iid, memory.reserve(256)?);
        }
        Ok(true)
    }

    fn insert_scalar(&mut self, key: ScalarKey<'_>, memory: &Arc<MemoryBudget>) -> Result<bool> {
        let (iid, entries) = match (self, key) {
            (Self::Entity(EntityIdDomain::Integer, entries), ScalarKey::Signed(value)) => (EntityId::Integer(value).iid(), entries),
            (Self::Entity(EntityIdDomain::String, entries), ScalarKey::Text(value)) => (EntityId::string_iid(value), entries),
            _ => return Ok(false),
        };
        if !entries.contains_key(&iid) {
            entries.insert(iid, memory.reserve(256)?);
        }
        Ok(true)
    }
}

struct ColumnFilter {
    bloom: Option<BloomFilter>,
    iids: Option<IidCandidates>,
    _reservation: Arc<MemoryReservation>,
}

pub(crate) struct Pushdowns {
    columns: Vec<(ColumnId, Arc<ColumnFilter>)>,
    _reservation: Arc<MemoryReservation>,
}
impl Pushdowns {
    pub fn merge(inherited: Option<&Arc<Self>>, own: Option<Arc<Self>>, memory: &Arc<MemoryBudget>) -> Result<Option<Arc<Self>>> {
        let Some(own) = own else { return Ok(inherited.cloned()); };
        let Some(inherited) = inherited else { return Ok(Some(own)); };
        let reservation = memory.reserve(map_bound(inherited.columns.len().checked_add(own.columns.len()).ok_or_else(size_error)?)?)?;
        let mut columns = inherited.columns.clone();
        for (id, filter) in &own.columns {
            if let Some(existing) = columns.iter_mut().find(|(key, _)| key == id) { existing.1 = Arc::clone(filter); }
            else { columns.push((*id, Arc::clone(filter))); }
        }
        Ok(Some(Arc::new(Self { columns, _reservation: reservation })))
    }
    pub fn project(self: &Arc<Self>, expressions: &[NamedExpr], memory: &Arc<MemoryBudget>) -> Result<Arc<Self>> {
        let reservation = memory.reserve(map_bound(self.columns.len())?)?;
        let mut columns: Vec<(ColumnId, Arc<ColumnFilter>)> = Vec::with_capacity(self.columns.len());
        for (id, filter) in &self.columns {
            let id = match expressions.iter().find(|expr| expr.field.id == *id) {
                Some(NamedExpr { expr: Expr::Column { id, .. }, .. }) => *id,
                Some(_) => continue,
                None => *id,
            };
            if let Some(existing) = columns.iter_mut().find(|(key, _)| *key == id) { existing.1 = Arc::clone(filter); }
            else { columns.push((id, Arc::clone(filter))); }
        }
        Ok(Arc::new(Self { columns, _reservation: reservation }))
    }
    pub fn needs_entity_iid_proof(&self, schema: &[Field]) -> bool {
        if self.iid_filter(schema, false).is_some() { return false; }
        let Some(field) = schema.iter().find(|field| field.name == "_id") else { return false; };
        self.columns.iter().find(|(id, _)| *id == field.id)
            .and_then(|(_, filter)| filter.iids.as_ref())
            .and_then(|iids| iids.entries_for(field, true))
            .is_some_and(|entries| !entries.is_empty())
    }
    pub fn iid_matches(&self, schema: &[Field], iid: Iid, canonical_entity_iids: bool) -> bool {
        self.iid_filter(schema, canonical_entity_iids).is_none_or(|iids| iids.is_empty() || iids.contains_key(&iid))
    }
    pub fn path_matches(&self, schema: &[Field], lower: Iid, upper: Iid, canonical_entity_iids: bool) -> bool {
        self.iid_filter(schema, canonical_entity_iids).is_none_or(|iids| iids.is_empty() || iids.range(lower..=upper).next().is_some())
    }
    fn iid_filter(&self, schema: &[Field], canonical_entity_iids: bool) -> Option<&IidEntries> {
        ["_iid", "_id"].into_iter().find_map(|name| {
            let field = schema.iter().find(|field| field.name == name)?;
            self.columns.iter().find(|(id, _)| *id == field.id)?.1.iids.as_ref()?.entries_for(field, canonical_entity_iids)
        })
    }
    pub fn page_matches(&self, schema: &[Field], columns: &rtdb_format::metadata::LeafStatistics<'_>, memory: &Arc<MemoryBudget>) -> Result<bool> {
        for field in schema {
            let Some((_, filter)) = self.columns.iter().find(|(id, _)| *id == field.id) else { continue; };
            let Some(bloom) = &filter.bloom else { continue; };
            let Some(column) = columns.root_column(&field.name) else { return Ok(false); };
            // A mixed column's persisted byte Bloom does not cover its Date64 leg.
            if crate::page_metadata::may_have_date64(&field.data_type) { continue; }
            if let Some(bytes) = column.bloom() {
                let _reservation = memory.reserve(bytes.len().checked_mul(8).and_then(|bytes| bytes.checked_add(1 << 20)).ok_or_else(size_error)?)?;
                if !bloom.intersects(&BloomFilter::from_bytes(bytes)?) { return Ok(false); }
            }
        }
        Ok(true)
    }
    pub fn description_bound(&self, schema: &[Field]) -> Result<usize> {
        schema.iter().filter(|field| self.columns.iter().any(|(id, _)| *id == field.id)).try_fold(256usize, |bytes, field| {
            bytes.checked_add(field.name.len().checked_mul(8).and_then(|n| n.checked_add(4096)).ok_or_else(size_error)?).ok_or_else(size_error)
        })
    }
    pub fn describe(&self, schema: &[Field], canonical_entity_iids: bool) -> Result<Value> {
        let mut columns = Vec::new();
        for field in schema {
            let Some((_, filter)) = self.columns.iter().find(|(id, _)| *id == field.id) else { continue; };
            let mut entries = vec![(keyword("column"), TransitValue::String(field.name.clone()))];
            if let Some(bloom) = &filter.bloom { entries.push((keyword("bloom-filter"), TransitValue::Map(vec![(keyword("cardinality"), TransitValue::Integer(bloom.cardinality() as i64))]))); }
            if let Some(count) = filter.iids.as_ref().and_then(|iids| iids.count_for(field, canonical_entity_iids)) { entries.push((keyword("iids"), TransitValue::Map(vec![(keyword("count"), TransitValue::Integer(count as i64))]))); }
            columns.push(TransitValue::Map(entries));
        }
        if columns.is_empty() { Ok(Value::Null) } else { encode_msgpack(&TransitValue::Array(columns)).map(Value::Transit) }
    }
}

/// A build key's filter, with the keys it has already admitted: a repeated key admits nothing new, so its IID is not rehashed.
/// A build key's filters while the build side is read; its Bloom positions are buffered and added in bulk.
struct Key { expression: Expr, probe: ColumnId, filter: ColumnFilter, pending: Pending, seen: std::collections::HashSet<rtdb_types::EntityId>, seen_reservation: Option<Arc<MemoryReservation>> }
const PENDING_POSITIONS: usize = 16 * 1024;
/// Builds of more rows than this mark their Bloom positions in a dense bitmap rather than sorting them.
const DENSE_ROWS: i64 = 16 * 1024;
const DENSE_WORDS: usize = (1usize << rtdb_format::bloom::BLOOM_BITS) / 64;
enum Pending { Sorted(Vec<u32>), Dense(Vec<u64>) }
impl Key {
    fn add(&mut self, positions: [u32; rtdb_format::bloom::BLOOM_K as usize]) {
        match &mut self.pending {
            Pending::Sorted(pending) => {
                pending.extend(positions);
                if pending.len() + rtdb_format::bloom::BLOOM_K as usize > PENDING_POSITIONS { self.flush(); }
            }
            Pending::Dense(words) => for position in positions { words[position as usize / 64] |= 1 << (position % 64); },
        }
    }
    fn flush(&mut self) {
        let Some(bloom) = &mut self.filter.bloom else { return };
        match &mut self.pending {
            Pending::Sorted(pending) => bloom.insert_positions(pending),
            Pending::Dense(words) => {
                bloom.insert_sorted_positions(words.iter().enumerate().flat_map(|(index, word)| {
                    let mut bits = *word;
                    std::iter::from_fn(move || (bits != 0).then(|| { let bit = bits.trailing_zeros(); bits &= bits - 1; index as u32 * 64 + bit }))
                }));
                words.iter_mut().for_each(|word| *word = 0);
            }
        }
    }
}
pub(crate) struct PushdownBuilder { keys: Vec<Key>, _reservation: Arc<MemoryReservation> }
impl PushdownBuilder {
    pub fn new(condition: Option<&Expr>, build: &Schema, probe: &Schema, rows: i64, blooms: bool, execution: &Execution) -> Result<Self> {
        let count = condition.map_or(0, key_bound);
        let bound = map_bound(count)?.checked_add(condition.map(crate::runtime_schema::expression_bound).transpose()?.unwrap_or(0)).ok_or_else(size_error)?;
        let reservation = execution.context.memory.reserve(bound)?;
        let mut keys = Vec::new();
        let bitmap_bound = usize::try_from(rows).ok().and_then(|rows| rows.checked_mul(160)).and_then(|bytes| bytes.checked_add(16_384)).unwrap_or(1 << 20).min(1 << 20);
        if let Some(condition) = condition { collect_keys(condition, build, probe, bitmap_bound, blooms, rows, execution, &mut keys)?; }
        Ok(Self { keys, _reservation: reservation })
    }
    pub async fn insert(&mut self, schema: &Schema, row: &[Value], execution: &Execution) -> Result<()> {
        let mut evaluation = None;
        for key in &mut self.keys {
            if key.filter.bloom.is_none() { continue; }
            let value = match &key.expression {
                Expr::Column { id, .. } => {
                    let mut value = schema.position(*id).and_then(|position| row.get(position)).cloned().ok_or_else(|| invalid("pushdown-key", "Build key column is outside its row"))?;
                    value.admit_expression_dates()?;
                    value
                }
                expression => {
                    if evaluation.is_none() { evaluation = Some(execution.scope()?); }
                    execution.evaluate(evaluation.as_ref().expect("the scope was just opened"), expression, schema, row).await?
                }
            };
            key.add(BloomFilter::value_positions(&value)?);
            if matches!(value, Value::Null | Value::Undefined) { continue; }
            if key.filter.iids.is_some() && let Ok(entity) = value.entity_id() {
                if key.seen.contains(&entity) { continue; }
                let bytes = key.seen.len().saturating_add(1).saturating_mul(value.retained_bytes()?.saturating_add(64)).saturating_add(256);
                match &mut key.seen_reservation {
                    Some(reservation) if reservation.bytes() >= bytes => {}
                    slot => *slot = Some(execution.context.memory.reserve(bytes.saturating_mul(2))?),
                }
                key.seen.insert(entity);
            }
            if let Some(iids) = &mut key.filter.iids {
                if evaluation.is_none() { evaluation = Some(execution.scope()?); }
                if !iids.insert(&value, evaluation.as_ref().expect("the scope was just opened"), &execution.context.memory)? {
                    key.filter.iids = None;
                }
            }
        }
        Ok(())
    }
    pub fn insert_arrow(&mut self, schema: &Schema, batch: &ColumnBatch, row: usize, memory: &Arc<MemoryBudget>) -> Result<()> {
        for key in &mut self.keys {
            if key.filter.bloom.is_none() { continue; }
            let Expr::Column { id, .. } = &key.expression else {
                return Err(invalid("pushdown-key", "Arrow join filter requires a column key"));
            };
            let column = schema.position(*id).and_then(|position| batch.columns.get(position))
                .ok_or_else(|| invalid("pushdown-key", "Build key column is outside its batch"))?;
            if let Some(value) = ScalarKey::read(column, row) {
                key.add(rtdb_format::bloom::hashes(value.hash() as i32, value.murmur3() as i32));
                if value != ScalarKey::Null && let Some(iids) = &mut key.filter.iids
                    && !iids.insert_scalar(value, memory)? {
                    key.filter.iids = None;
                }
                continue;
            }
            if let Some(((xxhash, murmur3), null)) = crate::set_keys::direct_bloom_hashes(column, row) {
                key.add(rtdb_format::bloom::hashes(xxhash, murmur3));
                if !null { key.filter.iids = None; }
                continue;
            }
            let scope = rtdb_expr::EvaluationScope::new(memory)?;
            let value = column.expression_value(row, true, Some(&scope))?;
            key.add(BloomFilter::value_positions(&value)?);
            if !matches!(value, Value::Null | Value::Undefined)
                && let Some(iids) = &mut key.filter.iids && !iids.insert(&value, &scope, memory)? {
                key.filter.iids = None;
            }
        }
        Ok(())
    }
    pub fn finish(self, memory: &Arc<MemoryBudget>) -> Result<Option<Arc<Pushdowns>>> {
        if self.keys.is_empty() { return Ok(None); }
        let reservation = memory.reserve(map_bound(self.keys.len())?)?;
        let mut columns: Vec<(ColumnId, Arc<ColumnFilter>)> = Vec::with_capacity(self.keys.len());
        for mut key in self.keys {
            key.flush();
            if let Some(existing) = columns.iter_mut().find(|(id, _)| *id == key.probe) { existing.1 = Arc::new(key.filter); }
            else { columns.push((key.probe, Arc::new(key.filter))); }
        }
        Ok(Some(Arc::new(Pushdowns { columns, _reservation: reservation })))
    }
}
fn key_bound(expr: &Expr) -> usize { match expr { Expr::Binary { op: BinaryOp::And, left, right } => key_bound(left).saturating_add(key_bound(right)), _ => 1 } }
fn collect_keys(expr: &Expr, build: &Schema, probe: &Schema, bitmap_bound: usize, blooms: bool, rows: i64, execution: &Execution, keys: &mut Vec<Key>) -> Result<()> {
    match expr {
        Expr::Binary { op: BinaryOp::And, left, right } => { collect_keys(left, build, probe, bitmap_bound, blooms, rows, execution, keys)?; collect_keys(right, build, probe, bitmap_bound, blooms, rows, execution, keys)?; }
        Expr::Binary { op: BinaryOp::Eq, left, right } => {
            for (expression, candidate) in [(left.as_ref(), right.as_ref()), (right.as_ref(), left.as_ref())] {
                let Expr::Column { id, .. } = candidate else { continue; };
                let Some(field) = probe.fields().iter().find(|field| field.id == *id) else { continue; };
                let iids = IidCandidates::for_probe(field);
                if !blooms && iids.is_none() { continue; }
                let columns = expression.referenced_columns();
                if columns.is_empty() || columns.iter().any(|id| !build.fields().iter().any(|field| field.id == *id)) { continue; }
                let dense = blooms && rows > DENSE_ROWS;
                let pending = if !blooms { 0 } else if dense { DENSE_WORDS * std::mem::size_of::<u64>() } else { PENDING_POSITIONS * std::mem::size_of::<u32>() };
                let reservation = execution.context.memory.reserve(if blooms { bitmap_bound.saturating_add(pending) } else { 256 })?;
                keys.push(Key { expression: expression.clone(), probe: *id, filter: ColumnFilter { bloom: blooms.then(BloomFilter::default), iids, _reservation: reservation },
                    pending: if dense { Pending::Dense(vec![0; DENSE_WORDS]) } else { Pending::Sorted(Vec::with_capacity(if blooms { PENDING_POSITIONS } else { 0 })) }, seen: Default::default(), seen_reservation: None });
                break;
            }
        }
        _ => {}
    }
    Ok(())
}
fn keyword(name: &str) -> TransitValue { TransitValue::Keyword(name.into()) }
fn map_bound(count: usize) -> Result<usize> { count.checked_mul(512).and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error) }
fn size_error() -> rtdb_types::Anomaly { invalid("pushdown-size", "Join filter allocation exceeds addressable memory") }

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_format::metadata::LeafColumns;
    use rtdb_types::StructFields;

    #[test]
    fn byte_bloom_does_not_hide_a_date64_value_in_a_mixed_column() {
        let memory = MemoryBudget::new(2 << 20);
        let mut bloom = BloomFilter::default();
        bloom.insert_value(&Value::DateDay(-1)).unwrap();
        let filters = Pushdowns {
            columns: vec![(ColumnId(0), Arc::new(ColumnFilter {
                bloom: Some(bloom),
                iids: None,
                _reservation: memory.reserve(1024).unwrap(),
            }))],
            _reservation: memory.reserve(1024).unwrap(),
        };
        let page = LeafColumns::new(vec![Value::Struct(StructFields::from([
            ("col-name".into(), Value::Utf8("d".into())),
            ("root-col?".into(), Value::Bool(true)),
            ("bloom".into(), Value::Binary(BloomFilter::default().to_bytes().unwrap())),
        ]))]);
        let stats = page.statistics().unwrap();
        let field = Field { id: ColumnId(0), name: "d".into(), qualifier: None, qualified_only: false, unnamed: false,
            data_type: LogicalType::from_legs([LogicalType::Scalar(ScalarType::Utf8), LogicalType::Scalar(ScalarType::DateMilli)]) };
        assert!(filters.page_matches(&[field], &stats, &memory).unwrap());
    }
}
