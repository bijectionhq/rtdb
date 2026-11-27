use std::{collections::BTreeSet, sync::Arc};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::{bloom::BloomFilter, transit::{TransitValue, encode_msgpack}};
use rtdb_plan::{BinaryOp, ColumnId, Expr, Field, NamedExpr, Relation, RelationNode, Schema};
use rtdb_types::{EntityId, Iid, LogicalType, Result, ScalarType, Value};
use crate::{columnar::ColumnBatch, context::Execution, invalid, join_key::BorrowedListKey, scalar_key::ScalarKey};

struct IidEntries {
    values: BTreeSet<Iid>,
    limit: usize,
    reservation: Option<Arc<MemoryReservation>>,
}

struct ExactIids {
    values: Box<[Iid]>,
    _reservation: Option<Arc<MemoryReservation>>,
}

impl IidEntries {
    const MAX_CANDIDATES: usize = 1024;

    fn new(limit: usize) -> Self {
        Self { values: BTreeSet::new(), limit, reservation: None }
    }

    fn insert(&mut self, iid: Iid, memory: &Arc<MemoryBudget>) -> Result<bool> {
        if self.values.contains(&iid) { return Ok(true); }
        let len = self.values.len();
        if len == self.limit { return Ok(false); }
        if len == 0 || len.is_power_of_two() {
            let bytes = (len * 2).max(1).min(self.limit) * (256 + size_of::<Iid>());
            if let Some(reservation) = &mut self.reservation {
                Arc::get_mut(reservation).expect("The IID builder owns its reservation").grow_to(bytes)?;
            } else {
                self.reservation = Some(memory.reserve(bytes)?);
            }
        }
        self.values.insert(iid);
        Ok(true)
    }

    fn finish(self) -> Result<ExactIids> {
        let Self { values, mut reservation, .. } = self;
        let values: Box<[Iid]> = values.into_iter().collect();
        if let Some(reservation) = &mut reservation {
            Arc::get_mut(reservation).expect("The IID builder owns its reservation")
                .shrink_to(std::mem::size_of_val(values.as_ref()))?;
        }
        Ok(ExactIids { values, _reservation: reservation })
    }
}

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

enum IidCandidates<Entries> {
    Raw(Entries),
    Entity(EntityIdDomain, Entries),
    UnresolvedEntity,
    LimitExceeded(usize),
}

impl IidCandidates<IidEntries> {
    fn for_probe(field: &Field, limit: usize) -> Option<Self> {
        match field.name.as_str() {
            "_iid" => Some(Self::Raw(IidEntries::new(limit))),
            "_id" => Some(EntityIdDomain::of_type(&field.data_type)
                .map_or(Self::UnresolvedEntity, |domain| Self::Entity(domain, IidEntries::new(limit)))),
            _ => None,
        }
    }

    fn insert(&mut self, value: &Value, scope: &rtdb_expr::EvaluationScope, memory: &Arc<MemoryBudget>) -> Result<bool> {
        let (iid, entries) = match self {
            Self::LimitExceeded(_) => return Ok(true),
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
        if !entries.insert(iid, memory)? {
            *self = Self::LimitExceeded(entries.limit + 1);
        }
        Ok(true)
    }

    fn insert_scalar(&mut self, key: ScalarKey<'_>, memory: &Arc<MemoryBudget>) -> Result<bool> {
        let (iid, entries) = match (&mut *self, key) {
            (Self::LimitExceeded(_), _) => return Ok(true),
            (Self::Entity(EntityIdDomain::Integer, entries), ScalarKey::Signed(value)) => (EntityId::Integer(value).iid(), entries),
            (Self::Entity(EntityIdDomain::String, entries), ScalarKey::Text(value)) => (EntityId::string_iid(value), entries),
            (Self::Entity(EntityIdDomain::Keyword, entries), ScalarKey::Keyword(value)) => (EntityId::keyword_iid(value), entries),
            (Self::Entity(EntityIdDomain::Uuid, entries), ScalarKey::Uuid(value)) => (Iid(*value), entries),
            (Self::Raw(entries), ScalarKey::FixedBytes(value)) => {
                let Ok(bytes) = value.try_into() else { return Ok(false); };
                (Iid(bytes), entries)
            }
            _ => return Ok(false),
        };
        if !entries.insert(iid, memory)? {
            *self = Self::LimitExceeded(entries.limit + 1);
        }
        Ok(true)
    }

    fn finish(self) -> Result<IidCandidates<ExactIids>> {
        Ok(match self {
            Self::Raw(entries) => IidCandidates::Raw(entries.finish()?),
            Self::Entity(domain, entries) => IidCandidates::Entity(domain, entries.finish()?),
            Self::UnresolvedEntity => IidCandidates::UnresolvedEntity,
            Self::LimitExceeded(lower_bound) => IidCandidates::LimitExceeded(lower_bound),
        })
    }
}

impl IidCandidates<ExactIids> {
    fn entries_for(&self, field: &Field, canonical_entity_iids: bool) -> Option<&[Iid]> {
        match self {
            Self::Raw(entries) if field.name == "_iid" => Some(&entries.values),
            Self::Entity(domain, entries) if field.name == "_id"
                && (canonical_entity_iids || entries.values.is_empty())
                && EntityIdDomain::of_type(&field.data_type) == Some(*domain) => Some(&entries.values),
            _ => None,
        }
    }

    fn count_for(&self, field: &Field, canonical_entity_iids: bool) -> Option<usize> {
        if matches!(self, Self::UnresolvedEntity) && field.name == "_id" {
            Some(0)
        } else {
            self.entries_for(field, canonical_entity_iids).map(<[Iid]>::len)
        }
    }

    fn distinct_keys(&self) -> (usize, bool) {
        match self {
            Self::Raw(entries) | Self::Entity(_, entries) => (entries.values.len(), true),
            Self::UnresolvedEntity => (0, true),
            Self::LimitExceeded(lower_bound) => (*lower_bound, false),
        }
    }
}

struct ColumnFilter<Entries = ExactIids> {
    bloom: Option<BloomFilter>,
    iids: Option<IidCandidates<Entries>>,
    _reservation: Arc<MemoryReservation>,
}

pub(crate) struct Pushdowns {
    columns: Vec<(ColumnId, Arc<ColumnFilter>)>,
    _reservation: Arc<MemoryReservation>,
}

pub(crate) struct JoinIidEvidence {
    pub build_distinct: u64,
    pub complete: bool,
    pub admitted: u64,
}

/// Borrows a task's candidate set for probes in nondecreasing IID order.
pub(crate) struct SortedIidProbe<'a>(&'a [Iid]);

impl SortedIidProbe<'_> {
    pub fn matches(&mut self, iid: Iid) -> bool {
        while let Some((&candidate, remaining)) = self.0.split_first() && candidate < iid {
            self.0 = remaining;
        }
        self.0.first() == Some(&iid)
    }
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
        self.iid_candidates(schema, canonical_entity_iids).is_none_or(|iids| iids.binary_search(&iid).is_ok())
    }
    pub fn path_matches(&self, schema: &[Field], lower: Iid, upper: Iid, canonical_entity_iids: bool) -> bool {
        self.iid_candidates(schema, canonical_entity_iids).is_none_or(|iids| {
            iids.get(iids.partition_point(|iid| *iid < lower)).is_some_and(|iid| *iid <= upper)
        })
    }
    pub fn sorted_iid_probe(&self, schema: &[Field], canonical_entity_iids: bool, lower: Iid) -> Option<SortedIidProbe<'_>> {
        let iids = self.iid_candidates(schema, canonical_entity_iids)?;
        Some(SortedIidProbe(&iids[iids.partition_point(|iid| *iid < lower)..]))
    }
    fn iid_candidates(&self, schema: &[Field], canonical_entity_iids: bool) -> Option<&[Iid]> {
        self.iid_filter(schema, canonical_entity_iids).filter(|iids| !iids.is_empty())
    }
    fn iid_filter(&self, schema: &[Field], canonical_entity_iids: bool) -> Option<&[Iid]> {
        ["_iid", "_id"].into_iter().find_map(|name| {
            let field = schema.iter().find(|field| field.name == name)?;
            self.columns.iter().find(|(id, _)| *id == field.id)?.1.iids.as_ref()?.entries_for(field, canonical_entity_iids)
        })
    }
    pub fn iid_evidence(&self, schema: &[Field], canonical_entity_iids: bool, fixed_iids: bool) -> Option<JoinIidEvidence> {
        let mut evidence = None;
        for field in schema.iter().filter(|field| matches!(field.name.as_str(), "_iid" | "_id")) {
            let Some((_, filter)) = self.columns.iter().find(|(id, _)| *id == field.id) else { continue; };
            let evidence = evidence.get_or_insert(JoinIidEvidence { build_distinct: 0, complete: true, admitted: 0 });
            let (count, complete) = filter.iids.as_ref().map_or((0, false), IidCandidates::distinct_keys);
            evidence.build_distinct = evidence.build_distinct.saturating_add(count as u64);
            evidence.complete &= complete;
        }
        if !fixed_iids && let Some(evidence) = &mut evidence {
            evidence.admitted = self.iid_candidates(schema, canonical_entity_iids).map_or(0, |iids| iids.len() as u64);
        }
        evidence
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

/// A build key's filters while the build side is read; its Bloom positions are buffered and added in bulk.
struct Key { expression: Expr, probe: ColumnId, filter: ColumnFilter<IidEntries>, pending: Pending, recorded: RecordedIntegers }

/// Integer build keys already added with these Bloom hashes, so a repeated key is not hashed into the filter again.
#[derive(Default)]
struct RecordedIntegers { keys: std::collections::HashSet<(i64, i32, i32)>, charge: Option<Arc<MemoryReservation>> }

impl RecordedIntegers {
    const CHARGE_STEP: usize = 1024;

    /// Whether this is the first time the key is recorded.
    fn record(&mut self, key: (i64, i32, i32), memory: &Arc<MemoryBudget>) -> Result<bool> {
        if self.keys.contains(&key) { return Ok(false); }
        if self.keys.len().is_multiple_of(Self::CHARGE_STEP) {
            let bytes = (self.keys.len() + Self::CHARGE_STEP).checked_mul(2 * size_of::<(i64, i32, i32)>()).ok_or_else(size_error)?;
            match &mut self.charge {
                Some(charge) => Arc::get_mut(charge).expect("The recorded keys own their charge").grow_to(bytes)?,
                None => self.charge = Some(memory.reserve(bytes)?),
            }
        }
        self.keys.insert(key);
        Ok(true)
    }
}
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
    pub fn new(condition: Option<&Expr>, build: &Schema, probe: &Schema, probe_relation: &Relation, rows: i64, blooms: bool, execution: &Execution) -> Result<Self> {
        let count = condition.map_or(0, key_bound);
        let bound = map_bound(count)?.checked_add(condition.map(crate::runtime_schema::expression_bound).transpose()?.unwrap_or(0)).ok_or_else(size_error)?;
        let reservation = execution.context.memory.reserve(bound)?;
        let mut keys = Vec::new();
        let bitmap_bound = usize::try_from(rows).ok().and_then(|rows| rows.checked_mul(160)).and_then(|bytes| bytes.checked_add(16_384)).unwrap_or(1 << 20).min(1 << 20);
        if let Some(condition) = condition {
            let iid_limit = iid_candidate_limit(probe_rows(probe_relation, &execution.context.catalog));
            collect_keys(condition, build, probe, bitmap_bound, blooms, rows, iid_limit, execution, &mut keys)?;
        }
        Ok(Self { keys, _reservation: reservation })
    }
    pub fn insert_arrow(&mut self, schema: &Schema, batch: &ColumnBatch, row: usize, memory: &Arc<MemoryBudget>) -> Result<()> {
        for key in &mut self.keys {
            if key.filter.bloom.is_none() { continue; }
            let Expr::Column { id, .. } = &key.expression else {
                return Err(invalid("pushdown-key", "Arrow join filter requires a column key"));
            };
            let column = schema.position(*id).and_then(|position| batch.columns.get(position))
                .ok_or_else(|| invalid("pushdown-key", "Build key column is outside its batch"))?;
            if let Some(((xxhash, murmur3), scalar)) = crate::set_keys::direct_bloom_hashes(column, row) {
                if let Some(ScalarKey::Signed(value)) = scalar && !key.recorded.record((value, xxhash, murmur3), memory)? { continue; }
                key.add(rtdb_format::bloom::hashes(xxhash, murmur3));
                let other_zero = match scalar {
                    Some(ScalarKey::Signed(0)) => Some(-0.0),
                    Some(ScalarKey::Float(value)) if value == 0.0 => Some(-value),
                    _ => None,
                };
                if let Some(zero) = other_zero {
                    let zero = ScalarKey::Float(zero);
                    key.add(rtdb_format::bloom::hashes(zero.hash() as i32, zero.murmur3() as i32));
                }
                if let Some(iids) = &mut key.filter.iids {
                    let supported = match scalar {
                        Some(ScalarKey::Null) => true,
                        Some(scalar) => iids.insert_scalar(scalar, memory)?,
                        None => false,
                    };
                    if !supported { key.filter.iids = None; }
                }
                continue;
            }
            if let Some(list) = BorrowedListKey::read(column, row) {
                let (xxhash, murmur3) = list.hashes::<true>();
                key.add(rtdb_format::bloom::hashes(xxhash, murmur3));
                if matches!(list, BorrowedListKey::Values { .. })
                    && !matches!(key.filter.iids, Some(IidCandidates::LimitExceeded(_))) {
                    key.filter.iids = None;
                }
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
            let filter = Arc::new(ColumnFilter {
                bloom: key.filter.bloom,
                iids: key.filter.iids.map(IidCandidates::finish).transpose()?,
                _reservation: key.filter._reservation,
            });
            if let Some(existing) = columns.iter_mut().find(|(id, _)| *id == key.probe) { existing.1 = filter; }
            else { columns.push((key.probe, filter)); }
        }
        Ok(Some(Arc::new(Pushdowns { columns, _reservation: reservation })))
    }
}

fn probe_rows(mut relation: &Relation, catalog: &crate::CatalogSnapshot) -> Option<u64> {
    loop {
        relation = match relation.node() {
            RelationNode::Scan(scan) => return crate::optimize::table_rows(catalog, &scan.table.name),
            RelationNode::Filter { input, .. } | RelationNode::Project { input, .. }
            | RelationNode::Rename { input, .. } | RelationNode::Extend { input, .. } => input,
            _ => return None,
        };
    }
}

fn iid_candidate_limit(probe_rows: Option<u64>) -> usize {
    let cap = IidEntries::MAX_CANDIDATES as u64;
    probe_rows.map_or(cap, |rows| rows.saturating_sub(1).clamp(1, cap)) as usize
}

fn key_bound(expr: &Expr) -> usize { match expr { Expr::Binary { op: BinaryOp::And, left, right } => key_bound(left).saturating_add(key_bound(right)), _ => 1 } }
fn collect_keys(expr: &Expr, build: &Schema, probe: &Schema, bitmap_bound: usize, blooms: bool, rows: i64, iid_limit: usize, execution: &Execution, keys: &mut Vec<Key>) -> Result<()> {
    match expr {
        Expr::Binary { op: BinaryOp::And, left, right } => { collect_keys(left, build, probe, bitmap_bound, blooms, rows, iid_limit, execution, keys)?; collect_keys(right, build, probe, bitmap_bound, blooms, rows, iid_limit, execution, keys)?; }
        Expr::Binary { op: BinaryOp::Eq, left, right } => {
            for (expression, candidate) in [(left.as_ref(), right.as_ref()), (right.as_ref(), left.as_ref())] {
                let Expr::Column { id, .. } = candidate else { continue; };
                let Some(field) = probe.fields().iter().find(|field| field.id == *id) else { continue; };
                let iids = IidCandidates::for_probe(field, iid_limit);
                if !blooms && iids.is_none() { continue; }
                let columns = expression.referenced_columns();
                if columns.is_empty() || columns.iter().any(|id| !build.fields().iter().any(|field| field.id == *id)) { continue; }
                let dense = blooms && rows > DENSE_ROWS;
                let pending = if !blooms { 0 } else if dense { DENSE_WORDS * std::mem::size_of::<u64>() } else { PENDING_POSITIONS * std::mem::size_of::<u32>() };
                let reservation = execution.context.memory.reserve(if blooms { bitmap_bound.saturating_add(pending) } else { 256 })?;
                keys.push(Key { expression: expression.clone(), probe: *id, filter: ColumnFilter { bloom: blooms.then(BloomFilter::default), iids, _reservation: reservation },
                    pending: if dense { Pending::Dense(vec![0; DENSE_WORDS]) } else { Pending::Sorted(Vec::with_capacity(if blooms { PENDING_POSITIONS } else { 0 })) },
                    recorded: RecordedIntegers::default() });
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
    use super::{BloomFilter, ColumnBatch, ColumnFilter, ColumnId, EntityId, Field, Iid, IidCandidates, IidEntries, Key,
        LogicalType, MemoryBudget, Pending, PushdownBuilder, Pushdowns, RecordedIntegers, ScalarType, Schema, SortedIidProbe,
        Value, iid_candidate_limit};
    use rtdb_format::metadata::LeafColumns;
    use rtdb_types::StructFields;
    use std::sync::Arc;

    #[test]
    fn a_repeated_integer_key_is_recorded_once_per_bloom_hashing() {
        let memory = MemoryBudget::new(1 << 20);
        let mut recorded = RecordedIntegers::default();
        assert!(recorded.record((7, 1, 2), &memory).unwrap());
        assert!(!recorded.record((7, 1, 2), &memory).unwrap());
        assert!(recorded.record((7, 3, 4), &memory).unwrap(), "the same value hashed as another width still marks its positions");
        assert!(memory.used_bytes() > 0);
        drop(recorded);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn candidate_limit_uses_probe_rows_when_known_and_preserves_the_hard_bound() {
        assert_eq!(iid_candidate_limit(Some(0)), 1);
        assert_eq!(iid_candidate_limit(Some(1)), 1);
        assert_eq!(iid_candidate_limit(Some(3)), 2);
        assert_eq!(iid_candidate_limit(Some(9)), 8);
        assert_eq!(iid_candidate_limit(Some(u64::MAX)), IidEntries::MAX_CANDIDATES);
        assert_eq!(iid_candidate_limit(None), IidEntries::MAX_CANDIDATES);
    }

    #[test]
    fn sorted_iid_probe_reuses_each_matching_candidate_until_the_next_iid() {
        let iid = |byte| Iid([byte; 16]);
        let memory = MemoryBudget::new(4096);
        let mut entries = IidEntries::new(3);
        for candidate in [iid(7), iid(2), iid(5), iid(5)] {
            assert!(entries.insert(candidate, &memory).unwrap());
        }
        let exact = entries.finish().unwrap();
        assert_eq!(exact.values.as_ref(), &[iid(2), iid(5), iid(7)]);
        assert_eq!(memory.used_bytes(), 3 * size_of::<Iid>());
        let mut probe = SortedIidProbe(&exact.values);
        for (byte, matches) in [(1, false), (2, true), (2, true), (4, false), (5, true), (7, true), (8, false)] {
            assert_eq!(probe.matches(iid(byte)), matches);
        }
        drop(exact);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn numeric_zero_join_bloom_covers_both_persisted_zero_encodings() {
        let memory = MemoryBudget::new(1 << 20);
        let positive = BloomFilter::value_positions(&Value::F64(0.0)).unwrap();
        let negative = BloomFilter::value_positions(&Value::F64(-0.0)).unwrap();
        assert_ne!(positive, negative);

        for value in [Value::I64(0), Value::F32(-0.0), Value::F64(0.0)] {
            let field = Field { id: ColumnId(0), name: "k".into(), qualifier: None, qualified_only: false,
                unnamed: false, data_type: value.logical_type() };
            let schema = Schema::new(vec![field.clone()]).unwrap();
            let mut builder = PushdownBuilder {
                keys: vec![Key {
                    expression: field.expression(), probe: field.id,
                    filter: ColumnFilter { bloom: Some(BloomFilter::default()), iids: None,
                        _reservation: memory.reserve(1024).unwrap() },
                    pending: Pending::Sorted(Vec::new()),
                    recorded: RecordedIntegers::default(),
                }],
                _reservation: memory.reserve(1024).unwrap(),
            };
            let mut persisted = BloomFilter::default();
            persisted.insert_value(&value).unwrap();
            assert_ne!(persisted.contains_positions(&positive), persisted.contains_positions(&negative));

            let batch = ColumnBatch::settled(vec![crate::columnar::Column::Values(vec![value])], 1, memory.reserve(4096).unwrap()).unwrap();
            builder.insert_arrow(&schema, &batch, 0, &memory).unwrap();
            let filters = builder.finish(&memory).unwrap().unwrap();
            let bloom = filters.columns[0].1.bloom.as_ref().unwrap();
            assert!(bloom.contains_positions(&positive));
            assert!(bloom.contains_positions(&negative));
            assert!(bloom.intersects(&persisted));
        }
    }

    #[test]
    fn arrow_keyword_bloom_retains_the_iid_candidate_without_owning_the_spelling() {
        use arrow_array::{ArrayRef, StringArray};
        use crate::columnar::Column;

        let memory = MemoryBudget::new(16 * 1024);
        let spelling = "key".repeat(32 * 1024);
        let logical = LogicalType::Scalar(ScalarType::Keyword);
        let array = Arc::new(StringArray::from(vec![spelling.as_str()])) as ArrayRef;
        let arrow = Arc::new(rtdb_columnar::to_field("_id", &logical).unwrap());
        let input = Field { id: ColumnId(0), name: "_id".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: logical.clone() };
        let schema = Schema::new(vec![input.clone()]).unwrap();
        let custody = MemoryBudget::new(1 << 20).reserve(array.get_array_memory_size() + 4096).unwrap();
        let batch = ColumnBatch::settled(vec![Column::Arrow { array, field: arrow, logical: Some(logical) }], 1, custody).unwrap();
        assert!(batch.columns[0].bound(0).unwrap() > memory.limit_bytes());
        let mut builder = PushdownBuilder {
            keys: vec![Key {
                expression: input.expression(),
                probe: input.id,
                filter: ColumnFilter {
                    bloom: Some(BloomFilter::default()),
                    iids: IidCandidates::for_probe(&input, IidEntries::MAX_CANDIDATES),
                    _reservation: memory.reserve(1024).unwrap(),
                },
                pending: Pending::Sorted(Vec::new()),
                recorded: RecordedIntegers::default(),
            }],
            _reservation: memory.reserve(1024).unwrap(),
        };

        builder.insert_arrow(&schema, &batch, 0, &memory).unwrap();
        let filters = builder.finish(&memory).unwrap().unwrap();
        assert!(filters.needs_entity_iid_proof(schema.fields()));
        assert!(filters.sorted_iid_probe(schema.fields(), false, Iid::MIN).is_none());
        let candidates = filters.iid_filter(schema.fields(), true).unwrap();
        assert_eq!(candidates, &[EntityId::keyword_iid(&spelling)]);
    }

    #[test]
    fn borrowed_join_list_bloom_hashes_selected_cells_without_admitting_a_decoded_list() {
        use crate::columnar::Column;

        for fixed in [false, true] {
            let memory = MemoryBudget::new(16 << 10);
            let values = vec![Value::Utf8("key".repeat(32 * 1024)), Value::Null];
            let list = if fixed { Value::FixedSizeList(values.into()) } else { Value::list(values) };
            let logical = list.logical_type().maybe();
            let field = Field { id: ColumnId(0), name: "_iid".into(), qualifier: None, qualified_only: false,
                unnamed: false, data_type: logical.clone() };
            let schema = Schema::new(vec![field.clone()]).unwrap();
            let array = rtdb_columnar::build_array(&logical, &[list.clone(), Value::Null]).unwrap();
            let custody = MemoryBudget::new(1 << 20).reserve(array.get_array_memory_size() + 4096).unwrap();
            let source = ColumnBatch::settled(vec![Column::Arrow {
                array, field: Arc::new(rtdb_columnar::to_field("_iid", &logical).unwrap()), logical: Some(logical),
            }], 2, custody).unwrap();
            let selected = ColumnBatch::take_rows(&[&source], &[(0, 1), (0, 0)], &[0], &memory).unwrap();
            assert!(selected.columns[0].bound(1).unwrap() > memory.limit_bytes());
            let mut builder = PushdownBuilder {
                keys: vec![Key {
                    expression: field.expression(), probe: field.id,
                    filter: ColumnFilter { bloom: Some(BloomFilter::default()), iids: IidCandidates::for_probe(&field, 1),
                        _reservation: memory.reserve(1024).unwrap() },
                    pending: Pending::Sorted(Vec::new()),
                    recorded: RecordedIntegers::default(),
                }],
                _reservation: memory.reserve(1024).unwrap(),
            };
            builder.insert_arrow(&schema, &selected, 0, &memory).unwrap();
            assert!(matches!(builder.keys[0].filter.iids, Some(IidCandidates::Raw(_))));
            builder.insert_arrow(&schema, &selected, 1, &memory).unwrap();
            let filters = builder.finish(&memory).unwrap().unwrap();
            assert!(filters.iid_filter(schema.fields(), true).is_none());
            let mut expected = BloomFilter::default();
            expected.insert_value(&Value::Null).unwrap();
            expected.insert_value(&list).unwrap();
            assert_eq!(filters.columns[0].1.bloom.as_ref(), Some(&expected));
        }
    }

    #[test]
    fn large_join_drops_the_entire_iid_filter_and_keeps_adding_to_the_bloom() {
        use arrow_array::Int64Array;
        use crate::columnar::Column;

        let memory = MemoryBudget::new(2 << 20);
        let logical = LogicalType::Scalar(ScalarType::I64);
        let field = Field { id: ColumnId(0), name: "_id".into(), qualifier: None, qualified_only: false, unnamed: false,
            data_type: logical.clone() };
        let schema = Schema::new(vec![field.clone()]).unwrap();
        let last = IidEntries::MAX_CANDIDATES as i64 + 1;
        let values = (0..last).chain([0, last]).collect::<Vec<_>>();
        let len = values.len();
        let batch = ColumnBatch::settled(vec![Column::Arrow {
            array: Arc::new(Int64Array::from(values)),
            field: Arc::new(rtdb_columnar::to_field("_id", &logical).unwrap()),
            logical: Some(logical),
        }], len, memory.reserve(64 << 10).unwrap()).unwrap();
        let mut builder = PushdownBuilder {
            keys: vec![Key {
                expression: field.expression(),
                probe: field.id,
                filter: ColumnFilter { bloom: Some(BloomFilter::default()), iids: IidCandidates::for_probe(&field, IidEntries::MAX_CANDIDATES),
                    _reservation: memory.reserve(128 << 10).unwrap() },
                pending: Pending::Sorted(Vec::new()),
                recorded: RecordedIntegers::default(),
            }],
            _reservation: memory.reserve(1024).unwrap(),
        };

        for row in 0..IidEntries::MAX_CANDIDATES {
            builder.insert_arrow(&schema, &batch, row, &memory).unwrap();
        }
        builder.insert_arrow(&schema, &batch, len - 2, &memory).unwrap();
        assert!(matches!(&builder.keys[0].filter.iids, Some(IidCandidates::Entity(_, entries)) if entries.values.len() == IidEntries::MAX_CANDIDATES));
        builder.insert_arrow(&schema, &batch, IidEntries::MAX_CANDIDATES, &memory).unwrap();
        builder.insert_arrow(&schema, &batch, len - 1, &memory).unwrap();

        let filters = builder.finish(&memory).unwrap().unwrap();
        assert!(!filters.needs_entity_iid_proof(schema.fields()));
        let evidence = filters.iid_evidence(schema.fields(), true, false).unwrap();
        assert_eq!(evidence.build_distinct, IidEntries::MAX_CANDIDATES as u64 + 1);
        assert!(!evidence.complete);
        assert_eq!(evidence.admitted, 0);
        for value in [0, last - 1, last, last + 1] {
            assert!(filters.iid_matches(schema.fields(), EntityId::Integer(value).iid(), true));
        }
        let mut last_bloom = BloomFilter::default();
        last_bloom.insert_value(&Value::I64(last)).unwrap();
        assert!(filters.columns[0].1.bloom.as_ref().unwrap().intersects(&last_bloom));
    }

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
