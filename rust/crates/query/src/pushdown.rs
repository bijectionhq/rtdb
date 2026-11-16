use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::{bloom::BloomFilter, transit::{TransitValue, encode_msgpack}};
use rtdb_plan::{BinaryOp, ColumnId, Expr, Field, NamedExpr, Schema};
use rtdb_types::{Iid, Result, Value};
use crate::{context::Execution, invalid};

struct ColumnFilter {
    bloom: Option<BloomFilter>,
    iids: Option<BTreeMap<Iid, Arc<MemoryReservation>>>,
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
    pub fn iid_matches(&self, schema: &[Field], iid: Iid) -> bool {
        self.iid_filter(schema).is_none_or(|iids| iids.is_empty() || iids.contains_key(&iid))
    }
    pub fn path_matches(&self, schema: &[Field], lower: Iid, upper: Iid) -> bool {
        self.iid_filter(schema).is_none_or(|iids| iids.is_empty() || iids.range(lower..=upper).next().is_some())
    }
    fn iid_filter(&self, schema: &[Field]) -> Option<&BTreeMap<Iid, Arc<MemoryReservation>>> {
        ["_iid", "_id"].into_iter().find_map(|name| {
            let field = schema.iter().find(|field| field.name == name)?;
            self.columns.iter().find(|(id, _)| *id == field.id)?.1.iids.as_ref()
        })
    }
    pub fn page_matches(&self, schema: &[Field], columns: &[Value], memory: &Arc<MemoryBudget>) -> Result<bool> {
        for field in schema {
            let Some((_, filter)) = self.columns.iter().find(|(id, _)| *id == field.id) else { continue; };
            let Some(bloom) = &filter.bloom else { continue; };
            let Some(column) = columns.iter().find(|column| matches!(column.field("col-name"), Some(Value::Utf8(name)) if name == &field.name) && column.field("root-col?") == Some(&Value::Bool(true))) else { return Ok(false); };
            if let Some(Value::Binary(bytes)) = column.field("bytes").and_then(|value| value.field("bloom")) {
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
    pub fn describe(&self, schema: &[Field]) -> Result<Value> {
        let mut columns = Vec::new();
        for field in schema {
            let Some((_, filter)) = self.columns.iter().find(|(id, _)| *id == field.id) else { continue; };
            let mut entries = vec![(keyword("column"), TransitValue::String(field.name.clone()))];
            if let Some(bloom) = &filter.bloom { entries.push((keyword("bloom-filter"), TransitValue::Map(vec![(keyword("cardinality"), TransitValue::Integer(bloom.cardinality() as i64))]))); }
            if let Some(iids) = &filter.iids { entries.push((keyword("iids"), TransitValue::Map(vec![(keyword("count"), TransitValue::Integer(iids.len() as i64))]))); }
            columns.push(TransitValue::Map(entries));
        }
        if columns.is_empty() { Ok(Value::Null) } else { encode_msgpack(&TransitValue::Array(columns)).map(Value::Transit) }
    }
}

struct Key { expression: Expr, probe: ColumnId, raw_iid: bool, filter: ColumnFilter }
pub(crate) struct PushdownBuilder { keys: Vec<Key>, _reservation: Arc<MemoryReservation> }
impl PushdownBuilder {
    pub fn new(condition: Option<&Expr>, build: &Schema, probe: &Schema, rows: i64, blooms: bool, execution: &Execution) -> Result<Self> {
        let count = condition.map_or(0, key_bound);
        let bound = map_bound(count)?.checked_add(condition.map(crate::runtime_schema::expression_bound).transpose()?.unwrap_or(0)).ok_or_else(size_error)?;
        let reservation = execution.context.memory.reserve(bound)?;
        let mut keys = Vec::new();
        let bitmap_bound = usize::try_from(rows).ok().and_then(|rows| rows.checked_mul(160)).and_then(|bytes| bytes.checked_add(16_384)).unwrap_or(1 << 20).min(1 << 20);
        if let Some(condition) = condition { collect_keys(condition, build, probe, bitmap_bound, blooms, execution, &mut keys)?; }
        Ok(Self { keys, _reservation: reservation })
    }
    pub async fn insert(&mut self, schema: &Schema, row: &[Value], execution: &Execution) -> Result<()> {
        let scope = execution.scope()?;
        for key in &mut self.keys {
            let Some(bloom) = &mut key.filter.bloom else { continue; };
            let value = execution.evaluate(&scope, &key.expression, schema, row).await?;
            bloom.insert_value(&value)?;
            if !matches!(value, Value::Null | Value::Undefined) {
                if let Some(iids) = &mut key.filter.iids {
                    let iid = if key.raw_iid {
                        let Value::FixedSizeBinary(bytes) = &value else { return Err(invalid("join-iid", "An _iid join key must contain 16 bytes")); };
                        Iid(bytes.as_slice().try_into().map_err(|_| invalid("join-iid", "An _iid join key must contain 16 bytes"))?)
                    } else {
                        scope.reserve(value.retained_bytes()?.checked_mul(3).and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?)?;
                        value.entity_id()?.iid()
                    };
                    if !iids.contains_key(&iid) {
                        let reservation = execution.context.memory.reserve(256)?;
                        iids.insert(iid, reservation);
                    }
                }
            }
        }
        Ok(())
    }
    pub fn finish(self, memory: &Arc<MemoryBudget>) -> Result<Option<Arc<Pushdowns>>> {
        if self.keys.is_empty() { return Ok(None); }
        let reservation = memory.reserve(map_bound(self.keys.len())?)?;
        let mut columns: Vec<(ColumnId, Arc<ColumnFilter>)> = Vec::with_capacity(self.keys.len());
        for key in self.keys {
            if let Some(existing) = columns.iter_mut().find(|(id, _)| *id == key.probe) { existing.1 = Arc::new(key.filter); }
            else { columns.push((key.probe, Arc::new(key.filter))); }
        }
        Ok(Some(Arc::new(Pushdowns { columns, _reservation: reservation })))
    }
}
fn key_bound(expr: &Expr) -> usize { match expr { Expr::Binary { op: BinaryOp::And, left, right } => key_bound(left).saturating_add(key_bound(right)), _ => 1 } }
fn collect_keys(expr: &Expr, build: &Schema, probe: &Schema, bitmap_bound: usize, blooms: bool, execution: &Execution, keys: &mut Vec<Key>) -> Result<()> {
    match expr {
        Expr::Binary { op: BinaryOp::And, left, right } => { collect_keys(left, build, probe, bitmap_bound, blooms, execution, keys)?; collect_keys(right, build, probe, bitmap_bound, blooms, execution, keys)?; }
        Expr::Binary { op: BinaryOp::Eq, left, right } => {
            for (expression, candidate) in [(left.as_ref(), right.as_ref()), (right.as_ref(), left.as_ref())] {
                let Expr::Column { id, .. } = candidate else { continue; };
                let Some(field) = probe.fields().iter().find(|field| field.id == *id) else { continue; };
                let iid = matches!(field.name.as_str(), "_id" | "_iid");
                if !blooms && !iid { continue; }
                let columns = expression.referenced_columns();
                if columns.is_empty() || columns.iter().any(|id| !build.fields().iter().any(|field| field.id == *id)) { continue; }
                let reservation = execution.context.memory.reserve(if blooms { bitmap_bound } else { 256 })?;
                let raw_iid = matches!(expression, Expr::Column { id, .. } if build.fields().iter().any(|field| field.id == *id && field.name == "_iid"));
                keys.push(Key { expression: expression.clone(), probe: *id, raw_iid, filter: ColumnFilter { bloom: blooms.then(BloomFilter::default), iids: iid.then(BTreeMap::new), _reservation: reservation } });
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
