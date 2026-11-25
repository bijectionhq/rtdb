use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use rtdb_plan::{BinaryOp, Expr};
use rtdb_query::{CandidateIids, CatalogSnapshot, DatabaseSnapshot, QueryContext, QueryOptions, ScanAccelerator, ScanRequest, ScanSelection};
use rtdb_types::{Iid, Result, Value};

use crate::{
    definition::{self, CatalogDefinition, VectorDefinition},
    rows::{Partition, UnitProbe},
    shortlist::{Budget, Cut, Round, Shortlist, declined},
};

/// The opt-in predicate a scan carries to take the approximate route: `APPROXIMATE_NEAREST(column, probe, limit)`.
/// Evaluated on a row it holds where the column is not null, so a scan no accelerator narrows ranks exactly.
pub const PREDICATE: &str = "approximate_nearest";

/// Narrows a nearest-neighbour scan to the shortlist of an approximate index (vector.allium: ApproximateRead).
/// The scan resolves and filters every candidate at its cut and the query reranks them on their full-precision vectors,
/// so erased, deleted and superseded versions never reach a result.
/// The scan takes the exact route whenever this declines: no definition, a temporal range, an IID scope small enough to rank exactly,
/// a frontier above `max_uncovered_bytes`, or more than `max_candidates` before enough rows pass the filter.
pub struct VectorAccelerator {
    pub max_uncovered_bytes: i64,
    pub max_candidates: usize,
    pub exact_scope: usize,
}

impl Default for VectorAccelerator {
    fn default() -> Self {
        let budget = Budget::default();
        Self { max_uncovered_bytes: budget.max_uncovered_bytes, max_candidates: budget.max_candidates, exact_scope: 4_096 }
    }
}

/// What the scan asked of the approximate route.
struct Asked { definition: CatalogDefinition, probe: UnitProbe, limit: usize }

#[async_trait]
impl ScanAccelerator for VectorAccelerator {
    fn accepts_arrow_iids(&self) -> bool { true }

    async fn candidate_iids(&self, request: &ScanRequest<'_>) -> Result<Option<BTreeSet<Iid>>> {
        Ok(self.selected(request).await?.map(|iids| iids.iter().collect()))
    }

    async fn select_candidates(&self, request: &ScanRequest<'_>) -> Result<Option<ScanSelection>> {
        Ok(self.selected(request).await?.map(|iids| ScanSelection { iids, scores: None }))
    }
}

impl VectorAccelerator {
    async fn selected(&self, request: &ScanRequest<'_>) -> Result<Option<CandidateIids>> {
        match self.narrow(request).await {
            Err(error) if declined(&error) => Ok(None),
            result => result,
        }
    }

    async fn narrow(&self, request: &ScanRequest<'_>) -> Result<Option<CandidateIids>> {
        let Some(asked) = self.asked(request).await? else { return Ok(None) };
        let scope = request.iid_scope();
        if scope.is_some_and(|scope| scope.len() <= self.exact_scope.max(asked.limit)) { return Ok(None); }
        let partition = partition_filter(request, &asked.definition).await?;
        let cut = Cut { snapshot: request.snapshot, storage: request.storage, memory: request.memory, cancellation: request.cancellation, bounds: request.bounds };
        let budget = Budget { max_uncovered_bytes: self.max_uncovered_bytes, max_candidates: self.max_candidates };
        let Some(mut shortlist) = Shortlist::open(cut, &asked.definition, asked.probe, asked.limit, partition, scope, budget).await? else { return Ok(None) };
        loop {
            let Some(Round { candidates, complete }) = shortlist.round(cut, scope).await? else { return Ok(None) };
            let Some(visible) = request.verifier.retain_visible(&candidates).await? else { return Ok(Some(candidates)) };
            if visible.len() >= asked.limit || complete { return Ok(Some(visible)); }
            // Too few candidates passed the filter: widen the probe and the shortlist until it is satisfied or the budget is spent.
            if !shortlist.widen() { return Ok(None); }
        }
    }

    async fn asked(&self, request: &ScanRequest<'_>) -> Result<Option<Asked>> {
        let scan = request.scan;
        let mut found = None;
        for predicate in &scan.predicates { conjuncts(predicate, &mut |expression| {
            if found.is_none() && let Expr::Call { name, args, .. } = expression
                && name.eq_ignore_ascii_case(PREDICATE) && let [Expr::Column { id, .. }, probe, limit] = args.as_slice()
                && let Some(field) = scan.columns.iter().find(|field| field.id == *id) {
                found = Some((field.name.as_str(), probe, limit));
            }
        }); }
        let Some((column, probe, limit)) = found else { return Ok(None) };
        let (Some(probe), Some(limit)) = (request.constants.constant(probe).await?, request.constants.constant(limit).await?) else { return Ok(None) };
        let Some(limit) = row_count(&limit) else { return Ok(None) };
        let table = &scan.table.name.table;
        let definition = if table.is_owned_event_data() {
            let catalog = CatalogSnapshot::new(request.database, BTreeMap::from([(request.database.to_owned(), DatabaseSnapshot {
                partitions: request.partitions.to_vec(), storage: Arc::clone(request.storage),
            })]), false)?;
            let context = QueryContext::new(catalog, Arc::clone(request.memory), request.current_time, "UTC", QueryOptions::default(), request.cancellation)?;
            definition::owned_definition(context, table, column).await?
        } else {
            definition::sql_definition_for(request, table, column).await?
        };
        let Some(definition) = definition else { return Ok(None) };
        let Some(probe) = UnitProbe::from_value(&probe, definition.dimensions, request.memory)? else { return Ok(None) };
        Ok(Some(Asked { definition, probe, limit }))
    }
}

/// The partition every eligible row shares, where the scan fixes each partition column by equality to a constant.
async fn partition_filter(request: &ScanRequest<'_>, definition: &VectorDefinition) -> Result<Option<Partition>> {
    if definition.partition.is_empty() { return Ok(Some(Partition::whole())); }
    let scan = request.scan;
    let mut equalities = Vec::new();
    for predicate in &scan.predicates { conjuncts(predicate, &mut |expression| {
        if let Expr::Binary { op: BinaryOp::Eq, left, right } = expression {
            let column = |side: &Expr| match side {
                Expr::Column { id, .. } => scan.columns.iter().find(|field| field.id == *id).map(|field| field.name.as_str()),
                _ => None,
            };
            let binding = column(left).map(|name| (name, right.as_ref()))
                .or_else(|| column(right).map(|name| (name, left.as_ref())));
            if let Some((name, operand)) = binding
                && definition.partition.iter().any(|column| column == name)
            {
                equalities.push((name, operand));
            }
        }
    }); }
    let mut fixed = BTreeMap::new();
    for (name, expression) in equalities {
        if fixed.contains_key(name) { continue; }
        if let Some(value) = request.constants.constant(expression).await? { fixed.insert(name, value); }
    }
    let mut parts = Vec::with_capacity(definition.partition.len());
    for column in &definition.partition {
        let Some(value) = fixed.remove(column.as_str()) else { return Ok(None) };
        if matches!(value, Value::Null | Value::Undefined) { return Ok(None); }
        parts.push(Some(std::borrow::Cow::Owned(value)));
    }
    Partition::of(parts).map(Some).or_else(|error| if error.code == "rtdb.vector/partition-type" { Ok(None) } else { Err(error) })
}

fn conjuncts<'a>(expression: &'a Expr, visit: &mut impl FnMut(&'a Expr)) {
    match expression {
        Expr::Binary { op: BinaryOp::And, left, right } => { conjuncts(left, visit); conjuncts(right, visit); }
        expression => visit(expression),
    }
}

fn row_count(value: &Value) -> Option<usize> {
    let count = match value {
        Value::I8(value) => i64::from(*value), Value::I16(value) => i64::from(*value), Value::I32(value) => i64::from(*value),
        Value::I64(value) => *value, Value::U8(value) => i64::from(*value), Value::U16(value) => i64::from(*value),
        Value::U32(value) => i64::from(*value), Value::U64(value) => i64::try_from(*value).ok()?,
        _ => return None,
    };
    usize::try_from(count).ok().filter(|count| *count > 0)
}
