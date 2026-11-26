use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_plan::{Expr, Relation, Schema};
use rtdb_types::{Anomaly, LogicalType as L, Result};

use crate::runtime_schema::{EmissionMode, EmittedRelation, declared_parameter_types, expression_bound, schema_bound};
use crate::{CatalogSnapshot, invalid};

/// A prepared query compiled once for each parameter-type signature it runs with, beside what its relation alone decides.
/// The catalog contributes nothing a compilation reads but the plan's own table bindings and the fixed system schemas,
/// so every snapshot runs a signature's compiled plan the same way.
/// Everything kept is charged to `keeper`.
/// At most `KEPT` signatures of each kind are kept, and a new one past that, or one the keeper refuses, evicts the least
/// recently used.
/// Without a keeper, each execution compiles for itself and nothing is kept.
pub(crate) struct CompiledQuery {
    keeper: Option<Arc<MemoryBudget>>,
    plans: Kept<CompiledPlan>,
    declarations: Kept<Declarations>,
    allocation_bound: OnceLock<usize>,
    facts: [OnceLock<bool>; 4],
}

/// One signature's emitted relation, each of whose nodes keeps its operator's template.
struct CompiledPlan { mode: EmissionMode, parameters: Vec<L>, emitted: Arc<EmittedRelation> }

struct Declarations { mode: EmissionMode, width: usize, types: Arc<[Option<L>]>, _reservation: Arc<MemoryReservation> }

/// A yes-or-no answer the relation alone decides.
#[derive(Clone, Copy)]
pub(crate) enum Fact { RequestsToken, FillsRowBatches, UsesRowBatchLimit, ProjectionHasComputation }

impl CompiledQuery {
    /// Signatures kept of each kind: a statement runs with very few parameter-type signatures, and one that sees more
    /// keeps the ones it uses most recently.
    const KEPT: usize = 4;

    pub(crate) fn kept_by(keeper: Option<Arc<MemoryBudget>>) -> Self {
        Self { keeper, plans: Kept::new(Self::KEPT), declarations: Kept::new(Self::KEPT), allocation_bound: OnceLock::new(), facts: Default::default() }
    }

    pub(crate) fn fact(&self, fact: Fact, decide: impl FnOnce() -> bool) -> bool { *self.facts[fact as usize].get_or_init(decide) }

    pub(crate) fn allocation_bound(&self, relation: &Relation) -> Result<usize> {
        if let Some(bound) = self.allocation_bound.get() { return Ok(*bound); }
        let bound = crate::runtime_schema::allocation_bound(relation)?;
        Ok(*self.allocation_bound.get_or_init(|| bound))
    }

    /// The parameter types `relation` declares for `width` parameters.
    pub(crate) fn declared(&self, relation: &Relation, width: usize, mode: EmissionMode) -> Result<Arc<[Option<L>]>> {
        let found = |kept: &Declarations| (kept.mode == mode && kept.width == width).then(|| Arc::clone(&kept.types));
        if let Some(types) = self.declarations.find(&found) { return Ok(types); }
        let types: Arc<[Option<L>]> = declared_parameter_types(relation, width, mode).into();
        let Some(keeper) = &self.keeper else { return Ok(types); };
        let inline = width.checked_mul(std::mem::size_of::<Option<L>>()).ok_or_else(size_error)?;
        let bytes = types.iter().flatten().try_fold(inline, |bytes, ty| bytes.checked_add(ty.retained_bytes()?).ok_or_else(size_error))?;
        let bytes = bytes.checked_add(256).ok_or_else(size_error)?;
        let Some(reservation) = self.declarations.admit(|| keeper.reserve(bytes))? else { return Ok(types); };
        Ok(self.declarations.insert(Declarations { mode, width, types, _reservation: reservation }, &found))
    }

    /// The plan compiled for `parameters` in `mode`, kept where there is a keeper, or compiled in `memory` for this use
    /// alone where there is none or it cannot hold the plan even with nothing else kept.
    pub(crate) fn emit(&self, relation: &Arc<Relation>, catalog: &CatalogSnapshot, parameters: &[L], memory: &Arc<MemoryBudget>,
        mode: EmissionMode) -> Result<Arc<EmittedRelation>> {
        let found = |plan: &CompiledPlan| (plan.mode == mode && plan.parameters.as_slice() == parameters).then(|| Arc::clone(&plan.emitted));
        if let Some(emitted) = self.plans.find(&found) { return Ok(emitted); }
        let unkept = || EmittedRelation::emit(Arc::clone(relation), catalog, parameters, memory, mode);
        let Some(keeper) = &self.keeper else { return unkept(); };
        let Some(emitted) = self.plans.admit(|| EmittedRelation::emit(Arc::clone(relation), catalog, parameters, keeper, mode))? else {
            return unkept();
        };
        emitted.keep_operators(keeper);
        Ok(self.plans.insert(CompiledPlan { mode, parameters: parameters.to_vec(), emitted }, &found))
    }
}

/// At most `capacity` entries, each stamped with its last use.
struct Kept<T> { entries: RwLock<Vec<KeptEntry<T>>>, clock: AtomicU64, capacity: usize }

struct KeptEntry<T> { value: T, used: AtomicU64 }

impl<T> Kept<T> {
    fn new(capacity: usize) -> Self { Self { entries: RwLock::new(Vec::new()), clock: AtomicU64::new(0), capacity } }
    fn read(&self) -> RwLockReadGuard<'_, Vec<KeptEntry<T>>> { self.entries.read().unwrap_or_else(PoisonError::into_inner) }
    fn write(&self) -> RwLockWriteGuard<'_, Vec<KeptEntry<T>>> { self.entries.write().unwrap_or_else(PoisonError::into_inner) }
    fn tick(&self) -> u64 { self.clock.fetch_add(1, Ordering::Relaxed) }

    /// What `found` answers for the first entry it matches, which counts as a use of that entry.
    fn find<R>(&self, found: &impl Fn(&T) -> Option<R>) -> Option<R> {
        self.read().iter().find_map(|entry| {
            let answer = found(&entry.value)?;
            entry.used.store(self.tick(), Ordering::Relaxed);
            Some(answer)
        })
    }

    /// Runs `admit`, evicting the least recently used entry after each refusal by the keeper.
    /// `None` once a refusal leaves nothing to evict; any other error is returned.
    fn admit<A>(&self, mut admit: impl FnMut() -> Result<A>) -> Result<Option<A>> {
        loop {
            match admit() {
                Ok(admitted) => return Ok(Some(admitted)),
                Err(error) if refused(&error) => if !self.evict() { return Ok(None); },
                Err(error) => return Err(error),
            }
        }
    }

    fn evict(&self) -> bool { Self::evict_from(&mut self.write()) }

    fn evict_from(entries: &mut Vec<KeptEntry<T>>) -> bool {
        let Some(oldest) = entries.iter().enumerate().min_by_key(|(_, entry)| entry.used.load(Ordering::Relaxed)).map(|(index, _)| index) else {
            return false;
        };
        entries.swap_remove(oldest);
        true
    }

    /// Keeps `value` unless another caller kept a matching entry first, and answers `found` for whichever is kept.
    fn insert<R>(&self, value: T, found: &impl Fn(&T) -> Option<R>) -> R {
        let mut entries = self.write();
        if let Some(raced) = entries.iter().find_map(|entry| found(&entry.value)) { return raced; }
        let answer = found(&value).expect("a kept entry matches its own signature");
        while entries.len() >= self.capacity && Self::evict_from(&mut entries) {}
        entries.push(KeptEntry { value, used: AtomicU64::new(self.tick()) });
        answer
    }

    #[cfg(test)]
    fn len(&self) -> usize { self.read().len() }
}

fn refused(error: &Anomaly) -> bool { error.code == "rtdb/memory-budget-exceeded" }

fn size_error() -> Anomaly { invalid("compiled-plan-size", "Compiled plan exceeds addressable memory") }

/// What building one emitted node's operator decides from the plan and its types alone, decided once per node.
/// Only a node of a kept compiled plan keeps its template, charged to that plan's keeper.
#[derive(Default)]
pub(crate) struct Operators {
    keeper: OnceLock<Arc<MemoryBudget>>,
    shape: OnceLock<Result<Option<bool>>>,
    state_bound: OnceLock<Result<usize>>,
    template: OnceLock<KeptTemplate>,
}

struct KeptTemplate { template: Arc<OperatorTemplate>, _reservation: Arc<MemoryReservation> }

impl Operators {
    /// Whether this call gave the node its keeper.
    pub(crate) fn keep(&self, keeper: &Arc<MemoryBudget>) -> bool { self.keeper.set(Arc::clone(keeper)).is_ok() }

    pub(crate) fn shape(&self, derive: impl FnOnce() -> Result<Option<bool>>) -> Result<Option<bool>> {
        if let Some(shape) = self.shape.get() { return shape.clone(); }
        let shape = derive();
        self.shape.get_or_init(|| shape).clone()
    }

    pub(crate) fn state_bound(&self, derive: impl FnOnce() -> Result<usize>) -> Result<usize> {
        if let Some(bound) = self.state_bound.get() { return bound.clone(); }
        let bound = derive();
        self.state_bound.get_or_init(|| bound).clone()
    }

    /// The node's template, built by `build` unless kept; one the keeper refuses serves this execution alone.
    pub(crate) fn template(&self, build: impl FnOnce() -> Result<OperatorTemplate>) -> Result<Arc<OperatorTemplate>> {
        if let Some(kept) = self.template.get() { return Ok(Arc::clone(&kept.template)); }
        let template = Arc::new(build()?);
        let Some(keeper) = self.keeper.get() else { return Ok(template); };
        let reservation = match keeper.reserve(template.retained_bytes()?) {
            Ok(reservation) => reservation,
            Err(error) if refused(&error) => return Ok(template),
            Err(error) => return Err(error),
        };
        Ok(Arc::clone(&self.template.get_or_init(|| KeptTemplate { template, _reservation: reservation }).template))
    }

    #[cfg(test)]
    pub(crate) fn kept_template(&self) -> Option<&Arc<OperatorTemplate>> { self.template.get().map(|kept| &kept.template) }
}

/// The parts of an operator that its node's relation and emitted types alone decide.
#[derive(Debug, PartialEq)]
pub(crate) enum OperatorTemplate {
    /// A table scan, with the conjunction of its predicates over its own output where it has any.
    Scan { filter: Option<Predicates> },
    /// A filter over its input, and the filters directly beneath it that one operator applies along with it where nothing observes them apart.
    Filter { single: Predicates, stacked: Option<StackedFilters> },
    /// A projection: the input positions it selects where it only selects columns, otherwise its expressions.
    Project { positions: Option<Arc<[usize]>>, expressions: Projections },
    /// The input's columns followed by the extending expressions.
    Extend { expressions: Projections },
    Other,
}

/// Predicates over a fixed input schema, with whether each has been admitted for it.
#[derive(Debug)]
pub(crate) struct Predicates { pub(crate) schema: Arc<Schema>, pub(crate) stages: Arc<[Expr]>, pub(crate) admissions: Arc<[Admission]> }

impl PartialEq for Predicates {
    fn eq(&self, other: &Self) -> bool { self.schema == other.schema && self.stages == other.stages }
}

impl Predicates {
    pub(crate) fn new(schema: Arc<Schema>, stages: Arc<[Expr]>) -> Self {
        let admissions = stages.iter().map(Admission::of).collect();
        Self { schema, stages, admissions }
    }
    fn retained_bytes(&self) -> Result<usize> {
        let bytes = schema_bound(&self.schema)?;
        let bytes = self.stages.iter().try_fold(bytes, |bytes, stage| bytes.checked_add(expression_bound(stage)?).ok_or_else(size_error))?;
        self.admissions.len().checked_mul(std::mem::size_of::<Admission>()).and_then(|admissions| bytes.checked_add(admissions)).ok_or_else(size_error)
    }
}

/// The filters directly beneath a filter, in the order they apply, each with its local allocation bound in the order they nest.
#[derive(Debug, PartialEq)]
pub(crate) struct StackedFilters { pub(crate) predicates: Predicates, pub(crate) bounds: Arc<[usize]> }

/// Expressions over a fixed input schema, with whether any falls back to row evaluation and whether each has been admitted.
#[derive(Debug)]
pub(crate) struct Projections { pub(crate) schema: Arc<Schema>, pub(crate) expressions: Arc<[Expr]>, pub(crate) scalar_fallback: bool, pub(crate) admissions: Arc<[Admission]> }

impl PartialEq for Projections {
    fn eq(&self, other: &Self) -> bool {
        self.schema == other.schema && self.expressions == other.expressions && self.scalar_fallback == other.scalar_fallback
    }
}

impl Projections {
    pub(crate) fn new(schema: Arc<Schema>, expressions: Arc<[Expr]>, scalar_fallback: bool) -> Self {
        let admissions = expressions.iter().map(Admission::of).collect();
        Self { schema, expressions, scalar_fallback, admissions }
    }
    fn retained_bytes(&self) -> Result<usize> {
        let bytes = schema_bound(&self.schema)?;
        let bytes = self.expressions.iter().try_fold(bytes, |bytes, expression| bytes.checked_add(expression_bound(expression)?).ok_or_else(size_error))?;
        self.admissions.len().checked_mul(std::mem::size_of::<Admission>()).and_then(|admissions| bytes.checked_add(admissions)).ok_or_else(size_error)
    }
}

impl OperatorTemplate {
    fn retained_bytes(&self) -> Result<usize> {
        let bytes = match self {
            Self::Scan { filter: None } | Self::Other => 0,
            Self::Scan { filter: Some(predicates) } => predicates.retained_bytes()?,
            Self::Filter { single, stacked } => {
                let stacked = stacked.as_ref().map_or(Ok(0), |stacked| {
                    stacked.predicates.retained_bytes()?.checked_add(stacked.bounds.len().checked_mul(size_of::<usize>()).ok_or_else(size_error)?).ok_or_else(size_error)
                })?;
                single.retained_bytes()?.checked_add(stacked).ok_or_else(size_error)?
            }
            Self::Project { positions, expressions } => {
                let positions = positions.as_ref().map_or(0, |positions| positions.len().saturating_mul(size_of::<usize>()));
                expressions.retained_bytes()?.checked_add(positions).ok_or_else(size_error)?
            }
            Self::Extend { expressions } => expressions.retained_bytes()?,
        };
        bytes.checked_add(size_of::<Self>() + 256).ok_or_else(size_error)
    }
}

/// Whether an expression has been prepared without error over its compiled plan's input and parameter types, kept only
/// where preparing each of its nodes reads nothing its compiled signature does not fix.
#[derive(Debug)]
pub(crate) struct Admission { invariant: bool, admitted: AtomicBool }

impl Admission {
    pub(crate) fn of(expression: &Expr) -> Self {
        let mut invariant = true;
        expression.visit(&mut |expression| invariant &= prepared_from_signature(expression));
        Self { invariant, admitted: AtomicBool::new(false) }
    }
    pub(crate) fn admitted(&self) -> bool { self.admitted.load(Ordering::Acquire) }
    pub(crate) fn admit(&self) { if self.invariant { self.admitted.store(true, Ordering::Release); } }
}

/// Whether preparing this node, apart from its children, reads only its plan's input types and parameter types.
/// Every variant is listed, so that a new one is classified before any plan can skip its preparation.
fn prepared_from_signature(expression: &Expr) -> bool {
    match expression {
        // A replacement compiles the pattern a parameter's value may supply.
        Expr::RegexReplace { .. } => false,
        // A correlated column takes its type from the outer row.
        Expr::Correlated { .. } => false,
        // A parameter typed nowhere in its plan takes its type from its value where the execution binds no type for it.
        Expr::Parameter { data_type: L::Nothing, .. } => false,
        Expr::Literal(_) | Expr::Column { .. } | Expr::Parameter { .. } | Expr::Unary { .. } | Expr::Binary { .. } | Expr::Call { .. }
        | Expr::Cast { .. } | Expr::Case { .. } | Expr::SimpleCase { .. } | Expr::Between { .. } | Expr::Coalesce(_) | Expr::Field { .. }
        | Expr::Subscript { .. } | Expr::List(_) | Expr::Struct(_) | Expr::RowCopy(_) | Expr::In { .. } | Expr::Like { .. }
        | Expr::DurationLiteral(_) => true,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use rtdb_plan::{ColumnId, Field, RelationNode};
    use rtdb_storage::{MemoryStore, cache::BufferPool};
    use rtdb_types::ScalarType;

    use super::*;

    fn catalog() -> CatalogSnapshot {
        let database = crate::DatabaseSnapshot { partitions: vec![], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
        CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap()
    }

    fn parameter_values(ty: L) -> Arc<Relation> {
        let field = Field { id: ColumnId(0), name: "a".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: ty.clone() };
        let parameter = Expr::Parameter { index: 0, data_type: ty };
        Arc::new(Relation::new(Schema::new(vec![field]).unwrap(), RelationNode::Values { rows: vec![vec![parameter]] }).unwrap())
    }

    #[test]
    fn a_query_keeps_one_compiled_plan_per_mode_and_parameter_types_charged_to_its_keeper() {
        let relation = parameter_values(L::Scalar(ScalarType::I64));
        let (keeper, query) = (MemoryBudget::new(1 << 24), MemoryBudget::new(1 << 24));
        let compiled = CompiledQuery::kept_by(Some(Arc::clone(&keeper)));
        let integers = [L::Scalar(ScalarType::I64)];
        let texts = [L::Scalar(ScalarType::Utf8)];
        let first = compiled.emit(&relation, &catalog(), &integers, &query, EmissionMode::Execution).unwrap();
        let again = compiled.emit(&relation, &catalog(), &integers, &query, EmissionMode::Execution).unwrap();
        assert!(Arc::ptr_eq(&first, &again));
        for (types, mode) in [(&texts, EmissionMode::Execution), (&integers, EmissionMode::ArrowFields)] {
            let kept = compiled.emit(&relation, &catalog(), types, &query, mode).unwrap();
            let fresh = EmittedRelation::emit(Arc::clone(&relation), &catalog(), types, &query, mode).unwrap();
            assert!(!Arc::ptr_eq(&kept, &first));
            assert_eq!(kept.schema(), fresh.schema());
        }
        assert_eq!(query.used_bytes(), 0);
        assert!(keeper.used_bytes() > 0);
        first.operators().template(|| Ok(OperatorTemplate::Other)).unwrap();
        assert!(first.operators().kept_template().is_some());
        let unkept = CompiledQuery::kept_by(None).emit(&relation, &catalog(), &integers, &query, EmissionMode::Execution).unwrap();
        assert!(query.used_bytes() > 0);
        unkept.operators().template(|| Ok(OperatorTemplate::Other)).unwrap();
        assert!(unkept.operators().kept_template().is_none());
        drop(unkept);
        assert_eq!(query.used_bytes(), 0);
    }

    #[test]
    fn a_query_keeps_its_declared_parameter_types_charged_to_its_keeper() {
        let relation = parameter_values(L::Scalar(ScalarType::U32));
        let keeper = MemoryBudget::new(1 << 24);
        let compiled = CompiledQuery::kept_by(Some(Arc::clone(&keeper)));
        let first = compiled.declared(&relation, 1, EmissionMode::Execution).unwrap();
        assert!(Arc::ptr_eq(&first, &compiled.declared(&relation, 1, EmissionMode::Execution).unwrap()));
        assert!(keeper.used_bytes() > 0);
        for (width, mode) in [(1, EmissionMode::Execution), (3, EmissionMode::Execution), (1, EmissionMode::ArrowFields), (0, EmissionMode::Execution),
            (2, EmissionMode::Execution), (2, EmissionMode::ArrowFields)] {
            assert_eq!(&*compiled.declared(&relation, width, mode).unwrap(), declared_parameter_types(&relation, width, mode).as_slice());
        }
        let unkept = CompiledQuery::kept_by(None);
        assert!(!Arc::ptr_eq(&unkept.declared(&relation, 1, EmissionMode::Execution).unwrap(), &unkept.declared(&relation, 1, EmissionMode::Execution).unwrap()));
    }

    fn scalars() -> [L; 5] {
        [ScalarType::I64, ScalarType::F64, ScalarType::Utf8, ScalarType::Bool, ScalarType::I32].map(L::Scalar)
    }

    #[test]
    fn a_signature_past_the_kept_ones_evicts_the_least_recently_used_and_is_kept() {
        let relation = parameter_values(L::Scalar(ScalarType::I64));
        let (keeper, query) = (MemoryBudget::new(1 << 24), MemoryBudget::new(1 << 24));
        let compiled = CompiledQuery::kept_by(Some(Arc::clone(&keeper)));
        let [first, second, third, fourth, fifth] = scalars();
        let emit = |ty: &L| compiled.emit(&relation, &catalog(), std::slice::from_ref(ty), &query, EmissionMode::Execution).unwrap();
        let kept_first = emit(&first);
        let kept_second = emit(&second);
        for ty in [&third, &fourth] { emit(ty); }
        assert!(Arc::ptr_eq(&kept_first, &emit(&first)), "a use refreshes the first signature");
        let kept_fifth = emit(&fifth);
        assert_eq!(compiled.plans.len(), CompiledQuery::KEPT);
        assert!(Arc::ptr_eq(&kept_fifth, &emit(&fifth)), "the fifth signature is kept rather than compiled again");
        assert!(Arc::ptr_eq(&kept_first, &emit(&first)));
        assert!(!Arc::ptr_eq(&kept_second, &emit(&second)), "the least recently used signature was evicted");
        assert_eq!(compiled.plans.len(), CompiledQuery::KEPT);
        assert_eq!(query.used_bytes(), 0, "every signature compiled into the keeper");
    }

    #[test]
    fn a_signature_the_keeper_refuses_evicts_a_kept_one_instead_of_compiling_unkept() {
        let relation = parameter_values(L::Scalar(ScalarType::I64));
        let query = MemoryBudget::new(1 << 24);
        let [integers, doubles, ..] = scalars();
        let measured = MemoryBudget::new(1 << 24);
        drop(CompiledQuery::kept_by(Some(Arc::clone(&measured))).emit(&relation, &catalog(), std::slice::from_ref(&integers), &query, EmissionMode::Execution).unwrap());
        let keeper = MemoryBudget::new(measured.peak_bytes());
        let compiled = CompiledQuery::kept_by(Some(Arc::clone(&keeper)));
        drop(compiled.emit(&relation, &catalog(), std::slice::from_ref(&integers), &query, EmissionMode::Execution).unwrap());
        assert_eq!(compiled.plans.len(), 1);
        let kept = compiled.emit(&relation, &catalog(), std::slice::from_ref(&doubles), &query, EmissionMode::Execution).unwrap();
        assert_eq!(compiled.plans.len(), 1);
        assert!(Arc::ptr_eq(&kept, &compiled.emit(&relation, &catalog(), std::slice::from_ref(&doubles), &query, EmissionMode::Execution).unwrap()));
        assert_eq!(query.used_bytes(), 0);
    }

    #[test]
    fn a_plan_the_keeper_cannot_hold_alone_compiles_for_its_execution() {
        let relation = parameter_values(L::Scalar(ScalarType::I64));
        let (keeper, query) = (MemoryBudget::new(0), MemoryBudget::new(1 << 24));
        let compiled = CompiledQuery::kept_by(Some(keeper));
        let unkept = compiled.emit(&relation, &catalog(), &[L::Scalar(ScalarType::I64)], &query, EmissionMode::Execution).unwrap();
        assert_eq!(compiled.plans.len(), 0);
        assert!(query.used_bytes() > 0);
        drop(unkept);
        assert_eq!(query.used_bytes(), 0);
        assert_eq!(&*compiled.declared(&relation, 1, EmissionMode::Execution).unwrap(), declared_parameter_types(&relation, 1, EmissionMode::Execution).as_slice());
        assert_eq!(compiled.declarations.len(), 0);
    }

    #[test]
    fn a_kept_node_keeps_its_template_charged_to_the_keeper_and_an_unkept_node_builds_it_each_time() {
        let schema = Arc::new(parameter_values(L::Scalar(ScalarType::I64)).schema().clone());
        let build = || Ok(OperatorTemplate::Scan { filter: Some(Predicates::new(Arc::clone(&schema), vec![Expr::Literal(rtdb_types::Value::Bool(true))].into())) });
        let keeper = MemoryBudget::new(1 << 24);
        let kept = Operators::default();
        assert!(kept.keep(&keeper));
        assert!(!kept.keep(&keeper));
        let first = kept.template(build).unwrap();
        let charged = keeper.used_bytes();
        assert!(charged > 0);
        assert!(Arc::ptr_eq(&first, &kept.template(|| panic!("a kept template is built once")).unwrap()));
        assert_eq!(*first, build().unwrap());
        assert_eq!(keeper.used_bytes(), charged);
        let unkept = Operators::default();
        assert!(!Arc::ptr_eq(&unkept.template(build).unwrap(), &unkept.template(build).unwrap()));
        assert!(unkept.kept_template().is_none());
        drop((first, kept));
        assert_eq!(keeper.used_bytes(), 0);
    }

    #[test]
    fn a_template_the_keeper_cannot_hold_serves_one_execution() {
        let schema = Arc::new(parameter_values(L::Scalar(ScalarType::I64)).schema().clone());
        let keeper = MemoryBudget::new(64);
        let operators = Operators::default();
        operators.keep(&keeper);
        let template = operators.template(|| Ok(OperatorTemplate::Scan { filter: Some(Predicates::new(schema, vec![Expr::Literal(rtdb_types::Value::Bool(true))].into())) })).unwrap();
        assert!(matches!(*template, OperatorTemplate::Scan { filter: Some(_) }));
        assert!(operators.kept_template().is_none());
        assert_eq!(keeper.used_bytes(), 0);
    }

    #[test]
    fn decided_shapes_and_bounds_replay_their_errors() {
        let operators = Operators::default();
        let error = invalid("runtime-plan", "Runtime relation child is absent");
        assert_eq!(operators.shape(|| Err(error.clone())).unwrap_err(), error);
        assert_eq!(operators.shape(|| Ok(Some(true))).unwrap_err(), error);
        assert_eq!(operators.state_bound(|| Ok(7)).unwrap(), 7);
        assert_eq!(operators.state_bound(|| Ok(8)).unwrap(), 7);
    }

    #[test]
    fn only_expressions_typed_by_their_signature_alone_stay_admitted() {
        let parameter = Expr::Parameter { index: 0, data_type: L::Scalar(ScalarType::I64) };
        let typed = Admission::of(&parameter);
        assert!(!typed.admitted());
        typed.admit();
        assert!(typed.admitted());
        let correlated = Expr::Correlated { depth: 0, id: ColumnId(0), data_type: L::Scalar(ScalarType::I64) };
        let outer = Admission::of(&Expr::binary(rtdb_plan::BinaryOp::Eq, parameter.clone(), correlated));
        outer.admit();
        assert!(!outer.admitted());
        let untyped = Admission::of(&Expr::binary(rtdb_plan::BinaryOp::Eq, parameter, Expr::Parameter { index: 1, data_type: L::Nothing }));
        untyped.admit();
        assert!(!untyped.admitted(), "a parameter its plan does not type is typed by its value");
    }
}
