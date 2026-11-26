use std::collections::HashMap;
use std::sync::Arc;

use rtdb_columnar::{MemoryBudget, MemoryReservation};

use super::{PlannedMutation, mutation_query};

/// Mutations a resolver planned for earlier transactions, by statement text, so that a statement submitted again is
/// neither planned nor optimized again. A transaction reuses one only while its prepare options match and its own
/// catalog answers the planning's lookups the same, as an operation already does from one argument row to the next.
///
/// A kept plan retains no transaction's memory: only plans that retain nothing their planning reserved are kept, and
/// the cache's own scope of the resolver's budget is charged for the plan, its prepared query and its lookups.
pub(crate) struct MutationPlans {
    memory: Arc<MemoryBudget>,
    plans: std::sync::Mutex<Plans>,
}

/// A kept plan, its lookups' charge, and when it was last used.
struct Kept { planned: PlannedMutation, _lookups: Arc<MemoryReservation>, used: u64 }

#[derive(Default)]
struct Plans {
    entries: HashMap<Box<str>, Kept>,
    clock: u64,
}

impl MutationPlans {
    /// Statements longer than this are planned for their transaction alone.
    pub(crate) const MAXIMUM_SQL_BYTES: usize = 4096;
    const MAXIMUM_PLANS: usize = 256;

    pub(crate) fn new(memory: &Arc<MemoryBudget>) -> Arc<Self> {
        Arc::new(Self { memory: memory.scope(), plans: Default::default() })
    }

    fn plans(&self) -> std::sync::MutexGuard<'_, Plans> { self.plans.lock().unwrap_or_else(std::sync::PoisonError::into_inner) }

    pub(crate) fn get(&self, sql: &str) -> Option<PlannedMutation> {
        let mut plans = self.plans();
        plans.clock += 1;
        let clock = plans.clock;
        let kept = plans.entries.get_mut(sql)?;
        kept.used = clock;
        Some(kept.planned.clone())
    }

    /// Keeps `planned`, which still holds `retained_bytes` its planning reserved, with its input prepared; the least
    /// recently used plan makes way. A plan that holds memory or literal rows, or a statement too long to key, is not kept.
    pub(crate) fn remember(&self, sql: &str, planned: &PlannedMutation, retained_bytes: usize) {
        if retained_bytes != 0 || sql.len() > Self::MAXIMUM_SQL_BYTES || !shareable(&planned.mutation) { return; }
        // The prepared query's charge, twice its relation's bound, covers the source relation kept beside it.
        let kept = (|| {
            let query = mutation_query(&planned.mutation, &self.memory)?;
            let lookups = self.memory.reserve(planned.lookups.retained_bytes()?)?;
            rtdb_types::Result::Ok(Kept { planned: PlannedMutation { query, ..planned.clone() }, _lookups: lookups, used: 0 })
        })();
        let Ok(mut kept) = kept else { return; };
        let evicted = {
            let mut plans = self.plans();
            plans.clock += 1;
            kept.used = plans.clock;
            let evicted = if plans.entries.len() >= Self::MAXIMUM_PLANS && !plans.entries.contains_key(sql) {
                let oldest = plans.entries.iter().min_by_key(|(_, kept)| kept.used).map(|(sql, _)| sql.clone());
                oldest.and_then(|oldest| plans.entries.remove(&oldest))
            } else {
                None
            };
            let replaced = plans.entries.insert(sql.into(), kept);
            (evicted, replaced)
        };
        drop(evicted);
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize { self.plans().entries.len() }
}

/// Whether a plan may outlive its transaction: a put, delete or erase whose input carries no literal pages.
fn shareable(mutation: &rtdb_sql::Mutation) -> bool {
    fn literal(relation: &rtdb_plan::Relation) -> bool {
        use rtdb_plan::RelationNode as N;
        matches!(relation.node(), N::LiteralPages { .. } | N::ArrowLiteralPages { .. })
            || relation.children().into_iter().any(|child| literal(child))
    }
    match mutation {
        rtdb_sql::Mutation::Put { input, .. } | rtdb_sql::Mutation::Delete { input, .. } | rtdb_sql::Mutation::Erase { input, .. } => !literal(input),
        _ => false,
    }
}
