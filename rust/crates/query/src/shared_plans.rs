use std::{collections::{BTreeMap, VecDeque}, sync::Arc};

use parking_lot::Mutex;
use rtdb_columnar::MemoryReservation;
use rtdb_plan::{CatalogLookups, SqlNotice};
use rtdb_types::{Anomaly, LogicalType, MemoryBudget, Result};

use crate::PreparedQuery;

/// Everything besides its catalog lookups that planning a SQL statement reads.
#[derive(Clone, Debug, PartialEq)]
pub struct SharedPlanKey {
    pub source: String,
    pub search_path: Vec<String>,
    pub parameter_types: BTreeMap<usize, LogicalType>,
    pub timezone: String,
    pub generate_series_end_exclusive: bool,
}

impl SharedPlanKey {
    fn retained_bytes(&self) -> Result<usize> {
        let mut bytes = self.source.capacity().checked_add(self.timezone.capacity()).and_then(|n| n.checked_add(1024)).ok_or_else(size_error)?;
        for path in &self.search_path { bytes = bytes.checked_add(path.capacity()).and_then(|n| n.checked_add(64)).ok_or_else(size_error)?; }
        for ty in self.parameter_types.values() { bytes = bytes.checked_add(ty.retained_bytes()?).and_then(|n| n.checked_add(256)).ok_or_else(size_error)?; }
        Ok(bytes)
    }
}

/// A plan, what its planning read from the catalog, and what binding inferred alongside it.
pub struct SharedPlan {
    pub lookups: Arc<CatalogLookups>,
    pub query: PreparedQuery,
    pub parameter_types: BTreeMap<usize, LogicalType>,
    pub notices: Vec<SqlNotice>,
    _metadata: Arc<MemoryReservation>,
}

/// Plans shared between a node's sessions.
/// A plan is reused for an equal key only where its catalog lookups replay the same answers.
pub struct SharedPlans {
    memory: Arc<MemoryBudget>,
    entries: Mutex<VecDeque<(SharedPlanKey, Arc<SharedPlan>)>>,
}

impl SharedPlans {
    const ENTRIES: usize = 128;
    const ENTRY_BYTES: usize = 512 << 10;

    pub fn new(memory: &Arc<MemoryBudget>) -> Self {
        Self { memory: memory.scope_with_limit((memory.limit_bytes() / 64).min(16 << 20)), entries: Mutex::new(VecDeque::new()) }
    }

    /// The budget to plan a statement in before `insert`ing it, since a plan's raw expression metadata stays charged to
    /// the budget it was parsed in.
    pub fn entry_budget(&self) -> Arc<MemoryBudget> { self.memory.scope_with_limit(Self::ENTRY_BYTES) }

    pub fn get(&self, key: &SharedPlanKey) -> Option<Arc<SharedPlan>> {
        let mut entries = self.entries.lock();
        let index = entries.iter().position(|(cached, _)| cached == key)?;
        let entry = entries.remove(index)?;
        let plan = Arc::clone(&entry.1);
        entries.push_back(entry);
        Some(plan)
    }

    /// Keeps `query`, planned in an `entry_budget`, evicting the least recently used plan when full.
    /// A plan whose metadata exceeds the budget is not kept.
    pub fn insert(
        &self, key: SharedPlanKey, lookups: Arc<CatalogLookups>, query: PreparedQuery,
        parameter_types: BTreeMap<usize, LogicalType>, notices: Vec<SqlNotice>,
    ) -> Option<Arc<SharedPlan>> {
        let metadata = || -> Result<usize> {
            let mut bytes = key.retained_bytes()?.checked_add(lookups.retained_bytes()?).ok_or_else(size_error)?;
            for ty in parameter_types.values() { bytes = bytes.checked_add(ty.retained_bytes()?).and_then(|n| n.checked_add(256)).ok_or_else(size_error)?; }
            for notice in &notices { bytes = bytes.checked_add(notice.message.capacity()).and_then(|n| n.checked_add(64)).ok_or_else(size_error)?; }
            Ok(bytes)
        };
        let _metadata = metadata().and_then(|bytes| self.memory.reserve(bytes)).ok()?;
        let plan = Arc::new(SharedPlan { lookups, query, parameter_types, notices, _metadata });
        let mut entries = self.entries.lock();
        entries.retain(|(cached, _)| *cached != key);
        if entries.len() == Self::ENTRIES { entries.pop_front(); }
        entries.push_back((key, Arc::clone(&plan)));
        Some(plan)
    }
}

fn size_error() -> Anomaly { Anomaly::incorrect("rtdb.query/shared-plan-size", "Shared plan retained size exceeds usize") }
