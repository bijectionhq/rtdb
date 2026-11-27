use std::{collections::BTreeMap, sync::Arc};

use parking_lot::Mutex;
use rtdb_columnar::MemoryReservation;
use rtdb_plan::{CatalogLookups, SqlNotice};
use rtdb_types::{Anomaly, LogicalType, MemoryBudget, Result};

use crate::{CatalogSnapshot, PreparedQuery};
use crate::shared_plan_entries::{admit, evict_oldest, find, touch};

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
/// A plan is reused for an equal key where its catalog lookups and join estimates still agree.
pub struct SharedPlans {
    memory: Arc<MemoryBudget>,
    entry_bytes: usize,
    entries: Mutex<Vec<(SharedPlanKey, Arc<SharedPlan>)>>,
}

impl SharedPlans {
    const ENTRIES: usize = 128;
    const ENTRY_BYTES: usize = 512 << 10;

    pub fn new(memory: &Arc<MemoryBudget>) -> Self {
        let memory = memory.scope_with_limit((memory.limit_bytes() / 64).min(16 << 20));
        let entry_bytes = Self::ENTRY_BYTES.min(memory.limit_bytes() / 4);
        Self { memory, entry_bytes, entries: Mutex::new(Vec::new()) }
    }

    /// The budget to plan a statement in before `insert`ing it, since a plan's raw expression metadata stays charged to
    /// the budget it was parsed in.
    /// Evicts the least recently used plans until a plan of the largest admitted size fits within the cache's own limit.
    pub fn entry_budget(&self) -> Arc<MemoryBudget> {
        let mut entries = self.entries.lock();
        while self.memory.limit_bytes().saturating_sub(self.memory.used_bytes()) < self.entry_bytes && evict_oldest(&mut entries).is_some() {}
        self.memory.scope_with_limit(self.entry_bytes)
    }

    /// Returns a retained plan for this catalog snapshot and propagates catalog lookup errors.
    pub fn get(&self, key: &SharedPlanKey, catalog: &CatalogSnapshot) -> Result<Option<Arc<SharedPlan>>> {
        let plan = {
            let entries = self.entries.lock();
            let Some(index) = find(&entries, key) else { return Ok(None); };
            Arc::clone(&entries[index].1)
        };
        if !plan.lookups.replay(catalog)? || !plan.query.join_statistics_match(catalog) {
            return Ok(None);
        }
        let mut entries = self.entries.lock();
        if let Some(index) = find(&entries, key) && Arc::ptr_eq(&entries[index].1, &plan) {
            touch(&mut entries, index);
        }
        Ok(Some(plan))
    }

    /// Keeps `query`, planned in an `entry_budget`, evicting least recently used plans while the cache is full or its budget is short.
    /// A plan whose metadata exceeds the whole budget is not kept, and the answer is None; sizing errors propagate.
    pub fn insert(
        &self, key: SharedPlanKey, lookups: Arc<CatalogLookups>, query: PreparedQuery,
        parameter_types: BTreeMap<usize, LogicalType>, notices: Vec<SqlNotice>,
    ) -> Result<Option<Arc<SharedPlan>>> {
        let metadata = || -> Result<usize> {
            let mut bytes = key.retained_bytes()?.checked_add(lookups.retained_bytes()?).ok_or_else(size_error)?;
            for ty in parameter_types.values() { bytes = bytes.checked_add(ty.retained_bytes()?).and_then(|n| n.checked_add(256)).ok_or_else(size_error)?; }
            for notice in &notices { bytes = bytes.checked_add(notice.message.capacity()).and_then(|n| n.checked_add(64)).ok_or_else(size_error)?; }
            Ok(bytes)
        };
        let bytes = metadata()?;
        let mut entries = self.entries.lock();
        if let Some(index) = find(&entries, &key) { entries.remove(index); }
        let _metadata = loop {
            if let Some(reservation) = self.memory.try_reserve(bytes) { break reservation; }
            if evict_oldest(&mut entries).is_none() { return Ok(None); }
        };
        let plan = Arc::new(SharedPlan { lookups, query, parameter_types, notices, _metadata });
        admit(&mut entries, key, Arc::clone(&plan), Self::ENTRIES);
        Ok(Some(plan))
    }
}

fn size_error() -> Anomaly { Anomaly::incorrect("rtdb.query/shared-plan-size", "Shared plan retained size exceeds usize").with_kind(rtdb_types::AnomalyKind::ProgramLimitExceeded) }
