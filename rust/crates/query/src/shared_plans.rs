use std::{collections::BTreeMap, sync::{Arc, atomic::{AtomicU64, Ordering}}};

use parking_lot::Mutex;
use rtdb_columnar::MemoryReservation;
use rtdb_plan::{CatalogLookups, SqlNotice};
use rtdb_sql::{CachedReadSyntax, ParsedStatement};
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

/// Cumulative counters and current charges for a node's read cache.
pub struct SharedPlanCacheStats {
    pub read_preparations: u64,
    pub plan_misses: u64,
    pub syntax_hits: u64,
    pub syntax_misses: u64,
    pub plan_charged_bytes: usize,
    pub syntax_retained_bytes: usize,
}

/// Read plans and resolved syntax shared between a node's sessions.
/// A plan is reused for an equal key where its catalog lookups and join estimates still agree.
pub struct SharedPlans {
    memory: Arc<MemoryBudget>,
    entry_bytes: usize,
    entries: Mutex<Vec<(SharedPlanKey, Arc<SharedPlan>)>>,
    syntax_memory: Arc<MemoryBudget>,
    syntax: Mutex<Vec<Arc<CachedReadSyntax>>>,
    read_preparations: AtomicU64,
    plan_misses: AtomicU64,
    syntax_hits: AtomicU64,
    syntax_misses: AtomicU64,
}

impl SharedPlans {
    const ENTRIES: usize = 128;
    const ENTRY_BYTES: usize = 512 << 10;
    const SYNTAX_ENTRY_BYTES: usize = 128 << 10;

    pub fn new(memory: &Arc<MemoryBudget>) -> Self {
        let memory = memory.scope_with_limit((memory.limit_bytes() / 64).min(16 << 20));
        let syntax_limit = memory.limit_bytes() / 8;
        let syntax_memory = memory.scope_with_limit(syntax_limit);
        let memory = memory.scope_with_limit(memory.limit_bytes() - syntax_limit);
        let entry_bytes = Self::ENTRY_BYTES.min(memory.limit_bytes() / 4);
        Self {
            memory, entry_bytes, entries: Mutex::new(Vec::new()),
            syntax_memory, syntax: Mutex::new(Vec::new()),
            read_preparations: AtomicU64::new(0), plan_misses: AtomicU64::new(0),
            syntax_hits: AtomicU64::new(0), syntax_misses: AtomicU64::new(0),
        }
    }

    /// Counts an initial preparation of a shareable read, including requests that miss the plan cache.
    pub fn record_read_preparation(&self) {
        self.read_preparations.fetch_add(1, Ordering::Relaxed);
    }

    /// Samples node-wide activity without retaining statement identities.
    pub fn stats(&self) -> SharedPlanCacheStats {
        SharedPlanCacheStats {
            read_preparations: self.read_preparations.load(Ordering::Relaxed),
            plan_misses: self.plan_misses.load(Ordering::Relaxed),
            syntax_hits: self.syntax_hits.load(Ordering::Relaxed),
            syntax_misses: self.syntax_misses.load(Ordering::Relaxed),
            plan_charged_bytes: self.memory.used_bytes(),
            syntax_retained_bytes: self.syntax_memory.used_bytes(),
        }
    }

    /// Looks up resolved syntax by complete input, independent of the current binding options.
    pub fn read_syntax(&self, source: &str) -> Option<Arc<CachedReadSyntax>> {
        let mut entries = self.syntax.lock();
        let Some(index) = entries.iter().position(|entry| entry.source() == source) else {
            self.syntax_misses.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        let entry = entries.remove(index);
        let syntax = Arc::clone(&entry);
        entries.push(entry);
        self.syntax_hits.fetch_add(1, Ordering::Relaxed);
        Some(syntax)
    }

    /// Checks eligibility and the per-entry ceiling before extending the syntax lifetime.
    pub fn can_keep_read_syntax(&self, parsed: &ParsedStatement<'_>) -> bool {
        let limit = Self::SYNTAX_ENTRY_BYTES.min(self.syntax_memory.limit_bytes() / 4);
        parsed.cacheable_read_bytes().is_some_and(|bytes| bytes <= limit)
    }

    /// Admits resolved syntax, evicting the least recently used entries from the syntax budget.
    pub fn remember_read_syntax(&self, parsed: &ParsedStatement<'_>) {
        if !self.can_keep_read_syntax(parsed) { return; }
        let mut entries = self.syntax.lock();
        if entries.iter().any(|entry| entry.source() == parsed.source()) { return; }
        if entries.len() == Self::ENTRIES { entries.remove(0); }
        loop {
            if let Some(syntax) = parsed.retain_read_syntax(&self.syntax_memory) {
                entries.push(Arc::new(syntax));
                return;
            }
            if entries.is_empty() {
                *entries = Vec::new();
                return;
            }
            entries.remove(0);
        }
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
            let Some(index) = find(&entries, key) else {
                self.plan_misses.fetch_add(1, Ordering::Relaxed);
                return Ok(None);
            };
            Arc::clone(&entries[index].1)
        };
        if !plan.lookups.replay(catalog)? || !plan.query.join_statistics_match(catalog) {
            self.plan_misses.fetch_add(1, Ordering::Relaxed);
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
