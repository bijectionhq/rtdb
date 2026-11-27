use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::MemoryReservation;
use rtdb_plan::{QuerySettings, SettingValue};
use rtdb_types::{Anomaly, Instant, Result, TimeBasis, Timestamp, TransactionKey, TxId, Value};
use crate::{CatalogSnapshot, QueryContext, invalid};

mod retained_position;

#[async_trait::async_trait]
pub trait SnapshotAdmission: Send + Sync {
    async fn reopen(&self, token: &str, current: &QueryContext) -> Result<SqlSnapshotView>;
}

pub type SnapshotTokenIssuer = dyn Fn(SqlSnapshotView) -> Result<String> + Send + Sync;

/// Exactly the catalog and interpretation admitted for the original execution.
/// Session and request owners are installed afresh when redeeming this view.
#[derive(Clone)]
pub struct SqlSnapshotView {
    pub catalog: CatalogSnapshot,
    pub current_time: Instant,
    pub timezone: String,
    /// The portable token's original database and partition entries, with nanosecond precision.
    pub token_basis: Option<TimeBasis>,
}

impl SqlSnapshotView {
    pub fn database(&self) -> &str { &self.catalog.default_database }

    pub fn retained_bytes(&self) -> Result<usize> {
        let bytes = self.token_basis.as_ref().map(basis_bound).transpose()?.unwrap_or_default();
        self.timezone.capacity().checked_add(catalog_bound(&self.catalog)?)
            .and_then(|capacity| capacity.checked_add(bytes)).ok_or_else(size_error)
    }

    /// The current catalog with each named database's partitions replaced, read at the current clock and zone,
    /// as a redeemed XTDB snapshot token is.
    pub fn from_partitions(current: &QueryContext, partitions: BTreeMap<String, Vec<rtdb_index::Snapshot>>) -> Result<Self> {
        let mut catalog = current.catalog.clone();
        for (name, replaced) in partitions {
            let database = catalog.databases_mut().get_mut(&name)
                .filter(|database| database.partitions.len() == replaced.len())
                .ok_or_else(|| invalid("snapshot-scope", "A redeemed snapshot must replace admitted partitions"))?;
            database.partitions = replaced;
        }
        catalog.reset_sql_snapshot_view();
        Ok(Self { catalog, current_time: current.current_time, timezone: current.timezone.clone(), token_basis: None })
    }
}

pub(crate) struct ReadBasis {
    times: Option<TimeBasis>,
    token_basis: Option<TimeBasis>,
    snapshot_time: Option<Timestamp>,
    clock_fallback: rtdb_types::Instant,
    pub token: Option<String>,
    admitted_token: Option<String>,
    _reservation: Arc<MemoryReservation>,
}

fn size_error() -> Anomaly { invalid("settings-size", "Query setting metadata exceeds addressable memory") }
fn basis_bound(basis: &TimeBasis) -> Result<usize> {
    basis.databases.iter().try_fold(4096usize, |bytes, (name, times)| {
        bytes.checked_add(name.len().checked_mul(8).ok_or_else(size_error)?)
            .and_then(|bytes| times.len().checked_mul(256).and_then(|times| bytes.checked_add(times)))
            .and_then(|bytes| bytes.checked_add(512)).ok_or_else(size_error)
    })
}

fn catalog_bound(catalog: &CatalogSnapshot) -> Result<usize> {
    let mut bytes = 4096usize.checked_add(catalog.default_database.len()).ok_or_else(size_error)?;
    for (name, database) in catalog.database_snapshots().iter() {
        bytes = bytes.checked_add(name.len().checked_mul(8).ok_or_else(size_error)?).and_then(|bytes| database.partitions.len().checked_mul(512).and_then(|size| bytes.checked_add(size))).ok_or_else(size_error)?;
        for partition in &database.partitions { bytes = bytes.checked_add(partition.clone_allocation_bound()?.checked_mul(4).ok_or_else(size_error)?).ok_or_else(size_error)?; }
    }
    Ok(bytes)
}

impl QueryContext {
    /// Retains API read options separately from the captured heads and SQL execution settings.
    /// `clock_fallback` is the engine clock used when a CLOCK_TIME parameter resolves to null.
    pub fn with_read_options(self: Arc<Self>, times: &TimeBasis, snapshot_time: Option<Timestamp>, clock_fallback: impl Into<rtdb_types::Instant>) -> Result<Arc<Self>> {
        let charge = self.memory.reserve(basis_bound(times)?.checked_add(catalog_bound(&self.catalog)?).ok_or_else(size_error)?)?;
        let times = times.clone();
        let token = None;
        let mut context = self.execution_child_with_memory(Arc::clone(&self.memory));
        Arc::get_mut(&mut context).ok_or_else(|| invalid("settings-owner", "New context is unexpectedly shared"))?.read_basis = Some(Arc::new(ReadBasis { times: Some(times), token_basis: None, snapshot_time, clock_fallback: clock_fallback.into(), token, admitted_token: None, _reservation: charge }));
        Ok(context)
    }

    pub fn with_snapshot_token_issuer(mut self: Arc<Self>, issuer: Arc<SnapshotTokenIssuer>) -> Arc<Self> {
        Arc::make_mut(&mut self).snapshot_tokens = Some(issuer);
        self
    }

    pub fn with_snapshot_admission(mut self: Arc<Self>, admission: Arc<dyn SnapshotAdmission>) -> Arc<Self> {
        Arc::make_mut(&mut self).snapshot_admission = Some(admission);
        self
    }

    fn admitted_snapshot_token(&self) -> Option<&str> {
        self.read_basis.as_ref().and_then(|basis| basis.admitted_token.as_deref().or(basis.token.as_deref()))
    }

    pub fn with_snapshot_view(mut self: Arc<Self>, mut view: SqlSnapshotView, token: String) -> Result<Arc<Self>> {
        let option_cap = self.read_basis.as_ref().and_then(|basis| basis.snapshot_time);
        let clock_fallback = self.read_basis.as_ref().map_or(view.current_time, |basis| basis.clock_fallback);
        let mut bytes = token.len().checked_mul(2).and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
        if let Some(times) = &view.token_basis { bytes = bytes.checked_add(basis_bound(times)?).ok_or_else(size_error)?; }
        let charge = self.memory.reserve(bytes)?;
        view.catalog.runtime = self.catalog.runtime.clone();
        let mut recapped = false;
        if let Some(cap) = option_cap {
            view.token_basis = view.token_basis.map(|times| times.capped_at(cap.into()));
            for (name, database) in view.catalog.databases_mut() {
                for (index, partition) in database.partitions.iter_mut().enumerate() {
                    let next = if let Some(times) = &view.token_basis {
                        portable_partition_cap(times, name, index, partition)?
                    } else {
                        partition.system_time_cap().or(partition.tx_basis().map(|head| head.system_time))
                            .map(|time| time.min(cap))
                    };
                    if next != partition.system_time_cap() {
                        *partition = partition.clone().with_system_time_cap(next)?;
                        recapped = true;
                    }
                }
            }
        }
        let context = Arc::make_mut(&mut self);
        context.catalog = view.catalog;
        context.current_time = view.current_time;
        context.timezone = view.timezone;
        let caps = captured_caps(&context.catalog);
        let effective = (!recapped && view.token_basis.is_none()).then(|| token.clone());
        context.read_basis = Some(Arc::new(ReadBasis {
            times: caps, token_basis: view.token_basis, snapshot_time: option_cap, clock_fallback,
            token: effective, admitted_token: Some(token), _reservation: charge,
        }));
        Ok(self)
    }

    pub async fn reopen_sql_snapshot(self: Arc<Self>, settings: &QuerySettings, parameters: rtdb_expr::Parameters<'_>) -> Result<Arc<Self>> {
        let Some(setting) = &settings.snapshot_token else { return Ok(self); };
        match setting_value(setting, parameters)? {
            Value::Null | Value::Undefined => Ok(self),
            Value::Utf8(token) if self.admitted_snapshot_token() == Some(token) => Ok(self),
            Value::Utf8(token) => {
                let admission = self.snapshot_admission.as_ref().ok_or_else(|| invalid("snapshot-unretained", "No SQL snapshot owner is installed"))?;
                let view = admission.reopen(token, &self).await?;
                self.with_snapshot_view(view, token.to_owned())
            }
            _ => Err(invalid("invalid-snapshot-token", "SNAPSHOT_TOKEN requires a string or null")),
        }
    }

    pub fn issue_snapshot_token(&self) -> Result<Option<String>> {
        if let Some(token) = self.snapshot_token() { return Ok(Some(token.to_owned())); }
        let Some(issuer) = &self.snapshot_tokens else { return Ok(None); };
        self.check_cancelled()?;
        let mut catalog = self.catalog.clone();
        catalog.runtime = None;
        catalog.fragmenter = None;
        catalog.fragment_residence = None;
        let token_basis = self.read_basis.as_ref().and_then(|basis| basis.token_basis.clone());
        issuer(SqlSnapshotView { catalog, current_time: self.current_time, timezone: self.timezone.clone(), token_basis }).map(Some)
    }

    pub fn with_exact_snapshot_token(mut self: Arc<Self>, token: String) -> Result<Arc<Self>> {
        let token_basis = self.read_basis.as_ref().and_then(|basis| basis.token_basis.as_ref());
        let mut bytes = token.len().checked_add(256).ok_or_else(size_error)?;
        if let Some(times) = token_basis { bytes = bytes.checked_add(basis_bound(times)?).ok_or_else(size_error)?; }
        let charge = self.memory.reserve(bytes)?;
        let token_basis = token_basis.cloned();
        let snapshot_time = self.read_basis.as_ref().and_then(|basis| basis.snapshot_time);
        let clock_fallback = self.read_basis.as_ref().map_or(self.current_time, |basis| basis.clock_fallback);
        let admitted_token = self.read_basis.as_ref().and_then(|basis| basis.admitted_token.clone());
        let caps = captured_caps(&self.catalog);
        Arc::make_mut(&mut self).read_basis = Some(Arc::new(ReadBasis {
            times: caps, token_basis, snapshot_time, clock_fallback,
            token: Some(token), admitted_token, _reservation: charge,
        }));
        Ok(self)
    }

    /// Caps the captured database at `system_time` and fixes the SQL clock at `current_time`.
    /// The context must have one partition, and a nonempty partition must cover the cut.
    pub fn with_wire_cut(mut self: Arc<Self>, system_time: Timestamp, current_time: Instant) -> Result<Arc<Self>> {
        let database = self.catalog.default_database.clone();
        let [snapshot] = self.catalog.database(&database)?.partitions.as_slice() else {
            return Err(invalid("wire-cut", "The read requires one partition"));
        };
        let cut = snapshot.tx_basis().map(|_| system_time);
        let times = TimeBasis { databases: BTreeMap::from([(database.clone(), vec![cut.map(Into::into)])]) };
        self = self.with_read_options(&times, Some(system_time), current_time)?;
        let context = Arc::make_mut(&mut self);
        let database = context.catalog.databases_mut().get_mut(&database)
            .ok_or_else(|| invalid("wire-cut", "The read requires its captured database"))?;
        let [snapshot] = database.partitions.as_mut_slice() else {
            return Err(invalid("wire-cut", "The read requires one partition"));
        };
        *snapshot = snapshot.clone().with_system_time_cap(cut)?;
        context.current_time = current_time;
        Ok(self)
    }

    pub fn with_wire_snapshot(
        mut self: Arc<Self>, snapshot: rtdb_index::Snapshot, system_time: Timestamp, current_time: Instant,
    ) -> Result<Arc<Self>> {
        let context = Arc::make_mut(&mut self);
        let catalog = &mut context.catalog;
        let default_database = catalog.default_database.clone();
        let database = catalog.databases_mut().get_mut(&default_database)
            .ok_or_else(|| invalid("wire-cut", "The read requires its captured database"))?;
        let physical = snapshot.catalog_cut().is_some();
        database.partitions = vec![snapshot];
        catalog.reset_sql_snapshot_view();
        if !physical {
            catalog.runtime = None;
            catalog.ordered_indexes = None;
        }
        self.with_wire_cut(system_time, current_time)
    }

    /// The system cut an owned read holds: its one partition's cap.
    /// A receiver-clocked read keeps its query clock apart from this cut.
    pub fn owned_held_cut(&self) -> Option<Timestamp> {
        let database = self.catalog.database(&self.catalog.default_database).ok()?;
        let [snapshot] = database.partitions.as_slice() else { return None; };
        snapshot.system_time_cap()
    }

    /// Reuses the captured store generation and lease while lowering one owned read's system cap.
    pub fn with_owned_system_cut(
        &self, system_time: Timestamp, cancellation: &tokio_util::sync::CancellationToken,
    ) -> Result<Arc<Self>> {
        self.check_cancelled()?;
        let original = self.catalog.database(&self.catalog.default_database)?;
        let [snapshot] = original.partitions.as_slice() else {
            return Err(invalid("owned-system-cut", "An owned historical read requires one partition"));
        };
        if snapshot.system_time_cap().is_none_or(|present| system_time > present) {
            return Err(invalid("owned-system-cut", "An owned historical read cannot raise the system cap"));
        }
        let times = TimeBasis { databases: BTreeMap::from([
            (self.catalog.default_database.clone(), vec![Some(system_time.into())]),
        ]) };
        let charge = self.memory.reserve(basis_bound(&times)?.checked_add(catalog_bound(&self.catalog)?)
            .ok_or_else(size_error)?)?;
        let mut context = (*self).clone();
        let default_database = context.catalog.default_database.clone();
        let database = context.catalog.databases_mut().get_mut(&default_database)
            .ok_or_else(|| invalid("owned-system-cut", "An owned historical read requires its captured database"))?;
        database.partitions[0] = snapshot.clone().with_system_time_cap(Some(system_time))?;
        context.current_time = system_time.into();
        context.cancellation = cancellation.child_token();
        context.read_basis = Some(Arc::new(ReadBasis {
            token: None,
            admitted_token: None,
            times: Some(times), token_basis: None, snapshot_time: Some(system_time), clock_fallback: system_time.into(),
            _reservation: charge,
        }));
        Ok(Arc::new(context))
    }

    /// Narrows an owned archive read to the selected original publication.
    pub fn with_owned_transaction_cut(
        &self, through: TransactionKey, cancellation: &tokio_util::sync::CancellationToken,
    ) -> Result<Arc<Self>> {
        self.with_owned_system_cut(through.system_time, cancellation)?
            .with_owned_position_cut(through.tx_id)
    }

    /// The exact position already admitted for an owned archive view.
    pub fn owned_position_cut(&self) -> Option<TxId> {
        self.catalog.owned_transaction_cut
    }

    /// Probes a prefix of the captured history without inventing a SQL time for its endpoint.
    pub fn with_owned_position_cut(mut self: Arc<Self>, through: TxId) -> Result<Arc<Self>> {
        self.check_cancelled()?;
        let original = self.catalog.database(&self.catalog.default_database)?;
        let [snapshot] = original.partitions.as_slice() else {
            return Err(invalid("owned-transaction-cut", "An owned publication requires one captured partition"));
        };
        let Some(position) = retained_position::admit(
            snapshot.tx_basis().map(|basis| basis.tx_id.0),
            self.catalog.owned_transaction_cut.map(|cut| cut.0),
            through.0,
        ) else {
            return Err(invalid("owned-transaction-cut", "The requested publication is above its captured cut"));
        };
        let context = Arc::make_mut(&mut self);
        context.catalog.owned_transaction_cut = Some(TxId(position));
        context.catalog.fragmenter = None;
        context.accelerators = Default::default();
        Ok(self)
    }

    pub(crate) fn apply_settings(self: Arc<Self>, settings: &QuerySettings, parameters: rtdb_expr::Parameters<'_>) -> Result<Arc<Self>> {
        self.check_cancelled()?;
        let mut bytes = catalog_bound(&self.catalog)?;
        if let Some(times) = self.read_basis.as_ref().and_then(|basis| basis.times.as_ref()) { bytes = bytes.checked_add(basis_bound(times)?).ok_or_else(size_error)?; }
        let admitted_basis = self.read_basis.as_ref().and_then(|basis| basis.token_basis.as_ref());
        if let Some(times) = admitted_basis { bytes = bytes.checked_add(basis_bound(times)?).ok_or_else(size_error)?; }
        let setting_token = settings.snapshot_token.as_ref().map(|expr| setting_value(expr, parameters)).transpose()?;
        if let Some(value) = setting_token {
            bytes = bytes.checked_add(value.retained_bytes()?.checked_mul(256).ok_or_else(size_error)?).ok_or_else(size_error)?;
        }
        let memory = self.memory.scope();
        let charge = memory.reserve(bytes)?;
        let fallback = self.read_basis.as_ref().map_or(self.current_time, |basis| basis.clock_fallback);
        let current_time = match settings.clock_time.as_ref() {
            None | Some(SettingValue::Literal(Value::Null | Value::Undefined)) => self.current_time,
            Some(SettingValue::TimestampLiteral(text)) => rtdb_types::parse_sql_timestamp_instant(text, &self.timezone)?,
            Some(expression) => match setting_value(expression, parameters)? {
                Value::Null | Value::Undefined => fallback,
                Value::Utf8(text) => rtdb_types::parse_query_clock(text, &self.timezone)?,
                value => rtdb_expr::query_clock_instant(value, &self.timezone)?,
            },
        };
        let reset_portable = admitted_basis.is_some() && matches!(setting_token, Some(Value::Null | Value::Undefined));
        let mut times = if let Some(value) = setting_token {
            match value {
                Value::Null | Value::Undefined if reset_portable => captured_times(&self.catalog, |partition| partition.tx_basis().map(|head| head.system_time)),
                Value::Null | Value::Undefined => captured_caps(&self.catalog),
                Value::Utf8(token) if self.admitted_snapshot_token() == Some(token) => captured_caps(&self.catalog),
                Value::Utf8(_) => return Err(invalid("snapshot-unretained", "SNAPSHOT_TOKEN must be readmitted before binding")),
                _ => return Err(invalid("invalid-snapshot-token", "SNAPSHOT_TOKEN requires a string or null")),
            }
        } else if let Some(basis) = &self.read_basis { basis.times.clone() }
        else { captured_caps(&self.catalog) };
        validate_basis(times.as_ref(), &self.catalog)?;
        let requested_cap = match &settings.snapshot_time {
            Some(SettingValue::TimestampLiteral(text)) =>
                Some(rtdb_types::parse_sql_timestamp_instant(text, &self.timezone)?),
            Some(expression) => match setting_value(expression, parameters)? {
                Value::Null | Value::Undefined => None,
                Value::Utf8(text) if admitted_basis.is_some() && !matches!(text.as_str(), "infinity" | "-infinity") =>
                    Some(rtdb_types::parse_sql_timestamp_instant(text, &self.timezone)?),
                value @ (Value::Timestamp { .. } | Value::Date(_) | Value::DateDay(_) | Value::DateMilli(_)) if admitted_basis.is_some() =>
                    Some(rtdb_expr::query_clock_instant(value, &self.timezone)?),
                value => Some(rtdb_expr::timestamp_micros(value, &self.timezone)?.into()),
            },
            None => None,
        };
        let option_cap = self.read_basis.as_ref().and_then(|basis| basis.snapshot_time).map(Instant::from);
        let upper = match (option_cap, requested_cap) {
            (Some(option), Some(requested)) => Some(option.min(requested)),
            (option, requested) => option.or(requested),
        };
        let token_basis = admitted_basis.filter(|_| !reset_portable).map(|basis| match upper {
            Some(upper) => basis.capped_at(upper),
            None => basis.clone(),
        });
        let snapshot_time = upper.map(Instant::as_micros).transpose()?;
        if let Some(cap) = snapshot_time { times = times.map(|times| times.capped_at(cap.into())); }
        let visible = |name: &str, index: usize, partition: &rtdb_index::Snapshot| -> Result<Option<Timestamp>> {
            if let Some(basis) = &token_basis { return portable_partition_cap(basis, name, index, partition); }
            let cap = times.as_ref().and_then(|basis| basis.databases.get(name)).and_then(|times| times.get(index)).copied().flatten().map(|time| time.as_micros()).transpose()?;
            Ok(cap.or(snapshot_time).zip(partition.tx_basis()).map(|(cap, head)| cap.min(head.system_time)))
        };
        let mut recapped = false;
        'databases: for (name, database) in self.catalog.database_snapshots().iter() {
            for (index, partition) in database.partitions.iter().enumerate() {
                if visible(name, index, partition)? != partition.system_time_cap() { recapped = true; break 'databases; }
            }
        }
        let token = if recapped || token_basis.as_ref() != admitted_basis { None } else { self.snapshot_token().map(str::to_owned) };
        let admitted_token = self.read_basis.as_ref().and_then(|basis| basis.admitted_token.clone());
        let catalog = if recapped {
            let mut catalog = self.catalog.clone();
            for (name, database) in catalog.databases_mut() {
                for (index, partition) in database.partitions.iter_mut().enumerate() {
                    let visible = visible(name, index, partition)?;
                    *partition = partition.clone().with_system_time_cap(visible)?;
                }
            }
            Some(catalog)
        } else { None };
        let mut context = self.execution_child_with_memory(memory);
        let mutable = Arc::get_mut(&mut context).ok_or_else(|| invalid("settings-owner", "New context is unexpectedly shared"))?;
        if let Some(catalog) = catalog { mutable.catalog = catalog; }
        mutable.current_time = current_time;
        mutable.read_basis = Some(Arc::new(ReadBasis { times, token_basis, snapshot_time, clock_fallback: fallback, token, admitted_token, _reservation: charge }));
        Ok(context)
    }
}

fn setting_value<'a>(expression: &'a SettingValue, parameters: rtdb_expr::Parameters<'a>) -> Result<&'a Value> {
    match expression {
        SettingValue::Literal(value) => Ok(value),
        SettingValue::Parameter { index } => parameters.get(*index)?.ok_or_else(|| invalid("missing-parameter", "Query setting parameter is absent")),
        SettingValue::TimestampLiteral(_) => Err(invalid("invalid-setting-value", "An unadmitted timestamp literal requires CLOCK_TIME resolution")),
        SettingValue::Unevaluated { .. } => Err(invalid("invalid-setting-value", "Query settings resolve literals and parameters, not scalar expressions")),
    }
}

fn captured_caps(catalog: &CatalogSnapshot) -> Option<TimeBasis> {
    captured_times(catalog, rtdb_index::Snapshot::system_time_cap)
}

fn captured_times(catalog: &CatalogSnapshot, time: impl Fn(&rtdb_index::Snapshot) -> Option<Timestamp>) -> Option<TimeBasis> {
    let databases: BTreeMap<_, _> = catalog.database_snapshots().iter().filter_map(|(name, database)| {
        let times: Vec<_> = database.partitions.iter().map(|snapshot| time(snapshot).map(Into::into)).collect();
        times.iter().any(Option::is_some).then(|| (name.clone(), times))
    }).collect();
    (!databases.is_empty()).then_some(TimeBasis { databases })
}

fn portable_partition_cap(basis: &TimeBasis, name: &str, index: usize, partition: &rtdb_index::Snapshot) -> Result<Option<Timestamp>> {
    let Some(head) = partition.tx_basis() else { return Ok(None); };
    let time = basis.databases.get(name).and_then(|times| times.get(index)).copied().flatten();
    Ok(Some(match time {
        Some(time) => time.as_micros()?.min(head.system_time),
        None => head.system_time,
    }))
}

fn validate_basis(basis: Option<&TimeBasis>, catalog: &CatalogSnapshot) -> Result<()> {
    for (name, database) in catalog.database_snapshots().iter() {
        for (index, snapshot) in database.partitions.iter().enumerate() {
            if let Some(requested) = basis.and_then(|basis| basis.databases.get(name)).and_then(|times| times.get(index)).copied().flatten() {
                if !head_covers(snapshot.tx_basis(), requested) {
                    return Err(unindexed_tx(requested, snapshot.tx_basis()));
                }
            }
        }
    }
    Ok(())
}

/// Whether a partition head can serve a read capped at `requested`.
/// A cut at the earliest system time admits no transaction, so every head, including an empty one, covers it.
pub fn head_covers(head: Option<TransactionKey>, requested: Instant) -> bool {
    requested <= Timestamp::MIN.into() || head.is_some_and(|head| requested <= head.system_time.into())
}

/// The anomaly for a read basis whose system time is after a database's latest completed transaction.
pub fn unindexed_tx(requested: Instant, latest: Option<TransactionKey>) -> Anomaly {
    let instant = |time: Instant| time.to_iso_string().unwrap_or_else(|_| format!("{time:?}"));
    let latest_text = latest.map_or_else(|| "nil".to_owned(), |key| format!("#xt/tx-key {{:tx-id {}, :system-time #xt/instant \"{}\"}}", key.tx_id.0, instant(key.system_time.into())));
    let error = Anomaly::incorrect("xtdb/unindexed-tx", format!("system-time ({}) is after the latest completed tx ({latest_text})", instant(requested)));
    let context = || -> Result<Anomaly> {
        use rtdb_types::transit_value::TransitValue as T;
        let latest = match latest {
            None => T::Null,
            Some(key) => T::Tagged { tag: "xtdb/tx-key".into(), representation: Box::new(T::Map(vec![
                (T::Keyword("tx-id".into()), T::Integer(key.tx_id.0)),
                (T::Keyword("system-time".into()), T::instant(key.system_time.into())?),
            ])) },
        };
        error.clone().with_transit_context("latest-completed-tx", &latest)?.with_transit_context("system-time", &T::instant(requested)?)
    };
    context().unwrap_or(error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_index::{PartitionState, TransactionBuffer};
    use rtdb_storage::{MemoryStore, cache::BufferPool};
    use rtdb_types::{Interval, MemoryBudget, StructFields, TableRef};

    #[test]
    fn settings_that_keep_every_partition_cap_share_the_captured_catalog() {
        let memory = MemoryBudget::new(1 << 26);
        let state = PartitionState::empty(Default::default()).unwrap();
        for tx in [1, 2] {
            let mut buffer = TransactionBuffer::new(state.value().snapshot(), TransactionKey { tx_id: TxId(tx), system_time: Timestamp(tx) }, Arc::clone(&memory)).unwrap();
            buffer.put(TableRef::new("public", "docs"), StructFields::from([("_id".into(), Value::I64(tx))]), Interval::from_micros(0, i64::MAX).unwrap()).unwrap();
            state.apply(&buffer.stage().unwrap()).unwrap();
        }
        let database = crate::DatabaseSnapshot { partitions: vec![state.value().snapshot()], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
        let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
        let context = QueryContext::new(catalog, memory, Timestamp(100), "UTC", crate::QueryOptions::default(), &tokio_util::sync::CancellationToken::new()).unwrap();
        let cap = |context: &QueryContext| context.catalog.database_snapshots()["xtdb"].partitions[0].system_time_cap();
        let kept = Arc::clone(&context).apply_settings(&QuerySettings::default(), rtdb_expr::Parameters::Values(&[])).unwrap();
        assert!(Arc::ptr_eq(kept.catalog.database_snapshots(), context.catalog.database_snapshots()));
        assert_eq!((cap(&kept), cap(&context)), (Some(Timestamp(2)), Some(Timestamp(2))));
        let times = TimeBasis { databases: BTreeMap::from([("xtdb".to_owned(), vec![Some(Timestamp(1).into())])]) };
        let earlier = Arc::clone(&context).with_read_options(&times, None, Timestamp(100)).unwrap();
        let capped = Arc::clone(&earlier).apply_settings(&QuerySettings::default(), rtdb_expr::Parameters::Values(&[])).unwrap();
        assert!(!Arc::ptr_eq(capped.catalog.database_snapshots(), earlier.catalog.database_snapshots()));
        assert_eq!((cap(&capped), cap(&earlier)), (Some(Timestamp(1)), Some(Timestamp(2))));
    }

    #[test]
    fn relation_lookups_after_a_partition_swap_answer_from_the_new_snapshot() {
        use rtdb_expr::PgCatalogLookup;
        let memory = MemoryBudget::new(1 << 26);
        let partition = |table: &str| {
            let state = PartitionState::empty(Default::default()).unwrap();
            let key = TransactionKey { tx_id: TxId(1), system_time: Timestamp(1) };
            let mut buffer = TransactionBuffer::new(state.value().snapshot(), key, Arc::clone(&memory)).unwrap();
            buffer.put(TableRef::new("public", table), StructFields::from([("_id".into(), Value::I64(1))]), Interval::from_micros(0, i64::MAX).unwrap()).unwrap();
            state.apply(&buffer.stage().unwrap()).unwrap();
            state.value().snapshot()
        };
        let database = crate::DatabaseSnapshot { partitions: vec![partition("before")], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
        let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
        let context = QueryContext::new(catalog, Arc::clone(&memory), Timestamp(100), "UTC", crate::QueryOptions::default(), &tokio_util::sync::CancellationToken::new()).unwrap();
        let scope = rtdb_expr::EvaluationScope::new(&memory).unwrap();
        let before = context.catalog.relation_oid(Some("public"), "before", &scope).unwrap();
        assert!(before.is_some());

        let view = SqlSnapshotView::from_partitions(&context, BTreeMap::from([("xtdb".into(), vec![partition("after")])])).unwrap();
        assert_eq!(view.catalog.relation_oid(Some("public"), "before", &scope).unwrap(), None);
        let after = view.catalog.relation_oid(Some("public"), "after", &scope).unwrap().unwrap();
        assert_eq!(view.catalog.relation_name(after, &scope).unwrap().as_deref(), Some("after"));
        assert_eq!(context.catalog.relation_oid(Some("public"), "before", &scope).unwrap(), before);
        assert_eq!(context.catalog.relation_oid(Some("public"), "after", &scope).unwrap(), None);
    }

    #[tokio::test]
    async fn snapshot_token_combines_with_clock_time_and_snapshot_time() {
        struct StaticAdmission(BTreeMap<String, rtdb_index::Snapshot>);
        #[async_trait::async_trait]
        impl SnapshotAdmission for StaticAdmission {
            async fn reopen(&self, token: &str, current: &QueryContext) -> Result<SqlSnapshotView> {
                let snapshot = self.0.get(token).cloned().ok_or_else(|| invalid("snapshot-unretained", "unknown token"))?;
                SqlSnapshotView::from_partitions(current, BTreeMap::from([("xtdb".into(), vec![snapshot])]))
            }
        }

        let memory = MemoryBudget::new(1 << 26);
        let state = PartitionState::empty(Default::default()).unwrap();
        for tx in [1, 2] {
            let mut buffer = TransactionBuffer::new(state.value().snapshot(), TransactionKey { tx_id: TxId(tx), system_time: Timestamp(tx) }, Arc::clone(&memory)).unwrap();
            buffer.put(TableRef::new("public", "docs"), StructFields::from([("_id".into(), Value::I64(tx))]), Interval::from_micros(0, i64::MAX).unwrap()).unwrap();
            state.apply(&buffer.stage().unwrap()).unwrap();
        }
        let head = state.value().snapshot();
        let cap1 = head.clone().with_system_time_cap(Some(Timestamp(1))).unwrap();
        let cap2 = head.clone().with_system_time_cap(Some(Timestamp(2))).unwrap();
        let admission = Arc::new(StaticAdmission(BTreeMap::from([("tok1".into(), cap1), ("tok2".into(), cap2)])));
        let database = crate::DatabaseSnapshot { partitions: vec![head], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
        let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
        let base = QueryContext::new(catalog, memory, Timestamp(100), "UTC", crate::QueryOptions::default(), &tokio_util::sync::CancellationToken::new())
            .unwrap()
            .with_snapshot_admission(admission);

        // SETTING SNAPSHOT_TOKEN + CLOCK_TIME keeps the token and updates the query clock.
        let settings = QuerySettings {
            snapshot_token: Some(SettingValue::Literal(Value::Utf8("tok2".into()))),
            clock_time: Some(SettingValue::TimestampLiteral("2025-01-01T00:00:00Z".into())),
            snapshot_time: None,
        };
        let reopened = Arc::clone(&base).reopen_sql_snapshot(&settings, rtdb_expr::Parameters::Values(&[])).await.unwrap();
        let applied = reopened.apply_settings(&settings, rtdb_expr::Parameters::Values(&[])).unwrap();
        assert_eq!(applied.snapshot_token(), Some("tok2"));
        assert_eq!(applied.current_time, rtdb_types::parse_sql_timestamp_instant("2025-01-01T00:00:00Z", "UTC").unwrap());
        assert_eq!(applied.catalog.database_snapshots()["xtdb"].partitions[0].system_time_cap(), Some(Timestamp(2)));

        // SETTING SNAPSHOT_TOKEN + SNAPSHOT_TIME lowers the cap and clears the cached token so it re-encodes.
        let capped_settings = QuerySettings {
            snapshot_token: Some(SettingValue::Literal(Value::Utf8("tok2".into()))),
            clock_time: Some(SettingValue::TimestampLiteral("2025-01-01T00:00:00Z".into())),
            snapshot_time: Some(SettingValue::Literal(Value::Timestamp { value: 1, unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()) })),
        };
        let reopened = Arc::clone(&base).reopen_sql_snapshot(&capped_settings, rtdb_expr::Parameters::Values(&[])).await.unwrap();
        let applied = reopened.apply_settings(&capped_settings, rtdb_expr::Parameters::Values(&[])).unwrap();
        assert_eq!(applied.snapshot_token(), None);
        assert_eq!(applied.catalog.database_snapshots()["xtdb"].partitions[0].system_time_cap(), Some(Timestamp(1)));
        assert_eq!(applied.current_time, rtdb_types::parse_sql_timestamp_instant("2025-01-01T00:00:00Z", "UTC").unwrap());

        // Read-option SNAPSHOT_TIME applied alongside a reopened token lowers the cap on admission and stays idempotent on reopen.
        let with_cap = Arc::clone(&base).with_read_options(&TimeBasis::default(), Some(Timestamp(1)), Timestamp(100)).unwrap();
        let reopened = with_cap.reopen_sql_snapshot(&settings, rtdb_expr::Parameters::Values(&[])).await.unwrap();
        assert_eq!(reopened.snapshot_token(), None);
        assert_eq!(reopened.catalog.database_snapshots()["xtdb"].partitions[0].system_time_cap(), Some(Timestamp(1)));
        let reopened_again = reopened.reopen_sql_snapshot(&settings, rtdb_expr::Parameters::Values(&[])).await.unwrap();
        let applied = reopened_again.apply_settings(&settings, rtdb_expr::Parameters::Values(&[])).unwrap();
        assert_eq!(applied.snapshot_token(), None);
        assert_eq!(applied.catalog.database_snapshots()["xtdb"].partitions[0].system_time_cap(), Some(Timestamp(1)));
    }
}
