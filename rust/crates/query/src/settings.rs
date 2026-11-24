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
}

impl SqlSnapshotView {
    pub fn database(&self) -> &str { &self.catalog.default_database }

    pub fn retained_bytes(&self) -> Result<usize> {
        self.timezone.capacity().checked_add(catalog_bound(&self.catalog)?).ok_or_else(size_error)
    }

    pub fn from_exact_partition(
        current: &QueryContext, snapshot: rtdb_index::Snapshot, current_time: Instant,
        suffix: Option<Arc<dyn Send + Sync>>,
        protected_storage: Option<Arc<rtdb_storage::cache::BufferPool>>,
    ) -> Result<Self> {
        let mut catalog = current.catalog.clone();
        let database = Arc::make_mut(&mut catalog.databases).get_mut(&catalog.default_database)
            .filter(|database| database.partitions.len() == 1)
            .ok_or_else(|| invalid("snapshot-scope", "An exact partition must replace one admitted partition"))?;
        database.partitions[0] = snapshot;
        let original_storage = protected_storage.map(|store| std::mem::replace(&mut database.storage, store));
        catalog.reset_sql_snapshot_view(suffix, original_storage);
        Ok(Self { catalog, current_time, timezone: current.timezone.clone() })
    }
}

pub(crate) struct ReadBasis {
    times: Option<TimeBasis>,
    snapshot_time: Option<Timestamp>,
    clock_fallback: rtdb_types::Instant,
    pub token: Option<String>,
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
    for (name, database) in catalog.databases.iter() {
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
        let mut context = self.execution_child();
        Arc::get_mut(&mut context).ok_or_else(|| invalid("settings-owner", "New context is unexpectedly shared"))?.read_basis = Some(Arc::new(ReadBasis { times: Some(times), snapshot_time, clock_fallback: clock_fallback.into(), token, _reservation: charge }));
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

    pub fn with_snapshot_view(mut self: Arc<Self>, mut view: SqlSnapshotView, token: String) -> Result<Arc<Self>> {
        view.catalog.runtime = self.catalog.runtime.clone();
        let context = Arc::make_mut(&mut self);
        context.catalog = view.catalog;
        context.current_time = view.current_time;
        context.timezone = view.timezone;
        self.with_exact_snapshot_token(token)
    }

    pub async fn reopen_sql_snapshot(self: Arc<Self>, settings: &QuerySettings, parameters: rtdb_expr::Parameters<'_>) -> Result<Arc<Self>> {
        let Some(setting) = &settings.snapshot_token else { return Ok(self); };
        match setting_value(setting, parameters)? {
            Value::Null | Value::Undefined => Ok(self),
            Value::Utf8(token) if self.snapshot_token() == Some(token) => Ok(self),
            Value::Utf8(token) => {
                if self.snapshot_token().is_some() || settings.clock_time.is_some() || settings.snapshot_time.is_some() {
                    return Err(invalid("snapshot-scope", "An exact snapshot fixes its catalog, clock and system ceiling"));
                }
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
        issuer(SqlSnapshotView { catalog, current_time: self.current_time, timezone: self.timezone.clone() }).map(Some)
    }

    pub fn with_exact_snapshot_token(mut self: Arc<Self>, token: String) -> Result<Arc<Self>> {
        let charge = self.memory.reserve(token.len().checked_add(256).ok_or_else(size_error)?)?;
        let current_time = self.current_time;
        let caps = captured_caps(&self.catalog);
        Arc::make_mut(&mut self).read_basis = Some(Arc::new(ReadBasis {
            times: caps, snapshot_time: None, clock_fallback: current_time,
            token: Some(token), _reservation: charge,
        }));
        Ok(self)
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
        let database = Arc::make_mut(&mut context.catalog.databases).get_mut(&context.catalog.default_database)
            .ok_or_else(|| invalid("owned-system-cut", "An owned historical read requires its captured database"))?;
        database.partitions[0] = snapshot.clone().with_system_time_cap(Some(system_time))?;
        context.current_time = system_time.into();
        context.cancellation = cancellation.child_token();
        context.read_basis = Some(Arc::new(ReadBasis {
            token: None,
            times: Some(times), snapshot_time: Some(system_time), clock_fallback: system_time.into(),
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
        if let Some(value) = settings.snapshot_token.as_ref().map(|expr| setting_value(expr, parameters)).transpose()? {
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
        let mut times = if let Some(expression) = &settings.snapshot_token {
            match setting_value(expression, parameters)? {
                Value::Null | Value::Undefined => captured_caps(&self.catalog),
                Value::Utf8(token) if self.snapshot_token() == Some(token) => captured_caps(&self.catalog),
                Value::Utf8(_) => return Err(invalid("snapshot-unretained", "SNAPSHOT_TOKEN must be readmitted before binding")),
                _ => return Err(invalid("invalid-snapshot-token", "SNAPSHOT_TOKEN requires a string or null")),
            }
        } else if let Some(basis) = &self.read_basis { basis.times.clone() }
        else { captured_caps(&self.catalog) };
        validate_basis(times.as_ref(), &self.catalog)?;
        let requested_cap = match &settings.snapshot_time {
            Some(SettingValue::TimestampLiteral(text)) =>
                Some(rtdb_types::parse_sql_timestamp_instant(text, &self.timezone)?.as_micros()?),
            Some(expression) => match setting_value(expression, parameters)? {
                Value::Null | Value::Undefined => None,
                value => Some(rtdb_expr::timestamp_micros(value, &self.timezone)?),
            },
            None => None,
        };
        if self.snapshot_token().is_some() && requested_cap.is_some() {
            return Err(invalid("snapshot-scope", "An exact snapshot fixes its system ceiling"));
        }
        let option_cap = self.read_basis.as_ref().and_then(|basis| basis.snapshot_time);
        let snapshot_time = match (option_cap, requested_cap) {
            (Some(option), Some(requested)) => Some(option.min(requested)),
            (option, requested) => option.or(requested),
        };
        if let Some(cap) = snapshot_time { times = times.map(|times| times.capped_at(cap.into())); }
        if self.snapshot_token().is_some() && current_time != self.current_time {
            return Err(invalid("snapshot-scope", "A readmitted snapshot fixes its original query clock"));
        }
        let token = self.snapshot_token().map(str::to_owned);
        let mut catalog = self.catalog.clone();
        for (name, database) in Arc::make_mut(&mut catalog.databases) {
            for (index, partition) in database.partitions.iter_mut().enumerate() {
                let cap = times.as_ref().and_then(|basis| basis.databases.get(name)).and_then(|times| times.get(index)).copied().flatten().map(|time| time.as_micros()).transpose()?;
                let visible = cap.or(snapshot_time).zip(partition.tx_basis()).map(|(cap, head)| cap.min(head.system_time));
                *partition = partition.clone().with_system_time_cap(visible)?;
            }
        }
        let mut context = self.execution_child_with_memory(memory);
        let mutable = Arc::get_mut(&mut context).ok_or_else(|| invalid("settings-owner", "New context is unexpectedly shared"))?;
        mutable.catalog = catalog;
        mutable.current_time = current_time;
        mutable.read_basis = Some(Arc::new(ReadBasis { times, snapshot_time, clock_fallback: fallback, token, _reservation: charge }));
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
    let databases: BTreeMap<_, _> = catalog.databases.iter().filter_map(|(name, database)| {
        let times: Vec<_> = database.partitions.iter().map(|snapshot| snapshot.system_time_cap().map(Into::into)).collect();
        times.iter().any(Option::is_some).then(|| (name.clone(), times))
    }).collect();
    (!databases.is_empty()).then_some(TimeBasis { databases })
}

fn validate_basis(basis: Option<&TimeBasis>, catalog: &CatalogSnapshot) -> Result<()> {
    for (name, database) in catalog.databases.iter() {
        for (index, snapshot) in database.partitions.iter().enumerate() {
            if let Some(requested) = basis.and_then(|basis| basis.databases.get(name)).and_then(|times| times.get(index)).copied().flatten() {
                if snapshot.tx_basis().is_none_or(|head| requested > head.system_time.into()) {
                    return Err(unindexed_tx(requested, snapshot.tx_basis()));
                }
            }
        }
    }
    Ok(())
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
