use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::MemoryReservation;
use rtdb_plan::{QuerySettings, SettingValue};
use rtdb_types::{Anomaly, Instant, Result, TimeBasis, Timestamp, TransactionKey, Value};
use crate::{CatalogSnapshot, QueryContext, invalid};

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
    for (name, database) in &catalog.databases {
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
        let token = Some(rtdb_format::basis::encode_time_basis(&times));
        let mut context = self.execution_child();
        Arc::get_mut(&mut context).ok_or_else(|| invalid("settings-owner", "New context is unexpectedly shared"))?.read_basis = Some(Arc::new(ReadBasis { times: Some(times), snapshot_time, clock_fallback: clock_fallback.into(), token, _reservation: charge }));
        Ok(context)
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
        let database = context.catalog.databases.get_mut(&context.catalog.default_database)
            .ok_or_else(|| invalid("owned-system-cut", "An owned historical read requires its captured database"))?;
        database.partitions[0] = snapshot.clone().with_system_time_cap(Some(system_time))?;
        context.current_time = system_time.into();
        context.cancellation = cancellation.child_token();
        context.read_basis = Some(Arc::new(ReadBasis {
            token: Some(rtdb_format::basis::encode_time_basis(&times)),
            times: Some(times), snapshot_time: Some(system_time), clock_fallback: system_time.into(),
            _reservation: charge,
        }));
        Ok(Arc::new(context))
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
                Value::Null | Value::Undefined => default_basis(&self.catalog),
                Value::Utf8(token) => Some(rtdb_format::basis::decode_time_basis(token)?),
                _ => return Err(invalid("invalid-snapshot-token", "SNAPSHOT_TOKEN requires a string or null")),
            }
        } else if let Some(basis) = &self.read_basis { basis.times.clone() }
        else { captured_caps(&self.catalog) };
        validate_basis(times.as_ref(), &self.catalog)?;
        let snapshot_time = self.read_basis.as_ref().and_then(|basis| basis.snapshot_time);
        if let Some(cap) = snapshot_time { times = times.map(|times| times.capped_at(cap.into())); }
        let token = times.as_ref().map(rtdb_format::basis::encode_time_basis);
        let mut catalog = self.catalog.clone();
        for (name, database) in &mut catalog.databases {
            for (index, partition) in database.partitions.iter_mut().enumerate() {
                let cap = times.as_ref().and_then(|basis| basis.databases.get(name)).and_then(|times| times.get(index)).copied().flatten().map(|time| time.as_micros()).transpose()?;
                let visible = cap.zip(partition.tx_basis()).map(|(cap, head)| cap.min(head.system_time));
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

fn default_basis(catalog: &CatalogSnapshot) -> Option<TimeBasis> {
    let databases: BTreeMap<_, _> = catalog.databases.iter().filter_map(|(name, database)| {
        let times: Vec<_> = database.partitions.iter().map(|snapshot| snapshot.tx_basis().map(|key| key.system_time.into())).collect();
        times.iter().any(Option::is_some).then(|| (name.clone(), times))
    }).collect();
    (!databases.is_empty()).then_some(TimeBasis { databases })
}

fn captured_caps(catalog: &CatalogSnapshot) -> Option<TimeBasis> {
    let databases: BTreeMap<_, _> = catalog.databases.iter().filter_map(|(name, database)| {
        let times: Vec<_> = database.partitions.iter().map(|snapshot| snapshot.system_time_cap().map(Into::into)).collect();
        times.iter().any(Option::is_some).then(|| (name.clone(), times))
    }).collect();
    (!databases.is_empty()).then_some(TimeBasis { databases })
}

fn validate_basis(basis: Option<&TimeBasis>, catalog: &CatalogSnapshot) -> Result<()> {
    for (name, database) in &catalog.databases {
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
