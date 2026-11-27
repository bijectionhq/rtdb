use crate::{AccessMode, Expr, Identifier};
use serde::{Deserialize, Serialize};
use rtdb_types::Result;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ControlValue<E = Expr> {
    Default,
    Expression(E),
    /// A CLOCK_TIME timestamp literal resolved in the transaction's effective timezone before Arrow admission.
    TimestampLiteral(String),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReadOnlyOptions<E = Expr> {
    pub snapshot_token: Option<ControlValue<E>>,
    pub snapshot_time: Option<ControlValue<E>>,
    pub clock_time: Option<ControlValue<E>>,
    pub await_token: Option<ControlValue<E>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReadWriteOptions<E = Expr> {
    pub system_time: Option<ControlValue<E>>,
    pub metadata: Option<ControlValue<E>>,
    pub asynchronous: Option<ControlValue<E>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ControlAccessOptions<E = Expr> { ReadOnly(ReadOnlyOptions<E>), ReadWrite(ReadWriteOptions<E>) }

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TransactionOptions<E = Expr> {
    pub access: Option<ControlAccessOptions<E>>,
    pub timezone: Option<ControlValue<E>>,
}

impl<E> Default for ReadOnlyOptions<E> {
    fn default() -> Self {
        Self { snapshot_token: None, snapshot_time: None, clock_time: None, await_token: None }
    }
}

impl<E> Default for ReadWriteOptions<E> {
    fn default() -> Self { Self { system_time: None, metadata: None, asynchronous: None } }
}

impl<E> Default for TransactionOptions<E> {
    fn default() -> Self { Self { access: None, timezone: None } }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlCommitMode { Sync, Async }

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiscardTarget { All, Plans, Temporary, Sequences }

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CopyFormat { TransitJson, TransitMsgpack, ArrowFile, ArrowStream }

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CopyOutFormat { ArrowStream, HistoryLabelDictionaryV1 }

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ControlStatement<E = Expr> {
    Begin(TransactionOptions<E>),
    SetTransaction(TransactionOptions<E>),
    Commit(Option<ControlCommitMode>),
    Rollback,
    SetSessionCharacteristics(Option<AccessMode>),
    SetTimeZone(E),
    SetAwaitToken(E),
    SetParameter { name: String, value: E },
    SetRole,
    Discard(DiscardTarget),
    Show { name: String },
    ShowTransactionIsolationLevel,
    Prepare { name: String, source: String },
    Execute { name: String, arguments: Vec<E> },
    CopyIn { table: Vec<Identifier>, format: Option<CopyFormat> },
    CopyOut { query: String, format: CopyOutFormat },
    AttachDatabase { name: String, configuration_yaml: Option<String>, source: String },
    DetachDatabase { name: String },
    ShowSessionParameter { name: String },
}

impl<E> ControlStatement<E> {
    /// Converts control operands in resolution order, including the clock value's separate timestamp representation.
    pub fn try_map_expressions<T>(
        self, mut map: impl FnMut(E) -> Result<T>, map_clock: impl FnOnce(E) -> Result<ControlValue<T>>,
    ) -> Result<ControlStatement<T>> {
        Ok(match self {
            Self::Begin(options) => ControlStatement::Begin(options.try_map_expressions(&mut map, map_clock)?),
            Self::SetTransaction(options) => ControlStatement::SetTransaction(options.try_map_expressions(&mut map, map_clock)?),
            Self::Commit(mode) => ControlStatement::Commit(mode),
            Self::Rollback => ControlStatement::Rollback,
            Self::SetSessionCharacteristics(access) => ControlStatement::SetSessionCharacteristics(access),
            Self::SetTimeZone(value) => ControlStatement::SetTimeZone(map(value)?),
            Self::SetAwaitToken(value) => ControlStatement::SetAwaitToken(map(value)?),
            Self::SetParameter { name, value } => ControlStatement::SetParameter { name, value: map(value)? },
            Self::SetRole => ControlStatement::SetRole,
            Self::Discard(target) => ControlStatement::Discard(target),
            Self::Show { name } => ControlStatement::Show { name },
            Self::ShowTransactionIsolationLevel => ControlStatement::ShowTransactionIsolationLevel,
            Self::Prepare { name, source } => ControlStatement::Prepare { name, source },
            Self::Execute { name, arguments } => ControlStatement::Execute { name, arguments: arguments.into_iter().map(map).collect::<Result<_>>()? },
            Self::CopyIn { table, format } => ControlStatement::CopyIn { table, format },
            Self::CopyOut { query, format } => ControlStatement::CopyOut { query, format },
            Self::AttachDatabase { name, configuration_yaml, source } => ControlStatement::AttachDatabase { name, configuration_yaml, source },
            Self::DetachDatabase { name } => ControlStatement::DetachDatabase { name },
            Self::ShowSessionParameter { name } => ControlStatement::ShowSessionParameter { name },
        })
    }

    pub fn is_show(&self) -> bool {
        matches!(self, Self::Show { .. } | Self::ShowTransactionIsolationLevel | Self::ShowSessionParameter { .. })
    }
}

impl<E> TransactionOptions<E> {
    fn try_map_expressions<T>(
        self, mut bind: impl FnMut(E) -> Result<T>, bind_clock: impl FnOnce(E) -> Result<ControlValue<T>>,
    ) -> Result<TransactionOptions<T>> {
        let mut map = |value| map_control_value(value, |expression| bind(expression).map(ControlValue::Expression));
        let timezone = map(self.timezone)?;
        let access = match self.access {
            Some(ControlAccessOptions::ReadOnly(options)) => Some(ControlAccessOptions::ReadOnly(ReadOnlyOptions {
                snapshot_token: map(options.snapshot_token)?,
                snapshot_time: map(options.snapshot_time)?,
                clock_time: map_control_value(options.clock_time, bind_clock)?,
                await_token: map(options.await_token)?,
            })),
            Some(ControlAccessOptions::ReadWrite(options)) => Some(ControlAccessOptions::ReadWrite(ReadWriteOptions {
                system_time: map(options.system_time)?,
                metadata: map(options.metadata)?,
                asynchronous: map(options.asynchronous)?,
            })),
            None => None,
        };
        Ok(TransactionOptions { timezone, access })
    }
}

fn map_control_value<E, T>(
    value: Option<ControlValue<E>>, map: impl FnOnce(E) -> Result<ControlValue<T>>,
) -> Result<Option<ControlValue<T>>> {
    value.map(|value| match value {
        ControlValue::Default => Ok(ControlValue::Default),
        ControlValue::TimestampLiteral(text) => Ok(ControlValue::TimestampLiteral(text)),
        ControlValue::Expression(expression) => map(expression),
    }).transpose()
}

impl ControlStatement {
    pub fn parameter_count(&self) -> usize {
        self.expressions().into_iter().map(Expr::parameter_count).max().unwrap_or(0)
    }
    pub fn expressions(&self) -> Vec<&Expr> {
        match self {
            Self::Begin(options) | Self::SetTransaction(options) => options.expressions(),
            Self::SetTimeZone(expr) | Self::SetAwaitToken(expr) | Self::SetParameter { value: expr, .. } => vec![expr],
            Self::Execute { arguments, .. } => arguments.iter().collect(),
            _ => Vec::new(),
        }
    }
    pub fn requires_read_transaction(&self) -> bool {
        matches!(self, Self::ShowTransactionIsolationLevel)
            || matches!(self, Self::Show { name } if [
                "snapshot_token", "clock_time", "timezone", "rtdb_hosted_source_owner",
            ].contains(&name.as_str()))
    }
}

impl TransactionOptions {
    pub fn expressions(&self) -> Vec<&Expr> {
        let mut values = vec![self.timezone.as_ref()];
        match &self.access {
            Some(ControlAccessOptions::ReadOnly(options)) => values.extend([options.snapshot_token.as_ref(), options.snapshot_time.as_ref(), options.clock_time.as_ref(), options.await_token.as_ref()]),
            Some(ControlAccessOptions::ReadWrite(options)) => values.extend([options.system_time.as_ref(), options.metadata.as_ref(), options.asynchronous.as_ref()]),
            None => (),
        }
        values.into_iter().flatten().filter_map(|value| match value { ControlValue::Expression(expr) => Some(expr), ControlValue::Default | ControlValue::TimestampLiteral(_) => None }).collect()
    }
}
