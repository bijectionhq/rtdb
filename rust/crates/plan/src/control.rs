use crate::{AccessMode, Expr, Identifier};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ControlValue {
    Default,
    Expression(Expr),
    /// A CLOCK_TIME timestamp literal resolved in the transaction's effective timezone before Arrow admission.
    TimestampLiteral(String),
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ReadOnlyOptions {
    pub snapshot_token: Option<ControlValue>,
    pub snapshot_time: Option<ControlValue>,
    pub clock_time: Option<ControlValue>,
    pub await_token: Option<ControlValue>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ReadWriteOptions {
    pub system_time: Option<ControlValue>,
    pub metadata: Option<ControlValue>,
    pub asynchronous: Option<ControlValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ControlAccessOptions { ReadOnly(ReadOnlyOptions), ReadWrite(ReadWriteOptions) }

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TransactionOptions {
    pub access: Option<ControlAccessOptions>,
    pub timezone: Option<ControlValue>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlCommitMode { Sync, Async }

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiscardTarget { All, Plans, Temporary, Sequences }

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CopyFormat { TransitJson, TransitMsgpack, ArrowFile, ArrowStream }

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ControlStatement {
    Begin(TransactionOptions),
    SetTransaction(TransactionOptions),
    Commit(Option<ControlCommitMode>),
    Rollback,
    SetSessionCharacteristics(Option<AccessMode>),
    SetTimeZone(Expr),
    SetAwaitToken(Expr),
    SetParameter { name: String, value: Expr },
    SetRole,
    Discard(DiscardTarget),
    Show { name: String },
    Prepare { name: String, source: String },
    Execute { name: String, arguments: Vec<Expr> },
    CopyIn { table: Vec<Identifier>, format: Option<CopyFormat> },
    AttachDatabase { name: String, configuration_yaml: Option<String> },
    DetachDatabase { name: String },
}

impl ControlStatement {
    pub fn parameter_count(&self) -> usize {
        let expressions: Vec<&Expr> = match self {
            Self::Begin(options) | Self::SetTransaction(options) => options.expressions(),
            Self::SetTimeZone(expr) | Self::SetAwaitToken(expr) | Self::SetParameter { value: expr, .. } => vec![expr],
            Self::Execute { arguments, .. } => arguments.iter().collect(),
            _ => Vec::new(),
        };
        expressions.into_iter().map(Expr::parameter_count).max().unwrap_or(0)
    }
    pub fn requires_read_transaction(&self) -> bool {
        matches!(self, Self::Show { name } if ["snapshot_token", "clock_time", "timezone", "transaction_isolation"].contains(&name.as_str()))
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
