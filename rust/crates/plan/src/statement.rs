use crate::{ColumnId, Expr, Identifier, QualifiedTable, Relation, TemporalFilter};
use rtdb_types::{LogicalType, Value};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccessMode { ReadOnly, ReadWrite }

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Dml {
    Insert { table: QualifiedTable, input: Arc<Relation> },
    Update { table: QualifiedTable, input: Arc<Relation>, target_columns: BTreeSet<ColumnId>, assignments: Vec<(String, Expr)>, valid_time: TemporalFilter },
    Delete { table: QualifiedTable, input: Arc<Relation>, valid_time: TemporalFilter },
    Erase { table: QualifiedTable, input: Arc<Relation> },
    Patch { table: QualifiedTable, input: Arc<Relation>, valid_time: TemporalFilter },
    Assert { input: Arc<Relation>, message: Option<String> },
    CreateTable { table: QualifiedTable, columns: Vec<(String, LogicalType)> },
    GrantRole { user: String, role: String, revoke: bool },
    /// `history` is false when the index covers only the current side (fts.allium: SearchIndexDefinition).
    CreateSearchIndex { name: String, table: QualifiedTable, columns: Vec<String>, history: bool, if_not_exists: bool },
    DropSearchIndex { name: String },
    CreateOrderedIndex { name: String, table: QualifiedTable, columns: Vec<String>, if_not_exists: bool },
    DropOrderedIndex { name: String },
    /// An approximate nearest-neighbour index over one vector column, partitioned by `partition` (vector.allium: VectorIndexDefinition).
    CreateVectorIndex { name: String, table: QualifiedTable, column: String, partition: Vec<String>, dimensions: i64, method: String, recall: f64, if_not_exists: bool },
    DropVectorIndex { name: String },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Statement {
    Query(Arc<Relation>),
    Dml(Dml),
    Begin { access_mode: Option<AccessMode> },
    Commit,
    Rollback,
    Set { name: Vec<Identifier>, value: Expr, local: bool },
    Show { name: Vec<Identifier> },
    Explain { relation: Arc<Relation>, mode: crate::ExplainMode },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// A planning warning, rendered by PostgreSQL as WARNING with SQLSTATE 01000.
pub struct SqlNotice { pub message: String }

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct QuerySettings {
    pub clock_time: Option<SettingValue>,
    pub snapshot_token: Option<SettingValue>,
    pub snapshot_time: Option<SettingValue>,
}

/// Prepared query metadata resolves literal values and parameters without evaluating scalar forms.
/// Unevaluated forms retain their parameter count after syntax and literal validation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum SettingValue {
    Literal(Value),
    /// A SQL timestamp literal retained before Arrow admission for CLOCK_TIME resolution.
    TimestampLiteral(String),
    Parameter { index: usize },
    Unevaluated { parameter_count: usize },
}

impl SettingValue {
    pub fn parameter_count(&self) -> usize {
        match self { Self::Literal(_) | Self::TimestampLiteral(_) => 0, Self::Parameter { index } => index + 1, Self::Unevaluated { parameter_count } => *parameter_count }
    }
}

impl QuerySettings {
    /// Bounds owned setting values, including unadmitted literal text, for the lifetime of the prepared query.
    /// Size overflow returns an allocation anomaly.
    pub fn retained_bytes(&self) -> rtdb_types::Result<usize> {
        let overflow = || rtdb_types::Anomaly::incorrect("rtdb.plan/settings-size", "Query setting allocation overflow");
        [&self.clock_time,&self.snapshot_token,&self.snapshot_time].into_iter().flatten().try_fold(std::mem::size_of::<Self>(),|bytes,value| {
            let retained = match value { SettingValue::Literal(value) => value.retained_bytes()?,SettingValue::TimestampLiteral(text) => text.capacity(),_ => 0 };
            bytes.checked_add(retained).ok_or_else(overflow)
        })
    }
    pub fn parameter_count(&self) -> usize {
        [&self.clock_time, &self.snapshot_token, &self.snapshot_time].into_iter().flatten()
            .map(SettingValue::parameter_count).max().unwrap_or(0)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PreparedStatement {
    pub statement: Statement,
    pub parameter_types: BTreeMap<usize, LogicalType>,
    pub source: String,
    #[serde(default)]
    pub notices: Vec<SqlNotice>,
    #[serde(default)]
    pub settings: QuerySettings,
}
