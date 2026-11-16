use crate::{ColumnId, Expr, Identifier, QualifiedTable, Relation, TemporalFilter};
use rtdb_types::LogicalType;
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
    CreateTable { table: QualifiedTable, columns: Vec<String> },
    GrantRole { user: String, role: String, revoke: bool },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Statement {
    Query(Arc<Relation>),
    Dml(Dml),
    Begin { access_mode: Option<AccessMode>, system_time: Option<Expr> },
    Commit,
    Rollback,
    Set { name: Vec<Identifier>, value: Expr, local: bool },
    Show { name: Vec<Identifier> },
    Explain { statement: Box<Statement> },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PreparedStatement {
    pub statement: Statement,
    pub parameter_types: BTreeMap<usize, LogicalType>,
    pub source: String,
}
