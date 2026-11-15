use crate::{ColumnId, Expr};
use rtdb_types::{Anomaly, LogicalType, Result, TableRef};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Identifier { pub text: String, pub quoted: bool }

impl Identifier {
    pub fn unquoted(text: impl Into<String>) -> Self { Self { text: text.into(), quoted: false } }
    pub fn quoted(text: impl Into<String>) -> Self { Self { text: text.into(), quoted: true } }
    pub fn normalized(&self) -> String {
        if self.quoted { return self.text.clone(); }
        let text = self.text.replace('-', "_");
        let text = text.strip_prefix("xt/").map_or_else(|| text.clone(), |rest| format!("_{rest}"));
        text.replace(['.', '/', '$'], "$").to_lowercase()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct QualifiedTable { pub database: String, pub table: TableRef }

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableBinding {
    pub name: QualifiedTable,
    pub oid: Option<i32>,
    pub columns: BTreeMap<String, LogicalType>,
}

/// Implementations resolve names, OIDs, and column types from one retained catalog basis.
pub trait Catalog {
    fn default_database(&self) -> &str;
    fn databases(&self) -> BTreeSet<String>;
    fn table(&self, name: &QualifiedTable) -> Result<Option<TableBinding>>;
    fn tables(&self, database: &str) -> Result<Vec<TableBinding>>;
    fn transaction_scoped(&self) -> bool { false }
}

pub fn resolve_table(catalog: &dyn Catalog, name: &[Identifier], search_path: &[String]) -> Result<TableBinding> {
    let normalized: Vec<_> = name.iter().map(Identifier::normalized).collect();
    let default_database = catalog.default_database();
    let mut candidates = match normalized.as_slice() {
        [table] => search_path.iter().map(|schema| QualifiedTable { database: default_database.to_owned(), table: TableRef::new(schema, table) }).collect::<Vec<_>>(),
        [prefix, table] => {
            let mut candidates = vec![QualifiedTable { database: default_database.to_owned(), table: TableRef::new(prefix, table) }];
            if !catalog.transaction_scoped() && catalog.databases().contains(prefix) {
                candidates.push(QualifiedTable { database: prefix.clone(), table: TableRef::new("public", table) });
            }
            candidates
        }
        [database, schema, table] => {
            if catalog.transaction_scoped() && database != default_database { return Err(Anomaly::incorrect("xtdb/table-not-found", format!("A transaction resolves tables only within its own database ('{default_database}')"))); }
            if !catalog.databases().contains(database) { return Err(Anomaly::incorrect("xtdb/unknown-db", format!("Unknown database: {database}"))); }
            vec![QualifiedTable { database: database.clone(), table: TableRef::new(schema, table) }]
        }
        _ => return Err(Anomaly::incorrect("xtdb/sql-error", "Table names have one, two, or three components")),
    };
    let mut seen = BTreeSet::new();
    candidates.retain(|candidate| seen.insert(candidate.clone()));
    let mut matches = Vec::new();
    for name in candidates {
        if let Some(binding) = catalog.table(&name)? {
            if normalized.len() == 1 { return Ok(binding); }
            matches.push(binding);
        }
    }
    match matches.len() {
        1 => return matches.pop().ok_or_else(|| Anomaly::fault("rtdb.plan/name-resolution", "Resolved table disappeared")),
        2.. => return Err(Anomaly::incorrect("xtdb/ambiguous-table", format!("Ambiguous table reference: {}", normalized.join(".")))),
        _ => {}
    }
    let suffix = if catalog.transaction_scoped() { format!(" - a transaction resolves tables only within its own database ('{default_database}'), reading a two-part name as schema.table") } else { String::new() };
    Err(Anomaly::incorrect("xtdb/table-not-found", format!("Table not found: {}{suffix}", normalized.join("."))))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Field {
    pub id: ColumnId,
    pub name: String,
    pub qualifier: Option<String>,
    pub data_type: LogicalType,
}

impl Field {
    pub fn expression(&self) -> Expr { Expr::column(self.id, self.data_type.clone()) }
    pub fn unqualified(mut self) -> Self { self.qualifier = None; self }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Vec<Field>", into = "Vec<Field>")]
pub struct Schema(Vec<Field>);

impl Schema {
    pub fn new(fields: Vec<Field>) -> Result<Self> {
        let mut ids = BTreeSet::new();
        for field in &fields {
            if !ids.insert(field.id) { return Err(Anomaly::fault("rtdb.plan/duplicate-column-id", format!("Repeated column {}", field.id.0))); }
            field.data_type.validate()?;
        }
        Ok(Self(fields))
    }
    pub fn fields(&self) -> &[Field] { &self.0 }
    pub fn len(&self) -> usize { self.0.len() }
    pub fn is_empty(&self) -> bool { self.0.is_empty() }
    pub fn ids(&self) -> BTreeSet<ColumnId> { self.0.iter().map(|field| field.id).collect() }
    pub fn get(&self, id: ColumnId) -> Option<&Field> { self.0.iter().find(|field| field.id == id) }
    pub fn position(&self, id: ColumnId) -> Option<usize> { self.0.iter().position(|field| field.id == id) }
    pub fn resolve(&self, name: &[Identifier]) -> Result<&Field> {
        let parts: Vec<_> = name.iter().map(Identifier::normalized).collect();
        let matches: Vec<_> = self.0.iter().filter(|field| match parts.as_slice() {
            [column] => &field.name == column,
            [qualifier, column] => &field.name == column && field.qualifier.as_ref() == Some(qualifier),
            _ => false,
        }).collect();
        match matches.as_slice() {
            [field] => Ok(field),
            [] => Err(Anomaly::incorrect("xtdb/column-not-found", format!("Column not found: {}", parts.join(".")))),
            _ => Err(Anomaly::incorrect("xtdb/ambiguous-column", format!("Ambiguous column: {}", parts.join(".")))),
        }
    }
    pub fn concat(&self, other: &Self) -> Result<Self> { Self::new(self.0.iter().chain(&other.0).cloned().collect()) }
    pub fn nullable(&self) -> Result<Self> {
        Self::new(self.0.iter().cloned().map(|mut field| { field.data_type = field.data_type.maybe(); field }).collect())
    }
}

impl TryFrom<Vec<Field>> for Schema { type Error = Anomaly; fn try_from(fields: Vec<Field>) -> Result<Self> { Self::new(fields) } }
impl From<Schema> for Vec<Field> { fn from(schema: Schema) -> Self { schema.0 } }

#[derive(Default)]
pub struct Binder { next_column: u32, parameter_types: BTreeMap<usize, LogicalType> }

impl Binder {
    pub fn field(&mut self, name: impl Into<String>, qualifier: Option<String>, data_type: LogicalType) -> Result<Field> {
        let id = ColumnId(self.next_column);
        self.next_column = self.next_column.checked_add(1).ok_or_else(|| Anomaly::incorrect("xtdb/query-too-large", "Column identifier space is exhausted"))?;
        Ok(Field { id, name: name.into(), qualifier, data_type })
    }
    pub fn parameter(&mut self, index: usize, data_type: LogicalType) -> Expr {
        let data_type = self.parameter_types.entry(index).and_modify(|old| *old = old.join(&data_type)).or_insert(data_type).clone();
        Expr::Parameter { index, data_type }
    }
    pub fn parameter_types(&self) -> &BTreeMap<usize, LogicalType> { &self.parameter_types }
}
