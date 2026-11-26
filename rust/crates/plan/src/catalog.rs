use rtdb_types::StructFields;
use crate::{ColumnId, Expr};
use rtdb_types::{Anomaly, LogicalType, Result, ScalarType, TableRef, TimeUnit};
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
    pub columns: StructFields<LogicalType>,
}

impl TableBinding {
    /// Resolves a scan column, including virtual bitemporal columns that are absent from document schemas.
    pub fn scan_column_type(&self, name: &str) -> LogicalType {
        let timestamp = || LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) });
        match name {
            "_iid" => LogicalType::Scalar(ScalarType::FixedSizeBinary(16)),
            "_assertion_ordinal" if self.name.table.is_owned_event_data() => LogicalType::Scalar(ScalarType::I64),
            rtdb_types::HOSTED_RECEIPT_TX_ID => LogicalType::Scalar(ScalarType::I64).maybe(),
            rtdb_types::OWNED_EVENT_POSITION_COLUMN => LogicalType::Scalar(ScalarType::Binary).maybe(),
            "_valid_from" | "_system_from" => timestamp(),
            "_valid_to" | "_system_to" => timestamp().maybe(),
            _ => self.columns.get(name).cloned().unwrap_or(LogicalType::Null),
        }
    }
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
        [table] => search_path.iter().map(|schema| QualifiedTable { database: default_database.to_owned(), table: TableRef::new(schema, table) })
            .chain(std::iter::once(QualifiedTable { database: default_database.to_owned(), table: TableRef::new("pg_catalog", table) })).collect::<Vec<_>>(),
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

/// Identifier components are normalized before construction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnQualifier {
    BaseTable { database: String, schema: String, alias: String },
    Alias(String),
    ScopedAlias { alias: String, symbol: String },
}

impl ColumnQualifier {
    pub fn alias(&self) -> &str { match self { Self::BaseTable { alias, .. } | Self::Alias(alias) | Self::ScopedAlias { alias, .. } => alias } }

    pub fn scope_symbol(&self) -> Option<&str> {
        match self { Self::ScopedAlias { symbol, .. } => Some(symbol), _ => None }
    }

    /// Includes this descriptor and its owned string capacities.
    pub fn retained_bytes(&self) -> Result<usize> {
        let mut bytes = std::mem::size_of::<Self>();
        let parts: &[&String] = match self {
            Self::BaseTable { database, schema, alias } => &[database, schema, alias],
            Self::Alias(alias) => &[alias],
            Self::ScopedAlias { alias, symbol } => &[alias, symbol],
        };
        for part in parts { bytes = bytes.checked_add(part.capacity()).ok_or_else(|| Anomaly::incorrect("rtdb.plan/qualifier-size", "Column qualifier exceeds addressable memory"))?; }
        Ok(bytes)
    }

    fn matches(&self, parts: &[String]) -> bool {
        let Some(alias) = parts.last() else { return true; };
        if alias != self.alias() { return false; }
        match self {
            Self::BaseTable { database, schema, .. } => parts.iter().rev().nth(1).is_none_or(|part| part == schema)
                && parts.iter().rev().nth(2).is_none_or(|part| part == database),
            Self::Alias(_) | Self::ScopedAlias { .. } => true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Field {
    pub id: ColumnId,
    pub name: String,
    pub qualifier: Option<ColumnQualifier>,
    /// Retained in wildcards and qualified lookup, excluded from one-part name lookup.
    #[serde(default)]
    pub qualified_only: bool,
    /// An internal table-column name; SQL projection assigns its public label.
    #[serde(default)]
    pub unnamed: bool,
    pub data_type: LogicalType,
}

impl Field {
    pub fn expression(&self) -> Expr { Expr::column(self.id, self.data_type.clone()) }
    pub fn unqualified(mut self) -> Self { self.qualifier = None; self.qualified_only = false; self }
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
    /// Accepts left-to-right identifiers, matching up to database.schema.alias.column for base tables or alias.column for alias scopes.
    /// Earlier components are ignored; missing or multiple matches return a name-resolution anomaly.
    pub fn resolve(&self, name: &[Identifier]) -> Result<&Field> {
        self.lookup(name).map_err(|unresolved| unresolved.into_anomaly(name))
    }
    /// Resolves as `resolve` does, saying why a name matched no single field instead of raising.
    pub fn lookup(&self, name: &[Identifier]) -> std::result::Result<&Field, Unresolved> {
        let parts: Vec<_> = name.iter().map(Identifier::normalized).collect();
        let mut matches = self.0.iter().filter(|field| match parts.split_last() {
            Some((column, [])) => !field.qualified_only && &field.name == column,
            Some((column, qualifiers)) => &field.name == column && field.qualifier.as_ref().is_some_and(|qualifier| qualifier.matches(qualifiers)),
            None => false,
        });
        match (matches.next(), matches.next()) {
            (Some(field), None) => Ok(field),
            (None, _) => Err(Unresolved::Missing),
            (Some(_), Some(_)) => Err(Unresolved::Ambiguous),
        }
    }
    pub fn concat(&self, other: &Self) -> Result<Self> { Self::new(self.0.iter().chain(&other.0).cloned().collect()) }
    pub fn nullable(&self) -> Result<Self> {
        Self::new(self.0.iter().cloned().map(|mut field| { field.data_type = field.data_type.maybe(); field }).collect())
    }
}

/// Why a column name matched no single field of a schema.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unresolved { Missing, Ambiguous }

impl Unresolved {
    pub fn into_anomaly(self, name: &[Identifier]) -> Anomaly {
        let parts = name.iter().map(Identifier::normalized).collect::<Vec<_>>().join(".");
        match self {
            Self::Missing => Anomaly::incorrect("xtdb/column-not-found", format!("Column not found: {parts}")),
            Self::Ambiguous => Anomaly::incorrect("xtdb/ambiguous-column", format!("Ambiguous column: {parts}")),
        }
    }
}

impl TryFrom<Vec<Field>> for Schema { type Error = Anomaly; fn try_from(fields: Vec<Field>) -> Result<Self> { Self::new(fields) } }
impl From<Schema> for Vec<Field> { fn from(schema: Schema) -> Self { schema.0 } }

#[derive(Default)]
pub struct Binder { next_column: u32, parameter_types: BTreeMap<usize, LogicalType> }

impl Binder {
    pub fn after(schema: &Schema) -> Result<Self> {
        let next_column = schema.fields().iter().map(|field| field.id.0).max().map(|id| id.checked_add(1).ok_or_else(|| Anomaly::incorrect("xtdb/query-too-large", "Column identifier space is exhausted"))).transpose()?.unwrap_or(0);
        Ok(Self { next_column, parameter_types: BTreeMap::new() })
    }
    pub fn field(&mut self, name: impl Into<String>, qualifier: Option<String>, data_type: LogicalType) -> Result<Field> {
        let id = ColumnId(self.next_column);
        self.next_column = self.next_column.checked_add(1).ok_or_else(|| Anomaly::incorrect("xtdb/query-too-large", "Column identifier space is exhausted"))?;
        Ok(Field { id, name: name.into(), qualifier: qualifier.map(ColumnQualifier::Alias), qualified_only: false, unnamed: false, data_type })
    }
    pub fn parameter(&mut self, index: usize, data_type: LogicalType) -> Expr {
        let data_type = self.parameter_types.entry(index).and_modify(|old| *old = old.join(&data_type)).or_insert(data_type).clone();
        Expr::Parameter { index, data_type }
    }
    pub fn parameter_types(&self) -> &BTreeMap<usize, LogicalType> { &self.parameter_types }
}

#[derive(Clone, Debug, PartialEq)]
enum Lookup {
    DefaultDatabase(String),
    Databases(BTreeSet<String>),
    Table(QualifiedTable, Option<TableBinding>),
    Tables(String, Vec<TableBinding>),
    TransactionScoped(bool),
}

/// What a planning read from its catalog: a later catalog that answers each lookup the same plans the statement the same way.
#[derive(Clone, Debug, PartialEq)]
pub struct CatalogLookups(Vec<Lookup>);
impl CatalogLookups {
    pub fn retained_bytes(&self) -> Result<usize> {
        let overflow = || Anomaly::incorrect("rtdb.plan/catalog-size", "Catalog lookup storage exceeds addressable memory");
        let name_bytes = |name: &QualifiedTable| name.database.capacity().saturating_add(name.table.schema.capacity()).saturating_add(name.table.table.capacity());
        let table_bytes = |table: &TableBinding| -> Result<usize> {
            let mut size = name_bytes(&table.name);
            for (name, ty) in &table.columns {
                size = size.checked_add(name.capacity()).and_then(|n| n.checked_add(256)).and_then(|n| n.checked_add(ty.retained_bytes().ok()?)).ok_or_else(overflow)?;
            }
            Ok(size)
        };
        let mut bytes = self.0.capacity().checked_mul(std::mem::size_of::<Lookup>()).ok_or_else(overflow)?;
        for lookup in &self.0 {
            let size = match lookup {
                Lookup::DefaultDatabase(database) => database.capacity(),
                Lookup::Databases(databases) => databases.iter().try_fold(0usize, |size, database| size.checked_add(database.capacity()).and_then(|n| n.checked_add(128))).ok_or_else(overflow)?,
                Lookup::Table(name, table) => name_bytes(name).checked_add(table.as_ref().map(table_bytes).transpose()?.unwrap_or(0)).ok_or_else(overflow)?,
                Lookup::Tables(database, tables) => {
                    let mut size = tables.capacity().checked_mul(std::mem::size_of::<TableBinding>()).and_then(|n| n.checked_add(database.capacity())).ok_or_else(overflow)?;
                    for table in tables { size = size.checked_add(table_bytes(table)?).ok_or_else(overflow)?; }
                    size
                }
                Lookup::TransactionScoped(_) => 0,
            };
            bytes = bytes.checked_add(size).ok_or_else(overflow)?;
        }
        Ok(bytes)
    }

    pub fn replay(&self, catalog: &dyn Catalog) -> Result<bool> {
        crate::lookup_replay::replays_every(&self.0, |lookup| Ok(match lookup {
            Lookup::DefaultDatabase(database) => catalog.default_database() == database,
            Lookup::Databases(databases) => catalog.databases() == *databases,
            Lookup::Table(name, binding) => catalog.table(name)? == *binding,
            Lookup::Tables(database, tables) => catalog.tables(database)? == *tables,
            Lookup::TransactionScoped(scoped) => catalog.transaction_scoped() == *scoped,
        }))
    }
}

/// A catalog that records each answer it gives; a lookup that fails leaves nothing to replay, so `finish` returns none.
pub struct RecordingCatalog<'a> { catalog: &'a dyn Catalog, lookups: std::cell::RefCell<Option<Vec<Lookup>>> }
impl<'a> RecordingCatalog<'a> {
    pub fn new(catalog: &'a dyn Catalog) -> Self { Self { catalog, lookups: std::cell::RefCell::new(Some(Vec::new())) } }
    pub fn finish(self) -> Option<CatalogLookups> { self.lookups.into_inner().map(CatalogLookups) }
    fn record(&self, lookup: Lookup) { if let Some(lookups) = self.lookups.borrow_mut().as_mut() { lookups.push(lookup); } }
    fn recorded<T>(&self, answer: Result<T>, lookup: impl FnOnce(&T) -> Lookup) -> Result<T> {
        match &answer { Ok(value) => self.record(lookup(value)), Err(_) => *self.lookups.borrow_mut() = None }
        answer
    }
}
impl Catalog for RecordingCatalog<'_> {
    fn default_database(&self) -> &str {
        let database = self.catalog.default_database();
        self.record(Lookup::DefaultDatabase(database.to_owned()));
        database
    }
    fn databases(&self) -> BTreeSet<String> {
        let databases = self.catalog.databases();
        self.record(Lookup::Databases(databases.clone()));
        databases
    }
    fn table(&self, name: &QualifiedTable) -> Result<Option<TableBinding>> {
        self.recorded(self.catalog.table(name), |binding| Lookup::Table(name.clone(), binding.clone()))
    }
    fn tables(&self, database: &str) -> Result<Vec<TableBinding>> {
        self.recorded(self.catalog.tables(database), |tables| Lookup::Tables(database.to_owned(), tables.clone()))
    }
    fn transaction_scoped(&self) -> bool {
        let scoped = self.catalog.transaction_scoped();
        self.record(Lookup::TransactionScoped(scoped));
        scoped
    }
}
