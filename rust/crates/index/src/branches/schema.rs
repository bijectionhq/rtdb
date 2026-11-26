use rtdb_types::{Anomaly, EntityId, LogicalType, Result, ScalarType, StructFields, Value};
use serde::{Deserialize, Serialize};

/// Columns RTDB maintains on every row of a table; a schema never declares them.
pub const TEMPORAL_COLUMNS: [&str; 4] = ["_valid_from", "_valid_to", "_system_from", "_system_to"];

/// The column contract of one table.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TableSchema {
    /// A table that has never declared a schema.
    /// Classic declarations widen these columns, and writes may add others.
    Open(StructFields<LogicalType>),
    /// A declared key type and columns: every write conforms to them.
    Declared(DeclaredSchema),
}

impl Default for TableSchema {
    fn default() -> Self { Self::Open(StructFields::new()) }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeclaredSchema {
    key: ScalarType,
    columns: StructFields<LogicalType>,
}

impl TableSchema {
    pub fn is_declared(&self) -> bool { matches!(self, Self::Declared(_)) }

    /// The columns a query of this table resolves, before any row exists.
    /// A declared table includes its `_id`; RTDB supplies the temporal columns to every scan.
    pub fn columns(&self) -> StructFields<LogicalType> {
        match self {
            Self::Open(columns) => columns.clone(),
            Self::Declared(schema) => {
                let mut columns = StructFields::from([("_id".to_owned(), LogicalType::Scalar(schema.key.clone()))]);
                for (name, ty) in &schema.columns { columns.insert(name.clone(), ty.clone()); }
                columns
            }
        }
    }

    /// Applies a schema declaration, which must name a non-null `_id`.
    /// A redeclaration keeps the key, may add columns, and leaves every existing column's type unchanged.
    pub fn declare(&self, fields: &StructFields<LogicalType>) -> Result<Self> {
        let key = match fields.get("_id") {
            None => return Err(Anomaly::incorrect("rtdb/schema-key", "A declared schema names a non-null _id field")),
            Some(LogicalType::Scalar(key @ (ScalarType::Utf8 | ScalarType::Keyword | ScalarType::I64 | ScalarType::Uuid))) => key.clone(),
            Some(_) => return Err(Anomaly::incorrect("rtdb/schema-key",
                "The declared _id must be a non-null Utf8, keyword, Int64 or UUID field")),
        };
        let mut columns = match self {
            Self::Open(_) => StructFields::new(),
            Self::Declared(prior) if prior.key != key => {
                return Err(Anomaly::incorrect("rtdb/schema-key-change", "A redeclaration keeps the table's _id type"));
            }
            Self::Declared(prior) => prior.columns.clone(),
        };
        for (name, ty) in fields {
            if name == "_id" { continue; }
            if TEMPORAL_COLUMNS.contains(&name.as_str()) {
                return Err(Anomaly::incorrect("rtdb/reserved-field", format!("RTDB maintains the temporal column {name}")));
            }
            match columns.get(name) {
                Some(prior) if prior != ty => {
                    return Err(Anomaly::incorrect("rtdb/schema-type-change", format!("A redeclaration keeps the type of column {name}")));
                }
                Some(_) => {}
                None => { columns.insert(name.clone(), ty.clone()); }
            }
        }
        Ok(Self::Declared(DeclaredSchema { key, columns }))
    }

    /// Records the columns of a Classic declaration.
    /// An open table widens to them; a declared table only admits columns that already conform.
    pub fn widen(&mut self, fields: &StructFields<LogicalType>) -> Result<()> {
        match self {
            Self::Open(columns) => {
                for (name, ty) in fields {
                    let ty = columns.get(name).map_or_else(|| ty.clone(), |prior| prior.join(ty));
                    columns.insert(name.clone(), ty);
                }
                Ok(())
            }
            Self::Declared(_) => self.admit_types(fields),
        }
    }

    pub fn admit_key(&self, id: &EntityId) -> Result<()> {
        match self {
            Self::Declared(schema) if id.as_value().logical_type() != LogicalType::Scalar(schema.key.clone()) => {
                Err(Anomaly::incorrect("rtdb/schema-type", "The row ID does not have the table's declared _id type"))
            }
            _ => Ok(()),
        }
    }

    /// Admits one written cell other than `_id`; a null cell conforms to any declared column.
    pub fn admit_value(&self, name: &str, value: &Value) -> Result<()> {
        let Self::Declared(schema) = self else { return Ok(()); };
        let ty = schema.columns.get(name).ok_or_else(|| undeclared(name))?;
        if !matches!(value, Value::Null | Value::Undefined) && ty.join(&value.logical_type()) != *ty {
            return Err(schema_type(name));
        }
        Ok(())
    }

    /// Admits the column types observed in a write, including its `_id`.
    /// Temporal columns are RTDB's and nulls conform, so only the value legs are compared.
    pub fn admit_types(&self, types: &StructFields<LogicalType>) -> Result<()> {
        let Self::Declared(schema) = self else { return Ok(()); };
        for (name, observed) in types {
            if TEMPORAL_COLUMNS.contains(&name.as_str()) || name == "_iid" { continue; }
            let observed = observed.unencoded();
            let admitted = if name == "_id" {
                observed.legs().iter().all(|leg| match (leg, &schema.key) {
                    (LogicalType::Null, _) => true,
                    (LogicalType::Scalar(ScalarType::I8 | ScalarType::I16 | ScalarType::I32 | ScalarType::I64), ScalarType::I64) => true,
                    (LogicalType::Scalar(scalar), key) => scalar == key,
                    _ => false,
                })
            } else {
                let declared = schema.columns.get(name).ok_or_else(|| undeclared(name))?.unencoded().maybe();
                matches!(observed, LogicalType::Nothing | LogicalType::Null) || declared.join(&observed.maybe()) == declared
            };
            if !admitted { return Err(schema_type(name)); }
        }
        Ok(())
    }
}

fn undeclared(name: &str) -> Anomaly {
    Anomaly::incorrect("rtdb/undeclared-column", format!("Column {name} is not declared by the table's schema"))
}

fn schema_type(name: &str) -> Anomaly {
    Anomaly::incorrect("rtdb/schema-type", format!("Invalid type for declared column {name}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utf8() -> LogicalType { LogicalType::Scalar(ScalarType::Utf8) }
    fn i64() -> LogicalType { LogicalType::Scalar(ScalarType::I64) }
    fn fields(fields: &[(&str, LogicalType)]) -> StructFields<LogicalType> {
        fields.iter().map(|(name, ty)| ((*name).to_owned(), ty.clone())).collect()
    }
    fn refusal(result: Result<impl std::fmt::Debug>) -> String { result.unwrap_err().code }

    #[test]
    fn a_declaration_names_a_non_null_supported_key_and_no_temporal_column() {
        let open = TableSchema::default();
        assert_eq!(refusal(open.declare(&fields(&[("name", utf8())]))), "rtdb/schema-key");
        assert_eq!(refusal(open.declare(&fields(&[("_id", i64().maybe())]))), "rtdb/schema-key");
        assert_eq!(refusal(open.declare(&fields(&[("_id", LogicalType::Scalar(ScalarType::I32))]))), "rtdb/schema-key");
        for temporal in TEMPORAL_COLUMNS {
            assert_eq!(refusal(open.declare(&fields(&[("_id", i64()), (temporal, i64())]))), "rtdb/reserved-field", "{temporal}");
        }
        let declared = open.declare(&fields(&[("_id", utf8()), ("name", utf8().maybe())])).unwrap();
        assert_eq!(declared.columns(), fields(&[("_id", utf8()), ("name", utf8().maybe())]));
    }

    #[test]
    fn a_redeclaration_keeps_the_key_and_existing_types_and_may_add_columns() {
        let declared = TableSchema::default().declare(&fields(&[("_id", i64()), ("n", i64())])).unwrap();
        assert_eq!(refusal(declared.declare(&fields(&[("_id", utf8())]))), "rtdb/schema-key-change");
        assert_eq!(refusal(declared.declare(&fields(&[("_id", i64()), ("n", i64().maybe())]))), "rtdb/schema-type-change");
        let wider = declared.declare(&fields(&[("_id", i64()), ("n", i64()), ("s", utf8())])).unwrap();
        assert_eq!(wider.declare(&fields(&[("_id", i64())])).unwrap(), wider);
        assert_eq!(wider.columns(), fields(&[("_id", i64()), ("n", i64()), ("s", utf8())]));
    }

    #[test]
    fn a_declared_table_admits_only_declared_columns_of_their_types() {
        let declared = TableSchema::default().declare(&fields(&[("_id", i64()), ("n", i64())])).unwrap();
        declared.admit_value("n", &Value::I64(1)).unwrap();
        declared.admit_value("n", &Value::Null).unwrap();
        assert_eq!(refusal(declared.admit_value("n", &Value::Utf8("x".into()))), "rtdb/schema-type");
        assert_eq!(refusal(declared.admit_value("other", &Value::I64(1))), "rtdb/undeclared-column");
        declared.admit_key(&EntityId::Integer(1)).unwrap();
        assert_eq!(refusal(declared.admit_key(&EntityId::String("a".into()))), "rtdb/schema-type");

        declared.admit_types(&fields(&[("_id", LogicalType::Scalar(ScalarType::I32)), ("n", i64().maybe()), ("_valid_from", i64())])).unwrap();
        assert_eq!(refusal(declared.admit_types(&fields(&[("_id", utf8())]))), "rtdb/schema-type");
        assert_eq!(refusal(declared.admit_types(&fields(&[("n", i64().join(&utf8()))]))), "rtdb/schema-type");
        assert_eq!(refusal(declared.admit_types(&fields(&[("other", i64())]))), "rtdb/undeclared-column");
        let mut widened = declared.clone();
        assert_eq!(refusal(widened.widen(&fields(&[("other", i64())]))), "rtdb/undeclared-column");
        assert_eq!(widened, declared);
    }

    #[test]
    fn an_open_table_widens_and_admits_any_column() {
        let mut open = TableSchema::default();
        open.widen(&fields(&[("n", i64())])).unwrap();
        open.widen(&fields(&[("n", utf8()), ("s", utf8())])).unwrap();
        assert_eq!(open.columns(), fields(&[("n", i64().join(&utf8())), ("s", utf8())]));
        open.admit_value("anything", &Value::I64(1)).unwrap();
        open.admit_types(&fields(&[("anything", utf8())])).unwrap();
        open.admit_key(&EntityId::String("a".into())).unwrap();
    }
}
