use rtdb_types::StructFields;
use std::collections::BTreeSet;
use std::sync::Arc;
use rtdb_plan::*;
use rtdb_types::{LogicalType, Result, ScalarType, TableRef, Value};

struct CatalogFixture { database: String, oid: i32, ty: LogicalType }
impl Catalog for CatalogFixture {
    fn default_database(&self) -> &str { &self.database }
    fn databases(&self) -> BTreeSet<String> { BTreeSet::from([self.database.clone()]) }
    fn table(&self, name: &QualifiedTable) -> Result<Option<TableBinding>> {
        Ok((name.database == self.database && name.table == TableRef::new("public", "prices")).then(|| TableBinding {
            name: name.clone(), oid: Some(self.oid), columns: StructFields::from([("price".into(), self.ty.clone())]),
        }))
    }
    fn tables(&self, database: &str) -> Result<Vec<TableBinding>> {
        Ok(self.table(&QualifiedTable { database: database.into(), table: TableRef::new("public", "prices") })?.into_iter().collect())
    }
}

#[test]
fn identical_names_bind_to_the_captured_database_oid_and_schema() {
    let left = CatalogFixture { database: "left".into(), oid: 12, ty: LogicalType::Scalar(ScalarType::I64) };
    let right = CatalogFixture { database: "right".into(), oid: 18, ty: LogicalType::Scalar(ScalarType::Utf8) };
    let name = [Identifier::unquoted("prices")];
    let path = ["public".into()];
    let a = resolve_table(&left, &name, &path).unwrap();
    let b = resolve_table(&right, &name, &path).unwrap();
    assert_eq!((a.oid, b.oid), (Some(12), Some(18)));
    assert_ne!(a.columns, b.columns);
    assert_eq!(a.name.database, "left");
    assert_eq!(b.name.database, "right");
}

#[test]
fn two_part_names_consider_database_and_schema_and_reject_ambiguity() {
    struct Ambiguous;
    impl Catalog for Ambiguous {
        fn default_database(&self) -> &str { "xtdb" }
        fn databases(&self) -> BTreeSet<String> { BTreeSet::from(["xtdb".into(), "public".into()]) }
        fn table(&self, name: &QualifiedTable) -> Result<Option<TableBinding>> {
            Ok((name.table == TableRef::new("public", "prices")).then(|| TableBinding { name: name.clone(), oid: Some(1), columns: StructFields::new() }))
        }
        fn tables(&self, _: &str) -> Result<Vec<TableBinding>> { Ok(Vec::new()) }
    }
    let ambiguous = [Identifier::unquoted("public"), Identifier::unquoted("prices")];
    assert_eq!(resolve_table(&Ambiguous, &ambiguous, &["public".into()]).unwrap_err().code, "xtdb/ambiguous-table");
    let remote = [Identifier::unquoted("public"), Identifier::unquoted("public"), Identifier::unquoted("prices")];
    assert_eq!(resolve_table(&Ambiguous, &remote, &["public".into()]).unwrap().name.database, "public");
    let current_database = [Identifier::unquoted("xtdb"), Identifier::unquoted("prices")];
    assert_eq!(resolve_table(&Ambiguous, &current_database, &["public".into()]).unwrap().name.database, "xtdb");
}

#[test]
fn quoted_names_keep_case_while_regular_identifiers_use_xtdb_normal_form() {
    assert_eq!(Identifier::unquoted("My-Column").normalized(), "my_column");
    assert_eq!(Identifier::unquoted("xt/valid-from").normalized(), "_valid_from");
    assert_eq!(Identifier::quoted("My-Column").normalized(), "My-Column");
}

#[test]
fn scope_rejects_ambiguous_names_and_preserves_qualifiers() {
    let mut binder = Binder::default();
    let ty = LogicalType::Scalar(ScalarType::I64);
    let a = binder.field("price", Some("a".into()), ty.clone()).unwrap();
    let b = binder.field("price", Some("b".into()), ty).unwrap();
    let schema = Schema::new(vec![a.clone(), b]).unwrap();
    assert_eq!(schema.resolve(&[Identifier::unquoted("price")]).unwrap_err().code, "xtdb/ambiguous-column");
    assert_eq!(schema.resolve(&[Identifier::unquoted("a"), Identifier::unquoted("price")]).unwrap(), &a);
}

#[test]
fn lookup_says_why_a_name_matched_no_single_field() {
    let mut binder = Binder::default();
    let ty = LogicalType::Scalar(ScalarType::I64);
    let a = binder.field("price", Some("a".into()), ty.clone()).unwrap();
    let b = binder.field("price", Some("b".into()), ty).unwrap();
    let schema = Schema::new(vec![a.clone(), b]).unwrap();
    assert_eq!(schema.lookup(&[Identifier::unquoted("price")]), Err(Unresolved::Ambiguous));
    assert_eq!(schema.lookup(&[Identifier::unquoted("cost")]), Err(Unresolved::Missing));
    assert_eq!(schema.lookup(&[Identifier::unquoted("a"), Identifier::unquoted("price")]), Ok(&a));
    let missing = Unresolved::Missing.into_anomaly(&[Identifier::unquoted("a"), Identifier::unquoted("cost")]);
    assert_eq!((missing.code.as_str(), missing.message.as_deref()), ("xtdb/column-not-found", Some("Column not found: a.cost")));
    assert_eq!(Unresolved::Ambiguous.into_anomaly(&[Identifier::unquoted("price")]).code, "xtdb/ambiguous-column");
}

#[test]
fn base_table_column_qualifiers_match_database_schema_and_alias_as_one_namespace() {
    let field = |id, database: &str, schema: &str, alias: &str| Field { id: ColumnId(id), name: "v".into(),
        qualifier: Some(ColumnQualifier::BaseTable { database: database.into(), schema: schema.into(), alias: alias.into() }),
        qualified_only: false, unnamed: false, data_type: LogicalType::Scalar(ScalarType::I64) };
    let first = field(1, "xtdb", "public", "f");
    let second = field(2, "archive", "public", "f");
    let input = Schema::new(vec![first.clone(), second]).unwrap();
    let names = |parts: &[&str]| parts.iter().map(|part| Identifier::unquoted(*part)).collect::<Vec<_>>();
    assert_eq!(input.resolve(&names(&["xtdb", "public", "f", "v"])).unwrap(), &first);
    assert_eq!(input.resolve(&names(&["ignored", "xtdb", "public", "f", "v"])).unwrap(), &first);
    assert_eq!(input.resolve(&names(&["public", "f", "v"])).unwrap_err().code, "xtdb/ambiguous-column");
    assert_eq!(input.resolve(&names(&["f", "v"])).unwrap_err().code, "xtdb/ambiguous-column");
    assert_eq!(input.resolve(&names(&["xtdb", "other", "f", "v"])).unwrap_err().code, "xtdb/column-not-found");
    let mut derived = first.clone(); derived.qualifier = Some(ColumnQualifier::Alias("d".into()));
    let input = Schema::new(vec![derived.clone()]).unwrap();
    assert_eq!(input.resolve(&names(&["ignored", "prefix", "d", "v"])).unwrap(), &derived);
    assert!(input.resolve(&names(&["f", "v"])).is_err());
    let encoded = serde_json::to_vec(&first).unwrap();
    assert_eq!(serde_json::from_slice::<Field>(&encoded).unwrap(), first);
    assert!(first.qualifier.as_ref().unwrap().retained_bytes().unwrap() >= "xtdbpublicf".len());
}

#[test]
fn schema_and_deserialization_reject_duplicate_column_ids() {
    let field = Field { id: ColumnId(1), name: "x".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::Null };
    assert!(Schema::new(vec![field.clone(), field.clone()]).is_err());
    let encoded = serde_json::to_string(&vec![field.clone(), field]).unwrap();
    assert!(serde_json::from_str::<Schema>(&encoded).is_err());
}

#[test]
fn projection_cannot_reference_a_column_outside_its_input() {
    let field = Field { id: ColumnId(1), name: "x".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::Null };
    let projection = Arc::new(Relation::unit()).project(vec![NamedExpr { field: field.clone(), expr: field.expression() }]);
    assert_eq!(projection.unwrap_err().code, "rtdb.plan/unbound-column");
}

#[test]
fn raw_record_packing_requires_existing_columns_and_unique_output_names() {
    let field = Field { id: ColumnId(1), name: "x".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::Null };
    let expression = Expr::RowCopy(vec![field.clone()]);
    assert_eq!(expression.referenced_columns(), BTreeSet::from([field.id]));
    assert_eq!(expression.validate_columns(&BTreeSet::new()).unwrap_err().code, "rtdb.plan/unbound-column");
    assert_eq!(expression.data_type(), LogicalType::Struct(StructFields::from([("x".into(), LogicalType::Null)])));
    expression.validate_columns(&BTreeSet::from([field.id])).unwrap();
    let duplicate = Expr::RowCopy(vec![field.clone(), field.clone()]);
    assert_eq!(duplicate.validate_columns(&BTreeSet::from([field.id])).unwrap_err().code, "rtdb.plan/duplicate-record-field");
    let encoded = serde_json::to_vec(&expression).unwrap();
    assert_eq!(serde_json::from_slice::<Expr>(&encoded).unwrap(), expression);
}

#[test]
fn constant_filter_elimination_keeps_schema_and_multiplicity() {
    let unit = Arc::new(Relation::unit());
    let pass = simplify(Arc::new(unit.clone().filter(Expr::Literal(Value::Bool(true))).unwrap())).unwrap();
    assert_eq!(pass, unit);
    let empty = simplify(Arc::new(unit.clone().filter(Expr::Literal(Value::Null)).unwrap())).unwrap();
    assert_eq!(empty.schema(), unit.schema());
    assert!(matches!(empty.node(), RelationNode::Values { rows } if rows.is_empty()));
}

#[test]
fn nonconstant_filter_remains_above_temporal_scan() {
    let catalog = CatalogFixture { database: "xtdb".into(), oid: 9, ty: LogicalType::Scalar(ScalarType::I64) };
    let table = resolve_table(&catalog, &[Identifier::unquoted("prices")], &["public".into()]).unwrap();
    let mut binder = Binder::default();
    let price = binder.field("price", None, table.columns["price"].clone()).unwrap();
    let scan = Arc::new(Relation::scan(Scan {
        table, columns: vec![price.clone()], valid_time: TemporalFilter::All,
        system_time: TemporalFilter::All, clamp_valid_time: false, iids: None, predicates: Vec::new(), top_k: None, ordered_top_k: None, matches: Vec::new(),
    }).unwrap());
    let filtered = Arc::new(scan.clone().filter(Expr::binary(BinaryOp::Gt, price.expression(), Expr::Literal(Value::I64(100)))).unwrap());
    let simplified = simplify(filtered.clone()).unwrap();
    assert_eq!(simplified, filtered);
    assert!(matches!(simplified.node(), RelationNode::Filter { input, .. } if input == &scan));
}

#[test]
fn qualified_only_columns_keep_wildcard_order_and_qualified_resolution() {
    let mut binder = Binder::default();
    let left = binder.field("id", Some("l".into()), LogicalType::Scalar(ScalarType::I64)).unwrap();
    let mut right = binder.field("id", Some("r".into()), LogicalType::Scalar(ScalarType::I64).maybe()).unwrap();
    right.qualified_only = true;
    let schema = Schema::new(vec![left.clone(), right.clone()]).unwrap();
    assert_eq!(schema.resolve(&[Identifier::unquoted("id")]).unwrap(), &left);
    assert_eq!(schema.resolve(&[Identifier::unquoted("r"), Identifier::unquoted("id")]).unwrap(), &right);
    assert_eq!(schema.fields(), &[left, right]);
    assert_eq!(serde_json::from_slice::<Schema>(&serde_json::to_vec(&schema).unwrap()).unwrap(), schema);
}
