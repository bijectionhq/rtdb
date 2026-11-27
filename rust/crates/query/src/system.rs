use std::collections::{btree_map::Entry, BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use arrow_array::{ArrayRef, builder::{ArrayBuilder, BooleanBuilder, Date32Builder, DurationNanosecondBuilder, Float64Builder, Int32Builder, Int64Builder, MapBuilder, StringBuilder, StructBuilder, TimestampMicrosecondBuilder}};
use arrow_schema::Field;
use futures::{StreamExt, stream::BoxStream};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::{keys::TrieKey, log::proto::ArtefactRecord};
use rtdb_index::{CatalogTrie, Snapshot, TrieState};
use rtdb_plan::{CatalogMetric, CatalogRows, CatalogUser, Scan, TableBinding};
use rtdb_types::{Anomaly, LogicalType, PgType, Result, ScalarType, TableRef, pg_name_oid, pg_table_oid};

use crate::{columnar::{BatchCursor, Column, ColumnBatch}, context::{Execution, QueryContext}, invalid, system_schema::{definition, instant}};

type Tags = MapBuilder<StringBuilder, StringBuilder>;

enum Cell<'a> {
    Str(&'a str), Type(&'a LogicalType), TrieKey(&'a TrieKey), Char(char), I32(i32), I64(i64), F64(f64), Bool(bool), Date(i32), Duration(i64),
    Tags(&'a BTreeMap<String, String>), Temporal(&'a rtdb_format::log::proto::TemporalMetadata), Null,
}

struct CatalogColumns {
    fields: Vec<(Arc<Field>, LogicalType, Option<Box<dyn ArrayBuilder>>)>,
    rows: usize,
    reservation: Arc<MemoryReservation>,
    bound: usize,
}

impl CatalogColumns {
    fn new(scan: &Scan, catalog: &crate::context::CatalogSnapshot, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let bound = scan.columns.iter().try_fold(256, |bytes, field| {
            let layout = checked(scale(field.name.len(), 4)?, scale(field.data_type.retained_bytes()?, 8)?)?;
            checked(bytes, checked(1024, layout)?)
        })?;
        let reservation = memory.reserve(bound)?;
        let fields = scan.columns.iter().map(|field| {
            let logical = catalog.runtime_scan_type(&scan.table, field);
            let field = rtdb_columnar::to_field(&field.name, &logical)?;
            let builder: Option<Box<dyn ArrayBuilder>> = match field.data_type() {
                arrow_schema::DataType::Map(entries, _) => {
                    let arrow_schema::DataType::Struct(fields) = entries.data_type() else { unreachable!() };
                    let names = arrow_array::builder::MapFieldNames { entry: entries.name().clone(), key: fields[0].name().clone(), value: fields[1].name().clone() };
                    Some(Box::new(Tags::new(Some(names), StringBuilder::with_capacity(0, 0), StringBuilder::with_capacity(0, 0))
                        .with_keys_field(Arc::clone(&fields[0])).with_values_field(Arc::clone(&fields[1]))))
                }
                arrow_schema::DataType::Null => None,
                ty => Some(arrow_array::builder::make_builder(ty, 0)),
            };
            Ok((Arc::new(field), logical, builder))
        }).collect::<Result<_>>()?;
        Ok(Self { fields, rows: 0, reservation, bound })
    }

    fn push<'a>(&mut self, mut cell: impl FnMut(&str) -> Result<Cell<'a>>) -> Result<()> {
        for (field, _, builder) in &mut self.fields {
            let Some(builder) = builder else { continue; };
            let value = cell(field.name())?;
            let _text_scratch = match &value {
                Cell::Type(ty) => Some(self.reservation.budget().reserve(type_scratch_bound(ty)?)?),
                Cell::TrieKey(key) => Some(self.reservation.budget().reserve(checked(512, scale(key.part().len(), 4)?)?)?),
                _ => None,
            };
            let rendered = match &value {
                Cell::Type(ty) => Some(ty.to_edn()?),
                Cell::TrieKey(key) => Some(key.to_string()),
                _ => None,
            };
            let value = match rendered.as_deref() {
                Some(text) => Cell::Str(text),
                None => value,
            };
            let buffer_bytes = match &value {
                Cell::Str(value) => checked(5, value.len())?,
                Cell::Char(value) => 5 + value.len_utf8(),
                Cell::I32(_) | Cell::Date(_) => 5,
                Cell::I64(_) | Cell::F64(_) | Cell::Duration(_) => 9,
                Cell::Bool(_) => 2,
                Cell::Tags(tags) => tags.iter().try_fold(5, |bytes, (key, value)| {
                    checked(bytes, checked(10, checked(key.len(), value.len())?)?)
                })?,
                Cell::Temporal(_) => 55,
                Cell::Null => match field.data_type() {
                    arrow_schema::DataType::Struct(children) => checked(1, scale(children.len(), 9)?)?,
                    _ => 9,
                },
                Cell::Type(_) | Cell::TrieKey(_) => unreachable!(),
            };
            // Buffer growth can briefly keep both the old and new capacities live.
            self.bound = checked(self.bound, scale(buffer_bytes, 4)?)?;
            Arc::get_mut(&mut self.reservation).expect("A catalog page owns its allocation").grow_to(self.bound)?;
            let builder = builder.as_any_mut();
            macro_rules! primitive {
                ($builder:ty, $value:expr) => {{
                    builder.downcast_mut::<$builder>().ok_or_else(catalog_layout)?.append_value($value);
                }};
            }
            match value {
                Cell::Str(value) => primitive!(StringBuilder, value),
                Cell::Char(value) => primitive!(StringBuilder, value.encode_utf8(&mut [0; 4])),
                Cell::I32(value) => primitive!(Int32Builder, value),
                Cell::I64(value) => primitive!(Int64Builder, value),
                Cell::F64(value) => primitive!(Float64Builder, value),
                Cell::Bool(value) => primitive!(BooleanBuilder, value),
                Cell::Date(value) => primitive!(Date32Builder, value),
                Cell::Duration(value) => primitive!(DurationNanosecondBuilder, value),
                Cell::Tags(tags) => {
                    let map = builder.downcast_mut::<Tags>().ok_or_else(catalog_layout)?;
                    for (key, value) in tags { map.keys().append_value(key); map.values().append_value(value); }
                    map.append(true).map_err(catalog_arrow)?;
                }
                Cell::Temporal(metadata) => {
                    let structure = builder.downcast_mut::<StructBuilder>().ok_or_else(catalog_layout)?;
                    let arrow_schema::DataType::Struct(fields) = field.data_type() else { return Err(catalog_layout()); };
                    for (index, child) in fields.iter().enumerate() {
                        let value = match child.name().as_str() {
                            "min_valid_from" => metadata.min_valid_from.unwrap_or(i64::MAX),
                            "max_valid_from" => metadata.max_valid_from.unwrap_or(i64::MIN),
                            "min_valid_to" => metadata.min_valid_to.unwrap_or(i64::MAX),
                            "max_valid_to" => metadata.max_valid_to.unwrap_or(i64::MIN),
                            "min_system_from" => metadata.min_system_from.unwrap_or(i64::MAX),
                            "max_system_from" => metadata.max_system_from.unwrap_or(i64::MIN),
                            _ => return Err(catalog_layout()),
                        };
                        structure.field_builder::<TimestampMicrosecondBuilder>(index).ok_or_else(catalog_layout)?.append_value(value);
                    }
                    structure.append(true);
                }
                Cell::Null => {
                    if let Some(builder) = builder.downcast_mut::<StringBuilder>() { builder.append_null(); }
                    else if let Some(builder) = builder.downcast_mut::<Int32Builder>() { builder.append_null(); }
                    else if let Some(builder) = builder.downcast_mut::<Int64Builder>() { builder.append_null(); }
                    else if let Some(builder) = builder.downcast_mut::<Float64Builder>() { builder.append_null(); }
                    else if let Some(builder) = builder.downcast_mut::<BooleanBuilder>() { builder.append_null(); }
                    else if let Some(builder) = builder.downcast_mut::<Date32Builder>() { builder.append_null(); }
                    else if let Some(builder) = builder.downcast_mut::<DurationNanosecondBuilder>() { builder.append_null(); }
                    else if let Some(builder) = builder.downcast_mut::<TimestampMicrosecondBuilder>() { builder.append_null(); }
                    else if let Some(builder) = builder.downcast_mut::<StructBuilder>() {
                        for index in 0..builder.num_fields() {
                            builder.field_builder::<TimestampMicrosecondBuilder>(index).ok_or_else(catalog_layout)?.append_null();
                        }
                        builder.append(false);
                    }
                    else if let Some(builder) = builder.downcast_mut::<arrow_array::builder::ListBuilder<Box<dyn ArrayBuilder>>>() { builder.append(false); }
                    else { return Err(catalog_layout()); }
                }
                Cell::Type(_) | Cell::TrieKey(_) => unreachable!(),
            }
        }
        self.rows += 1;
        Ok(())
    }

    fn finish(self) -> Result<ColumnBatch> {
        let Self { fields, rows, mut reservation, .. } = self;
        let mut columns = Vec::with_capacity(fields.len());
        let mut retained = checked(256, scale(columns.capacity(), std::mem::size_of::<Column>())?)?;
        for (field, logical, builder) in fields {
            let array: ArrayRef = builder.map_or_else(|| arrow_array::new_null_array(field.data_type(), rows), |mut builder| builder.finish());
            let actual = Arc::new(field.as_ref().clone().with_data_type(array.data_type().clone())
                .with_nullable(field.is_nullable() || array.null_count() != 0));
            let logical = (field.data_type() == actual.data_type() && logical != LogicalType::Nothing)
                .then(|| if actual.is_nullable() && !field.is_nullable() { logical.maybe() } else { logical });
            retained = checked(retained, checked(array.get_array_memory_size(), actual.size())?)?;
            retained = checked(retained, logical.as_ref().map_or(Ok(0), LogicalType::retained_bytes)?)?;
            columns.push(Column::Arrow { array, field: actual, logical });
        }
        if retained > reservation.bytes() { return Err(Anomaly::fault("rtdb.query/catalog-allocation-bound", "System catalog rows exceed their reserved allocation")); }
        Arc::get_mut(&mut reservation).expect("A catalog batch owns its allocation").shrink_to(retained)?;
        Ok(ColumnBatch::sharing(columns, rows, reservation))
    }
}

struct CatalogPages<'a> {
    context: &'a QueryContext,
    scan: &'a Scan,
    page: Option<CatalogColumns>,
}

impl CatalogPages<'_> {
    fn push<'a>(&mut self, cell: impl FnMut(&str) -> Result<Cell<'a>>) -> Result<Option<ColumnBatch>> {
        self.context.check_cancelled_at(self.page.as_ref().map_or(0, |page| page.rows))?;
        let page = match &mut self.page {
            Some(page) => page,
            slot @ None => slot.insert(CatalogColumns::new(self.scan, &self.context.catalog, &self.context.memory)?),
        };
        page.push(cell)?;
        if page.rows == self.context.options.batch_rows { self.finish() } else { Ok(None) }
    }

    fn finish(&mut self) -> Result<Option<ColumnBatch>> {
        self.page.take().map(CatalogColumns::finish).transpose()
    }
}

fn catalog_layout() -> Anomaly { invalid("catalog-layout", "Catalog value differs from its Arrow field") }
fn catalog_arrow(error: arrow_schema::ArrowError) -> Anomaly { invalid("catalog-layout", error) }

macro_rules! record {
    ($output:expr, [$(($name:literal, $value:expr)),* $(,)?]) => {{
        $output.push(|name| Ok(match name {
            $($name => $value,)*
            _ => Cell::Null,
        }))
    }};
}

enum RuntimeRows {
    None,
    Users(CatalogRows<CatalogUser>),
    Roles { users: CatalogRows<CatalogUser>, memberships: CatalogRows<(String, String)> },
    Memberships(CatalogRows<(String, String)>),
    Metrics(CatalogRows<CatalogMetric>),
    OrderedIndexes(CatalogRows<crate::OrderedIndexStorage>),
}

pub(crate) struct SystemCursor {
    scan: Option<Scan>,
    context: Arc<QueryContext>,
    pages: Option<(BoxStream<'static, Result<ColumnBatch>>, Arc<MemoryReservation>)>,
}

impl SystemCursor {
    pub fn new(scan: Scan, execution: Execution) -> Self {
        Self { scan: Some(scan), context: execution.context, pages: None }
    }
}

#[async_trait]
impl BatchCursor for SystemCursor {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        if let Err(error) = self.context.check_cancelled() {
            self.scan = None;
            self.pages = None;
            return Err(error);
        }
        if let Some(scan) = self.scan.take() {
            let pages = catalog_pages(scan, Arc::clone(&self.context));
            let reservation = self.context.memory.reserve(checked(128, std::mem::size_of_val(&pages))?)?;
            self.pages = Some((Box::pin(pages), reservation));
        }
        let Some((pages, _)) = &mut self.pages else { return Ok(None); };
        let next = pages.next().await.transpose();
        if !matches!(&next, Ok(Some(_))) { self.pages = None; }
        next
    }
}

async fn runtime_rows(scan: &Scan, context: &QueryContext) -> Result<RuntimeRows> {
    let table = &scan.table.name.table;
    let runtime = || context.catalog.runtime.as_ref().ok_or_else(|| {
        Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, "rtdb.query/catalog-runtime-unavailable", "Runtime catalog provider was not supplied")
    });
    Ok(match (table.schema.as_str(), table.table.as_str()) {
        ("pg_catalog", "pg_user") => RuntimeRows::Users(runtime()?.users()?),
        ("pg_catalog", "pg_roles") => RuntimeRows::Roles { users: runtime()?.users()?, memberships: runtime()?.memberships().await? },
        ("pg_catalog", "pg_auth_members") => RuntimeRows::Memberships(runtime()?.memberships().await?),
        ("xt", "metrics_timers" | "metrics_gauges" | "metrics_counters") => RuntimeRows::Metrics(runtime()?.sample().await?),
        ("xt", "live_ordered_index_storage") => {
            let inventory = context.catalog.ordered_indexes.as_ref().ok_or_else(|| {
                Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, "rtdb.query/catalog-runtime-unavailable", "Ordered index inventory provider was not supplied")
            })?;
            RuntimeRows::OrderedIndexes(inventory.sample(&scan.table.name.database, context).await?)
        }
        _ => RuntimeRows::None,
    })
}

fn catalog_pages(scan: Scan, context: Arc<QueryContext>) -> impl futures::Stream<Item = Result<ColumnBatch>> + Send {
    async_stream::try_stream! {
        let database = &scan.table.name.database;
        let table = &scan.table.name.table;
        let kind = (table.schema.as_str(), table.table.as_str());
        let runtime = runtime_rows(&scan, &context).await?;
        context.check_cancelled()?;

        let names = match kind {
            ("information_schema", "tables" | "columns")
            | ("pg_catalog", "pg_tables" | "pg_class" | "pg_attribute" | "pg_stat_user_tables")
            | ("xt", "live_tables" | "live_columns" | "trie_stats" | "live_trie_artefacts") => {
                let names = context.catalog.table_names_reserved(database, &context.memory)?;
                for (index, table) in names.rows().iter().enumerate() {
                    context.check_cancelled_at(index)?;
                    context.catalog.catalog_table_oid(database, table)?;
                }
                Some(names)
            }
            _ => None,
        };
        let names = names.as_ref().map_or(&[][..], CatalogRows::rows);
        let tables = || names.iter().filter(|name| visible_table(name));
        let physical_tables = || names.iter().filter(|name| definition(name).is_none());
        let projected = |name: &str| scan.columns.iter().any(|column| column.name == name);
        let mut output = CatalogPages { context: &context, scan: &scan, page: None };

        match kind {
            ("information_schema", "tables") => for table in tables() {
                if let Some(page) = record!(output, [
                    ("table_catalog", Cell::Str(database)), ("table_schema", Cell::Str(&table.schema)), ("table_name", Cell::Str(&table.table)),
                    ("table_type", Cell::Str(if definition(table).is_some() { "VIEW" } else { "BASE TABLE" })),
                ])? { yield page; }
            },
            ("information_schema", "schemata") => for schema in namespaces() {
                if let Some(page) = record!(output, [("catalog_name", Cell::Str(database)), ("schema_name", Cell::Str(schema)), ("schema_owner", Cell::Str("xtdb"))])? { yield page; }
            },
            ("information_schema", "columns") | ("pg_catalog", "pg_attribute") => {
                let bindings = column_bindings(&context, database, tables())?;
                let information_schema = table.schema == "information_schema";
                let pg_projected = if information_schema {
                    ["data_type", "udt_name", "numeric_precision", "numeric_precision_radix", "numeric_scale"].into_iter().any(projected)
                } else {
                    ["atttypid", "attlen"].into_iter().any(projected)
                };
                let xt_type_projected = information_schema && projected("xt_type");
                let _defaults = context.memory.reserve(512)?;
                let defaults = temporal_defaults();
                // A later field's invalid type must still fail the first pull, even for a short read.
                for binding in bindings.rows() {
                    context.check_cancelled()?;
                    let order = column_order(binding, &context.memory)?;
                    for (index, (_, ty)) in ordered_columns(binding, &defaults, order.as_ref()).enumerate() {
                        context.check_cancelled_at(index)?;
                        ordinal(index)?;
                        validate_pg_type(ty)?;
                        if xt_type_projected { ty.validate_type_form()?; }
                    }
                }

                for binding in bindings.rows() {
                    context.check_cancelled()?;
                    let order = column_order(binding, &context.memory)?;
                    for (index, (name, ty)) in ordered_columns(binding, &defaults, order.as_ref()).enumerate() {
                        let ordinal = ordinal(index)?;
                        let pg = if pg_projected {
                            let _scratch = context.memory.reserve(type_scratch_bound(ty)?)?;
                            column_pg_type(ty)?
                        } else {
                            PgType::Text
                        };
                        let page = if information_schema {
                            let precision = match pg { PgType::Int2 => Some(16), PgType::Int4 => Some(32), PgType::Int8 => Some(64), PgType::Float4 => Some(24), PgType::Float8 => Some(53), _ => None };
                            record!(output, [
                                ("table_catalog", Cell::Str(database)), ("table_schema", Cell::Str(&binding.name.table.schema)), ("table_name", Cell::Str(&binding.name.table.table)),
                                ("column_name", Cell::Str(name)), ("data_type", Cell::Str(pg.data_type())), ("udt_schema", Cell::Str("pg_catalog")), ("udt_name", Cell::Str(pg.descriptor().name)),
                                ("ordinal_position", Cell::I32(ordinal)), ("column_default", Cell::Null), ("is_nullable", Cell::Str(if column_nullable(ty) { "YES" } else { "NO" })),
                                ("is_identity", Cell::Str("NO")), ("is_generated", Cell::Str("NEVER")), ("numeric_precision", precision.map(Cell::I32).unwrap_or(Cell::Null)),
                                ("numeric_precision_radix", precision.map(|_| Cell::I32(2)).unwrap_or(Cell::Null)),
                                ("numeric_scale", if matches!(pg, PgType::Int2 | PgType::Int4 | PgType::Int8) { Cell::I32(0) } else { Cell::Null }),
                                ("xt_type", Cell::Type(ty)),
                            ])?
                        } else {
                            let descriptor = pg.descriptor();
                            record!(output, [
                                ("attrelid", Cell::I32(pg_table_oid(binding.oid, &binding.name.table))), ("attname", Cell::Str(name)),
                                ("atttypid", Cell::I32(descriptor.oid)), ("attlen", Cell::I32(i32::from(descriptor.length))),
                                ("attnum", Cell::I32(ordinal)), ("attisdropped", Cell::Bool(false)), ("attnotnull", Cell::Bool(false)),
                                ("atttypmod", Cell::I32(-1)), ("attidentity", Cell::Str("")), ("attgenerated", Cell::Str("")), ("attcompression", Cell::Str("")),
                            ])?
                        };
                        if let Some(page) = page { yield page; }
                    }
                }
            }
            ("pg_catalog", "pg_tables") => for table in tables() {
                if let Some(page) = record!(output, [("schemaname", Cell::Str(&table.schema)), ("tablename", Cell::Str(&table.table)), ("tableowner", Cell::Str("xtdb")), ("tablespace", Cell::Null)])? { yield page; }
            },
            ("pg_catalog", "pg_class") => for table in tables() {
                if let Some(page) = record!(output, [
                    ("oid", Cell::I32(pg_table_oid(context.catalog.catalog_table_oid(database, table)?, table))), ("relname", Cell::Str(&table.table)), ("relnamespace", Cell::I32(pg_name_oid(&table.schema))),
                    ("relowner", Cell::I32(pg_name_oid("xtdb"))), ("relkind", Cell::Str("r")), ("relam", Cell::I32(2)), ("relchecks", Cell::I32(0)),
                    ("relhasindex", Cell::Bool(true)), ("relhasrules", Cell::Bool(false)), ("relhastriggers", Cell::Bool(false)), ("relrowsecurity", Cell::Bool(false)),
                    ("relforcerowsecurity", Cell::Bool(false)), ("relispartition", Cell::Bool(false)), ("reltablespace", Cell::I32(0)), ("reloftype", Cell::I32(0)),
                    ("relpersistence", Cell::Str("p")), ("relreplident", Cell::Str("d")), ("reltoastrelid", Cell::I32(0)),
                ])? { yield page; }
            },
            ("pg_catalog", "pg_type") => for ty in PgType::ALL {
                let descriptor = ty.descriptor();
                if let Some(page) = record!(output, [
                    ("oid", Cell::I32(descriptor.oid)), ("typname", Cell::Str(descriptor.name)), ("typnamespace", Cell::I32(pg_name_oid("pg_catalog"))), ("typowner", Cell::I32(pg_name_oid("xtdb"))),
                    ("typtype", Cell::Str("b")), ("typcategory", Cell::Char(descriptor.category)), ("typbasetype", Cell::I32(0)), ("typnotnull", Cell::Bool(false)), ("typtypmod", Cell::I32(-1)),
                    ("typrelid", Cell::I32(0)), ("typelem", Cell::I32(descriptor.element_oid.unwrap_or(0))), ("typarray", Cell::I32(descriptor.array_oid.unwrap_or(0))),
                    ("typsend", Cell::Str(descriptor.send)), ("typreceive", Cell::Str(descriptor.receive)), ("typinput", Cell::Str(descriptor.input.unwrap_or(""))), ("typoutput", Cell::Str(descriptor.output.unwrap_or(""))),
                ])? { yield page; }
            },
            ("pg_catalog", "pg_namespace") => for namespace in namespaces() {
                if let Some(page) = record!(output, [("oid", Cell::I32(pg_name_oid(namespace))), ("nspname", Cell::Str(namespace)), ("nspowner", Cell::I32(pg_name_oid("xtdb"))), ("nspacl", Cell::Null)])? { yield page; }
            },
            ("pg_catalog", "pg_proc") => {
                if let Some(page) = record!(output, [("oid", Cell::I32(pg_name_oid("array_in"))), ("proname", Cell::Str("array_in")), ("pronamespace", Cell::I32(pg_name_oid("pg_catalog")))])? { yield page; }
            }
            ("pg_catalog", "pg_database") => for database in context.catalog.database_names() {
                if let Some(page) = record!(output, [("oid", Cell::I32(pg_name_oid(database))), ("datname", Cell::Str(database)), ("datallowconn", Cell::Bool(true)), ("datistemplate", Cell::Bool(false)), ("datcollate", Cell::Str("C"))])? { yield page; }
            },
            ("pg_catalog", "pg_stat_user_tables") => for table in tables() {
                if let Some(page) = record!(output, [("relid", Cell::I32(pg_table_oid(context.catalog.catalog_table_oid(database, table)?, table))), ("schemaname", Cell::Str(&table.schema)), ("relname", Cell::Str(&table.table)), ("n_live_tup", Cell::I64(0))])? { yield page; }
            },
            ("pg_catalog", "pg_settings") => {
                if let Some(page) = record!(output, [("name", Cell::Str("max_index_keys")), ("setting", Cell::Str("32"))])? { yield page; }
            }
            ("pg_catalog", "pg_range") => {
                if let Some(page) = record!(output, [("rngtypid", Cell::I32(3910)), ("rngsubtype", Cell::I32(1184)), ("rngmultitypid", Cell::I32(4534)), ("rngcollation", Cell::I32(0)), ("rngsubopc", Cell::I32(3127)), ("rngcanonical", Cell::Str("")), ("rngsubdiff", Cell::Str("tstzrange_subdiff"))])? { yield page; }
            }
            ("pg_catalog", "pg_am") => for (oid, name, handler, ty) in [(2, "heap", "heap_tableam_handler", "t"), (403, "btree", "bthandler", "i"), (405, "hash", "hashhandler", "i")] {
                if let Some(page) = record!(output, [("oid", Cell::I32(oid)), ("amname", Cell::Str(name)), ("amhandler", Cell::Str(handler)), ("amtype", Cell::Str(ty))])? { yield page; }
            },
            ("pg_catalog", "pg_user") => {
                let users = match &runtime {
                    RuntimeRows::Users(users) => users,
                    _ => Err(invalid("catalog-runtime", "User snapshot is absent"))?,
                };
                for user in users.rows() {
                    if let Some(page) = record!(output, [("usename", Cell::Str(&user.name)), ("usesuper", Cell::Bool(user.superuser)), ("passwd", Cell::Null)])? { yield page; }
                }
            }
            ("pg_catalog", "pg_roles") => {
                let (users, memberships) = match &runtime {
                    RuntimeRows::Roles { users, memberships } => (users, memberships),
                    _ => Err(invalid("catalog-runtime", "Role snapshot is absent"))?,
                };
                let entries = checked(users.rows().len(), scale(memberships.rows().len(), 2)?)?;
                let entry_bytes = 128 + std::mem::size_of::<(&str, bool, bool)>();
                let _inventory = context.memory.reserve(checked(512, scale(entries, entry_bytes)?)?)?;
                let mut login = BTreeMap::new();
                for (index, user) in users.rows().iter().enumerate() {
                    context.check_cancelled_at(index)?;
                    login.insert(user.name.as_str(), user.superuser);
                }
                for (index, (user, _)) in memberships.rows().iter().enumerate() {
                    context.check_cancelled_at(index)?;
                    login.entry(user.as_str()).or_insert_with(|| context.catalog.runtime.as_ref().is_some_and(|runtime| runtime.is_superuser(user)));
                }
                let mut roles = BTreeSet::new();
                for (index, (_, role)) in memberships.rows().iter().enumerate() {
                    context.check_cancelled_at(index)?;
                    if !login.contains_key(role.as_str()) { roles.insert(role.as_str()); }
                }
                let mut rows: Vec<_> = login.into_iter().map(|(name, superuser)| (name, superuser, true))
                    .chain(roles.into_iter().map(|name| (name, false, false))).collect();
                rows.sort_unstable_by(|(left, _, left_login), (right, _, right_login)| {
                    right_login.cmp(left_login).then_with(|| catalog_name_order(left, right))
                });
                for (name, superuser, login) in rows {
                    if let Some(page) = record!(output, [("oid", Cell::I32(pg_name_oid(name))), ("rolname", Cell::Str(name)), ("rolsuper", Cell::Bool(superuser)), ("rolcanlogin", Cell::Bool(login))])? { yield page; }
                }
            }
            ("pg_catalog", "pg_auth_members") => {
                let memberships = match &runtime {
                    RuntimeRows::Memberships(memberships) => memberships,
                    _ => Err(invalid("catalog-runtime", "Membership snapshot is absent"))?,
                };
                for (user, role) in memberships.rows() {
                    if let Some(page) = record!(output, [("roleid", Cell::I32(pg_name_oid(role))), ("member", Cell::I32(pg_name_oid(user))), ("grantor", Cell::Null), ("admin_option", Cell::Bool(false))])? { yield page; }
                }
            }
            ("xt", "metrics_timers" | "metrics_gauges" | "metrics_counters") => {
                let metrics = match &runtime {
                    RuntimeRows::Metrics(metrics) => metrics,
                    _ => Err(invalid("catalog-runtime", "Telemetry snapshot is absent"))?,
                };
                for (index, metric) in metrics.rows().iter().enumerate() {
                    context.check_cancelled_at(index)?;
                    if let Some(page) = metric_record(&mut output, &table.table, metric)? { yield page; }
                }
            }
            ("xt", "live_trie_artefacts") => {
                let snapshots = &context.catalog.database(database)?.partitions;
                let (files, _inventory) = artefact_inventory(&context, snapshots, tables())?;
                for ((table, object), (historical, artefact)) in files {
                    if let Some(page) = record!(output, [
                        ("schema_name", Cell::Str(&table.schema)), ("table_name", Cell::Str(&table.table)),
                        ("object_key", Cell::Str(&object)), ("kind", Cell::Str(artefact.kind())),
                        ("definition_id", Cell::Str(artefact.definition_id())), ("definition_version", Cell::I64(artefact.definition_version())),
                        ("historical", Cell::Bool(historical)), ("file_size", artefact.file_size.map(Cell::I64).unwrap_or(Cell::Null)),
                    ])? { yield page; }
                }
            }
            ("xt", "live_ordered_index_storage") => {
                let indexes = match &runtime {
                    RuntimeRows::OrderedIndexes(indexes) => indexes,
                    _ => Err(invalid("catalog-runtime", "Ordered index inventory is absent"))?,
                };
                for index in indexes.rows() {
                    if let Some(page) = record!(output, [
                        ("schema_name", Cell::Str(&index.table.schema)), ("table_name", Cell::Str(&index.table.table)),
                        ("index_name", Cell::Str(&index.name)), ("definition_version", Cell::I64(index.version)),
                        ("through_block", Cell::I64(index.through_block)), ("file_bytes", Cell::I64(index.bytes)),
                    ])? { yield page; }
                }
            }
            ("xt", "live_tables") => for snapshot in &context.catalog.database(database)?.partitions {
                for (index, table) in physical_tables().enumerate() {
                    context.check_cancelled_at(index)?;
                    let count: i64 = snapshot.iter_segments(table).map(|segment| i64::from(segment.row_count())).sum();
                    if count != 0 {
                        if let Some(page) = record!(output, [("schema_name", Cell::Str(&table.schema)), ("table_name", Cell::Str(&table.table)), ("row_count", Cell::I64(count))])? { yield page; }
                    }
                }
            },
            ("xt", "live_columns") => {
                let snapshots = &context.catalog.database(database)?.partitions;
                if projected("col_type") {
                    for snapshot in snapshots {
                        for (index, table) in physical_tables().enumerate() {
                            context.check_cancelled_at(index)?;
                            for segment in snapshot.iter_segments(table).filter(|segment| segment.row_count() > 0) {
                                for (index, ty) in segment.column_types().values().enumerate() {
                                    context.check_cancelled_at(index)?;
                                    ty.validate_type_form()?;
                                }
                            }
                        }
                    }
                }
                for snapshot in snapshots {
                    for (index, table) in physical_tables().enumerate() {
                        context.check_cancelled_at(index)?;
                        for segment in snapshot.iter_segments(table).filter(|segment| segment.row_count() > 0) {
                            for (column, ty) in segment.column_types() {
                                if let Some(page) = record!(output, [("schema_name", Cell::Str(&table.schema)), ("table_name", Cell::Str(&table.table)), ("col_name", Cell::Str(column)), ("col_type", Cell::Type(ty))])? { yield page; }
                            }
                        }
                    }
                }
            }
            ("xt", "trie_stats") => {
                let snapshots = &context.catalog.database(database)?.partitions;
                if projected("level") {
                    for snapshot in snapshots {
                        for (index, table) in physical_tables().enumerate() {
                            context.check_cancelled_at(index)?;
                            for (index, trie) in snapshot.trie_catalog().iter_tries(table).enumerate() {
                                context.check_cancelled_at(index)?;
                                trie_level(trie)?;
                            }
                        }
                    }
                }
                for snapshot in snapshots {
                    for (index, table) in physical_tables().enumerate() {
                        context.check_cancelled_at(index)?;
                        let catalog = snapshot.trie_catalog();
                        let count = catalog.iter_tries(table).count();
                        if count == 0 { continue; }
                        let _inventory = context.memory.reserve(checked(256, scale(count, 4 * std::mem::size_of::<&CatalogTrie>())?)?)?;
                        for trie in catalog.all_tries(table) {
                            if let Some(page) = trie_record(&mut output, table, trie)? { yield page; }
                        }
                    }
                }
            }
            ("information_schema", "table_constraints" | "key_column_usage" | "constraint_column_usage")
            | ("pg_catalog", "pg_description" | "pg_views" | "pg_matviews" | "pg_depend" | "pg_enum" | "pg_constraint" | "pg_extension" | "pg_attrdef" | "pg_sequence" | "pg_collation" | "pg_opclass" | "pg_index") => {}
            _ => Err(Anomaly::fault("rtdb.query/catalog-table", "Declared system catalog has no row producer"))?,
        }

        if let Some(page) = output.finish()? { yield page; }
    }
}

fn namespaces() -> impl Iterator<Item = &'static str> { ["information_schema", "pg_catalog", "public", "xt"].into_iter() }

fn visible_table(table: &TableRef) -> bool {
    definition(table).is_none_or(|definition| definition.schema != "xt")
}

fn column_bindings<'a>(context: &QueryContext, database: &str, tables: impl Iterator<Item = &'a TableRef> + Clone) -> Result<CatalogRows<TableBinding>> {
    let count = tables.clone().count();
    let bytes = checked(256, scale(count, std::mem::size_of::<TableBinding>())?)?;
    let mut inventory = context.memory.reserve(bytes)?;
    let mut bindings = Vec::with_capacity(count);
    for table in tables {
        context.check_cancelled()?;
        let (binding, mut grant) = context.catalog.binding_reserved(database, table, &context.memory)?
            .ok_or_else(|| invalid("missing-table", "Enumerated table has no binding"))?;
        let bytes = [
            binding.name.database.capacity(), binding.name.table.schema.capacity(), binding.name.table.table.capacity(),
            binding.columns.table_bytes_bound().ok_or_else(size_error)?,
        ].into_iter().try_fold(0, checked)?;
        let bytes = binding.columns.iter().try_fold(bytes, |bytes, (name, ty)| {
            checked(bytes, checked(name.capacity(), scale(ty.retained_bytes()?, 4)?)?)
        })?;
        let grant = Arc::get_mut(&mut grant).expect("A new binding owns its allocation");
        grant.shrink_to(bytes)?;
        Arc::get_mut(&mut inventory).expect("The binding inventory owns its allocation").absorb(grant)?;
        bindings.push(binding);
    }
    Ok(CatalogRows::new(bindings, inventory))
}

type CatalogField<'a> = (&'a str, &'a LogicalType);

fn catalog_name_order(left: &str, right: &str) -> std::cmp::Ordering {
    left.encode_utf16().cmp(right.encode_utf16())
}

fn ordinal_order(left: &str, right: &str) -> std::cmp::Ordering {
    right.starts_with('_').cmp(&left.starts_with('_'))
        .then_with(|| catalog_name_order(left, right))
}

fn column_order<'a>(binding: &'a TableBinding, memory: &Arc<MemoryBudget>) -> Result<Option<CatalogRows<CatalogField<'a>>>> {
    if [true, false].into_iter().all(|system| {
        binding.columns.keys().filter(|name| name.starts_with('_') == system)
            .is_sorted_by(|left, right| !catalog_name_order(left, right).is_gt())
    }) {
        return Ok(None);
    }
    // StructFields preserves insertion order. Only out-of-order bindings need a permutation.
    let inventory = memory.reserve(checked(256, scale(binding.columns.len(), std::mem::size_of::<CatalogField<'_>>())?)?)?;
    let mut ordered = binding.columns.iter().map(|(name, ty)| (name.as_str(), ty)).collect::<Vec<_>>();
    ordered.sort_unstable_by(|(left, _), (right, _)| ordinal_order(left, right));
    Ok(Some(CatalogRows::new(ordered, inventory)))
}

fn temporal_defaults() -> [(&'static str, LogicalType); 4] {
    [("_system_from", instant()), ("_system_to", instant().maybe()),
        ("_valid_from", instant()), ("_valid_to", instant().maybe())]
}

fn ordered_columns<'a>(
    binding: &'a TableBinding,
    defaults: &'a [(&'static str, LogicalType); 4],
    order: Option<&'a CatalogRows<CatalogField<'a>>>,
) -> impl Iterator<Item = CatalogField<'a>> {
    let by_prefix = |system| binding.columns.iter().filter(move |(name, _)| name.starts_with('_') == system).map(|(name, ty)| (name.as_str(), ty));
    let mut explicit = by_prefix(true).chain(by_prefix(false));
    let mut sorted = order.into_iter().flat_map(|order| order.rows().iter().copied());
    let mut fields = std::iter::from_fn(move || if order.is_some() { sorted.next() } else { explicit.next() }).peekable();
    let table = &binding.name.table;
    let has_defaults = !matches!(table.schema.as_str(), "xt" | "pg_catalog" | "information_schema")
        || table.schema == "xt" && table.table == "txs";
    let mut defaults = defaults.iter().filter(move |(name, _)| has_defaults && !binding.columns.contains_key(*name))
        .map(|(name, ty)| (*name, ty)).peekable();
    std::iter::from_fn(move || match (fields.peek(), defaults.peek()) {
        (Some((field, _)), Some((default, _))) if ordinal_order(default, field).is_lt() => defaults.next(),
        (Some(_), _) => fields.next(),
        (None, _) => defaults.next(),
    })
}

fn ordinal(index: usize) -> Result<i32> { i32::try_from(checked(index, 1)?).map_err(|_| size_error()) }

fn column_pg_type(ty: &LogicalType) -> Result<PgType> {
    Ok(match PgType::from_logical(ty)? { PgType::Default => PgType::Json, PgType::Null => PgType::Text, ty => ty })
}

fn column_nullable(ty: &LogicalType) -> bool {
    match ty {
        LogicalType::Null | LogicalType::Maybe(_) => true,
        LogicalType::RunEndEncoded { value, .. } => column_nullable(value),
        LogicalType::Union(legs) => legs.iter().any(column_nullable),
        _ => false,
    }
}

fn validate_pg_type(ty: &LogicalType) -> Result<()> {
    match ty {
        LogicalType::Maybe(value) | LogicalType::RunEndEncoded { value, .. } => validate_pg_type(value),
        LogicalType::Union(legs) => legs.iter().try_for_each(validate_pg_type),
        LogicalType::Scalar(ScalarType::I8 | ScalarType::U8 | ScalarType::F16) => {
            Err(Anomaly::fault("rtdb.types/pg-type", "PostgreSQL has no mapping for this numeric width"))
        }
        _ => Ok(()),
    }
}

fn type_scratch_bound(ty: &LogicalType) -> Result<usize> { checked(512, scale(ty.retained_bytes()?, 32)?) }

type ArtefactFiles<'a> = BTreeMap<(&'a TableRef, String), (bool, &'a ArtefactRecord)>;

fn artefact_inventory<'a>(
    context: &QueryContext, snapshots: &'a [Snapshot], tables: impl Iterator<Item = &'a TableRef> + Clone,
) -> Result<(ArtefactFiles<'a>, Arc<MemoryReservation>)> {
    let mut inventory = context.memory.reserve(1024)?;
    let mut files = BTreeMap::new();
    for snapshot in snapshots {
        for (index, table) in tables.clone().enumerate() {
            context.check_cancelled_at(index)?;
            let Some(entry) = snapshot.table_entry(table) else { continue; };
            for (index, trie) in snapshot.trie_catalog().iter_tries(table).enumerate() {
                context.check_cancelled_at(index)?;
                if trie.state != TrieState::Live { continue; }
                let historical = trie.key.recency().is_some();
                for artefact in &trie.artefacts {
                    context.check_cancelled()?;
                    let names = [entry.slug.as_str().len(), trie.key.part().len(), artefact.kind().len(), artefact.definition_id().len()]
                        .into_iter().try_fold(0, checked)?;
                    let names = artefact.pack_members.iter().try_fold(names, |n, member| checked(n, member.len()))?;
                    let _key_scratch = context.memory.reserve(checked(1024, scale(names, 16)?)?)?;
                    let object = entry.slug.recorded_artefact_key(&trie.key, artefact)?;
                    if artefact.file_size.is_some_and(|size| size < 0) {
                        return Err(invalid("artefact-size", "A published artefact has a negative file size"));
                    }
                    let key_bytes = object.capacity();
                    match files.entry((table, object)) {
                        Entry::Vacant(file) => {
                            let bytes = checked(inventory.bytes(), checked(256, key_bytes)?)?;
                            Arc::get_mut(&mut inventory).expect("The artefact inventory owns its allocation").grow_to(bytes)?;
                            file.insert((historical, artefact));
                        }
                        Entry::Occupied(mut file) => match (file.get().1.file_size, artefact.file_size) {
                            (Some(first), Some(next)) if first != next => {
                                return Err(invalid("artefact-size", "Published sizes disagree for one artefact"));
                            }
                            (None, Some(_)) => { file.insert((historical, artefact)); }
                            _ => {}
                        },
                    }
                }
            }
        }
    }
    Ok((files, inventory))
}

fn trie_level(trie: &CatalogTrie) -> Result<i32> { i32::try_from(trie.key.level()).map_err(|_| size_error()) }

fn trie_record(output: &mut CatalogPages<'_>, table: &TableRef, trie: &CatalogTrie) -> Result<Option<ColumnBatch>> {
    let metadata = trie.metadata.as_ref().filter(|metadata| metadata.temporal_metadata.is_some());
    let temporal = metadata.and_then(|metadata| metadata.temporal_metadata.as_ref());
    record!(output, [
        ("schema_name", Cell::Str(&table.schema)), ("table_name", Cell::Str(&table.table)),
        ("trie_key", Cell::TrieKey(&trie.key)), ("level", Cell::I32(trie_level(trie)?)),
        ("recency", trie.key.recency().map_or(Cell::Null, |date| Cell::Date(date.epoch_day() as i32))),
        ("trie_state", Cell::Str(match trie.state { TrieState::Nascent => "nascent", TrieState::Live => "live", TrieState::Garbage { .. } => "garbage" })),
        ("data_file_size", Cell::I64(trie.data_file_size)), ("row_count", metadata.map(|metadata| Cell::I64(metadata.row_count.unwrap_or_default())).unwrap_or(Cell::Null)),
        ("temporal_metadata", temporal.map_or(Cell::Null, Cell::Temporal)),
    ])
}

fn metric_record(output: &mut CatalogPages<'_>, table: &str, metric: &CatalogMetric) -> Result<Option<ColumnBatch>> {
    let name = metric.name();
    let tags = metric.tags();
    match (table, metric) {
        ("metrics_timers", CatalogMetric::Timer { count, mean_ns, max_ns, p75_ns, p95_ns, p99_ns, p999_ns, .. }) => {
            let duration = |value: &Option<i64>| value.map_or(Cell::Null, Cell::Duration);
            record!(output, [("name", Cell::Str(name)), ("tags", Cell::Tags(tags)),
                ("count", Cell::I64(*count)), ("mean_time", duration(mean_ns)), ("max_time", duration(max_ns)),
                ("p75_time", duration(p75_ns)), ("p95_time", duration(p95_ns)), ("p99_time", duration(p99_ns)), ("p999_time", duration(p999_ns))])
        }
        ("metrics_gauges", CatalogMetric::Gauge { value, .. }) => record!(output, [("name", Cell::Str(name)), ("tags", Cell::Tags(tags)), ("value", Cell::F64(*value))]),
        ("metrics_counters", CatalogMetric::Counter { count, .. }) => record!(output, [("name", Cell::Str(name)), ("tags", Cell::Tags(tags)), ("count", Cell::F64(*count))]),
        _ => Ok(None),
    }
}

fn checked(left: usize, right: usize) -> Result<usize> { left.checked_add(right).ok_or_else(size_error) }
fn scale(bytes: usize, factor: usize) -> Result<usize> { bytes.checked_mul(factor).ok_or_else(size_error) }
fn size_error() -> Anomaly { invalid("catalog-size", "Catalog allocation size exceeds usize") }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{CatalogSnapshot, DatabaseSnapshot, QueryOptions};
    use rtdb_storage::{MemoryStore, cache::BufferPool};
    use tokio_util::sync::CancellationToken;

    fn test_context(batch_rows: usize, memory: usize) -> Arc<QueryContext> {
        let database = DatabaseSnapshot {
            partitions: vec![],
            storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)),
        };
        let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
        let options = QueryOptions { batch_rows, ..Default::default() };
        QueryContext::new(catalog, MemoryBudget::new(memory), rtdb_types::Timestamp(0), "UTC", options, &CancellationToken::new()).unwrap()
    }

    fn system_scan(schema: &str, table: &str, projection: &[&str]) -> Scan {
        let binding = definition(&TableRef::new(schema, table)).unwrap().binding("xtdb");
        let mut binder = rtdb_plan::Binder::default();
        let columns = projection.iter().map(|&name| binder.field(name, None, binding.columns[name].clone()).unwrap()).collect();
        Scan {
            table: binding,
            columns,
            valid_time: rtdb_plan::TemporalFilter::Default,
            system_time: rtdb_plan::TemporalFilter::Default,
            predicates: vec![],
            iids: None,
            clamp_valid_time: false,
            top_k: None,
            ordered_top_k: None,
            matches: vec![],
        }
    }

    #[test]
    fn column_ordinals_merge_temporal_defaults_with_borrowed_fields() {
        let i32_type = LogicalType::Scalar(ScalarType::I32);
        let defaults = temporal_defaults();
        let mut binding = TableBinding {
            name: rtdb_plan::QualifiedTable { database: "xtdb".into(), table: TableRef::new("public", "events") },
            oid: None,
            columns: [
                ("z".into(), i32_type.clone()), ("A".into(), i32_type.clone()),
                ("_valid_to".into(), i32_type.clone()), ("_alpha".into(), i32_type.clone()),
            ].into(),
        };
        let memory = MemoryBudget::new(1 << 16);
        let expected = ["_alpha", "_system_from", "_system_to", "_valid_from", "_valid_to", "A", "z"];
        let order = column_order(&binding, &memory).unwrap();
        assert!(order.is_some());
        let fields = ordered_columns(&binding, &defaults, order.as_ref()).collect::<Vec<_>>();
        assert_eq!(fields.iter().map(|(name, _)| *name).collect::<Vec<_>>(), expected);
        assert!(column_nullable(fields[2].1));
        assert_eq!(fields[4].1, &i32_type);
        drop(fields);
        drop(order);

        binding.columns.sort_unstable_by(|left, _, right, _| left.cmp(right));
        let order = column_order(&binding, &memory).unwrap();
        assert!(order.is_none());
        let names = ordered_columns(&binding, &defaults, order.as_ref()).map(|(name, _)| name).collect::<Vec<_>>();
        assert_eq!(names, expected);
    }

    #[test]
    fn catalog_columns_append_only_projected_cells() {
        let scan = system_scan("pg_catalog", "pg_namespace", &["oid"]);
        let context = test_context(16, 1 << 16);
        let mut columns = CatalogColumns::new(&scan, &context.catalog, &context.memory).unwrap();
        columns.push(|name| match name {
            "oid" => Ok(Cell::I32(7)),
            _ => Err(catalog_layout()),
        }).unwrap();
        let page = columns.finish().unwrap();
        let Column::Arrow { array, field, .. } = &page.columns[0] else { panic!("expected Arrow column") };
        assert!(!field.is_nullable());
        assert_eq!(array.as_any().downcast_ref::<arrow_array::Int32Array>().unwrap().values().as_ref(), &[7]);
    }

    #[tokio::test]
    async fn inspection_output_metadata_keeps_catalog_text_buffers_charged() {
        let context = test_context(2, 1 << 16);
        let baseline = context.memory.used_bytes();
        let scan = system_scan("pg_catalog", "pg_namespace", &["nspname", "nspacl"]);
        let schema = rtdb_plan::Schema::new(scan.columns.clone()).unwrap();
        let mut pages = Box::pin(catalog_pages(scan, Arc::clone(&context)));
        let page = pages.next().await.unwrap().unwrap();
        let lease = Arc::downgrade(page.column_reservation(0));
        let bytes = |array: &ArrayRef| array.as_any().downcast_ref::<arrow_array::StringArray>()
            .unwrap().value_data().as_ptr();
        let Column::Arrow { array, .. } = &page.columns[0] else { panic!("expected Arrow column") };
        let address = bytes(array);
        let output = crate::cursor::column_batch(&schema, &context.memory, page).unwrap();
        assert_eq!(bytes(output.batch().column(0)), address);
        assert_eq!(output.batch().column(1).logical_null_count(), 2);
        let text = Arc::clone(output.batch().column(0));
        drop((pages, output));
        assert!(lease.upgrade().is_some());
        drop(text);
        assert!(lease.upgrade().is_none());
        assert_eq!(context.memory.used_bytes(), baseline);
    }

    #[tokio::test]
    async fn inspection_output_metadata_reuses_prepacked_literal_text() -> Result<()> {
        use arrow_array::{RecordBatch, StringArray};
        use rtdb_columnar::{OwnedBatch, to_field};
        use rtdb_plan::{Relation, RelationNode, Schema};

        let context = test_context(2, 1 << 20);
        let memory = Arc::clone(&context.memory);
        let text = LogicalType::Scalar(ScalarType::Utf8).maybe();
        let field = rtdb_plan::Binder::default().field("text", None, text.clone())?;
        let strings = StringArray::from(vec![Some("literal"), None]);
        let address = strings.value_data().as_ptr();
        let record = RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(vec![to_field("text", &text)?])), vec![Arc::new(strings)]).unwrap();
        let page = OwnedBatch::new(record, &memory)?;
        let lease = Arc::downgrade(page.reservation());
        let relation = Relation::new(Schema::new(vec![field])?, RelationNode::ArrowLiteralPages {
            pages: rtdb_plan::LiteralArrowPages::new(vec![page]),
        })?;
        let query = crate::PreparedQuery::new_in(Arc::new(relation), &memory)?;
        let mut cursor = query.execute_pages(context, vec![]).await?;
        let result = cursor.next_batch().await?.unwrap();
        let array = Arc::clone(result.batch().column(0));
        drop((result, cursor, query));
        let strings = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strings.value_data().as_ptr(), address);
        assert_eq!(strings.iter().collect::<Vec<_>>(), [Some("literal"), None]);
        assert!(lease.upgrade().is_some());
        drop(array);
        assert_eq!(memory.used_bytes(), 0);
        Ok(())
    }

    #[test]
    fn catalog_type_validation_keeps_pg_mapping_and_edn_errors_distinct() {
        let narrow = LogicalType::Scalar(ScalarType::I8).maybe();
        assert!(validate_pg_type(&narrow).is_err());
        assert!(narrow.validate_type_form().is_ok());

        let nested = LogicalType::Struct([("u".into(), LogicalType::Scalar(ScalarType::U8))].into());
        assert!(validate_pg_type(&nested).is_ok());
        assert_eq!(column_pg_type(&nested).unwrap(), PgType::Json);
        assert!(nested.validate_type_form().is_err());
    }

    #[tokio::test]
    async fn catalog_pages_are_built_on_demand_and_release_independently() {
        let context = test_context(2, 1 << 16);
        let baseline = context.memory.used_bytes();
        let scan = system_scan("pg_catalog", "pg_namespace", &["oid", "nspacl"]);
        let mut pages = Box::pin(catalog_pages(scan, Arc::clone(&context)));
        let first = pages.next().await.unwrap().unwrap();
        let first_bytes = context.memory.used_bytes();
        assert!(first_bytes > baseline);
        assert_eq!(first.len, 2);
        let Column::Arrow { array, field, .. } = &first.columns[0] else { panic!("expected Arrow column") };
        assert!(!field.is_nullable());
        assert_eq!(array.as_any().downcast_ref::<arrow_array::Int32Array>().unwrap().values().as_ref(),
            &[pg_name_oid("information_schema"), pg_name_oid("pg_catalog")]);
        let Column::Arrow { array, field, .. } = &first.columns[1] else { panic!("expected Arrow column") };
        assert!(field.is_nullable());
        assert_eq!(array.logical_null_count(), first.len);

        let second = pages.next().await.unwrap().unwrap();
        assert_eq!(second.len, 2);
        assert!(context.memory.used_bytes() > first_bytes);
        drop(second);
        assert_eq!(context.memory.used_bytes(), first_bytes);
        assert!(pages.next().await.is_none());
        drop(first);
        assert_eq!(context.memory.used_bytes(), baseline);
    }

    #[tokio::test]
    async fn table_names_fit_without_funding_logical_bindings() {
        let context = test_context(1, 1 << 16);
        let scan = system_scan("information_schema", "tables", &["table_name"]);
        let mut pages = Box::pin(catalog_pages(scan, context));
        let page = pages.next().await.unwrap().unwrap();
        let Column::Arrow { array, .. } = &page.columns[0] else { panic!("expected Arrow column") };
        assert_eq!(array.as_any().downcast_ref::<arrow_array::StringArray>().unwrap().value(0), "columns");
    }

    #[tokio::test]
    async fn catalog_pages_cover_zero_columns_and_empty_input() {
        let context = test_context(3, 1 << 16);
        let mut rows = Box::pin(catalog_pages(system_scan("pg_catalog", "pg_namespace", &[]), Arc::clone(&context)));
        for count in [3, 1] {
            let page = rows.next().await.unwrap().unwrap();
            assert!(page.columns.is_empty());
            assert_eq!(page.len, count);
        }
        assert!(rows.next().await.is_none());
        let mut empty = Box::pin(catalog_pages(system_scan("pg_catalog", "pg_description", &["description"]), context));
        assert!(empty.next().await.is_none());
    }
}
