use std::collections::{btree_map::Entry, BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use async_trait::async_trait;
use arrow_array::{ArrayRef, builder::{ArrayBuilder, BooleanBuilder, Date32Builder, DurationNanosecondBuilder, Float64Builder, Int32Builder, Int64Builder, MapBuilder, StringBuilder, StructBuilder, TimestampMicrosecondBuilder}};
use arrow_schema::Field;
use rtdb_columnar::MemoryReservation;
use rtdb_format::log::proto::ArtefactRecord;
use rtdb_index::TrieState;
use rtdb_plan::{Catalog, CatalogMetric, CatalogRows, CatalogUser, Scan, TableBinding};
use rtdb_types::{Anomaly, LogicalType, PgType, Result, TableRef, pg_name_oid, pg_table_oid};

use crate::{columnar::{BatchCursor, Column, ColumnBatch}, context::Execution, invalid, system_schema::{definition, instant}};

type Tags = MapBuilder<StringBuilder, StringBuilder>;

enum Cell<'a> {
    Str(&'a str), Text(String), Char(char), I32(i32), I64(i64), F64(f64), Bool(bool), Date(i32), Duration(i64),
    Tags(&'a BTreeMap<String, String>), Temporal(&'a rtdb_format::log::proto::TemporalMetadata), Null,
}

struct CatalogColumns {
    fields: Vec<(Arc<Field>, Option<Box<dyn ArrayBuilder>>)>,
    rows: usize,
}

impl CatalogColumns {
    fn new(scan: &Scan) -> Result<Self> {
        let fields = scan.columns.iter().map(|field| {
            let field = rtdb_columnar::to_field(&field.name, &field.data_type)?;
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
            Ok((Arc::new(field), builder))
        }).collect::<Result<_>>()?;
        Ok(Self { fields, rows: 0 })
    }

    fn push<'a>(&mut self, mut cell: impl FnMut(&str) -> Result<Cell<'a>>) -> Result<()> {
        for (field, builder) in &mut self.fields {
            let Some(builder) = builder else { continue; };
            let builder = builder.as_any_mut();
            macro_rules! primitive {
                ($builder:ty, $value:expr) => {{
                    builder.downcast_mut::<$builder>().ok_or_else(catalog_layout)?.append_value($value);
                }};
            }
            match cell(field.name())? {
                Cell::Str(value) => primitive!(StringBuilder, value),
                Cell::Text(value) => primitive!(StringBuilder, value),
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
                    else if let Some(builder) = builder.downcast_mut::<StructBuilder>() {
                        for index in 0..builder.num_fields() {
                            builder.field_builder::<TimestampMicrosecondBuilder>(index).ok_or_else(catalog_layout)?.append_null();
                        }
                        builder.append(false);
                    }
                    else if let Some(builder) = builder.downcast_mut::<arrow_array::builder::ListBuilder<Box<dyn ArrayBuilder>>>() { builder.append(false); }
                    else { return Err(catalog_layout()); }
                }
            }
        }
        self.rows += 1;
        Ok(())
    }

    fn finish(self, limit: usize, mut reservation: Arc<MemoryReservation>) -> Result<VecDeque<ColumnBatch>> {
        if self.rows == 0 { return Ok(VecDeque::new()); }
        let columns = self.fields.into_iter().map(|(field, builder)| {
            let array: ArrayRef = builder.map_or_else(|| arrow_array::new_null_array(field.data_type(), self.rows), |mut builder| builder.finish());
            let actual = Arc::new(field.as_ref().clone().with_data_type(array.data_type().clone())
                .with_nullable(field.is_nullable() || array.null_count() != 0));
            (array, actual)
        }).collect::<Vec<_>>();
        let retained = columns.iter().try_fold(0usize, |bytes, (array, field)| checked(bytes, checked(array.get_array_memory_size(), field.size())?))?;
        let pages = self.rows.div_ceil(limit);
        let page_bytes = checked(256, columns.len().checked_mul(512).ok_or_else(size_error)?)?;
        let retained = checked(retained, pages.checked_mul(page_bytes).ok_or_else(size_error)?)?;
        if retained > reservation.bytes() { return Err(Anomaly::fault("rtdb.query/catalog-allocation-bound", "System catalog rows exceed their reserved allocation")); }
        Arc::get_mut(&mut reservation).expect("A catalog batch owns its allocation").shrink_to(retained)?;
        (0..self.rows).step_by(limit).map(|start| {
            let len = (self.rows - start).min(limit);
            let columns = columns.iter().map(|(array, field)| Column::Arrow { array: array.slice(start, len), field: Arc::clone(field), logical: None }).collect();
            Ok(ColumnBatch::sharing(columns, len, Arc::clone(&reservation)))
        }).collect()
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

pub(crate) struct SystemCursor { scan: Scan, execution: Execution, rows: Option<VecDeque<ColumnBatch>> }
impl SystemCursor {
    pub fn new(scan: Scan, execution: Execution) -> Self { Self { scan, execution, rows: None } }

    async fn initialize(&mut self) -> Result<()> {
        let context = &self.execution.context;
        let table = &self.scan.table.name.table;
        let runtime = || context.catalog.runtime.as_ref().ok_or_else(|| Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, "rtdb.query/catalog-runtime-unavailable", "Runtime catalog provider was not supplied"));
        let runtime_rows = match (table.schema.as_str(), table.table.as_str()) {
            ("pg_catalog", "pg_user") => RuntimeRows::Users(runtime()?.users()?),
            ("pg_catalog", "pg_roles") => RuntimeRows::Roles { users: runtime()?.users()?, memberships: runtime()?.memberships().await? },
            ("pg_catalog", "pg_auth_members") => RuntimeRows::Memberships(runtime()?.memberships().await?),
            ("xt", "metrics_timers" | "metrics_gauges" | "metrics_counters") => RuntimeRows::Metrics(runtime()?.sample().await?),
            ("xt", "live_ordered_index_storage") => {
                let inventory = context.catalog.ordered_indexes.as_ref().ok_or_else(|| {
                    Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, "rtdb.query/catalog-runtime-unavailable", "Ordered index inventory provider was not supplied")
                })?;
                RuntimeRows::OrderedIndexes(inventory.sample(&self.scan.table.name.database, context).await?)
            }
            _ => RuntimeRows::None,
        };
        context.check_cancelled()?;
        let needs_tables = matches!(table.table.as_str(), "tables" | "columns" | "pg_tables" | "pg_class" | "pg_attribute" | "pg_stat_user_tables" | "live_tables" | "live_columns" | "trie_stats" | "live_trie_artefacts");
        let tables = if needs_tables { context.catalog.tables(&self.scan.table.name.database)?.into_iter().filter(|binding| definition(&binding.name.table).is_none_or(|definition| definition.schema != "xt")).collect::<Vec<_>>() } else { vec![] };
        let reservation = context.memory.reserve(allocation_bound(&tables, &runtime_rows, &self.execution, &table.table)?)?;
        let mut columns = CatalogColumns::new(&self.scan)?;
        self.records(&tables, &runtime_rows, &mut columns)?;
        self.rows = Some(columns.finish(context.options.batch_rows, reservation)?);
        Ok(())
    }

    fn records(&self, tables: &[TableBinding], runtime: &RuntimeRows, output: &mut CatalogColumns) -> Result<()> {
        let database = &self.scan.table.name.database;
        let table = &self.scan.table.name.table;
        match (table.schema.as_str(), table.table.as_str()) {
            ("information_schema", "tables") => tables.iter().try_for_each(|binding| record!(output, [
                ("table_catalog", Cell::Str(database)), ("table_schema", Cell::Str(&binding.name.table.schema)), ("table_name", Cell::Str(&binding.name.table.table)),
                ("table_type", Cell::Str(if definition(&binding.name.table).is_some() { "VIEW" } else { "BASE TABLE" })),
            ])),
            ("information_schema", "schemata") => namespaces().try_for_each(|schema| record!(output, [("catalog_name", Cell::Str(database)), ("schema_name", Cell::Str(schema)), ("schema_owner", Cell::Str("xtdb"))])),
            ("information_schema", "columns") => column_rows(tables, |binding, name, ty, ordinal| {
                let pg = column_pg_type(ty)?;
                let precision = match pg { PgType::Int2 => Some(16), PgType::Int4 => Some(32), PgType::Int8 => Some(64), PgType::Float4 => Some(24), PgType::Float8 => Some(53), _ => None };
                record!(output, [
                    ("table_catalog", Cell::Str(database)), ("table_schema", Cell::Str(&binding.name.table.schema)), ("table_name", Cell::Str(&binding.name.table.table)),
                    ("column_name", Cell::Str(name)), ("data_type", Cell::Str(pg.data_type())), ("udt_schema", Cell::Str("pg_catalog")), ("udt_name", Cell::Str(pg.descriptor().name)),
                    ("ordinal_position", Cell::I32(ordinal)), ("column_default", Cell::Null), ("is_nullable", Cell::Str(if ty.nullable() { "YES" } else { "NO" })),
                    ("is_identity", Cell::Str("NO")), ("is_generated", Cell::Str("NEVER")), ("numeric_precision", precision.map(Cell::I32).unwrap_or(Cell::Null)),
                    ("numeric_precision_radix", precision.map(|_| Cell::I32(2)).unwrap_or(Cell::Null)),
                    ("numeric_scale", if matches!(pg, PgType::Int2 | PgType::Int4 | PgType::Int8) { Cell::I32(0) } else { Cell::Null }),
                    ("xt_type", Cell::Text(ty.to_edn()?)),
                ])
            }),
            ("pg_catalog", "pg_tables") => tables.iter().try_for_each(|binding| record!(output, [("schemaname", Cell::Str(&binding.name.table.schema)), ("tablename", Cell::Str(&binding.name.table.table)), ("tableowner", Cell::Str("xtdb")), ("tablespace", Cell::Null)])),
            ("pg_catalog", "pg_class") => tables.iter().try_for_each(|binding| record!(output, [
                ("oid", Cell::I32(table_oid(binding))), ("relname", Cell::Str(&binding.name.table.table)), ("relnamespace", Cell::I32(pg_name_oid(&binding.name.table.schema))),
                ("relowner", Cell::I32(pg_name_oid("xtdb"))), ("relkind", Cell::Str("r")), ("relam", Cell::I32(2)), ("relchecks", Cell::I32(0)),
                ("relhasindex", Cell::Bool(true)), ("relhasrules", Cell::Bool(false)), ("relhastriggers", Cell::Bool(false)), ("relrowsecurity", Cell::Bool(false)),
                ("relforcerowsecurity", Cell::Bool(false)), ("relispartition", Cell::Bool(false)), ("reltablespace", Cell::I32(0)), ("reloftype", Cell::I32(0)),
                ("relpersistence", Cell::Str("p")), ("relreplident", Cell::Str("d")), ("reltoastrelid", Cell::I32(0)),
            ])),
            ("pg_catalog", "pg_type") => PgType::ALL.iter().try_for_each(|ty| {
                let descriptor = ty.descriptor();
                record!(output, [
                    ("oid", Cell::I32(descriptor.oid)), ("typname", Cell::Str(descriptor.name)), ("typnamespace", Cell::I32(pg_name_oid("pg_catalog"))), ("typowner", Cell::I32(pg_name_oid("xtdb"))),
                    ("typtype", Cell::Str("b")), ("typcategory", Cell::Char(descriptor.category)), ("typbasetype", Cell::I32(0)), ("typnotnull", Cell::Bool(false)), ("typtypmod", Cell::I32(-1)),
                    ("typrelid", Cell::I32(0)), ("typelem", Cell::I32(descriptor.element_oid.unwrap_or(0))), ("typarray", Cell::I32(descriptor.array_oid.unwrap_or(0))),
                    ("typsend", Cell::Str(descriptor.send)), ("typreceive", Cell::Str(descriptor.receive)), ("typinput", Cell::Str(descriptor.input.unwrap_or(""))), ("typoutput", Cell::Str(descriptor.output.unwrap_or(""))),
                ])
            }),
            ("pg_catalog", "pg_attribute") => column_rows(tables, |binding, name, ty, ordinal| {
                let descriptor = column_pg_type(ty)?.descriptor();
                record!(output, [("attrelid", Cell::I32(table_oid(binding))), ("attname", Cell::Str(name)), ("atttypid", Cell::I32(descriptor.oid)), ("attlen", Cell::I32(i32::from(descriptor.length))),
                    ("attnum", Cell::I32(ordinal)), ("attisdropped", Cell::Bool(false)), ("attnotnull", Cell::Bool(false)), ("atttypmod", Cell::I32(-1)), ("attidentity", Cell::Str("")), ("attgenerated", Cell::Str("")), ("attcompression", Cell::Str(""))])
            }),
            ("pg_catalog", "pg_namespace") => namespaces().try_for_each(|namespace| record!(output, [("oid", Cell::I32(pg_name_oid(namespace))), ("nspname", Cell::Str(namespace)), ("nspowner", Cell::I32(pg_name_oid("xtdb"))), ("nspacl", Cell::Null)])),
            ("pg_catalog", "pg_proc") => record!(output, [("oid", Cell::I32(pg_name_oid("array_in"))), ("proname", Cell::Str("array_in")), ("pronamespace", Cell::I32(pg_name_oid("pg_catalog")))]),
            ("pg_catalog", "pg_database") => self.execution.context.catalog.databases().iter().try_for_each(|database| record!(output, [("oid", Cell::I32(pg_name_oid(database))), ("datname", Cell::Str(database)), ("datallowconn", Cell::Bool(true)), ("datistemplate", Cell::Bool(false)), ("datcollate", Cell::Str("C"))])),
            ("pg_catalog", "pg_stat_user_tables") => tables.iter().try_for_each(|binding| record!(output, [("relid", Cell::I32(table_oid(binding))), ("schemaname", Cell::Str(&binding.name.table.schema)), ("relname", Cell::Str(&binding.name.table.table)), ("n_live_tup", Cell::I64(0))])),
            ("pg_catalog", "pg_settings") => record!(output, [("name", Cell::Str("max_index_keys")), ("setting", Cell::Str("32"))]),
            ("pg_catalog", "pg_range") => record!(output, [("rngtypid", Cell::I32(3910)), ("rngsubtype", Cell::I32(1184)), ("rngmultitypid", Cell::I32(4534)), ("rngcollation", Cell::I32(0)), ("rngsubopc", Cell::I32(3127)), ("rngcanonical", Cell::Str("")), ("rngsubdiff", Cell::Str("tstzrange_subdiff"))]),
            ("pg_catalog", "pg_am") => [(2, "heap", "heap_tableam_handler", "t"), (403, "btree", "bthandler", "i"), (405, "hash", "hashhandler", "i")].into_iter().try_for_each(|(oid, name, handler, ty)| record!(output, [("oid", Cell::I32(oid)), ("amname", Cell::Str(name)), ("amhandler", Cell::Str(handler)), ("amtype", Cell::Str(ty))])),
            ("pg_catalog", "pg_user") => {
                let RuntimeRows::Users(users) = runtime else { return Err(invalid("catalog-runtime", "User snapshot is absent")); };
                users.rows().iter().try_for_each(|user| record!(output, [("usename", Cell::Str(&user.name)), ("usesuper", Cell::Bool(user.superuser)), ("passwd", Cell::Null)]))
            }
            ("pg_catalog", "pg_roles") => {
                let RuntimeRows::Roles { users, memberships } = runtime else { return Err(invalid("catalog-runtime", "Role snapshot is absent")); };
                let mut login: BTreeMap<&str, bool> = users.rows().iter().map(|user| (user.name.as_str(), user.superuser)).collect();
                for (user, _) in memberships.rows() { login.entry(user).or_insert_with(|| self.execution.context.catalog.runtime.as_ref().is_some_and(|runtime| runtime.is_superuser(user))); }
                let roles: BTreeSet<_> = memberships.rows().iter().map(|(_, role)| role.as_str()).filter(|role| !login.contains_key(role)).collect();
                login.into_iter().map(|(name, superuser)| (name, superuser, true))
                    .chain(roles.into_iter().map(|name| (name, false, false)))
                    .try_for_each(|(name, superuser, login)| role_row(output, name, superuser, login))
            }
            ("pg_catalog", "pg_auth_members") => {
                let RuntimeRows::Memberships(memberships) = runtime else { return Err(invalid("catalog-runtime", "Membership snapshot is absent")); };
                memberships.rows().iter().try_for_each(|(user, role)| record!(output, [("roleid", Cell::I32(pg_name_oid(role))), ("member", Cell::I32(pg_name_oid(user))), ("grantor", Cell::Null), ("admin_option", Cell::Bool(false))]))
            }
            ("xt", "metrics_timers" | "metrics_gauges" | "metrics_counters") => {
                let RuntimeRows::Metrics(metrics) = runtime else { return Err(invalid("catalog-runtime", "Telemetry snapshot is absent")); };
                metrics.rows().iter().try_for_each(|metric| metric_record(output, &table.table, metric))
            }
            ("xt", "live_trie_artefacts") => self.live_trie_artefacts(output, tables),
            ("xt", "live_ordered_index_storage") => {
                let RuntimeRows::OrderedIndexes(indexes) = runtime else { return Err(invalid("catalog-runtime", "Ordered index inventory is absent")); };
                indexes.rows().iter().try_for_each(|index| record!(output, [
                    ("schema_name", Cell::Str(&index.table.schema)), ("table_name", Cell::Str(&index.table.table)),
                    ("index_name", Cell::Str(&index.name)), ("definition_version", Cell::I64(index.version)),
                    ("through_block", Cell::I64(index.through_block)), ("file_bytes", Cell::I64(index.bytes)),
                ]))
            }
            ("xt", "live_tables" | "live_columns" | "trie_stats") => self.snapshot_records(output, &table.table, tables),
            ("information_schema", "table_constraints" | "key_column_usage" | "constraint_column_usage")
            | ("pg_catalog", "pg_description" | "pg_views" | "pg_matviews" | "pg_depend" | "pg_enum" | "pg_constraint" | "pg_extension" | "pg_attrdef" | "pg_sequence" | "pg_collation" | "pg_opclass" | "pg_index") => Ok(()),
            _ => return Err(Anomaly::fault("rtdb.query/catalog-table", "Declared system catalog has no row producer")),
        }
    }

    fn live_trie_artefacts(&self, output: &mut CatalogColumns, tables: &[TableBinding]) -> Result<()> {
        let database = self.execution.context.catalog.database(&self.scan.table.name.database)?;
        let mut files: BTreeMap<(&TableRef, String), (bool, &ArtefactRecord)> = BTreeMap::new();
        for snapshot in &database.partitions {
            for binding in tables {
                let table = &binding.name.table;
                let Some(entry) = snapshot.table_entry(table) else { continue; };
                for trie in snapshot.trie_catalog().iter_tries(table).filter(|trie| trie.state == TrieState::Live) {
                    let historical = trie.key.recency().is_some();
                    for artefact in &trie.artefacts {
                        self.execution.context.check_cancelled()?;
                        let object = entry.slug.recorded_artefact_key(&trie.key, artefact)?;
                        if artefact.file_size.is_some_and(|size| size < 0) {
                            return Err(invalid("artefact-size", "A published artefact has a negative file size"));
                        }
                        match files.entry((table, object)) {
                            Entry::Vacant(file) => { file.insert((historical, artefact)); }
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
        files.into_iter().try_for_each(|((table, object), (historical, artefact))| record!(output, [
            ("schema_name", Cell::Str(&table.schema)),
            ("table_name", Cell::Str(&table.table)),
            ("object_key", Cell::Str(&object)),
            ("kind", Cell::Str(artefact.kind())),
            ("definition_id", Cell::Str(artefact.definition_id())),
            ("definition_version", Cell::I64(artefact.definition_version())),
            ("historical", Cell::Bool(historical)),
            ("file_size", artefact.file_size.map(Cell::I64).unwrap_or(Cell::Null)),
        ]))
    }

    fn snapshot_records(&self, output: &mut CatalogColumns, name: &str, tables: &[TableBinding]) -> Result<()> {
        let database = self.execution.context.catalog.database(&self.scan.table.name.database)?;
        for snapshot in &database.partitions {
            for binding in tables.iter().filter(|binding| definition(&binding.name.table).is_none()) {
                let table = &binding.name.table;
                match name {
                    "live_tables" => {
                        let count: i64 = snapshot.iter_segments(table).map(|segment| i64::from(segment.row_count())).sum();
                        if count == 0 { continue; }
                        record!(output, [("schema_name", Cell::Str(&table.schema)), ("table_name", Cell::Str(&table.table)), ("row_count", Cell::I64(count))])?;
                    }
                    "live_columns" => for segment in snapshot.iter_segments(table).filter(|segment| segment.row_count() > 0) { for (column, ty) in segment.column_types() {
                        record!(output, [("schema_name", Cell::Str(&table.schema)), ("table_name", Cell::Str(&table.table)), ("col_name", Cell::Str(column)), ("col_type", Cell::Text(ty.to_edn()?))])?;
                    } },
                    "trie_stats" => for trie in snapshot.trie_catalog().all_tries(table) {
                        let metadata = trie.metadata.as_ref().filter(|metadata| metadata.temporal_metadata.is_some());
                        let temporal = metadata.and_then(|metadata| metadata.temporal_metadata.as_ref());
                        let recency = trie.key.recency();
                        record!(output, [("schema_name", Cell::Str(&table.schema)), ("table_name", Cell::Str(&table.table)), ("trie_key", Cell::Text(trie.key.to_string())), ("level", Cell::I32(i32::try_from(trie.key.level()).map_err(|_| size_error())?)),
                            ("recency", recency.map_or(Cell::Null, |date| Cell::Date(date.epoch_day() as i32))), ("trie_state", Cell::Str(match trie.state { rtdb_index::TrieState::Nascent => "nascent", rtdb_index::TrieState::Live => "live", rtdb_index::TrieState::Garbage { .. } => "garbage" })),
                            ("data_file_size", Cell::I64(trie.data_file_size)), ("row_count", metadata.map(|metadata| Cell::I64(metadata.row_count.unwrap_or_default())).unwrap_or(Cell::Null)), ("temporal_metadata", temporal.map_or(Cell::Null, Cell::Temporal))])?;
                    },
                    _ => unreachable!("snapshot catalog"),
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl BatchCursor for SystemCursor {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        self.execution.context.check_cancelled()?;
        if self.rows.is_none() { self.initialize().await?; }
        Ok(self.rows.as_mut().and_then(VecDeque::pop_front))
    }
}


fn namespaces() -> impl Iterator<Item = &'static str> { ["information_schema", "pg_catalog", "public", "xt"].into_iter() }
fn table_oid(table: &TableBinding) -> i32 { pg_table_oid(table.oid, &table.name.table) }
fn column_pg_type(ty: &LogicalType) -> Result<PgType> { Ok(match PgType::from_logical(ty)? { PgType::Default => PgType::Json, PgType::Null => PgType::Text, ty => ty }) }
fn role_row(output: &mut CatalogColumns, name: &str, superuser: bool, login: bool) -> Result<()> { record!(output, [("oid", Cell::I32(pg_name_oid(name))), ("rolname", Cell::Str(name)), ("rolsuper", Cell::Bool(superuser)), ("rolcanlogin", Cell::Bool(login))]) }

fn column_rows(tables: &[TableBinding], mut row: impl FnMut(&TableBinding, &str, &LogicalType, i32) -> Result<()>) -> Result<()> {
    for table in tables {
        let mut columns = table.columns.clone();
        if !matches!(table.name.table.schema.as_str(), "xt" | "pg_catalog" | "information_schema") || table.name.table == TableRef::new("xt", "txs") {
            for name in ["_system_from", "_valid_from"] { columns.entry(name.into()).or_insert_with(instant); }
            for name in ["_system_to", "_valid_to"] { columns.entry(name.into()).or_insert_with(|| instant().maybe()); }
        }
        let mut sorted: Vec<_> = columns.iter().collect();
        sorted.sort_by(|left, right| left.0.cmp(right.0));
        let ordered = sorted.iter().copied().filter(|(name, _)| name.starts_with('_')).chain(sorted.iter().copied().filter(|(name, _)| !name.starts_with('_')));
        for (index, (name, ty)) in ordered.enumerate() { row(table, name, ty, i32::try_from(index + 1).map_err(|_| size_error())?)?; }
    }
    Ok(())
}

fn metric_record(output: &mut CatalogColumns, table: &str, metric: &CatalogMetric) -> Result<()> {
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
        _ => Ok(()),
    }
}

fn allocation_bound(tables: &[TableBinding], runtime: &RuntimeRows, execution: &Execution, table: &str) -> Result<usize> {
    let mut bytes = if table == "pg_type" { checked(16384, PgType::ALL.len() * 16384)? } else { 16384 };
    for name in execution.context.catalog.databases() { bytes = checked(bytes, checked(name.len().checked_mul(32).ok_or_else(size_error)?, 4096)?)?; }
    let needs_artefacts = table == "live_trie_artefacts";
    for table in tables {
        let names = checked(table.name.database.len(), checked(table.name.table.schema.len(), table.name.table.table.len())?)?;
        bytes = checked(bytes, checked(names.checked_mul(64).ok_or_else(size_error)?, 32768)?)?;
        for (name, ty) in &table.columns {
            bytes = checked(bytes, checked(checked(ty.retained_bytes()?, checked(name.len(), names)?)?.checked_mul(32).ok_or_else(size_error)?, 16384)?)?;
        }
        for snapshot in &execution.context.catalog.database(&table.name.database)?.partitions {
            for segment in snapshot.iter_segments(&table.name.table) {
                for (name, ty) in segment.column_types() { bytes = checked(bytes, checked(checked(name.len(), ty.retained_bytes()?)?.checked_mul(32).ok_or_else(size_error)?, 16384)?)?; }
            }
            bytes = checked(bytes, snapshot.trie_catalog().iter_tries(&table.name.table).count().checked_mul(32768).ok_or_else(size_error)?)?;
            if needs_artefacts {
                for trie in snapshot.trie_catalog().iter_tries(&table.name.table).filter(|trie| trie.state == TrieState::Live) {
                    for artefact in &trie.artefacts {
                        let names = checked(names, checked(artefact.kind().len(), artefact.definition_id().len())?)?;
                        bytes = checked(bytes, checked(names.checked_mul(64).ok_or_else(size_error)?, 16384)?)?;
                    }
                }
            }
        }
    }
    let mut add_name = |name: &str| -> Result<()> { bytes = checked(bytes, checked(name.len().checked_mul(32).ok_or_else(size_error)?, 16384)?)?; Ok(()) };
    match runtime {
        RuntimeRows::Users(users) => for user in users.rows() { add_name(&user.name)?; },
        RuntimeRows::Roles { users, memberships } => { for user in users.rows() { add_name(&user.name)?; } for (user, role) in memberships.rows() { add_name(user)?; add_name(role)?; } }
        RuntimeRows::Memberships(memberships) => for (user, role) in memberships.rows() { add_name(user)?; add_name(role)?; },
        RuntimeRows::Metrics(metrics) => for metric in metrics.rows() { add_name(metric.name())?; for (key, value) in metric.tags() { add_name(key)?; add_name(value)?; } },
        RuntimeRows::OrderedIndexes(indexes) => for index in indexes.rows() {
            add_name(&index.name)?;
            add_name(&index.table.schema)?;
            add_name(&index.table.table)?;
        },
        RuntimeRows::None => {}
    }
    Ok(bytes)
}
fn checked(left: usize, right: usize) -> Result<usize> { left.checked_add(right).ok_or_else(size_error) }
fn size_error() -> Anomaly { invalid("catalog-size", "Catalog allocation size exceeds usize") }

#[test]
fn catalog_columns_append_only_projected_cells_and_keep_typed_pages() {
    let table = TableRef::new("pg_catalog", "pg_namespace");
    let binding = definition(&table).unwrap().binding("xtdb");
    let field = rtdb_plan::Binder::default().field("oid", None, binding.columns["oid"].clone()).unwrap();
    let mut columns = CatalogColumns::new(&Scan {
        table: binding,
        columns: vec![field],
        valid_time: rtdb_plan::TemporalFilter::Default,
        system_time: rtdb_plan::TemporalFilter::Default,
        predicates: vec![],
        iids: None,
        clamp_valid_time: false,
        top_k: None,
        ordered_top_k: None,
        matches: vec![],
    }).unwrap();
    for oid in [2, 5, 11] {
        record!(columns, [("oid", Cell::I32(oid)), ("nspname", Cell::Text(Err::<String, _>(catalog_layout())?))]).unwrap();
    }
    let memory = rtdb_columnar::MemoryBudget::new(1 << 20);
    let pages = columns.finish(2, memory.reserve(16384).unwrap()).unwrap();
    assert_eq!(pages.iter().map(|page| page.len).collect::<Vec<_>>(), [2, 1]);
    let actual = pages.iter().flat_map(|page| {
        let Column::Arrow { array, .. } = &page.columns[0] else { panic!("expected Arrow column") };
        array.as_any().downcast_ref::<arrow_array::Int32Array>().unwrap().values().iter().copied()
    }).collect::<Vec<_>>();
    assert_eq!(actual, [2, 5, 11]);
}
