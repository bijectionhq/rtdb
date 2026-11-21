use std::collections::{btree_map::Entry, BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use async_trait::async_trait;
use rtdb_format::log::proto::ArtefactRecord;
use rtdb_index::TrieState;
use rtdb_plan::{Catalog, CatalogMetric, CatalogRows, CatalogUser, Scan, TableBinding};
use rtdb_types::{Anomaly, LogicalType, PgType, Result, TableRef, TimeUnit, Value, pg_name_oid, pg_table_oid};

use crate::{context::Execution, cursor::{OwnedRow, RowCursor}, invalid, system_schema::{definition, instant}};

type Record = BTreeMap<&'static str, Value>;
enum RuntimeRows {
    None,
    Users(CatalogRows<CatalogUser>),
    Roles { users: CatalogRows<CatalogUser>, memberships: CatalogRows<(String, String)> },
    Memberships(CatalogRows<(String, String)>),
    Metrics(CatalogRows<CatalogMetric>),
    OrderedIndexes(CatalogRows<crate::OrderedIndexStorage>),
}

pub(crate) struct SystemCursor { scan: Scan, execution: Execution, rows: Option<VecDeque<OwnedRow>> }
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
        let records = self.records(&tables, &runtime_rows)?;
        let retained = records.iter().try_fold(0usize, |bytes, row| row.values().try_fold(bytes, |bytes, value| checked(bytes, checked(value.retained_bytes()?, 512)?)))?;
        if retained > reservation.bytes() { return Err(Anomaly::fault("rtdb.query/catalog-allocation-bound", "System catalog rows exceed their reserved allocation")); }
        self.rows = Some(records.into_iter().map(|mut record| {
            let values = self.scan.columns.iter().map(|field| record.remove(field.name.as_str()).unwrap_or(Value::Null)).collect();
            OwnedRow::with_reservation(values, Arc::clone(&reservation))
        }).collect::<Result<_>>()?);
        Ok(())
    }

    fn records(&self, tables: &[TableBinding], runtime: &RuntimeRows) -> Result<Vec<Record>> {
        let database = &self.scan.table.name.database;
        let table = &self.scan.table.name.table;
        let rows = match (table.schema.as_str(), table.table.as_str()) {
            ("information_schema", "tables") => tables.iter().map(|binding| record([
                ("table_catalog", text(database)), ("table_schema", text(&binding.name.table.schema)), ("table_name", text(&binding.name.table.table)),
                ("table_type", text(if definition(&binding.name.table).is_some() { "VIEW" } else { "BASE TABLE" })),
            ])).collect(),
            ("information_schema", "schemata") => namespaces().map(|schema| record([("catalog_name", text(database)), ("schema_name", text(schema)), ("schema_owner", text("xtdb"))])).collect(),
            ("information_schema", "columns") => column_rows(tables, |binding, name, ty, ordinal| {
                let pg = column_pg_type(ty)?;
                let precision = match pg { PgType::Int2 => Some(16), PgType::Int4 => Some(32), PgType::Int8 => Some(64), PgType::Float4 => Some(24), PgType::Float8 => Some(53), _ => None };
                Ok(record([
                    ("table_catalog", text(database)), ("table_schema", text(&binding.name.table.schema)), ("table_name", text(&binding.name.table.table)),
                    ("column_name", text(name)), ("data_type", text(pg.data_type())), ("udt_schema", text("pg_catalog")), ("udt_name", text(pg.descriptor().name)),
                    ("ordinal_position", Value::I32(ordinal)), ("column_default", Value::Null), ("is_nullable", text(if ty.nullable() { "YES" } else { "NO" })),
                    ("is_identity", text("NO")), ("is_generated", text("NEVER")), ("numeric_precision", precision.map(Value::I32).unwrap_or(Value::Null)),
                    ("numeric_precision_radix", precision.map(|_| Value::I32(2)).unwrap_or(Value::Null)),
                    ("numeric_scale", if matches!(pg, PgType::Int2 | PgType::Int4 | PgType::Int8) { Value::I32(0) } else { Value::Null }),
                    ("xt_type", Value::Utf8(ty.to_edn()?)),
                ]))
            })?,
            ("pg_catalog", "pg_tables") => tables.iter().map(|binding| record([("schemaname", text(&binding.name.table.schema)), ("tablename", text(&binding.name.table.table)), ("tableowner", text("xtdb")), ("tablespace", Value::Null)])).collect(),
            ("pg_catalog", "pg_class") => tables.iter().map(|binding| record([
                ("oid", Value::I32(table_oid(binding))), ("relname", text(&binding.name.table.table)), ("relnamespace", Value::I32(pg_name_oid(&binding.name.table.schema))),
                ("relowner", Value::I32(pg_name_oid("xtdb"))), ("relkind", text("r")), ("relam", Value::I32(2)), ("relchecks", Value::I32(0)),
                ("relhasindex", Value::Bool(true)), ("relhasrules", Value::Bool(false)), ("relhastriggers", Value::Bool(false)), ("relrowsecurity", Value::Bool(false)),
                ("relforcerowsecurity", Value::Bool(false)), ("relispartition", Value::Bool(false)), ("reltablespace", Value::I32(0)), ("reloftype", Value::I32(0)),
                ("relpersistence", text("p")), ("relreplident", text("d")), ("reltoastrelid", Value::I32(0)),
            ])).collect(),
            ("pg_catalog", "pg_type") => PgType::ALL.iter().map(|ty| {
                let descriptor = ty.descriptor();
                record([
                    ("oid", Value::I32(descriptor.oid)), ("typname", text(descriptor.name)), ("typnamespace", Value::I32(pg_name_oid("pg_catalog"))), ("typowner", Value::I32(pg_name_oid("xtdb"))),
                    ("typtype", text("b")), ("typcategory", text(&descriptor.category.to_string())), ("typbasetype", Value::I32(0)), ("typnotnull", Value::Bool(false)), ("typtypmod", Value::I32(-1)),
                    ("typrelid", Value::I32(0)), ("typelem", Value::I32(descriptor.element_oid.unwrap_or(0))), ("typarray", Value::I32(descriptor.array_oid.unwrap_or(0))),
                    ("typsend", text(descriptor.send)), ("typreceive", text(descriptor.receive)), ("typinput", text(descriptor.input.unwrap_or(""))), ("typoutput", text(descriptor.output.unwrap_or(""))),
                ])
            }).collect(),
            ("pg_catalog", "pg_attribute") => column_rows(tables, |binding, name, ty, ordinal| {
                let descriptor = column_pg_type(ty)?.descriptor();
                Ok(record([("attrelid", Value::I32(table_oid(binding))), ("attname", text(name)), ("atttypid", Value::I32(descriptor.oid)), ("attlen", Value::I32(i32::from(descriptor.length))),
                    ("attnum", Value::I32(ordinal)), ("attisdropped", Value::Bool(false)), ("attnotnull", Value::Bool(false)), ("atttypmod", Value::I32(-1)), ("attidentity", text("")), ("attgenerated", text("")), ("attcompression", text(""))]))
            })?,
            ("pg_catalog", "pg_namespace") => namespaces().map(|namespace| record([("oid", Value::I32(pg_name_oid(namespace))), ("nspname", text(namespace)), ("nspowner", Value::I32(pg_name_oid("xtdb"))), ("nspacl", Value::Null)])).collect(),
            ("pg_catalog", "pg_proc") => vec![record([("oid", Value::I32(pg_name_oid("array_in"))), ("proname", text("array_in")), ("pronamespace", Value::I32(pg_name_oid("pg_catalog")))])],
            ("pg_catalog", "pg_database") => self.execution.context.catalog.databases().iter().map(|database| record([("oid", Value::I32(pg_name_oid(database))), ("datname", text(database)), ("datallowconn", Value::Bool(true)), ("datistemplate", Value::Bool(false)), ("datcollate", text("C"))])).collect(),
            ("pg_catalog", "pg_stat_user_tables") => tables.iter().map(|binding| record([("relid", Value::I32(table_oid(binding))), ("schemaname", text(&binding.name.table.schema)), ("relname", text(&binding.name.table.table)), ("n_live_tup", Value::I64(0))])).collect(),
            ("pg_catalog", "pg_settings") => vec![record([("name", text("max_index_keys")), ("setting", text("32"))])],
            ("pg_catalog", "pg_range") => vec![record([("rngtypid", Value::I32(3910)), ("rngsubtype", Value::I32(1184)), ("rngmultitypid", Value::I32(4534)), ("rngcollation", Value::I32(0)), ("rngsubopc", Value::I32(3127)), ("rngcanonical", text("")), ("rngsubdiff", text("tstzrange_subdiff"))])],
            ("pg_catalog", "pg_am") => [(2, "heap", "heap_tableam_handler", "t"), (403, "btree", "bthandler", "i"), (405, "hash", "hashhandler", "i")].into_iter().map(|(oid, name, handler, ty)| record([("oid", Value::I32(oid)), ("amname", text(name)), ("amhandler", text(handler)), ("amtype", text(ty))])).collect(),
            ("pg_catalog", "pg_user") => {
                let RuntimeRows::Users(users) = runtime else { return Err(invalid("catalog-runtime", "User snapshot is absent")); };
                users.rows().iter().map(|user| record([("usename", text(&user.name)), ("usesuper", Value::Bool(user.superuser)), ("passwd", Value::Null)])).collect()
            }
            ("pg_catalog", "pg_roles") => {
                let RuntimeRows::Roles { users, memberships } = runtime else { return Err(invalid("catalog-runtime", "Role snapshot is absent")); };
                let mut login: BTreeMap<&str, bool> = users.rows().iter().map(|user| (user.name.as_str(), user.superuser)).collect();
                for (user, _) in memberships.rows() { login.entry(user).or_insert_with(|| self.execution.context.catalog.runtime.as_ref().is_some_and(|runtime| runtime.is_superuser(user))); }
                let roles: BTreeSet<_> = memberships.rows().iter().map(|(_, role)| role.as_str()).filter(|role| !login.contains_key(role)).collect();
                login.into_iter().map(|(name, superuser)| role_row(name, superuser, true)).chain(roles.into_iter().map(|name| role_row(name, false, false))).collect()
            }
            ("pg_catalog", "pg_auth_members") => {
                let RuntimeRows::Memberships(memberships) = runtime else { return Err(invalid("catalog-runtime", "Membership snapshot is absent")); };
                memberships.rows().iter().map(|(user, role)| record([("roleid", Value::I32(pg_name_oid(role))), ("member", Value::I32(pg_name_oid(user))), ("grantor", Value::Null), ("admin_option", Value::Bool(false))])).collect()
            }
            ("xt", "metrics_timers" | "metrics_gauges" | "metrics_counters") => {
                let RuntimeRows::Metrics(metrics) = runtime else { return Err(invalid("catalog-runtime", "Telemetry snapshot is absent")); };
                metrics.rows().iter().filter_map(|metric| metric_record(&table.table, metric)).collect()
            }
            ("xt", "live_trie_artefacts") => self.live_trie_artefacts(tables)?,
            ("xt", "live_ordered_index_storage") => {
                let RuntimeRows::OrderedIndexes(indexes) = runtime else { return Err(invalid("catalog-runtime", "Ordered index inventory is absent")); };
                indexes.rows().iter().map(|index| record([
                    ("schema_name", text(&index.table.schema)), ("table_name", text(&index.table.table)),
                    ("index_name", text(&index.name)), ("definition_version", Value::I64(index.version)),
                    ("through_block", Value::I64(index.through_block)), ("file_bytes", Value::I64(index.bytes)),
                ])).collect()
            }
            ("xt", "live_tables" | "live_columns" | "trie_stats") => self.snapshot_records(&table.table, tables)?,
            ("information_schema", "table_constraints" | "key_column_usage" | "constraint_column_usage")
            | ("pg_catalog", "pg_description" | "pg_views" | "pg_matviews" | "pg_depend" | "pg_enum" | "pg_constraint" | "pg_extension" | "pg_attrdef" | "pg_sequence" | "pg_collation" | "pg_opclass" | "pg_index") => vec![],
            _ => return Err(Anomaly::fault("rtdb.query/catalog-table", "Declared system catalog has no row producer")),
        };
        Ok(rows)
    }

    fn live_trie_artefacts(&self, tables: &[TableBinding]) -> Result<Vec<Record>> {
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
        Ok(files.into_iter().map(|((table, object), (historical, artefact))| record([
            ("schema_name", text(&table.schema)),
            ("table_name", text(&table.table)),
            ("object_key", Value::Utf8(object)),
            ("kind", text(artefact.kind())),
            ("definition_id", text(artefact.definition_id())),
            ("definition_version", Value::I64(artefact.definition_version())),
            ("historical", Value::Bool(historical)),
            ("file_size", artefact.file_size.map(Value::I64).unwrap_or(Value::Null)),
        ])).collect())
    }

    fn snapshot_records(&self, name: &str, tables: &[TableBinding]) -> Result<Vec<Record>> {
        let database = self.execution.context.catalog.database(&self.scan.table.name.database)?;
        let mut rows = Vec::new();
        for snapshot in &database.partitions {
            for binding in tables.iter().filter(|binding| definition(&binding.name.table).is_none()) {
                let table = &binding.name.table;
                match name {
                    "live_tables" => {
                        let count: i64 = snapshot.iter_segments(table).map(|segment| i64::from(segment.row_count())).sum();
                        if count == 0 { continue; }
                        rows.push(record([("schema_name", text(&table.schema)), ("table_name", text(&table.table)), ("row_count", Value::I64(count))]));
                    }
                    "live_columns" => for segment in snapshot.iter_segments(table).filter(|segment| segment.row_count() > 0) { for (column, ty) in segment.column_types() {
                        rows.push(record([("schema_name", text(&table.schema)), ("table_name", text(&table.table)), ("col_name", text(column)), ("col_type", text(&ty.to_edn()?))]));
                    } },
                    "trie_stats" => for trie in snapshot.trie_catalog().all_tries(table) {
                        let metadata = trie.metadata.as_ref().filter(|metadata| metadata.temporal_metadata.is_some());
                        let temporal = metadata.and_then(|metadata| metadata.temporal_metadata.as_ref()).map(|metadata| Value::Struct([
                            ("min_valid_from", metadata.min_valid_from), ("max_valid_from", metadata.max_valid_from), ("min_valid_to", metadata.min_valid_to),
                            ("max_valid_to", metadata.max_valid_to), ("min_system_from", metadata.min_system_from), ("max_system_from", metadata.max_system_from),
                        ].into_iter().map(|(name, value)| (name.into(), timestamp(value.unwrap_or(if name.starts_with("min_") { i64::MAX } else { i64::MIN })))).collect())).unwrap_or(Value::Null);
                        let recency = trie.key.recency().map(|date| Value::DateDay(date.epoch_day() as i32)).unwrap_or(Value::Null);
                        rows.push(record([("schema_name", text(&table.schema)), ("table_name", text(&table.table)), ("trie_key", text(&trie.key.to_string())), ("level", Value::I32(i32::try_from(trie.key.level()).map_err(|_| size_error())?)),
                            ("recency", recency), ("trie_state", text(match trie.state { rtdb_index::TrieState::Nascent => "nascent", rtdb_index::TrieState::Live => "live", rtdb_index::TrieState::Garbage { .. } => "garbage" })),
                            ("data_file_size", Value::I64(trie.data_file_size)), ("row_count", metadata.map(|metadata| Value::I64(metadata.row_count.unwrap_or_default())).unwrap_or(Value::Null)), ("temporal_metadata", temporal)]));
                    },
                    _ => unreachable!("snapshot catalog"),
                }
            }
        }
        Ok(rows)
    }
}

#[async_trait]
impl RowCursor for SystemCursor {
    async fn next(&mut self) -> Result<Option<OwnedRow>> {
        self.execution.context.check_cancelled()?;
        if self.rows.is_none() { self.initialize().await?; }
        Ok(self.rows.as_mut().and_then(VecDeque::pop_front))
    }
}

fn record<const N: usize>(entries: [(&'static str, Value); N]) -> Record { entries.into_iter().collect() }
fn text(value: &str) -> Value { Value::Utf8(value.to_owned()) }
fn timestamp(value: i64) -> Value { Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) } }
fn namespaces() -> impl Iterator<Item = &'static str> { ["information_schema", "pg_catalog", "public", "xt"].into_iter() }
fn table_oid(table: &TableBinding) -> i32 { pg_table_oid(table.oid, &table.name.table) }
fn column_pg_type(ty: &LogicalType) -> Result<PgType> { Ok(match PgType::from_logical(ty)? { PgType::Default => PgType::Json, PgType::Null => PgType::Text, ty => ty }) }
fn role_row(name: &str, superuser: bool, login: bool) -> Record { record([("oid", Value::I32(pg_name_oid(name))), ("rolname", text(name)), ("rolsuper", Value::Bool(superuser)), ("rolcanlogin", Value::Bool(login))]) }

fn column_rows(tables: &[TableBinding], mut row: impl FnMut(&TableBinding, &str, &LogicalType, i32) -> Result<Record>) -> Result<Vec<Record>> {
    let mut rows = Vec::new();
    for table in tables {
        let mut columns = table.columns.clone();
        if !matches!(table.name.table.schema.as_str(), "xt" | "pg_catalog" | "information_schema") || table.name.table == TableRef::new("xt", "txs") {
            for name in ["_system_from", "_valid_from"] { columns.entry(name.into()).or_insert_with(instant); }
            for name in ["_system_to", "_valid_to"] { columns.entry(name.into()).or_insert_with(|| instant().maybe()); }
        }
        let mut sorted: Vec<_> = columns.iter().collect();
        sorted.sort_by(|left, right| left.0.cmp(right.0));
        let ordered = sorted.iter().copied().filter(|(name, _)| name.starts_with('_')).chain(sorted.iter().copied().filter(|(name, _)| !name.starts_with('_')));
        for (index, (name, ty)) in ordered.enumerate() { rows.push(row(table, name, ty, i32::try_from(index + 1).map_err(|_| size_error())?)?); }
    }
    Ok(rows)
}

fn metric_record(table: &str, metric: &CatalogMetric) -> Option<Record> {
    let mut row = match (table, metric) {
        ("metrics_timers", CatalogMetric::Timer { count, mean_ns, max_ns, p75_ns, p95_ns, p99_ns, p999_ns, .. }) => {
            let duration = |value: &Option<i64>| value.map(|value| Value::Duration { value, unit: TimeUnit::Nanosecond }).unwrap_or(Value::Null);
            record([("count", Value::I64(*count)), ("mean_time", duration(mean_ns)), ("max_time", duration(max_ns)), ("p75_time", duration(p75_ns)), ("p95_time", duration(p95_ns)), ("p99_time", duration(p99_ns)), ("p999_time", duration(p999_ns))])
        }
        ("metrics_gauges", CatalogMetric::Gauge { value, .. }) => record([("value", Value::F64(*value))]),
        ("metrics_counters", CatalogMetric::Counter { count, .. }) => record([("count", Value::F64(*count))]),
        _ => return None,
    };
    row.insert("name", text(metric.name()));
    row.insert("tags", Value::Map { entries: metric.tags().iter().map(|(key, value)| (text(key), text(value))).collect(), sorted: false });
    Some(row)
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
