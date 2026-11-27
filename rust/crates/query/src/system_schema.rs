use rtdb_types::StructFields;
use rtdb_plan::{QualifiedTable, TableBinding};
use rtdb_types::{ListKind, LogicalType as L, ScalarType as S, TableRef, TimeUnit, pg_table_oid};

pub(crate) struct Definition { pub schema: &'static str, pub name: &'static str, fields: &'static str }
pub(crate) fn allocation_bound(database: &str) -> rtdb_types::Result<usize> {
    DEFINITIONS.iter().try_fold(4096usize, |bytes, definition| {
        bytes.checked_add(definition.allocation_bound(database)?).ok_or_else(system_schema_size)
    })
}
fn system_schema_size() -> rtdb_types::Anomaly {
    crate::invalid("catalog-size", "System schema allocation exceeds addressable memory")
}
macro_rules! definition { ($schema:literal, $name:literal, $fields:literal) => { Definition { schema: $schema, name: $name, fields: $fields } }; }
pub(crate) const DEFINITIONS: &[Definition] = &[
    definition!("information_schema", "tables", "table_catalog:s table_schema:s table_name:s table_type:s"),
    definition!("information_schema", "columns", "table_catalog:s table_schema:s table_name:s column_name:s data_type:s udt_schema:s udt_name:s ordinal_position:i column_default:s? is_nullable:s is_identity:s is_generated:s generation_expression:s? character_maximum_length:i? character_octet_length:i? numeric_precision:i? numeric_scale:i? datetime_precision:i? numeric_precision_radix:i? character_set_catalog:s? character_set_schema:s? character_set_name:s? collation_catalog:s? collation_name:s? xt_type:s"),
    definition!("information_schema", "schemata", "catalog_name:s schema_name:s schema_owner:s"),
    definition!("information_schema", "table_constraints", "constraint_catalog:s? constraint_schema:s? constraint_name:s? table_catalog:s? table_schema:s? table_name:s? constraint_type:s? is_deferrable:s? initially_deferred:s?"),
    definition!("information_schema", "key_column_usage", "constraint_catalog:s? constraint_schema:s? constraint_name:s? table_catalog:s? table_schema:s? table_name:s? column_name:s? ordinal_position:i? position_in_unique_constraint:i?"),
    definition!("information_schema", "constraint_column_usage", "table_catalog:s? table_schema:s? table_name:s? column_name:s? constraint_catalog:s? constraint_schema:s? constraint_name:s?"),
    definition!("pg_catalog", "pg_tables", "schemaname:s tablename:s tableowner:s tablespace:null"),
    definition!("pg_catalog", "pg_type", "oid:i typname:s typnamespace:i typowner:i typcategory:s typtype:s typbasetype:i typnotnull:b typtypmod:i typsend:s typreceive:s typinput:s typoutput:s typrelid:i typelem:i typarray:i typlen:i? typbyval:b? typalign:s? typstorage:s? typcollation:i? typdefault:s? typndims:i? typdelim:s? typispreferred:b? typisdefined:b?"),
    definition!("pg_catalog", "pg_class", "oid:i relname:s relnamespace:i relowner:i relkind:s relam:i relchecks:i relhasindex:b relhasrules:b relhastriggers:b relrowsecurity:b relforcerowsecurity:b relispartition:b reltablespace:i reloftype:i relpersistence:s relreplident:s reltoastrelid:i reltype:i? reltuples:f? relpages:i? relallvisible:i? relnatts:i? relhassubclass:b? relpartbound:s? reloptions:ls?"),
    definition!("pg_catalog", "pg_description", "objoid:i classoid:i objsubid:i16 description:s"),
    definition!("pg_catalog", "pg_views", "schemaname:s viewname:s viewowner:s"),
    definition!("pg_catalog", "pg_matviews", "schemaname:s matviewname:s matviewowner:s"),
    definition!("pg_catalog", "pg_attribute", "attrelid:i attname:s atttypid:i attlen:i attnum:i attisdropped:b attnotnull:b atttypmod:i attidentity:s attgenerated:s attcompression:s attstorage:s? attalign:s? atthasdef:b? attcollation:i? attislocal:b? attinhcount:i? atthasmissing:b? attndims:i?"),
    definition!("pg_catalog", "pg_namespace", "oid:i nspname:s nspowner:i nspacl:null"),
    definition!("pg_catalog", "pg_proc", "oid:i proname:s pronamespace:i"),
    definition!("pg_catalog", "pg_database", "oid:i datname:s datallowconn:b datistemplate:b datcollate:s"),
    definition!("pg_catalog", "pg_stat_user_tables", "relid:i schemaname:s relname:s n_live_tup:l"),
    definition!("pg_catalog", "pg_settings", "name:s setting:s"),
    definition!("pg_catalog", "pg_range", "rngtypid:i rngsubtype:i rngmultitypid:i rngcollation:i rngsubopc:i rngcanonical:s rngsubdiff:s"),
    definition!("pg_catalog", "pg_am", "oid:i amname:s amhandler:s amtype:s"),
    definition!("pg_catalog", "pg_user", "usename:s usesuper:b passwd:s?"),
    definition!("pg_catalog", "pg_roles", "oid:i rolname:s rolsuper:b rolcanlogin:b"),
    definition!("pg_catalog", "pg_auth_members", "roleid:i member:i grantor:i? admin_option:b"),
    definition!("pg_catalog", "pg_depend", "classid:i objid:i objsubid:i refclassid:i refobjid:i refobjsubid:i deptype:s"),
    definition!("pg_catalog", "pg_enum", "oid:i enumtypid:i enumsortorder:f enumlabel:s"),
    definition!("pg_catalog", "pg_constraint", "oid:i conname:s connamespace:i contype:s condeferrable:b condeferred:b convalidated:b conrelid:i contypid:i conindid:i conparentid:i confrelid:i confupdtype:s confdeltype:s confmatchtype:s conislocal:b coninhcount:i connoinherit:b conkey:li? confkey:li?"),
    definition!("pg_catalog", "pg_extension", "oid:i extname:s extowner:i extnamespace:i extrelocatable:b extversion:s extconfig:li? extcondition:ls?"),
    definition!("pg_catalog", "pg_attrdef", "oid:i adrelid:i adnum:i adbin:s?"),
    definition!("pg_catalog", "pg_sequence", "seqrelid:i seqtypid:i seqstart:l seqincrement:l seqmax:l seqmin:l seqcache:l seqcycle:b"),
    definition!("pg_catalog", "pg_collation", "oid:i collname:s collnamespace:i collowner:i collprovider:s collisdeterministic:b collencoding:i collcollate:s? collctype:s? colliculocale:s? collversion:s?"),
    definition!("pg_catalog", "pg_opclass", "oid:i opcmethod:i opcname:s opcnamespace:i opcowner:i opcfamily:i opcintype:i opcdefault:b opckeytype:i"),
    definition!("pg_catalog", "pg_index", "indexrelid:i indrelid:i indnatts:i indnkeyatts:i indisunique:b indnullsnotdistinct:b indisprimary:b indisexclusion:b indimmediate:b indisclustered:b indisvalid:b indcheckxmin:b indisready:b indislive:b indisreplident:b indkey:li? indcollation:li? indclass:li? indoption:li? indexprs:s? indpred:s?"),
    definition!("xt", "trie_stats", "schema_name:s table_name:s trie_key:s level:i recency:date? trie_state:s data_file_size:l row_count:l? temporal_metadata:temporal?"),
    definition!("xt", "live_trie_artefacts", "schema_name:s table_name:s object_key:s kind:s definition_id:s definition_version:l historical:b file_size:l?"),
    definition!("xt", "live_ordered_index_storage", "schema_name:s table_name:s index_name:s definition_version:l through_block:l file_bytes:l"),
    definition!("xt", "live_tables", "schema_name:s table_name:s row_count:l"),
    definition!("xt", "live_columns", "schema_name:s table_name:s col_name:s col_type:s"),
    definition!("xt", "metrics_timers", "name:s tags:tags count:l mean_time:duration? p75_time:duration? p95_time:duration? p99_time:duration? p999_time:duration? max_time:duration?"),
    definition!("xt", "metrics_gauges", "name:s tags:tags value:f"),
    definition!("xt", "metrics_counters", "name:s tags:tags count:f"),
    definition!("xt", "source_log_msgs", "msg_id:l log_offset:l log_timestamp:instant msg_type:s msg:s tx_ops:s?"),
    definition!("xt", "replica_log_msgs", "msg_id:l log_offset:l log_timestamp:instant msg_type:s msg:s"),
    definition!("xt", "block_files", "block_idx:s tx_id:l? system_time:instant? latest_processed_msg_id:l table_names:s boundary_replica_msg_id:l? file_size:l"),
    definition!("xt", "table_block_files", "table_name:s block_idx:s row_count:l fields:s hlls:s partition_count:i"),
    definition!("xt", "table_block_file_tries", "table_name:s block_idx:s level:i recency:s? part:s trie_key:s data_file_size:l trie_state:s? row_count:l? temporal_metadata:s?"),
];

pub(crate) fn definition(table: &TableRef) -> Option<&'static Definition> { DEFINITIONS.iter().find(|entry| entry.schema == table.schema && entry.name == table.table) }
impl Definition {
    pub(crate) fn allocation_bound(&self, database: &str) -> rtdb_types::Result<usize> {
        database.len().checked_add(self.schema.len()).and_then(|n| n.checked_add(self.name.len()))
            .and_then(|n| n.checked_add(self.fields.len())).and_then(|n| n.checked_mul(256))
            .and_then(|n| n.checked_add(4096)).ok_or_else(system_schema_size)
    }
    pub fn columns(&self) -> StructFields<L> {
        // Upstream `xtdb.sql/xform-table-info` orders every catalog table's columns alphabetically for `SELECT *`.
        let mut fields: Vec<(String, L)> = self.fields.split_ascii_whitespace().map(|field| {
            let (name, ty) = field.split_once(':').expect("catalog field delimiter");
            let base = ty.strip_suffix('?').unwrap_or(ty);
            let value = match base {
                "s" => L::Scalar(S::Utf8), "i" => L::Scalar(S::I32), "l" => L::Scalar(S::I64), "i16" => L::Scalar(S::I16),
                "b" => L::Scalar(S::Bool), "f" => L::Scalar(S::F64), "null" => L::Null,
                "li" => L::list(L::Scalar(S::I32)), "ls" => L::list(L::Scalar(S::Utf8)),
                "date" => L::Scalar(S::DateDay), "duration" => L::Scalar(S::Duration(TimeUnit::Nanosecond)),
                "instant" => instant(),
                "tags" => L::List { kind: ListKind::Map { sorted: false }, element: Box::new(L::Struct(StructFields::from([("key".into(), L::Scalar(S::Utf8)), ("value".into(), L::Scalar(S::Utf8))]))) },
                "temporal" => L::Struct(["min_valid_from", "max_valid_from", "min_valid_to", "max_valid_to", "min_system_from", "max_system_from"].into_iter().map(|name| (name.into(), instant())).collect()),
                _ => unreachable!("catalog field type"),
            };
            (name.to_owned(), if ty.ends_with('?') { value.maybe() } else { value })
        }).collect();
        fields.sort_by(|(left, _), (right, _)| left.cmp(right));
        fields.into_iter().collect()
    }
    pub fn binding(&self, database: &str) -> TableBinding {
        let table = TableRef::new(self.schema, self.name);
        TableBinding { oid: Some(pg_table_oid(None, &table)), name: QualifiedTable { database: database.into(), table }, columns: self.columns() }
    }
}
pub(crate) fn instant() -> L { L::Scalar(S::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) }) }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_table_columns_are_ordered_alphabetically_like_upstream() {
        for entry in DEFINITIONS {
            let names: Vec<_> = entry.columns().keys().cloned().collect();
            let mut sorted = names.clone();
            sorted.sort();
            assert_eq!(names, sorted, "{}.{} columns must be sorted alphabetically", entry.schema, entry.name);
        }
        assert_eq!(
            definition(&TableRef::new("information_schema", "key_column_usage")).unwrap().columns().keys().map(String::as_str).collect::<Vec<_>>(),
            ["column_name", "constraint_catalog", "constraint_name", "constraint_schema", "ordinal_position", "position_in_unique_constraint", "table_catalog", "table_name", "table_schema"],
        );
        assert_eq!(
            definition(&TableRef::new("information_schema", "table_constraints")).unwrap().columns().keys().map(String::as_str).collect::<Vec<_>>(),
            ["constraint_catalog", "constraint_name", "constraint_schema", "constraint_type", "initially_deferred", "is_deferrable", "table_catalog", "table_name", "table_schema"],
        );
    }
}
