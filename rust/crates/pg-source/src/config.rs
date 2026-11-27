use std::{collections::BTreeSet, sync::Arc, time::Duration};
use prost::Message;
use prost_types::Any;
use rtdb_format::postgres::proto::{DirectMirrorConfig, PostgresSourceConfig};
use rtdb_types::{Anomaly, Result, TableRef};

pub const CONFIG_TYPE: &str = "proto.xtdb.com/xtdb.postgres.proto.PostgresSourceConfig";
pub const DIRECT_MIRROR_TYPE: &str = "proto.xtdb.com/xtdb.postgres.proto.DirectMirrorConfig";

#[derive(Clone, Debug)]
pub struct SourceConfig {
    pub remote: String,
    pub slot_name: String,
    pub publication_name: String,
    pub indexer: Option<Any>,
    pub ignore_tables: BTreeSet<TableRef>,
}
impl SourceConfig {
    pub fn decode(any: &Any) -> Result<Self> {
        if any.type_url != CONFIG_TYPE { return Err(invalid("Configuration is not a PostgreSQL source")); }
        let wire = PostgresSourceConfig::decode(any.value.as_slice()).map_err(invalid)?;
        let mut ignore_tables = BTreeSet::new();
        for table in wire.ignore_tables {
            let table = if let Some((schema, name)) = table.split_once('.') {
                if schema.is_empty() || name.is_empty() { return Err(invalid("Ignored table requires nonempty schema and table names")); }
                TableRef::new(schema, name)
            } else {
                TableRef::parse(&table)?
            };
            ignore_tables.insert(table);
        }
        let config = Self { remote: wire.remote.unwrap_or_default(), slot_name: wire.slot_name.unwrap_or_default(), publication_name: wire.publication_name.unwrap_or_default(), indexer: wire.indexer, ignore_tables };
        config.validate()?;
        Ok(config)
    }
    pub fn validate(&self) -> Result<()> {
        if self.remote.is_empty() || self.slot_name.is_empty() || self.publication_name.is_empty() { return Err(invalid("PostgreSQL remote, slot name, and publication name are required")); }
        if self.slot_name.as_bytes().iter().any(|byte| !byte.is_ascii_lowercase() && !byte.is_ascii_digit() && *byte != b'_') { return Err(invalid("PostgreSQL slot names require lowercase letters, digits, or underscores")); }
        if self.publication_name.contains('\0') { return Err(invalid("Publication name contains a zero byte")); }
        if let Some(indexer) = &self.indexer && indexer.type_url == DIRECT_MIRROR_TYPE {
            DirectMirrorConfig::decode(indexer.value.as_slice()).map_err(invalid)?;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub enum SslMode { Disable, Prefer(Arc<tokio_rustls::rustls::ClientConfig>), Require(Arc<tokio_rustls::rustls::ClientConfig>) }
impl Default for SslMode {
    fn default() -> Self {
        let roots = tokio_rustls::rustls::RootCertStore { roots: webpki_roots::TLS_SERVER_ROOTS.to_vec() };
        Self::Prefer(Arc::new(tokio_rustls::rustls::ClientConfig::builder_with_provider(Arc::new(tokio_rustls::rustls::crypto::aws_lc_rs::default_provider()))
            .with_safe_default_protocol_versions().expect("AWS-LC supports the default TLS versions").with_root_certificates(roots).with_no_client_auth()))
    }
}
#[derive(Clone)]
pub struct Remote {
    pub hostname: String, pub port: u16, pub database: String, pub username: String, pub password: String,
    pub ssl: SslMode, pub connect_timeout: Duration, pub status_interval: Duration, pub maximum_message_bytes: usize,
}
impl Remote {
    pub fn new(hostname: String, database: String, username: String, password: String) -> Self {
        Self { hostname, port: 5432, database, username, password, ssl: SslMode::default(), connect_timeout: Duration::from_secs(10), status_interval: Duration::from_secs(10), maximum_message_bytes: 64 * 1024 * 1024 }
    }
    pub fn validate(&self) -> Result<()> {
        if self.hostname.is_empty() || self.database.is_empty() || self.username.is_empty() || self.connect_timeout.is_zero() || self.status_interval.is_zero() || self.maximum_message_bytes < 1024 { return Err(invalid("PostgreSQL remote fields, intervals, and message limit must be valid")); }
        if [&self.database, &self.username, &self.password].into_iter().any(|value| value.contains('\0')) { return Err(invalid("PostgreSQL credentials contain a zero byte")); }
        Ok(())
    }
}

pub(crate) fn identifier(value: &str) -> Result<String> {
    if value.contains('\0') { return Err(invalid("SQL identifier contains a zero byte")); }
    Ok(format!("\"{}\"", value.replace('"', "\"\"")))
}
pub(crate) fn literal(value: &str) -> Result<String> {
    if value.contains('\0') { return Err(invalid("SQL literal contains a zero byte")); }
    Ok(format!("E'{}'", value.replace('\\', "\\\\").replace('\'', "''")))
}
fn invalid(error: impl std::fmt::Display) -> Anomaly { Anomaly::incorrect("xtdb.postgres/invalid-config", error.to_string()) }

pub(crate) fn replication_literal(value: &str) -> Result<String> {
    if value.contains('\0') { return Err(invalid("Replication literal contains a zero byte")); }
    Ok(format!("'{}'", value.replace('\'', "''")))
}
