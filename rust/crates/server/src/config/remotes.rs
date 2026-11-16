use std::{collections::BTreeMap, path::PathBuf};
use rtdb_auth::Secret;
use rtdb_format::config::{Yaml, YamlFields};
use rtdb_types::Result;
use super::{invalid, decode_port};

#[derive(Clone, Debug)]
pub enum RemoteConfig {
    Kafka { bootstrap_servers: String, properties: BTreeMap<String, Secret>, properties_file: Option<PathBuf>, schema_registry_url: Option<String> },
    S3 { access_key: Secret, secret_key: Secret },
    Azure { connection_string: Option<Secret>, storage_account_key: Option<Secret> },
    Postgres { hostname: String, port: u16, database: String, username: String, password: Secret },
}

pub(super) fn decode_maps(clusters: Option<Yaml>, remotes: Option<Yaml>) -> Result<BTreeMap<String, RemoteConfig>> {
    let mut output = BTreeMap::new();
    for (field, value) in [("logClusters", clusters), ("remotes", remotes)] {
        if let Some(value) = value {
            for (name, config) in YamlFields::new(value, field)?.into_entries() {
                if output.contains_key(&name) { return Err(rtdb_types::Anomaly::incorrect("xtdb/duplicate-remote-alias", format!("alias appears under both 'logClusters:' and 'remotes:': {name}"))); }
                output.insert(name, decode(config)?);
            }
        }
    }
    Ok(output)
}
fn decode(value: Yaml) -> Result<RemoteConfig> {
    let (tag, mut fields) = YamlFields::tagged(value, "remote")?;
    let remote = match tag.as_str() {
        "!Kafka" => RemoteConfig::Kafka {
            bootstrap_servers: fields.required_string("bootstrapServers")?,
            properties: fields.string_map("propertiesMap")?.unwrap_or_default().into_iter().map(|(key, value)| (key, Secret::new(value))).collect(),
            properties_file: fields.nullable_string("propertiesFile")?.map(Into::into), schema_registry_url: fields.nullable_string("schemaRegistryUrl")?,
        },
        "!S3" => RemoteConfig::S3 { access_key: Secret::new(fields.required_string("accessKey")?), secret_key: Secret::new(fields.required_string("secretKey")?) },
        "!AzureBlob" => RemoteConfig::Azure { connection_string: fields.nullable_string("connectionString")?.map(Secret::new), storage_account_key: fields.nullable_string("storageAccountKey")?.map(Secret::new) },
        "!Postgres" => RemoteConfig::Postgres {
            hostname: fields.required_string("hostname")?, port: u16::try_from(decode_port(&mut fields, "port", 5432)?).map_err(|_| invalid("Postgres remote port must be nonnegative"))?,
            database: fields.required_string("database")?, username: fields.required_string("username")?, password: Secret::new(fields.required_string("password")?),
        },
        _ => return Err(invalid(format!("Unknown remote configuration tag '{tag}'"))),
    };
    fields.finish()?;
    Ok(remote)
}
