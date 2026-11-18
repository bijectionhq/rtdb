use std::{ops::Deref, sync::Arc};

use rdkafka::{ClientConfig, error::KafkaError};
use rtdb_types::{AllocationScope, MemoryBudget, Result};

use crate::{KafkaClusterConfig, client, config::{ClientProperties, properties_bound, property_allocation, providers}};

pub(crate) enum LogConfiguration {
    Common(ClientConfig),
    Providers(ProviderConfiguration),
}

pub(crate) struct ProviderConfiguration {
    original: ClientProperties,
    memory: Arc<MemoryBudget>,
}

#[derive(Clone, Copy)]
pub(crate) enum Role {
    Admin,
    Consumer,
    Producer,
}

impl Role {
    fn overrides(self) -> &'static [&'static str] {
        match self {
            Self::Admin => &[],
            Self::Consumer => &["key.deserializer", "value.deserializer"],
            Self::Producer => &["key.serializer", "value.serializer", "acks"],
        }
    }
}

pub(crate) struct PreparedLogClient {
    pub(crate) configuration: ClientConfig,
    allocation: Option<AllocationScope>,
}

pub(crate) struct ConfiguredLogClient<T> {
    native: T,
    _allocation: Option<AllocationScope>,
}

impl<T> Deref for ConfiguredLogClient<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target { &self.native }
}

impl PreparedLogClient {
    pub(crate) fn open<T>(self, open: impl FnOnce(ClientConfig) -> std::result::Result<T, KafkaError>) -> std::result::Result<ConfiguredLogClient<T>, KafkaError> {
        let Self { configuration, allocation } = self;
        let native = open(configuration)?;
        Ok(ConfiguredLogClient { native, _allocation: allocation })
    }
}

impl LogConfiguration {
    pub(crate) fn from_cluster(cluster: &KafkaClusterConfig, memory: Arc<MemoryBudget>) -> Result<Self> {
        let mut original = cluster.capture_client_properties(&memory)?;
        if !providers::configured(&original.values) {
            original.values.retain(|name, _| !providers::control_property(name));
            return client::prepare(&original.values, &memory).map(Self::Common);
        }
        Ok(Self::Providers(ProviderConfiguration { original, memory }))
    }

    pub(crate) fn prepare(&self, role: Role) -> Result<PreparedLogClient> {
        let provider = match self {
            Self::Common(configuration) => return Ok(PreparedLogClient { configuration: configuration.clone(), allocation: None }),
            Self::Providers(provider) => provider,
        };
        let allocation = AllocationScope::new(&provider.memory)?;
        allocation.reserve(properties_bound(&provider.original.values)?)?;
        let mut properties = provider.original.values.clone();
        for name in role.overrides() {
            properties.remove(*name);
        }
        if let std::borrow::Cow::Owned(resolved) = providers::resolve(&properties, &provider.memory, &mut |bytes| allocation.reserve(bytes))? {
            properties = resolved;
        }
        properties.retain(|name, _| !providers::control_property(name));
        allocation.reserve(properties_bound(&properties)?.checked_mul(8).ok_or_else(property_allocation)?)?;
        let configuration = client::prepare(&properties, &provider.memory)?;
        Ok(PreparedLogClient { configuration, allocation: Some(allocation) })
    }

    pub(crate) fn overridden_acks(&self) -> Option<&str> {
        let Self::Providers(provider) = self else { return None; };
        provider.original.values.get("acks").map(String::as_str).filter(|value| *value != "all" && *value != "-1")
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn log_client_roles_remove_source_overrides_before_provider_demand() {
        let folder = tempfile::tempdir().unwrap();
        let missing = folder.path().join("missing.properties");
        let value = format!("${{file:{}:unavailable}}", missing.display());
        let original = BTreeMap::from([
            ("config.providers".into(), "file".into()),
            ("config.providers.file.class".into(), "org.apache.kafka.common.config.provider.FileConfigProvider".into()),
        ]);
        let memory = MemoryBudget::new(32 << 20);
        for (role, overridden) in [(Role::Consumer, &["key.deserializer"][..]), (Role::Producer, &["key.serializer", "acks"][..])] {
            let mut properties = original.clone();
            for &name in overridden { properties.insert(name.into(), value.clone()); }
            let cluster = KafkaClusterConfig { properties_map: properties, ..KafkaClusterConfig::new("mock:9092") };
            let log = LogConfiguration::from_cluster(&cluster, Arc::clone(&memory)).unwrap();
            let retained = memory.used_bytes();
            let config = log.prepare(role).unwrap();
            assert_eq!(config.configuration.config_map().len(), 1);
            assert_eq!(config.configuration.get("bootstrap.servers"), Some("mock:9092"));
            assert!(log.prepare(Role::Admin).is_err());
            drop(config);
            assert_eq!(memory.used_bytes(), retained);
        }
        assert_eq!(memory.used_bytes(), 0);
    }
}
