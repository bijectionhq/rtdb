use std::{collections::BTreeMap, ops::Deref, sync::Arc};

use rdkafka::{ClientConfig, error::KafkaError};
use rtdb_types::{AllocationScope, Anomaly, MemoryBudget, MemoryReservation, Result};

use crate::{client, config::providers};

pub(crate) enum LogConfiguration {
    Common(ClientConfig),
    Providers(ProviderConfiguration),
}

pub(crate) struct ProviderConfiguration {
    original: BTreeMap<String, String>,
    memory: Arc<MemoryBudget>,
    _charge: Arc<MemoryReservation>,
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
    pub(crate) fn from_cluster(original: BTreeMap<String, String>, memory: Arc<MemoryBudget>) -> Result<Self> {
        if !providers::configured(&original) { return client::prepare(&original, &memory).map(Self::Common); }
        let charge = memory.reserve(property_bound(&original)?)?;
        Ok(Self::Providers(ProviderConfiguration { original, memory, _charge: charge }))
    }

    pub(crate) fn prepare(&self, role: Role) -> Result<PreparedLogClient> {
        let provider = match self {
            Self::Common(configuration) => return Ok(PreparedLogClient { configuration: configuration.clone(), allocation: None }),
            Self::Providers(provider) => provider,
        };
        let allocation = AllocationScope::new(&provider.memory)?;
        allocation.reserve(property_bound(&provider.original)?)?;
        let mut properties = provider.original.clone();
        for name in role.overrides() {
            properties.remove(*name);
        }
        if let std::borrow::Cow::Owned(resolved) = providers::resolve(&properties, &provider.memory, &mut |bytes| allocation.reserve(bytes))? {
            properties = resolved;
        }
        properties.retain(|name, _| !providers::control_property(name));
        allocation.reserve(property_bound(&properties)?.checked_mul(8).ok_or_else(overflow)?)?;
        let configuration = client::prepare(&properties, &provider.memory)?;
        Ok(PreparedLogClient { configuration, allocation: Some(allocation) })
    }

    pub(crate) fn overridden_acks(&self) -> Option<&str> {
        let Self::Providers(provider) = self else { return None; };
        provider.original.get("acks").map(String::as_str).filter(|value| *value != "all" && *value != "-1")
    }
}

fn property_bound(properties: &BTreeMap<String, String>) -> Result<usize> {
    properties.iter().try_fold(4096usize, |sum, (name, value)| {
        sum.checked_add(512)?.checked_add(name.capacity())?.checked_add(value.capacity())
    }).ok_or_else(overflow)
}

fn overflow() -> Anomaly {
    Anomaly::incorrect("xtdb.kafka/config-allocation", "Kafka configuration exceeds addressable memory")
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

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
            let log = LogConfiguration::from_cluster(properties, Arc::clone(&memory)).unwrap();
            let retained = memory.used_bytes();
            let config = log.prepare(role).unwrap();
            assert!(config.configuration.config_map().is_empty());
            assert!(log.prepare(Role::Admin).is_err());
            drop(config);
            assert_eq!(memory.used_bytes(), retained);
        }
        assert_eq!(memory.used_bytes(), 0);
    }
}
