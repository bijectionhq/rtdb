use std::{cell::RefCell, fmt, sync::Arc};

use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::config::{Yaml, resolve_env_vars};
use rtdb_types::{Anomaly, Result};
use serde::de::{self, DeserializeSeed, EnumAccess, MapAccess, SeqAccess, VariantAccess, Visitor};
use serde_yaml::{Mapping, value::{Tag, TaggedValue}};
use zeroize::Zeroizing;

use super::{NodeConfiguration, invalid};

pub struct EnvironmentValue { value: Zeroizing<String>, reservation: Arc<MemoryReservation> }
impl EnvironmentValue {
    /// Copies a borrowed environment value after reserving its parser and configuration expansion bound.
    pub fn copy(value: &str, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let reservation = memory.reserve(add(mul(value.len(), 64)?, 4096)?)?;
        Ok(Self { value: Zeroizing::new(value.to_owned()), reservation })
    }
}

/// Lookups reserve before copying their result; missing or non-Unicode variables return `None`.
pub trait ConfigurationEnvironment {
    fn lookup(&self, name: &str, memory: &Arc<MemoryBudget>) -> Result<Option<EnvironmentValue>>;
}

/// Process environment reads require the host to exclude concurrent native environment mutation.
pub struct ProcessEnvironment;
impl ConfigurationEnvironment for ProcessEnvironment {
    fn lookup(&self, name: &str, memory: &Arc<MemoryBudget>) -> Result<Option<EnvironmentValue>> { process_value(name, memory) }
}

pub struct ParsedConfiguration {
    configuration: NodeConfiguration,
    warnings: Vec<String>,
    charges: Charges,
}
pub struct ParsedYamlDocument {
    value: Yaml,
    charges: Charges,
}
impl ParsedYamlDocument {
    /// Parses and expands YAML without constructing services; the document retains all parser charges.
    pub fn from_yaml(text: &str, environment: &impl ConfigurationEnvironment, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let charges = Charges::new(text, memory)?;
        let parsed = if text.trim().is_empty() { Ok(Yaml::Mapping(Mapping::new())) } else {
            Seed(&charges).deserialize(serde_yaml::Deserializer::from_str(text)).map_err(|error| invalid(format!("Invalid YAML: {error}")))
        };
        charges.check()?;
        let mut value = parsed?;
        let lookup = |name: &str| charges.environment(name, environment);
        let resolved = resolve_env_vars(&mut value, &lookup);
        charges.check()?;
        resolved?;
        Ok(Self { value, charges })
    }

    pub fn value(&self) -> &Yaml { &self.value }
    /// Returns the retained parser/configuration allocation bound in bytes, including environment expansions.
    pub fn allocation_bound(&self) -> usize { *self.charges.bytes.borrow() }

    /// The same environment must be supplied for implicit settings as for explicit `!Env` expansion.
    pub fn into_node_configuration(self, environment: &impl ConfigurationEnvironment, ingest: bool) -> Result<ParsedConfiguration> {
        let configuration = NodeConfiguration::from_value(self.value, &|name| self.charges.environment(name, environment), ingest);
        self.charges.check()?;
        Ok(ParsedConfiguration { configuration: configuration?, warnings: Vec::new(), charges: self.charges })
    }
}
impl ParsedConfiguration {
    pub fn from_yaml_environment(
        name: &str,
        environment: &impl ConfigurationEnvironment,
        memory: &Arc<MemoryBudget>,
    ) -> Result<Self> {
        let input = environment.lookup(name, memory)?
            .filter(|input| !input.value.trim().is_empty())
            .ok_or_else(|| invalid("The Multi configuration environment variable is missing or empty"))?;
        if input.value.len() > 4 * 1024 * 1024 {
            return Err(invalid("Configuration exceeds 4 MiB"));
        }
        Self::from_yaml(&input.value, environment, memory, false).map_err(|error| {
            Anomaly::new(error.category, error.code, "Invalid Multi configuration from the environment")
        })
    }

    pub fn from_yaml(text: &str, environment: &impl ConfigurationEnvironment, memory: &Arc<MemoryBudget>, ingest: bool) -> Result<Self> {
        ParsedYamlDocument::from_yaml(text, environment, memory)?.into_node_configuration(environment, ingest)
    }

    pub fn from_edn(text: &str, environment: &impl ConfigurationEnvironment, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let charges = Charges::new(text, memory)?;
        let parsed = NodeConfiguration::from_edn_with_environment(text, &|name| charges.environment(name, environment));
        charges.check()?;
        let (configuration, warnings) = parsed?;
        Ok(Self { configuration, warnings, charges })
    }

    pub fn configuration(&self) -> &NodeConfiguration { &self.configuration }
    /// Applies a caller-captured object-sort selection to this unopened runtime configuration.
    pub fn with_object_sort(mut self,selection: rtdb_types::ObjectSort) -> Self { self.configuration.object_sort = selection;self }
    /// Returns the retained parser/configuration allocation bound in bytes, including environment expansions.
    pub fn allocation_bound(&self) -> usize { *self.charges.bytes.borrow() }
    pub fn warnings(&self) -> &[String] { &self.warnings }
    pub(crate) fn configuration_mut(&mut self) -> &mut NodeConfiguration { &mut self.configuration }

    /// Transfers startup charges before the node allocates from its execution/cache split.
    /// Retain the returned lease until the configuration and all allocations derived from it have been dropped.
    pub fn into_execution(self, allocation_limit_bytes: usize) -> Result<(NodeConfiguration, Arc<MemoryBudget>, Arc<MemoryReservation>)> {
        let limit = self.configuration.node_options(allocation_limit_bytes)?.execution_memory_bytes;
        let memory = MemoryBudget::new(limit);
        let retained = memory.reserve(*self.charges.bytes.borrow())?;
        Ok((self.configuration, memory, retained))
    }
}

struct Charges {
    memory: Arc<MemoryBudget>,
    reservations: RefCell<Vec<Arc<MemoryReservation>>>,
    bytes: RefCell<usize>,
    failure: RefCell<Option<Anomaly>>,
}
impl Charges {
    fn new(text: &str, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let tag_prefix = text.lines().filter(|line| line.trim_start_matches('\u{feff}').starts_with("%TAG")).map(str::len).max().unwrap_or(0);
        let expanded_tags = mul(mul(text.bytes().filter(|byte| *byte == b'!').count(), tag_prefix)?, 32)?;
        let bound = add(add(mul(text.len(), 512)?, expanded_tags)?, 65_536)?;
        let reservation = memory.reserve(bound)?;
        Ok(Self { memory: Arc::clone(memory), reservations: RefCell::new(vec![reservation]), bytes: RefCell::new(bound), failure: RefCell::new(None) })
    }
    fn retain(&self, reservation: Arc<MemoryReservation>) -> Result<()> {
        let bytes = add(*self.bytes.borrow(), reservation.bytes())?;
        self.reservations.borrow_mut().push(reservation);
        *self.bytes.borrow_mut() = bytes;
        Ok(())
    }
    fn reserve(&self, bytes: usize) -> Result<()> { self.retain(self.memory.reserve(bytes)?) }
    fn charge<E: de::Error>(&self, bytes: Result<usize>) -> std::result::Result<(), E> {
        bytes.and_then(|bytes| self.reserve(bytes)).map_err(|error| {
            *self.failure.borrow_mut() = Some(error);
            E::custom("Configuration allocation budget exhausted")
        })
    }
    fn environment(&self, name: &str, environment: &impl ConfigurationEnvironment) -> Option<String> {
        if self.failure.borrow().is_some() { return None; }
        let result = environment.lookup(name, &self.memory).and_then(|value| value.map(|value| {
            self.retain(value.reservation)?;
            Ok(value.value.to_string())
        }).transpose());
        match result { Ok(value) => value, Err(error) => { *self.failure.borrow_mut() = Some(error); None } }
    }
    fn check(&self) -> Result<()> { match self.failure.borrow_mut().take() { Some(error) => Err(error), None => Ok(()) } }
}

#[derive(Clone, Copy)]
struct Seed<'a>(&'a Charges);
impl<'de> DeserializeSeed<'de> for Seed<'_> {
    type Value = Yaml;
    fn deserialize<D: de::Deserializer<'de>>(self, deserializer: D) -> std::result::Result<Yaml, D::Error> {
        self.0.charge(Ok(4096))?;
        deserializer.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for Seed<'_> {
    type Value = Yaml;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result { formatter.write_str("a YAML configuration value") }
    fn visit_bool<E: de::Error>(self, value: bool) -> std::result::Result<Yaml, E> { Ok(Yaml::Bool(value)) }
    fn visit_i64<E: de::Error>(self, value: i64) -> std::result::Result<Yaml, E> { Ok(Yaml::Number(value.into())) }
    fn visit_u64<E: de::Error>(self, value: u64) -> std::result::Result<Yaml, E> { Ok(Yaml::Number(value.into())) }
    fn visit_f64<E: de::Error>(self, value: f64) -> std::result::Result<Yaml, E> { Ok(Yaml::Number(value.into())) }
    fn visit_str<E: de::Error>(self, value: &str) -> std::result::Result<Yaml, E> { self.0.charge(mul(value.len(), 32))?; Ok(Yaml::String(value.into())) }
    fn visit_string<E: de::Error>(self, value: String) -> std::result::Result<Yaml, E> { self.0.charge(mul(value.capacity(), 32))?; Ok(Yaml::String(value)) }
    fn visit_unit<E: de::Error>(self) -> std::result::Result<Yaml, E> { Ok(Yaml::Null) }
    fn visit_none<E: de::Error>(self) -> std::result::Result<Yaml, E> { Ok(Yaml::Null) }
    fn visit_some<D: de::Deserializer<'de>>(self, deserializer: D) -> std::result::Result<Yaml, D::Error> { self.deserialize(deserializer) }
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> std::result::Result<Yaml, A::Error> {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element_seed(self)? { values.push(value); }
        Ok(Yaml::Sequence(values))
    }
    fn visit_map<A: MapAccess<'de>>(self, mut mapping: A) -> std::result::Result<Yaml, A::Error> {
        let mut values = Mapping::new();
        while let Some(key) = mapping.next_key_seed(self)? {
            if values.contains_key(&key) { return Err(de::Error::custom("Duplicate YAML mapping key")); }
            values.insert(key, mapping.next_value_seed(self)?);
        }
        Ok(Yaml::Mapping(values))
    }
    fn visit_enum<A: EnumAccess<'de>>(self, tagged: A) -> std::result::Result<Yaml, A::Error> {
        let (tag, value) = tagged.variant_seed(TextSeed(self.0))?;
        Ok(Yaml::Tagged(Box::new(TaggedValue { tag: Tag::new(tag), value: value.newtype_variant_seed(self)? })))
    }
}
struct TextSeed<'a>(&'a Charges);
impl<'de> DeserializeSeed<'de> for TextSeed<'_> {
    type Value = String;
    fn deserialize<D: de::Deserializer<'de>>(self, deserializer: D) -> std::result::Result<String, D::Error> { deserializer.deserialize_string(self) }
}
impl<'de> Visitor<'de> for TextSeed<'_> {
    type Value = String;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result { formatter.write_str("a YAML tag") }
    fn visit_str<E: de::Error>(self, value: &str) -> std::result::Result<String, E> { self.0.charge(mul(value.len(), 32).and_then(|bytes| add(bytes, 4096)))?; Ok(value.into()) }
    fn visit_string<E: de::Error>(self, value: String) -> std::result::Result<String, E> { self.0.charge(mul(value.capacity(), 32).and_then(|bytes| add(bytes, 4096)))?; Ok(value) }
}

#[cfg(unix)]
fn process_value(name: &str, memory: &Arc<MemoryBudget>) -> Result<Option<EnvironmentValue>> {
    use std::ffi::{CStr, CString, c_char};
    unsafe extern "C" { fn getenv(name: *const c_char) -> *mut c_char; }
    let _name = memory.reserve(add(mul(name.len(), 2)?, 4096)?)?;
    let Ok(name) = CString::new(name) else { return Ok(None); };
    let value = unsafe { getenv(name.as_ptr()) };
    if value.is_null() { return Ok(None); }
    let Ok(value) = unsafe { CStr::from_ptr(value) }.to_str() else { return Ok(None); };
    EnvironmentValue::copy(value, memory).map(Some)
}

#[cfg(windows)]
fn process_value(name: &str, memory: &Arc<MemoryBudget>) -> Result<Option<EnvironmentValue>> {
    #[link(name = "kernel32")]
    unsafe extern "system" { fn GetEnvironmentVariableW(name: *const u16, buffer: *mut u16, size: u32) -> u32; fn GetLastError() -> u32; fn SetLastError(error: u32); }
    if name.contains('\0') { return Ok(None); }
    let _name = memory.reserve(add(mul(name.len(), 4)?, 4096)?)?;
    let name: Vec<u16> = name.encode_utf16().chain(Some(0)).collect();
    loop {
        unsafe { SetLastError(0); }
        let capacity = unsafe { GetEnvironmentVariableW(name.as_ptr(), std::ptr::null_mut(), 0) };
        if capacity == 0 {
            return if unsafe { GetLastError() } == 203 { Ok(None) } else { EnvironmentValue::copy("", memory).map(Some) };
        }
        let reservation = memory.reserve(add(mul(capacity as usize, 256)?, 4096)?)?;
        let mut buffer = Zeroizing::new(vec![0u16; capacity as usize]);
        let length = unsafe { GetEnvironmentVariableW(name.as_ptr(), buffer.as_mut_ptr(), capacity) };
        if length >= capacity { continue; }
        if length == 0 && unsafe { GetLastError() } == 203 { return Ok(None); }
        let Ok(value) = String::from_utf16(&buffer[..length as usize]) else { return Ok(None); };
        return Ok(Some(EnvironmentValue { value: Zeroizing::new(value), reservation }));
    }
}

#[cfg(not(any(unix, windows)))]
fn process_value(_: &str, _: &Arc<MemoryBudget>) -> Result<Option<EnvironmentValue>> { Err(Anomaly::unsupported("rtdb.config/environment-platform", "Budgeted environment lookup requires a Unix or Windows host")) }

fn add(left: usize, right: usize) -> Result<usize> { left.checked_add(right).ok_or_else(|| invalid("Configuration allocation size overflow")) }
fn mul(left: usize, right: usize) -> Result<usize> { left.checked_mul(right).ok_or_else(|| invalid("Configuration allocation size overflow")) }
