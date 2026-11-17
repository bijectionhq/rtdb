use std::collections::BTreeMap;
use std::sync::Arc;
use async_trait::async_trait;
use rtdb_types::Result;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use super::{ConnectConfig, ConnectData, PluginSpec, SinkRecord, error};

#[async_trait]
pub trait Converter: Send {
    /// None is a Kafka tombstone; failures stop the assignment before submitting its current batch.
    /// Reserve before allocating and retain shared cache charges in `context`; its owner must outlive the returned data.
    async fn decode(&mut self, topic: &str, bytes: Option<&[u8]>, context: &mut ConversionContext) -> Result<ConnectData>;
    /// The assignment owner awaits this after its final or cancelled decode, before releasing converter state.
    /// Implementations drain admitted external work even when decode returned an error.
    async fn close(&mut self) -> Result<()> { Ok(()) }
}
pub struct ConversionContext { memory: Arc<MemoryBudget>, charges: Vec<Arc<MemoryReservation>>, limit: usize, reserved: usize, registry_key: Option<Vec<u8>> }
impl ConversionContext {
    pub fn new(memory: Arc<MemoryBudget>) -> Self { let limit = memory.limit_bytes(); Self { memory, charges: Vec::new(), limit, reserved: 0, registry_key: None } }
    pub fn with_limit(memory: Arc<MemoryBudget>, limit: usize) -> Self { Self { memory, charges: Vec::new(), limit, reserved: 0, registry_key: None } }
    pub fn allocation_limit(&self) -> usize { self.limit }
    pub fn reserve(&mut self, bytes: usize) -> Result<Arc<MemoryReservation>> {
        let reserved = self.reserved.checked_add(bytes).filter(|bytes| *bytes <= self.limit).ok_or_else(|| error("batch-too-large", "Converter allocation exceeds its per-record limit"))?;
        let charge = self.memory.reserve(bytes)?; self.charges.push(Arc::clone(&charge)); self.reserved = reserved; Ok(charge)
    }
    pub fn retain(&mut self, charge: Arc<MemoryReservation>) -> Result<()> {
        if self.charges.iter().any(|existing| Arc::ptr_eq(existing, &charge)) { return Ok(()); }
        let reserved = self.reserved.checked_add(charge.bytes()).filter(|bytes| *bytes <= self.limit).ok_or_else(|| error("batch-too-large", "Retained converter allocation exceeds its per-record limit"))?;
        self.charges.push(charge); self.reserved = reserved; Ok(())
    }
    pub(super) fn set_registry_key(&mut self, bytes: Option<&[u8]>) -> Result<()> {
        if let Some(bytes) = bytes { self.reserve(bytes.len().checked_add(64).ok_or_else(|| error("allocation-overflow", "Registry key allocation overflow"))?)?; }
        self.registry_key = bytes.map(<[u8]>::to_vec);
        Ok(())
    }
    pub(super) fn registry_key(&self) -> Option<&[u8]> { self.registry_key.as_deref() }
    pub(super) fn clear_registry_key(&mut self) { self.registry_key = None; }
    pub(crate) fn into_charges(self) -> Vec<Arc<MemoryReservation>> { self.charges }
}
pub trait ConverterFactory: Send + Sync { fn open(&self, options: &BTreeMap<String, String>, is_key: bool) -> Result<Box<dyn Converter>>; }
pub trait Transform: Send {
    /// Peak additional allocation before `apply`; override when expansion exceeds four times the input's retained bytes.
    fn allocation_bound(&self, record: &SinkRecord) -> Result<usize> { record.retained_bytes()?.checked_mul(4).ok_or_else(|| error("allocation-overflow", "Transform allocation bound overflow")) }
    fn apply(&mut self, record: SinkRecord) -> Result<Option<SinkRecord>>;
}
pub trait TransformFactory: Send + Sync { fn open(&self, options: &BTreeMap<String, String>) -> Result<Box<dyn Transform>>; }
pub trait Predicate: Send { fn test(&mut self, record: &SinkRecord) -> Result<bool>; }
pub trait PredicateFactory: Send + Sync { fn open(&self, options: &BTreeMap<String, String>) -> Result<Box<dyn Predicate>>; }

#[derive(Clone, Default)]
pub struct PluginRegistry {
    converters: BTreeMap<String, Arc<dyn ConverterFactory>>,
    transforms: BTreeMap<String, Arc<dyn TransformFactory>>,
    predicates: BTreeMap<String, Arc<dyn PredicateFactory>>,
}
impl PluginRegistry {
    /// Additional bytes for cloned registry maps and class names; factory objects remain shared.
    pub fn clone_allocation_bound(&self) -> Result<usize> {
        self.converters.keys().chain(self.transforms.keys()).chain(self.predicates.keys()).try_fold(4096usize, |bytes, name| {
            bytes.checked_add(name.capacity()).and_then(|bytes| bytes.checked_add(512)).ok_or_else(|| error("allocation-overflow", "Plugin registry clone allocation overflow"))
        })
    }
    pub fn with_converter(mut self, class: String, factory: Arc<dyn ConverterFactory>) -> Result<Self> {
        if self.converters.insert(class, factory).is_some() { return Err(error("invalid-config", "Duplicate converter registration")); } Ok(self)
    }
    pub fn with_transform(mut self, class: String, factory: Arc<dyn TransformFactory>) -> Result<Self> {
        if self.transforms.insert(class, factory).is_some() { return Err(error("invalid-config", "Duplicate transform registration")); } Ok(self)
    }
    pub fn with_predicate(mut self, class: String, factory: Arc<dyn PredicateFactory>) -> Result<Self> {
        if self.predicates.insert(class, factory).is_some() { return Err(error("invalid-config", "Duplicate predicate registration")); } Ok(self)
    }
    pub fn converter(&self, spec: &PluginSpec, is_key: bool) -> Result<Box<dyn Converter>> {
        if let Some(factory) = self.converters.get(&spec.class_name) { return factory.open(&spec.options, is_key); }
        super::converter::open(spec, is_key)
    }
    /// Opens an assignment-owned, single-writer transform chain with explicit process date defaults.
    /// It opens each referenced predicate once in first-use order, then opens the transforms in declaration order.
    /// Built-in date transforms retain the shared environment; custom factories retain their own configuration.
    /// Eager configuration and allocation failures return before publishing the chain.
    /// TimestampRouter constructs its formatter on the first timestamped record, so its pattern errors are returned by `apply`.
    pub fn chain_with_environment(&self, config: &ConnectConfig, environment: Arc<super::DateEnvironment>) -> Result<TransformChain> {
        let mut predicates = BTreeMap::new();
        for alias in config.transforms.iter().filter_map(|spec| spec.predicate.as_ref()) {
            if !predicates.contains_key(alias) {
                let predicate = config.predicates.get(alias).ok_or_else(|| error("undeclared-predicate", alias))?;
                let opened = match self.predicates.get(&predicate.class_name) { Some(factory) => factory.open(&predicate.options)?, None => super::transforms::predicate(predicate)? };
                predicates.insert(alias.clone(), opened);
            }
        }
        let mut steps = Vec::new();
        for spec in &config.transforms {
            let transform = match self.transforms.get(&spec.plugin.class_name) { Some(factory) => factory.open(&spec.plugin.options)?, None => super::transforms::open(&spec.plugin, Arc::clone(&environment))? };
            let configuration_bytes = spec.plugin.options.iter().try_fold(4096usize, |size, (key, value)| size.checked_add(key.len()).and_then(|size| size.checked_add(value.len())).ok_or_else(|| error("allocation-overflow", "Transform configuration size overflow")))?;
            steps.push(Step { transform, predicate: spec.predicate.clone(), negate: spec.negate, configuration_bytes });
        }
        Ok(TransformChain { steps, predicates })
    }
}
struct Step { transform: Box<dyn Transform>, predicate: Option<String>, negate: bool, configuration_bytes: usize }
pub struct TransformChain { steps: Vec<Step>, predicates: BTreeMap<String, Box<dyn Predicate>> }
impl TransformChain {
    pub fn apply(&mut self, mut record: SinkRecord) -> Result<Option<SinkRecord>> {
        for step in &mut self.steps {
            let apply = match &step.predicate { None => true, Some(alias) => self.predicates.get_mut(alias).ok_or_else(|| error("undeclared-predicate", alias))?.test(&record)? != step.negate };
            if apply {
                if let Some(reservation) = record.reservations.first() {
                    let bound = step.transform.allocation_bound(&record)?.checked_add(step.configuration_bytes.checked_mul(8).ok_or_else(|| error("allocation-overflow", "Transform configuration bound overflow"))?).ok_or_else(|| error("allocation-overflow", "Transform allocation bound overflow"))?;
                    let charge = reservation.budget().reserve(bound)?; record.reservations.push(charge);
                }
                match step.transform.apply(record)? { Some(next) => record = next, None => return Ok(None) }
            }
        }
        Ok(Some(record))
    }
}

#[cfg(test)]
mod tests;
