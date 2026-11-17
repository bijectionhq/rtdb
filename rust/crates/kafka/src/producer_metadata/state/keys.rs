use std::{cmp::Ordering, sync::OnceLock};
use rtdb_types::{Anomaly, Result, java_hash_map::KeyOps};
use super::owned::{Budget, Shared, Text};

/// A source Java object's stable identity witness provider.
/// Called lazily only when HashMap's actual tree insertion needs System.identityHashCode.
pub trait IdentityWitness {
    /// Returns this object's observed signed Java identity hash, consistently across maps and calls.
    /// Native pointer values, region IDs and allocation counters are not Java identity hashes.
    fn topic_partition_hash(&mut self, key: &TopicPartitionKey) -> Result<i32>;
    /// Returns a string object's observed Java System.identityHashCode when a null/string collision needs identity ordering.
    /// The default rejects that profile without substituting native identity.
    fn string_hash(&mut self, _: &Text) -> Result<i32> { Err(identity_required()) }
}

/// Rejects an identity-dependent collision profile.
pub struct NoIdentityWitness;
impl IdentityWitness for NoIdentityWitness {
    fn topic_partition_hash(&mut self, _: &TopicPartitionKey) -> Result<i32> { Err(identity_required()) }
}
fn identity_required() -> Anomaly {
    Anomaly::unsupported("rtdb/kafka-java-identity-required", "Java object identity hash observation is required")
}

/// One admitted TopicPartition object; clones retain its object identity and any lazily observed hash.
#[derive(Clone)]
pub struct TopicPartitionKey(Shared<PartitionKeyValue>);
struct PartitionKeyValue { topic: Option<Text>, partition: i32, identity_hash: OnceLock<i32> }
impl TopicPartitionKey {
    pub(super) fn new(topic: Option<Text>, partition: i32, budget: &mut Budget) -> Result<Self> {
        Ok(Self(Shared::new(PartitionKeyValue { topic, partition, identity_hash: OnceLock::new() }, budget)?))
    }
    /// Borrows the nullable source topic name after Java replacement decoding.
    pub fn topic(&self) -> Option<&str> { self.0.topic.as_ref().map(Text::as_str) }
    /// Returns the source signed partition ID.
    pub fn partition(&self) -> i32 { self.0.partition }
    /// Compares source object ownership.
    pub fn same_object(&self, other: &Self) -> bool { self.0.same_object(&other.0) }
    pub(super) fn name(&self) -> &Option<Text> { &self.0.topic }
}

/// A Kafka UUID's exact network-order bits; all-zero denotes an absent topic ID in Metadata.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TopicId(pub [u8; 16]);
impl TopicId {
    pub(super) fn nonzero(self) -> Option<Self> { if self.0 == [0; 16] { None } else { Some(self) } }
    fn halves(self) -> (i64, i64) {
        (i64::from_be_bytes(self.0[..8].try_into().unwrap()), i64::from_be_bytes(self.0[8..].try_into().unwrap()))
    }
}

pub(super) trait Key {
    const CLASS: &'static str;
    fn is_null(&self) -> bool { false }
    fn same(&self, _: &Self) -> bool { false }
    fn hash(&self) -> i32;
    fn equal(&self, other: &Self) -> bool;
    fn comparable(&self) -> bool { true }
    fn compare(&self, other: &Self) -> Ordering;
    fn identity(&self, _: &mut impl IdentityWitness) -> Result<i32> { Err(identity_required()) }
}
impl Key for Option<Text> {
    const CLASS: &'static str = "java.lang.String";
    fn is_null(&self) -> bool { self.is_none() }
    fn same(&self, other: &Self) -> bool { matches!((self, other), (Some(a), Some(b)) if a.same_object(b)) }
    fn hash(&self) -> i32 { self.as_ref().map_or(0, Text::hash) }
    fn equal(&self, other: &Self) -> bool { self == other }
    fn comparable(&self) -> bool { self.is_some() }
    fn compare(&self, other: &Self) -> Ordering {
        match (self, other) { (Some(a), Some(b)) => a.as_str().encode_utf16().cmp(b.as_str().encode_utf16()), _ => Ordering::Equal }
    }
    fn identity(&self, provider: &mut impl IdentityWitness) -> Result<i32> {
        let Some(text) = self else { return Ok(0); };
        if let Some(hash) = text.identity_hash().get() { return Ok(*hash); }
        let hash = provider.string_hash(text)?;
        if text.identity_hash().set(hash).is_err() && text.identity_hash().get() != Some(&hash) {
            return Err(Anomaly::incorrect("rtdb/kafka-java-identity-conflict", "Conflicting Java identity hash observations for one String object"));
        }
        Ok(hash)
    }
}
impl Key for i32 {
    const CLASS: &'static str = "java.lang.Integer";
    fn hash(&self) -> i32 { *self }
    fn equal(&self, other: &Self) -> bool { self == other }
    fn compare(&self, other: &Self) -> Ordering { self.cmp(other) }
}
impl Key for TopicId {
    const CLASS: &'static str = "org.apache.kafka.common.Uuid";
    fn hash(&self) -> i32 { let (a, b) = self.halves(); let xor = a ^ b; (xor >> 32) as i32 ^ xor as i32 }
    fn equal(&self, other: &Self) -> bool { self == other }
    fn compare(&self, other: &Self) -> Ordering { self.halves().cmp(&other.halves()) }
}
impl Key for TopicPartitionKey {
    const CLASS: &'static str = "org.apache.kafka.common.TopicPartition";
    fn same(&self, other: &Self) -> bool { self.same_object(other) }
    fn hash(&self) -> i32 { 31i32.wrapping_add(self.partition()).wrapping_mul(31).wrapping_add(self.name().hash()) }
    fn equal(&self, other: &Self) -> bool { self.partition() == other.partition() && self.name() == other.name() }
    fn comparable(&self) -> bool { false }
    fn compare(&self, _: &Self) -> Ordering { Ordering::Equal }
    fn identity(&self, provider: &mut impl IdentityWitness) -> Result<i32> {
        if let Some(hash) = self.0.identity_hash.get() { return Ok(*hash); }
        let hash = provider.topic_partition_hash(self)?;
        if self.0.identity_hash.set(hash).is_err() && self.0.identity_hash.get() != Some(&hash) {
            return Err(Anomaly::incorrect("rtdb/kafka-java-identity-conflict", "Conflicting Java identity hash observations for one TopicPartition object"));
        }
        Ok(hash)
    }
}

pub(super) struct Ops<'a, I>(pub &'a mut I);
impl<K: Key, I: IdentityWitness> KeyOps<K, Budget> for Ops<'_, I> {
    type Comparable = ();
    fn is_null(&self, key: &K) -> bool { key.is_null() }
    fn same_identity(&self, key: &K, other: &K) -> bool { key.same(other) }
    fn hash_code(&mut self, key: &K, _: &mut Budget) -> Result<i32> { Ok(key.hash()) }
    fn equals(&mut self, key: &K, other: &K, _: &mut Budget) -> Result<bool> { Ok(key.equal(other)) }
    fn comparable_class(&mut self, key: &K, _: &mut Budget) -> Result<Option<()>> { Ok(key.comparable().then_some(())) }
    fn compare(&mut self, _: (), key: &K, other: &K, _: &mut Budget) -> Result<i32> {
        Ok(match key.compare(other) { Ordering::Less => -1, Ordering::Equal => 0, Ordering::Greater => 1 })
    }
    fn class_name<'a>(&'a self, _: &'a K) -> &'a str { K::CLASS }
    fn identity_hash_code(&mut self, key: &K, _: &mut Budget) -> Result<i32> { key.identity(self.0) }
}
