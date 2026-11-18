use std::sync::Arc;
use rdkafka::ordered_metadata::{ProtocolError, RawMetadataView};
use rtdb_types::{Anomaly, MemoryBudget, java_hash_map::Map};
use super::{refresh::{JavaBackoffObservations, RefreshState, RequestKind, RequestScope}, round_robin::{PartitionChoices, RoundRobinCounter, SelectedPartition}};

mod owned;
mod keys;
mod response;
mod snapshot;
mod topics;
mod merge;
mod bootstrap;
mod random;
mod exchange;
mod updater;
mod wait;
mod fatal;
mod monitor;

pub use keys::{IdentityWitness, NoIdentityWitness, TopicId, TopicPartitionKey};
pub use owned::Text;
pub use response::{Node, Partition};
pub use snapshot::Snapshot;
pub use bootstrap::{BootstrapAddresses, ResolvedEndpoint, JavaShuffleObservations, MissingShuffleObservations, ShuffleAdmission};
pub use random::{JavaRandom, JavaRandomSeeds, JavaShuffleRandom};
pub use exchange::{MetadataExchange, MetadataTarget, SubmissionFailure};
pub use updater::{MetadataNetwork, MetadataUpdater, SendMetadata, UpdateFailure, UpdateSettings};
pub use wait::{MetadataWait, WaitFailure, WaitStep};
pub use fatal::{FatalKind, MetadataError, MetadataTopicError, MetadataWaitObservation};
pub use monitor::{MetadataMonitor, WaitCompletion};
use keys::Ops;
use owned::{Budget, Lease, Shared, Vector};
use response::{Holder, Table, invalid_metadata};
use snapshot::{Input, Names, Sets};
use topics::ActiveTopics;

/// Source semantic failures remain distinct from native resource or unsupported-profile failures.
#[derive(Debug)]
pub enum Failure {
    /// Admission, allocation, identity-witness or unsupported collection geometry failure.
    Native(Anomaly),
    /// A raw wire view failed validation before its consumer completed.
    Protocol(ProtocolError),
    /// The response holder's broker collector encountered this duplicate ID.
    DuplicateBroker(i32),
    /// The snapshot's reverse topic collector encountered this duplicate UUID.
    DuplicateTopicId(TopicId),
    /// Producer topic retention received a null name.
    NullTopicName,
    /// MetadataResponse.errors encountered a null name before checking the topic's error code.
    NullErrorTopic,
    /// A reverse UUID collector encountered a null topic-name value.
    NullTopicIdName,
    /// Metadata response application was requested after close.
    Closed,
    /// Selection consumed the topic counter but found no partition.
    EmptyTopic,
    /// Rebootstrap was requested before any bootstrap addresses were installed.
    MissingBootstrapAddresses,
    /// A previous native failure ended this mutable producer capability.
    CapabilityFailed,
}
impl From<Anomaly> for Failure { fn from(value: Anomaly) -> Self { Self::Native(value) } }
impl From<ProtocolError> for Failure { fn from(value: ProtocolError) -> Self { Self::Protocol(value) } }
pub type Outcome<T> = Result<T, Failure>;

/// Kafka duration settings, all in signed milliseconds.
pub struct Settings {
    pub refresh_backoff_ms: i64,
    pub refresh_backoff_max_ms: i64,
    pub metadata_expire_ms: i64,
    pub metadata_idle_ms: i64,
}

/// One source request scope and its admitted topic list in builder encounter order.
/// Carry the scope unchanged to the corresponding response; automatic topic creation is enabled.
pub struct MetadataRequest { scope: RequestScope, topics: Vector<Text> }
impl MetadataRequest {
    pub fn scope(&self) -> RequestScope { self.scope }
    pub fn topics(&self) -> impl ExactSizeIterator<Item = &str> { self.topics.as_slice().iter().map(Text::as_str) }
    pub fn allow_auto_topic_creation(&self) -> bool { true }
}

/// The next network-owner action after a complete successful Metadata response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResponseAction { Published, RetryEmptyBrokers, Rebootstrap }

struct Counter { topic: Text, value: RoundRobinCounter }

/// A serialized producer's mutable metadata capability and persistent per-topic counters.
/// Native resource and unsupported-profile failures end mutation; previously retained snapshots stay readable.
/// The caller owns network scheduling, connection throttles, fatal errors and waiter notification.
pub struct ProducerMetadata {
    budget: Budget,
    refresh: RefreshState,
    idle_ms: i64,
    active: ActiveTopics,
    new_topics: Names,
    epochs: Map<TopicPartitionKey, i32, Lease>,
    snapshot: Snapshot,
    bootstrap_addresses: Option<BootstrapAddresses>,
    counters: Vector<Counter>,
    errors: Option<Table<Option<Text>, i16>>,
    invalid: Table<Option<Text>, ()>,
    unauthorized: Table<Option<Text>, ()>,
    fatal: Option<MetadataError>,
    bootstrap_fatal: Option<MetadataError>,
    failed: bool,
}

impl ProducerMetadata {
    /// Constructs empty metadata using the caller's retained budget and Java math observations.
    /// Settings must satisfy Kafka's nonnegative duration validation.
    /// Use `new_with_cluster_nodes` when a caller needs qualified Cluster node order.
    pub fn new(settings: Settings, budget: Arc<MemoryBudget>, observations: &mut impl JavaBackoffObservations, identity: &mut impl IdentityWitness) -> Outcome<Self> {
        Self::new_inner(settings, budget, observations, identity, None)
    }

    /// Constructs empty metadata after observing the default shuffle generator acquisition.
    /// The same caller-owned generator history must supply all later Cluster constructions.
    /// Settings and capacity follow `new`.
    pub fn new_with_cluster_nodes(settings: Settings, budget: Arc<MemoryBudget>, observations: &mut impl JavaBackoffObservations, identity: &mut impl IdentityWitness, shuffle: &mut dyn JavaShuffleObservations) -> Outcome<Self> {
        Self::new_inner(settings, budget, observations, identity, Some(shuffle))
    }

    fn new_inner(settings: Settings, budget: Arc<MemoryBudget>, observations: &mut impl JavaBackoffObservations, identity: &mut impl IdentityWitness, shuffle: Option<&mut dyn JavaShuffleObservations>) -> Outcome<Self> {
        let mut budget = Budget(budget);
        let snapshot = Snapshot::empty(&mut budget, identity, shuffle)?;
        let refresh = RefreshState::new(settings.refresh_backoff_ms, settings.refresh_backoff_max_ms, settings.metadata_expire_ms, observations);
        Ok(Self {
            snapshot, bootstrap_addresses: None, refresh, idle_ms: settings.metadata_idle_ms,
            active: ActiveTopics::new(), new_topics: Map::new(None)?, epochs: Map::new(None)?,
            counters: Vector::new(), errors: None, invalid: Map::new(None)?.finish(&mut budget)?,
            unauthorized: Map::new(None)?.finish(&mut budget)?,
            fatal: None, bootstrap_fatal: None, budget, failed: false,
        })
    }

    fn usable(&self) -> Outcome<()> { if self.failed { Err(Failure::CapabilityFailed) } else { Ok(()) } }
    fn completed<T>(&mut self, result: Outcome<T>) -> Outcome<T> {
        if matches!(&result, Err(Failure::Native(_))) { self.failed = true; }
        result
    }

    /// Retains the current immutable generation, including after mutable-capability failure.
    pub fn snapshot(&self) -> Snapshot { self.snapshot.clone() }
    /// Borrows the source refresh schedule; it owns no network or waiter activity.
    pub fn refresh(&self) -> &RefreshState { &self.refresh }
    /// Returns the retry/expiry delay in milliseconds, observing Java math/random calls in source order.
    pub fn time_to_next_update(&mut self, now_ms: i64, observations: &mut impl JavaBackoffObservations) -> Outcome<i64> {
        self.usable()?;
        Ok(self.refresh.time_to_next_update(now_ms, observations))
    }
    /// Closes metadata response application; already retained snapshots remain usable.
    pub fn close(&mut self) { self.refresh.close(); }
    /// Records one failed network metadata attempt without replacing the snapshot.
    pub fn failed_response(&mut self, now_ms: i64) -> Outcome<()> {
        self.usable()?; self.refresh.failed_response(now_ms); Ok(())
    }

    /// Requests all active topics and optionally resets equivalent-response backoff, returning the source update version.
    pub fn request_full(&mut self, reset_equivalent_backoff: bool) -> Outcome<i32> {
        self.usable()?; Ok(self.refresh.request_full(reset_equivalent_backoff))
    }

    /// Requests a partial refresh for a new topic, otherwise a full refresh, and returns the source update version.
    pub fn request_for_topic(&mut self, topic: &str, identity: &mut impl IdentityWitness) -> Outcome<i32> {
        self.usable()?;
        let result = (|| {
            let name = Some(Text::from_raw(topic.as_bytes(), &mut self.budget)?);
            Ok(if self.new_topics.contains_key(&name, &mut Ops(identity), &mut self.budget)? {
                self.refresh.request_new_topics()
            } else { self.refresh.request_full(false) })
        })();
        self.completed(result)
    }

    /// Refreshes one non-null topic's expiry, requesting new-topic metadata only for a newly inserted topic.
    pub fn add_topic(&mut self, topic: &str, now_ms: i64, identity: &mut impl IdentityWitness) -> Outcome<()> {
        self.usable()?;
        let result = (|| {
            let name = Text::from_raw(topic.as_bytes(), &mut self.budget)?;
            let expiry = now_ms.wrapping_add(self.idle_ms);
            if self.active.refresh_existing(&name, expiry) { return Ok(()); }
            if self.active.put(name.clone(), expiry, &mut self.budget)? {
                self.new_topics.put(Some(name), (), &mut Ops(identity), &mut self.budget)?;
                self.refresh.request_new_topics();
            }
            Ok(())
        })();
        self.completed(result)
    }

    /// Processes the supplied non-null topic sequence and requests one refresh if any topic is new.
    /// Returns the source update version only when that refresh was requested.
    pub fn add_topics<'a>(&mut self, topics: impl IntoIterator<Item = &'a str>, now_ms: i64, identity: &mut impl IdentityWitness) -> Outcome<Option<i32>> {
        self.usable()?;
        let result = (|| {
            let mut any_new = false;
            for topic in topics {
                let name = Text::from_raw(topic.as_bytes(), &mut self.budget)?;
                if self.active.put(name.clone(), now_ms.wrapping_add(self.idle_ms), &mut self.budget)? {
                    self.new_topics.put(Some(name), (), &mut Ops(identity), &mut self.budget)?;
                    any_new = true;
                }
            }
            Ok(any_new.then(|| self.refresh.request_new_topics()))
        })();
        self.completed(result)
    }

    /// Captures a request's scope and admitted names in the selected Java collection's encounter order.
    pub fn request(&mut self, now_ms: i64) -> Outcome<MetadataRequest> {
        self.usable()?;
        let result = (|| {
            let scope = self.refresh.new_request(now_ms);
            let mut topics = match scope.kind() {
                RequestKind::Full => Vector::with_capacity(self.active.iter().len(), &mut self.budget)?,
                RequestKind::NewTopics => Vector::with_capacity(self.new_topics.len(), &mut self.budget)?,
            };
            match scope.kind() {
                RequestKind::Full => for name in self.active.iter() { topics.push(name.clone(), &mut self.budget)?; },
                RequestKind::NewTopics => for (name, _) in self.new_topics.try_iter()? { topics.push(name.as_ref().expect("added producer topic is non-null").clone(), &mut self.budget)?; },
            }
            Ok(MetadataRequest { scope, topics })
        })();
        self.completed(result)
    }

    fn retain_topic(&mut self, name: &Option<Text>, now_ms: i64, identity: &mut impl IdentityWitness) -> Outcome<bool> {
        let topic = name.as_ref().ok_or(Failure::NullTopicName)?;
        let Some(expiry) = self.active.expiry(topic) else { return Ok(false); };
        if self.new_topics.contains_key(name, &mut Ops(identity), &mut self.budget)? || expiry > now_ms { return Ok(true); }
        self.active.remove(topic);
        Ok(false)
    }

    fn latest(&mut self, partition: &Shared<Partition>, reliable: bool, topic_id: Option<TopicId>, old_id: Option<TopicId>, identity: &mut impl IdentityWitness) -> Outcome<Option<Shared<Partition>>> {
        if let Some(epoch) = partition.epoch.filter(|_| reliable) {
            let old = self.epochs.get(&partition.key, &mut Ops(identity), &mut self.budget)?.copied();
            if old.is_none() || (topic_id.is_some() && topic_id != old_id) || old.is_some_and(|old| epoch >= old) {
                self.epochs.put(partition.key.clone(), epoch, &mut Ops(identity), &mut self.budget)?;
                if old.is_none() || (topic_id.is_some() && topic_id != old_id) || old.is_some_and(|old| epoch > old) { self.refresh.metadata_progress(); }
                Ok(Some(partition.clone()))
            } else {
                Ok(self.snapshot.0.partitions.get(&partition.key, &mut Ops(identity), &mut self.budget)?.cloned())
            }
        } else {
            self.epochs.remove(&partition.key, &mut Ops(identity), &mut self.budget)?;
            self.refresh.metadata_progress();
            Ok(Some(partition.without_epoch(&mut self.budget)?))
        }
    }

    /// Applies one validated raw response after connection throttling.
    /// Holder construction precedes empty-broker/rebootstrap handling; source refresh mutations precede snapshot construction.
    /// Native failure terminates mutation and may follow snapshot publication.
    /// Newly published generations have unqualified Cluster node order. Use `apply_with_cluster_nodes` to retain qualified order.
    pub fn apply(&mut self, view: RawMetadataView<'_>, scope: RequestScope, now_ms: i64, rebootstrap_enabled: bool, identity: &mut impl IdentityWitness) -> Outcome<ResponseAction> {
        self.usable()?;
        let result = self.apply_inner(view, scope, now_ms, rebootstrap_enabled, identity, None);
        self.completed(result)
    }

    /// Applies a response and qualifies the new generation's Cluster node order before publication.
    /// A shuffle failure leaves the previous snapshot installed, retains earlier refresh mutations, and ends native mutation.
    /// All response, ownership and identity contracts of `apply` still hold.
    pub fn apply_with_cluster_nodes(&mut self, view: RawMetadataView<'_>, scope: RequestScope, now_ms: i64, rebootstrap_enabled: bool, identity: &mut impl IdentityWitness, shuffle: &mut dyn JavaShuffleObservations) -> Outcome<ResponseAction> {
        self.usable()?;
        let result = self.apply_inner(view, scope, now_ms, rebootstrap_enabled, identity, Some(shuffle));
        self.completed(result)
    }

    fn apply_inner(&mut self, view: RawMetadataView<'_>, scope: RequestScope, now_ms: i64, rebootstrap_enabled: bool, identity: &mut impl IdentityWitness, shuffle: Option<&mut dyn JavaShuffleObservations>) -> Outcome<ResponseAction> {
        let holder = Holder::read(view, &mut self.budget, identity)?;
        let _network_errors = holder.errors(&mut self.budget, identity)?;
        if rebootstrap_enabled && holder.top_error == 129 { return Ok(ResponseAction::Rebootstrap); }
        if holder.nodes.is_empty() { self.refresh.failed_response(now_ms); return Ok(ResponseAction::RetryEmptyBrokers); }
        self.refresh.begin_response(scope, now_ms).map_err(|_| Failure::Closed)?;
        let mut seen: Names = Map::new(None)?;
        let mut sets = Sets::new()?;
        let mut partitions = Vector::new();
        let mut topic_ids = Map::new(None)?;
        for topic in holder.topics.as_slice() {
            seen.put(topic.name.clone(), (), &mut Ops(identity), &mut self.budget)?;
            let id = topic.id.nonzero();
            let old_id = if let Some(id) = id {
                topic_ids.put(topic.name.clone(), id, &mut Ops(identity), &mut self.budget)?;
                self.snapshot.0.topic_ids.get(&topic.name, &mut Ops(identity), &mut self.budget)?.copied()
            } else { None };
            if !self.retain_topic(&topic.name, now_ms, identity)? { continue; }
            if topic.internal { sets.internal.put(topic.name.clone(), (), &mut Ops(identity), &mut self.budget)?; }
            if topic.error == 0 {
                for partition in topic.partitions.as_slice() {
                    if let Some(partition) = self.latest(partition, holder.reliable_epochs, id, old_id, identity)? { partitions.push(partition, &mut self.budget)?; }
                    if invalid_metadata(partition.error) { self.refresh.request_full(false); }
                }
            } else {
                if invalid_metadata(topic.error) { self.refresh.request_full(false); }
                if topic.error == 17 { sets.invalid.put(topic.name.clone(), (), &mut Ops(identity), &mut self.budget)?; }
                else if topic.error == 29 { sets.unauthorized.put(topic.name.clone(), (), &mut Ops(identity), &mut self.budget)?; }
            }
        }
        let mut input = Input { nodes: holder.nodes.clone(), cluster_id: holder.cluster_id.clone(), controller: holder.controller, partitions, sets, topic_ids };
        if scope.kind() == RequestKind::NewTopics { input = self.merge(input, &seen, now_ms, identity)?; }
        self.snapshot = Snapshot::build(input, &mut self.budget, identity, shuffle)?;
        self.invalid = Map::new(None)?.finish(&mut self.budget)?;
        self.unauthorized = Map::new(None)?.finish(&mut self.budget)?;
        if !self.snapshot.0.invalid.is_empty() { self.invalid = merge::copy_set(&self.snapshot.0.invalid, &mut self.budget, identity)?.finish(&mut self.budget)?; }
        if !self.snapshot.0.unauthorized.is_empty() { self.unauthorized = merge::copy_set(&self.snapshot.0.unauthorized, &mut self.budget, identity)?.finish(&mut self.budget)?; }
        self.prune_epochs(now_ms, identity)?;
        self.errors = Some(holder.errors(&mut self.budget, identity)?);
        if self.new_topics.len() != 0 {
            for topic in holder.topics.as_slice() { self.new_topics.remove(&topic.name, &mut Ops(identity), &mut self.budget)?; }
        }
        Ok(ResponseAction::Published)
    }

    /// Returns the retained topic-level error.
    /// Rebootstrap, empty-broker retries and failed response application can leave an earlier error table retained.
    pub fn topic_error(&self, topic: &str) -> Option<i16> {
        self.errors.as_ref()?.iter().find(|(name, _)| name.as_ref().is_some_and(|name| name.as_str() == topic)).map(|(_, error)| *error)
    }

    /// Takes a topic's recoverable error, prioritizing authorization over invalid-name failure, then clears both sets for every topic.
    /// The enclosing producer must check bootstrap and fatal errors before calling this operation.
    pub fn take_topic_error(&mut self, topic: &str) -> Outcome<Option<i16>> {
        self.usable()?;
        let contains = |set: &Table<Option<Text>, ()>| set.iter().any(|(name, _)| name.as_ref().is_some_and(|name| name.as_str() == topic));
        let error = if contains(&self.unauthorized) { Some(29) } else if contains(&self.invalid) { Some(17) } else { None };
        let result = self.clear_recoverable_errors().map(|()| error);
        self.completed(result)
    }

    fn clear_recoverable_errors(&mut self) -> Outcome<()> {
        self.invalid = Map::new(None)?.finish(&mut self.budget)?;
        self.unauthorized = Map::new(None)?.finish(&mut self.budget)?;
        Ok(())
    }

    /// Consumes one persistent topic counter and fixes the selected partition for this record and every retry.
    /// The caller supplies an already admitted record owner; keys do not affect this stock RoundRobin policy.
    /// An empty topic still consumes its counter, while native admission failure ends this mutable capability.
    pub fn select_send<T>(&mut self, topic: &str, record: T) -> Outcome<PreparedSend<T>> {
        self.usable()?;
        let result = (|| {
            let index = match self.counters.as_slice().iter().position(|counter| counter.topic.as_str() == topic) {
                Some(index) => index,
                None => {
                    let index = self.counters.len();
                    let topic = Text::from_raw(topic.as_bytes(), &mut self.budget)?;
                    self.counters.push(Counter { topic, value: RoundRobinCounter::new() }, &mut self.budget)?;
                    index
                }
            };
            let choices = PartitionChoices::new(self.snapshot.partitions(topic)).map_err(|_| Anomaly::unsupported("rtdb/kafka-partition-list-size", "Java partition lists require a signed int length"))?;
            let partition = self.counters.as_mut_slice()[index].value.select(choices).map_err(|_| Failure::EmptyTopic)?;
            Ok(PreparedSend { record, partition, snapshot: self.snapshot.clone() })
        })();
        self.completed(result)
    }
}

/// A record with one chosen partition and the independently retained generation used for selection.
/// Retry submission must reuse this partition; the record owner must retain its payload admission through delivery.
pub struct PreparedSend<T> { record: T, partition: SelectedPartition, snapshot: Snapshot }
impl<T> PreparedSend<T> {
    pub fn partition_id(&self) -> i32 { self.partition.id() }
    pub fn record(&self) -> &T { &self.record }
    pub fn snapshot(&self) -> &Snapshot { &self.snapshot }
    pub fn into_parts(self) -> (T, SelectedPartition, Snapshot) { (self.record, self.partition, self.snapshot) }
}

#[cfg(test)]
mod tests;
