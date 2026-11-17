use rdkafka::ordered_metadata::{RawEvent, RawMetadataView};
use rtdb_types::java_hash_map::{Map, NormalizedMap, PutOutcome};
use super::{Failure, Outcome, keys::{IdentityWitness, Ops, TopicId, TopicPartitionKey}, owned::{Budget, Lease, Shared, Text, Vector, array}};

pub(super) type Table<K, V> = NormalizedMap<K, V, Lease>;

/// An admitted broker endpoint from one Metadata response.
pub struct Node {
    /// Signed broker ID.
    pub id: i32,
    /// Java replacement-decoded host.
    pub host: Text,
    /// Signed advertised port.
    pub port: i32,
    /// Nullable Java replacement-decoded rack.
    pub rack: Option<Text>,
}

/// Immutable admitted partition metadata; replica collections retain their wire encounter order.
pub struct Partition {
    pub(super) key: TopicPartitionKey,
    pub(super) error: i16,
    pub(super) leader: Option<i32>,
    pub(super) epoch: Option<i32>,
    pub(super) replicas: Shared<Vector<i32>>,
    pub(super) isr: Shared<Vector<i32>>,
    pub(super) offline: Shared<Vector<i32>>,
}
impl Partition {
    /// Borrows the source TopicPartition object, including its nullable topic name.
    pub fn key(&self) -> &TopicPartitionKey { &self.key }
    /// Returns the normalized Kafka error code; unknown wire codes map to UNKNOWN_SERVER_ERROR (-1).
    pub fn error(&self) -> i16 { self.error }
    /// Returns a nonnegative leader ID; every negative wire leader is absent.
    pub fn leader(&self) -> Option<i32> { self.leader }
    /// Returns the retained source epoch; absent, -1 and unreliable epochs are None, while other negative epochs remain present.
    pub fn epoch(&self) -> Option<i32> { self.epoch }
    /// Borrows signed replica IDs in source order.
    pub fn replicas(&self) -> &[i32] { self.replicas.as_slice() }
    /// Borrows signed in-sync replica IDs in source order.
    pub fn isr(&self) -> &[i32] { self.isr.as_slice() }
    /// Borrows signed offline replica IDs in source order.
    pub fn offline(&self) -> &[i32] { self.offline.as_slice() }
    pub(super) fn without_epoch(&self, budget: &mut Budget) -> rtdb_types::Result<Shared<Self>> {
        Shared::new(Self { key: self.key.clone(), error: self.error, leader: self.leader, epoch: None,
            replicas: self.replicas.clone(), isr: self.isr.clone(), offline: self.offline.clone() }, budget)
    }
}

pub(super) struct Topic { pub name: Option<Text>, pub id: TopicId, pub error: i16, pub internal: bool, pub partitions: Vector<Shared<Partition>> }
pub(super) struct Holder {
    pub nodes: Shared<Table<i32, Shared<Node>>>, pub topics: Vector<Topic>,
    pub cluster_id: Option<Text>, pub controller: i32, pub reliable_epochs: bool, pub top_error: i16,
}

impl Holder {
    pub(super) fn errors(&self, budget: &mut Budget, identity: &mut impl IdentityWitness) -> Outcome<Table<Option<Text>, i16>> {
        let mut errors = Map::new(None)?;
        for topic in self.topics.as_slice() {
            if topic.name.is_none() { return Err(Failure::NullErrorTopic); }
            if topic.error != 0 { errors.put(topic.name.clone(), topic.error, &mut Ops(identity), budget)?; }
        }
        Ok(errors.finish(budget)?)
    }

    pub(super) fn read(view: RawMetadataView<'_>, budget: &mut Budget, identity: &mut impl IdentityWitness) -> Outcome<Self> {
        let mut brokers = Map::new(None)?;
        let mut cluster_id = None;
        let mut controller = -1;
        let mut result = Ok(());
        view.visit(|event| {
            if result.is_err() { return; }
            result = (|| {
                match event {
                    RawEvent::Broker(broker) => {
                        let node = Shared::new(Node { id: broker.id, host: Text::from_raw(broker.host, budget)?, port: broker.port,
                            rack: broker.rack.map(|raw| Text::from_raw(raw, budget)).transpose()? }, budget)?;
                        if matches!(brokers.put_if_absent(broker.id, node, |_| false, &mut Ops(identity), budget)?, PutOutcome::Present(_)) {
                            return Err(Failure::DuplicateBroker(broker.id));
                        }
                    }
                    RawEvent::Cluster { id, controller: id_controller } => {
                        cluster_id = id.map(|raw| Text::from_raw(raw, budget)).transpose()?;
                        controller = id_controller;
                    }
                    _ => (),
                }
                Ok(())
            })();
        })?;
        result?;
        let nodes = Shared::new(brokers.finish(budget)?, budget)?;
        let mut topics: Vector<Topic> = Vector::with_capacity(view.summary().topics, budget)?;
        let mut result: rtdb_types::Result<()> = Ok(());
        view.visit(|event| {
            if result.is_err() { return; }
            result = (|| {
                match event {
                    RawEvent::Topic(topic) => {
                        let value = Topic { name: topic.name.map(|raw| Text::from_raw(raw, budget)).transpose()?,
                            id: TopicId(topic.id.copied().unwrap_or([0; 16])), error: error_code(topic.error), internal: topic.internal,
                            partitions: Vector::with_capacity(topic.partitions, budget)? };
                        topics.push(value, budget)?;
                    }
                    RawEvent::Partition(partition) => {
                        let topic = topics.as_mut_slice().last_mut().expect("validated Metadata partition follows a topic");
                        let value = Shared::new(Partition {
                            key: TopicPartitionKey::new(topic.name.clone(), partition.id, budget)?, error: error_code(partition.error),
                            leader: (partition.leader >= 0).then_some(partition.leader), epoch: partition.leader_epoch.filter(|epoch| *epoch != -1),
                            replicas: array(partition.replicas.iter(), budget)?, isr: array(partition.isr.iter(), budget)?, offline: array(partition.offline.iter(), budget)?,
                        }, budget)?;
                        topic.partitions.push(value, budget)?;
                    }
                    _ => (),
                }
                Ok(())
            })();
        })?;
        result?;
        Ok(Self { nodes, topics, cluster_id, controller, reliable_epochs: view.has_reliable_leader_epochs(), top_error: error_code(view.summary().error) })
    }
}

pub(super) fn error_code(wire: i16) -> i16 { if (-1..=136).contains(&wire) { wire } else { -1 } }
pub(super) fn invalid_metadata(error: i16) -> bool { matches!(error, 3 | 5 | 6 | 9 | 13 | 56 | 72 | 74 | 80 | 83 | 84 | 100 | 103) }
