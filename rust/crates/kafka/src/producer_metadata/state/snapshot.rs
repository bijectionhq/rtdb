use rtdb_types::java_hash_map::{Map, PutOutcome};
use super::{Failure, Outcome, keys::{IdentityWitness, Ops, TopicId, TopicPartitionKey}, owned::{Budget, Lease, Shared, Text, Vector}, response::{Node, Partition, Table}};
use super::super::round_robin::PartitionChoice;

pub(super) type Names = Map<Option<Text>, (), Lease>;
pub(super) struct Sets { pub unauthorized: Names, pub invalid: Names, pub internal: Names }
impl Sets {
    pub(super) fn new() -> rtdb_types::Result<Self> {
        Ok(Self { unauthorized: Map::new(None)?, invalid: Map::new(None)?, internal: Map::new(None)? })
    }
}
pub(super) struct Input {
    pub nodes: Shared<Table<i32, Shared<Node>>>, pub cluster_id: Option<Text>, pub controller: i32,
    pub partitions: Vector<Shared<Partition>>, pub sets: Sets, pub topic_ids: Map<Option<Text>, TopicId, Lease>,
}

/// One immutable, admitted metadata generation; clones retain all map geometry, payloads and leases.
/// Reading a retained generation is independent of later producer capability failures.
#[derive(Clone)]
pub struct Snapshot(pub(super) Shared<SnapshotValue>);
pub(super) struct SnapshotValue {
    pub nodes: Shared<Table<i32, Shared<Node>>>, pub cluster_id: Option<Text>, cluster: ClusterProfile,
    pub partitions: Table<TopicPartitionKey, Shared<Partition>>,
    pub topic_ids: Table<Option<Text>, TopicId>, pub topic_names: Table<TopicId, Option<Text>>,
    pub unauthorized: Table<Option<Text>, ()>, pub invalid: Table<Option<Text>, ()>, pub internal: Table<Option<Text>, ()>,
    groups: Table<Option<Text>, usize>, choices: Vector<Vector<PartitionChoice>>,
}

pub(super) enum ClusterProfile { Response { controller: i32 }, Bootstrap { nodes: Vector<Shared<Node>> } }

impl Snapshot {
    pub(super) fn empty(budget: &mut Budget, identity: &mut impl IdentityWitness) -> Outcome<Self> {
        let nodes = Shared::new(Map::new(None)?.finish(budget)?, budget)?;
        Self::build(Input { nodes, cluster_id: None, controller: -1, partitions: Vector::new(), sets: Sets::new()?, topic_ids: Map::new(None)? }, budget, identity)
    }

    pub(super) fn build(input: Input, budget: &mut Budget, identity: &mut impl IdentityWitness) -> Outcome<Self> {
        let cluster = ClusterProfile::Response { controller: input.controller };
        Self::build_with_profile(input, cluster, budget, identity)
    }

    pub(super) fn build_with_profile(input: Input, cluster: ClusterProfile, budget: &mut Budget, identity: &mut impl IdentityWitness) -> Outcome<Self> {
        let topic_ids = input.topic_ids.finish(budget)?;
        let mut topic_names = Map::new(None)?;
        for (name, id) in topic_ids.iter() {
            if name.is_none() { return Err(Failure::NullTopicIdName); }
            if matches!(topic_names.put_if_absent(*id, name.clone(), Option::is_none, &mut Ops(identity), budget)?, PutOutcome::Present(_)) {
                return Err(Failure::DuplicateTopicId(*id));
            }
        }
        let mut partitions = Map::new(Some(input.partitions.len()))?;
        for partition in input.partitions.as_slice() {
            partitions.put(partition.key.clone(), partition.clone(), &mut Ops(identity), budget)?;
        }
        let partitions = partitions.finish(budget)?;
        let mut groups = Map::new(None)?;
        let mut choices: Vector<Vector<PartitionChoice>> = Vector::new();
        for (_, partition) in partitions.iter() {
            let name = partition.key.name();
            let index = match groups.get(name, &mut Ops(identity), budget)?.copied() {
                Some(index) => index,
                None => {
                    let index = choices.len();
                    choices.push(Vector::new(), budget)?;
                    groups.put(name.clone(), index, &mut Ops(identity), budget)?;
                    index
                }
            };
            let has_leader = match partition.leader {
                Some(id) => input.nodes.contains_key(&id, &mut Ops(identity), budget)?,
                None => false,
            };
            choices.as_mut_slice()[index].push(PartitionChoice { id: partition.key.partition(), has_leader }, budget)?;
        }
        Ok(Self(Shared::new(SnapshotValue {
            nodes: input.nodes, cluster_id: input.cluster_id, cluster, partitions,
            topic_ids, topic_names: topic_names.finish(budget)?,
            unauthorized: input.sets.unauthorized.finish(budget)?, invalid: input.sets.invalid.finish(budget)?, internal: input.sets.internal.finish(budget)?,
            groups: groups.finish(budget)?, choices,
        }, budget)?))
    }

    /// Borrows the nullable Java-decoded cluster ID.
    pub fn cluster_id(&self) -> Option<&str> { self.0.cluster_id.as_ref().map(Text::as_str) }
    /// Iterates broker endpoints in the generation's source Java HashMap encounter order.
    pub fn brokers(&self) -> impl ExactSizeIterator<Item = &Node> { self.0.nodes.iter().map(|(_, node)| &**node) }
    /// Borrows the resolved controller, including a negative ID if the response actually contained it.
    /// Bootstrap generations have no controller.
    pub fn controller(&self) -> Option<&Node> {
        let ClusterProfile::Response { controller } = &self.0.cluster else { return None; };
        self.0.nodes.iter().find(|(id, _)| *id == controller).map(|(_, node)| &**node)
    }
    /// Reports the source Cluster bootstrap flag for this generation.
    pub fn is_bootstrap_configured(&self) -> bool { matches!(&self.0.cluster, ClusterProfile::Bootstrap { .. }) }
    /// Borrows the qualified shuffled bootstrap Cluster.nodes list, returning `None` for response generations.
    pub fn bootstrap_cluster_nodes(&self) -> Option<impl ExactSizeIterator<Item = &Node>> {
        match &self.0.cluster {
            ClusterProfile::Bootstrap { nodes } => Some(nodes.as_slice().iter().map(|node| &**node)),
            ClusterProfile::Response { .. } => None,
        }
    }
    /// Borrows one partition by Java-decoded topic value and signed partition ID without allocating.
    pub fn partition(&self, topic: &str, id: i32) -> Option<&Partition> {
        self.0.partitions.iter().find(|(key, _)| key.topic() == Some(topic) && key.partition() == id).map(|(_, value)| &**value)
    }
    /// Borrows the topic's complete partition choices in Java Cluster list order; absent topics return an empty list.
    pub fn partitions(&self, topic: &str) -> &[PartitionChoice] {
        self.0.groups.iter().find(|(name, _)| name.as_ref().is_some_and(|name| name.as_str() == topic))
            .map_or(&[], |(_, index)| self.0.choices.as_slice()[*index].as_slice())
    }
    /// Borrows partition values in the snapshot's actual Java HashMap encounter order.
    pub fn partition_metadata(&self) -> impl ExactSizeIterator<Item = &Partition> { self.0.partitions.iter().map(|(_, value)| &**value) }
    /// Returns a retained nonzero topic UUID by decoded name.
    pub fn topic_id(&self, topic: &str) -> Option<TopicId> {
        self.0.topic_ids.iter().find(|(name, _)| name.as_ref().is_some_and(|name| name.as_str() == topic)).map(|(_, id)| *id)
    }
    /// Borrows the decoded name associated with a retained UUID.
    pub fn topic_name(&self, id: TopicId) -> Option<&str> {
        self.0.topic_names.iter().find(|(key, _)| **key == id).and_then(|(_, name)| name.as_ref().map(Text::as_str))
    }
    /// Iterates internal topic names in Java HashSet encounter order.
    pub fn internal_topics(&self) -> impl ExactSizeIterator<Item = Option<&str>> { self.0.internal.iter().map(|(name, _)| name.as_ref().map(Text::as_str)) }
    /// Iterates unauthorized topic names in Java HashSet encounter order.
    pub fn unauthorized_topics(&self) -> impl ExactSizeIterator<Item = Option<&str>> { self.0.unauthorized.iter().map(|(name, _)| name.as_ref().map(Text::as_str)) }
    /// Iterates invalid topic names in Java HashSet encounter order.
    pub fn invalid_topics(&self) -> impl ExactSizeIterator<Item = Option<&str>> { self.0.invalid.iter().map(|(name, _)| name.as_ref().map(Text::as_str)) }
}
