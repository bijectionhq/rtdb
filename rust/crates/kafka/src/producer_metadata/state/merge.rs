use rtdb_types::java_hash_map::Map;
use super::{Outcome, ProducerMetadata, keys::{IdentityWitness, Ops}, owned::{Budget, Text, Vector, overflow}, response::Table, snapshot::{Input, Names, Sets, Snapshot}};

pub(super) fn copy_set(source: &Table<Option<Text>, ()>, budget: &mut Budget, identity: &mut impl IdentityWitness) -> rtdb_types::Result<Names> {
    let capacity = (source.len().max(12).checked_mul(4).and_then(|value| value.checked_add(2)).ok_or_else(overflow)? / 3).min(i32::MAX as usize);
    let mut result = Map::new(Some(capacity))?;
    for (name, _) in source.iter() { result.put(name.clone(), (), &mut Ops(identity), budget)?; }
    Ok(result)
}

impl ProducerMetadata {
    pub(super) fn prune_epochs(&mut self, now_ms: i64, identity: &mut impl IdentityWitness) -> Outcome<()> {
        let active = &mut self.active;
        let new_topics = &self.new_topics;
        self.epochs.remove_if(&mut Ops(identity), &mut self.budget, |key, _, ops, budget| {
            let topic = key.name().as_ref().expect("retained epoch has passed non-null producer topic retention");
            let Some(expiry) = active.expiry(topic) else { return Ok(true); };
            if new_topics.contains_key(key.name(), ops, budget)? || expiry > now_ms { return Ok(false); }
            active.remove(topic);
            Ok(true)
        })?;
        Ok(())
    }

    fn should_retain(&mut self, name: &Option<Text>, seen: &Names, now_ms: i64, identity: &mut impl IdentityWitness) -> Outcome<bool> {
        if seen.contains_key(name, &mut Ops(identity), &mut self.budget)? { return Ok(false); }
        self.retain_topic(name, now_ms, identity)
    }

    fn fill_set(&mut self, base: Names, old: &Table<Option<Text>, ()>, seen: &Names, now_ms: i64, identity: &mut impl IdentityWitness) -> Outcome<Names> {
        let base = base.finish(&mut self.budget)?;
        let mut result = copy_set(&base, &mut self.budget, identity)?;
        for (name, _) in old.iter() {
            if self.should_retain(name, seen, now_ms, identity)? { result.put(name.clone(), (), &mut Ops(identity), &mut self.budget)?; }
        }
        Ok(result)
    }

    pub(super) fn merge(&mut self, input: Input, seen: &Names, now_ms: i64, identity: &mut impl IdentityWitness) -> Outcome<Input> {
        let old: Snapshot = self.snapshot.clone();
        let mut partitions = Map::new(Some(input.partitions.len()))?;
        let mut topic_ids = Map::new(None)?;
        for (name, id) in old.0.topic_ids.iter() {
            if self.should_retain(name, seen, now_ms, identity)? { topic_ids.put(name.clone(), *id, &mut Ops(identity), &mut self.budget)?; }
        }
        for partition in input.partitions.as_slice() {
            partitions.put(partition.key.clone(), partition.clone(), &mut Ops(identity), &mut self.budget)?;
            let name = partition.key.name();
            if let Some(id) = input.topic_ids.get(name, &mut Ops(identity), &mut self.budget)? {
                topic_ids.put(name.clone(), *id, &mut Ops(identity), &mut self.budget)?;
            } else { topic_ids.remove(name, &mut Ops(identity), &mut self.budget)?; }
        }
        for (key, partition) in old.0.partitions.iter() {
            if self.should_retain(key.name(), seen, now_ms, identity)? {
                partitions.put_if_absent(key.clone(), partition.clone(), |_| false, &mut Ops(identity), &mut self.budget)?;
            }
        }
        let sets = Sets {
            unauthorized: self.fill_set(input.sets.unauthorized, &old.0.unauthorized, seen, now_ms, identity)?,
            invalid: self.fill_set(input.sets.invalid, &old.0.invalid, seen, now_ms, identity)?,
            internal: self.fill_set(input.sets.internal, &old.0.internal, seen, now_ms, identity)?,
        };
        let mut replay = Vector::with_capacity(partitions.len(), &mut self.budget)?;
        for (_, partition) in partitions.try_iter()? { replay.push(partition.clone(), &mut self.budget)?; }
        Ok(Input { nodes: input.nodes, cluster_id: input.cluster_id, controller: input.controller, partitions: replay, sets, topic_ids })
    }
}
