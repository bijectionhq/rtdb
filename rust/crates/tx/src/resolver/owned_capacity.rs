use std::{collections::BTreeSet, num::NonZeroUsize};

use rtdb_types::{EntityId, OwnedRelation, OwnedSchema, WorldShadow};

use super::{
    owned_unique::{MAX_KEY_BYTES, unique_id},
    replay_constraints::{ConstraintMember, ConstraintMembers},
    *,
};

use crate::hosted_control::ReplayConstraintKind;

const RULE: &str = "!rule";
const MAX_RULES: usize = 16;
const MAX_PREDICATES: usize = 4;
const MAX_PREDICATE_BYTES: usize = 2052;

#[derive(Default)]
pub(super) struct OwnedCapacityChanges {
    relations: BTreeMap<OwnedRelation, Option<TouchedRule>>,
    reservations: Vec<Arc<MemoryReservation>>,
}

struct TouchedRule {
    policy: CapacityPolicy,
    declarations: Vec<RuleDeclaration>,
    ids: BTreeSet<EntityId>,
    writes: i64,
}

struct RuleDeclaration {
    column: String,
    when: Vec<u8>,
    previous_cap: Option<i64>,
}

impl RuleDeclaration {
    fn matches(&self, rule: &CapacityRule) -> bool {
        rule.identity() == (self.column.as_str(), self.when.as_slice())
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(super) struct CapacityRule {
    column: String,
    prefix_bytes: Option<NonZeroUsize>,
    pub(super) cap: i64,
    pub(super) when: Vec<u8>,
}

impl CapacityRule {
    fn new(column: String, prefix_bytes: i64, cap: i64, when: Vec<u8>) -> Result<Self> {
        let prefix_bytes = usize::try_from(prefix_bytes)
            .ok()
            .filter(|n| *n <= MAX_KEY_BYTES)
            .ok_or_else(invalid_rule)?;
        if !valid_column(&column) || cap <= 0 {
            return Err(invalid_rule());
        }
        predicates(&when)?;
        Ok(Self {
            column,
            prefix_bytes: NonZeroUsize::new(prefix_bytes),
            cap,
            when,
        })
    }

    fn value(&self) -> Value {
        Value::Struct(StructFields::from([
            ("column".into(), Value::Utf8(self.column.clone())),
            (
                "prefix_bytes".into(),
                Value::I64(self.prefix_bytes.map_or(0, NonZeroUsize::get) as i64),
            ),
            ("cap".into(), Value::I64(self.cap)),
            ("when".into(), Value::Binary(self.when.clone())),
        ]))
    }

    fn bucket<'a>(&self, key: &'a [u8]) -> Option<&'a [u8]> {
        if key.len() > MAX_KEY_BYTES {
            return None;
        }
        key.get(..self.prefix_bytes.map_or(key.len(), NonZeroUsize::get))
    }

    fn bucket_id(&self, key: &[u8]) -> EntityId {
        let mut bytes = Vec::with_capacity(3 + self.column.len() + self.when.len() + key.len());
        bytes.push(self.column.len() as u8);
        bytes.extend_from_slice(self.column.as_bytes());
        bytes.extend_from_slice(&(self.when.len() as u16).to_be_bytes());
        bytes.extend_from_slice(&self.when);
        bytes.extend_from_slice(key);
        unique_id(&bytes)
    }

    fn identity(&self) -> (&str, &[u8]) {
        (&self.column, &self.when)
    }

    pub(super) fn admits(&self, schema: &rtdb_types::OwnedSchema) -> Result<bool> {
        Ok(
            schema.path_column_type(&self.column) == Some(ScalarType::Binary)
                && predicates(&self.when)?
                    .into_iter()
                    .all(|(column, _)| schema.path_column_type(column) == Some(ScalarType::Binary)),
        )
    }

    pub(super) fn columns<'a>(&'a self, qualifiers: &[(&'a str, &[u8])]) -> Vec<&'a str> {
        std::iter::once(self.column.as_str())
            .chain(
                qualifiers
                    .iter()
                    .map(|(name, _)| *name)
                    .filter(|&name| name != self.column.as_str()),
            )
            .collect()
    }

    pub(super) fn key(
        &self, qualifiers: &[(&str, &[u8])], mut value: impl FnMut(&str) -> Result<Value>,
    ) -> Result<Option<Vec<u8>>> {
        for &(column, expected) in qualifiers {
            if !matches_predicate(&value(column)?, expected)? {
                return Ok(None);
            }
        }
        let Value::Binary(mut key) = value(&self.column)? else {
            return Err(invalid_key());
        };
        let length = self.bucket(&key).ok_or_else(invalid_key)?.len();
        key.truncate(length);
        Ok(Some(key))
    }

    fn decode(value: &Value) -> Result<Self> {
        let Value::Struct(rule) = value else {
            return Err(uncovered());
        };
        let (
            Some(Value::Utf8(column)),
            Some(Value::I64(prefix_bytes)),
            Some(Value::I64(cap)),
            Some(Value::Binary(when)),
        ) = (
            rule.get("column"),
            rule.get("prefix_bytes"),
            rule.get("cap"),
            rule.get("when"),
        )
        else {
            return Err(uncovered());
        };
        if rule.len() != 4 {
            return Err(uncovered());
        }
        Self::new(column.clone(), *prefix_bytes, *cap, when.clone()).map_err(|_| uncovered())
    }
}

fn valid_column(column: &str) -> bool {
    !column.is_empty() && column.len() <= 96 && !column.starts_with('_')
}

pub(super) fn predicates(mut encoded: &[u8]) -> Result<Vec<(&str, &[u8])>> {
    let mut result = Vec::new();
    while let Some((&name_len, body)) = encoded.split_first() {
        let (name, body) = body
            .split_at_checked(usize::from(name_len))
            .ok_or_else(invalid_rule)?;
        let name = std::str::from_utf8(name).map_err(|_| invalid_rule())?;
        let (value_len, body) = body.split_first_chunk::<2>().ok_or_else(invalid_rule)?;
        let (value, tail) = body
            .split_at_checked(usize::from(u16::from_be_bytes(*value_len)))
            .ok_or_else(invalid_rule)?;
        if result.len() >= MAX_PREDICATES
            || !valid_column(name)
            || !(1..=MAX_PREDICATE_BYTES).contains(&value.len())
            || result.last().is_some_and(|&(prior, _)| prior >= name)
        {
            return Err(invalid_rule());
        }
        result.push((name, value));
        encoded = tail;
    }
    Ok(result)
}

fn matches_predicate(value: &Value, expected: &[u8]) -> Result<bool> {
    match value {
        Value::Binary(bytes) => Ok(bytes.as_slice() == expected),
        Value::Null | Value::Undefined => Ok(false),
        _ => Err(invalid_key()),
    }
}

fn shadow_charges(row: &StructFields<Value>, qualifiers: &[(&str, &[u8])]) -> Result<bool> {
    if WorldShadow::is_tombstone(row) {
        return Ok(false);
    }
    for &(column, expected) in qualifiers {
        if !matches_predicate(row.get(column).unwrap_or(&Value::Undefined), expected)? {
            return Ok(false);
        }
    }
    Ok(true)
}

#[derive(Clone, Default)]
pub(super) struct CapacityPolicy {
    pub(super) rules: Vec<CapacityRule>,
    pub(super) through_events: i64,
}

impl CapacityPolicy {
    fn document(&self) -> StructFields<Value> {
        StructFields::from([
            ("_id".into(), Value::Utf8(RULE.into())),
            (
                "policy".into(),
                Value::Struct(StructFields::from([
                    (
                        "rules".into(),
                        Value::List(self.rules.iter().map(CapacityRule::value).collect()),
                    ),
                    ("through_events".into(), Value::I64(self.through_events)),
                ])),
            ),
        ])
    }

    fn decode(value: Value) -> Result<Self> {
        let Value::Struct(policy) = value else {
            return Err(uncovered());
        };
        let (Some(Value::List(rules)), Some(Value::I64(through_events))) =
            (policy.get("rules"), policy.get("through_events"))
        else {
            return Err(uncovered());
        };
        if policy.len() != 2 || !(1..=MAX_RULES).contains(&rules.len()) || *through_events < 0 {
            return Err(uncovered());
        }
        let rules = rules
            .iter()
            .map(CapacityRule::decode)
            .collect::<Result<Vec<_>>>()?;
        if rules
            .windows(2)
            .any(|pair| pair[0].identity() >= pair[1].identity())
        {
            return Err(uncovered());
        }
        Ok(Self {
            rules,
            through_events: *through_events,
        })
    }

    pub(super) fn retained_bytes(&self) -> usize {
        1024 + self
            .rules
            .iter()
            .map(|rule| 256 + rule.when.capacity())
            .sum::<usize>()
    }
}

#[derive(Default)]
struct Delta {
    releases: i64,
    claims: i64,
}

struct CapacityCondition<'a> {
    relation: &'a OwnedRelation,
    column: &'a str,
    prefix: &'a [u8],
    at_most: i64,
}

fn capacity_conditions(operations: &[Operation]) -> impl Iterator<Item = CapacityCondition<'_>> {
    operations.iter().filter_map(|operation| match operation {
        Operation::Public(TxOp::OwnedCheckCapacity {
            relation,
            column,
            prefix,
            at_most,
        }) => Some(CapacityCondition {
            relation,
            column,
            prefix,
            at_most: *at_most,
        }),
        _ => None,
    })
}

impl OpenTransaction<'_> {
    pub(super) async fn require_noncharging_world_write(
        &self, owner: &str, root: &OwnedRelation, schema: &OwnedSchema, rows: &[StructFields<Value>],
    ) -> Result<()> {
        // A shadow outside every current bucket can only release occupancy,
        // regardless of whether it displaces an archived or local fact.
        if rows.iter().all(WorldShadow::is_tombstone) {
            return Ok(());
        }
        let base = self.buffer.base_snapshot();
        let stored;
        let policy = match self.capacity_changes.relations.get(root).and_then(Option::as_ref) {
            Some(touched) => &touched.policy,
            None => {
                stored = self.capacity_policy(base.clone(), owner, root).await?;
                &stored
            }
        };
        if !policy.rules.is_empty() && base.relation_event_frontier(&root.table(owner))? != policy.through_events {
            return Err(uncovered());
        }
        for rule in &policy.rules {
            if !rule.admits(schema)? {
                return Err(uncovered());
            }
            let qualifiers = predicates(&rule.when)?;
            for row in rows {
                self.context.check_cancelled()?;
                if shadow_charges(row, &qualifiers)? {
                    return Err(Anomaly::incorrect(
                        "rtdb.owned/world-capacity-uncovered",
                        "The world charge requires inherited capacity accounting",
                    ));
                }
            }
        }
        Ok(())
    }

    pub(super) async fn hosted_capacity_writer(&self) -> Result<Timestamp> {
        let unavailable = || Anomaly::new(
            AnomalyCategory::Forbidden,
            "rtdb.hosted/control-scope",
            "Hosted capacity declarations require the installed placed writer",
        );
        if self.context.owned_witness_scope.is_none() || !self.options.user.as_deref()
            .is_some_and(|user| self.context.authorization.is_hosted_control_owner(user))
        {
            return Err(unavailable());
        }
        if self.options.system_time.is_some() && !self.options.user_metadata.as_ref()
            .is_some_and(UserMetadata::requires_ordered_system_time)
        {
            return Err(Anomaly::incorrect(
                "rtdb.owned/ordered-system-time",
                "Hosted capacity publication requires the ordered system clock",
            ));
        }
        let context = self.context.query_context(
            self.buffer.base_snapshot(), self.tx_key().system_time, &self.options.default_tz, self.trace_parent,
        )?;
        let point = rtdb_query::read_hosted_control_point(
            context, TableRef::new("public", "bj_installation"), &EntityId::String("authority".into()), false,
        ).await?;
        point.row.map(|(_, at)| at).ok_or_else(unavailable)
    }

    pub(super) async fn stage_hosted_capacity_declarations(
        &mut self, mut cursor: QueryCursor, writer: Timestamp,
    ) -> Result<()> {
        let mut remaining = MAX_RULES;
        let ordered = self.tx_key().system_time;
        while let Some(batch) = cursor.next_batch().await? {
            let batch = batch.batch();
            remaining = remaining.checked_sub(batch.num_rows()).ok_or_else(invalid_rule)?;
            let documents = super::mutation_column(batch, "doc")?;
            let from = super::mutation_column(batch, "_valid_from")?;
            let to = super::mutation_column(batch, "_valid_to")?;
            for row in 0..batch.num_rows() {
                self.context.check_cancelled()?;
                let Value::Struct(document) = documents.read(row)? else { return Err(invalid_rule()); };
                let (relation, column, cap, when, expected) = hosted_capacity_declaration(&document)?;
                if from.time(row, ordered)? != ordered || to.time(row, Timestamp::MAX)? != Timestamp::MAX {
                    return Err(invalid_rule());
                }
                if expected != writer {
                    return Err(Anomaly::new(
                        AnomalyCategory::Conflict,
                        "rtdb.hosted/stale-control",
                        "The observed installation writer changed before capacity publication",
                    ));
                }
                self.declare_owned_capacity(&relation, column, 0, cap, when).await?;
            }
        }
        if remaining == MAX_RULES {
            return Err(invalid_rule());
        }
        Ok(())
    }

    pub(super) async fn check_capacity_source(&self, operations: &[Operation]) -> Result<()> {
        let mut conditions = capacity_conditions(operations).peekable();
        if conditions.peek().is_none() {
            return Ok(());
        }
        let has_deadline = operations
            .iter()
            .any(|op| matches!(op, Operation::Public(TxOp::OwnedDeadline { .. })));
        let base = self.buffer.base_snapshot();
        for CapacityCondition {
            relation,
            column,
            prefix,
            at_most,
        } in conditions
        {
            if at_most < 0
                || relation.world().is_some()
                || relation.is_world_catalog()
                || !has_deadline
            {
                return Err(Anomaly::incorrect(
                    "rtdb.owned/count-bound",
                    "Expected a nonnegative count bound on one owned root prefix with a deadline",
                ));
            }
            self.declared_prefix_count(base.clone(), relation, column, prefix)
                .await?;
        }
        Ok(())
    }

    pub(super) async fn check_final_constraints(&self, operations: &[Operation]) -> Result<()> {
        if let Some(archive) = self.context.world_constraints.as_deref() {
            self.check_world_constraints(operations, archive).await?;
        }
        let mut conditions = capacity_conditions(operations).peekable();
        if conditions.peek().is_none() {
            return Ok(());
        }
        let snapshot = self.snapshot()?;
        for CapacityCondition {
            relation,
            column,
            prefix,
            at_most,
        } in conditions
        {
            let count = self
                .declared_prefix_count(snapshot.clone(), relation, column, prefix)
                .await?;
            if count > at_most {
                return Err(Anomaly::new(
                    AnomalyCategory::Conflict,
                    "rtdb.owned/count-bound-exceeded",
                    "The final owned prefix count exceeds its transaction bound",
                ));
            }
        }
        Ok(())
    }

    async fn declared_prefix_count(
        &self,
        snapshot: Snapshot,
        relation: &OwnedRelation,
        column: &str,
        prefix: &[u8],
    ) -> Result<i64> {
        let owner = self
            .options
            .user
            .as_deref()
            .filter(|owner| !owner.is_empty())
            .ok_or_else(no_owned_principal)?;
        if !snapshot.contains_table(&relation.capacity_table(owner)) {
            return Err(Anomaly::incorrect(
                "rtdb.owned/count-bound",
                "The count bound requires a previously declared capacity rule",
            ));
        }
        let policy = self
            .capacity_policy(snapshot.clone(), owner, relation)
            .await
            .map_err(count_uncovered)?;
        let rule = policy.rules.iter().find(|rule| {
            rule.column == column && rule.when.is_empty() && rule.bucket(prefix) == Some(prefix)
        });
        let Some(rule) = rule else {
            return Err(Anomaly::incorrect(
                "rtdb.owned/count-bound",
                "The count interval must identify one declared Binary partition",
            ));
        };
        if snapshot.relation_event_frontier(&relation.table(owner))? != policy.through_events {
            return Err(count_uncovered(uncovered()));
        }
        let _key = self.context.memory.reserve(prefix.len() * 4 + 512)?;
        match self
            .capacity_cell(snapshot, owner, relation, &rule.bucket_id(prefix), "count")
            .await
            .map_err(count_uncovered)?
        {
            None => Ok(0),
            Some(Value::I64(count)) if (1..=rule.cap).contains(&count) => Ok(count),
            Some(_) => Err(count_uncovered(uncovered())),
        }
    }

    pub(super) async fn declare_owned_capacity(
        &mut self,
        relation: &OwnedRelation,
        column: &str,
        prefix_bytes: i64,
        cap: i64,
        when: &[u8],
    ) -> Result<()> {
        OwnedRelation::new(relation.name())?;
        if !relation.is_root_data() {
            return Err(invalid_rule());
        }
        let owner = self
            .options
            .user
            .as_deref()
            .filter(|owner| !owner.is_empty())
            .ok_or_else(no_owned_principal)?;
        let rule = CapacityRule::new(column.into(), prefix_bytes, cap, when.to_vec())?;
        let target = relation.table(owner);
        let base = self.buffer.base_snapshot();
        let base_frontier = base.relation_event_frontier(&target)?;
        let snapshot = self.snapshot()?;
        let frontier = snapshot.relation_event_frontier(&target)?;
        let mut policy = match self
            .capacity_changes
            .relations
            .get(relation)
            .and_then(Option::as_ref)
        {
            Some(touched) => touched.policy.clone(),
            None => self.capacity_policy(base, owner, relation).await?,
        };
        if !policy.rules.is_empty() && policy.through_events != base_frontier {
            return Err(uncovered());
        }
        let (index, previous_cap) = match policy
            .rules
            .binary_search_by(|prior| prior.identity().cmp(&rule.identity()))
        {
            Ok(index) if policy.rules[index] == rule => {
                return Ok(());
            }
            Ok(index) if policy.rules[index].prefix_bytes == rule.prefix_bytes => {
                (index, Some(policy.rules[index].cap))
            }
            Ok(_) => {
                return Err(Anomaly::new(
                    AnomalyCategory::Conflict,
                    "rtdb.owned/capacity-exists",
                    "A declared capacity projection retains its prefix width",
                ));
            }
            Err(index) => (index, None),
        };
        let schema = self.read_owned_schema(owner, relation).await?;
        if let Some((schema, _)) = &schema
            && (schema.lifetime_micros().is_some() || !rule.admits(schema)?)
        {
            return Err(invalid_rule());
        }
        // Existing caps already require acceptance-valid mutations. Otherwise
        // the immutable relation declaration must rule out scheduled changes.
        if policy.rules.is_empty()
            && (frontier != 0 || !snapshot.trie_entries(&target).is_empty())
            && !schema
                .as_ref()
                .is_some_and(|(schema, _)| schema.has_acceptance_validity())
        {
            return Err(Anomaly::incorrect(
                "rtdb.owned/capacity-bootstrap",
                "A populated relation needs acceptance validity to declare its first capacity rule",
            ));
        }
        if previous_cap.is_none() && policy.rules.len() >= MAX_RULES {
            return Err(invalid_rule());
        }
        let declaration = RuleDeclaration {
            column: rule.column.clone(),
            when: rule.when.clone(),
            previous_cap,
        };
        if previous_cap.is_some() {
            policy.rules[index] = rule;
        } else {
            policy.rules.insert(index, rule);
        }
        policy.through_events = base_frontier;
        let bytes = policy.retained_bytes()
            + declaration.column.capacity() + declaration.when.capacity() + 128;
        self.capacity_changes
            .reservations
            .push(self.context.memory.reserve(bytes)?);
        let writes = frontier
            .checked_sub(base_frontier)
            .filter(|writes| *writes >= 0)
            .ok_or_else(uncovered)?;
        let slot = self
            .capacity_changes
            .relations
            .entry(relation.clone())
            .or_default();
        if let Some(touched) = slot {
            if touched.writes != writes {
                return Err(uncovered());
            }
            if touched.declarations.iter().any(|prior| prior.matches(&policy.rules[index])) {
                return Err(Anomaly::incorrect(
                    "rtdb.owned/capacity-repeat",
                    "An owned capacity projection can change once per transaction",
                ));
            }
            touched.policy = policy;
            touched.declarations.push(declaration);
        } else {
            *slot = Some(TouchedRule {
                policy,
                declarations: vec![declaration],
                ids: BTreeSet::new(),
                writes,
            });
        }
        Ok(())
    }

    pub(super) fn has_staged_capacity(&self, relation: &OwnedRelation) -> bool {
        self.capacity_changes
            .relations
            .get(relation)
            .is_some_and(Option::is_some)
    }

    pub(super) async fn track_capacity_mutation(
        &mut self,
        owner: &str,
        relation: &OwnedRelation,
        id: &EntityId,
        valid: Interval,
    ) -> Result<()> {
        if relation.world().is_some() {
            return Ok(());
        }
        if !self.capacity_changes.relations.contains_key(relation) {
            let snapshot = self.buffer.base_snapshot();
            let policy = self.capacity_policy(snapshot, owner, relation).await?;
            self.capacity_changes
                .reservations
                .push(self.context.memory.reserve(policy.retained_bytes())?);
            let rule = (!policy.rules.is_empty()).then_some(TouchedRule {
                policy,
                declarations: Vec::new(),
                ids: BTreeSet::new(),
                writes: 0,
            });
            self.capacity_changes
                .relations
                .insert(relation.clone(), rule);
        }
        let valid_at = self.owned_constraint_present();
        let Some(touched) = self
            .capacity_changes
            .relations
            .get_mut(relation)
            .and_then(Option::as_mut)
        else {
            return Ok(());
        };
        if valid.lower() != valid_at || valid.upper() != Timestamp::MAX {
            return Err(Anomaly::incorrect(
                "rtdb.owned/capacity-valid-time",
                "Capacity writes require validity from ordered present without an end",
            ));
        }
        touched.writes = touched.writes.checked_add(1).ok_or_else(overflow)?;
        if !touched.ids.contains(id) {
            let bytes = id
                .as_value()
                .retained_bytes()?
                .checked_add(1024)
                .ok_or_else(overflow)?;
            self.capacity_changes
                .reservations
                .push(self.context.memory.reserve(bytes)?);
            touched.ids.insert(id.clone());
        }
        Ok(())
    }

    pub(super) async fn finish_owned_capacity(&mut self) -> Result<()> {
        let changes = std::mem::take(&mut self.capacity_changes);
        if changes
            .relations
            .values()
            .flatten()
            .all(|touched| touched.writes == 0 && touched.declarations.is_empty())
        {
            return Ok(());
        }
        let owner = self
            .options
            .user
            .as_deref()
            .filter(|owner| !owner.is_empty())
            .ok_or_else(no_owned_principal)?
            .to_owned();
        let base = self.buffer.base_snapshot();
        let next = self.snapshot()?;
        for (relation, touched) in changes.relations {
            let Some(TouchedRule {
                mut policy,
                declarations,
                ids,
                writes,
            }) = touched
            else {
                continue;
            };
            if writes == 0 && declarations.is_empty() {
                continue;
            }
            let target = relation.table(&owner);
            let through = policy
                .through_events
                .checked_add(writes)
                .ok_or_else(overflow)?;
            if base.relation_event_frontier(&target)? != policy.through_events
                || next.relation_event_frontier(&target)? != through
            {
                return Err(uncovered());
            }
            let _locators = self
                .context
                .memory
                .reserve(64 + ids.len() * policy.rules.len() * 128)?;
            let mut locators = BTreeSet::from([EntityId::String(RULE.into()).iid()]);
            let mut proof = self.hosted_replay.as_ref().map(|_| ConstraintMembers::default());
            for rule in &policy.rules {
                let previous_cap = match declarations.iter().find(|declaration| declaration.matches(rule)) {
                    Some(RuleDeclaration { previous_cap: None, .. }) => {
                        let counts = self.scan_capacity_counts(&owner, &relation, rule, &next).await?;
                        let storage = relation.capacity_table(&owner);
                        for (id, (count, _charge)) in counts {
                            if !locators.insert(id.iid()) {
                                return Err(uncovered());
                            }
                            if let Some(proof) = proof.as_mut() {
                                proof.insert(&id, ConstraintMember::Count(count), &self.context.memory)?;
                            }
                            self.buffer.put(storage.clone(), count_document(&id, count), Interval::ALL)?;
                        }
                        continue;
                    }
                    Some(RuleDeclaration { previous_cap: Some(cap), .. }) => {
                        if rule.cap < *cap {
                            self.scan_capacity_counts(&owner, &relation, rule, &next).await?;
                        }
                        *cap
                    }
                    None => rule.cap,
                };
                if writes != 0 {
                    self.apply_capacity_rule(
                        &owner,
                        &relation,
                        rule,
                        previous_cap,
                        &ids,
                        &base,
                        &next,
                        &mut locators,
                        proof.as_mut(),
                    )
                    .await?;
                }
            }
            if let (Some(replay), Some(proof)) = (&mut self.hosted_replay, proof) {
                replay.match_constraint(
                    &relation,
                    ReplayConstraintKind::Capacity,
                    writes,
                    through,
                    proof.finish(),
                    declarations.len(),
                )?;
            }
            policy.through_events = through;
            self.buffer.put(
                relation.capacity_table(&owner),
                policy.document(),
                Interval::ALL,
            )?;
        }
        Ok(())
    }

    async fn scan_capacity_counts(
        &self,
        owner: &str,
        relation: &OwnedRelation,
        rule: &CapacityRule,
        snapshot: &Snapshot,
    ) -> Result<BTreeMap<EntityId, (i64, Arc<MemoryReservation>)>> {
        let qualifiers = predicates(&rule.when)?;
        let columns = rule.columns(&qualifiers);
        let context = self
            .context
            .query_context(
                snapshot.clone(),
                self.tx_key().system_time,
                &self.options.default_tz,
                self.trace_parent,
            )?
            .with_accelerators(Arc::from([]));
        let Some(mut rows) = rtdb_query::open_owned_root_projection(
            context,
            owner,
            relation,
            self.owned_constraint_present(),
            &columns,
        )
        .await?
        else {
            return Ok(BTreeMap::new());
        };
        let mut counts = BTreeMap::new();
        while let Some(rows) = rows.next_batch().await? {
            let rows = rows.batch();
            for row in 0..rows.num_rows() {
                self.context.check_cancelled()?;
                let Some(prefix) = self.capacity_key(rows, row, rule, &qualifiers)? else {
                    continue;
                };
                let id = rule.bucket_id(&prefix);
                let count = match counts.entry(id) {
                    std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        let charge = self.context.memory.reserve(
                            (prefix.len() + rule.column.len() + rule.when.len()) * 4 + 512,
                        )?;
                        entry.insert((0i64, charge))
                    }
                };
                if count.0 == rule.cap {
                    return Err(Anomaly::new(
                        AnomalyCategory::Conflict,
                        "rtdb.owned/capacity-already-exceeded",
                        "An owned prefix already exceeds its declared capacity",
                    ));
                }
                count.0 += 1;
            }
        }
        Ok(counts)
    }

    async fn apply_capacity_rule(
        &mut self,
        owner: &str,
        relation: &OwnedRelation,
        rule: &CapacityRule,
        previous_cap: i64,
        ids: &BTreeSet<EntityId>,
        base: &Snapshot,
        next: &Snapshot,
        locators: &mut BTreeSet<rtdb_types::Iid>,
        mut proof: Option<&mut ConstraintMembers>,
    ) -> Result<()> {
        let qualifiers = predicates(&rule.when)?;
        let columns = rule.columns(&qualifiers);
        let valid = Interval::at(self.owned_constraint_present())?;
        let mut deltas = BTreeMap::<EntityId, Delta>::new();
        let mut allocations = Vec::new();
        for id in ids {
            for (snapshot, final_state) in [(base, false), (next, true)] {
                let context = self.context.query_context(
                    snapshot.clone(),
                    self.tx_key().system_time,
                    &self.options.default_tz,
                    self.trace_parent,
                )?;
                let cursor = rtdb_query::open_owned_point(
                    context,
                    owner,
                    relation,
                    id,
                    Some(valid),
                    Some(&columns),
                )
                .await?;
                let Some(row) = self.read_occupancy_row(cursor, id, uncovered).await? else {
                    continue;
                };
                let Some(prefix) = self.capacity_key(row.batch(), 0, rule, &qualifiers)? else {
                    continue;
                };
                allocations.push(
                    self.context
                        .memory
                        .reserve((prefix.len() + rule.column.len() + rule.when.len()) * 4 + 512)?,
                );
                let delta = deltas.entry(rule.bucket_id(&prefix)).or_default();
                let count = if final_state {
                    &mut delta.claims
                } else {
                    &mut delta.releases
                };
                *count = count.checked_add(1).ok_or_else(overflow)?;
            }
        }
        let storage = relation.capacity_table(owner);
        for (id, delta) in deltas {
            self.context.check_cancelled()?;
            if !locators.insert(id.iid()) {
                return Err(uncovered());
            }
            let count = match self
                .capacity_cell(base.clone(), owner, relation, &id, "count")
                .await?
            {
                None => 0,
                Some(Value::I64(count)) if (1..=previous_cap).contains(&count) => count,
                Some(_) => return Err(uncovered()),
            };
            if delta.releases > count {
                return Err(uncovered());
            }
            let final_count = (count - delta.releases)
                .checked_add(delta.claims)
                .ok_or_else(overflow)?;
            if final_count > rule.cap {
                return Err(Anomaly::new(
                    AnomalyCategory::Conflict,
                    "rtdb.owned/capacity-exceeded",
                    "An owned prefix exceeds its capacity",
                ));
            }
            if final_count == count {
                continue;
            }
            if let Some(proof) = proof.as_deref_mut() {
                let member = if final_count == 0 {
                    ConstraintMember::Removed
                } else {
                    ConstraintMember::Count(final_count)
                };
                proof.insert(&id, member, &self.context.memory)?;
            }
            if final_count == 0 {
                self.buffer.delete(storage.clone(), &id, Interval::ALL)?;
            } else {
                self.buffer.put(
                    storage.clone(),
                    count_document(&id, final_count),
                    Interval::ALL,
                )?;
            }
        }
        Ok(())
    }

    fn capacity_key(
        &self,
        rows: &RecordBatch,
        row: usize,
        rule: &CapacityRule,
        qualifiers: &[(&str, &[u8])],
    ) -> Result<Option<Vec<u8>>> {
        rule.key(qualifiers, |column| self.read_occupancy_value_at(rows, row, column, uncovered))
    }

    pub(super) async fn check_capacity_schema(
        &self,
        snapshot: Snapshot,
        owner: &str,
        relation: &OwnedRelation,
        schema: &rtdb_types::OwnedSchema,
    ) -> Result<()> {
        let stored;
        let policy = match self
            .capacity_changes
            .relations
            .get(relation)
            .and_then(Option::as_ref)
        {
            Some(touched) => &touched.policy,
            None => {
                stored = self.capacity_policy(snapshot, owner, relation).await?;
                &stored
            }
        };
        for rule in &policy.rules {
            if schema.lifetime_micros().is_some() || !rule.admits(schema)? {
                return Err(invalid_rule());
            }
        }
        Ok(())
    }

    pub(super) async fn capacity_policy(
        &self,
        snapshot: Snapshot,
        owner: &str,
        relation: &OwnedRelation,
    ) -> Result<CapacityPolicy> {
        if !snapshot.contains_table(&relation.capacity_table(owner)) {
            return Ok(CapacityPolicy::default());
        }
        self.capacity_cell(
            snapshot,
            owner,
            relation,
            &EntityId::String(RULE.into()),
            "policy",
        )
        .await?
        .ok_or_else(uncovered)
        .and_then(CapacityPolicy::decode)
    }

    async fn capacity_cell(
        &self,
        snapshot: Snapshot,
        owner: &str,
        relation: &OwnedRelation,
        id: &EntityId,
        column: &str,
    ) -> Result<Option<Value>> {
        let context = self.context.query_context(
            snapshot,
            self.tx_key().system_time,
            &self.options.default_tz,
            self.trace_parent,
        )?;
        let cursor =
            rtdb_query::open_owned_capacity_point(context, owner, relation, id, &[column]).await?;
        self.read_occupancy_cell(cursor, id, column, uncovered)
            .await
    }
}

fn hosted_capacity_declaration(document: &StructFields<Value>) -> Result<(OwnedRelation, &str, i64, &[u8], Timestamp)> {
    let (
        Some(Value::Utf8(id)),
        Some(Value::Utf8(relation)),
        Some(Value::Utf8(column)),
        Some(Value::I64(cap)),
        Some(Value::Binary(when)),
        Some(Value::I64(writer)),
    ) = (
        document.get("_id"), document.get("relation"), document.get("column"),
        document.get("cap"), document.get("predicate"), document.get("writer_micros"),
    ) else {
        return Err(invalid_rule());
    };
    let relation = OwnedRelation::new(relation.as_str())?;
    if document.len() != 6 || id != column || !relation.is_root_data() || *writer <= 0 {
        return Err(invalid_rule());
    }
    Ok((relation, column, *cap, when, Timestamp(*writer)))
}

fn count_document(id: &EntityId, count: i64) -> StructFields<Value> {
    StructFields::from([
        ("_id".into(), id.as_value()),
        ("count".into(), Value::I64(count)),
    ])
}

fn invalid_rule() -> Anomaly {
    Anomaly::incorrect(
        "rtdb.owned/capacity-rule",
        "A capacity rule requires a Binary column, a whole key or bounded prefix, and a positive cap",
    )
}

fn invalid_key() -> Anomaly {
    Anomaly::incorrect(
        "rtdb.owned/capacity-key",
        "A capacity key must contain its declared Binary partition within 4096 bytes",
    )
}

pub(super) fn uncovered() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb.owned/capacity-uncovered",
        "The owned relation cannot verify its capacity accounting",
    )
}

fn count_uncovered(error: Anomaly) -> Anomaly {
    if error.code == "rtdb.owned/capacity-uncovered" {
        Anomaly::new(
            AnomalyCategory::Conflict,
            "rtdb.owned/count-bound-uncovered",
            "The declared prefix cannot prove its exact count at the ordered cut",
        )
    } else {
        error
    }
}
