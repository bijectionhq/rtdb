use rtdb_types::{
    Anomaly, EntityId, OwnedRelation, PlacedAttemptScope, Result, StructFields, TableRef,
    TimeUnit, Timestamp, Value,
};

pub(crate) const CONTROL_TABLES: &str = "hosted_control_tables";
const RESTORE: &str = "bj_rtdb_restore_hold";
const NATIVE_FRONTIER: &str = "native-frontier.v1";
const REPLAY_TRANSACTIONS: &str = "bj_rtdb_restore_transactions";

#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostedReplay {
    binding: [u8; 32],
    archive: [u8; 32],
    through: u64,
    previous: Option<rtdb_types::TransactionKey>,
    source: rtdb_types::TransactionKey,
    logical_at: u64,
    fingerprint: [u8; 32],
}

/// The only admitted hosted write shape for the held native restore ledger.
/// The resolver checks each observation and its contents in the ordered base.
pub fn hosted_replay_controls<'a>(
    controls: impl Iterator<Item = &'a HostedControlWrite> + Clone,
) -> Result<Option<HostedReplay>> {
    if !controls.clone().any(|control| control.name() == RESTORE && !control.is_check()) {
        return Ok(None);
    }
    let selected = |relation: &str, id: &str| controls.clone().find(|control| {
        control.name() == relation && matches!(control.id(), EntityId::String(key) if key == id)
    });
    let check = |relation: &str, id: &str, exists: bool| {
        selected(relation, id).is_some_and(|control| control.is_check() && control.expected().is_some() == exists)
    };
    let head = selected(RESTORE, NATIVE_FRONTIER).ok_or_else(replay_invalid)?;
    let encoded = replay_body(head).ok_or_else(replay_invalid)?;
    let replay: HostedReplay = serde_json::from_slice(encoded).map_err(|_| replay_invalid())?;
    let source = replay.source;
    let receipt_id = format!("native-transaction/{}", source.tx_id.0);
    let receipt = selected(RESTORE, &receipt_id).ok_or_else(replay_invalid)?;
    let coordinates = selected(REPLAY_TRANSACTIONS, &receipt_id).ok_or_else(replay_invalid)?;
    if controls.clone().filter(|control| control.name() == RESTORE).count() != 5
        || controls.clone().filter(|control| control.name() == "bj_installation").count() != 1
        || controls.clone().filter(|control| control.name() == REPLAY_TRANSACTIONS).count() != 1
        || !check("bj_installation", "authority", true)
        || !check(RESTORE, "restore", true) || !check(RESTORE, "archive", true)
        || !check(RESTORE, "replayed", false)
        || head.expected().is_some() != replay.previous.is_some()
        || receipt.expected().is_some() || replay_body(receipt) != Some(encoded)
        || source.tx_id.0 < 0 || source.system_time.0 <= 0
        || replay.logical_at == 0 || replay.logical_at % 1_000 != 0
        || u64::try_from(source.system_time.0).ok().and_then(|time| time.checked_mul(1_000))
            .is_none_or(|time| time < replay.logical_at || time > replay.through)
        || replay.previous.is_some_and(|previous| previous.tx_id.0 < 0 || previous.tx_id >= source.tx_id
            || previous.system_time.0 <= 0 || previous.system_time >= source.system_time)
        || !replay.matches_coordinates(coordinates)
    {
        return Err(replay_invalid());
    }
    Ok(Some(replay))
}

fn replay_body(control: &HostedControlWrite) -> Option<&[u8]> {
    let document = control.replacement()?;
    let Value::Binary(body) = document.get("binding")? else { return None; };
    (document.len() == 2 && body.len() <= 4096).then_some(body.as_slice())
}

impl HostedReplay {
    fn matches_coordinates(&self, control: &HostedControlWrite) -> bool {
        let Some(document) = control.replacement() else { return false; };
        let matches_bytes = |name: &str, bytes: &[u8]| {
            matches!(document.get(name), Some(Value::Binary(value)) if value.as_slice() == bytes)
        };
        let matches_time = |name: &str, micros: i64| {
            matches!(document.get(name), Some(Value::Timestamp {
                value, unit: TimeUnit::Microsecond, timezone: Some(_),
            }) if *value == micros)
        };
        control.expected().is_none()
            && document.len() == 6
            && matches_bytes("binding", &self.binding)
            && matches_bytes("digest", &self.fingerprint)
            && matches_time("source_at", self.source.system_time.0)
            && matches_time("logical_at", (self.logical_at / 1_000) as i64)
            && matches_time("source_cut", (self.through / 1_000) as i64)
    }

    pub(crate) fn check_order(&self, ordered: Timestamp) -> Result<()> {
        if u64::try_from(ordered.0).ok().and_then(|time| time.checked_mul(1_000)).is_none_or(|time| time <= self.through) {
            return Err(replay_invalid());
        }
        Ok(())
    }

    pub(crate) fn check_base(&self, control: &HostedControlWrite, row: Option<&arrow_array::RecordBatch>) -> Result<()> {
        use arrow_array::{Array, BinaryArray};
        if control.name() != RESTORE { return Ok(()); }
        let EntityId::String(id) = control.id() else { return Err(replay_invalid()); };
        let binding = || -> Result<&[u8]> {
            let row = row.filter(|row| row.num_rows() == 1).ok_or_else(replay_invalid)?;
            let bytes = row.column_by_name("binding").and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
                .filter(|bytes| bytes.is_valid(0)).ok_or_else(replay_invalid)?;
            Ok(bytes.value(0))
        };
        let matches = match id.as_str() {
            "restore" => binding()? == self.binding.as_slice(),
            "archive" => binding()?.split_first_chunk::<32>().is_some_and(|(archive, cut)| {
                archive == &self.archive && cut == self.through.to_be_bytes().as_slice()
            }),
            NATIVE_FRONTIER if self.previous.is_some() => {
                let bytes = binding()?;
                if bytes.len() > 4096 { return Err(replay_invalid()); }
                let previous: Self = serde_json::from_slice(bytes).map_err(|_| replay_invalid())?;
                previous.binding == self.binding && previous.archive == self.archive && previous.through == self.through
                    && Some(previous.source) == self.previous && previous.logical_at < self.logical_at
            }
            _ => true,
        };
        if !matches { return Err(replay_invalid()); }
        Ok(())
    }

    pub(crate) fn source_time(&self) -> Timestamp { self.source.system_time }
}

fn replay_invalid() -> Anomaly {
    Anomaly::new(rtdb_types::AnomalyCategory::Conflict, "rtdb.hosted/restore", "Native replay requires its exact original transaction and held archive controls")
}

pub(crate) fn admits_name(name: &str) -> bool {
    name.starts_with("bj_") && !name.contains('-') && OwnedRelation::new(name).is_ok()
}

pub(crate) fn hosted_root_world_reads(operations: &[crate::source::Operation]) -> bool {
    use crate::{TxOp, source::Operation};

    let mut scopes = 0;
    let mut guards = 0;
    let mut hosted = false;
    let mut preceding_guard = None;
    for operation in operations {
        let guard = match operation {
            Operation::Public(TxOp::OwnedCheckWorldPurge { relation, .. })
                if relation.world().is_some() =>
            {
                guards += 1;
                Some(relation)
            },
            Operation::Public(TxOp::OwnedCheckWorldScope { .. }) => {
                scopes += 1;
                None
            },
            Operation::Public(TxOp::HostedControl(_)) => {
                hosted = true;
                None
            },
            Operation::Public(TxOp::OwnedDeadline { .. }) => None,
            Operation::Public(TxOp::OwnedCheck { relation, .. }) if relation.world().is_some() => {
                if preceding_guard != Some(relation) {
                    return false;
                }
                None
            },
            Operation::Public(TxOp::OwnedCheck { relation, .. } | TxOp::OwnedCheckRange { relation, .. }
                | TxOp::OwnedCheckCapacity { relation, .. } | TxOp::OwnedInsertDocs { relation, .. }
                | TxOp::OwnedReplaceDocs { relation, .. } | TxOp::OwnedDelete { relation, .. })
            | Operation::OwnedWriteIids { relation, .. }
                if relation.world().is_none() && !relation.is_world_catalog() => None,
            _ => return false,
        };
        preceding_guard = guard;
    }
    hosted && scopes == 1 && guards > 0
}

/// A retained drop can settle a bounded set of existing owner rows.
pub fn retained_release_controls<'a>(
    controls: impl IntoIterator<Item = &'a HostedControlWrite>,
) -> bool {
    let mut checks = 0;
    let mut replacements = 0;
    for control in controls {
        if control.is_check() {
            checks += 1;
        } else if control.expected().is_some() {
            replacements += 1;
        } else {
            return false;
        }
    }
    checks != 0 && (1..=2).contains(&replacements)
}

pub fn retained_delete_controls<'a>(controls: impl IntoIterator<Item = &'a HostedControlWrite>) -> bool {
    let mut checks = 0;
    let mut replacements = 0;
    let mut existing = 0;
    for control in controls {
        if control.is_check() {
            checks += 1;
        } else {
            replacements += 1;
            existing += usize::from(control.expected().is_some());
        }
    }
    checks > 0 && existing > 0 && (1..=5).contains(&replacements)
}

pub(crate) fn retained_release_world(operations: &[crate::source::Operation]) -> Option<rtdb_types::Iid> {
    use crate::{TxOp, source::Operation};
    let mut world = None;
    let mut controls = Vec::new();
    let mut delete = None;
    let mut root_checks = 0;
    let mut delete_root = false;
    for op in operations {
        match op {
            Operation::Public(TxOp::HostedControl(control)) => controls.push(control),
            Operation::Public(TxOp::OwnedDeadline { .. } | TxOp::OwnedCheckWorldScope { .. }) => (),
            Operation::Public(TxOp::OwnedCheck { relation, .. } | TxOp::OwnedCheckRange { relation, .. })
                if relation.is_root_data() => { root_checks += 1; }
            Operation::Public(TxOp::OwnedDelete { relation, id, valid_from: None, valid_to: None })
                if relation.is_root_data() && delete.is_none() => delete = Some((relation, id)),
            Operation::Public(TxOp::OwnedPublishWorld {
                world: next, expected: None, advance_to: None, drop_world: true, delete_root: writes, land_root: None, ..
            }) if world.is_none() => {
                world = Some(*next);
                delete_root = writes.is_some();
            }
            _ => return None,
        }
    }
    world.filter(|_| {
        if !delete_root {
            return delete.is_none() && root_checks == 0 && retained_release_controls(controls);
        }
        let Some((relation, id)) = delete else { return false; };
        let checked = operations.iter().any(|operation| matches!(
            operation,
            Operation::Public(TxOp::OwnedCheck { relation: checked, id: observed, expected: Some(_), .. })
                if checked == relation && observed == id
        ));
        checked && retained_delete_controls(controls)
    })
}

/// An assertion or checked replacement of one installed hosted control row.
#[derive(Clone, Debug)]
pub struct HostedControlWrite {
    relation: OwnedRelation,
    scope: PlacedAttemptScope,
    id: EntityId,
    expected: Option<Timestamp>,
    document: StructFields<Value>,
    check_only: bool,
}

impl HostedControlWrite {
    pub fn new(
        relation: OwnedRelation,
        scope: PlacedAttemptScope,
        expected: Option<Timestamp>,
        document: StructFields<Value>,
    ) -> Result<Self> {
        if !admits_name(relation.name())
            || document.len() > 128
            || document
                .keys()
                .any(|name| name != "_id" && name.starts_with('_'))
        {
            return Err(invalid());
        }
        let Some(Value::Utf8(id)) = document.get("_id") else {
            return Err(invalid());
        };
        if id.is_empty() || id.len() > 4096 {
            return Err(invalid());
        }
        let id = EntityId::String(id.clone());
        let result = Self {
            relation,
            scope,
            id,
            expected,
            document,
            check_only: false,
        };
        if result.retained_bytes()? > 1024 * 1024 {
            return Err(invalid());
        }
        Ok(result)
    }

    pub fn check(
        relation: OwnedRelation,
        scope: PlacedAttemptScope,
        expected: Option<Timestamp>,
        document: StructFields<Value>,
    ) -> Result<Self> {
        if document.len() != 1 {
            return Err(invalid());
        }
        let mut result = Self::new(relation, scope, expected, document)?;
        result.check_only = true;
        Ok(result)
    }

    pub fn is_check(&self) -> bool {
        self.check_only
    }

    pub(crate) fn name(&self) -> &str {
        self.relation.name()
    }

    pub fn table(&self) -> TableRef {
        TableRef::new("public", self.relation.name())
    }
    pub fn scope(&self) -> PlacedAttemptScope {
        self.scope
    }
    pub fn id(&self) -> &EntityId {
        &self.id
    }
    pub fn expected(&self) -> Option<Timestamp> {
        self.expected
    }
    pub fn replacement(&self) -> Option<&StructFields<Value>> {
        (!self.check_only).then_some(&self.document)
    }

    pub fn retained_bytes(&self) -> Result<usize> {
        Value::document_retained_bytes(&self.document)?
            .checked_add(self.id.as_value().retained_bytes()?)
            .and_then(|n| n.checked_add(self.relation.name().len() + 512))
            .ok_or_else(invalid)
    }

    pub(crate) fn body(&self) -> StructFields<Value> {
        StructFields::from([
            ("relation".into(), Value::Utf8(self.relation.name().into())),
            ("scope".into(), Value::Binary(self.scope.encode().to_vec())),
            (
                "expected".into(),
                self.expected.map_or(Value::Null, |time| Value::I64(time.0)),
            ),
            ("document".into(), Value::Struct(self.document.clone())),
        ])
    }
}

fn invalid() -> Anomaly {
    Anomaly::incorrect("rtdb.hosted/control", "Invalid hosted control replacement")
}

#[cfg(test)]
mod world_read_tests {
    use super::*;
    use crate::{TxOp, source::Operation};
    use rtdb_types::Iid;

    #[test]
    fn hosted_world_reads_admit_root_writes_with_guarded_world_points() -> Result<()> {
        let scope = PlacedAttemptScope::new("hosted-child-read", 1)?;
        let root = OwnedRelation::new("facts")?;
        let branch = root.in_world(Iid([1; 16]))?;
        let id = EntityId::String("entity".into());
        let controls = OwnedRelation::new("bj_owner")?;
        let value = StructFields::from([("_id".into(), Value::Utf8("installed".into()))]);
        let owner = HostedControlWrite::check(controls, scope, Some(Timestamp(1)), value)?;
        let point = TxOp::OwnedCheck {
            relation: branch.clone(), id: id.clone(), valid_at: Timestamp(4), expected: None,
        };
        let purge = TxOp::OwnedCheckWorldPurge {
            relation: branch.clone(), pin: Iid([2; 16]), frontier: 0, local_frontier: None,
        };
        let mut operations = [
            TxOp::OwnedDeadline { until: Timestamp(8) },
            TxOp::HostedControl(owner),
            TxOp::OwnedDelete { relation: root, id: id.clone(), valid_from: None, valid_to: None },
            TxOp::OwnedCheckWorldScope { scope },
            purge,
            point,
        ].into_iter().map(Operation::Public).collect::<Vec<_>>();
        assert!(hosted_root_world_reads(&operations));
        operations.swap(4, 5);
        assert!(!hosted_root_world_reads(&operations));
        operations.swap(4, 5);
        operations[2] = Operation::Public(TxOp::OwnedDelete {
            relation: branch, id, valid_from: None, valid_to: None,
        });
        assert!(!hosted_root_world_reads(&operations));
        Ok(())
    }
}

#[cfg(test)]
mod replay_tests {
    use super::*;

    #[test]
    fn replay_requires_the_hold_and_identical_create_only_receipt() -> Result<()> {
        let scope = PlacedAttemptScope::new("native-restore", 1)?;
        let hold = OwnedRelation::new(RESTORE)?;
        let fields = |id: &str| StructFields::from([("_id".into(), Value::Utf8(id.into()))]);
        let (binding, archive, fingerprint) = ([1u8; 32], [2u8; 32], [3u8; 32]);
        let body = serde_json::json!({"binding": binding, "archive": archive, "through": 4000,
            "previous": null, "source": {"tx_id": 3, "system_time": 4}, "logical_at": 3000,
            "fingerprint": fingerprint}).to_string().into_bytes();
        let replace = |id: &str, expected, body: &[u8]| {
            let mut document = fields(id);
            document.insert("binding".into(), Value::Binary(body.to_vec()));
            HostedControlWrite::new(hold.clone(), scope, expected, document)
        };
        let projection = |logical_at| {
            let time = |value| Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) };
            let mut document = fields("native-transaction/3");
            document.insert("binding".into(), Value::Binary(binding.to_vec()));
            document.insert("digest".into(), Value::Binary(fingerprint.to_vec()));
            document.insert("source_at".into(), time(4));
            document.insert("logical_at".into(), time(logical_at));
            document.insert("source_cut".into(), time(4));
            HostedControlWrite::new(OwnedRelation::new(REPLAY_TRANSACTIONS)?, scope, None, document)
        };
        let mut controls = vec![
            HostedControlWrite::check(OwnedRelation::new("bj_installation")?, scope, Some(Timestamp(9)), fields("authority"))?,
            HostedControlWrite::check(hold.clone(), scope, Some(Timestamp(9)), fields("archive"))?,
            HostedControlWrite::check(hold.clone(), scope, None, fields("replayed"))?,
            replace(NATIVE_FRONTIER, None, &body)?,
            replace("native-transaction/3", None, &body)?,
            projection(3)?,
            HostedControlWrite::check(hold.clone(), scope, Some(Timestamp(9)), fields("restore"))?,
        ];
        let replay = hosted_replay_controls(controls.iter())?.unwrap();
        assert_eq!(replay.source_time(), Timestamp(4));
        assert!(replay.check_order(Timestamp(4)).is_err());
        assert!(replay.check_order(Timestamp(5)).is_ok());
        controls[4] = replace("native-transaction/3", Some(Timestamp(9)), &body)?;
        assert!(hosted_replay_controls(controls.iter()).is_err());
        controls[4] = replace("native-transaction/3", None, &body)?;
        controls[5] = projection(2)?;
        assert!(hosted_replay_controls(controls.iter()).is_err());
        controls[5] = projection(3)?;
        controls.pop();
        assert!(hosted_replay_controls(controls.iter()).is_err());
        Ok(())
    }
}

#[cfg(test)]
mod retained_release_tests {
    use super::*;
    use crate::{TxOp, source::Operation};
    use rtdb_types::Iid;

    #[test]
    fn retained_release_requires_existing_replacements_and_one_terminal_world() -> Result<()> {
        let scope = PlacedAttemptScope::new("root-release", 1)?;
        let relation = OwnedRelation::new("bj_owner")?;
        let control = |id: &str| StructFields::from([("_id".into(), Value::Utf8(id.into()))]);
        let check = HostedControlWrite::check(relation.clone(), scope, Some(Timestamp(1)), control("installed"))?;
        let replacement = |expected| HostedControlWrite::new(relation.clone(), scope, expected, control("holder"));
        let drop = |world| TxOp::OwnedPublishWorld {
            world,
            attempt: "0000000000000000000000".into(),
            fingerprint: [1; 32],
            expected: None,
            advance_to: None,
            drop_world: true,
            settlement_pin: None,
            delete_root: None,
            land_root: None,
        };
        let world = Iid([1; 16]);
        let mut operations = [
            TxOp::OwnedDeadline { until: Timestamp(8) },
            TxOp::OwnedCheckWorldScope { scope },
            TxOp::HostedControl(check),
            TxOp::HostedControl(replacement(Some(Timestamp(2)))?),
            drop(world),
        ].into_iter().map(Operation::Public).collect::<Vec<_>>();
        assert_eq!(retained_release_world(&operations), Some(world));
        operations.push(Operation::Public(drop(Iid([2; 16]))));
        assert_eq!(retained_release_world(&operations), None);
        operations.pop();
        operations[3] = Operation::Public(TxOp::HostedControl(replacement(None)?));
        assert_eq!(retained_release_world(&operations), None);
        Ok(())
    }

    #[test]
    fn proposal_period_requires_its_entity_point_and_original_overlay_frontier() -> Result<()> {
        let scope = PlacedAttemptScope::new("proposal-period", 1)?;
        let world = Iid([1; 16]);
        let facts = OwnedRelation::new("facts")?.in_world(world)?;
        let control_relation = OwnedRelation::new("bj_owner")?;
        let control = |id: &str| StructFields::from([("_id".into(), Value::Utf8(id.into()))]);
        let id = EntityId::String("entity".into());
        let memory = rtdb_columnar::MemoryBudget::new(1 << 20);
        let shadow = rtdb_types::WorldShadow::Deleted.record(&id)?;
        let mut operations = [
            TxOp::HostedControl(HostedControlWrite::check(control_relation.clone(), scope, Some(Timestamp(1)), control("installed"))?),
            TxOp::HostedControl(HostedControlWrite::new(control_relation, scope, Some(Timestamp(2)), control("owner"))?),
            TxOp::OwnedPublishWorld {
                world, attempt: "0000000000000000000000".into(), fingerprint: [1; 32],
                expected: None, advance_to: None, drop_world: false, settlement_pin: None,
                delete_root: None, land_root: None,
            },
            TxOp::OwnedReplaceDocs {
                relation: facts.clone(), documents: crate::Documents::new(vec![shadow], &memory)?,
                valid_from: Some(Timestamp(10)), valid_to: Some(Timestamp(20)),
            },
        ].into_iter().map(Operation::Public).collect::<Vec<_>>();
        operations.push(Operation::Public(TxOp::OwnedCheckWorldPurge {
            relation: facts.clone(), pin: world, frontier: 0, local_frontier: Some(0),
        }));
        assert_eq!(hosted_proposal_overlay(&operations), None);
        operations.push(Operation::Public(TxOp::OwnedCheck {
            relation: facts.clone(), id: id.clone(), valid_at: Timestamp(11), expected: None,
        }));
        assert_eq!(hosted_proposal_overlay(&operations), None);
        operations.pop();
        operations.push(Operation::Public(TxOp::OwnedCheck {
            relation: facts, id, valid_at: Timestamp(10), expected: None,
        }));
        assert_eq!(hosted_proposal_overlay(&operations), Some(world));
        operations.remove(4);
        assert_eq!(hosted_proposal_overlay(&operations), None);
        operations.push(Operation::Public(TxOp::OwnedCheckWorldPurge {
            relation: OwnedRelation::new("facts")?.in_world(Iid([2; 16]))?,
            pin: world, frontier: 0, local_frontier: Some(0),
        }));
        assert_eq!(hosted_proposal_overlay(&operations), None);
        Ok(())
    }
}

/// Proposal content settlements may also replace their exact generation row.
pub fn proposal_overlay_controls<'a>(controls: impl IntoIterator<Item = &'a HostedControlWrite>) -> bool {
    let mut checks = 0;
    let mut existing = 0;
    let mut replacements = 0;
    for control in controls {
        if control.is_check() {
            checks += 1;
        } else {
            existing += usize::from(control.expected().is_some());
            replacements += 1;
        }
    }
    checks > 0 && (1..=6).contains(&replacements)
        && existing >= if replacements == 6 { 2 } else { 1 }
}

/// Classify an overlay settled with hosted owner controls and its original checks.
pub(crate) fn hosted_proposal_overlay(operations: &[crate::source::Operation]) -> Option<rtdb_types::Iid> {
    use std::collections::{BTreeMap, BTreeSet};
    use crate::{TxOp, source::Operation};
    let mut world = None;
    let mut controls = Vec::new();
    let mut writes = Vec::new();
    let mut terminal = false;
    let mut create = false;
    let mut points = BTreeMap::<_, BTreeSet<_>>::new();
    let mut timelines = BTreeSet::new();
    for operation in operations {
        match operation {
            Operation::Public(TxOp::HostedControl(control)) => controls.push(control),
            Operation::Public(TxOp::OwnedDeadline { .. } | TxOp::OwnedCheckWorldScope { .. }
                | TxOp::OwnedCheckRange { .. }) => (),
            Operation::Public(TxOp::OwnedCheckWorldPurge { relation, local_frontier, .. }) => {
                if local_frontier.is_some() { timelines.insert(relation); }
            }
            Operation::Public(TxOp::OwnedCheck { relation, id, valid_at, expected }) => {
                points.entry((relation, id.handle_bytes())).or_default().insert((*valid_at, expected.is_none()));
            }
            Operation::Public(TxOp::OwnedPublishWorld {
                world: next, advance_to: None, drop_world: false, settlement_pin, delete_root: None, expected, ..
            }) if world.is_none() => {
                world = Some((*next, expected.is_none()));
                terminal = settlement_pin.is_some();
            },
            operation => {
                let write = proposal_overlay_write(operation)?;
                let start = match (write.from, write.to) {
                    (None, None) => None,
                    (Some(from), Some(to)) if Timestamp::MIN < from && from < to && to < Timestamp::MAX => Some(from),
                    _ => return None,
                };
                if write.documents.rows().len() != 1 { return None; }
                let id = write.documents.rows()[0].get("_id")?.entity_id().ok()?;
                if write.create_only {
                    if start.is_some() || create { return None; }
                    create = true;
                }
                writes.push((write.relation, id.handle_bytes(), start));
            }
        }
    }
    let (world, initial) = world?;
    if create && (!initial || writes.len() != 1) { return None; }
    let controls_allowed = if terminal { retained_delete_controls(controls) } else { proposal_overlay_controls(controls) };
    (terminal == writes.is_empty() && controls_allowed && writes.into_iter().all(|(relation, id, start)| {
        relation.world() == Some(world)
            && points.remove(&(relation, id)).is_some_and(|instants| {
                if create {
                    instants.iter().all(|(_, absent)| *absent)
                } else {
                    start.is_none_or(|start| instants.iter().any(|(at, _)| *at == start) && timelines.contains(&relation))
                }
            })
    })).then_some(world)
}

pub(crate) struct ProposalOverlayWrite<'a> {
    pub(crate) relation: &'a OwnedRelation,
    pub(crate) documents: &'a crate::Documents,
    pub(crate) from: Option<Timestamp>,
    pub(crate) to: Option<Timestamp>,
    pub(crate) create_only: bool,
}

pub(crate) fn proposal_overlay_write(
    operation: &crate::source::Operation,
) -> Option<ProposalOverlayWrite<'_>> {
    use crate::{TxOp, source::Operation};
    let (relation, documents, from, to, create_only) = match operation {
        Operation::Public(TxOp::OwnedReplaceDocs { relation, documents, valid_from, valid_to }) =>
            (relation, documents, valid_from, valid_to, false),
        Operation::Public(TxOp::OwnedInsertDocs { relation, documents, valid_from, valid_to }) =>
            (relation, documents, valid_from, valid_to, true),
        Operation::OwnedWriteIids { relation, documents, valid_from, valid_to, create_only, .. } =>
            (relation, documents, valid_from, valid_to, *create_only),
        _ => return None,
    };
    relation.world().map(|_| ProposalOverlayWrite {
        relation, documents, from: *from, to: *to, create_only,
    })
}

/// A landing settles its existing owner and one new immutable receipt.
pub fn proposal_land_controls<'a>(controls: impl IntoIterator<Item = &'a HostedControlWrite>) -> bool {
    let mut checks = 0;
    let mut replacements = 0;
    let mut receipts = 0;
    for control in controls {
        if control.is_check() {
            checks += 1;
        } else if control.expected().is_some() {
            replacements += 1;
        } else {
            receipts += 1;
        }
    }
    checks > 0 && replacements == 1 && receipts == 1
}

/// Last-holder closure replaces the exact census, owner, and quota, and writes a receipt.
pub fn proposal_close_controls<'a>(controls: impl IntoIterator<Item = &'a HostedControlWrite>) -> bool {
    let mut checks = 0;
    let mut replacements = 0;
    let mut inserts = 0;
    for control in controls {
        if control.is_check() {
            checks += 1;
        } else if control.expected().is_some() {
            replacements += 1;
        } else {
            inserts += 1;
        }
    }
    checks > 0 && (3..=5).contains(&replacements) && (1..=2).contains(&inserts)
}

pub const MAX_PROPOSAL_LAND_ROWS: usize = 16;

/// One exact Proposal terminal effect and its matching witnessed Root writes.
pub(crate) fn hosted_proposal_land(
    operations: &[crate::source::Operation],
) -> Option<(rtdb_types::Iid, rtdb_types::Iid, bool)> {
    use crate::{TxOp, source::Operation};
    let mut landing = None;
    let mut replacements = std::collections::BTreeMap::new();
    let mut close = false;
    let mut insert = false;
    let mut controls = Vec::new();
    for op in operations {
        match op {
            Operation::Public(TxOp::HostedControl(control)) => controls.push(control),
            Operation::Public(TxOp::OwnedDeadline { .. } | TxOp::OwnedCheckWorldScope { .. }
                | TxOp::OwnedCheck { .. } | TxOp::OwnedCheckRange { .. } | TxOp::OwnedCheckWorldPurge { .. }) => (),
            Operation::Public(TxOp::OwnedReplaceDocs { relation, documents, valid_from: None, valid_to: None })
            | Operation::OwnedWriteIids { relation, documents, valid_from: None, valid_to: None, create_only: false, .. }
                if relation.is_root_data() && !close && !insert && !documents.rows().is_empty() =>
            {
                for row in documents.rows() {
                    let id = row.get("_id")?.entity_id().ok()?;
                    if replacements.insert((relation.name(), id.handle_bytes()), (relation, id)).is_some()
                        || replacements.len() > MAX_PROPOSAL_LAND_ROWS { return None; }
                }
            }
            Operation::Public(TxOp::OwnedInsertDocs { relation, documents, valid_from: None, valid_to: None })
            | Operation::OwnedWriteIids { relation, documents, valid_from: None, valid_to: None, create_only: true, .. }
                if relation.is_root_data() && replacements.is_empty() && documents.rows().len() == 1 =>
            {
                let id = documents.rows()[0].get("_id")?.entity_id().ok()?;
                replacements.insert((relation.name(), id.handle_bytes()), (relation, id));
                insert = true;
            }
            Operation::Public(TxOp::OwnedDelete { relation, id, valid_from: None, valid_to: None })
                if relation.is_root_data() && replacements.is_empty() => {
                close = true;
                replacements.insert((relation.name(), id.handle_bytes()), (relation, id.clone()));
            }
            Operation::Public(TxOp::OwnedPublishWorld {
                world, land_root: Some(pin), delete_root: None, advance_to: None,
                drop_world: true, expected, ..
            }) if landing.is_none() => landing = Some((*world, *pin, *expected)),
            _ => return None,
        }
    }
    let (world, pin, expected_clock) = landing?;
    if replacements.is_empty() { return None; }
    if operations.iter().filter_map(Operation::owned_relation)
        .any(|relation| relation.is_world_catalog() || relation.world().is_some_and(|other| other != world))
    {
        return None;
    }
    for (_, (relation, id)) in replacements {
        let root_checked = operations.iter().any(|op| matches!(op,
            Operation::Public(TxOp::OwnedCheck { relation: checked, id: observed, expected, .. })
                if checked == relation && observed == &id && expected.is_none() == insert
        ));
        if close {
            return (root_checked && proposal_close_controls(controls)
                && !operations.iter().any(|op| match op {
                    Operation::Public(TxOp::OwnedCheckWorldPurge { .. }) => true,
                    _ => op.owned_relation().is_some_and(|relation| relation.world().is_some()),
                })).then_some((world, pin, true));
        }
        let local = relation.in_world(world).ok()?;
        let local_checked = operations.windows(2).any(|pair| matches!(pair, [
            Operation::Public(TxOp::OwnedCheckWorldPurge { relation: guarded, pin: original, .. }),
            Operation::Public(TxOp::OwnedCheck { relation: checked, id: observed, expected: Some(_), .. })
        ] if guarded == &local && checked == &local && observed == &id && *original == pin));
        if !root_checked || !local_checked { return None; }
    }
    (expected_clock.is_some() && proposal_land_controls(controls)).then_some((world, pin, false))
}
