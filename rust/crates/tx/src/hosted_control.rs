use rtdb_types::{
    Anomaly, EntityId, OwnedRelation, PlacedAttemptScope, Result, StructFields, TableRef,
    Timestamp, Value,
};

pub(crate) const CONTROL_TABLES: &str = "hosted_control_tables";

pub(crate) fn admits_name(name: &str) -> bool {
    name.starts_with("bj_") && !name.contains('-') && OwnedRelation::new(name).is_ok()
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

/// Classify one acceptance-valid Proposal overlay settled with hosted owner controls.
/// Every changed locator needs an original point check in the same overlay.
pub(crate) fn hosted_proposal_overlay(operations: &[crate::source::Operation]) -> Option<rtdb_types::Iid> {
    use crate::{TxOp, source::Operation};
    let mut world = None;
    let mut controls = Vec::new();
    let mut writes = Vec::new();
    let mut terminal = false;
    let mut points = std::collections::BTreeSet::new();
    for operation in operations {
        match operation {
            Operation::Public(TxOp::HostedControl(control)) => controls.push(control),
            Operation::Public(TxOp::OwnedDeadline { .. } | TxOp::OwnedCheckWorldScope { .. }
                | TxOp::OwnedCheckWorldPurge { .. } | TxOp::OwnedCheckRange { .. }) => (),
            Operation::Public(TxOp::OwnedCheck { relation, id, .. }) => {
                points.insert((relation.name(), id.handle_bytes()));
            }
            Operation::Public(TxOp::OwnedPublishWorld {
                world: next, advance_to: None, drop_world: false, settlement_pin, delete_root: None, ..
            }) if world.is_none() => {
                world = Some(*next);
                terminal = settlement_pin.is_some();
            },
            Operation::Public(TxOp::OwnedReplaceDocs {
                relation, documents, valid_from: None, valid_to: None,
            }) | Operation::OwnedWriteIids {
                relation, documents, valid_from: None, valid_to: None, create_only: false, ..
            } if relation.world().is_some() && documents.rows().len() == 1 => {
                let id = documents.rows()[0].get("_id")?.entity_id().ok()?;
                writes.push((relation, id.handle_bytes()));
            }
            _ => return None,
        }
    }
    let world = world?;
    let controls_allowed = if terminal { retained_delete_controls(controls) } else { proposal_overlay_controls(controls) };
    (terminal == writes.is_empty() && controls_allowed && writes.into_iter().all(|(relation, id)| {
        relation.world() == Some(world) && points.remove(&(relation.name(), id))
    })).then_some(world)
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

/// One witnessed in-place clear Root replacement and its exact Proposal close.
pub(crate) fn hosted_proposal_land(
    operations: &[crate::source::Operation],
) -> Option<(rtdb_types::Iid, rtdb_types::Iid)> {
    use crate::{TxOp, source::Operation};
    let mut landing = None;
    let mut replacement = None;
    let mut controls = Vec::new();
    for op in operations {
        match op {
            Operation::Public(TxOp::HostedControl(control)) => controls.push(control),
            Operation::Public(TxOp::OwnedDeadline { .. } | TxOp::OwnedCheckWorldScope { .. }
                | TxOp::OwnedCheck { .. } | TxOp::OwnedCheckRange { .. } | TxOp::OwnedCheckWorldPurge { .. }) => (),
            Operation::Public(TxOp::OwnedReplaceDocs { relation, documents, valid_from: None, valid_to: None })
            | Operation::OwnedWriteIids { relation, documents, valid_from: None, valid_to: None, create_only: false, .. }
                if relation.is_root_data() && replacement.is_none() && documents.rows().len() == 1 =>
            {
                replacement = Some((relation, documents.rows()[0].get("_id")?.entity_id().ok()?));
            }
            Operation::Public(TxOp::OwnedPublishWorld {
                world, land_root: Some(pin), delete_root: None, advance_to: None,
                drop_world: true, expected: Some(_), ..
            }) if landing.is_none() => landing = Some((*world, *pin)),
            _ => return None,
        }
    }
    let (world, pin) = landing?;
    let (relation, id) = replacement?;
    if operations.iter().filter_map(Operation::owned_relation)
        .any(|relation| relation.is_world_catalog() || relation.world().is_some_and(|other| other != world))
    {
        return None;
    }
    let local = relation.in_world(world).ok()?;
    let root_checked = operations.iter().any(|op| matches!(op,
        Operation::Public(TxOp::OwnedCheck { relation: checked, id: observed, expected: Some(_), .. })
            if checked == relation && observed == &id
    ));
    let local_checked = operations.windows(2).any(|pair| matches!(pair, [
        Operation::Public(TxOp::OwnedCheckWorldPurge { relation: guarded, pin: original, .. }),
        Operation::Public(TxOp::OwnedCheck { relation: checked, id: observed, expected: Some(_), .. })
    ] if guarded == &local && checked == &local && observed == &id && *original == pin));
    (root_checked && local_checked && proposal_land_controls(controls)).then_some((world, pin))
}
