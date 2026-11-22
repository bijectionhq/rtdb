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
                world: next, expected: None, advance_to: None, drop_world: true, delete_root: writes, ..
            }) if world.is_none() => {
                world = Some(*next);
                delete_root = *writes;
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
            delete_root: false,
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
