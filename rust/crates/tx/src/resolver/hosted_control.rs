use std::collections::BTreeSet;

use super::*;

impl OpenTransaction<'_> {
    pub(super) async fn check_hosted_controls(
        &self,
        operations: &[Operation],
        source_scope: Option<rtdb_types::PlacedAttemptScope>,
    ) -> Result<()> {
        let controls = operations.iter().filter_map(|operation| match operation {
            Operation::Public(TxOp::HostedControl(control)) => Some(control),
            _ => None,
        });
        if controls.clone().next().is_none() {
            return Ok(());
        }
        let unavailable = || {
            Anomaly::new(
                AnomalyCategory::Forbidden,
                "rtdb.hosted/control-scope",
                "Hosted controls require an admitted placed commit",
            )
        };
        let Some(scope) =
            source_scope.filter(|scope| Some(*scope) == self.context.owned_witness_scope)
        else {
            return Err(unavailable());
        };
        let permits = |operation: &Operation| {
            matches!(
                operation,
                Operation::Public(
                    TxOp::OwnedDeadline { .. }
                        | TxOp::OwnedCheck { .. }
                        | TxOp::OwnedCheckRange { .. }
                        | TxOp::OwnedCheckCapacity { .. }
                        | TxOp::OwnedInsertDocs { .. }
                        | TxOp::OwnedReplaceDocs { .. }
                        | TxOp::OwnedDelete { .. }
                        | TxOp::HostedControl(_)
                ) | Operation::OwnedWriteIids { .. }
            )
        };
        if !operations
            .iter()
            .any(|operation| matches!(operation, Operation::Public(TxOp::OwnedDeadline { .. })))
        {
            return Err(unavailable());
        }
        let validating_world = self.owned_world.is_some();
        let publishing_world = operations.iter().any(|op| matches!(op, Operation::Public(TxOp::OwnedPublishWorld { .. })));
        if validating_world && (
            controls.clone().any(|control| !control.is_check())
            || !operations.iter().any(|operation| matches!(operation, Operation::Public(TxOp::OwnedCheckWorldScope { .. })))
        ) {
            return Err(unavailable());
        }
        let mut has_fact_write = false;
        for operation in operations {
            if validating_world {
                if !matches!(operation, Operation::Public(
                    TxOp::OwnedDeadline { .. } | TxOp::OwnedCheck { .. } | TxOp::OwnedCheckWorldScope { .. }
                        | TxOp::OwnedCheckWorldPurge { .. } | TxOp::HostedControl(_)
                )) && !(publishing_world && matches!(operation,
                    Operation::Public(TxOp::OwnedPublishWorld { .. } | TxOp::OwnedReplaceDocs { .. })
                        | Operation::OwnedWriteIids { create_only: false, .. }
                )) {
                    return Err(unavailable());
                }
            } else if !permits(operation) || operation.owned_relation().is_some_and(|relation|
                relation.world().is_some() || relation.is_world_catalog()
            ) {
                return Err(unavailable());
            }
            has_fact_write |= match operation {
                Operation::Public(TxOp::OwnedDelete { .. }) => true,
                Operation::Public(
                    TxOp::OwnedInsertDocs { documents, .. }
                    | TxOp::OwnedReplaceDocs { documents, .. },
                )
                | Operation::OwnedWriteIids { documents, .. } => !documents.rows().is_empty(),
                _ => false,
            };
        }
        if !has_fact_write && controls.clone().any(|control| !control.is_check()) {
            return Err(unavailable());
        }

        let base = self.buffer.base_snapshot();
        let context = self.context.query_context(
            base.clone(),
            self.tx_key().system_time,
            &self.options.default_tz,
            self.trace_parent,
        )?;
        let mut locators = BTreeSet::new();
        let mut selected_tables = BTreeSet::new();
        let mut charges = Vec::new();
        let mut rows = 0usize;
        for control in controls {
            self.context.check_cancelled()?;
            let table = control.table();
            rows += 1;
            selected_tables.insert(control.name());
            if rows > 4096 || selected_tables.len() > 32 {
                return Err(unavailable());
            }
            if control.scope() != scope || !base.contains_table(&table) {
                return Err(unavailable());
            }
            if let Some(document) = control.replacement() {
                let fields = document.keys().cloned();
                let existing = base.column_types_for(&table, fields);
                if document.iter().any(|(name, value)| {
                    existing.get(name).is_none_or(|kind| {
                        matches!(kind, LogicalType::Nothing | LogicalType::Null)
                            || matches!(value, Value::Undefined)
                            || kind.join(&value.logical_type()) != *kind
                    })
                }) {
                    return Err(Anomaly::incorrect(
                        "rtdb.hosted/control-shape",
                        "Hosted control fields must match the installed ordinary column types",
                    ));
                }
            }
            charges.push(self.context.memory.reserve(table.table.len() + 512)?);
            if !locators.insert((table.clone(), control.id().iid())) {
                return Err(Anomaly::incorrect(
                    "rtdb.hosted/control-repeat",
                    "A hosted control row must appear once per commit",
                ));
            }
            let point = rtdb_query::read_hosted_control_point(
                Arc::clone(&context),
                table,
                control.id(),
                false,
            )
            .await?;
            let revision = point.row.map(|(_, revision)| revision);
            if revision != control.expected() {
                return Err(Anomaly::new(
                    AnomalyCategory::Conflict,
                    "rtdb.hosted/stale-control",
                    "An observed hosted control changed before ordering",
                ));
            }
        }
        Ok(())
    }
}
