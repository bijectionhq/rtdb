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
                "Hosted controls require an admitted placed root commit",
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
        let mut has_fact_write = false;
        for operation in operations {
            if !permits(operation)
                || operation.owned_relation().is_some_and(|relation| {
                    relation.world().is_some() || relation.is_world_catalog()
                })
            {
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
        if !has_fact_write {
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
            let fields = control.document().keys().cloned();
            let existing = base.column_types_for(&table, fields);
            if control.document().iter().any(|(name, value)| {
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
            charges.push(self.context.memory.reserve(table.table.len() + 512)?);
            if !locators.insert((table.clone(), control.id().iid())) {
                return Err(Anomaly::incorrect(
                    "rtdb.hosted/control-repeat",
                    "A hosted control row must appear once per commit",
                ));
            }
            let mut cursor =
                rtdb_query::open_hosted_control_point(Arc::clone(&context), table, control.id())
                    .await?
                    .ok_or_else(unavailable)?;
            let mut revision = None;
            while let Some(batch) = cursor.next_batch().await? {
                let batch = batch.batch();
                if batch.num_rows() == 0 {
                    continue;
                }
                if batch.num_rows() != 1 || revision.is_some() {
                    return Err(unavailable());
                }
                let column = |name| {
                    let (index, field) = batch
                        .schema_ref()
                        .column_with_name(name)
                        .ok_or_else(unavailable)?;
                    rtdb_columnar::read_value(field, batch.column(index).as_ref(), 0)
                };
                if !matches!(
                    column("_valid_from")?,
                    Value::Timestamp {
                        value: 0,
                        unit: TimeUnit::Microsecond,
                        ..
                    }
                ) || column("_valid_to")? != Value::Null
                {
                    return Err(unavailable());
                }
                revision = Some(rtdb_query::owned_row_revision(
                    batch,
                    0,
                    control.id(),
                    &self.context.memory,
                )?);
            }
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
