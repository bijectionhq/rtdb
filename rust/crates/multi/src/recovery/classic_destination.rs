use std::sync::Arc;

use rtdb_directory::{CheckpointTransferTerminal, DeploymentRecord, Directory, Lifecycle};
use rtdb_server::multi::ClassicCandidate;
use rtdb_types::{Anomaly, EntityId, Result, TableRef, TransactionKey};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{RecoveryService, Reply, valid_manifest};
use crate::archive::{
    self, ClassicProjection, ClassicRead, ReceiptHistory, ReceiptLookup, StageSelector,
};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Selection {
    destination: Uuid,
    stage_operation: Uuid,
    staged_manifest_sha256: String,
    ciphertext_sha256: [u8; 32],
    command_id: [u8; 16],
    command_digest: [u8; 32],
    original_receipt_table: Option<TableRef>,
}

pub(super) fn scratch_name(operation: Uuid) -> String {
    format!("checkpoint-stage-{operation}")
}

impl Selection {
    fn source_terminal<'a>(
        &self,
        source: &'a DeploymentRecord,
        authority: Uuid,
        capture: Uuid,
    ) -> Option<&'a CheckpointTransferTerminal> {
        let expected_scratch = scratch_name(self.stage_operation);
        source
            .checkpoint_transfer_terminal
            .as_deref()
            .filter(|terminal| {
                let stage = &terminal.fence.destination;
                source.id == authority.to_string()
                    && stage.transfer_only
                    && source.lifecycle == Lifecycle::Restoring
                    && source.holder.is_none()
                    && source.checkpoint_stage.is_none()
                    && source.epoch == terminal.fence.fenced_writer_epoch
                    && source.incarnation == stage.source_incarnation
                    && source.log_epoch == stage.source_log_epoch
                    && source.checkpoint == Some(stage.checkpoint)
                    && source.checkpoint_transfer_fence.as_deref() == Some(&terminal.fence)
                    && stage.intent.source == authority
                    && stage.intent.capture == capture
                    && stage.intent.operation == self.stage_operation
                    && stage.intent.destination == self.stage_operation
                    && stage.intent.name == expected_scratch
                    && stage.intent.org == source.org
                    && stage.staged_manifest_sha256.as_deref()
                        == Some(self.staged_manifest_sha256.as_str())
                    && stage
                        .intent
                        .classic_migration
                        .is_some_and(|migration| migration.destination == self.destination)
            })
    }

    fn matches_scratch(
        &self,
        scratch: &DeploymentRecord,
        terminal: &CheckpointTransferTerminal,
    ) -> bool {
        let stage = &terminal.fence.destination;
        scratch.id == self.stage_operation.to_string()
            && scratch.lineage == scratch.id
            && scratch.incarnation == 1
            && scratch.lifecycle == Lifecycle::Restoring
            && scratch.parent.is_none()
            && scratch.holder.is_none()
            && scratch.name == stage.intent.name
            && scratch.org == stage.intent.org
            && scratch.checkpoint_stage.as_deref() == Some(stage)
            && scratch.checkpoint_transfer_fence.as_deref() == Some(&terminal.fence)
            && scratch
                .checkpoint_transfer_bootstrap
                .as_deref()
                .is_some_and(|bootstrap| {
                    &bootstrap.terminal == terminal
                        && bootstrap.ciphertext_sha256 == self.ciphertext_sha256
                })
    }
}

pub(super) enum Operation {
    Stage,
    Install { projection_sha256: [u8; 32] },
    Status { classic_cut: Option<TransactionKey> },
    OriginalReceipt { id: EntityId },
}

#[derive(Serialize)]
pub(super) struct Evidence {
    name: String,
    authority: Uuid,
    capture: Uuid,
    source_incarnation: u64,
    source_log_epoch: i32,
    source_through: i64,
    replica_through: i64,
    terminal: TransactionKey,
    terminal_sha256: [u8; 32],
    request_sha256: [u8; 32],
    selection: Selection,
    candidate: Option<ClassicCandidate>,
    projection: Option<ClassicProjection>,
    original_receipt: Option<ReceiptHistory>,
    mapped_cut: Option<(TransactionKey, rtdb_types::rtdb_v1::Basis)>,
}

impl RecoveryService {
    pub(super) async fn classic_destination(
        &self,
        directory: Arc<dyn Directory>,
        source: DeploymentRecord,
        authority: Uuid,
        capture: Uuid,
        selection: Selection,
        operation: Operation,
    ) -> Result<Reply> {
        let Some(runtime) = self.classic_destination.as_ref() else {
            return Ok(Reply::Refused);
        };
        if selection.destination != authority
            || selection.stage_operation.is_nil()
            || selection.stage_operation == authority
            || selection.command_id == [0; 16]
            || selection.command_digest == [0; 32]
            || selection.ciphertext_sha256 == [0; 32]
            || !valid_manifest(&selection.staged_manifest_sha256)
            || selection
                .original_receipt_table
                .as_ref()
                .is_some_and(|table| table.schema != "public" || table.table.is_empty())
        {
            return Ok(Reply::Refused);
        }
        let terminal = selection
            .source_terminal(&source, authority, capture)
            .ok_or_else(conflict)?;
        if !directory
            .deployment(&selection.stage_operation.to_string())
            .await?
            .is_some_and(|scratch| selection.matches_scratch(&scratch, terminal))
        {
            return Err(conflict());
        }
        let terminal_sha256 = archive::sha256(&archive::encode(terminal)?);
        let request_sha256 = archive::sha256(&archive::encode(&(
            "rtdb.multi/classic-destination-request/v1",
            &source.name,
            &source.org,
            authority,
            capture,
            terminal_sha256,
            &selection,
        ))?);
        let mut evidence = Evidence {
            name: source.name,
            authority,
            capture,
            source_incarnation: source.incarnation,
            source_log_epoch: source.log_epoch,
            source_through: terminal.source_through,
            replica_through: terminal.replica_through,
            terminal: terminal.basis,
            terminal_sha256,
            request_sha256,
            selection,
            candidate: None,
            projection: None,
            original_receipt: None,
            mapped_cut: None,
        };
        let candidate = runtime
            .classic_candidate_status(&evidence.name, authority, request_sha256)
            .await?;
        if let Operation::Status { classic_cut } = operation {
            if let Some(cut) = classic_cut
                && candidate
                    .as_ref()
                    .is_some_and(|candidate| candidate.installation.is_some())
            {
                if !cut.at_or_before(evidence.terminal) {
                    return Err(conflict());
                }
                let basis = runtime
                    .classic_candidate_cut(&evidence.name, authority, request_sha256, cut)
                    .await?;
                evidence.mapped_cut = Some((cut, basis));
            }
            evidence.candidate = candidate;
            return Ok(Reply::ClassicMigrationDestination { evidence });
        }
        if let Operation::Install { projection_sha256 } = &operation {
            let selected = candidate
                .as_ref()
                .filter(|candidate| candidate.projection_sha256 == *projection_sha256)
                .ok_or_else(conflict)?;
            if selected.installation.is_some() {
                evidence.candidate = candidate;
                return Ok(Reply::ClassicMigrationDestination { evidence });
            }
        }
        let selection = &evidence.selection;
        let selector = StageSelector {
            destination: selection.stage_operation,
            operation: selection.stage_operation,
            manifest_sha256: selection.staged_manifest_sha256.clone(),
            source: Some((authority, capture, self.org.clone())),
            transfer_only: true,
        };
        let read = match &operation {
            Operation::OriginalReceipt { id } => {
                let table = selection
                    .original_receipt_table
                    .clone()
                    .ok_or_else(conflict)?;
                ClassicRead::Receipt(ReceiptLookup::new(table, id.clone()))
            }
            _ => ClassicRead::Projection(request_sha256),
        };
        let (_, prepared, original_receipt) = Box::pin(archive::read_classic_migration(
            directory,
            &self.storage,
            self.keys.as_ref(),
            selector,
            authority,
            selection.ciphertext_sha256,
            self.hosted_task.clone(),
            read,
        ))
        .await?;
        if let Some(receipt) = original_receipt {
            evidence.original_receipt = Some(receipt);
            evidence.candidate = candidate;
            return Ok(Reply::ClassicMigrationDestination { evidence });
        }
        let prepared = prepared.ok_or_else(conflict)?;
        if let Some(transactions) = &prepared.transactions {
            let projection = prepared.proof.sha256;
            evidence.candidate = Some(match operation {
                Operation::Stage => {
                    runtime
                        .stage_classic_candidate(
                            &evidence.name,
                            authority,
                            request_sha256,
                            projection,
                        )
                        .await?
                }
                Operation::Install { projection_sha256 } if projection == projection_sha256 => {
                    runtime
                        .install_classic_candidate(
                            &evidence.name,
                            authority,
                            request_sha256,
                            projection,
                            transactions,
                        )
                        .await?
                }
                Operation::Install { .. } => return Err(conflict()),
                Operation::Status { .. } | Operation::OriginalReceipt { .. } => unreachable!(),
            });
        } else if candidate.is_some() {
            return Err(conflict());
        }
        if let Some(installed) = evidence
            .candidate
            .as_ref()
            .and_then(|candidate| candidate.installation.as_ref())
            && (installed.basis != prepared.proof.native_basis
                || installed.system_time != prepared.proof.terminal.system_time
                || Some(installed.checkpoint_sha256) != prepared.proof.checkpoint_sha256)
        {
            return Err(conflict());
        }
        evidence.projection = Some(prepared.proof);
        Ok(Reply::ClassicMigrationDestination { evidence })
    }
}

fn conflict() -> Anomaly {
    Anomaly::new(
        rtdb_types::AnomalyCategory::Conflict,
        "rtdb.multi/classic-destination",
        "The request differs from the sealed Classic migration",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_directory::{
        CellMedia, Checkpoint, CheckpointStage, CheckpointStageIntent, CheckpointTransferBootstrap,
        CheckpointTransferFence, ClassicMigration, ClassicMigrationSource,
    };
    use rtdb_types::{Timestamp, TxId};

    #[test]
    fn named_classic_tenant_selects_its_separate_deterministic_scratch() {
        let mut source = DeploymentRecord::new("tenant-orders", "cell", vec![], 0);
        let authority = source.id.parse().unwrap();
        let attempt = Uuid::new_v4();
        let capture = Uuid::new_v4();
        source.lifecycle = Lifecycle::Restoring;
        source.epoch = 9;
        source.log_epoch = 2;
        let checkpoint = Checkpoint {
            block: 1,
            boundary: 5,
            source: 6,
            system_time: 12,
            root_sha256: Some([1; 32]),
        };
        source.checkpoint = Some(checkpoint);
        let stage = CheckpointStage {
            intent: CheckpointStageIntent {
                operation: attempt,
                source: authority,
                capture,
                source_manifest_sha256: "original".into(),
                destination: attempt,
                name: scratch_name(attempt),
                org: source.org.clone(),
                classic_migration: Some(ClassicMigration {
                    source: ClassicMigrationSource {
                        incarnation: source.incarnation,
                        log_epoch: source.log_epoch,
                        writer_epoch: 8,
                    },
                    destination: authority,
                }),
            },
            source_incarnation: source.incarnation,
            source_log_epoch: source.log_epoch,
            checkpoint,
            media: CellMedia {
                objects: [2; 32],
                keys: "keys".into(),
            },
            transfer_only: true,
            staged_manifest_sha256: Some("verified-stage".into()),
        };
        let fence = CheckpointTransferFence {
            destination: stage.clone(),
            source_writer_epoch: 8,
            fenced_writer_epoch: 9,
            drained_source_through: Some(6),
        };
        let terminal = CheckpointTransferTerminal {
            fence: fence.clone(),
            replica_through: 5,
            source_through: 6,
            basis: TransactionKey {
                tx_id: TxId(6),
                system_time: Timestamp(12),
            },
            placed_debt: None,
            replica_sha256: None,
            lifetime_floors_sha256: [3; 32],
        };
        source.checkpoint_transfer_fence = Some(Box::new(fence.clone()));
        source.checkpoint_transfer_terminal = Some(Box::new(terminal.clone()));
        let mut scratch =
            crate::admin::installation_record(attempt, &scratch_name(attempt), "cell").unwrap();
        scratch.lifecycle = Lifecycle::Restoring;
        scratch.checkpoint_stage = Some(Box::new(stage));
        scratch.checkpoint_transfer_fence = Some(Box::new(fence));
        scratch.checkpoint_transfer_bootstrap = Some(Box::new(CheckpointTransferBootstrap {
            terminal: terminal.clone(),
            ciphertext_sha256: [4; 32],
        }));
        let selection = Selection {
            destination: authority,
            stage_operation: attempt,
            staged_manifest_sha256: "verified-stage".into(),
            ciphertext_sha256: [4; 32],
            command_id: [5; 16],
            command_digest: [6; 32],
            original_receipt_table: None,
        };

        assert_eq!(source.name, "tenant-orders");
        assert_ne!(source.name, scratch.name);
        assert_eq!(
            selection.source_terminal(&source, authority, capture),
            Some(&terminal)
        );
        assert!(selection.matches_scratch(&scratch, &terminal));
        assert!(
            selection
                .source_terminal(&source, attempt, capture)
                .is_none()
        );
        scratch.name = source.name.clone();
        assert!(!selection.matches_scratch(&scratch, &terminal));
        let wrong = source.checkpoint_transfer_terminal.as_mut().unwrap();
        wrong.fence.destination.intent.name = source.name.clone();
        source.checkpoint_transfer_fence = Some(Box::new(wrong.fence.clone()));
        assert!(
            selection
                .source_terminal(&source, authority, capture)
                .is_none()
        );
    }
}
