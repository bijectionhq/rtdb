use std::sync::Arc;

use rtdb_directory::{DeploymentRecord, Directory, Lifecycle};
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
        let terminal = source
            .checkpoint_transfer_terminal
            .as_deref()
            .filter(|terminal| {
                let stage = &terminal.fence.destination;
                source.lifecycle == Lifecycle::Restoring
                    && source.holder.is_none()
                    && source.checkpoint_stage.is_none()
                    && source.epoch == terminal.fence.fenced_writer_epoch
                    && source.incarnation == stage.source_incarnation
                    && source.log_epoch == stage.source_log_epoch
                    && source.checkpoint == Some(stage.checkpoint)
                    && source.checkpoint_transfer_fence.as_deref() == Some(&terminal.fence)
                    && stage.intent.source == authority
                    && stage.intent.capture == capture
                    && stage.intent.name == source.name
                    && stage.intent.org == source.org
                    && stage.intent.operation == selection.stage_operation
                    && stage.intent.destination == selection.stage_operation
                    && stage.staged_manifest_sha256.as_deref()
                        == Some(selection.staged_manifest_sha256.as_str())
                    && stage
                        .intent
                        .classic_migration
                        .is_some_and(|migration| migration.destination == selection.destination)
            })
            .ok_or_else(conflict)?;
        let scratch = directory
            .deployment(&selection.stage_operation.to_string())
            .await?
            .ok_or_else(conflict)?;
        if !scratch
            .checkpoint_transfer_bootstrap
            .as_deref()
            .is_some_and(|bootstrap| {
                &bootstrap.terminal == terminal
                    && bootstrap.ciphertext_sha256 == selection.ciphertext_sha256
            })
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
