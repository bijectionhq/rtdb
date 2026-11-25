use std::sync::Arc;

use rtdb_directory::{CheckpointTransferTerminal, DeploymentRecord, Directory, Lifecycle};
use rtdb_server::multi::{
    ClassicCandidate,
    classic_ingress::{
        Authorization, Binding, HeldReady, MAX_CHECKPOINT_BYTES, Operation as CandidateOperation,
        Response,
    },
};

mod relay;
pub(crate) use relay::Relay;
use rtdb_types::{Anomaly, EntityId, Result, TableRef, TransactionKey};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{RecoveryService, Reply, classic_provider, valid_manifest};
use crate::archive::{
    self, ApplicationEvidence, ClassicProjection, ClassicRead, PointHistoryRead, ReceiptHistory,
    ReceiptLookup, ReceiptLookups, StageSelector,
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
    Prepare {
        begin_sha256: [u8; 32],
    },
    Stage {
        authorization: Option<Authorization>,
    },
    Install {
        projection_sha256: [u8; 32],
        authorization: Option<Authorization>,
    },
    Status {
        classic_cut: Option<TransactionKey>,
        authorization: Option<Authorization>,
    },
    HeldReady {
        authorization: Authorization,
        record: bool,
    },
    OriginalReceipt {
        id: EntityId,
        coverage: Option<CoveragePoint>,
    },
    Application {
        candidate_binding: Binding,
        classic_cut: TransactionKey,
        points: Option<PointHistoryRead>,
        provider: Option<classic_provider::Population>,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CoveragePoint {
    table: TableRef,
    id: EntityId,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    candidate_binding: Option<Binding>,
    #[serde(skip_serializing_if = "Option::is_none")]
    candidate_blocker: Option<&'static str>,
    projection: Option<ClassicProjection>,
    original_receipt: Option<ReceiptHistory>,
    #[serde(skip_serializing_if = "Option::is_none")]
    original_receipt_coverage: Option<ReceiptHistory>,
    mapped_cut: Option<(TransactionKey, rtdb_types::rtdb_v1::Basis)>,
    #[serde(skip_serializing_if = "Option::is_none")]
    held_ready: Option<Box<HeldReady>>,
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
            name: source.name.clone(),
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
            candidate_binding: None,
            candidate_blocker: None,
            projection: None,
            original_receipt: None,
            original_receipt_coverage: None,
            mapped_cut: None,
            held_ready: None,
        };
        if let Operation::OriginalReceipt { id, coverage } = operation {
            let table = evidence
                .selection
                .original_receipt_table
                .clone()
                .ok_or_else(conflict)?;
            if coverage
                .as_ref()
                .is_some_and(|point| point.table.schema != "public" || point.table.table.is_empty())
            {
                return Err(conflict());
            }
            let lookups = ReceiptLookups::new(
                ReceiptLookup::new(table, id),
                coverage.map(|point| ReceiptLookup::new(point.table, point.id)),
            )?;
            let (_, _, readback) = Box::pin(archive::read_classic_migration(
                directory,
                &self.storage,
                self.keys.as_ref(),
                self.classic_stage_selector(&evidence),
                authority,
                evidence.selection.ciphertext_sha256,
                self.hosted_task.clone(),
                ClassicRead::Receipt(lookups),
            ))
            .await?;
            let readback = readback.ok_or_else(conflict)?;
            evidence.original_receipt = Some(readback.receipt);
            evidence.original_receipt_coverage = readback.coverage;
            return Ok(Reply::ClassicMigrationDestination { evidence });
        }
        if let Operation::Application { candidate_binding, classic_cut, points, provider } = operation {
            let Some(layout) = &self.classic_application_layout else {
                return Ok(Reply::Refused);
            };
            if !evidence.matches_binding(&candidate_binding)
                || candidate_binding.begin_sha256 == [0; 32]
                || candidate_binding.catalog.is_nil()
                || candidate_binding.catalog == authority
                || self.classic_destination.as_ref().is_some_and(|relay| relay.catalog() != candidate_binding.catalog)
                || !classic_cut.at_or_before(evidence.terminal)
                || points.as_ref().is_some_and(|points| !layout.admits(points)
                    || evidence.selection.original_receipt_table.as_ref().is_some_and(|table| points.selects(table)))
            {
                return Err(conflict());
            }
            let provider_release = match &provider {
                Some(classic_provider::Population::FamilyRelease { registration }) => {
                    let registered = registration.registration()?;
                    let history = rtdb_types::PlacedAttemptScope::new(&source.stream("replica"), terminal.fence.source_writer_epoch)?;
                    if registered.owner().source_history != history.history_fingerprint()
                        || registered.owner().deployment != authority
                        || source.storage_namespaces.is_none_or(|namespaces| namespaces.keys == [0; 32])
                    {
                        return Err(conflict());
                    }
                    Some(registration.clone())
                }
                Some(classic_provider::Population::LocalCatalog) if source.classic_catalog_drain.as_ref().is_none_or(|drain| drain.drained_at.is_none()) => {
                    return Ok(Reply::Deferred);
                }
                _ => None,
            };
            let (_, prepared, _) = Box::pin(archive::read_classic_migration(
                directory, &self.storage, self.keys.as_ref(), self.classic_stage_selector(&evidence),
                authority, evidence.selection.ciphertext_sha256, self.hosted_task.clone(),
                ClassicRead::Application {
                    request: request_sha256, begin: candidate_binding.begin_sha256, cut: classic_cut,
                    points, provider_release, layout: Arc::clone(layout),
                },
            )).await?;
            let prepared = prepared.ok_or_else(conflict)?;
            let checkpoint = prepared.checkpoint.as_ref().ok_or_else(conflict)?;
            if prepared.proof.sha256 != candidate_binding.projection_sha256
                || prepared.proof.checkpoint_sha256 != Some(candidate_binding.checkpoint_sha256)
                || prepared.proof.native_basis != candidate_binding.basis
                || checkpoint.len() != candidate_binding.checkpoint_bytes as usize
                || checkpoint.len() > MAX_CHECKPOINT_BYTES
            {
                return Err(conflict());
            }
            let application = prepared.application.ok_or_else(conflict)?;
            if let Some(provider) = provider {
                let ApplicationEvidence::Closure(application) = application else { return Err(conflict()); };
                let stage = &terminal.fence.destination;
                let migration = stage.intent.classic_migration.ok_or_else(conflict)?;
                if migration.source.incarnation != source.incarnation || migration.source.log_epoch != source.log_epoch
                    || migration.source.writer_epoch != terminal.fence.source_writer_epoch
                {
                    return Err(conflict());
                }
                let provider_terminal = match provider {
                    classic_provider::Population::LocalCatalog => {
                        let drain = source.classic_catalog_drain.as_ref().ok_or_else(conflict)?;
                        let cohort = drain.cohort.clone();
                        let drained_at = drain.drained_at.ok_or_else(conflict)?;
                        if source.version < drained_at || drained_at <= cohort.fenced_at
                            || source.storage_namespaces != Some(cohort.namespaces)
                            || source.storage_prefix() != cohort.residence
                        {
                            return Err(conflict());
                        }
                        classic_provider::Terminal::LocalCatalog(classic_provider::CatalogDrain {
                            fenced_at: cohort.fenced_at, drained_at, residence: cohort.residence,
                            object_namespace: cohort.namespaces.objects, readers: cohort.readers,
                        })
                    }
                    classic_provider::Population::FamilyRelease { registration } => {
                        let namespaces = source.storage_namespaces.ok_or_else(conflict)?;
                        if self.keys.existing_namespace_id().await? != Some(namespaces.keys) { return Err(conflict()); }
                        let (settled_at, settlement_sha256) = application.provider_release.ok_or_else(conflict)?;
                        let registered = registration.registration()?;
                        let region = rtdb_storage::IoRegion::new();
                        let vault = self.keys.assertion_families(region.clone()).ok_or_else(conflict)?;
                        let observed = vault.retained_release_terminal(registered).await;
                        region.close_and_wait().await;
                        let Some((disposition, terminal_sha256)) = observed? else { return Ok(Reply::Deferred); };
                        classic_provider::Terminal::FamilyRelease(classic_provider::ReleaseDrain {
                            locator: registration.locator,
                            address_sha256: archive::sha256(&registration.address),
                            settled_at, settlement_sha256, disposition, terminal_sha256,
                        })
                    }
                };
                return Ok(Reply::ClassicMigrationProviderDrain {
                    selection: evidence.selection, candidate_binding, source: migration.source,
                    fenced_writer_epoch: terminal.fence.fenced_writer_epoch,
                    application: classic_provider::ApplicationClose {
                        classic_cut: application.classic_cut, sql_cutoff: application.sql_cutoff(),
                        epoch: application.source_epoch, assignment: application.source_assignment,
                        custody_sha256: application.custody_sha256, selection_sha256: application.selection_sha256,
                    },
                    terminal: provider_terminal,
                });
            }
            return Ok(match application {
                ApplicationEvidence::Closure(application) => Reply::ClassicMigrationApplicationReadback {
                    selection: evidence.selection, candidate_binding, application,
                },
                ApplicationEvidence::Points(histories) => Reply::ClassicMigrationPointHistories {
                    selection: evidence.selection, candidate_binding, classic_cut,
                    valid_at: histories.valid_at, points: histories.points,
                },
            });
        }
        let Some(relay) = self.classic_destination.as_ref() else {
            return Ok(Reply::Refused);
        };
        let (begin_sha256, authorization) = match operation {
            Operation::Prepare { begin_sha256 } if begin_sha256 != [0; 32] => (begin_sha256, None),
            Operation::Stage {
                authorization: Some(authorization),
            } => {
                relay.verify(&authorization, &CandidateOperation::Stage)?;
                (
                    authorization.grant.binding.begin_sha256,
                    Some(authorization),
                )
            }
            Operation::Install {
                projection_sha256,
                authorization: Some(authorization),
            } if projection_sha256 == authorization.grant.binding.projection_sha256 => {
                relay.verify(&authorization, &CandidateOperation::Install)?;
                (
                    authorization.grant.binding.begin_sha256,
                    Some(authorization),
                )
            }
            Operation::Status {
                classic_cut,
                authorization: Some(authorization),
            } => {
                relay.verify(&authorization, &CandidateOperation::Status { classic_cut })?;
                if !evidence.matches_binding(&authorization.grant.binding) {
                    return Err(conflict());
                }
                evidence.candidate_binding = Some(authorization.grant.binding.clone());
                evidence.observe_candidate(relay.forward(authorization, &[]).await?)?;
                return Ok(Reply::ClassicMigrationDestination { evidence });
            }
            Operation::HeldReady {
                authorization,
                record,
            } => {
                let ready = match (&authorization.grant.operation, record) {
                    (CandidateOperation::HoldReady { ready }, true)
                    | (CandidateOperation::HeldReadyStatus { ready }, false) => ready.clone(),
                    _ => return Ok(Reply::Refused),
                };
                if !evidence.matches_binding(&authorization.grant.binding) {
                    return Err(conflict());
                }
                let binding = authorization.grant.binding.clone();
                evidence.observe_candidate(relay.forward(authorization, &[]).await?)?;
                if evidence.mapped_cut != Some((ready.classic_cut, ready.native_basis))
                    || !evidence.candidate.as_ref().is_some_and(|candidate| {
                        candidate.reserved_at == ready.reserved_at
                            && candidate.installation.as_ref() == Some(&ready.installation)
                    })
                {
                    return Err(conflict());
                }
                match &evidence.held_ready {
                    Some(held)
                        if &held.ready == ready.as_ref()
                            && held.binding_sha256 == binding.identity()?
                            && held.recorded_at.incarnation()
                                == ready.reserved_at.incarnation()
                            && held.recorded_at > ready.reserved_at =>
                    {
                        ()
                    }
                    None if !record => (),
                    _ => return Err(conflict()),
                }
                evidence.candidate_binding = Some(binding);
                return Ok(Reply::ClassicMigrationDestination { evidence });
            }
            _ => return Ok(Reply::Refused),
        };
        if authorization
            .as_ref()
            .is_some_and(|authorization| !evidence.matches_binding(&authorization.grant.binding))
        {
            return Err(conflict());
        }
        let (_, prepared, _) = Box::pin(archive::read_classic_migration(
            directory,
            &self.storage,
            self.keys.as_ref(),
            self.classic_stage_selector(&evidence),
            authority,
            evidence.selection.ciphertext_sha256,
            self.hosted_task.clone(),
            ClassicRead::Projection(request_sha256),
        ))
        .await?;
        let prepared = prepared.ok_or_else(conflict)?;
        if prepared
            .checkpoint
            .as_ref()
            .is_some_and(|checkpoint| checkpoint.len() > MAX_CHECKPOINT_BYTES)
        {
            if authorization.is_some() {
                return Err(conflict());
            }
            evidence.candidate_blocker = Some("checkpoint_exceeds_ingress_bound");
        } else if let Some(checkpoint) = &prepared.checkpoint {
            let binding = Binding {
                begin_sha256,
                request_sha256,
                catalog: relay.catalog(),
                name: evidence.name.clone(),
                destination: authority,
                capture,
                scratch: evidence.selection.stage_operation,
                terminal: evidence.terminal,
                terminal_sha256: evidence.terminal_sha256,
                artifact_sha256: evidence.selection.ciphertext_sha256,
                projection_sha256: prepared.proof.sha256,
                checkpoint_sha256: prepared.proof.checkpoint_sha256.ok_or_else(conflict)?,
                checkpoint_bytes: checkpoint.len() as u32,
                basis: prepared.proof.native_basis,
            };
            if let Some(authorization) = authorization {
                if authorization.grant.binding != binding {
                    return Err(conflict());
                }
                let bytes = if authorization.grant.operation == CandidateOperation::Install {
                    checkpoint.as_ref()
                } else {
                    &[]
                };
                evidence.observe_candidate(relay.forward(authorization, bytes).await?)?;
            }
            evidence.candidate_binding = Some(binding);
        } else if authorization.is_some() {
            return Err(conflict());
        }
        evidence.projection = Some(prepared.proof);
        Ok(Reply::ClassicMigrationDestination { evidence })
    }
    fn classic_stage_selector(&self, evidence: &Evidence) -> StageSelector {
        let selection = &evidence.selection;
        StageSelector {
            destination: selection.stage_operation,
            operation: selection.stage_operation,
            manifest_sha256: selection.staged_manifest_sha256.clone(),
            source: Some((evidence.authority, evidence.capture, self.org.clone())),
            transfer_only: true,
        }
    }
}

impl Evidence {
    fn matches_binding(&self, binding: &Binding) -> bool {
        binding.name == self.name
            && binding.destination == self.authority
            && binding.capture == self.capture
            && binding.scratch == self.selection.stage_operation
            && binding.terminal == self.terminal
            && binding.terminal_sha256 == self.terminal_sha256
            && binding.artifact_sha256 == self.selection.ciphertext_sha256
            && binding.request_sha256 == self.request_sha256
    }

    fn observe_candidate(&mut self, response: Response) -> Result<()> {
        let Response::Observed {
            candidate,
            mapped_cut,
            held_ready,
            ..
        } = response
        else {
            return Err(conflict());
        };
        self.candidate = candidate;
        self.mapped_cut = mapped_cut;
        self.held_ready = held_ready;
        Ok(())
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
