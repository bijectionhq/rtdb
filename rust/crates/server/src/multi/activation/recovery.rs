use super::{
    ActivationAuthority, Binding, Publication, RECOVERY_INTENT_PROTOCOL, State, VerifiedSuccessor,
    clock, conflict, journal_error, unavailable,
};

use journal_contract::{Epoch, Journal, JournalError, Lsn, StreamId, WriterSessionId};
use rtdb_types::{Anomaly, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum LossPolicy {
    Lossless,
    AcceptUnknownTail,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Operation {
    pub(in crate::multi) predecessor: Binding,
    pub(in crate::multi) operation_id: Uuid,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Intent {
    pub(in crate::multi) operation: Operation,
    pub(in crate::multi) destination_cell: Option<Uuid>,
    pub(in crate::multi) loss_policy: LossPolicy,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SourceCut {
    pub(super) lsn: u64,
    pub(super) position: u64,
    pub(super) system_time: i64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Installation {
    pub(super) manifest_digest: [u8; 32],
    pub(super) source_cut: SourceCut,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Pending {
    intent: Intent,
    installation: Option<Installation>,
    final_source_lsn: Option<Lsn>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Completed {
    loss_policy: LossPolicy,
    source_cut: SourceCut,
    final_source_lsn: Option<Lsn>,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(tag = "stage", rename_all = "kebab-case")]
pub(crate) enum Status {
    Pending {
        intent: Intent,
        candidate: Uuid,
        grants_closed: bool,
        installation: Option<Installation>,
        final_source_lsn: Option<Lsn>,
    },
    Published {
        intent: Intent,
        binding: Binding,
        installation: Installation,
        final_source_lsn: Option<Lsn>,
    },
}

impl State {
    pub(super) fn valid_recovery(&self) -> bool {
        if self.version < RECOVERY_INTENT_PROTOCOL {
            return self.recovery.is_none()
                && self.superseded_recoveries.is_empty()
                && self
                    .publication
                    .is_none_or(|publication| publication.recovery.is_none());
        }
        let pending_valid = self.recovery.is_some_and(|pending| {
            pending.intent.operation.predecessor == self.binding
                && !pending.intent.operation.operation_id.is_nil()
                && !self
                    .superseded_recoveries
                    .contains(&pending.intent.operation.operation_id)
                && pending
                    .intent
                    .destination_cell
                    .is_none_or(|cell| !cell.is_nil())
                && (pending.final_source_lsn.is_none() || self.grants_closed)
                && pending.installation.is_none_or(|installation| {
                    installation.source_cut.lsn != 0
                        && match pending.intent.loss_policy {
                            LossPolicy::Lossless => {
                                pending.final_source_lsn == Some(Lsn(installation.source_cut.lsn))
                            }
                            LossPolicy::AcceptUnknownTail => pending.final_source_lsn.is_none(),
                        }
                })
        });
        let publication_valid = self.publication.is_some_and(|publication| {
            if let Some(completed) = publication.recovery {
                completed.source_cut.lsn != 0
                    && publication.predecessor.revision.checked_add(1)
                        == Some(self.binding.revision)
                    && match completed.loss_policy {
                        LossPolicy::Lossless => {
                            completed.final_source_lsn == Some(Lsn(completed.source_cut.lsn))
                        }
                        LossPolicy::AcceptUnknownTail => completed.final_source_lsn.is_none(),
                    }
            } else {
                pending_valid
            }
        });
        (self.recovery.is_none() || pending_valid)
            && (self.recovery.is_some() || self.superseded_recoveries.is_empty())
            && (self.publication.is_none() || publication_valid)
            && (pending_valid || publication_valid)
    }

    fn pending(&self, operation: Operation) -> Result<Pending> {
        self.require(operation.predecessor)?;
        self.recovery
            .filter(|pending| pending.intent.operation == operation)
            .ok_or_else(conflict)
    }

    fn publication_status(&self, operation: Operation) -> Result<Option<Status>> {
        let Some(publication) = self
            .publication
            .filter(|prior| prior.operation == operation.operation_id)
        else {
            return Ok(None);
        };
        if operation.predecessor != publication.predecessor {
            return Err(conflict());
        }
        let completed = publication.recovery.ok_or_else(conflict)?;
        Ok(Some(Status::Published {
            intent: Intent {
                operation,
                destination_cell: publication.destination_cell,
                loss_policy: completed.loss_policy,
            },
            binding: self.binding,
            installation: Installation {
                manifest_digest: publication.manifest_digest,
                source_cut: completed.source_cut,
            },
            final_source_lsn: completed.final_source_lsn,
        }))
    }
}

impl ActivationAuthority {
    pub(crate) async fn begin_recovery(
        &self,
        name: &str,
        intent: Intent,
        supersedes: Option<Uuid>,
    ) -> Result<Status> {
        let operation = intent.operation;
        self.candidate(name, intent)?;
        let state = self
            .change(self.stream_id(name)?, |state| {
                let state = state.ok_or_else(unavailable)?;
                if let Some(Status::Published {
                    intent: published, ..
                }) = state.publication_status(operation)?
                {
                    if published != intent {
                        return Err(conflict());
                    }
                    return Ok(None);
                }
                state.require(operation.predecessor)?;
                if state
                    .superseded_recoveries
                    .contains(&operation.operation_id)
                {
                    return Err(conflict());
                }
                let mut next = state.clone();
                let mut final_source_lsn = None;
                if let Some(pending) = state.recovery {
                    if pending.intent == intent {
                        return Ok(None);
                    }
                    if supersedes != Some(pending.intent.operation.operation_id)
                        || operation.operation_id == pending.intent.operation.operation_id
                        || (pending.final_source_lsn.is_some()
                            && intent.loss_policy != LossPolicy::Lossless)
                    {
                        return Err(conflict());
                    }
                    next.superseded_recoveries
                        .insert(pending.intent.operation.operation_id);
                    if intent.loss_policy == LossPolicy::Lossless {
                        final_source_lsn = pending.final_source_lsn;
                    }
                } else if supersedes.is_some() {
                    return Err(conflict());
                }
                next.version = RECOVERY_INTENT_PROTOCOL;
                next.recovery = Some(Pending {
                    intent,
                    installation: None,
                    final_source_lsn,
                });
                Ok(Some(next))
            })
            .await?;
        self.recovery_status_at(name, &state, operation)
    }

    pub(crate) async fn recovery_status(&self, name: &str, operation: Operation) -> Result<Status> {
        let (_, state) = self.read(self.stream_id(name)?).await?;
        self.recovery_status_at(name, &state.ok_or_else(unavailable)?, operation)
    }

    fn recovery_status_at(
        &self,
        name: &str,
        state: &State,
        operation: Operation,
    ) -> Result<Status> {
        if let Some(status) = state.publication_status(operation)? {
            return Ok(status);
        }
        let pending = state.pending(operation)?;
        Ok(Status::Pending {
            intent: pending.intent,
            candidate: self.candidate(name, pending.intent)?,
            grants_closed: state.grants_closed,
            installation: pending.installation,
            final_source_lsn: pending.final_source_lsn,
        })
    }

    pub(crate) async fn freeze_recovery(&self, name: &str, operation: Operation) -> Result<Status> {
        let state = self
            .change(self.stream_id(name)?, |state| {
                let state = state.ok_or_else(unavailable)?;
                if state.publication_status(operation)?.is_some() {
                    return Ok(None);
                }
                state.pending(operation)?;
                if state.grants_closed {
                    return Ok(None);
                }
                let mut next = state.clone();
                next.grants_closed = true;
                Ok(Some(next))
            })
            .await?;
        self.recovery_status_at(name, &state, operation)
    }

    pub(crate) async fn record_recovery_installation(
        &self,
        name: &str,
        operation: Operation,
        installed: VerifiedSuccessor,
    ) -> Result<Status> {
        let installation = Installation {
            manifest_digest: installed.manifest_digest,
            source_cut: installed.source_cut,
        };
        if installation.source_cut.lsn == 0 {
            return Err(conflict());
        }
        let state = self.change(self.stream_id(name)?, |state| {
            let state = state.ok_or_else(unavailable)?;
            let published = state.publication_status(operation)?;
            let intent = if let Some(Status::Published { intent, .. }) = published {
                intent
            } else {
                state.pending(operation)?.intent
            };
            if installed.predecessor != operation.predecessor.incarnation
                || installed.successor != self.candidate(name, intent)?
                || installed.destination_cell != intent.destination_cell
                || installed.activation_scope != Some(rtdb_db::ActivationScope::for_cell(
                    self.identity, name, intent.destination_cell,
                )?)
            {
                return Err(conflict());
            }
            if let Some(Status::Published { installation: prior, .. }) = published {
                return same_installation(prior, installation);
            }
            let mut pending = state.pending(operation)?;
            if let Some(prior) = pending.installation {
                return same_installation(prior, installation);
            }
            if intent.loss_policy == LossPolicy::Lossless
                && pending.final_source_lsn != Some(Lsn(installation.source_cut.lsn))
            {
                return Err(Anomaly::incorrect(
                    "rtdb/activation-final-source-cut",
                    "Lossless recovery requires an installation of the sealed predecessor's final position",
                ));
            }
            pending.installation = Some(installation);
            let mut next = state.clone();
            next.recovery = Some(pending);
            Ok(Some(next))
        }).await?;
        self.recovery_status_at(name, &state, operation)
    }

    pub(super) async fn seal_recovery_source(
        &self,
        name: &str,
        operation: Operation,
        source: &dyn Journal,
        source_cell: Option<Uuid>,
    ) -> Result<Status> {
        self.access.require_operator()?;
        let stream = self.stream_id(name)?;
        let (_, state) = self.read(stream).await?;
        let state = state.ok_or_else(unavailable)?;
        if let Some(status) = state.publication_status(operation)? {
            return Ok(status);
        }
        let pending = state.pending(operation)?;
        if !state.grants_closed
            || pending.intent.loss_policy != LossPolicy::Lossless
            || state.destination_cell() != source_cell
        {
            return Err(conflict());
        }
        if pending.final_source_lsn.is_some() {
            return self.recovery_status_at(name, &state, operation);
        }
        clock::wait_out(
            self.clock.as_ref(),
            clock::drain_window(state.grants_end.is_some()),
        )
        .await?;
        // The source verifier has opened this commissioned predecessor. A new quorum
        // writer fence orders all still in-flight writer requests before this cut.
        let final_source_lsn = sealed_head(source, operation).await?;
        let state = self
            .change(stream, |state| {
                let state = state.ok_or_else(unavailable)?;
                if let Some(Status::Published {
                    final_source_lsn: prior,
                    ..
                }) = state.publication_status(operation)?
                {
                    return if prior == Some(final_source_lsn) {
                        Ok(None)
                    } else {
                        Err(conflict())
                    };
                }
                let mut pending = state.pending(operation)?;
                if !state.grants_closed
                    || pending.intent.loss_policy != LossPolicy::Lossless
                    || state.destination_cell() != source_cell
                {
                    return Err(conflict());
                }
                if let Some(prior) = pending.final_source_lsn {
                    return if prior == final_source_lsn {
                        Ok(None)
                    } else {
                        Err(conflict())
                    };
                }
                pending.final_source_lsn = Some(final_source_lsn);
                let mut next = state.clone();
                next.recovery = Some(pending);
                Ok(Some(next))
            })
            .await?;
        self.recovery_status_at(name, &state, operation)
    }

    pub(crate) async fn publish_recovery(
        &self,
        name: &str,
        operation: Operation,
    ) -> Result<Status> {
        self.access.require_operator()?;
        let stream = self.stream_id(name)?;
        let (_, frozen) = self.read(stream).await?;
        let frozen = frozen.ok_or_else(unavailable)?;
        if let Some(status) = frozen.publication_status(operation)? {
            return Ok(status);
        }
        let pending = frozen.pending(operation)?;
        if !frozen.grants_closed {
            return Err(conflict());
        }
        let installation = pending.installation.ok_or_else(|| {
            Anomaly::incorrect(
                "rtdb/activation-installation-required",
                "Recovery requires a verified installation recorded by the authority",
            )
        })?;
        let successor = self.candidate(name, pending.intent)?;
        let publication = Publication {
            operation: operation.operation_id,
            predecessor: operation.predecessor,
            manifest_digest: installation.manifest_digest,
            destination_cell: pending.intent.destination_cell,
            recovery: Some(Completed {
                loss_policy: pending.intent.loss_policy,
                source_cut: installation.source_cut,
                final_source_lsn: pending.final_source_lsn,
            }),
        };
        clock::wait_out(
            self.clock.as_ref(),
            clock::drain_window(frozen.grants_end.is_some()),
        )
        .await?;
        let state = self
            .change(stream, |state| {
                let state = state.ok_or_else(unavailable)?;
                if state.already_published(publication, successor)? {
                    return Ok(None);
                }
                if state.pending(operation)? != pending {
                    return Err(conflict());
                }
                self.install_binding(name, state, publication, successor)
                    .map(Some)
            })
            .await?;
        self.recovery_status_at(name, &state, operation)
    }

    fn candidate(&self, name: &str, intent: Intent) -> Result<Uuid> {
        self.successor_incarnation_in_cell(
            name,
            intent.operation.predecessor,
            intent.operation.operation_id,
            intent.destination_cell,
        )
    }
}

async fn sealed_head(source: &dyn Journal, operation: Operation) -> Result<Lsn> {
    let stream = StreamId(operation.predecessor.incarnation.as_u128());
    source
        .require_immutable(stream)
        .await
        .map_err(journal_error)?;
    let mut identity = Sha256::new();
    identity.update(b"rtdb.activation.source-seal.v1");
    identity.update(operation.predecessor.incarnation.as_bytes());
    identity.update(operation.predecessor.revision.to_be_bytes());
    identity.update(operation.operation_id.as_bytes());
    let digest = identity.finalize();
    let session = WriterSessionId(u128::from_be_bytes(digest[..16].try_into().unwrap()));
    let mut expected = Epoch(0);
    for _ in 0..super::ATTEMPTS {
        match source.seal_session(stream, expected, session).await {
            Ok(receipt) if receipt.sealed_head_lsn != Lsn(0) => return Ok(receipt.sealed_head_lsn),
            Ok(_) => return Err(conflict()),
            Err(JournalError::Fenced { current }) if current != expected => {
                // Reopen the original receipt after a crash following the source seal.
                if let Some(previous) = current.0.checked_sub(1) {
                    match source.seal_session(stream, Epoch(previous), session).await {
                        Ok(receipt) if receipt.sealed_head_lsn != Lsn(0) => {
                            return Ok(receipt.sealed_head_lsn);
                        }
                        Ok(_) => return Err(conflict()),
                        Err(JournalError::Fenced { .. }) => (),
                        Err(error) => return Err(journal_error(error)),
                    }
                }
                expected = current;
            }
            Err(error) => return Err(journal_error(error)),
        }
    }
    Err(unavailable())
}

fn same_installation(prior: Installation, supplied: Installation) -> Result<Option<State>> {
    if prior == supplied {
        Ok(None)
    } else {
        Err(conflict())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::{
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        time::Duration,
    };
    use stream_logkeeper::stream::consensus::{CLUSTER_VERSION_CURRENT, testing::LocalCluster};

    #[tokio::test]
    async fn lossless_recovery_seals_source_before_accepting_cut() {
        const PRINCIPAL: &str = "urn:logkeeper:client:lossless-recovery";
        let mut keepers = LocalCluster::start_replicated_catalog(&[PRINCIPAL])
            .await
            .unwrap();
        let config = keepers.journal_config(64 << 20, 4 << 20);
        let journal = keepers.journal(PRINCIPAL, &config).unwrap();
        journal
            .client()
            .finalize_version(CLUSTER_VERSION_CURRENT)
            .await
            .unwrap();
        let data = tempfile::tempdir().unwrap();
        let identity = Uuid::new_v4();
        let mut authority =
            ActivationAuthority::open_local(&data.path().join("authority"), identity).unwrap();
        let ticks = AtomicU64::new(0);
        Arc::get_mut(&mut authority).unwrap().clock =
            Arc::new(move || Some(Duration::from_secs(ticks.fetch_add(6, Ordering::SeqCst))));

        let predecessor = authority.bootstrap("xtdb", Uuid::new_v4()).await.unwrap();
        let stream = StreamId(predecessor.incarnation.as_u128());
        journal.create_immutable(stream).await.unwrap();
        let writer = journal
            .seal_session(stream, Epoch(0), WriterSessionId(1))
            .await
            .unwrap();
        journal
            .append_session_batch(writer.fence, Lsn(1), vec![Bytes::from_static(b"first")])
            .await
            .unwrap();

        let operation = Operation {
            predecessor,
            operation_id: Uuid::from_u128(writer.fence.writer_session_id.0),
        };
        let intent = Intent {
            operation,
            destination_cell: None,
            loss_policy: LossPolicy::Lossless,
        };
        let candidate = match authority
            .begin_recovery("xtdb", intent, None)
            .await
            .unwrap()
        {
            Status::Pending { candidate, .. } => candidate,
            other => panic!("expected a pending recovery, got {other:?}"),
        };
        let installed = |lsn| VerifiedSuccessor {
            predecessor: predecessor.incarnation,
            successor: candidate,
            manifest_digest: [7; 32],
            destination_cell: None,
            activation_scope: Some(rtdb_db::ActivationScope::new(identity, "xtdb").unwrap()),
            source_cut: SourceCut {
                lsn,
                position: 1,
                system_time: 2,
            },
        };
        assert_eq!(
            authority
                .record_recovery_installation("xtdb", operation, installed(1))
                .await
                .unwrap_err()
                .code,
            "rtdb/activation-final-source-cut"
        );
        assert_eq!(
            authority
                .seal_recovery_source("xtdb", operation, &journal, None)
                .await
                .unwrap_err()
                .code,
            "rtdb/activation-conflict"
        );
        authority.freeze_recovery("xtdb", operation).await.unwrap();
        assert_eq!(
            authority
                .publish_recovery("xtdb", operation)
                .await
                .unwrap_err()
                .code,
            "rtdb/activation-installation-required"
        );
        // An admitted write can settle during the freeze window before the independent seal.
        journal
            .append_session_batch(writer.fence, Lsn(2), vec![Bytes::from_static(b"last")])
            .await
            .unwrap();
        let sealed = authority
            .seal_recovery_source("xtdb", operation, &journal, None)
            .await
            .unwrap();
        assert!(matches!(
            sealed,
            Status::Pending {
                final_source_lsn: Some(Lsn(2)),
                ..
            }
        ));
        assert!(matches!(
            journal
                .append_session_batch(writer.fence, Lsn(3), vec![Bytes::from_static(b"late")])
                .await,
            Err(JournalError::Fenced { .. })
        ));
        assert_eq!(
            authority
                .record_recovery_installation("xtdb", operation, installed(1))
                .await
                .unwrap_err()
                .code,
            "rtdb/activation-final-source-cut"
        );

        let replacement = Operation {
            operation_id: Uuid::new_v4(),
            ..operation
        };
        let replacement_intent = Intent {
            operation: replacement,
            ..intent
        };
        let replacement_candidate = match authority
            .begin_recovery("xtdb", replacement_intent, Some(operation.operation_id))
            .await
            .unwrap()
        {
            Status::Pending {
                candidate,
                grants_closed: true,
                final_source_lsn: Some(Lsn(2)),
                ..
            } => candidate,
            other => panic!("expected the sealed cut to survive supersession, got {other:?}"),
        };
        assert_eq!(
            authority
                .recovery_status("xtdb", operation)
                .await
                .unwrap_err()
                .code,
            "rtdb/activation-conflict"
        );
        let exact = VerifiedSuccessor {
            successor: replacement_candidate,
            ..installed(2)
        };
        authority
            .record_recovery_installation("xtdb", replacement, exact)
            .await
            .unwrap();
        let published = authority
            .publish_recovery("xtdb", replacement)
            .await
            .unwrap();
        assert!(matches!(
            published,
            Status::Published { binding, final_source_lsn: Some(Lsn(2)), .. }
                if binding.incarnation == replacement_candidate
        ));
        assert!(matches!(
            authority
                .publish_recovery("xtdb", replacement)
                .await
                .unwrap(),
            Status::Published {
                final_source_lsn: Some(Lsn(2)),
                ..
            }
        ));
        keepers.shutdown().await.unwrap();
    }
}
