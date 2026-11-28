use super::{
    ActivationAuthority, Binding, Publication, RECOVERY_HOSTED_CUSTODY_PROTOCOL, RECOVERY_INTENT_PROTOCOL, RECOVERY_JOURNAL_CUSTODY_PROTOCOL,
    RECOVERY_KEY_CUSTODY_PROTOCOL, State, VerifiedSuccessor,
    clock, conflict, journal_custody::RetainedJournal, journal_error, unavailable,
};

use journal_contract::{Epoch, Journal, JournalError, Lsn, SealReceipt, StreamId, WriterSessionId};
use rtdb_db::{AuthenticatedCut, InstallationAnchor};
use rtdb_lineage::LineageSegment;
use rtdb_stream_journal::ConsensusJournal;
use rtdb_types::{Anomaly, AnomalyCategory, Result};
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

/// The authenticated source cut a lineage installation inherits, and the
/// committed installation that anchors it in the destination journal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Authentication {
    pub(super) source: AuthenticatedCut,
    pub(super) anchor: InstallationAnchor,
}

impl Authentication {
    fn from_parts(source: Option<AuthenticatedCut>, anchor: Option<InstallationAnchor>) -> Option<Option<Self>> {
        match (source, anchor) {
            (Some(source), Some(anchor)) => Some(Some(Self { source, anchor })),
            (None, None) => Some(None),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "InstallationRecord", into = "InstallationRecord")]
pub(crate) struct Installation {
    pub(super) manifest_digest: [u8; 32],
    pub(super) source_cut: SourceCut,
    pub(super) key_hold: Option<rtdb_crypt::RecoveryKeyHold>,
    pub(super) journal_hold: Option<RetainedJournal>,
    pub(super) objects_sha256: Option<[u8; 32]>,
    pub(super) authentication: Option<Authentication>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct InstallationRecord {
    manifest_digest: [u8; 32],
    source_cut: SourceCut,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    key_hold: Option<rtdb_crypt::RecoveryKeyHold>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    journal_hold: Option<RetainedJournal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    objects_sha256: Option<[u8; 32]>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source_head: Option<AuthenticatedCut>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    installation_anchor: Option<InstallationAnchor>,
}

impl TryFrom<InstallationRecord> for Installation {
    type Error = &'static str;

    fn try_from(record: InstallationRecord) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            manifest_digest: record.manifest_digest,
            source_cut: record.source_cut,
            key_hold: record.key_hold,
            journal_hold: record.journal_hold,
            objects_sha256: record.objects_sha256,
            authentication: Authentication::from_parts(record.source_head, record.installation_anchor)
                .ok_or(UNPAIRED_AUTHENTICATION)?,
        })
    }
}

impl From<Installation> for InstallationRecord {
    fn from(installation: Installation) -> Self {
        Self {
            manifest_digest: installation.manifest_digest,
            source_cut: installation.source_cut,
            key_hold: installation.key_hold,
            journal_hold: installation.journal_hold,
            objects_sha256: installation.objects_sha256,
            source_head: installation.authentication.map(|authentication| authentication.source),
            installation_anchor: installation.authentication.map(|authentication| authentication.anchor),
        }
    }
}

const UNPAIRED_AUTHENTICATION: &str = "an installation names its source cut without its anchor, or the reverse";

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "PendingRecord", into = "PendingRecord")]
pub(super) struct Pending {
    intent: Intent,
    installation: Option<Installation>,
    seal: SourceSeal,
}

/// Where a lossless recovery sealed its predecessor's source journal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SourceSeal {
    Unsealed,
    End(Lsn),
    /// A lineage seal, at the authenticated cut whose head ends the segment.
    Cut(AuthenticatedCut),
}

impl SourceSeal {
    fn end(self) -> Option<Lsn> {
        match self {
            Self::Unsealed => None,
            Self::End(end) => Some(end),
            Self::Cut(cut) => Some(cut.end()),
        }
    }

    fn cut(self) -> Option<AuthenticatedCut> {
        match self {
            Self::Cut(cut) => Some(cut),
            Self::Unsealed | Self::End(_) => None,
        }
    }
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PendingRecord {
    intent: Intent,
    installation: Option<Installation>,
    #[serde(rename = "final_source_lsn")]
    sealed_segment_end: Option<Lsn>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sealed_head: Option<AuthenticatedCut>,
}

impl TryFrom<PendingRecord> for Pending {
    type Error = &'static str;

    fn try_from(record: PendingRecord) -> std::result::Result<Self, Self::Error> {
        let seal = match (record.sealed_segment_end, record.sealed_head) {
            (None, None) => SourceSeal::Unsealed,
            (Some(end), None) => SourceSeal::End(end),
            (Some(end), Some(cut)) if cut.end() == end => SourceSeal::Cut(cut),
            _ => return Err("a pending recovery's sealed head does not end at its sealed end"),
        };
        Ok(Self { intent: record.intent, installation: record.installation, seal })
    }
}

impl From<Pending> for PendingRecord {
    fn from(pending: Pending) -> Self {
        Self {
            intent: pending.intent,
            installation: pending.installation,
            sealed_segment_end: pending.seal.end(),
            sealed_head: pending.seal.cut(),
        }
    }
}

/// A published recovery. Its source cut is the publication's lineage
/// segment's, so it keeps only the anchor of its installation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Completed {
    pub(super) ranked_closure: Option<super::ranked::RankedClosure>,
    loss_policy: LossPolicy,
    source_cut: SourceCut,
    sealed_segment_end: Option<Lsn>,
    key_hold: Option<rtdb_crypt::RecoveryKeyHold>,
    journal_hold: Option<RetainedJournal>,
    objects_sha256: Option<[u8; 32]>,
    anchor: Option<InstallationAnchor>,
}

/// The persisted form of a publication, which older activation readers
/// parse: its recovery repeats the segment's source cut as `source_head`.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PublicationRecord {
    operation: Uuid,
    predecessor: Binding,
    manifest_digest: [u8; 32],
    #[serde(default, skip_serializing_if = "Option::is_none")]
    destination_cell: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    recovery: Option<CompletedRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    segment: Option<LineageSegment>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CompletedRecord {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ranked_closure: Option<super::ranked::RankedClosure>,
    loss_policy: LossPolicy,
    source_cut: SourceCut,
    #[serde(rename = "final_source_lsn")]
    sealed_segment_end: Option<Lsn>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    key_hold: Option<rtdb_crypt::RecoveryKeyHold>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    journal_hold: Option<RetainedJournal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    objects_sha256: Option<[u8; 32]>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source_head: Option<AuthenticatedCut>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    installation_anchor: Option<InstallationAnchor>,
}

impl TryFrom<PublicationRecord> for Publication {
    type Error = &'static str;

    fn try_from(record: PublicationRecord) -> std::result::Result<Self, Self::Error> {
        let source = record.segment.as_ref().map(|segment| segment.source);
        let recovery = record.recovery.map(|completed| {
            if completed.source_head != source {
                return Err("a recovery's source cut differs from its lineage segment's");
            }
            if Authentication::from_parts(completed.source_head, completed.installation_anchor).is_none() {
                return Err(UNPAIRED_AUTHENTICATION);
            }
            Ok(Completed {
                ranked_closure: completed.ranked_closure,
                loss_policy: completed.loss_policy,
                source_cut: completed.source_cut,
                sealed_segment_end: completed.sealed_segment_end,
                key_hold: completed.key_hold,
                journal_hold: completed.journal_hold,
                objects_sha256: completed.objects_sha256,
                anchor: completed.installation_anchor,
            })
        }).transpose()?;
        Ok(Self {
            operation: record.operation,
            predecessor: record.predecessor,
            manifest_digest: record.manifest_digest,
            destination_cell: record.destination_cell,
            recovery,
            segment: record.segment,
        })
    }
}

impl From<Publication> for PublicationRecord {
    fn from(publication: Publication) -> Self {
        let source_head = publication.segment.as_ref().map(|segment| segment.source);
        Self {
            operation: publication.operation,
            predecessor: publication.predecessor,
            manifest_digest: publication.manifest_digest,
            destination_cell: publication.destination_cell,
            recovery: publication.recovery.map(|completed| CompletedRecord {
                ranked_closure: completed.ranked_closure,
                loss_policy: completed.loss_policy,
                source_cut: completed.source_cut,
                sealed_segment_end: completed.sealed_segment_end,
                key_hold: completed.key_hold,
                journal_hold: completed.journal_hold,
                objects_sha256: completed.objects_sha256,
                source_head,
                installation_anchor: completed.anchor,
            }),
            segment: publication.segment,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(tag = "stage", rename_all = "kebab-case")]
pub(crate) enum Status {
    Pending {
        intent: Intent,
        candidate: Uuid,
        grants_closed: bool,
        installation: Option<Installation>,
        #[serde(rename = "final_source_lsn")]
        sealed_segment_end: Option<Lsn>,
        #[serde(skip_serializing_if = "Option::is_none")]
        ranked_closure: Option<super::ranked::RankedClosure>,
    },
    Published {
        intent: Intent,
        binding: Binding,
        installation: Installation,
        #[serde(rename = "final_source_lsn")]
        sealed_segment_end: Option<Lsn>,
        #[serde(skip_serializing_if = "Option::is_none")]
        ranked_closure: Option<super::ranked::RankedClosure>,
    },
}

#[cfg(unix)]
pub(super) enum CustodySettlement {
    Abandoned,
    Published(Installation),
    Unrecorded { hosted_custody: bool },
}

fn custody_protocol(
    key_hold: Option<rtdb_crypt::RecoveryKeyHold>,
    journal_hold: Option<RetainedJournal>,
    objects_sha256: Option<[u8; 32]>,
) -> Option<u8> {
    match (key_hold, journal_hold, objects_sha256) {
        (None, None, None) => Some(RECOVERY_INTENT_PROTOCOL),
        (Some(_), None, None) => Some(RECOVERY_KEY_CUSTODY_PROTOCOL),
        (Some(_), Some(_), None) => Some(RECOVERY_JOURNAL_CUSTODY_PROTOCOL),
        (Some(_), Some(_), Some(digest)) if digest != [0; 32] => Some(RECOVERY_HOSTED_CUSTODY_PROTOCOL),
        _ => None,
    }
}

impl State {
    fn installation_anchor(&self) -> Option<InstallationAnchor> {
        self.publication.as_ref()?.recovery.as_ref()?.anchor
    }

    /// Whether a cut through `end` keeps every append the serving
    /// incarnation already published as its lineage floor.
    pub(super) fn keeps_lineage_floor(&self, end: Lsn) -> bool {
        self.floor.as_ref().is_none_or(|floor| floor.end() <= end)
    }

    pub(super) fn valid_recovery(&self) -> bool {
        if self.recovery_protocol() < RECOVERY_INTENT_PROTOCOL {
            return self.recovery.is_none()
                && self.superseded_recoveries.is_empty()
                && self
                    .publication
                    .as_ref()
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
                && (pending.seal == SourceSeal::Unsealed || self.grants_closed)
                && match pending.seal {
                    SourceSeal::Unsealed => true,
                    SourceSeal::End(_) => !self.uses_lineage(),
                    SourceSeal::Cut(cut) => {
                        self.uses_lineage() && cut.matches(*self.binding.incarnation.as_bytes(), cut.end())
                    }
                }
                && pending.installation.is_none_or(|installation| {
                    installation.source_cut.lsn != 0
                        && installation.valid_authentication(self.uses_lineage(), self.binding, self.installation_anchor())
                        && custody_protocol(installation.key_hold, installation.journal_hold, installation.objects_sha256)
                            .is_some_and(|protocol| self.recovery_protocol() >= protocol)
                        && match pending.intent.loss_policy {
                            LossPolicy::Lossless => {
                                pending.seal.end() == Some(Lsn(installation.source_cut.lsn))
                            }
                            LossPolicy::AcceptUnknownTail => pending.seal == SourceSeal::Unsealed,
                        }
                })
        });
        let publication_valid = self.publication.as_ref().is_some_and(|publication| {
            if let Some(completed) = &publication.recovery {
                completed.source_cut.lsn != 0
                    && publication.installation()
                        .is_some_and(|installation| installation.valid_authentication(self.uses_lineage(), publication.predecessor, None))
                    && publication.segment.is_some() == self.uses_lineage()
                    && publication.segment.as_ref().is_none_or(|segment| {
                        segment.predecessor == publication.predecessor && segment.successor == self.binding
                            && segment.end() == Lsn(completed.source_cut.lsn)
                    })
                    && custody_protocol(completed.key_hold, completed.journal_hold, completed.objects_sha256)
                        .is_some_and(|protocol| self.recovery_protocol() >= protocol)
                    && completed.ranked_closure.is_none_or(|closure| {
                        self.ranked.is_some() && closure.valid_for(publication.predecessor.incarnation)
                    })
                    && publication.predecessor.revision.checked_add(1)
                        == Some(self.binding.revision)
                    && match completed.loss_policy {
                        LossPolicy::Lossless => {
                            completed.sealed_segment_end == Some(Lsn(completed.source_cut.lsn))
                        }
                        LossPolicy::AcceptUnknownTail => completed.sealed_segment_end.is_none(),
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

    pub(super) fn pending(&self, operation: Operation) -> Result<Pending> {
        self.require(operation.predecessor)?;
        self.recovery
            .filter(|pending| pending.intent.operation == operation)
            .ok_or_else(conflict)
    }

    pub(super) fn publication_status(&self, operation: Operation) -> Result<Option<Status>> {
        let Some(publication) = self
            .publication
            .as_ref()
            .filter(|prior| prior.operation == operation.operation_id)
        else {
            return Ok(None);
        };
        if operation.predecessor != publication.predecessor {
            return Err(conflict());
        }
        publication.recovery_status(self.binding).map(Some)
    }
}

impl Publication {
    fn recovery_status(&self, binding: Binding) -> Result<Status> {
        let completed = self.recovery.as_ref().ok_or_else(conflict)?;
        Ok(Status::Published {
            intent: Intent {
                operation: Operation { predecessor: self.predecessor, operation_id: self.operation },
                destination_cell: self.destination_cell,
                loss_policy: completed.loss_policy,
            },
            binding,
            installation: self.installation().ok_or_else(conflict)?,
            sealed_segment_end: completed.sealed_segment_end,
            ranked_closure: completed.ranked_closure,
        })
    }
}

impl Publication {
    fn installation(&self) -> Option<Installation> {
        let completed = self.recovery.as_ref()?;
        Some(Installation {
            manifest_digest: self.manifest_digest,
            source_cut: completed.source_cut,
            key_hold: completed.key_hold,
            journal_hold: completed.journal_hold,
            objects_sha256: completed.objects_sha256,
            authentication: self.segment.as_ref().zip(completed.anchor)
                .map(|(segment, anchor)| Authentication { source: segment.source, anchor }),
        })
    }
}

impl Installation {
    #[cfg(feature = "backup-object-holds")]
    pub(in crate::multi) fn lineage_custody(self) -> Option<([u8; 32], rtdb_crypt::RecoveryKeyHold, [u8; 32], AuthenticatedCut)> {
        self.journal_hold?;
        Some((self.manifest_digest, self.key_hold?, self.objects_sha256?, self.authentication?.source))
    }

    fn valid_authentication(self, lineage: bool, predecessor: Binding, inherited: Option<InstallationAnchor>) -> bool {
        match (lineage, self.authentication) {
            (true, Some(Authentication { source, anchor })) => anchor.valid()
                && source.matches(*predecessor.incarnation.as_bytes(), Lsn(self.source_cut.lsn))
                && inherited.is_none_or(|anchor| anchor.is_in(source)),
            (false, None) => inherited.is_none(),
            _ => false,
        }
    }
}

fn same_source_tree(left: AuthenticatedCut, right: AuthenticatedCut) -> bool {
    left.catalog_root == right.catalog_root && left.head.catalog == right.head.catalog
        && left.head.stream == right.head.stream && left.head.size == right.head.size && left.head.root == right.head.root
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
                let mut seal = SourceSeal::Unsealed;
                if let Some(pending) = state.recovery {
                    if pending.intent == intent {
                        return Ok(None);
                    }
                    if supersedes != Some(pending.intent.operation.operation_id)
                        || operation.operation_id == pending.intent.operation.operation_id
                        || (pending.seal != SourceSeal::Unsealed
                            && intent.loss_policy != LossPolicy::Lossless)
                    {
                        return Err(conflict());
                    }
                    next.superseded_recoveries
                        .insert(pending.intent.operation.operation_id);
                    if intent.loss_policy == LossPolicy::Lossless {
                        seal = pending.seal;
                    }
                } else if supersedes.is_some() {
                    return Err(conflict());
                }
                next.advance_protocol(RECOVERY_INTENT_PROTOCOL);
                next.recovery = Some(Pending {
                    intent,
                    installation: None,
                    seal,
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

    pub(in crate::multi) async fn recovery_status_for_predecessor(&self, name: &str, predecessor: Binding) -> Result<Status> {
        let (_, state) = self.read(self.stream_id(name)?).await?;
        let state = state.ok_or_else(unavailable)?;
        if state.binding == predecessor {
            let pending = state.recovery.ok_or_else(conflict)?;
            return self.recovery_status_at(name, &state, pending.intent.operation);
        }
        if let Some(publication) = state.publication.as_ref().filter(|publication| publication.predecessor == predecessor) {
            return publication.recovery_status(state.binding);
        }
        let revision = predecessor.revision.checked_add(1).ok_or_else(conflict)?;
        if revision >= state.binding.revision {
            return Err(conflict());
        }
        let publication = self.archived_publication(name, predecessor).await?.ok_or_else(conflict)?;
        let incarnation = self.successor_incarnation_in_cell(
            name, predecessor, publication.operation, publication.destination_cell,
        )?;
        publication.recovery_status(Binding { incarnation, revision })
    }

    #[cfg(unix)]
    pub(super) async fn require_custody_release(
        &self, name: &str, operation: Operation, cell: Option<Uuid>, candidate: Uuid,
    ) -> Result<CustodySettlement> {
        let derived = self.successor_incarnation_in_cell(
            name, operation.predecessor, operation.operation_id, cell,
        )?;
        if derived != candidate { return Err(conflict()); }
        let (_, state) = self.read(self.stream_id(name)?).await?;
        let state = state.ok_or_else(unavailable)?;
        let protocol = state.recovery_protocol();
        if protocol < RECOVERY_INTENT_PROTOCOL {
            return Err(conflict());
        }
        let selected_revision = operation.predecessor.revision.checked_add(1).ok_or_else(conflict)?;
        if state.binding == operation.predecessor
            && state.superseded_recoveries.contains(&operation.operation_id)
        {
            return Ok(CustodySettlement::Abandoned);
        }
        let unrecorded = CustodySettlement::Unrecorded {
            hosted_custody: protocol >= RECOVERY_HOSTED_CUSTODY_PROTOCOL,
        };
        let publication = match state.binding.revision.cmp(&selected_revision) {
            std::cmp::Ordering::Greater => match self.archived_publication(name, operation.predecessor).await? {
                Some(publication) => Some(publication),
                None => return Ok(unrecorded),
            },
            std::cmp::Ordering::Equal => state.publication.filter(|publication| publication.predecessor == operation.predecessor),
            std::cmp::Ordering::Less => None,
        };
        if let Some(publication) = publication {
            if publication.operation != operation.operation_id {
                return Ok(if publication.recovery.is_some() { CustodySettlement::Abandoned } else { unrecorded });
            }
            if publication.destination_cell == cell
                && (state.binding.revision > selected_revision || state.binding.incarnation == candidate)
                && let Some(installation) = publication.installation()
            {
                return Ok(CustodySettlement::Published(installation));
            }
        }
        Err(Anomaly::new(
            AnomalyCategory::Conflict,
            "rtdb/activation-custody-pending",
            "The recovery authority has not settled this candidate",
        ))
    }

    #[cfg(unix)]
    pub(super) async fn object_release_installation(
        &self, name: &str, operation: Operation, cell: Option<Uuid>, candidate: Uuid,
    ) -> Result<Option<Installation>> {
        match self.require_custody_release(name, operation, cell, candidate).await {
            Ok(CustodySettlement::Abandoned) => return Ok(None),
            Ok(CustodySettlement::Published(installation)) => return Ok(Some(installation)),
            Ok(CustodySettlement::Unrecorded { .. }) => (),
            Err(error) if error.code == "rtdb/activation-custody-pending" => (),
            Err(error) => return Err(error),
        }
        Err(Anomaly::new(
            AnomalyCategory::Conflict,
            "rtdb/activation-object-custody",
            "The retained activation decision must settle this exact candidate",
        ))
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
            sealed_segment_end: pending.seal.end(),
            ranked_closure: state.ranked.and_then(|ranked| ranked.closure),
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
            key_hold: installed.key_hold,
            journal_hold: installed.journal_hold,
            objects_sha256: installed.objects_sha256,
            authentication: Authentication::from_parts(installed.source_head, installed.installation_anchor)
                .ok_or_else(conflict)?,
        };
        let stream = self.stream_id(name)?;
        let lineage = self.supports_lineage(stream).await?;
        if installation.source_cut.lsn == 0 || !installation.valid_authentication(lineage, operation.predecessor, None) {
            return Err(conflict());
        }
        let protocol = custody_protocol(installation.key_hold, installation.journal_hold, installation.objects_sha256)
            .ok_or_else(conflict)?;
        let state = self.change(stream, |state| {
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
                || installed.key_hold.is_some_and(|hold| {
                    hold.authority != self.identity
                        || Some(hold.authority_sha256) != self.connection_sha256
                        || hold.operation != operation.operation_id
                        || hold.generation.database != installed.successor
                })
                || installed.journal_hold.is_some_and(|held| {
                    held.hold.operation_id != operation.operation_id.as_u128()
                        || held.connection_sha256 == [0; 32]
                })
                || installed.activation_scope != Some(rtdb_db::ActivationScope::for_cell(
                    self.identity, name, intent.destination_cell,
                )?)
            {
                return Err(conflict());
            }
            if let Some(Status::Published { installation: prior, .. }) = published {
                return same_installation(prior, installation);
            }
            let required = state.recovery_protocol();
            if required >= RECOVERY_JOURNAL_CUSTODY_PROTOCOL && protocol < required {
                return Err(conflict());
            }
            let mut pending = state.pending(operation)?;
            if lineage != state.uses_lineage()
                || !installation.valid_authentication(lineage, operation.predecessor, state.installation_anchor())
                || pending.seal.cut().is_some_and(|sealed| {
                    installation.authentication.is_none_or(|authentication| !same_source_tree(sealed, authentication.source))
                })
                || pending.intent.loss_policy == LossPolicy::Lossless
                    && (installation.authentication.is_some() != pending.seal.cut().is_some())
            {
                return Err(conflict());
            }
            if let Some(prior) = pending.installation {
                return same_installation(prior, installation);
            }
            if intent.loss_policy == LossPolicy::Lossless
                && pending.seal.end() != Some(Lsn(installation.source_cut.lsn))
            {
                return Err(Anomaly::incorrect(
                    "rtdb/activation-final-source-cut",
                    "Lossless recovery requires an installation of the sealed predecessor's final position",
                ));
            }
            pending.installation = Some(installation);
            let mut next = state.clone();
            next.advance_protocol(protocol);
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
        authenticated: Option<&ConsensusJournal>,
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
        if pending.seal != SourceSeal::Unsealed {
            return self.recovery_status_at(name, &state, operation);
        }
        let lineage = self.supports_lineage(stream).await?;
        if lineage != state.uses_lineage() { return Err(conflict()); }
        let authenticated = if lineage {
            let journal = authenticated.ok_or_else(conflict)?;
            journal.require_authenticated(StreamId(operation.predecessor.incarnation.as_u128())).await.map_err(journal_error)?;
            Some(journal)
        } else {
            None
        };
        clock::wait_out(
            self.clock.as_ref(),
            clock::drain_window(state.grants_end.is_some()),
        )
        .await?;
        // The source verifier has opened this commissioned predecessor. A new quorum
        // writer fence orders all still in-flight writer requests before this cut.
        let seal = seal_predecessor(source, operation).await?;
        let sealed_segment_end = seal.sealed_head_lsn;
        let sealed = match authenticated {
            Some(journal) => {
                let (head, certification) = journal.authenticated_head(seal.fence.stream, Some(sealed_segment_end.0)).await.map_err(journal_error)?;
                let catalog_root = certification.history.genesis.root();
                head.verify(catalog_root, &certification).map_err(|_| conflict())?;
                let cut = AuthenticatedCut { head: head.head, catalog_root };
                if !cut.matches(*operation.predecessor.incarnation.as_bytes(), sealed_segment_end) { return Err(conflict()); }
                SourceSeal::Cut(cut)
            }
            None => SourceSeal::End(sealed_segment_end),
        };
        let state = self
            .change(stream, |state| {
                let state = state.ok_or_else(unavailable)?;
                if let Some(Status::Published {
                    sealed_segment_end: prior,
                    ..
                }) = state.publication_status(operation)?
                {
                    return if prior == Some(sealed_segment_end) {
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
                if let Some(prior) = pending.seal.end() {
                    return if prior == sealed_segment_end { Ok(None) } else { Err(conflict()) };
                }
                pending.seal = sealed;
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
        let (frozen_at, frozen) = self.read_record(stream).await?;
        let (frozen, frozen_record) = frozen.ok_or_else(unavailable)?;
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
        let lineage = self.supports_lineage(stream).await?;
        if lineage != frozen.uses_lineage()
            || !installation.valid_authentication(lineage, operation.predecessor, frozen.installation_anchor())
        {
            return Err(conflict());
        }
        if !frozen.keeps_lineage_floor(Lsn(installation.source_cut.lsn)) {
            return Err(below_lineage_floor());
        }
        let ranked_closure = frozen.require_ranked_closure()?;
        let successor = self.candidate(name, pending.intent)?;
        let successor_binding = Binding {
            incarnation: successor,
            revision: operation.predecessor.revision.checked_add(1).ok_or_else(conflict)?,
        };
        let segment = installation.authentication.map(|Authentication { source, .. }| rtdb_lineage::LineageSegment {
            authority: self.identity,
            database: name.to_owned(),
            predecessor: operation.predecessor,
            successor: successor_binding,
            source,
            previous: frozen.publication.as_ref().and_then(|prior| prior.segment.as_ref()).map(|segment| segment.digest()),
        });
        if segment.as_ref().is_some_and(|segment| !segment.valid()) { return Err(conflict()); }
        let publication = Publication {
            operation: operation.operation_id,
            predecessor: operation.predecessor,
            manifest_digest: installation.manifest_digest,
            destination_cell: pending.intent.destination_cell,
            recovery: Some(Completed {
                ranked_closure,
                loss_policy: pending.intent.loss_policy,
                source_cut: installation.source_cut,
                sealed_segment_end: pending.seal.end(),
                key_hold: installation.key_hold,
                journal_hold: installation.journal_hold,
                objects_sha256: installation.objects_sha256,
                anchor: installation.authentication.map(|authentication| authentication.anchor),
            }),
            segment,
        };
        self.preserve_publication(name, &frozen, frozen_at, frozen_record).await?;
        clock::wait_out(
            self.clock.as_ref(),
            clock::drain_window(frozen.grants_end.is_some()),
        )
        .await?;
        let state = self
            .change(stream, |state| {
                let state = state.ok_or_else(unavailable)?;
                if state.already_published(&publication, successor)? {
                    return Ok(None);
                }
                if state.pending(operation)? != pending
                    || state.require_ranked_closure()? != ranked_closure
                {
                    return Err(conflict());
                }
                Ok(Some(self.install_binding(name, state, &publication, successor)?))
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

fn below_lineage_floor() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict,
        "rtdb/lineage-floor",
        "The recovery cut ends before the lineage floor already published for this incarnation. Restore a backup through the floor.",
    )
}

async fn seal_predecessor(source: &dyn Journal, operation: Operation) -> Result<SealReceipt> {
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
            Ok(receipt) => return checked_seal(receipt, stream, session),
            Err(JournalError::Fenced { current }) if current != expected => {
                // Reopen the original receipt after a crash following the source seal.
                if let Some(previous) = current.0.checked_sub(1) {
                    match source.seal_session(stream, Epoch(previous), session).await {
                        Ok(receipt) => return checked_seal(receipt, stream, session),
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

fn checked_seal(receipt: SealReceipt, stream: StreamId, session: WriterSessionId) -> Result<SealReceipt> {
    if receipt.fence.stream != stream || receipt.fence.writer_session_id != session
        || receipt.fence.writer_epoch == Epoch(0) || receipt.sealed_head_lsn == Lsn(0)
    {
        return Err(conflict());
    }
    Ok(receipt)
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

    fn recorded_installation() -> serde_json::Value {
        let head = rtdb_stream_journal::authenticated::Head {
            catalog: Uuid::from_u128(1),
            stream: StreamId(2),
            size: 3,
            root: [4; 32],
            timestamp_micros: 5,
            authority_sha256: [6; 32],
        };
        serde_json::json!({
            "manifest_digest": Some([7u8; 32]),
            "source_cut": SourceCut { lsn: 3, position: 1, system_time: 8 },
            "source_head": AuthenticatedCut { head, catalog_root: rtdb_db::CatalogRoot([9; 32]) },
            "installation_anchor": InstallationAnchor {
                catalog: Uuid::from_u128(10), catalog_root: rtdb_db::CatalogRoot([11; 32]), position: Lsn(12),
            },
        })
    }

    #[test]
    fn published_recovery_repeats_its_segments_source_cut() {
        let installation = recorded_installation();
        let source: AuthenticatedCut = serde_json::from_value(installation["source_head"].clone()).unwrap();
        let segment = LineageSegment {
            authority: Uuid::from_u128(13),
            database: "xtdb".into(),
            predecessor: Binding { incarnation: Uuid::from_u128(2), revision: 1 },
            successor: Binding { incarnation: Uuid::from_u128(14), revision: 2 },
            source,
            previous: None,
        };
        let recorded = serde_json::json!({
            "operation": Uuid::from_u128(15),
            "predecessor": segment.predecessor,
            "manifest_digest": installation["manifest_digest"],
            "recovery": {
                "loss_policy": "lossless",
                "source_cut": installation["source_cut"],
                "final_source_lsn": 3,
                "source_head": source,
                "installation_anchor": installation["installation_anchor"],
            },
            "segment": segment,
        });
        let publication: Publication = serde_json::from_value(recorded.clone()).unwrap();
        assert_eq!(serde_json::to_value(&publication.installation().unwrap()).unwrap(), installation);
        assert_eq!(serde_json::to_value(&publication).unwrap(), recorded);

        let mut diverged = recorded.clone();
        diverged["recovery"]["source_head"]["catalog_root"] = serde_json::to_value([16u8; 32]).unwrap();
        assert!(serde_json::from_value::<Publication>(diverged).is_err());
        let mut unpaired = recorded;
        unpaired["recovery"].as_object_mut().unwrap().remove("installation_anchor");
        assert!(serde_json::from_value::<Publication>(unpaired).is_err());
    }

    #[test]
    fn pending_recovery_seals_at_its_heads_end() {
        let installation = recorded_installation();
        let intent = Intent {
            operation: Operation { predecessor: Binding { incarnation: Uuid::from_u128(2), revision: 1 }, operation_id: Uuid::from_u128(3) },
            destination_cell: None,
            loss_policy: LossPolicy::Lossless,
        };
        let recorded = serde_json::json!({
            "intent": intent,
            "installation": null,
            "final_source_lsn": 3,
            "sealed_head": installation["source_head"],
        });
        let pending: Pending = serde_json::from_value(recorded.clone()).unwrap();
        assert_eq!(pending.seal.end(), Some(Lsn(3)));
        assert!(pending.seal.cut().is_some());
        assert_eq!(serde_json::to_value(pending).unwrap(), recorded);

        let mut unsealed = recorded.clone();
        unsealed["final_source_lsn"] = serde_json::Value::Null;
        unsealed.as_object_mut().unwrap().remove("sealed_head");
        assert_eq!(serde_json::from_value::<Pending>(unsealed).unwrap().seal, SourceSeal::Unsealed);
        for end in [serde_json::Value::Null, serde_json::json!(4)] {
            let mut misplaced = recorded.clone();
            misplaced["final_source_lsn"] = end;
            assert!(serde_json::from_value::<Pending>(misplaced).is_err());
        }
    }

    #[test]
    fn recorded_installation_pairs_its_source_cut_with_its_anchor() {
        let recorded = recorded_installation();
        let installation: Installation = serde_json::from_value(recorded.clone()).unwrap();
        assert!(installation.authentication.is_some());
        assert_eq!(serde_json::to_value(installation).unwrap(), recorded);
        for field in ["source_head", "installation_anchor"] {
            let mut unpaired = recorded.clone();
            unpaired.as_object_mut().unwrap().remove(field);
            assert!(serde_json::from_value::<Installation>(unpaired).is_err(), "{field}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn lineage_floor_settles_a_serving_incarnation_and_binds_later_cuts() {
        use std::{collections::BTreeSet, net::Ipv4Addr};
        use rtdb_crypt::FileKms;
        use rtdb_db::{ActivationScope, MultiDatabase, MultiOptions, Role};
        use rtdb_node::{Clock, SystemClock};
        use rtdb_storage::MemoryStore;
        use rtdb_tx::transact::Transact;
        use rtdb_lineage::{Authorization, Published, Settlement, verify_settlement};

        const NAME: &str = "floor";
        const PRINCIPAL: &str = "urn:logkeeper:client:lineage-floor";
        let control = LocalCluster::start_authenticated(&[PRINCIPAL]).await.unwrap();
        let source = LocalCluster::start_authenticated(&[PRINCIPAL]).await.unwrap();
        let open = |cluster: &LocalCluster| {
            Arc::new(cluster.journal(PRINCIPAL, &cluster.journal_config(64 << 20, 4 << 20)).unwrap())
        };
        let control_journal = open(&control);
        let source_journal = open(&source);
        for journal in [&control_journal, &source_journal] {
            journal.finalize_version(CLUSTER_VERSION_CURRENT).await.unwrap();
        }
        let control_root = control.catalog_root().unwrap();
        let source_root = source.catalog_root().unwrap();
        let identity = Uuid::new_v4();
        let mut authority = ActivationAuthority::new(control_journal.clone(), identity).unwrap();
        Arc::get_mut(&mut authority).unwrap().authenticated_journal = Some(control_journal.clone());

        let directory = tempfile::tempdir().unwrap();
        let vault = directory.path().join("source");
        FileKms::create_directory(&vault).unwrap();
        let keys = Arc::new(FileKms::open(&vault, || SystemClock.now().0).unwrap());
        let node = Ipv4Addr::LOCALHOST.into();
        let binding = authority.bootstrap(NAME, Uuid::new_v4()).await.unwrap();
        let mut database = MultiDatabase::new(
            MultiOptions { database: binding.incarnation, node, lease_micros: 60_000_000, drift_micros: 0 },
            source_journal.clone(), Arc::new(MemoryStore::default()), keys, Arc::new(|| SystemClock.now()),
        ).unwrap().with_immutable_journal().with_authenticated_journal(source_journal.clone(), rtdb_db::NewStream::Authenticated)
            .with_activation_scope(ActivationScope::new(identity, NAME).unwrap()).unwrap();
        database.create().await.unwrap();
        assert!(matches!(database.tick(&BTreeSet::from([node])).await.unwrap(), Role::Writer(_)));
        database.transact(&Transact {
            custody_request_digest: None, request_memory: None, key: vec![7; 16], require: vec![],
            window: Default::default(), guards: vec![], requires: vec![], otherwise: vec![], ops: vec![],
        }).await.unwrap();

        let through = source_journal.head(StreamId(binding.incarnation.as_u128())).await.unwrap();
        let proof = authority.publish_lineage_floor(NAME, binding, &source_journal, through).await.unwrap();
        let stream = StreamId(binding.incarnation.as_u128());
        let Published::Open(floor) = proof.published(identity, NAME, control_root, Some(stream)).unwrap()
            else { panic!("expected an open lineage floor") };
        assert_eq!(floor.binding, binding);
        assert_eq!(floor.end(), through);
        assert_eq!(authority.publish_lineage_floor(NAME, binding, &source_journal, Lsn(through.0 - 1)).await.unwrap()
            .published(identity, NAME, control_root, Some(stream)).unwrap(), Published::Open(floor.clone()));

        let at = floor.end();
        let inclusion = source_journal.inclusion(stream, at, Some(at.0)).await.unwrap();
        let committed = inclusion.verify(source_root).unwrap();
        let authorization = Authorization {
            catalog: floor.source.head.catalog,
            stream,
            writer_epoch: committed.writer_epoch,
            at,
            payload_sha256: [9; 32],
            timing: rtdb_stream_journal::authenticated::Timing {
                exclusive_cutoff_micros: i64::MAX,
                receiver_certificate_sha256: [8; 32],
            },
        };
        let settle = |authorization, inclusion| verify_settlement(
            identity, NAME, control_root, source_root, authorization, Some(&proof), inclusion,
        ).unwrap();
        assert_eq!(settle(authorization, Some(&inclusion)), Settlement::Release);
        assert_eq!(settle(Authorization { at: Lsn(at.0 + 1), ..authorization }, None), Settlement::Pending);

        let (_, state) = authority.read(authority.stream_id(NAME).unwrap()).await.unwrap();
        let state = state.unwrap();
        assert!(state.keeps_lineage_floor(at));
        assert!(!state.keeps_lineage_floor(Lsn(at.0 - 1)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn lineage_database_names_an_unauthenticated_application_catalog() {
        use rtdb_crypt::FileKms;
        use rtdb_db::{MultiDatabase, MultiOptions};
        use rtdb_node::{Clock, SystemClock};
        use rtdb_storage::MemoryStore;

        const PRINCIPAL: &str = "urn:logkeeper:client:lineage-legacy-application";
        let keepers = LocalCluster::start_replicated_catalog(&[PRINCIPAL]).await.unwrap();
        let journal = Arc::new(keepers.journal(PRINCIPAL, &keepers.journal_config(64 << 20, 4 << 20)).unwrap());
        journal.finalize_version(CLUSTER_VERSION_CURRENT).await.unwrap();
        let directory = tempfile::tempdir().unwrap();
        let vault = directory.path().join("keys");
        FileKms::create_directory(&vault).unwrap();
        let keys = Arc::new(FileKms::open(&vault, || SystemClock.now().0).unwrap());
        let database = MultiDatabase::new(
            MultiOptions { database: Uuid::new_v4(), node: std::net::Ipv4Addr::LOCALHOST.into(), lease_micros: 60_000_000, drift_micros: 0 },
            journal.clone(), Arc::new(MemoryStore::default()), keys, Arc::new(|| SystemClock.now()),
        ).unwrap().with_immutable_journal().with_authenticated_journal(journal, rtdb_db::NewStream::Authenticated);
        assert_eq!(database.create().await.unwrap_err().code, "rtdb/lineage-application-catalog");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn lineage_recovery_publishes_a_cut_after_source_loss_and_proves_it_after_trim() {
        use std::{collections::BTreeSet, net::Ipv4Addr};
        use rtdb_crypt::FileKms;
        use rtdb_db::{ActivationScope, MultiDatabase, MultiOptions, PortableBackup, Role};
        use rtdb_node::{Clock, SystemClock};
        use rtdb_storage::MemoryStore;

        const NAME: &str = "lineage";
        const PRINCIPAL: &str = "urn:logkeeper:client:lineage-recovery";
        let mut control = LocalCluster::start_authenticated(&[PRINCIPAL]).await.unwrap();
        let mut source = LocalCluster::start_authenticated(&[PRINCIPAL]).await.unwrap();
        let mut destination = LocalCluster::start_authenticated(&[PRINCIPAL]).await.unwrap();
        let open = |cluster: &LocalCluster| {
            Arc::new(cluster.journal(PRINCIPAL, &cluster.journal_config(64 << 20, 4 << 20)).unwrap())
        };
        let control_journal = open(&control);
        let source_journal = open(&source);
        let destination_journal = open(&destination);
        for journal in [&control_journal, &source_journal, &destination_journal] {
            journal.finalize_version(CLUSTER_VERSION_CURRENT).await.unwrap();
        }
        let control_root = control_journal.catalog_history().await.unwrap().genesis.root();
        let source_root = source_journal.catalog_history().await.unwrap().genesis.root();
        let identity = Uuid::new_v4();
        let mut authority = ActivationAuthority::new(control_journal.clone(), identity).unwrap();
        let ticks = AtomicU64::new(0);
        let owner = Arc::get_mut(&mut authority).unwrap();
        owner.authenticated_journal = Some(control_journal.clone());
        owner.clock = Arc::new(move || Some(Duration::from_secs(ticks.fetch_add(6, Ordering::SeqCst))));

        let directory = tempfile::tempdir().unwrap();
        let vault = |name| {
            let path = directory.path().join(name);
            FileKms::create_directory(&path).unwrap();
            Arc::new(FileKms::open(&path, || SystemClock.now().0).unwrap())
        };
        let source_vault = vault("source");
        let destination_vault = vault("destination");
        let node = Ipv4Addr::LOCALHOST.into();
        let scope = ActivationScope::new(identity, NAME).unwrap();
        let database = |incarnation, journal: Arc<ConsensusJournal>, keys: Arc<FileKms>| {
            MultiDatabase::new(
                MultiOptions { database: incarnation, node, lease_micros: 60_000_000, drift_micros: 0 },
                journal.clone(), Arc::new(MemoryStore::default()), keys, Arc::new(|| SystemClock.now()),
            ).unwrap().with_immutable_journal().with_authenticated_journal(journal, rtdb_db::NewStream::Authenticated)
                .with_activation_scope(scope).unwrap()
        };
        let predecessor = authority.bootstrap(NAME, Uuid::new_v4()).await.unwrap();
        let mut original = database(predecessor.incarnation, source_journal, source_vault.clone());
        original.create().await.unwrap();
        assert!(matches!(original.tick(&BTreeSet::from([node])).await.unwrap(), Role::Writer(_)));
        let backup = original.portable_backup().await.unwrap().into_bytes();
        drop(original);
        source.shutdown().await.unwrap();

        let operation = Operation { predecessor, operation_id: Uuid::new_v4() };
        let intent = Intent { operation, destination_cell: None, loss_policy: LossPolicy::AcceptUnknownTail };
        let Status::Pending { candidate, .. } = authority.begin_recovery(NAME, intent, None).await.unwrap()
            else { panic!("expected a pending recovery") };
        let mut successor = database(candidate, destination_journal, destination_vault.clone());
        successor.create().await.unwrap();
        assert!(matches!(successor.tick(&BTreeSet::from([node])).await.unwrap(), Role::Writer(_)));
        let before_restore = successor.portable_backup().await.unwrap().into_bytes();
        let verified = PortableBackup::verify(&backup, source_vault).await.unwrap();
        let mut detached = verified.lineage_proof_bytes(&backup).unwrap().unwrap();
        let archive = rtdb_stream_journal::authenticated::Archive::read_from(&mut detached).unwrap();
        assert!(detached.is_empty());
        assert_eq!(archive.signed_head.head, verified.authenticated_cut().unwrap().head);
        archive.inclusion(verified.source_lsn()).unwrap().verify(source_root).unwrap();
        let mut different_backup = backup.clone();
        *different_backup.last_mut().unwrap() ^= 1;
        assert!(verified.lineage_proof_bytes(&different_backup).is_err());
        let installed = successor.install_portable_backup(verified).await.unwrap();
        let ready = VerifiedSuccessor::from_installation(&installed);
        assert_eq!(authority.status(NAME).await.unwrap().grant_protocol_version, super::super::LINEAGE_PROTOCOL);
        for incomplete in [
            VerifiedSuccessor { source_head: None, ..ready },
            VerifiedSuccessor { installation_anchor: None, ..ready },
        ] {
            assert_eq!(authority.record_recovery_installation(NAME, operation, incomplete).await.unwrap_err().code,
                "rtdb/activation-conflict");
        }
        authority.record_recovery_installation(NAME, operation, ready).await.unwrap();
        authority.freeze_recovery(NAME, operation).await.unwrap();
        let Status::Published { binding, .. } = authority.publish_recovery(NAME, operation).await.unwrap()
            else { panic!("expected a published recovery") };
        let proof = authority.lineage_publication_proof(NAME, predecessor).await.unwrap();

        let next = Operation { predecessor: binding, operation_id: Uuid::new_v4() };
        let Status::Pending { candidate, .. } = authority.begin_recovery(NAME, Intent { operation: next, ..intent }, None).await.unwrap()
            else { panic!("expected a second pending recovery") };
        let before_restore = PortableBackup::verify(&before_restore, destination_vault).await.unwrap();
        assert_eq!(authority.record_recovery_installation(NAME, next, VerifiedSuccessor {
            predecessor: binding.incarnation,
            successor: candidate,
            manifest_digest: before_restore.manifest_sha256(),
            source_head: before_restore.authenticated_cut(),
            source_cut: SourceCut {
                lsn: before_restore.source_lsn().0,
                position: before_restore.source_basis().position(),
                system_time: before_restore.source_system_time().0,
            },
            ..ready
        }).await.unwrap_err().code, "rtdb/activation-conflict");

        let at = proof.control_inclusion.leaf.at;
        let stream = authority.stream_id(NAME).unwrap();
        assert!(matches!(control_journal.read_page(stream, at, at, 1, Duration::ZERO).await,
            Err(JournalError::Trimmed { .. })));
        let archived = authority.lineage_publication_proof(NAME, predecessor).await.unwrap();
        assert_eq!(proof.control_record, archived.control_record);
        assert_eq!(at, archived.control_inclusion.leaf.at);
        destination.shutdown().await.unwrap();
        control.shutdown().await.unwrap();

        let segment = archived.verify(identity, NAME, control_root).unwrap();
        assert_eq!(segment.successor, binding);
        let application = PortableBackup::lineage_archive(&backup, source_root).unwrap().unwrap();
        assert_eq!(application.signed_head.head, segment.source.head);
        assert_eq!(application.inclusion(segment.end()).unwrap().verify(source_root).unwrap().at, segment.end());
    }

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
        let mut authority_keepers = LocalCluster::start_independent_catalog(&[PRINCIPAL]).await.unwrap();
        let authority_config = authority_keepers.journal_config(64 << 20, 4 << 20);
        let authority_journal = Arc::new(authority_keepers.journal(PRINCIPAL, &authority_config).unwrap());
        authority_journal.finalize_version(CLUSTER_VERSION_CURRENT).await.unwrap();
        let identity = Uuid::new_v4();
        let mut authority = ActivationAuthority::new(authority_journal.clone(), identity).unwrap();
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
            key_hold: None,
            journal_hold: None,
            objects_sha256: None,
            source_head: None,
            installation_anchor: None,
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
                .seal_recovery_source("xtdb", operation, &journal, None, None)
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
            .seal_recovery_source("xtdb", operation, &journal, None, None)
            .await
            .unwrap();
        assert!(matches!(
            sealed,
            Status::Pending {
                sealed_segment_end: Some(Lsn(2)),
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
                sealed_segment_end: Some(Lsn(2)),
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
        let staged = authority
            .record_recovery_installation("xtdb", replacement, exact)
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(authority.recovery_status_for_predecessor("xtdb", predecessor).await.unwrap()).unwrap(),
            serde_json::to_value(staged).unwrap(),
        );
        let published = authority
            .publish_recovery("xtdb", replacement)
            .await
            .unwrap();
        assert!(matches!(
            published,
            Status::Published { binding, sealed_segment_end: Some(Lsn(2)), .. }
                if binding.incarnation == replacement_candidate
        ));
        assert!(matches!(
            authority
                .publish_recovery("xtdb", replacement)
                .await
                .unwrap(),
            Status::Published {
                sealed_segment_end: Some(Lsn(2)),
                ..
            }
        ));
        let published = serde_json::to_value(published).unwrap();
        assert_eq!(
            serde_json::to_value(authority.recovery_status_for_predecessor("xtdb", predecessor).await.unwrap()).unwrap(),
            published,
        );
        #[cfg(unix)]
        {
            let current = authority.binding("xtdb").await.unwrap();
            let operation_after = Operation { predecessor: current, operation_id: Uuid::new_v4() };
            let intent_after = Intent {
                operation: operation_after, destination_cell: None, loss_policy: LossPolicy::AcceptUnknownTail,
            };
            let Status::Pending { candidate: successor, .. } =
                authority.begin_recovery("xtdb", intent_after, None).await.unwrap()
            else { panic!("expected recovery to start from the published successor"); };
            authority.record_recovery_installation("xtdb", operation_after, VerifiedSuccessor {
                predecessor: current.incarnation, successor, ..installed(3)
            }).await.unwrap();
            authority.freeze_recovery("xtdb", operation_after).await.unwrap();
            authority.publish_recovery("xtdb", operation_after).await.unwrap();

            let reopened = ActivationAuthority::with_access(
                authority_journal, identity, super::super::Access::ConsensusReader,
            ).unwrap();
            assert_eq!(
                serde_json::to_value(reopened.recovery_status_for_predecessor("xtdb", predecessor).await.unwrap()).unwrap(),
                published,
            );
            assert_eq!(
                reopened.recovery_status("xtdb", replacement).await.unwrap_err().code,
                "rtdb/activation-conflict",
            );
            for wrong in [
                Binding { incarnation: Uuid::new_v4(), ..predecessor },
                Binding { revision: predecessor.revision + 1, ..predecessor },
            ] {
                assert_eq!(
                    reopened.recovery_status_for_predecessor("xtdb", wrong).await.unwrap_err().code,
                    "rtdb/activation-conflict",
                );
            }
            assert!(matches!(
                reopened.require_custody_release("xtdb", operation, None, candidate).await.unwrap(),
                CustodySettlement::Abandoned
            ));
            assert!(matches!(
                reopened.require_custody_release("xtdb", replacement, None, replacement_candidate).await.unwrap(),
                CustodySettlement::Published(installation) if installation.source_cut.lsn == 2
            ));
            let untaken = Operation {
                predecessor: Binding { incarnation: Uuid::new_v4(), revision: predecessor.revision },
                ..operation
            };
            let unknown = reopened.successor_incarnation_in_cell("xtdb", untaken.predecessor, untaken.operation_id, None).unwrap();
            assert!(matches!(
                reopened.require_custody_release("xtdb", untaken, None, unknown).await.unwrap(),
                CustodySettlement::Unrecorded { .. }
            ));
        }
        authority_keepers.shutdown().await.unwrap();
        keepers.shutdown().await.unwrap();
    }
}
