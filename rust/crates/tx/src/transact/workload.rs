use super::*;

mod appointment;
mod provision;
pub use appointment::{FundedAppointmentScope, ManagedAppointment};
pub use provision::LineProvision;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Point {
    pub table: TableRef,
    pub id: EntityId,
    pub valid_at: Timestamp,
}

impl Point {
    pub fn new(relation: &str, row_id: String, valid_at: Timestamp) -> Result<Self> {
        if !bounded(relation)
            || !relation.contains('.')
            || !bounded(&row_id)
            || valid_at == Timestamp::MAX
        {
            return Err(Anomaly::incorrect(
                "rtdb/managed-workload-point",
                "Configure an exact schema-qualified relation, string row ID and finite valid time",
            ));
        }
        Ok(Self {
            table: TableRef::parse(relation)?,
            id: EntityId::String(row_id),
            valid_at,
        })
    }
}

pub fn require_family_branch(index: &BranchIndex, branch: &str, created: Basis, now: Timestamp) -> Result<()> {
    let view = index.view(branch, At::Latest, Layer::Merged, now)?;
    if view.lineage().iter().any(|line| line.branch == branch && line.created == created
        && line.writable && line.dropped.is_none() && line.expires_at.is_none_or(|until| now < until))
    {
        Ok(())
    } else {
        Err(stale())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Holder {
    pub assignment_id: [u8; 16],
    pub holder: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Placement {
    pub epoch: u64,
    pub assignment: Option<Holder>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Applied {
    pub namespace: [u8; 32],
    pub key: Vec<u8>,
    pub basis: [u8; Basis::WIDTH],
    pub system_time_micros: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fence {
    pub placement: Placement,
    pub applied: Applied,
}

impl Fence {
    /// Bound a workload to the imported placement or a later ordered fence.
    /// The writer still verifies the actual receipt and the current control row.
    pub fn at_or_after_import(&self, imported: &Self) -> bool {
        if self == imported {
            return true;
        }
        let (Ok(basis), Ok(origin)) = (
            Basis::from_bytes(&self.applied.basis),
            Basis::from_bytes(&imported.applied.basis),
        ) else {
            return false;
        };
        basis.incarnation() == origin.incarnation()
            && basis > origin
            && self.placement.epoch > imported.placement.epoch
            && self.applied.system_time_micros > imported.applied.system_time_micros
    }

    pub fn covers_imported_family_line(&self, imported: &Self, writer: &Applied, created: Basis) -> bool {
        let (Ok(placement), Ok(written)) = (Basis::from_bytes(&self.applied.basis), Basis::from_bytes(&writer.basis)) else {
            return false;
        };
        self.at_or_after_import(imported)
            && written.incarnation() == placement.incarnation() && written > placement
            && created.incarnation() == placement.incarnation() && created > written
    }
}

pub struct ImportedPlacement {
    point: Point,
    placement: Placement,
    decision: [u8; 32],
    application_cut: BranchCut,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Predecessor {
    pub fence: Fence,
    pub writer: Applied,
    pub close: Applied,
    pub closure: [u8; 16],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Previous {
    Empty,
    Vacant { fence: Fence },
    Closed { predecessor: Predecessor },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Family {
    AdvancePlacement,
    ClaimWriter,
    Write,
    CloseWriter,
    CloseEmptyAssignment,
    CustodyWrite,
    OriginalFamilyWrite,
    ProvisionBranch,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "role", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Authority {
    AdvancePlacement {
        installation_intent: [u8; 16],
        previous: Previous,
        next: Placement,
    },
    ClaimWriter {
        fence: Fence,
    },
    ReadRoot {
        fence: Fence,
        writer: Applied,
    },
    ReadCustody {
        fence: Fence,
        writer: Applied,
        groups: Vec<[u8; 16]>,
    },
    Write {
        fence: Fence,
        writer: Applied,
    },
    CustodyWrite {
        fence: Fence,
        writer: Applied,
        request_digest: [u8; 32],
    },
    ProvisionBranch {
        fence: Fence,
        writer: Applied,
        line: LineProvision,
        request_digest: [u8; 32],
    },
    OriginalFamilyWrite {
        fence: Fence,
        writer: Applied,
        branch: String,
        created: Basis,
        group: CustodyGroupId,
        session: [u8; 32],
        request_digest: [u8; 32],
        #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
        authoring_controls: BTreeSet<TableRef>,
    },
    FundedAppointment(FundedAppointmentScope),
    CloseWriter {
        fence: Fence,
        writer: Applied,
        closure: [u8; 16],
    },
    CloseWriterGuarded {
        fence: Fence,
        writer: Applied,
        closure: [u8; 16],
        request_digest: [u8; 32],
    },
    CloseEmptyAssignment {
        fence: Fence,
        closure: [u8; 16],
        vacant_epoch: u64,
    },
    ObservePlacement {
        fence: Fence,
    },
    PredecessorEvidence {
        predecessor: Predecessor,
    },
    ReceiptRecovery {
        original_namespace: [u8; 32],
        family: Family,
    },
    AttemptSettlement {
        original_namespace: [u8; 32],
        family: Family,
        key: Vec<u8>,
        request_digest: [u8; 32],
    },
}

impl Authority {
    pub fn permits_imported_workload(&self, imported: &Fence) -> bool {
        let fence = match self {
            Self::ClaimWriter { fence } | Self::ReadRoot { fence, .. }
            | Self::ReadCustody { fence, .. } | Self::Write { fence, .. }
            | Self::CustodyWrite { fence, .. } | Self::ProvisionBranch { fence, .. } | Self::CloseWriter { fence, .. }
            | Self::CloseWriterGuarded { fence, .. } | Self::CloseEmptyAssignment { fence, .. }
            | Self::ObservePlacement { fence }
            | Self::AdvancePlacement { previous: Previous::Vacant { fence }, .. } => fence,
            Self::AdvancePlacement { previous: Previous::Closed { predecessor }, .. }
            | Self::PredecessorEvidence { predecessor } => &predecessor.fence,
            _ => return false,
        };
        fence.at_or_after_import(imported)
    }

    pub fn valid(&self, incarnation: [u8; 16]) -> bool {
        let applied = |value: &Applied| {
            valid_key(&value.key)
                && value.namespace != [0; 32]
                && Basis::from_bytes(&value.basis)
                    .is_ok_and(|basis| basis.incarnation() == incarnation && basis.position() > 0)
        };
        let fence = |value: &Fence| value.placement.valid() && applied(&value.applied);
        let held = |value: &Fence| value.placement.assignment.is_some() && fence(value);
        let predecessor = |value: &Predecessor| {
            held(&value.fence)
                && applied(&value.writer)
                && applied(&value.close)
                && value.closure != [0; 16]
        };
        match self {
            Self::AdvancePlacement {
                installation_intent,
                previous,
                next,
            } => {
                *installation_intent != [0; 16]
                    && next.valid()
                    && match previous {
                        Previous::Empty => next.assignment.is_some(),
                        Previous::Vacant { fence: vacancy } => {
                            fence(vacancy)
                                && vacancy.placement.assignment.is_none()
                                && next.assignment.is_some()
                                && next.epoch > vacancy.placement.epoch
                        }
                        Previous::Closed { predecessor: prior } => {
                            predecessor(prior) && next.epoch > prior.fence.placement.epoch
                        }
                    }
            }
            Self::ClaimWriter { fence } => held(fence),
            Self::ReadRoot { fence, writer } | Self::Write { fence, writer } => {
                held(fence) && applied(writer)
            }
            Self::ReadCustody {
                fence,
                writer,
                groups,
            } => held(fence) && applied(writer) && valid_groups(groups),
            Self::CustodyWrite {
                fence,
                writer,
                request_digest,
            } => held(fence) && applied(writer) && *request_digest != [0; 32],
            Self::ProvisionBranch { fence, writer, line, request_digest } =>
                held(fence) && applied(writer) && line.valid() && *request_digest != [0; 32],
            Self::OriginalFamilyWrite { fence, writer, branch, created, group, session, request_digest, authoring_controls } =>
                held(fence) && applied(writer) && branch != MAIN_BRANCH
                    && rtdb_types::rtdb_v1::valid_branch_name(branch)
                    && created.incarnation() == incarnation && created.position() > 0
                    && *group != [0; 16] && *session != [0; 32] && *request_digest != [0; 32]
                    && valid_family_authoring_controls(authoring_controls),
            Self::FundedAppointment(scope) => scope.valid(incarnation),
            Self::CloseWriter {
                fence,
                writer,
                closure,
            } => held(fence) && applied(writer) && *closure != [0; 16],
            Self::CloseWriterGuarded {
                fence,
                writer,
                closure,
                request_digest,
            } => {
                held(fence) && applied(writer) && *closure != [0; 16] && *request_digest != [0; 32]
            }
            Self::CloseEmptyAssignment {
                fence,
                closure,
                vacant_epoch,
            } => held(fence) && *closure != [0; 16] && *vacant_epoch > fence.placement.epoch,
            Self::ObservePlacement { fence: value } => fence(value),
            Self::PredecessorEvidence { predecessor: value } => predecessor(value),
            Self::ReceiptRecovery {
                original_namespace, family,
            } => *original_namespace != [0; 32] && *family != Family::OriginalFamilyWrite,
            Self::AttemptSettlement {
                original_namespace,
                family,
                key,
                request_digest,
                ..
            } => *original_namespace != [0; 32] && *family != Family::OriginalFamilyWrite
                && valid_key(key) && *request_digest != [0; 32],
        }
    }

    pub fn original_family_claim_scope(
        &self, namespace: [u8; 32],
    ) -> Result<rtdb_crypt::tokens::OriginalFamilyClaimScope<'_>> {
        let Self::OriginalFamilyWrite { fence, writer, branch, created, group, session, .. } = self else {
            return Err(denied());
        };
        if namespace == [0; 32] || *session == [0; 32] {
            return Err(denied());
        }
        let mut hash = Sha256::new();
        hash.update(b"rtdb.v1/original-family-claim\0");
        hash.update(namespace);
        hash.update(session);
        hash.update(serde_json::to_vec(&(fence, writer, branch, created, group)).map_err(|_| denied())?);
        Ok(rtdb_crypt::tokens::OriginalFamilyClaimScope { binding: hash.finalize().into(), branch, created: *created })
    }

    pub fn assignment_binding(&self) -> Result<Option<[u8; 32]>> {
        let (fence, writer) = match self {
            Self::ReadRoot { fence, writer }
            | Self::ReadCustody { fence, writer, .. }
            | Self::Write { fence, writer }
            | Self::CustodyWrite { fence, writer, .. }
            | Self::ProvisionBranch { fence, writer, .. }
            | Self::CloseWriterGuarded { fence, writer, .. } => (fence, writer),
            _ => return Ok(None),
        };
        let bytes = serde_json::to_vec(&(fence, writer)).map_err(|_| denied())?;
        let mut hash = Sha256::new();
        hash.update(b"rtdb.v1/managed-assignment-claim\0");
        hash.update(bytes);
        Ok(Some(hash.finalize().into()))
    }

    pub fn family(&self) -> Option<Family> {
        Some(match self {
            Self::AdvancePlacement { .. } => Family::AdvancePlacement,
            Self::ClaimWriter { .. } => Family::ClaimWriter,
            Self::Write { .. } => Family::Write,
            Self::CustodyWrite { .. } => Family::CustodyWrite,
            Self::OriginalFamilyWrite { .. } => Family::OriginalFamilyWrite,
            Self::ProvisionBranch { .. } => Family::ProvisionBranch,
            Self::CloseWriter { .. } | Self::CloseWriterGuarded { .. } => Family::CloseWriter,
            Self::CloseEmptyAssignment { .. } => Family::CloseEmptyAssignment,
            _ => return None,
        })
    }
}

impl Placement {
    fn valid(&self) -> bool {
        self.epoch > 0
            && self
                .assignment
                .as_ref()
                .is_none_or(|a| a.assignment_id != [0; 16] && bounded(&a.holder))
    }
}

fn valid_groups(groups: &[[u8; 16]]) -> bool {
    (1..=32).contains(&groups.len())
        && groups.iter().all(|group| *group != [0; 16])
        && groups.windows(2).all(|pair| pair[0] < pair[1])
}

pub fn bounded(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 255
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

fn valid_key(key: &[u8]) -> bool {
    (16..=64).contains(&key.len())
}

pub fn denied() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Forbidden,
        "rtdb/managed-workload-authority",
        "The command requires exact managed workload authority",
    )
}

#[derive(Clone, Copy)]
struct Commitment {
    family: Family,
    digest: [u8; 32],
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Request {
    pub point: Point,
    pub namespace: [u8; 32],
    pub expires_at: Timestamp,
    pub operation: Operation,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum Operation {
    Submit {
        key: Vec<u8>,
        authority: Authority,
        transaction: Option<Transact>,
        digest: [u8; 32],
    },
    Settle {
        key: Vec<u8>,
        family: Family,
        digest: [u8; 32],
    },
    ImportedSettle {
        key: Vec<u8>,
        family: Family,
        digest: [u8; 32],
        after: Basis,
    },
    Status {
        key: Vec<u8>,
        family: Family,
    },
    ImportedStatus {
        key: Vec<u8>,
        family: Family,
        after: Basis,
    },
    Observe(Fence),
    ReadRoot {
        fence: Fence,
        writer: Applied,
    },
    ReadCustody {
        fence: Fence,
        writer: Applied,
        groups: Vec<[u8; 16]>,
    },
    Predecessor(Predecessor),
}

#[derive(Clone, Serialize, Deserialize)]
pub enum Reply {
    Receipt(Receipt),
    Placement(Evidence),
    Predecessor(Box<PredecessorEvidence>),
    ReadCut(BranchCut),
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Evidence {
    pub applied: Applied,
    pub placement: Placement,
    pub writer: String,
    pub predecessor_retirement_basis: Option<Basis>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct PredecessorEvidence {
    pub fence: Evidence,
    pub writer: Evidence,
    pub close: Evidence,
}

pub struct Admission {
    pub namespace: [u8; 32],
    ordering: Ordering,
    commitment: Commitment,
}

enum Ordering {
    Timed(Timestamp),
    Settle,
    ImportedPlacement,
}

pub enum Prepared {
    Answer(Reply),
    Append {
        transaction: Transact,
        admission: Admission,
    },
}

impl ImportedPlacement {
    pub fn new(point: Point, placement: Placement, decision: [u8; 32], basis: Basis, system_time: Timestamp) -> Self {
        Self { point, placement, decision, application_cut: BranchCut { basis, system_time } }
    }

    fn namespace(&self) -> [u8; 32] {
        Sha256::digest(b"rtdb.multi/classic-imported-placement/v1").into()
    }

    fn commitment(&self) -> Result<Commitment> {
        let mut digest = Sha256::new();
        digest.update(b"rtdb.multi/classic-imported-placement/fence/v1\0");
        digest.update(serde_json::to_vec(&(&self.point, &self.placement, self.decision, self.application_cut)).map_err(|_| denied())?);
        Ok(Commitment { family: Family::AdvancePlacement, digest: digest.finalize().into() })
    }

    pub fn recover(&self, state: &TransactState, now: Timestamp) -> Result<Option<Fence>> {
        let namespace = self.namespace();
        let Some(receipt) = state.recover_workload(namespace, &self.decision, self.commitment()?)? else {
            return Ok(None);
        };
        let fence = Fence {
            placement: self.placement.clone(),
            applied: Applied {
                namespace, key: self.decision.to_vec(), basis: receipt.basis.to_bytes(),
                system_time_micros: receipt.system_time.0,
            },
        };
        self.point.fence(state, &fence, now.max(receipt.system_time))?;
        Ok(Some(fence))
    }

    pub fn prepare(&self, state: &TransactState, now: Timestamp) -> Result<(Transact, Admission)> {
        if self.decision == [0; 32] || !self.placement.valid() || self.placement.assignment.is_none()
            || now < self.point.valid_at || state.has_applied_workload_placement()
        {
            return Err(denied());
        }
        let view = state.index.view(MAIN_BRANCH, At::Latest, Layer::Local, now)?;
        if view.table(&self.point.table).is_some_and(|table| table.ttl_micros.is_some()
            || table.visibility.is_some() || table.dropped)
        {
            return Err(denied());
        }
        let prior = state.index.view(MAIN_BRANCH, At::Exactly(self.application_cut.basis), Layer::Local, now)?;
        let row = view.row(&self.point.table, &self.point.id, self.point.valid_at).ok_or_else(denied)?;
        let original = prior.row(&self.point.table, &self.point.id, self.point.valid_at);
        let image = row.image_at(self.point.valid_at).ok_or_else(denied)?;
        let cells = &image.cells;
        let incarnation = state.index.head().basis.incarnation();
        let original_epoch = self.placement.epoch.checked_sub(1).ok_or_else(denied)?;
        if prior.cut() != self.application_cut || original != Some(row) || original_epoch == 0
            || image.custody_group.is_some() || image.custody_payload.is_some()
            || cells.get("incarnation") != Some(&Value::Binary(incarnation.to_vec()))
            || cells.get("kind") != Some(&Value::Utf8("hosted".into()))
            || cells.get("placement_epoch") != Some(&Value::Utf8(format!("{original_epoch:020}")))
            || !matches!(cells.get("placement_assignment"), Some(Value::Binary(assignment)) if assignment.len() == 16 && assignment.as_slice() != [0; 16].as_slice())
            || cells.get("writer") != Some(&Value::Utf8(String::new()))
        {
            return Err(denied());
        }
        let mut transaction = empty_transaction(self.decision.to_vec());
        self.point.write_placement(&mut transaction, incarnation,
            &self.placement, Cell::TxBasis, String::new(), None)?;
        Ok((transaction, Admission { namespace: self.namespace(), ordering: Ordering::ImportedPlacement, commitment: self.commitment()? }))
    }
}

impl Request {
    pub fn prepare(&self, state: &TransactState, now: Timestamp) -> Result<Prepared> {
        self.check_expiry(now)?;
        match &self.operation {
            Operation::Status { key, family } => {
                let stored = state.workload_receipt(self.namespace, key)?;
                if stored.workload != Some(*family) {
                    return Err(super::unknown_receipt());
                }
                Ok(Prepared::Answer(Reply::Receipt(replay(&stored.receipt))))
            }
            Operation::ImportedStatus { key, family, after } => {
                let stored = state.workload_receipt(self.namespace, key)?;
                if stored.workload != Some(*family) || stored.receipt.basis.incarnation() != after.incarnation()
                    || stored.receipt.basis <= *after
                {
                    return Err(super::unknown_receipt());
                }
                Ok(Prepared::Answer(Reply::Receipt(replay(&stored.receipt))))
            }
            Operation::ReadRoot { fence, writer } => {
                let authority = Authority::ReadRoot {
                    fence: fence.clone(),
                    writer: writer.clone(),
                };
                if !authority.valid(state.index.head().basis.incarnation()) {
                    return Err(denied());
                }
                self.point.check_current(state, now, &authority)?;
                Ok(Prepared::Answer(Reply::ReadCut(state.index.head())))
            }
            Operation::ReadCustody {
                fence,
                writer,
                groups,
            } => {
                let authority = Authority::ReadCustody {
                    fence: fence.clone(),
                    writer: writer.clone(),
                    groups: groups.clone(),
                };
                if !authority.valid(state.index.head().basis.incarnation()) {
                    return Err(denied());
                }
                self.point.check_current(state, now, &authority)?;
                for group in groups {
                    if !state.index.custody_group_live(*group)? {
                        return Err(stale());
                    }
                }
                Ok(Prepared::Answer(Reply::ReadCut(state.index.head())))
            }
            Operation::Observe(fence) => {
                let evidence = self.point.fence(state, fence, now)?;
                Ok(Prepared::Answer(Reply::Placement(evidence)))
            }
            Operation::Predecessor(predecessor) => {
                let evidence = self.point.predecessor(state, predecessor, now)?;
                Ok(Prepared::Answer(Reply::Predecessor(Box::new(evidence))))
            }
            Operation::Settle {
                key,
                family,
                digest,
            } => self.prepare_settlement(state, key, *family, *digest, None),
            Operation::ImportedSettle {
                key,
                family,
                digest,
                after,
            } => self.prepare_settlement(state, key, *family, *digest, Some(*after)),
            Operation::Submit {
                key,
                authority,
                transaction,
                digest,
            } => {
                if !authority.valid(state.index.head().basis.incarnation())
                    || matches!(authority, Authority::CloseWriter { closure, .. } | Authority::CloseWriterGuarded { closure, .. }
                        | Authority::CloseEmptyAssignment { closure, .. }
                        if key.as_slice() != closure.as_slice())
                {
                    return Err(denied());
                }
                if matches!(authority, Authority::CustodyWrite { request_digest, .. }
                    | Authority::OriginalFamilyWrite { request_digest, .. }
                    | Authority::ProvisionBranch { request_digest, .. }
                    | Authority::CloseWriterGuarded { request_digest, .. } if request_digest != digest)
                {
                    return Err(denied());
                }
                let commitment = Commitment {
                    family: authority.family().ok_or_else(denied)?,
                    digest: *digest,
                };
                if let Some(receipt) = state.recover_workload(self.namespace, key, commitment)? {
                    return Ok(Prepared::Answer(Reply::Receipt(receipt)));
                }
                if let Authority::OriginalFamilyWrite { .. } = authority {
                    for place in transaction.iter().flat_map(|tx| &tx.ops).filter_map(|op| match op {
                        Op::LandCustodyBranch { placements, .. } => Some(placements),
                        _ => None,
                    }).flatten() {
                        let stored = state.workload_receipt(self.namespace, &place.reserve_key)?;
                        if stored.workload != Some(Family::OriginalFamilyWrite)
                            || stored.receipt.basis != place.reserved_at || stored.receipt.outcome != Outcome::Then
                        {
                            return Err(denied());
                        }
                    }
                }
                let transaction =
                    self.point
                        .transaction(state, now, key, authority, transaction.as_ref())?;
                let cutoff = transaction
                    .window
                    .before
                    .map_or(self.expires_at, |before| before.min(self.expires_at));
                Ok(Prepared::Append {
                    transaction,
                    admission: Admission {
                        namespace: self.namespace,
                        ordering: Ordering::Timed(cutoff),
                        commitment,
                    },
                })
            }
        }
    }

    fn prepare_settlement(
        &self,
        state: &TransactState,
        key: &[u8],
        family: Family,
        digest: [u8; 32],
        after: Option<Basis>,
    ) -> Result<Prepared> {
        if family == Family::OriginalFamilyWrite {
            return Err(denied());
        }
        let head = state.index.head().basis;
        if after.is_some_and(|after| head.incarnation() != after.incarnation() || head < after) {
            return Err(stale());
        }
        let commitment = Commitment { family, digest };
        if let Some(receipt) = state.recover_workload(self.namespace, key, commitment)? {
            if after.is_some_and(|after| {
                receipt.basis.incarnation() != after.incarnation() || receipt.basis <= after
            }) {
                return Err(super::unknown_receipt());
            }
            return Ok(Prepared::Answer(Reply::Receipt(receipt)));
        }
        Ok(Prepared::Append {
            transaction: empty_transaction(key.to_vec()),
            admission: Admission {
                namespace: self.namespace,
                ordering: Ordering::Settle,
                commitment,
            },
        })
    }

    pub fn check_expiry(&self, now: Timestamp) -> Result<()> {
        if now >= self.expires_at {
            return Err(denied());
        }
        Ok(())
    }

    pub fn check_terminal(
        &self,
        state: &TransactState,
        now: Timestamp,
        claims: &dyn ClaimVerifier,
    ) -> Result<()> {
        self.check_effect(state, now)?;
        if let Operation::Submit {
            authority: Authority::CloseWriterGuarded { .. },
            transaction: Some(transaction),
            ..
        } = &self.operation
        {
            for guard in &transaction.requires {
                if !guard.evaluate(
                    &state.index,
                    now.max(state.index.head().system_time),
                    claims,
                    None,
                )? {
                    return Err(stale());
                }
            }
        }
        Ok(())
    }

    pub fn check_effect(&self, state: &TransactState, now: Timestamp) -> Result<()> {
        self.check_expiry(now)?;
        let Operation::Submit { authority, .. } = &self.operation else {
            return Err(denied());
        };
        self.point.check_current(state, now, authority)?;
        if let Authority::OriginalFamilyWrite { branch, created, .. } = authority {
            require_family_branch(state.index(), branch, *created, now)?;
        }
        Ok(())
    }
}

impl Point {
    fn transaction(
        &self,
        state: &TransactState,
        now: Timestamp,
        key: &[u8],
        authority: &Authority,
        transaction: Option<&Transact>,
    ) -> Result<Transact> {
        self.check_current(state, now, authority)?;
        let mut control = match (authority, transaction) {
            (Authority::CloseWriterGuarded { .. }, Some(guarded)) => {
                if guarded.key != key
                    || guarded.requires.is_empty()
                    || !guarded.guards.is_empty()
                    || !guarded.ops.is_empty()
                    || !guarded.otherwise.is_empty()
                    || guarded.custody_request_digest.is_some()
                    || guarded.requires.iter().any(|guard| {
                        guard.after()
                            || match guard {
                                Guard::Version { branch, .. }
                                | Guard::CountAtMost { branch, .. } => branch != MAIN_BRANCH,
                                Guard::Unwritten { .. } | Guard::RetainedStoreBasis { .. }
                                | Guard::StoreWriter { .. } | Guard::SetStoreWriter { .. } | Guard::ClaimStoreWriter { .. }
                                | Guard::StoreWriterGeneration { .. } => true,
                                Guard::Unchanged(_) => false,
                            }
                    })
                {
                    return Err(denied());
                }
                guarded.clone()
            }
            (Authority::CloseWriterGuarded { .. }, None) => return Err(denied()),
            _ => empty_transaction(key.to_vec()),
        };
        let (placement, fence_basis, writer, retirement) = match authority {
            Authority::AdvancePlacement { previous, next, .. } => {
                let retirement = match previous {
                    Previous::Closed { predecessor } => {
                        Some(Basis::from_bytes(&predecessor.close.basis)?)
                    }
                    Previous::Vacant { fence } => {
                        self.fence(state, fence, now)?.predecessor_retirement_basis
                    }
                    Previous::Empty => None,
                };
                (next.clone(), Cell::TxBasis, String::new(), retirement)
            }
            Authority::ClaimWriter { fence } => {
                let evidence = self.fence(state, fence, now)?;
                (
                    fence.placement.clone(),
                    binary_cell(fence.applied.basis),
                    hex(key),
                    evidence.predecessor_retirement_basis,
                )
            }
            Authority::CloseWriter { fence, closure, .. }
            | Authority::CloseWriterGuarded { fence, closure, .. } => {
                let evidence = self.fence(state, fence, now)?;
                (
                    fence.placement.clone(),
                    binary_cell(fence.applied.basis),
                    format!("closed:{}", hex(closure)),
                    evidence.predecessor_retirement_basis,
                )
            }
            Authority::CloseEmptyAssignment {
                fence,
                vacant_epoch,
                ..
            } => {
                let retirement = self.fence(state, fence, now)?.predecessor_retirement_basis;
                (
                    Placement {
                        epoch: *vacant_epoch,
                        assignment: None,
                    },
                    Cell::TxBasis,
                    String::new(),
                    retirement,
                )
            }
            Authority::Write { .. } | Authority::CustodyWrite { .. } => {
                let transaction = transaction.filter(|tx| tx.key == key).ok_or_else(denied)?;
                self.check_write(
                    state.index(),
                    transaction,
                    matches!(authority, Authority::CustodyWrite { .. }),
                )?;
                return Ok(transaction.clone());
            }
            Authority::ProvisionBranch { line, writer, .. } => {
                let transaction = transaction.filter(|tx| tx.key == key).ok_or_else(denied)?;
                line.check(self, writer, state.index(), transaction, now)?;
                return Ok(transaction.clone());
            }
            Authority::OriginalFamilyWrite { branch, created, group, authoring_controls, .. } => {
                let transaction = transaction.filter(|tx| tx.key == key).ok_or_else(denied)?;
                require_family_branch(state.index(), branch, *created, now)?;
                let reviewed_reserve = transaction.ops.iter().any(|op| matches!(op, Op::ReserveReviewedCustodyPlacement { .. }));
                if transaction.claims().count() != usize::from(reviewed_reserve)
                    || reviewed_reserve && !transaction.requires.iter().any(|guard| matches!(guard, Guard::Unchanged(_)))
                {
                    return Err(denied());
                }
                let reviewed = transaction.uses_reviewed_custody();
                let data_branch = if reviewed { MAIN_BRANCH } else { branch };
                let named_placement = !reviewed && transaction.ops.iter()
                    .filter(|op| matches!(op, Op::PlaceCustodyRow { .. })).count() == 1
                    && transaction.ops.iter().all(|op| matches!(op,
                        Op::Put { .. } | Op::Delete { .. } | Op::PlaceCustodyRow { .. } | Op::CreateCustodyGroup { .. }
                    ));
                let mut main_points = BTreeSet::new();
                if !transaction.uses_custody() || !transaction.otherwise.is_empty()
                    || transaction.custody_request_digest.is_some() || state.index.has_visibility_history()
                    || transaction.ops.iter().any(|op| match op {
                        Op::Put { branch: target, table, rows, .. } =>
                            *table == self.table || (target != data_branch
                                && !(named_placement && target == MAIN_BRANCH
                                    && authoring_controls.contains(table)
                                    && rows.iter().all(|row| {
                                        main_points.insert((table, &row.id))
                                            && ordered_authoring_control(transaction, table, row)
                                    })))
                                || rows.iter().any(|row| row.custody_group.is_some()
                                    || row.custody_payload.is_some() || !row.protected_columns.is_empty()
                                    || state.index.has_custody_address(table, &row.id)),
                        Op::Delete { branch: target, table, ids, .. } =>
                            target != data_branch || *table == self.table
                                || ids.iter().any(|id| state.index.has_custody_address(table, id)),
                        Op::PlaceCustodyRow { branch: target, table, row, .. } =>
                            target != branch || *table == self.table || row.custody_group != Some(*group),
                        Op::ReserveCustodyPlacement { group: target, reservation } =>
                            target != group || reservation.target.branch != *branch || reservation.target.table == self.table,
                        Op::ReserveReviewedCustodyPlacement { group: target, source, reservation, .. } =>
                            target != group || source.branch != *branch || reservation.target.branch != MAIN_BRANCH
                                || reservation.target.table == self.table,
                        Op::LandCustodyBranch { branch: source, placements } =>
                            source != branch || placements.len() != 1 || placements[0].table == self.table
                                || placements[0].row.custody_group != Some(*group),
                        Op::CreateCustodyGroup { group: target } => target != group,
                        _ => true,
                    })
                    || main_points.len() > 64
                {
                    return Err(denied());
                }
                return Ok(transaction.clone());
            }
            _ => return Err(denied()),
        };
        if transaction.is_some() && !matches!(authority, Authority::CloseWriterGuarded { .. }) {
            return Err(denied());
        }
        self.write_placement(&mut control, state.index.head().basis.incarnation(), &placement, fence_basis, writer, retirement)?;
        Ok(control)
    }

    fn write_placement(&self, control: &mut Transact, incarnation: [u8; 16], placement: &Placement,
        fence_basis: Cell, writer: String, retirement: Option<Basis>,
    ) -> Result<()> {
        let mut cells = StructFields::from_iter([
            ("incarnation".into(), binary_cell(incarnation)),
            ("kind".into(), text_cell("hosted".into())),
            (
                "placement_epoch".into(),
                text_cell(format!("{:020}", placement.epoch)),
            ),
            (
                "placement_assignment".into(),
                Cell::Value(Value::Binary(
                    placement
                        .assignment
                        .as_ref()
                        .map_or_else(Vec::new, |a| a.assignment_id.to_vec()),
                )),
            ),
            (
                "placement_holder".into(),
                text_cell(
                    placement
                        .assignment
                        .as_ref()
                        .map_or_else(String::new, |a| a.holder.clone()),
                ),
            ),
            ("placement_fence_basis".into(), fence_basis),
            ("writer".into(), text_cell(writer)),
        ]);
        if let Some(retirement) = retirement {
            cells.insert(
                "predecessor_retirement_basis".into(),
                binary_cell(retirement.to_bytes()),
            );
        }
        let valid = ValidTime::new(
            ValidTimeBound::Instant(self.valid_at),
            ValidTimeBound::Instant(Timestamp::MAX),
        )?;
        let row = Row {
            id: self.id.clone(),
            valid: Some(valid),
            cells,
            custody_group: None,
            protected_columns: Vec::new(),
            custody_payload: None,
        };
        control.ops.push(Op::Put {
            branch: MAIN_BRANCH.into(),
            table: self.table.clone(),
            rows: vec![row],
            if_absent: false,
        });
        Ok(())
    }

    fn check_write(&self, index: &BranchIndex, request: &Transact, custody: bool) -> Result<()> {
        if request.ops.is_empty() && request.otherwise.is_empty()
            || request.uses_custody() != custody
            || request.custody_request_digest.is_some()
            || index.has_visibility_history()
        {
            return Err(denied());
        }
        if request
            .guards
            .iter()
            .chain(&request.requires)
            .any(|guard| match guard {
                Guard::Version {
                    branch, table, id, ..
                } => branch != MAIN_BRANCH || !custody && index.has_custody_address(table, id),
                Guard::CountAtMost { branch, range, .. } => {
                    branch != MAIN_BRANCH || !custody && index.has_custody_relation(&range.table)
                }
                Guard::Unwritten { .. } | Guard::RetainedStoreBasis { .. }
                | Guard::StoreWriter { .. } | Guard::SetStoreWriter { .. } | Guard::ClaimStoreWriter { .. }
                | Guard::StoreWriterGeneration { .. } => true,
                Guard::Unchanged(_) => false,
            })
        {
            return Err(denied());
        }
        let mut placements = BTreeMap::<&TableRef, BTreeSet<&EntityId>>::new();
        for op in request.ops.iter().chain(&request.otherwise) {
            if let Op::PlaceCustodyRow { table, row, .. } = op {
                placements.entry(table).or_default().insert(&row.id);
            }
        }
        let protected = |table: &TableRef, id: &EntityId| {
            index.has_custody_address(table, id)
                || placements.get(table).is_some_and(|ids| ids.contains(id))
        };
        for op in request.ops.iter().chain(&request.otherwise) {
            match op {
                Op::Put {
                    branch,
                    table,
                    rows,
                    ..
                } if branch == MAIN_BRANCH
                    && *table != self.table
                    && rows.iter().all(|row| {
                        row.custody_group.is_none()
                            && row.custody_payload.is_none()
                            && row.protected_columns.is_empty()
                            && !protected(table, &row.id)
                    }) => {}
                Op::Delete {
                    branch, table, ids, ..
                } if branch == MAIN_BRANCH
                    && *table != self.table
                    && ids.iter().all(|id| !protected(table, id)) => {}
                Op::CreateCustodyGroup { group } if custody && *group != [0; 16] => {}
                Op::ReserveCustodyPlacement { group, reservation }
                    if custody
                        && *group != [0; 16]
                        && reservation.target.branch == MAIN_BRANCH
                        && reservation.target.table != self.table => {}
                Op::PlaceCustodyRow {
                    branch,
                    table,
                    row,
                    placement,
                } if custody
                    && branch == MAIN_BRANCH
                    && *table != self.table
                    && row.custody_group.is_some_and(|group| group != [0; 16])
                    && !row.protected_columns.is_empty()
                    && row.custody_payload.is_none()
                    && placement.commitments.is_none() => {}
                Op::Declare { table, change }
                    if *table != self.table
                        && !matches!(
                            change,
                            Declaration::Drop
                                | Declaration::Visibility(_)
                                | Declaration::SingleLabelPerTx
                        )
                        && (!matches!(change, Declaration::Ttl(_))
                            || !index.has_custody_relation(table)) => {}
                _ => return Err(denied()),
            }
        }
        Ok(())
    }

    fn check_current(
        &self,
        state: &TransactState,
        now: Timestamp,
        authority: &Authority,
    ) -> Result<()> {
        let now = now.max(state.index.head().system_time);
        if now < self.valid_at {
            return Err(stale());
        }
        let expected = match authority {
            Authority::AdvancePlacement { previous, .. } => match previous {
                Previous::Empty => {
                    let view = state
                        .index
                        .view(MAIN_BRANCH, At::Latest, Layer::Local, now)?;
                    if view.history(&self.table, &self.id).is_empty()
                        && view.table(&self.table).is_none_or(|table| {
                            table.ttl_micros.is_none()
                                && table.visibility.is_none()
                                && !table.dropped
                        })
                        && !state.has_applied_workload_placement()
                    {
                        return Ok(());
                    }
                    return Err(stale());
                }
                Previous::Vacant { fence } => self.fence(state, fence, now)?.applied,
                Previous::Closed { predecessor } => {
                    self.predecessor(state, predecessor, now)?.close.applied
                }
            },
            Authority::ClaimWriter { fence } | Authority::CloseEmptyAssignment { fence, .. } => {
                self.fence(state, fence, now)?.applied
            }
            Authority::ReadRoot { fence, writer }
            | Authority::ReadCustody { fence, writer, .. }
            | Authority::Write { fence, writer }
            | Authority::CustodyWrite { fence, writer, .. }
            | Authority::ProvisionBranch { fence, writer, .. }
            | Authority::OriginalFamilyWrite { fence, writer, .. }
            | Authority::CloseWriter { fence, writer, .. }
            | Authority::CloseWriterGuarded { fence, writer, .. } => {
                self.fence(state, fence, now)?;
                self.writer(state, fence, writer, now)?.applied
            }
            _ => return Err(denied()),
        };
        let fence = CustodyWriterFence {
            table: self.table.clone(),
            id: self.id.clone(),
            valid_at: self.valid_at,
            written: Basis::from_bytes(&expected.basis)?,
        };
        if fence.matches(&state.index, now)? {
            Ok(())
        } else {
            Err(stale())
        }
    }

    fn fence(&self, state: &TransactState, fence: &Fence, now: Timestamp) -> Result<Evidence> {
        self.evidence(
            state,
            &fence.applied,
            fence,
            Family::AdvancePlacement,
            String::new(),
            now,
        )
    }

    fn writer(
        &self,
        state: &TransactState,
        fence: &Fence,
        writer: &Applied,
        now: Timestamp,
    ) -> Result<Evidence> {
        if Basis::from_bytes(&writer.basis)? <= Basis::from_bytes(&fence.applied.basis)? {
            return Err(stale());
        }
        self.evidence(
            state,
            writer,
            fence,
            Family::ClaimWriter,
            hex(&writer.key),
            now,
        )
    }

    fn predecessor(
        &self,
        state: &TransactState,
        predecessor: &Predecessor,
        now: Timestamp,
    ) -> Result<PredecessorEvidence> {
        let Predecessor {
            fence,
            writer,
            close,
            closure,
        } = predecessor;
        if close.key.as_slice() != closure.as_slice()
            || Basis::from_bytes(&close.basis)? <= Basis::from_bytes(&writer.basis)?
        {
            return Err(stale());
        }
        let fence_evidence = self.fence(state, fence, now)?;
        let writer_evidence = self.writer(state, fence, writer, now)?;
        let close_evidence = self.evidence(
            state,
            close,
            fence,
            Family::CloseWriter,
            format!("closed:{}", hex(closure)),
            now,
        )?;
        Ok(PredecessorEvidence {
            fence: fence_evidence,
            writer: writer_evidence,
            close: close_evidence,
        })
    }

    fn evidence(
        &self,
        state: &TransactState,
        applied: &Applied,
        fence: &Fence,
        family: Family,
        writer: String,
        now: Timestamp,
    ) -> Result<Evidence> {
        let stored = state.workload_receipt(applied.namespace, &applied.key)?;
        let receipt = &stored.receipt;
        let basis = Basis::from_bytes(&applied.basis)?;
        let expected_family = stored.workload == Some(family)
            || family == Family::AdvancePlacement
                && fence.placement.assignment.is_none()
                && stored.workload == Some(Family::CloseEmptyAssignment);
        if !expected_family
            || receipt.basis != basis
            || receipt.system_time != Timestamp(applied.system_time_micros)
            || !matches!(receipt.outcome, Outcome::Then)
        {
            return Err(stale());
        }
        let view = state
            .index
            .view(MAIN_BRANCH, At::Exactly(basis), Layer::Local, now)?;
        if view.cut().system_time != receipt.system_time {
            return Err(stale());
        }
        let row_at = |valid_at| {
            view.row(&self.table, &self.id, valid_at)
                .filter(|row| {
                    row.written == rtdb_index::branches::WrittenVersion::Branch
                        && row.cut.basis == basis
                        && row.cut.system_time == receipt.system_time
                })
                .and_then(|row| row.image_at(valid_at))
                .ok_or_else(stale)
        };
        let row = row_at(self.valid_at)?;
        if row_at(receipt.system_time)? != row {
            return Err(stale());
        }
        let text = |column| match row.cells.get(column) {
            Some(Value::Utf8(value)) => Some(value.as_str()),
            _ => None,
        };
        let binary = |column| match row.cells.get(column) {
            Some(Value::Binary(value)) => Some(value.as_slice()),
            _ => None,
        };
        let assignment = fence.placement.assignment.as_ref();
        if text("kind") != Some("hosted")
            || text("placement_epoch") != Some(format!("{:020}", fence.placement.epoch).as_str())
            || text("placement_holder") != Some(assignment.map_or("", |a| a.holder.as_str()))
            || text("writer") != Some(writer.as_str())
            || binary("incarnation") != Some(basis.incarnation().as_slice())
            || binary("placement_assignment")
                != Some(assignment.map_or(&[][..], |a| a.assignment_id.as_slice()))
            || binary("placement_fence_basis") != Some(fence.applied.basis.as_slice())
        {
            return Err(stale());
        }
        let predecessor_retirement_basis = binary("predecessor_retirement_basis")
            .map(Basis::from_bytes)
            .transpose()?;
        Ok(Evidence {
            applied: applied.clone(),
            placement: fence.placement.clone(),
            writer,
            predecessor_retirement_basis,
        })
    }
}

fn ordered_authoring_control(transaction: &Transact, table: &TableRef, row: &Row) -> bool {
    let Some(ValidTime::Literal(valid)) = row.valid else { return false; };
    if valid.lower() != Timestamp(0) || valid.upper() != Timestamp::MAX
        || !matches!(&row.id, EntityId::String(id) if bounded(id))
    {
        return false;
    }
    transaction.requires.iter().any(|guard| matches!(guard, Guard::Version {
        branch, table: guarded_table, id, valid_at: Some(Timestamp(0)),
        expect: Version::Absent | Version::Written(_),
    } if branch == MAIN_BRANCH && guarded_table == table && id == &row.id))
}

pub fn valid_family_authoring_controls(tables: &BTreeSet<TableRef>) -> bool {
    tables.len() <= 16 && tables.iter().all(provision::application_table)
}

impl TransactState {
    fn workload_receipt(&self, namespace: [u8; 32], key: &[u8]) -> Result<&StoredReceipt> {
        self.lookup_receipt(key, Some(namespace))
    }

    fn recover_workload(
        &self,
        namespace: [u8; 32],
        key: &[u8],
        commitment: Commitment,
    ) -> Result<Option<Receipt>> {
        match self.find_receipt(key, Some(namespace))? {
            Some(stored)
                if stored.workload == Some(commitment.family)
                    && stored.matches_workload_digest(commitment.digest) =>
            {
                Ok(Some(replay(&stored.receipt)))
            }
            Some(_) => Err(conflicting_key()),
            None => Ok(None),
        }
    }

    fn has_applied_workload_placement(&self) -> bool {
        self.receipts
            .values()
            .flat_map(|receipts| receipts.values())
            .any(|stored| {
                stored.workload == Some(Family::AdvancePlacement)
                    && matches!(stored.receipt.outcome, Outcome::Then)
            })
    }
}

impl Admission {
    pub fn cutoff(&self) -> Option<Timestamp> {
        match self.ordering {
            Ordering::Timed(until) => Some(until),
            Ordering::Settle | Ordering::ImportedPlacement => None,
        }
    }

    pub fn family(&self) -> Family {
        self.commitment.family
    }

    pub(super) fn authorization_digest(&self) -> [u8; 32] {
        self.commitment.digest
    }

    pub(super) fn settlement(&self) -> Option<Outcome> {
        matches!(self.ordering, Ordering::Settle).then(|| Outcome::Refused {
            error: Anomaly::new(
                AnomalyCategory::Conflict,
                "rtdb/managed-workload-attempt-settled",
                "Settlement closed the original attempt before it applied",
            ),
        })
    }
}

fn empty_transaction(key: Vec<u8>) -> Transact {
    Transact {
        key,
        require: Vec::new(),
        window: Window::default(),
        guards: Vec::new(),
        ops: Vec::new(),
        otherwise: Vec::new(),
        requires: Vec::new(),
        custody_request_digest: None,
        request_memory: None,
    }
}

fn binary_cell<const N: usize>(value: [u8; N]) -> Cell {
    Cell::Value(Value::Binary(value.to_vec()))
}
fn text_cell(value: String) -> Cell {
    Cell::Value(Value::Utf8(value))
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(result, "{byte:02x}").expect("writing a String cannot fail");
    }
    result
}

fn stale() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict,
        "rtdb/managed-workload-fence",
        "The exact managed placement or writer evidence does not match the Original",
    )
}

#[cfg(test)]
mod tests;
