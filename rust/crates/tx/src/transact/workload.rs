use super::*;

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
    Write {
        fence: Fence,
        writer: Applied,
    },
    CloseWriter {
        fence: Fence,
        writer: Applied,
        closure: [u8; 16],
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
            Self::CloseWriter {
                fence,
                writer,
                closure,
            } => held(fence) && applied(writer) && *closure != [0; 16],
            Self::CloseEmptyAssignment {
                fence,
                closure,
                vacant_epoch,
            } => held(fence) && *closure != [0; 16] && *vacant_epoch > fence.placement.epoch,
            Self::ObservePlacement { fence: value } => fence(value),
            Self::PredecessorEvidence { predecessor: value } => predecessor(value),
            Self::ReceiptRecovery {
                original_namespace, ..
            } => *original_namespace != [0; 32],
            Self::AttemptSettlement {
                original_namespace,
                key,
                request_digest,
                ..
            } => *original_namespace != [0; 32] && valid_key(key) && *request_digest != [0; 32],
        }
    }

    pub fn family(&self) -> Option<Family> {
        Some(match self {
            Self::AdvancePlacement { .. } => Family::AdvancePlacement,
            Self::ClaimWriter { .. } => Family::ClaimWriter,
            Self::Write { .. } => Family::Write,
            Self::CloseWriter { .. } => Family::CloseWriter,
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
    Status {
        key: Vec<u8>,
        family: Family,
    },
    Observe(Fence),
    ReadRoot {
        fence: Fence,
        writer: Applied,
    },
    Predecessor(Predecessor),
}

#[derive(Clone, Serialize, Deserialize)]
pub enum Reply {
    Receipt(Receipt),
    Placement(Evidence),
    Predecessor(Box<PredecessorEvidence>),
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
    cutoff: Option<Timestamp>,
    commitment: Commitment,
}

pub enum Prepared {
    Answer(Reply),
    Append {
        transaction: Transact,
        admission: Admission,
    },
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
            Operation::ReadRoot { fence, writer } => {
                let authority = Authority::ReadRoot {
                    fence: fence.clone(),
                    writer: writer.clone(),
                };
                if !authority.valid(state.index.head().basis.incarnation()) {
                    return Err(denied());
                }
                self.point.check_current(state, now, &authority)?;
                let evidence = self.point.writer(state, fence, writer, now)?;
                Ok(Prepared::Answer(Reply::Placement(evidence)))
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
            } => {
                let commitment = Commitment {
                    family: *family,
                    digest: *digest,
                };
                if let Some(receipt) = state.recover_workload(self.namespace, key, commitment)? {
                    return Ok(Prepared::Answer(Reply::Receipt(receipt)));
                }
                Ok(Prepared::Append {
                    transaction: empty_transaction(key.clone()),
                    admission: Admission {
                        namespace: self.namespace,
                        cutoff: None,
                        commitment,
                    },
                })
            }
            Operation::Submit {
                key,
                authority,
                transaction,
                digest,
            } => {
                if !authority.valid(state.index.head().basis.incarnation())
                    || matches!(authority, Authority::CloseWriter { closure, .. } | Authority::CloseEmptyAssignment { closure, .. }
                        if key.as_slice() != closure.as_slice())
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
                        cutoff: Some(cutoff),
                        commitment,
                    },
                })
            }
        }
    }

    pub fn check_expiry(&self, now: Timestamp) -> Result<()> {
        if now >= self.expires_at {
            return Err(denied());
        }
        Ok(())
    }

    pub fn check_effect(&self, state: &TransactState, now: Timestamp) -> Result<()> {
        self.check_expiry(now)?;
        let Operation::Submit { authority, .. } = &self.operation else {
            return Err(denied());
        };
        self.point.check_current(state, now, authority)
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
        let mut control = empty_transaction(key.to_vec());
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
            Authority::CloseWriter { fence, closure, .. } => {
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
            Authority::Write { .. } => {
                let transaction = transaction.filter(|tx| tx.key == key).ok_or_else(denied)?;
                self.check_write(transaction)?;
                return Ok(transaction.clone());
            }
            _ => return Err(denied()),
        };
        if transaction.is_some() {
            return Err(denied());
        }
        let incarnation = state.index.head().basis.incarnation();
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
        Ok(control)
    }

    fn check_write(&self, request: &Transact) -> Result<()> {
        if request.ops.is_empty() && request.otherwise.is_empty()
            || request.uses_custody()
            || request.custody_request_digest.is_some()
        {
            return Err(denied());
        }
        if request
            .guards
            .iter()
            .chain(&request.requires)
            .any(|guard| match guard {
                Guard::Version { branch, .. } | Guard::CountAtMost { branch, .. } => {
                    branch != MAIN_BRANCH
                }
                Guard::Unchanged(_) => false,
            })
        {
            return Err(denied());
        }
        for op in request.ops.iter().chain(&request.otherwise) {
            match op {
                Op::Put { branch, table, .. } | Op::Delete { branch, table, .. }
                    if branch == MAIN_BRANCH && *table != self.table => {}
                Op::Declare { table, change }
                    if *table != self.table && !matches!(change, Declaration::Drop) => {}
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
            | Authority::Write { fence, writer }
            | Authority::CloseWriter { fence, writer, .. } => {
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
        match self.workload_receipt(namespace, key) {
            Ok(stored)
                if stored.workload == Some(commitment.family)
                    && stored.fingerprint == commitment.digest =>
            {
                Ok(Some(replay(&stored.receipt)))
            }
            Ok(_) => Err(conflicting_key()),
            Err(error) if error.code == "rtdb/unknown-transaction" => Ok(None),
            Err(error) => Err(error),
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
        self.cutoff
    }

    pub(super) fn family(&self) -> Family {
        self.commitment.family
    }

    pub(super) fn digest(&self) -> [u8; 32] {
        self.commitment.digest
    }

    pub(super) fn settlement(&self) -> Option<Outcome> {
        self.cutoff.is_none().then(|| Outcome::Refused {
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
