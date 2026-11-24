use std::{collections::{BTreeMap, BTreeSet}, sync::Arc};

use bytes::Bytes;
use rtdb_index::branches::{
    BranchCut, BranchEffect, BranchIndex, BranchTable, BranchView, RowImage, VisibleHistory,
};
pub use rtdb_index::branches::{
    CountRange, CustodyGroupId, CustodyOccurrence, CustodyPayload, CustodyRelease, CustodyReservation,
    CustodyPlacementId, CustodyTarget, GroupedCount, IndexDefinition, Visibility,
};
use rtdb_types::{
    Anomaly, AnomalyCategory, EntityId, Interval, LogicalType, ReplicaPosition, Result, SourcePosition, StructFields, TableRef,
    Term, TimeUnit, Timestamp, Value,
    rtdb_v1::{At, Basis, Branch, Claim, ClaimVerifier, LabelScope, Layer, MAIN_BRANCH, valid_branch_name},
};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::value::{RawValue, to_raw_value};
use sha2::{Digest, Sha256};

mod custody_release;
mod operations;
mod pipeline;

pub use custody_release::{CUSTODY_RELEASE_NAMESPACE, CustodyWriterFence, TrustedCustodyRelease};
pub use pipeline::{Decision, Terminal, TransactPipeline, TxKey};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Window {
    pub not_before: Option<Timestamp>,
    pub before: Option<Timestamp>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Transact {
    pub key: Vec<u8>,
    #[serde(serialize_with = "serialize_features")]
    pub require: Vec<String>,
    pub window: Window,
    pub guards: Vec<Guard>,
    pub ops: Vec<Op>,
    pub otherwise: Vec<Op>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requires: Vec<Guard>,
    #[serde(skip)]
    pub custody_request_digest: Option<[u8; 32]>,
    #[serde(skip)]
    pub request_memory: Option<Arc<rtdb_columnar::MemoryReservation>>,
}

impl Transact {
    pub fn claims(&self) -> impl Iterator<Item = &Claim> {
        self.requires.iter().chain(&self.guards).filter_map(|guard| match guard {
            Guard::Unchanged(claim) => Some(claim),
            _ => None,
        })
    }

    pub fn logical_sha256(&self) -> Result<[u8; 32]> {
        Ok(Sha256::digest(self.canonical_request()?.get().as_bytes()).into())
    }

    pub fn canonical_request(&self) -> Result<Box<RawValue>> {
        to_raw_value(self).map_err(|error| Anomaly::incorrect("rtdb/transaction-encoding", error.to_string()))
    }

    /// Decode a request captured by an input log. The caller still resolves it
    /// at the writer's ordered prefix with the route's verified authority.
    pub fn decode_canonical_request(bytes: &[u8]) -> Result<Self> {
        let request: Self = serde_json::from_slice(bytes).map_err(|error| {
            Anomaly::incorrect("rtdb/transaction-encoding", error.to_string())
        })?;
        validate_key(&request.key)?;
        if request.canonical_request()?.get().as_bytes() != bytes {
            return Err(Anomaly::incorrect("rtdb/transaction-encoding", "The logged transaction request is not canonical"));
        }
        Ok(request)
    }

    pub fn uses_custody(&self) -> bool { self.ops.iter().chain(&self.otherwise).any(Op::uses_custody) }

    pub fn uses_erasure(&self) -> bool {
        self.ops.iter().chain(&self.otherwise).any(|op| matches!(op, Op::Erase { .. }))
    }

    pub fn is_response_release(&self) -> bool {
        matches!(self.ops.as_slice(), [Op::CustodyRelease(_)])
            && self.otherwise.is_empty() && self.guards.is_empty()
            && matches!(self.requires.as_slice(),
                [Guard::Version { branch, valid_at: Some(_), expect: Version::Written(_), .. }] if branch == MAIN_BRANCH)
            && self.window == Window::default()
    }

    pub fn contains_response_release(&self) -> bool {
        self.ops.iter().chain(&self.otherwise).any(|op| matches!(op, Op::CustodyRelease(_)))
    }

    pub fn receipt_namespace(&self, scope: Option<[u8; 32]>) -> Option<[u8; 32]> {
        scope.or_else(|| self.is_response_release().then_some(CUSTODY_RELEASE_NAMESPACE))
    }

    pub fn require_sealed_custody(&self, features: &[&str]) -> Result<()> {
        if !self.has_unsealed_custody() { return Ok(()); }
        Err(if features.contains(&ROW_CUSTODY) {
            Anomaly::incorrect("rtdb/custody-payload", "Protected columns must be sealed before admission")
        } else {
            feature_required(ROW_CUSTODY)
        })
    }

    fn has_unsealed_custody(&self) -> bool {
        self.ops.iter().chain(&self.otherwise).any(|op| {
            op.put_rows().iter().any(|row| {
                !row.protected_columns.is_empty() && (row.custody_payload.is_none()
                    || row.protected_columns.iter().any(|column| row.cells.get(column).is_some()))
            })
        })
    }
}

fn serialize_features<S: Serializer>(features: &[String], serializer: S) -> std::result::Result<S::Ok, S::Error> {
    // Flight already orders and deduplicates its feature list.
    if features.windows(2).all(|pair| pair[0] < pair[1]) {
        return features.serialize(serializer);
    }
    features.iter().collect::<BTreeSet<_>>().serialize(serializer)
}

fn serialize_cells<S: Serializer>(cells: &StructFields<Cell>, serializer: S) -> std::result::Result<S::Ok, S::Error> {
    if cells.keys().is_sorted() {
        return cells.serialize(serializer);
    }
    cells.iter().collect::<BTreeMap<_, _>>().serialize(serializer)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Guard {
    Unchanged(Claim),
    Version {
        branch: Branch,
        table: TableRef,
        id: EntityId,
        valid_at: Option<Timestamp>,
        expect: Version,
    },
    CountAtMost { branch: Branch, range: CountRange, when: When },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Version { Absent, Present, Written(Basis) }

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum When { Before, After }

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Cell { Value(Value), TxTime, TxBasis }

impl Cell {
    fn resolve(&self, cut: BranchCut) -> Value {
        match self {
            Self::Value(value) => value.clone(),
            Self::TxTime => Value::Timestamp {
                value: cut.system_time.0, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()),
            },
            Self::TxBasis => Value::Binary(cut.basis.to_bytes().to_vec()),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Row {
    pub id: EntityId,
    pub valid: Option<ValidTime>,
    #[serde(serialize_with = "serialize_cells")]
    pub cells: StructFields<Cell>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custody_group: Option<CustodyGroupId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[serde(serialize_with = "serialize_features")]
    pub protected_columns: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custody_payload: Option<CustodyPayload>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ValidTime {
    Literal(Interval),
    Relative { from: ValidTimeBound, to: ValidTimeBound },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValidTimeBound {
    Instant(Timestamp),
    TransactionMicros(i64),
}

impl ValidTime {
    pub fn new(from: ValidTimeBound, to: ValidTimeBound) -> Result<Self> {
        match (from, to) {
            (ValidTimeBound::Instant(from), ValidTimeBound::Instant(to)) => {
                Ok(Self::Literal(Interval::new(from, to)?))
            }
            (from, to) => Ok(Self::Relative { from, to }),
        }
    }

    pub fn resolve(self, time: Timestamp) -> Result<Interval> {
        let bound = |bound| match bound {
            ValidTimeBound::Instant(at) => Ok(at),
            ValidTimeBound::TransactionMicros(offset) => time.0.checked_add(offset).map(Timestamp).ok_or_else(|| {
                Anomaly::incorrect("rtdb/invalid-valid-time", "The transaction-relative valid time is out of range")
            }),
        };
        match self {
            Self::Literal(interval) => Ok(interval),
            Self::Relative { from, to } => Interval::new(bound(from)?, bound(to)?),
        }
    }

    fn is_literal(self) -> bool { matches!(self, Self::Literal(_)) }
}

impl From<Interval> for ValidTime {
    fn from(interval: Interval) -> Self { Self::Literal(interval) }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Op {
    Put { branch: Branch, table: TableRef, rows: Vec<Row>, if_absent: bool },
    Delete { branch: Branch, table: TableRef, ids: Vec<EntityId>, valid: Option<ValidTime> },
    Erase { branch: Option<Branch>, table: TableRef, ids: Vec<EntityId> },
    Declare { table: TableRef, change: Declaration },
    Fork { child: Branch, from: (Branch, Basis), writable: bool, expires_at: Option<Timestamp> },
    Renew { branch: Branch, expires_at: Timestamp },
    Merge { from: Branch, into: Branch },
    DropBranch { branch: Branch },
    CreateCustodyGroup { group: CustodyGroupId },
    RevokeCustodyGroup { group: CustodyGroupId, inventory_sha256: [u8; 32] },
    ReserveCustodyPlacement { group: CustodyGroupId, reservation: CustodyReservation },
    PlaceCustodyRow { branch: Branch, table: TableRef, row: Row, placement: CustodyPlacement },
    #[serde(skip_deserializing)]
    CustodyRelease(CustodyRelease),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CustodyPlacement {
    pub placement_id: CustodyPlacementId,
    pub reservation: Option<[u8; 32]>,
    pub carrier_column: String,
    pub link_column: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commitments: Option<CustodyPlacementCommitments>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct CustodyPlacementCommitments {
    pub carrier_sha256: [u8; 32],
    pub link_sha256: Option<[u8; 32]>,
}

impl Op {
    fn put_rows(&self) -> &[Row] {
        match self {
            Self::Put { rows, .. } => rows,
            Self::PlaceCustodyRow { row, .. } => std::slice::from_ref(row),
            _ => &[],
        }
    }

    fn uses_custody(&self) -> bool {
        match self {
            Self::CreateCustodyGroup { .. } | Self::RevokeCustodyGroup { .. }
            | Self::ReserveCustodyPlacement { .. } | Self::PlaceCustodyRow { .. } | Self::CustodyRelease(_) => true,
            Self::Put { rows, .. } => rows.iter().any(|row| row.custody_group.is_some() || !row.protected_columns.is_empty()),
            _ => false,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Declaration {
    Schema(StructFields<LogicalType>),
    Index(IndexDefinition),
    Ttl(Option<u64>),
    Constraint { name: String, range: Option<CountRange> },
    GroupedConstraint { name: String, rule: GroupedCount },
    Drop,
    Visibility(Visibility),
    SingleLabelPerTx,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Outcome {
    Then,
    Otherwise { failed: Vec<usize> },
    Refused { error: Anomaly },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Receipt {
    pub key: Vec<u8>,
    pub basis: Basis,
    pub system_time: Timestamp,
    pub outcome: Outcome,
    pub replayed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub erasure: Option<Erasure>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Erasure {
    Pending,
    Armed { unreadable_by: Timestamp },
}

pub const TIMED_TRANSACT: &str = "timed-transact";
pub const LINEARIZABLE: &str = "linearizable";
pub const TRANSACT_REQUIRES: &str = "transact.requires";
pub const RELATIVE_VALID_TIME: &str = "transact.relative-valid-time";
pub const ROW_CUSTODY: &str = "row-custody";
pub const PHYSICAL_ERASURE: &str = "erasure";
pub const COUNT_RANGE_CONSTRAINT: &str = "declare.constraint.count-range";
pub const GROUPED_COUNT_CONSTRAINT: &str = "declare.constraint.grouped-count";
pub const REQUIREMENT_FAILED: &str = "rtdb/requirement-failed";

#[derive(Clone, Copy)]
pub struct ScopedTransact<'a> {
    pub receipt_namespace: [u8; 32],
    pub labels: &'a LabelScope,
}

pub enum Resolution {
    Answer(Receipt),
    Append(PreparedAppend),
}

pub struct PreparedAppend {
    position: u64,
    record: TransactionRecord,
}

impl PreparedAppend {
    pub fn position(&self) -> u64 { self.position }
    pub fn record(&self) -> &TransactionRecord { &self.record }
    pub fn into_record(self) -> TransactionRecord { self.record }

    /// Call only after the original append durably acknowledges under this writer.
    pub fn after_original_ack(&self, clock_upper: Timestamp, terminal_position: u64) -> Option<TransactionRecord> {
        let TransactionRecord::Tx(tx) = &self.record else { return None; };
        let before = tx.before?;
        let tx_lsn = self.position;
        if clock_upper < before && tx.receipt.system_time < before
            && terminal_position == tx.receipt.basis.position()
        {
            Some(TransactionRecord::Settle { tx_lsn })
        } else {
            Some(TransactionRecord::Refuse { tx_lsn })
        }
    }
}

const RECORD_VERSION: u8 = 2;

#[derive(Debug, Serialize, Deserialize)]
pub enum TransactionRecord {
    Tx(ResolvedTransact),
    Settle { tx_lsn: u64 },
    Refuse { tx_lsn: u64 },
}

impl TransactionRecord {
    pub fn encode(&self) -> Result<Bytes> {
        if matches!(self, Self::Tx(tx) if tx.request.is_none()) {
            return Err(Anomaly::fault("rtdb/transaction-encoding", "An admitted transaction needs its canonical request"));
        }
        serde_json::to_vec(&(RECORD_VERSION, self)).map(Bytes::from)
            .map_err(|error| Anomaly::fault("rtdb/transaction-encoding", error.to_string()))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let (version, record): (u8, Self) = serde_json::from_slice(bytes)
            .map_err(|error| Anomaly::fault("rtdb/transaction-decoding", error.to_string()))?;
        if version != RECORD_VERSION {
            return Err(Anomaly::unsupported("rtdb/transaction-version", "Unknown resolved transaction version"));
        }
        if let Self::Tx(tx) = &record {
            tx.validate_admission()?;
        }
        Ok(record)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResolvedTransact {
    fingerprint: [u8; 32],
    receipt: Receipt,
    effects: Vec<BranchEffect>,
    before: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    receipt_namespace: Option<[u8; 32]>,
    #[serde(alias = "submitted_at")]
    admitted_at: Timestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    request: Option<Box<RawValue>>,
}

impl ResolvedTransact {
    /// The ordered admission uses the request key, writer clock and canonical fingerprint.
    pub fn admission(&self) -> (&[u8], Timestamp, [u8; 32]) {
        (&self.receipt.key, self.admitted_at, self.fingerprint)
    }

    pub fn canonical_request(&self) -> Option<&RawValue> { self.request.as_deref() }

    pub fn effects(&self) -> &[BranchEffect] { &self.effects }

    pub fn receipt_namespace(&self) -> Option<[u8; 32]> { self.receipt_namespace }

    fn validate_admission(&self) -> Result<()> {
        #[derive(Deserialize)]
        struct AdmissionFields {
            key: Vec<u8>,
            window: AdmissionWindow,
        }
        #[derive(Deserialize)]
        struct AdmissionWindow {
            before: Option<Timestamp>,
        }

        // Older v2 records are replayable using their stored effects and digest.
        if let Some(request) = &self.request {
            let fields: AdmissionFields = serde_json::from_str(request.get()).map_err(|_| invalid_record())?;
            if fields.key != self.receipt.key || fields.window.before != self.before
                || self.admitted_at > self.receipt.system_time
            {
                return Err(invalid_record());
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
struct Pending {
    position: u64,
    tx: ResolvedTransact,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct StoredReceipt {
    receipt: Receipt,
    fingerprint: [u8; 32],
    #[serde(default, skip_serializing_if = "Option::is_none")]
    receipt_namespace: Option<[u8; 32]>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    installed: Option<InstalledReceipt>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstalledReceipt {
    pub source: SourcePosition,
    pub replica: ReplicaPosition,
    pub term: Term,
}

/// Durable receipts by receipt namespace, then idempotency key.
pub(crate) type Receipts = BTreeMap<Option<[u8; 32]>, BTreeMap<Vec<u8>, StoredReceipt>>;

/// What a decision reads: an index, the durable receipts, and whether a timed write is pending.
pub(crate) struct View<'a> {
    pub(crate) index: &'a BranchIndex,
    pub(crate) durable_index: &'a BranchIndex,
    pub(crate) receipts: &'a Receipts,
    pub(crate) pending: bool,
    pub(crate) time_floor: Option<Timestamp>,
}

/// State folded from a single stream. A caller resolves at its fresh committed prefix.
#[derive(Clone)]
pub struct TransactState {
    index: BranchIndex,
    receipts: Receipts,
    pending: Option<Pending>,
    pending_erasure: Option<TxKey>,
}

impl TransactState {
    pub fn new(incarnation: [u8; 16]) -> Self {
        Self { index: BranchIndex::new(incarnation), receipts: BTreeMap::new(), pending: None, pending_erasure: None }
    }

    pub fn index(&self) -> &BranchIndex { &self.index }

    pub fn replace_applied_index(&mut self, index: BranchIndex) -> Result<()> {
        if self.pending.is_some() || index.head().basis.incarnation() != self.index.head().basis.incarnation()
            || index.head().basis < self.index.head().basis
        {
            return Err(invalid_record());
        }
        self.index = index;
        Ok(())
    }

    pub fn pending_effects(&self) -> Option<&[BranchEffect]> {
        self.pending.as_ref().map(|pending| pending.tx.effects())
    }

    pub fn pending_receipt_namespace(&self) -> Option<[u8; 32]> {
        self.pending.as_ref().and_then(|pending| pending.tx.receipt_namespace())
    }

    pub fn has_receipts(&self) -> bool { !self.receipts.is_empty() }

    pub fn install_receipt_position(&mut self, namespace: Option<[u8; 32]>, receipt: &Receipt, position: InstalledReceipt) -> Result<()> {
        let stored = self.receipts.get_mut(&namespace).and_then(|receipts| receipts.get_mut(&receipt.key)).ok_or_else(invalid_record)?;
        if stored.receipt != *receipt || stored.installed.is_some() { return Err(invalid_record()); }
        stored.installed = Some(position);
        Ok(())
    }

    pub fn installed_receipt(&self, namespace: Option<[u8; 32]>, key: &[u8], basis: Basis) -> Option<InstalledReceipt> {
        let receipt = self.receipts.get(&namespace)?.get(key)?;
        (receipt.receipt.basis == basis).then_some(receipt.installed).flatten()
    }

    pub fn validate_installed_receipts(&self, source: Option<SourcePosition>, replica: Option<ReplicaPosition>, term: Term) -> Result<()> {
        for position in self.receipts.values().flat_map(|receipts| receipts.values()).filter_map(|receipt| receipt.installed) {
            if source.is_none_or(|source| position.source > source) || replica.is_none_or(|replica| position.replica > replica)
                || position.term > term || position.source.0 < 0 || position.replica.0 < 0
            {
                return Err(invalid_record());
            }
        }
        Ok(())
    }

    /// A checkpoint can replace the settled prefix once the mode installs its manifest.
    pub fn encode_checkpoint(&self) -> Result<Bytes> {
        if self.pending.is_some() { return Err(pending_error()); }
        self.encode_checkpoint_index(&self.index)
    }

    /// Export the selected cut while retaining its branch ancestry and prior receipts.
    pub fn encode_checkpoint_at(&self, basis: Basis) -> Result<Bytes> {
        if self.pending.is_some() { return Err(pending_error()); }
        let index = self.index.checkpoint_at(basis)?;
        if self.pending_erasure.is_some() { return Err(erasure_pending()); }
        self.encode_checkpoint_index(&index)
    }

    /// Re-namespace the retained receipt coordinates along with their fold.
    pub fn restore_in(&mut self, incarnation: [u8; 16]) -> Result<()> {
        if self.pending.is_some() { return Err(pending_error()); }
        if self.pending_erasure.is_some() { return Err(erasure_pending()); }
        self.index.restore_in(incarnation)?;
        for stored in self.receipts.values_mut().flat_map(|namespace| namespace.values_mut()) {
            stored.receipt.basis = Basis::new(incarnation, stored.receipt.basis.position());
            stored.installed = None;
        }
        Ok(())
    }

    fn encode_checkpoint_index(&self, index: &BranchIndex) -> Result<Bytes> {
        let receipts = self.receipts.values().flat_map(|namespace| namespace.values())
            .filter(|stored| stored.receipt.basis <= index.head().basis).collect::<Vec<_>>();
        serde_json::to_vec(&(RECORD_VERSION, index, receipts)).map(Bytes::from)
            .map_err(|error| Anomaly::fault("rtdb/transaction-checkpoint", error.to_string()))
    }

    pub fn decode_checkpoint(bytes: &[u8], expected: BranchCut) -> Result<Self> {
        let invalid = || Anomaly::fault("rtdb/transaction-checkpoint", "Checkpoint does not match the expected cut");
        let (version, index, receipts): (u8, BranchIndex, Vec<StoredReceipt>) = serde_json::from_slice(bytes)
            .map_err(|error| Anomaly::fault("rtdb/transaction-checkpoint", error.to_string()))?;
        if version != RECORD_VERSION || index.head() != expected || index.exact(expected.basis)? != expected {
            return Err(invalid());
        }
        let mut state = Self { index, receipts: BTreeMap::new(), pending: None, pending_erasure: None };
        for stored in receipts {
            let receipt = &stored.receipt;
            validate_key(&receipt.key).map_err(|_| invalid())?;
            if receipt.replayed || state.index.exact(receipt.basis)?.system_time != receipt.system_time {
                return Err(invalid());
            }
            if receipt.erasure == Some(Erasure::Pending) && state.pending_erasure.is_some() {
                return Err(invalid());
            }
            if state.store_receipt(stored).is_some() { return Err(invalid()); }
        }
        Ok(state)
    }

    fn store_receipt(&mut self, stored: StoredReceipt) -> Option<StoredReceipt> {
        let key = stored.receipt.key.clone();
        if stored.receipt.erasure == Some(Erasure::Pending) {
            self.pending_erasure = Some(TxKey { namespace: stored.receipt_namespace, key: key.clone() });
        }
        self.receipts.entry(stored.receipt_namespace).or_default().insert(key, stored)
    }

    pub fn pending_erasure(&self) -> Option<Basis> {
        let key = self.pending_erasure.as_ref()?;
        self.receipts.get(&key.namespace)?.get(&key.key).map(|stored| stored.receipt.basis)
    }

    pub fn arm_erasure(&mut self, basis: Basis, unreadable_by: Timestamp) -> Result<()> {
        let key = self.pending_erasure.as_ref().ok_or_else(invalid_record)?;
        let receipt = &mut self.receipts.get_mut(&key.namespace)
            .and_then(|receipts| receipts.get_mut(&key.key)).ok_or_else(invalid_record)?.receipt;
        if receipt.basis != basis || receipt.erasure != Some(Erasure::Pending) {
            return Err(invalid_record());
        }
        receipt.erasure = Some(Erasure::Armed { unreadable_by });
        self.pending_erasure = None;
        Ok(())
    }

    pub fn tx_status(&self, key: &[u8]) -> Result<Receipt> {
        self.tx_status_with_namespace(key, None)
    }

    pub fn tx_status_scoped(&self, key: &[u8], authority: ScopedTransact<'_>) -> Result<Receipt> {
        self.tx_status_with_namespace(key, Some(authority.receipt_namespace))
    }

    /// Compare a retry to an already applied admission without resolving it or recording a new write.
    pub fn recover_exact(&self, key: &[u8], namespace: Option<[u8; 32]>, fingerprint: [u8; 32]) -> Result<Receipt> {
        let receipt = self.tx_status_with_namespace(key, namespace)?;
        let stored = self.receipts.get(&namespace).and_then(|namespace| namespace.get(key)).ok_or_else(invalid_record)?;
        if stored.fingerprint != fingerprint {
            return Err(conflicting_key());
        }
        Ok(receipt)
    }

    fn tx_status_with_namespace(&self, key: &[u8], namespace: Option<[u8; 32]>) -> Result<Receipt> {
        validate_key(key)?;
        if self.pending.as_ref().is_some_and(|pending| {
            pending.tx.receipt_namespace == namespace && pending.tx.receipt.key == key
        }) {
            return Err(pending_error());
        }
        self.receipts.get(&namespace).and_then(|receipts| receipts.get(key))
            .map(|stored| replay(&stored.receipt)).ok_or_else(|| {
            Anomaly::new(AnomalyCategory::NotFound, "rtdb/unknown-transaction", "No transaction has committed with this key")
        })
    }

    pub fn resolve(
        &self, request: &Transact, position: u64, now: Timestamp, features: &[&str], claims: &dyn ClaimVerifier,
    ) -> Result<Resolution> {
        self.resolve_with_scope(request, position, now, features, claims, None)
    }

    /// The driver binds claims, written-version selectors, and receipt coordinates to the admitted scope.
    pub fn resolve_scoped(
        &self, request: &Transact, position: u64, now: Timestamp, features: &[&str], claims: &dyn ClaimVerifier,
        authority: ScopedTransact<'_>,
    ) -> Result<Resolution> {
        self.resolve_with_scope(request, position, now, features, claims, Some(authority))
    }

    pub fn project_receipt(&self, mut receipt: Receipt, scope: &LabelScope) -> Result<Receipt> {
        let cut = self.index.project_main_visibility(scope)?.projected_cut(receipt.basis)?;
        receipt.basis = cut.basis;
        receipt.system_time = cut.system_time;
        Ok(receipt)
    }

    fn resolve_with_scope(
        &self, request: &Transact, position: u64, now: Timestamp, features: &[&str], claims: &dyn ClaimVerifier,
        authority: Option<ScopedTransact<'_>>,
    ) -> Result<Resolution> {
        let view = View {
            index: &self.index,
            durable_index: &self.index,
            receipts: &self.receipts,
            pending: self.pending.is_some(),
            time_floor: None,
        };
        Self::resolve_in(view, request, position, now, features, claims, authority).map(|(resolution, _)| resolution)
    }

    /// Resolves `request` at `position` against `view`. For an append it also returns the
    /// index after the transaction publishes: exactly what [`TransactState::apply`] computes
    /// for the record, or for a timed write for its `Settle` (advance to the receipt cut,
    /// then the effects in order).
    pub(crate) fn resolve_in(
        view: View<'_>, request: &Transact, position: u64, now: Timestamp, features: &[&str], claims: &dyn ClaimVerifier,
        authority: Option<ScopedTransact<'_>>,
    ) -> Result<(Resolution, Option<BranchIndex>)> {
        validate_key(&request.key)?;
        let receipt_namespace = request.receipt_namespace(authority.map(|authority| authority.receipt_namespace));
        let (fingerprint, canonical_request) = match request.custody_request_digest {
            Some(digest) => (digest, None),
            None => {
                let request = request.canonical_request()?;
                (Sha256::digest(request.get().as_bytes()).into(), Some(request))
            }
        };
        if let Some(stored) = view.receipts.get(&receipt_namespace).and_then(|namespace| namespace.get(&request.key)) {
            if stored.fingerprint != fingerprint {
                return Err(conflicting_key());
            }
            return Ok((Resolution::Answer(replay(&stored.receipt)), None));
        }
        if request.has_unsealed_custody() {
            return Err(if !features.contains(&ROW_CUSTODY) {
                feature_required(ROW_CUSTODY)
            } else if request.ops.iter().chain(&request.otherwise).flat_map(Op::put_rows)
                .any(|row| row.custody_group.is_some_and(|group| view.index.custody_revocation(group).is_some()))
            {
                operations::erased_custody_group()
            } else {
                Anomaly::incorrect("rtdb/custody-payload", "Protected columns must be sealed before admission")
            });
        }

        if view.pending { return Err(pending_error()); }
        let head = view.index.head();
        let validation = request.ops.is_empty() && request.otherwise.is_empty();
        let before = if validation {
            if !features.contains(&LINEARIZABLE) { return Err(feature_required(LINEARIZABLE)); }
            None
        } else {
            if request.window.before.is_some() && !features.contains(&TIMED_TRANSACT) {
                return Err(feature_required(TIMED_TRANSACT));
            }
            request.window.before
        };
        let cut = if validation {
            head
        } else {
            if position <= head.basis.position() {
                return Err(Anomaly::fault("rtdb/transaction-order", "The append must follow the applied prefix"));
            }
            let receipt_position = if before.is_some() {
                position.checked_add(1).ok_or_else(|| {
                    Anomaly::fault("rtdb/transaction-order", "The proposed transaction has no terminal position")
                })?
            } else {
                position
            };
            BranchCut {
                basis: Basis::new(head.basis.incarnation(), receipt_position),
                system_time: now.max(head.system_time.next()?).max(view.time_floor.unwrap_or(Timestamp::MIN)),
            }
        };
        let mut base = view.index.clone();
        if !validation { base.advance(cut)?; }
        let decision_time = now.max(cut.system_time);
        let advanced = base.clone();
        let scope = authority.map(|authority| authority.labels);
        let result = Self::decide(request, base, now, decision_time, features, claims, scope, view.durable_index);
        let (outcome, effects, after) = match result {
            Ok(decision) => decision,
            Err(error) if error.is_caller_error() => (Outcome::Refused { error }, Vec::new(), advanced),
            Err(error) => return Err(error),
        };
        let receipt = Receipt {
            key: request.key.clone(), basis: cut.basis, system_time: cut.system_time, outcome, replayed: false,
            erasure: effects.iter().any(|effect| matches!(effect, BranchEffect::Erase { .. })).then_some(Erasure::Pending),
        };
        if validation {
            Ok((Resolution::Answer(receipt), None))
        } else {
            // For a timed write, `after` is the index once it settles at `position + 1`.
            let canonical_request = canonical_request.map_or_else(|| request.canonical_request(), Ok)?;
            let record = TransactionRecord::Tx(ResolvedTransact {
                fingerprint, receipt, effects, before, receipt_namespace, admitted_at: now,
                request: Some(canonical_request),
            });
            Ok((Resolution::Append(PreparedAppend { position, record }), Some(after)))
        }
    }

    fn decide(
        request: &Transact, base: BranchIndex, sample: Timestamp, time: Timestamp,
        features: &[&str], claims: &dyn ClaimVerifier, scope: Option<&LabelScope>, durable: &BranchIndex,
    ) -> Result<(Outcome, Vec<BranchEffect>, BranchIndex)> {
        if let Some(feature) = request.require.iter().filter(|feature| !features.contains(&feature.as_str())).min() {
            return Err(feature_required(feature));
        }
        if !request.requires.is_empty() && !features.contains(&TRANSACT_REQUIRES) {
            return Err(feature_required(TRANSACT_REQUIRES));
        }
        let response_release = request.is_response_release();
        if request.uses_custody() {
            if request.contains_response_release() && (!response_release || scope.is_some()) {
                return Err(Anomaly::incorrect("rtdb/custody-release", "Response release requires its original writer and private receipt namespace"));
            }
            if !response_release && !features.contains(&ROW_CUSTODY) {
                return Err(feature_required(ROW_CUSTODY));
            }
        }
        let Window { not_before, before } = request.window;
        if not_before.zip(before).is_some_and(|(start, end)| start >= end) {
            return Err(Anomaly::incorrect("rtdb/invalid-window", "The transaction window is empty"));
        }
        if before.is_some_and(|before| time >= before) {
            return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb/window-expired", "The transaction window has expired"));
        }
        if let Some(start) = not_before.filter(|start| sample < *start) {
            return Err(Anomaly::new(AnomalyCategory::Busy, "rtdb/window-pending", "The transaction window has not opened")
                .with_context("retry_after_micros", start.0.saturating_sub(sample.0).into()));
        }

        let visibility = std::cell::OnceCell::new();
        let evaluate = |guard: &Guard, index: &BranchIndex| {
            let visible = match (scope, guard) {
                (Some(scope), Guard::Version { expect: Version::Written(_), .. }) => Some(
                    visibility.get_or_init(|| durable.project_main_visibility(scope))
                        .as_ref().map_err(Clone::clone)?,
                ),
                _ => None,
            };
            guard.evaluate(index, time, claims, visible)
        };
        for (i, requirement) in request.requires.iter().enumerate() {
            if requirement.after() {
                return Err(Anomaly::incorrect(
                    "rtdb/invalid-requirement", "A count requirement must use the Before phase",
                ));
            }
            let valid = if response_release {
                CustodyWriterFence::check_requirement(requirement, &base, time)?
            } else {
                evaluate(requirement, &base)?
            };
            if !valid {
                return Err(Anomaly::new(
                    AnomalyCategory::Conflict, REQUIREMENT_FAILED, format!("Requirement {i} failed"),
                ));
            }
        }

        let mut failed = Vec::new();
        for (i, guard) in request.guards.iter().enumerate().filter(|(_, guard)| !guard.after()) {
            if !evaluate(guard, &base)? { failed.push(i); }
        }
        if failed.is_empty() {
            let mut then = Candidate::new(base.clone(), scope);
            then.execute(&request.ops)?;
            for (i, guard) in request.guards.iter().enumerate().filter(|(_, guard)| guard.after()) {
                if !evaluate(guard, &then.index)? { failed.push(i); }
            }
            if failed.is_empty() {
                if !request.ops.is_empty() { then.check_constraints()?; }
                return Ok((Outcome::Then, then.effects, then.index));
            }
        }
        let mut otherwise = Candidate::new(base, scope);
        otherwise.execute(&request.otherwise)?;
        if !request.otherwise.is_empty() { otherwise.check_constraints()?; }
        Ok((Outcome::Otherwise { failed }, otherwise.effects, otherwise.index))
    }

    /// A writer recovering the prefix orders refusal before admitting another write.
    pub fn recover_pending(&self) -> Option<TransactionRecord> {
        self.pending.as_ref().map(|pending| TransactionRecord::Refuse { tx_lsn: pending.position })
    }

    pub fn apply(&mut self, position: u64, record: TransactionRecord) -> Result<Option<Receipt>> {
        match record {
            TransactionRecord::Tx(tx) => {
                let receipt = &tx.receipt;
                let expected = if tx.before.is_some() { position.checked_add(1) } else { Some(position) };
                let head = self.index.head();
                if self.pending.is_some() || position <= head.basis.position()
                    || Some(receipt.basis.position()) != expected
                    || receipt.basis.incarnation() != head.basis.incarnation() || receipt.system_time <= head.system_time
                    || receipt.replayed || validate_key(&receipt.key).is_err()
                    || self.receipts.get(&tx.receipt_namespace).is_some_and(|namespace| namespace.contains_key(&receipt.key))
                    || (matches!(&receipt.outcome, Outcome::Refused { .. }) && !tx.effects.is_empty())
                    || matches!(receipt.erasure, Some(Erasure::Armed { .. }))
                    || (receipt.erasure == Some(Erasure::Pending)
                        && (self.pending_erasure.is_some() || !tx.effects.iter().any(|effect| matches!(effect, BranchEffect::Erase { .. }))))
                {
                    return Err(invalid_record());
                }
                if tx.before.is_some() {
                    self.pending = Some(Pending { position, tx });
                    Ok(None)
                } else {
                    self.publish(tx.receipt, tx.fingerprint, tx.receipt_namespace, tx.effects).map(Some)
                }
            }
            TransactionRecord::Settle { tx_lsn } => {
                let pending = self.pending.as_ref().filter(|pending| pending.position == tx_lsn)
                    .ok_or_else(invalid_record)?;
                if position != pending.tx.receipt.basis.position()
                    || pending.tx.before.is_none_or(|cutoff| pending.tx.receipt.system_time >= cutoff)
                {
                    return Err(invalid_record());
                }
                let receipt = self.publish(
                    pending.tx.receipt.clone(), pending.tx.fingerprint, pending.tx.receipt_namespace, pending.tx.effects.clone(),
                )?;
                self.pending = None;
                Ok(Some(receipt))
            }
            TransactionRecord::Refuse { tx_lsn } => {
                let pending = self.pending.as_ref().filter(|pending| pending.position == tx_lsn && position > tx_lsn)
                    .ok_or_else(invalid_record)?;
                let mut receipt = pending.tx.receipt.clone();
                receipt.basis = Basis::new(receipt.basis.incarnation(), position);
                receipt.erasure = None;
                if !matches!(receipt.outcome, Outcome::Refused { .. }) {
                    receipt.outcome = Outcome::Refused {
                        error: Anomaly::new(AnomalyCategory::Conflict, "rtdb/timed-transaction-refused",
                            "The original transaction lacks timely durable acknowledgement"),
                    };
                }
                let receipt = self.publish(receipt, pending.tx.fingerprint, pending.tx.receipt_namespace, Vec::new())?;
                self.pending = None;
                Ok(Some(receipt))
            }
        }
    }

    fn publish(&mut self, receipt: Receipt, fingerprint: [u8; 32], receipt_namespace: Option<[u8; 32]>,
               effects: Vec<BranchEffect>) -> Result<Receipt> {
        let mut candidate = self.index.clone();
        candidate.advance(BranchCut { basis: receipt.basis, system_time: receipt.system_time })?;
        for effect in effects { candidate.apply(effect)?; }
        self.index = candidate;
        let previous = self.store_receipt(StoredReceipt { fingerprint, receipt: receipt.clone(), receipt_namespace, installed: None });
        debug_assert!(previous.is_none());
        Ok(receipt)
    }
}

fn erasure_pending() -> Anomaly {
    Anomaly::new(AnomalyCategory::Busy, "rtdb/erasure-pending", "The erase receipt is waiting for durable key retirement")
}

fn conflicting_key() -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb/idempotency-key", "The key belongs to a different transaction")
}

fn invalid_record() -> Anomaly {
    Anomaly::fault("rtdb/invalid-resolved-transaction", "The record cannot be applied at the committed position")
}

fn pending_error() -> Anomaly {
    Anomaly::new(AnomalyCategory::Busy, "rtdb/transaction-pending", "The timed transaction is waiting for a terminal record")
}

fn feature_required(feature: &str) -> Anomaly {
    Anomaly::unsupported("rtdb/feature-required", format!("Required feature {feature} is unavailable"))
}

fn replay(receipt: &Receipt) -> Receipt {
    Receipt { replayed: true, ..receipt.clone() }
}

fn validate_key(key: &[u8]) -> Result<()> {
    if !(16..=64).contains(&key.len()) {
        return Err(Anomaly::incorrect("rtdb/invalid-idempotency-key", "An idempotency key must contain 16 to 64 bytes"));
    }
    Ok(())
}

impl Guard {
    fn after(&self) -> bool {
        matches!(self, Self::CountAtMost { when: When::After, .. })
    }

    fn evaluate(
        &self, index: &BranchIndex, time: Timestamp, claims: &dyn ClaimVerifier, visible: Option<&VisibleHistory>,
    ) -> Result<bool> {
        let view = |branch| index.view(branch, At::Latest, Layer::Merged, time);
        match self {
            Self::Unchanged(claim) => index.claim_unchanged(&claims.verify(claim)?, time),
            Self::Version { branch, table, id, valid_at, expect } => {
                let expect = match (*expect, visible) {
                    (Version::Written(basis), Some(history)) => {
                        if branch != MAIN_BRANCH {
                            return Err(Anomaly::unsupported(
                                "rtdb/scoped-branch", "Scoped written versions require the main branch",
                            ));
                        }
                        Version::Written(history.source_basis(basis)?)
                    }
                    (expect, _) => expect,
                };
                if let Version::Written(basis) = expect { index.validate_written_basis(basis)?; }
                let at = valid_at.unwrap_or(time);
                let row = view(branch)?.row(table, id, at);
                Ok(match expect {
                    Version::Absent => row.is_none(),
                    Version::Present => row.is_some(),
                    Version::Written(basis) => row.is_some_and(|event| {
                        if visible.is_some() { event.written_basis().is_some() && event.cut.basis == basis }
                        else { event.written_basis() == Some(basis) }
                    }),
                })
            }
            Self::CountAtMost { branch, range, .. } => view(branch)?.count_at_most(range, time),
        }
    }
}

struct Candidate<'a> {
    index: BranchIndex,
    effects: Vec<BranchEffect>,
    touched: BTreeSet<Branch>,
    declared: BTreeSet<TableRef>,
    scope: Option<&'a LabelScope>,
    affected_labels: BTreeSet<Vec<u8>>,
    single_label: bool,
}

impl<'a> Candidate<'a> {
    fn new(index: BranchIndex, scope: Option<&'a LabelScope>) -> Self {
        Self {
            index, effects: Vec::new(), touched: BTreeSet::new(), declared: BTreeSet::new(),
            scope, affected_labels: BTreeSet::new(), single_label: false,
        }
    }

    fn push(&mut self, effect: BranchEffect) -> Result<()> {
        self.index.apply(effect.clone())?;
        self.effects.push(effect);
        Ok(())
    }
}

#[cfg(test)]
mod tests;
#[cfg(test)]
mod perf;
