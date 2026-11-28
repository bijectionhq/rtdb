use std::{collections::{BTreeMap, BTreeSet}, sync::Arc};

use bytes::Bytes;
use rtdb_index::branches::{
    BranchCut, BranchEffect, BranchIndex, BranchTable, BranchView, RowImage, VisibleHistory,
};
pub use rtdb_index::branches::{
    CountRange, CustodyGroupId, CustodyOccurrence, CustodyPayload, CustodyRelease, CustodyReservation, CustodyResponseAdmission,
    CustodyPlacementId, CustodyReview, CustodyTarget, DerivedCustodyReservation, GroupedCount, IndexDefinition,
    ReviewedCustodyReservation, Visibility,
    RankedIndexDeclaration, RankedIndexDefinition, RankedIndexKind, ScopedHoldCommand,
    store::{KeyRange, StoreBound, StoreWrite},
};
use rtdb_index::branches::store::{
    StoreStamp, VALUE_COLUMN, check_bounds, check_derives, check_placeholders, claim_refused, decode_bound_value, resolve_derives, stamp_writes,
    store_table,
};
use rtdb_types::{
    Anomaly, AnomalyCategory, EntityId, Interval, LogicalType, ReplicaPosition, Result, SourcePosition, StructFields, TableRef,
    Term, TimeUnit, Timestamp, Value,
    rtdb_v1::{At, Basis, Branch, Claim, ClaimVerifier, LabelScope, Layer, MAIN_BRANCH, NativeCheckpointCustody, NativeCustodyFence, NativeDependentCustodyOrder, NativeDependentCustodyScope, valid_branch_name},
};
pub use rtdb_types::rtdb_v1::{ValidTime, ValidTimeBound};
use serde::{Deserialize, Serialize, Serializer};
use sha2::{Digest, Sha256};

mod custody_prepare;
mod custody_release;
pub mod encoding;
mod funding;
mod operations;
mod pipeline;
mod store_clock;
pub mod workload;
use workload::Family as WorkloadFamily;

pub use custody_prepare::prepare_custody;
pub use custody_release::{CUSTODY_RELEASE_NAMESPACE, CustodyWriterFence, TrustedCustodyAdmission, TrustedCustodyCommand, TrustedCustodyRelease};
pub use pipeline::{Decision, Terminal, TransactPipeline, TxKey};
pub use funding::{DECISION_CLOCK_OFFER, FundedReceipt, FundingCommand, FundingInput, funding_invalid, funding_kernel_unavailable, funding_unsupported};
pub use funding::appointment::{AppointmentCommand, FUNDED_APPOINTMENT, appointment_invalid, appointment_unavailable};
use funding::{FundingDecision, resolve_funding};
use funding::appointment::{AppointmentDecision, check_appointment_effects, resolve_appointment};

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
    #[serde(default)]
    pub requires: Vec<Guard>,
    #[serde(skip)]
    pub custody_request_digest: Option<[u8; 32]>,
    #[serde(skip)]
    pub request_memory: Option<Arc<rtdb_columnar::MemoryReservation>>,
}

impl Transact {
    /// The store append this request is, when its one operation is [`Op::Store`].
    pub fn store_append(&self) -> Option<&StoreAppend> {
        match self.ops.as_slice() {
            [Op::Store(append)] => Some(append),
            _ => None,
        }
    }

    /// Whether the request only validates: no operations in either arm. It answers at
    /// the acquired basis and publishes no record. A store append is never one, even
    /// without writes: every outcome but `outside` is a receipt under its key
    /// ([`TransactState::resolve_store`]).
    pub fn validates_only(&self) -> bool {
        self.otherwise.is_empty() && self.ops.is_empty()
    }

    pub fn claims(&self) -> impl Iterator<Item = &Claim> {
        self.requires.iter().chain(&self.guards).filter_map(|guard| match guard {
            Guard::Unchanged(claim) => Some(claim),
            _ => None,
        })
    }

    pub fn logical_sha256(&self) -> Result<[u8; 32]> {
        Ok(Sha256::digest(self.canonical_request()?.as_bytes()).into())
    }

    pub fn canonical_request(&self) -> Result<CanonicalRequest> {
        encoding::encode(self).map(CanonicalRequest)
            .map_err(|error| Anomaly::incorrect("rtdb/transaction-encoding", error.to_string()))
    }

    /// Decode a request captured by an input log. The caller still resolves it
    /// at the writer's ordered prefix with the route's verified authority.
    pub fn decode_canonical_request(bytes: &[u8]) -> Result<Self> {
        let request: Self = encoding::decode(bytes).map_err(|error| {
            Anomaly::incorrect("rtdb/transaction-encoding", error.to_string())
        })?;
        validate_key(&request.key)?;
        if request.canonical_request()?.as_bytes() != bytes {
            return Err(Anomaly::incorrect("rtdb/transaction-encoding", "The logged transaction request is not canonical"));
        }
        Ok(request)
    }

    pub fn uses_custody(&self) -> bool { self.ops.iter().chain(&self.otherwise).any(Op::uses_custody) }

    pub fn uses_reviewed_custody(&self) -> bool {
        self.ops.iter().chain(&self.otherwise).any(Op::is_reviewed_custody)
    }

    pub fn uses_erasure(&self) -> bool {
        self.ops.iter().chain(&self.otherwise).any(|op| matches!(op, Op::Erase { .. }))
    }

    pub fn is_response_release(&self) -> bool {
        matches!(self.ops.as_slice(), [Op::CustodyRelease(_) | Op::AdmitCustodyResponse(_)])
            && self.otherwise.is_empty() && self.guards.is_empty()
            && matches!(self.requires.as_slice(),
                [Guard::Version { branch, valid_at: Some(_), expect: Version::Written(_), .. }] if branch == MAIN_BRANCH)
            && self.window == Window::default()
    }

    pub fn contains_response_release(&self) -> bool {
        self.ops.iter().chain(&self.otherwise).any(|op| matches!(op, Op::CustodyRelease(_) | Op::AdmitCustodyResponse(_)))
    }

    pub fn receipt_namespace(&self, scope: Option<[u8; 32]>) -> Option<[u8; 32]> {
        scope.or_else(|| self.is_response_release().then_some(CUSTODY_RELEASE_NAMESPACE))
            .or_else(|| self.contains_native_custody_transfer().then(native_checkpoint_namespace))
    }

    pub fn contains_native_custody_transfer(&self) -> bool {
        self.ops.iter().chain(&self.otherwise)
            .any(|op| matches!(op, Op::NativeCustodyTransfer { .. } | Op::NativeDependentCustody(_)))
    }

    pub fn native_custody_transfer(custody: NativeCheckpointCustody, destination: Option<Basis>) -> Self {
        let key = native_custody_key(&custody, destination.is_some());
        Self {
            key, require: Vec::new(), window: Window::default(), guards: Vec::new(),
            ops: vec![Op::NativeCustodyTransfer { custody, destination }], otherwise: Vec::new(),
            requires: Vec::new(), custody_request_digest: None, request_memory: None,
        }
    }

    pub fn native_dependent_custody(order: NativeDependentCustodyOrder) -> Self {
        Self {
            key: dependent_custody_key(&order), require: Vec::new(), window: Window::default(),
            guards: Vec::new(), ops: vec![Op::NativeDependentCustody(order)], otherwise: Vec::new(),
            requires: Vec::new(), custody_request_digest: None, request_memory: None,
        }
    }

    pub fn scoped_hold(command: ScopedHoldCommand) -> Self {
        let mut hash = Sha256::new();
        hash.update(b"rtdb/v1/projected-hold\0");
        hash.update(command.id());
        hash.update([u8::from(matches!(&command, ScopedHoldCommand::Release { .. }))]);
        Self {
            key: hash.finalize().to_vec(), require: Vec::new(), window: Window::default(), guards: Vec::new(),
            ops: vec![Op::ScopedHold(command)], otherwise: Vec::new(), requires: Vec::new(),
            custody_request_digest: None, request_memory: None,
        }
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
            op.put_rows().any(|row| {
                !row.protected_columns.is_empty() && (row.custody_payload.is_none()
                    || row.protected_columns.iter().any(|column| row.cells.get(column).is_some()))
            })
        })
    }
}

/// A [`Transact`]'s canonical encoding, as its logs record it, digests cover it
/// and peers forward it. [`Transact::decode_canonical_request`] decodes it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CanonicalRequest(Vec<u8>);

impl CanonicalRequest {
    pub fn as_bytes(&self) -> &[u8] { &self.0 }

    pub fn decode(&self) -> Result<Transact> { Transact::decode_canonical_request(&self.0) }
}

impl Serialize for CanonicalRequest {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for CanonicalRequest {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct Bytes;
        impl<'de> serde::de::Visitor<'de> for Bytes {
            type Value = CanonicalRequest;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("canonical request bytes")
            }
            fn visit_bytes<E: serde::de::Error>(self, bytes: &[u8]) -> std::result::Result<Self::Value, E> {
                Ok(CanonicalRequest(bytes.to_vec()))
            }
            fn visit_byte_buf<E: serde::de::Error>(self, bytes: Vec<u8>) -> std::result::Result<Self::Value, E> {
                Ok(CanonicalRequest(bytes))
            }
            // A JSON carrier writes the bytes as an array of numbers.
            fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error> {
                let mut bytes = Vec::with_capacity(seq.size_hint().unwrap_or(0).min(1 << 20));
                while let Some(byte) = seq.next_element()? { bytes.push(byte); }
                Ok(CanonicalRequest(bytes))
            }
        }
        deserializer.deserialize_bytes(Bytes)
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
    /// The store contract's read validation: nothing in `range` of the main
    /// branch's store relation was written or erased after `since`.
    Unwritten { range: KeyRange, since: Basis },
    /// The store's final private guard names an exact retained dependency.
    RetainedStoreBasis { basis: Basis },
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
    pub custody_group: Option<CustodyGroupId>,
    #[serde(default, serialize_with = "serialize_features")]
    pub protected_columns: Vec<String>,
    pub custody_payload: Option<CustodyPayload>,
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
    ReserveReviewedCustodyPlacement { group: CustodyGroupId, reservation: CustodyReservation, source: CustodyReview, read_proof: Vec<u8> },
    LandCustodyBranch { branch: Branch, placements: Vec<ReviewedCustodyPlacement> },
    ScopedHold(ScopedHoldCommand),
    /// `rtdb.store.v1.Append`'s writes and window: the request's only operation,
    /// resolved by [`TransactState::resolve_store`], never executed as an operation.
    Store(StoreAppend),
    // The variants that skip deserializing stay last: a derived decoder numbers
    // only the variants it accepts, so one placed earlier would shift the binary
    // index of every accepted variant after it.
    #[serde(skip_deserializing)]
    CustodyRelease(CustodyRelease),
    #[serde(skip_deserializing)]
    AdmitCustodyResponse(CustodyResponseAdmission),
    #[serde(skip_deserializing)]
    NativeCustodyTransfer { custody: NativeCheckpointCustody, destination: Option<Basis> },
    #[serde(skip_deserializing)]
    NativeDependentCustody(NativeDependentCustodyOrder),
}

/// A store append (`dev/doc/ordered-bytes-store.adoc`): its writes, with stamp
/// placeholders the resolver fills at the ordering point, its atomic bounded
/// counter updates (`bounds`), and its clock window, judged against the clock
/// sample the stamp carries. No writes or bounds only validates.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreAppend {
    pub writes: Vec<StoreWrite>,
    #[serde(default)]
    pub bounds: Vec<StoreBound>,
    #[serde(default)]
    pub derives: Vec<Vec<u8>>,
    pub window: Window,
}

/// The clock sample is before the store window's `not_before`.
pub const STORE_OUTSIDE_EARLY: &str = "rtdb.store/outside-early";
/// The clock sample is at or past the store window's `before`.
pub const STORE_OUTSIDE_LATE: &str = "rtdb.store/outside-late";
/// A `Bound` check left `[min, max]` or overflowed `i64`; nothing was written.
pub const STORE_BOUND: &str = "rtdb.store/bound";
/// A derive's unique claim, filled at the stamp, counted a second claimant
/// (Bijection T7-4, `store_derive::claim_exceeded`); nothing was written.
pub const STORE_CLAIM: &str = "rtdb.store/claim";

/// The append's time is smoothed past the head's, so the judge never sees a
/// regressed reading.
fn store_time_order() -> Anomaly {
    Anomaly::fault("rtdb.tx/system-time", "The store append's time does not follow the head's")
}

fn store_append_shape() -> Anomaly {
    Anomaly::incorrect("rtdb.store/append-shape", "A store append carries only its store operation and read guards")
}

fn invalid_scoped_hold() -> Anomaly {
    Anomaly::incorrect("rtdb/scoped-hold", "Projected retention requires the exact verified scope")
}

fn native_custody_key(custody: &NativeCheckpointCustody, installed: bool) -> Vec<u8> {
    let mut hash = Sha256::new();
    hash.update(b"rtdb/v1/native-checkpoint-command\0");
    hash.update(custody.reservation.target.incarnation);
    hash.update([u8::from(installed)]);
    hash.finalize().to_vec()
}

fn native_checkpoint_namespace() -> [u8; 32] {
    Sha256::digest(b"rtdb/v1/native-checkpoint-receipt\0").into()
}

fn dependent_custody_key(order: &NativeDependentCustodyOrder) -> Vec<u8> {
    native_dependent_key(order.scope(), matches!(order, NativeDependentCustodyOrder::Dispose(_))).to_vec()
}

pub fn native_dependent_disposition_key(scope: NativeDependentCustodyScope) -> [u8; 32] {
    native_dependent_key(scope, true)
}

fn native_dependent_key(scope: NativeDependentCustodyScope, disposition: bool) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"rtdb/v1/native-dependent-custody-command\0");
    hash.update(scope.reservation.source.basis.to_bytes());
    hash.update(scope.reservation.source.pin);
    hash.update(scope.reservation.source.manifest_sha256);
    hash.update(scope.reservation.target.request);
    hash.update(scope.reservation.reserved_at.to_bytes());
    hash.update(scope.destination.to_bytes());
    hash.update(scope.source_enrolled_at.to_bytes());
    hash.update(scope.group);
    hash.update(scope.checkpoint_group_sha256);
    hash.update([u8::from(disposition)]);
    hash.finalize().into()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CustodyPlacement {
    pub placement_id: CustodyPlacementId,
    pub reservation: Option<[u8; 32]>,
    #[serde(default)]
    pub derived: Option<DerivedCustodyReservation>,
    pub carrier_column: String,
    pub link_column: Option<String>,
    pub commitments: Option<CustodyPlacementCommitments>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct CustodyPlacementCommitments {
    pub carrier_sha256: [u8; 32],
    pub link_sha256: Option<[u8; 32]>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReviewedCustodyPlacement {
    pub table: TableRef,
    pub row: Row,
    pub placement: CustodyPlacement,
    pub reserved_at: Basis,
    pub reserve_key: Vec<u8>,
}

impl Op {
    fn put_rows(&self) -> impl Iterator<Item = &Row> {
        let ordinary = match self {
            Self::Put { rows, .. } => rows.as_slice(),
            Self::PlaceCustodyRow { row, .. } => std::slice::from_ref(row),
            _ => &[],
        };
        let landing = match self {
            Self::LandCustodyBranch { placements, .. } => placements.as_slice(),
            _ => &[],
        };
        ordinary.iter().chain(landing.iter().map(|placement| &placement.row))
    }

    fn is_reviewed_custody(&self) -> bool {
        matches!(self, Self::ReserveReviewedCustodyPlacement { .. } | Self::LandCustodyBranch { .. })
    }

    fn uses_custody(&self) -> bool {
        match self {
            Self::CreateCustodyGroup { .. } | Self::RevokeCustodyGroup { .. }
            | Self::ReserveCustodyPlacement { .. } | Self::PlaceCustodyRow { .. }
            | Self::ReserveReviewedCustodyPlacement { .. } | Self::LandCustodyBranch { .. }
            | Self::CustodyRelease(_) | Self::AdmitCustodyResponse(_) => true,
            Self::Put { rows, .. } => rows.iter().any(|row| row.custody_group.is_some() || !row.protected_columns.is_empty()),
            _ => false,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Declaration {
    Schema(StructFields<LogicalType>),
    Index(IndexDefinition),
    RankedIndex { name: String, definition: RankedIndexDefinition },
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
    #[serde(default)]
    pub erasure: Option<Erasure>,
    #[serde(default)]
    pub decision_clock: Option<FundedReceipt>,
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
pub const REVIEWED_CUSTODY_PLACEMENT: &str = "transact.custody-reviewed-placement";
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

const RECORD_VERSION: u8 = 6;
const FUNDED_RECORD_VERSION: u8 = 7;
const APPOINTMENT_RECORD_VERSION: u8 = 8;
const MANAGED_APPOINTMENT_RECORD_VERSION: u8 = 9;
const PRUNED_STORE_CHECKPOINT_VERSION: u8 = 10;

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
        encoding::encode_versioned(self.version(), self).map(Bytes::from)
            .map_err(|error| Anomaly::fault("rtdb/transaction-encoding", error.to_string()))
    }

    /// Decodes a record after its version byte, refusing a version this encoder does not write.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let unknown = || Anomaly::unsupported("rtdb/transaction-version", "Unknown resolved transaction version");
        let (&version, record) = bytes.split_first().ok_or_else(unknown)?;
        if !(RECORD_VERSION..=MANAGED_APPOINTMENT_RECORD_VERSION).contains(&version) { return Err(unknown()); }
        let record: Self = encoding::decode(record)
            .map_err(|error| Anomaly::fault("rtdb/transaction-decoding", error.to_string()))?;
        if version != record.version() { return Err(unknown()); }
        if let Self::Tx(tx) = &record {
            tx.validate_admission()?;
        }
        Ok(record)
    }

    fn version(&self) -> u8 {
        match self {
            Self::Tx(tx) => tx.receipt.record_version(tx.receipt_namespace),
            _ => RECORD_VERSION,
        }
    }

    pub fn funded_journal_cutoff(bytes: &[u8]) -> Result<Option<rtdb_types::TimedJournalCutoff>> {
        if !funded_record(bytes) { return Ok(None); }
        let Self::Tx(tx) = Self::decode(bytes)? else { return Err(invalid_record()); };
        Ok(tx.funded_guard()?.map(|(_, _, cutoff)| cutoff))
    }
}

fn funded_record(bytes: &[u8]) -> bool {
    bytes.first().is_some_and(|version| (FUNDED_RECORD_VERSION..=MANAGED_APPOINTMENT_RECORD_VERSION).contains(version))
}

impl Receipt {
    fn record_version(&self, namespace: Option<[u8; 32]>) -> u8 {
        match self.decision_clock {
            Some(offer) if offer.is_appointment() && namespace.is_some() => MANAGED_APPOINTMENT_RECORD_VERSION,
            Some(offer) if offer.is_appointment() => APPOINTMENT_RECORD_VERSION,
            Some(_) => FUNDED_RECORD_VERSION,
            None => RECORD_VERSION,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResolvedTransact {
    fingerprint: [u8; 32],
    receipt: Receipt,
    effects: Vec<BranchEffect>,
    before: Option<Timestamp>,
    receipt_namespace: Option<[u8; 32]>,
    admitted_at: Timestamp,
    workload: Option<WorkloadFamily>,
    workload_digest: Option<[u8; 32]>,
    request: Option<CanonicalRequest>,
}

impl ResolvedTransact {
    /// The ordered admission uses the request key, writer clock and canonical fingerprint.
    pub fn admission(&self) -> (&[u8], Timestamp, [u8; 32]) {
        (&self.receipt.key, self.admitted_at, self.fingerprint)
    }

    pub fn canonical_request(&self) -> Option<&CanonicalRequest> { self.request.as_ref() }

    pub fn effects(&self) -> &[BranchEffect] { &self.effects }

    pub fn receipt_namespace(&self) -> Option<[u8; 32]> { self.receipt_namespace }

    pub fn funded_guard(&self) -> Result<Option<(Timestamp, Timestamp, rtdb_types::TimedJournalCutoff)>> {
        if let Some(funding) = self.receipt.decision_clock
            && !matches!(self.receipt.outcome, Outcome::Refused { .. })
        {
            if funding.is_appointment() { return funding.appointment_guard(); }
            return funding.journal_guard();
        }
        Ok(None)
    }

    fn validate_admission(&self) -> Result<()> {
        // The leading fields of `Transact`, in its declaration order.
        #[derive(Deserialize)]
        struct AdmissionFields {
            key: Vec<u8>,
            _require: Vec<String>,
            window: Window,
        }

        if self.workload_digest.is_some() && (self.workload.is_none() || self.request.is_none()) {
            return Err(invalid_record());
        }
        if let Some(funding) = self.receipt.decision_clock {
            let request = self.request.as_ref().ok_or_else(invalid_record)?;
            let request = request.decode().map_err(|_| invalid_record())?;
            if funding.is_appointment() { return funding.check_appointment_admission(self, &request); }
            request.check_funded_shape(self.receipt_namespace.is_some()).map_err(|_| invalid_record())?;
            funding.check_admission(&request, &self.receipt, self.receipt.basis.position(), self.receipt_namespace)?;
            if request.key != self.receipt.key || request.logical_sha256()? != self.fingerprint
                || self.before.is_some() || self.workload.is_some() || self.receipt.erasure.is_some()
                || (funding.booked() && (!self.effects.is_empty() || self.receipt.outcome != Outcome::Then))
                || (!funding.booked() && !matches!(self.receipt.outcome, Outcome::Refused { .. }) && !funding.acceptance_is_valid())
            {
                return Err(invalid_record());
            }
            return Ok(());
        }
        // A record without funding replays from its stored effects and digest.
        if let Some(request) = &self.request {
            let fields: AdmissionFields = encoding::decode_prefix(request.as_bytes()).map_err(|_| invalid_record())?;
            let window_matches = if self.workload.is_some() {
                self.receipt_namespace.is_some()
                    && fields.window.before.is_none_or(|before| self.before.is_some_and(|cutoff| cutoff <= before))
            } else {
                fields.window.before == self.before
            };
            if fields.key != self.receipt.key || !window_matches
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
#[serde(deny_unknown_fields)]
pub(crate) struct StoredReceipt {
    receipt: Receipt,
    fingerprint: [u8; 32],
    #[serde(default, skip_serializing_if = "Option::is_none")]
    receipt_namespace: Option<[u8; 32]>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    installed: Option<InstalledReceipt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    workload: Option<WorkloadFamily>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    workload_digest: Option<[u8; 32]>,
}

impl StoredReceipt {
    fn matches_workload_digest(&self, digest: [u8; 32]) -> bool {
        // Older workload receipts used the signed action digest as their fingerprint.
        self.workload_digest.unwrap_or(self.fingerprint) == digest
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstalledReceipt {
    pub source: SourcePosition,
    pub replica: ReplicaPosition,
    pub term: Term,
}

/// Durable receipts by receipt namespace, then idempotency key.
/// Receipts by namespace and key. The per-namespace maps are persistent, so a
/// writer can capture the whole state for a checkpoint in constant time.
pub(crate) type Receipts = BTreeMap<Option<[u8; 32]>, NamespaceReceipts>;
pub(crate) type NamespaceReceipts = imbl::OrdMap<Vec<u8>, StoredReceipt>;

/// What a decision reads: an index, the durable receipts, and whether a timed write is pending.
pub(crate) struct View<'a> {
    pub(crate) index: &'a BranchIndex,
    pub(crate) durable_index: &'a BranchIndex,
    pub(crate) receipts: &'a Receipts,
    appointment_addresses: &'a BTreeSet<[u8; 32]>,
    pub(crate) pending: bool,
}

/// State folded from a single stream. A caller resolves at its fresh committed prefix.
#[derive(Clone)]
pub struct TransactState {
    index: BranchIndex,
    receipts: Receipts,
    appointment_addresses: BTreeSet<[u8; 32]>,
    pending: Option<Pending>,
    pending_erasure: Option<TxKey>,
}

impl TransactState {
    pub fn new(incarnation: [u8; 16]) -> Self {
        Self {
            index: BranchIndex::new(incarnation), receipts: BTreeMap::new(), appointment_addresses: BTreeSet::new(),
            pending: None, pending_erasure: None,
        }
    }

    pub fn index(&self) -> &BranchIndex { &self.index }

    pub fn reclaim_store_history(&mut self, keep_from: u64) {
        self.index.reclaim_store_history(keep_from);
    }

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

    /// Copies authenticated shared-checkpoint receipts onto their exact native transaction cuts.
    /// Original physical coordinates remain evidence on the source installation and are cleared in the copy.
    pub fn import_classic_receipts(
        &mut self, original: &Self, map: impl Fn(InstalledReceipt) -> Option<Basis>,
    ) -> Result<usize> {
        if self.pending.is_some() || !self.receipts.is_empty() || original.pending.is_some() || original.pending_erasure.is_some() {
            return Err(invalid_record());
        }
        let mut receipts = original.receipts.clone();
        let mut count = 0;
        for namespace in receipts.values_mut() {
            let mut copied = NamespaceReceipts::new();
            for (key, stored) in namespace.iter() {
                let mut stored = stored.clone();
                let basis = stored.installed.and_then(&map).ok_or_else(invalid_record)?;
                if stored.workload.is_some() || stored.receipt.decision_clock.is_some_and(FundedReceipt::is_appointment)
                    || self.index.exact(basis)?.system_time != stored.receipt.system_time
                {
                    return Err(invalid_record());
                }
                stored.receipt.basis = basis;
                stored.installed = None;
                count += 1;
                copied.insert(key.clone(), stored);
            }
            *namespace = copied;
        }
        self.receipts = receipts;
        Ok(count)
    }

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

    pub fn verify_repaired_checkpoint(&self, selected: &Self) -> Result<()> {
        if self.pending.is_some() || selected.pending.is_some() || self.receipts.len() != selected.receipts.len() {
            return Err(invalid_record());
        }
        for (scope, receipts) in &selected.receipts {
            let actual = self.receipts.get(scope).filter(|actual| actual.len() == receipts.len()).ok_or_else(invalid_record)?;
            for (key, expected) in receipts {
                let actual = actual.get(key).ok_or_else(invalid_record)?;
                if actual.fingerprint != expected.fingerprint || actual.receipt != expected.receipt
                    || actual.workload != expected.workload || actual.workload_digest != expected.workload_digest
                    || expected.installed.is_some_and(|position| actual.installed != Some(position))
                {
                    return Err(invalid_record());
                }
            }
        }
        struct HashWriter(Sha256);
        impl std::io::Write for HashWriter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.update(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
        }
        let index_digest = |index: &BranchIndex| -> Result<[u8; 32]> {
            let mut writer = HashWriter(Sha256::new());
            serde_json::to_writer(&mut writer, index).map_err(|error| Anomaly::fault("rtdb/transaction-checkpoint", error.to_string()))?;
            Ok(writer.0.finalize().into())
        };
        if index_digest(&self.index)? != index_digest(&selected.index)? { return Err(invalid_record()); }
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

    pub fn require_portable_checkpoint(&self) -> Result<()> {
        if self.pending.is_some() { return Err(pending_error()); }
        if self.pending_erasure.is_some() { return Err(erasure_pending()); }
        let unsupported_receipt = self.receipts.values().flat_map(|scope| scope.values()).any(|stored| {
            stored.receipt_namespace.is_some() || stored.receipt.decision_clock.is_some()
                || stored.installed.is_some() || stored.workload.is_some()
        });
        if unsupported_receipt || self.index.has_custody_groups() || self.index.inherited_custody().is_some()
            || self.index.retained_images().any(|(_, table, _, image)| {
                image.custody_group.is_some() || image.custody_payload.is_some()
                    || BranchIndex::unowned_checkpoint_image(table, image)
            })
        {
            return Err(Anomaly::unsupported("rtdb/backup-custody", "The checkpoint needs an independent custody transfer"));
        }
        Ok(())
    }

    pub fn encode_authenticated_portable_checkpoint(&self) -> Result<Bytes> {
        let mut cut = self.clone();
        for receipts in cut.receipts.values_mut() {
            *receipts = receipts.iter().filter_map(|(key, stored)| {
                let mut stored = stored.clone();
                if let Some(offer) = stored.receipt.decision_clock.filter(|offer| !offer.is_appointment())
                    && stored.receipt_namespace.is_none() && stored.installed.is_none() && stored.workload.is_none()
                {
                    if offer.booked() { return None; }
                    stored.receipt.decision_clock = None;
                }
                Some((key.clone(), stored))
            }).collect();
        }
        cut.receipts.retain(|_, receipts| !receipts.is_empty());
        cut.require_portable_checkpoint()?;
        cut.encode_binary_checkpoint()
    }

    pub fn encode_native_checkpoint_at(&self, basis: Basis) -> Result<Bytes> {
        if self.pending.is_some() { return Err(pending_error()); }
        if self.pending_erasure.is_some() { return Err(erasure_pending()); }
        self.require_native_checkpoint_receipts()?;
        self.encode_checkpoint_index(&self.index.native_checkpoint_at(basis)?)
    }

    pub fn require_native_checkpoint_receipts(&self) -> Result<()> {
        for (key, basis) in self.index.native_checkpoint_receipts() {
            if self.original_custody_receipt(key, basis).is_err() {
                return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb/native-checkpoint-receipt", "A carrier has no matching original applied receipt"));
            }
        }
        Ok(())
    }

    pub fn original_custody_receipt(&self, key: &[u8], basis: Basis) -> Result<Receipt> {
        Self::exact_original_receipt(&self.receipts, key, basis, original_custody_family)
    }

    pub fn require_native_custody_fence(&self, fence: &NativeCustodyFence) -> Result<()> {
        Self::check_native_custody_fence(&self.receipts, fence)
    }

    pub fn native_dependent_custody_status(&self, scope: NativeDependentCustodyScope, fence: &NativeCustodyFence) -> Result<Option<Receipt>> {
        self.index.require_native_dependent_fence(scope, fence)?;
        self.require_native_custody_fence(fence)?;
        let Some((_, Some(terminal))) = self.index.native_dependent_custody(scope)? else { return Ok(None); };
        if &terminal.fence != fence { return Err(native_custody_command_error()); }
        let request = Transact::native_dependent_custody(NativeDependentCustodyOrder::Dispose(terminal.clone()));
        let receipt = self.recover_exact(&request.key, Some(native_checkpoint_namespace()), request.logical_sha256()?)?;
        if receipt.outcome != Outcome::Then { return Err(invalid_record()); }
        Ok(Some(receipt))
    }

    fn check_native_custody_fence(receipts: &Receipts, fence: &NativeCustodyFence) -> Result<()> {
        let receipt = Self::exact_original_receipt(receipts, &fence.revoke_key, fence.revoked_at, original_custody_family)?;
        if receipt.outcome != Outcome::Then || receipt.erasure.is_some() {
            return Err(native_custody_command_error());
        }
        Ok(())
    }

    pub fn original_writer_receipt(&self, key: &[u8], basis: Basis) -> Result<Receipt> {
        Self::exact_original_receipt(&self.receipts, key, basis, |family| matches!(family,
            WorkloadFamily::CustodyWrite | WorkloadFamily::ClaimWriter))
    }

    pub fn original_line_creation_receipt(&self, key: &[u8], basis: Basis) -> Result<Receipt> {
        Self::exact_original_receipt(&self.receipts, key, basis, |family| family == WorkloadFamily::ProvisionBranch)
    }

    fn exact_original_receipt(
        receipts: &Receipts, key: &[u8], basis: Basis, allows_family: impl Fn(WorkloadFamily) -> bool,
    ) -> Result<Receipt> {
        validate_key(key)?;
        let mut matches = receipts.iter().filter_map(|(namespace, receipts)| {
            let stored = receipts.get(key)?;
            (stored.receipt.basis == basis
                && (namespace.is_none() || stored.workload.is_some_and(&allows_family))
                && !stored.receipt.decision_clock.is_some_and(|offer| offer.booked())
                && stored.receipt.erasure != Some(Erasure::Pending)
                && !matches!(stored.receipt.outcome, Outcome::Refused { .. }))
                .then_some(&stored.receipt)
        });
        let receipt = matches.next().ok_or_else(unknown_receipt)?;
        if matches.next().is_some() { return Err(invalid_record()); }
        Ok(replay(receipt))
    }

    pub fn restore_native_checkpoint(&mut self, custody: &NativeCheckpointCustody) -> Result<()> {
        if self.pending.is_some() { return Err(pending_error()); }
        if self.pending_erasure.is_some() { return Err(erasure_pending()); }
        self.require_native_checkpoint_receipts()?;
        self.index.restore_native_checkpoint(custody)?;
        self.receipts.clear();
        self.appointment_addresses.clear();
        Ok(())
    }

    /// Re-namespace the retained receipt coordinates along with their fold.
    pub fn restore_in(&mut self, incarnation: [u8; 16]) -> Result<()> {
        if self.pending.is_some() { return Err(pending_error()); }
        if self.pending_erasure.is_some() { return Err(erasure_pending()); }
        self.index.restore_in(incarnation)?;
        self.appointment_addresses.clear();
        for receipts in self.receipts.values_mut() {
            *receipts = receipts.iter()
                .filter(|(_, stored)| !stored.receipt.decision_clock.is_some_and(|offer| offer.booked()))
                .map(|(key, stored)| {
                    let mut stored = stored.clone();
                    stored.receipt.basis = Basis::new(incarnation, stored.receipt.basis.position());
                    stored.receipt.decision_clock = None;
                    stored.installed = None;
                    (key.clone(), stored)
                })
                .collect();
        }
        self.receipts.retain(|_, receipts| !receipts.is_empty());
        Ok(())
    }

    fn encode_checkpoint_index(&self, index: &BranchIndex) -> Result<Bytes> {
        let (version, receipts) = self.checkpoint_receipts(index);
        serde_json::to_vec(&(version, index, receipts)).map(Bytes::from)
            .map_err(|error| Anomaly::fault("rtdb/transaction-checkpoint", error.to_string()))
    }

    fn checkpoint_receipts(&self, index: &BranchIndex) -> (u8, Vec<std::borrow::Cow<'_, StoredReceipt>>) {
        use std::borrow::Cow;
        let cut = index.head().basis;
        let receipts = self.receipts.values().flat_map(|namespace| namespace.values()).filter_map(|stored| {
            if stored.receipt.basis <= cut { return Some(Cow::Borrowed(stored)); }
            stored.receipt.decision_clock?.appointment_checkpoint(stored, cut).map(Cow::Owned)
        }).collect::<Vec<_>>();
        let version = Self::checkpoint_version(index, receipts.iter().map(|receipt| receipt.as_ref()));
        (version, receipts)
    }

    fn checkpoint_version<'a>(index: &BranchIndex, receipts: impl Iterator<Item = &'a StoredReceipt>) -> u8 {
        let floor_version = if index.store_history_floor() == 0 { RECORD_VERSION } else { PRUNED_STORE_CHECKPOINT_VERSION };
        receipts.map(|stored| stored.receipt.record_version(stored.receipt_namespace)).fold(floor_version, u8::max)
    }

    /// The checkpoint of [`Self::encode_checkpoint`] in CBOR instead of JSON:
    /// the same fields, with byte arrays and integers in binary.
    pub fn encode_binary_checkpoint(&self) -> Result<Bytes> {
        if self.pending.is_some() { return Err(pending_error()); }
        let (version, receipts) = self.checkpoint_receipts(&self.index);
        let mut bytes = Vec::new();
        ciborium::into_writer(&(version, &self.index, receipts), &mut bytes)
            .map_err(|error| Anomaly::fault("rtdb/transaction-checkpoint", error.to_string()))?;
        Ok(Bytes::from(bytes))
    }

    pub fn decode_binary_checkpoint(bytes: &[u8], expected: BranchCut) -> Result<Self> {
        let (version, index, receipts): (u8, BranchIndex, Vec<StoredReceipt>) = ciborium::from_reader(bytes)
            .map_err(|error| Anomaly::fault("rtdb/transaction-checkpoint", error.to_string()))?;
        Self::from_checkpoint(version, index, receipts, expected)
    }

    pub fn decode_checkpoint(bytes: &[u8], expected: BranchCut) -> Result<Self> {
        let (version, index, receipts): (u8, BranchIndex, Vec<StoredReceipt>) = serde_json::from_slice(bytes)
            .map_err(|error| Anomaly::fault("rtdb/transaction-checkpoint", error.to_string()))?;
        Self::from_checkpoint(version, index, receipts, expected)
    }

    fn from_checkpoint(version: u8, index: BranchIndex, receipts: Vec<StoredReceipt>, expected: BranchCut) -> Result<Self> {
        let invalid = || Anomaly::fault("rtdb/transaction-checkpoint", "Checkpoint does not match the expected cut");
        if version != Self::checkpoint_version(&index, receipts.iter())
            || index.store_history_floor() > expected.basis.position()
            || index.head() != expected || index.exact(expected.basis)? != expected
        {
            return Err(invalid());
        }
        let mut state = Self {
            index, receipts: BTreeMap::new(), appointment_addresses: BTreeSet::new(),
            pending: None, pending_erasure: None,
        };
        for stored in receipts {
            let receipt = &stored.receipt;
            validate_key(&receipt.key).map_err(|_| invalid())?;
            if receipt.replayed || state.index.exact(receipt.basis)?.system_time != receipt.system_time
                || stored.workload_digest.is_some() && stored.workload.is_none()
            {
                return Err(invalid());
            }
            if receipt.erasure == Some(Erasure::Pending) && state.pending_erasure.is_some() {
                return Err(invalid());
            }
            if let Some(offer) = receipt.decision_clock
                && let Some(address) = offer.appointment_address()
                && (stored.receipt_namespace.is_some_and(|namespace| !offer.belongs_to(namespace))
                    || stored.workload.is_some() || receipt.erasure.is_some()
                    || !offer.appointment_valid(BranchCut { basis: receipt.basis, system_time: receipt.system_time })
                    || stored.receipt_namespace.is_some() && !offer.managed_appointment_time_valid(BranchCut {
                        basis: receipt.basis, system_time: receipt.system_time,
                    })
                    || state.appointment_addresses.contains(&address))
            {
                return Err(invalid());
            }
            if state.store_receipt(stored).is_some() { return Err(invalid()); }
        }
        Ok(state)
    }

    fn store_receipt(&mut self, stored: StoredReceipt) -> Option<StoredReceipt> {
        let key = stored.receipt.key.clone();
        if let Some(address) = stored.receipt.decision_clock.and_then(FundedReceipt::appointment_address) {
            self.appointment_addresses.insert(address);
        }
        if stored.receipt.erasure == Some(Erasure::Pending) {
            self.pending_erasure = Some(TxKey { namespace: stored.receipt_namespace, key: key.clone() });
        }
        self.receipts.entry(stored.receipt_namespace).or_default().insert(key, stored)
    }

    pub fn pending_erasure(&self) -> Option<Basis> {
        let key = self.pending_erasure.as_ref()?;
        self.receipts.get(&key.namespace)?.get(&key.key).map(|stored| stored.receipt.basis)
    }

    pub fn pending_erasure_installation(&self) -> Option<(Basis, InstalledReceipt)> {
        let key = self.pending_erasure.as_ref()?;
        let stored = self.receipts.get(&key.namespace)?.get(&key.key)?;
        Some((stored.receipt.basis, stored.installed?))
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
        let stored = self.lookup_receipt(key, namespace)?;
        if stored.fingerprint != fingerprint {
            return Err(conflicting_key());
        }
        Ok(replay(&stored.receipt))
    }

    fn tx_status_with_namespace(&self, key: &[u8], namespace: Option<[u8; 32]>) -> Result<Receipt> {
        let stored = self.lookup_receipt(key, namespace)?;
        Ok(replay(&stored.receipt))
    }

    fn lookup_receipt(&self, key: &[u8], namespace: Option<[u8; 32]>) -> Result<&StoredReceipt> {
        self.find_receipt(key, namespace)?.ok_or_else(unknown_receipt)
    }

    /// The committed receipt under `key`, or None where no transaction has committed with it.
    /// A key whose transaction is still pending is an error, not an absence.
    fn find_receipt(&self, key: &[u8], namespace: Option<[u8; 32]>) -> Result<Option<&StoredReceipt>> {
        validate_key(key)?;
        if self.pending.as_ref().is_some_and(|pending| {
            pending.tx.receipt_namespace == namespace && pending.tx.receipt.key == key
        }) {
            return Err(pending_error());
        }
        let Some(stored) = self.receipts.get(&namespace).and_then(|receipts| receipts.get(key)) else { return Ok(None); };
        if stored.receipt.decision_clock.is_some_and(|offer| offer.booked()) { return Err(pending_error()); }
        Ok(Some(stored))
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
        if receipt.decision_clock.is_some_and(FundedReceipt::is_appointment) { return Err(appointment_invalid()); }
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
            appointment_addresses: &self.appointment_addresses,
            pending: self.pending.is_some(),
        };
        Self::resolve_in(view, request, position, now, features, claims, authority, None, None).map(|(resolution, _)| resolution)
    }

    /// Resolves `request` at `position` against `view`. For an append it also returns the
    /// index after the transaction publishes: exactly what [`TransactState::apply`] computes
    /// for the record, or for a timed write for its `Settle` (advance to the receipt cut,
    /// then the effects in order).
    pub(crate) fn resolve_in(
        view: View<'_>, request: &Transact, position: u64, now: Timestamp, features: &[&str], claims: &dyn ClaimVerifier,
        authority: Option<ScopedTransact<'_>>, workload: Option<&workload::Admission>, funding: Option<FundingInput>,
    ) -> Result<(Resolution, Option<BranchIndex>)> {
        view.index.require_local_custody()?;
        validate_key(&request.key)?;
        if request.contains_native_custody_transfer() && (authority.is_some() || workload.is_some()) {
            return Err(native_custody_command_error());
        }
        if let [Op::NativeDependentCustody(NativeDependentCustodyOrder::Dispose(disposition))] = request.ops.as_slice() {
            Self::check_native_custody_fence(view.receipts, &disposition.fence)?;
        }
        let appointment = funding.filter(|input| input.is_appointment());
        let funding = funding.filter(|input| !input.is_appointment());
        let funded_request = funding.is_some() || appointment.is_some();
        if appointment.is_some() && (authority.is_some() || workload.is_some()) {
            return Err(appointment_invalid());
        }
        if appointment.is_none() && request.require.iter().any(|feature| feature == FUNDED_APPOINTMENT) {
            return Err(appointment_invalid());
        }
        let hold = if request.ops.iter().chain(&request.otherwise).any(|op| matches!(op, Op::ScopedHold(_))) {
            let [Op::ScopedHold(command)] = request.ops.as_slice() else { return Err(invalid_scoped_hold()); };
            if authority.is_none_or(|authority| authority.receipt_namespace != command.owner())
                || workload.is_some() || request.canonical_request()? != Transact::scoped_hold(command.clone()).canonical_request()?
                || request.custody_request_digest.is_some()
            {
                return Err(invalid_scoped_hold());
            }
            Some(command)
        } else { None };
        let receipt_namespace = request.receipt_namespace(
            workload.map(|admission| admission.namespace).or(authority.map(|authority| authority.receipt_namespace))
                .or(appointment.and_then(FundingInput::managed_appointment_namespace))
        );
        let family = workload.map(workload::Admission::family);
        let workload_digest = workload.map(workload::Admission::authorization_digest);
        let canonical_request = request.canonical_request()?;
        let fingerprint = request.custody_request_digest
            .unwrap_or_else(|| Sha256::digest(canonical_request.as_bytes()).into());
        let stored = view.receipts.get(&receipt_namespace).and_then(|namespace| namespace.get(&request.key));
        if let Some(stored) = stored && appointment.is_none() {
            let same_request = match workload_digest {
                Some(digest) => stored.matches_workload_digest(digest)
                    && (stored.workload_digest.is_none() || stored.fingerprint == fingerprint),
                None => stored.fingerprint == fingerprint,
            };
            if !same_request || stored.workload != family {
                return Err(conflicting_key());
            }
            if funding.is_none() {
                if stored.receipt.decision_clock.is_some() { return Err(conflicting_key()); }
                return Ok((Resolution::Answer(replay(&stored.receipt)), None));
            }
        }
        if request.ops.iter().chain(&request.otherwise).any(|op| matches!(op, Op::Store(_))) {
            let append = request.store_append().ok_or_else(store_append_shape)?;
            if authority.is_some() || workload.is_some() || funded_request || hold.is_some() || receipt_namespace.is_some() {
                return Err(store_append_shape());
            }
            return Self::resolve_store(view, request, append, position, now, claims, fingerprint, canonical_request);
        }
        let funded = if let Some(input) = funding {
            if workload.is_some() { return Err(funding_invalid()); }
            match resolve_funding(&view, request, input, stored, authority)? {
                FundingDecision::Replay(receipt) => return Ok((Resolution::Answer(receipt), None)),
                decision => Some(decision),
            }
        } else { None };
        if workload.is_some() {
            for claim in request.claims() {
                if claims.verify(claim)?.scope.branch != MAIN_BRANCH { return Err(workload::denied()); }
            }
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
        let validation = !funded_request && workload.is_none() && request.ops.is_empty() && request.otherwise.is_empty();
        let before = if funded_request {
            None
        } else if validation {
            if !features.contains(&LINEARIZABLE) { return Err(feature_required(LINEARIZABLE)); }
            None
        } else {
            if request.window.before.is_some() && !features.contains(&TIMED_TRANSACT) {
                return Err(feature_required(TIMED_TRANSACT));
            }
            workload.map_or(request.window.before, |admission| admission.cutoff())
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
            let receiver_time = appointment.filter(|input| input.managed_appointment_namespace().is_some())
                .and_then(FundingInput::appointment_observed_at).unwrap_or(Timestamp::MIN);
            BranchCut {
                basis: Basis::new(head.basis.incarnation(), receipt_position),
                system_time: now.max(head.system_time.next()?).max(receiver_time),
            }
        };
        let appointment = match appointment {
            Some(FundingInput::Appointment { owner, command, sample, .. }) => {
                match resolve_appointment(&view, request, owner, command, sample, stored, cut, fingerprint)? {
                    AppointmentDecision::Replay(receipt) => return Ok((Resolution::Answer(receipt), None)),
                    decision => Some(decision),
                }
            }
            _ => None,
        };
        if appointment.is_some() && receipt_namespace.is_some() {
            for claim in request.claims() { claims.verify(claim)?; }
        }
        let mut base = view.index.clone();
        if !validation { base.advance(cut)?; }
        let decision_time = now.max(cut.system_time);
        let advanced = base.clone();
        let scope = authority.map(|authority| authority.labels);
        let mut decision_clock = None;
        let result = if let Some(appointment) = appointment {
            let (offer, decision) = match appointment {
                AppointmentDecision::Prepare(offer) => (offer, (Outcome::Then, Vec::new(), base)),
                AppointmentDecision::Activate { appointment, sample } => {
                    let decision = Self::decide(request, base, sample, sample, features, claims, None,
                        view.durable_index, Some(sample))?;
                    (appointment, decision)
                }
                AppointmentDecision::Consume { appointment, sample } => {
                    let decision = Self::decide(request, base, sample, sample, features, claims, None,
                        view.durable_index, Some(appointment.reading()))?;
                    (appointment, decision)
                }
                AppointmentDecision::Replay(_) => unreachable!("replay answered above"),
            };
            if receipt_namespace.is_some() && !offer.managed_appointment_time_valid(cut) {
                return Err(appointment_unavailable());
            }
            check_appointment_effects(request, offer, &decision.0, &decision.1)?;
            decision_clock = Some(offer);
            Ok(decision)
        } else if let Some(funded) = funded {
            match funded {
                FundingDecision::Book(offer) => {
                    decision_clock = Some(offer);
                    Ok((Outcome::Then, Vec::new(), base))
                }
                FundingDecision::Consume { receipt, sample, refusal } => {
                    decision_clock = Some(receipt);
                    match refusal {
                        Some(error) => Err(error),
                        None => Self::decide(request, base, sample, sample, features, claims, scope,
                            view.durable_index, Some(receipt.reading())),
                    }
                }
                FundingDecision::Replay(_) => unreachable!("replay answered above"),
            }
        } else if let Some(command) = hold {
            let labels = scope.ok_or_else(invalid_scoped_hold)?;
            base.check_scoped_hold(command, labels, now).and_then(|()| {
                let effect = BranchEffect::ScopedHold { command: command.clone(), observed_at: now };
                base.apply(effect.clone())?;
                Ok((Outcome::Then, vec![effect], base))
            })
        } else if let Some(outcome) = workload.and_then(workload::Admission::settlement) {
            Ok((outcome, Vec::new(), base))
        } else if workload.is_some() && before.is_some_and(|deadline| decision_time >= deadline) {
            return Err(workload::denied());
        } else {
            Self::decide(request, base, now, decision_time, features, claims, scope, view.durable_index, None)
        };
        let (outcome, effects, after) = match result {
            Ok(decision) => decision,
            Err(error) if error.is_caller_error() => (Outcome::Refused { error }, Vec::new(), advanced),
            Err(error) => return Err(error),
        };
        let receipt = Receipt {
            key: request.key.clone(), basis: cut.basis, system_time: cut.system_time, outcome, replayed: false,
            erasure: effects.iter().any(|effect| matches!(effect, BranchEffect::Erase { .. })).then_some(Erasure::Pending),
            decision_clock,
        };
        if validation {
            Ok((Resolution::Answer(receipt), None))
        } else {
            // For a timed write, `after` is the index once it settles at `position + 1`.
            let record = TransactionRecord::Tx(ResolvedTransact {
                fingerprint, receipt, effects, before, receipt_namespace, admitted_at: now, workload: family,
                workload_digest,
                request: Some(canonical_request),
            });
            Ok((Resolution::Append(PreparedAppend { position, record }), Some(after)))
        }
    }

    /// `rtdb.store.v1.Append`, resolved at the ordering point. Its time is the
    /// writer's clock sample `now` smoothed past the head's as XTDB's `TxResolver`
    /// smooths every transaction, `max(now, head + 1 µs)`, so a classic head stamped
    /// one tick past its predecessor or at a supplied time ahead of the clock is
    /// followed, never refused. Its reads are judged first, then that time by
    /// [`store_clock::judge_store_clock`], the vendored `store_judge::admit`
    /// (Bijection's `fact_store::persistence::judge::admit`, V9′); a time outside
    /// the window decides and records nothing so a retry is judged again.
    ///
    /// Every other outcome is a receipt under the request's key, so a retry returns
    /// the first one: a commit with its stamp `{ micros: time, version: position }`,
    /// a conflict, or a write-free append's validation. A validation is recorded at
    /// its own position with no effects; that position is the version it validated
    /// its reads through.
    #[allow(clippy::too_many_arguments)]
    fn resolve_store(
        view: View<'_>, request: &Transact, append: &StoreAppend, position: u64, now: Timestamp,
        claims: &dyn ClaimVerifier, fingerprint: [u8; 32], canonical_request: CanonicalRequest,
    ) -> Result<(Resolution, Option<BranchIndex>)> {
        let (read_guards, retained_basis) = match request.guards.split_last() {
            Some((Guard::RetainedStoreBasis { basis }, reads)) => (reads, Some(*basis)),
            _ => (request.guards.as_slice(), None),
        };
        if !request.otherwise.is_empty() || !request.requires.is_empty() || !request.require.is_empty()
            || request.window != Window::default() || request.custody_request_digest.is_some()
            || !read_guards.iter().all(|guard| matches!(guard, Guard::Unwritten { .. }))
        {
            return Err(store_append_shape());
        }
        let Window { not_before, before } = append.window;
        if not_before.zip(before).is_some_and(|(start, end)| start >= end) {
            return Err(Anomaly::incorrect("rtdb.store/empty-window", "The store append's window is empty"));
        }
        check_placeholders(&append.writes)?;
        check_bounds(&append.bounds)?;
        let decoded_derives = check_derives(&append.derives, &append.writes)?;
        let head = view.index.head();
        let mutates = !append.writes.is_empty() || !append.bounds.is_empty() || !decoded_derives.is_empty();
        let receipt = |basis: Basis, system_time: Timestamp, outcome: Outcome| Receipt {
            key: request.key.clone(), basis, system_time, outcome, replayed: false, erasure: None, decision_clock: None,
        };

        let last = (head.basis.position() > 0).then_some(head.system_time.0);
        let sample = now;
        let now = crate::system_time::automatic_time(last, sample.0).map(Timestamp)
            .ok_or_else(|| Anomaly::incorrect("xtdb/time-overflow", "Timestamp has no following chronon"))?;
        let guard_time = now;
        if let Some(basis) = retained_basis {
            view.index.check_retained_store_basis(basis)?;
        }
        let mut failed = Vec::new();
        for (i, guard) in read_guards.iter().enumerate() {
            if !guard.evaluate(view.index, guard_time, claims, None)? { failed.push(i); }
        }

        if failed.is_empty() {
            match store_clock::judge_store_clock(
                head.basis.position(),
                last,
                now.0,
                not_before.map(|t| t.0),
                before.map(|t| t.0),
                !mutates,
            ) {
                store_judge::Admitted::Regressed => return Err(store_time_order()),
                store_judge::Admitted::Outside(store_judge::Side::Early) => {
                    return Err(Anomaly::new(
                        AnomalyCategory::Busy,
                        STORE_OUTSIDE_EARLY,
                        "The store append's window has not opened",
                    )
                    .with_context("retry_after_micros", not_before.unwrap_or(now).0.saturating_sub(now.0).into()));
                }
                store_judge::Admitted::Outside(store_judge::Side::Late) => {
                    return Err(Anomaly::new(
                        AnomalyCategory::Conflict,
                        STORE_OUTSIDE_LATE,
                        "The store append's window has closed",
                    ));
                }
                store_judge::Admitted::Exhausted => {
                    return Err(Anomaly::fault("rtdb/transaction-order", "The store version space is exhausted"));
                }
                store_judge::Admitted::Validated(_) | store_judge::Admitted::Committed { .. } => {}
            }
        }

        // Evaluate bounded-counter ops against the live store state at the
        // ordering point, after read guards and window checks. Concurrent Bounds
        // on the same key never add to reads and never conflict with each other.
        let mut failed_bound: Option<usize> = None;
        let mut bound_updates: BTreeMap<Vec<u8>, i64> = BTreeMap::new();
        if failed.is_empty() && !append.bounds.is_empty() {
            let store_view = view.index.view(MAIN_BRANCH, At::Latest, Layer::Merged, guard_time)?;
            for (idx, bound) in append.bounds.iter().enumerate() {
                let current = match bound_updates.get(&bound.key) {
                    Some(&value) => value,
                    None => decode_bound_value(store_view.store_value(&bound.key)?)?,
                };
                match current.checked_add(bound.delta) {
                    Some(next) if next >= bound.min && next <= bound.max => {
                        bound_updates.insert(bound.key.clone(), next);
                    }
                    _ => {
                        failed_bound = Some(idx);
                        break;
                    }
                }
            }
        }

        if position <= head.basis.position() {
            return Err(Anomaly::fault("rtdb/transaction-order", "The append must follow the applied prefix"));
        }
        // Derived writes resolve at the ordering point against the line the
        // prior state holds, after the reads, window and bounds are judged
        // and before plain writes (T3). Applying them never changes the
        // outcome; the unique claims they make are judged once they all
        // apply, on their filled keys (Bijection T7-4).
        let stamp = StoreStamp { micros: now.0, version: position };
        let mut failed_claim: Option<usize> = None;
        let derived = if mutates && failed.is_empty() && failed_bound.is_none() {
            let prior_view = view.index.view(MAIN_BRANCH, At::Latest, Layer::Merged, guard_time)?;
            let derived = resolve_derives(&prior_view, &decoded_derives, stamp)?;
            failed_claim = claim_refused(&prior_view, &decoded_derives, &derived, stamp)?;
            derived
        } else {
            Vec::new()
        };
        let commits = mutates && failed.is_empty() && failed_bound.is_none() && failed_claim.is_none();
        // Every store record takes the append's time: a conflict's, a bound
        // refusal's and a validation's too.
        let system_time = now;
        let cut = BranchCut { basis: Basis::new(head.basis.incarnation(), position), system_time };
        let mut index = view.index.clone();
        index.advance(cut)?;
        let (outcome, effects, after) = if !failed.is_empty() {
            (Outcome::Otherwise { failed }, Vec::new(), index)
        } else if let Some(bound_idx) = failed_bound {
            let error = Anomaly::new(AnomalyCategory::Conflict, STORE_BOUND, "A store bound check failed")
                .with_context("bound", (bound_idx as u64).into());
            (Outcome::Refused { error }, Vec::new(), index)
        } else if let Some(derive_idx) = failed_claim {
            let error = Anomaly::new(AnomalyCategory::Conflict, STORE_CLAIM, "A derive's unique claim counted a second claimant")
                .with_context("derive", (derive_idx as u64).into());
            (Outcome::Refused { error }, Vec::new(), index)
        } else if commits {
            let mut ops: Vec<Op> = Vec::new();
            let bound_writes = bound_updates.into_iter().map(|(key, next)| (key, Some(next.to_be_bytes().to_vec())));
            for (key, value) in derived.into_iter().chain(stamp_writes(&append.writes, stamp)?).chain(bound_writes) {
                let id = EntityId::Binary(key);
                // Consecutive puts and deletes keep their order, so a later write of a key wins.
                match (value, ops.last_mut()) {
                    (Some(value), Some(Op::Put { rows, .. })) => rows.push(store_row(id, value)),
                    (Some(value), _) => ops.push(Op::Put {
                        branch: MAIN_BRANCH.into(), table: store_table(), rows: vec![store_row(id, value)], if_absent: false,
                    }),
                    (None, Some(Op::Delete { ids, .. })) => ids.push(id),
                    (None, _) => ops.push(Op::Delete { branch: MAIN_BRANCH.into(), table: store_table(), ids: vec![id], valid: None }),
                }
            }
            let mut then = Candidate::new(index, None);
            then.execute(&ops, &request.key, None)?;
            then.check_constraints()?;
            (Outcome::Then, then.effects, then.index)
        } else {
            // A validation: its receipt, and nothing else, at its own position.
            (Outcome::Then, Vec::new(), index)
        };
        let record = TransactionRecord::Tx(ResolvedTransact {
            fingerprint, receipt: receipt(cut.basis, cut.system_time, outcome), effects, before: None,
            receipt_namespace: None, admitted_at: sample, workload: None, workload_digest: None,
            request: Some(canonical_request),
        });
        Ok((Resolution::Append(PreparedAppend { position, record }), Some(after)))
    }

    fn decide(
        request: &Transact, base: BranchIndex, sample: Timestamp, time: Timestamp,
        features: &[&str], claims: &dyn ClaimVerifier, scope: Option<&LabelScope>, durable: &BranchIndex,
        terms_time: Option<Timestamp>,
    ) -> Result<(Outcome, Vec<BranchEffect>, BranchIndex)> {
        if let Some(feature) = request.require.iter().filter(|feature| {
            !features.contains(&feature.as_str())
                && !(feature.as_str() == REVIEWED_CUSTODY_PLACEMENT && features.contains(&ROW_CUSTODY))
                && !(matches!(feature.as_str(), DECISION_CLOCK_OFFER | FUNDED_APPOINTMENT) && terms_time.is_some())
        }).min() {
            return Err(feature_required(feature));
        }
        if !request.requires.is_empty() && !features.contains(&TRANSACT_REQUIRES) {
            return Err(feature_required(TRANSACT_REQUIRES));
        }
        let ranked_index = rtdb_types::rtdb_v1::RANKED_INDEX_DECLARATIONS;
        if !features.contains(&ranked_index) && request.ops.iter().chain(&request.otherwise)
            .any(|op| matches!(op, Op::Declare { change: Declaration::RankedIndex { .. }, .. }))
        {
            return Err(feature_required(ranked_index));
        }
        let response_release = request.is_response_release();
        let reviewed_custody = request.uses_reviewed_custody();
        if reviewed_custody && (
            scope.is_some() || request.otherwise.iter().any(Op::is_reviewed_custody)
                || !matches!(request.requires.first(), Some(Guard::Version {
                    branch, valid_at: Some(_), expect: Version::Written(_), ..
                }) if branch == MAIN_BRANCH)
                || !request.require.iter().any(|feature| feature == REVIEWED_CUSTODY_PLACEMENT)
        ) {
            return Err(Anomaly::incorrect("rtdb/custody-review", "A reviewed placement requires original writer authority and the accepting arm"));
        }
        if request.contains_native_custody_transfer() {
            let key = match request.ops.as_slice() {
                [Op::NativeCustodyTransfer { custody, destination }] => native_custody_key(custody, destination.is_some()),
                [Op::NativeDependentCustody(order)] => dependent_custody_key(order),
                _ => return Err(native_custody_command_error()),
            };
            if scope.is_some() || !request.otherwise.is_empty() || !request.guards.is_empty()
                || !request.requires.is_empty() || request.window != Window::default()
                || request.custody_request_digest.is_some() || request.key != key
            {
                return Err(native_custody_command_error());
            }
        }
        if request.uses_custody() {
            if request.contains_response_release() && (!response_release || scope.is_some()) {
                return Err(Anomaly::incorrect("rtdb/custody-release", "Response release requires its original writer and private receipt namespace"));
            }
            if !response_release && !features.contains(&ROW_CUSTODY) {
                return Err(feature_required(ROW_CUSTODY));
            }
        }
        if let [Op::AdmitCustodyResponse(admission)] = request.ops.as_slice()
            && request.key.as_slice() != admission.part_key()?.as_slice()
        {
            return Err(Anomaly::incorrect("rtdb/custody-release", "Response admission requires the original part key"));
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
        let guard_time = if request.require.iter().any(|feature| feature == FUNDED_APPOINTMENT) {
            time
        } else {
            terms_time.unwrap_or(time)
        };
        let evaluate = |guard: &Guard, index: &BranchIndex| {
            if let (Some(reading), Some(scope)) = (terms_time, scope) {
                let projected = visibility.get_or_init(|| durable.project_main_visibility(scope))
                    .as_ref().map_err(Clone::clone)?;
                return guard.evaluate_funded_scope(projected, reading, claims);
            }
            let visible = match (scope, guard) {
                (Some(scope), Guard::Version { expect: Version::Written(_), .. }) => Some(
                    visibility.get_or_init(|| durable.project_main_visibility(scope))
                        .as_ref().map_err(Clone::clone)?,
                ),
                _ => None,
            };
            guard.evaluate(index, guard_time, claims, visible)
        };
        for (i, requirement) in request.requires.iter().enumerate() {
            if requirement.after() {
                return Err(Anomaly::incorrect(
                    "rtdb/invalid-requirement", "A count requirement must use the Before phase",
                ));
            }
            let valid = if response_release || (reviewed_custody && i == 0) {
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

        for source in request.ops.iter().filter_map(|op| match op {
            Op::ReserveReviewedCustodyPlacement { source, .. } => Some(source),
            _ => None,
        }) {
            let approved = request.requires.iter().any(|guard| {
                let Guard::Unchanged(claim) = guard else { return false; };
                claims.verify(claim).is_ok_and(|evidence| {
                    evidence.scope.branch == source.branch && evidence.scope.basis == source.basis
                        && evidence.scope.layer == Layer::Merged
                })
            });
            if !approved {
                return Err(Anomaly::incorrect("rtdb/custody-review", "The reservation needs a tracked Query requirement for its reviewed cut"));
            }
        }

        let mut failed = Vec::new();
        for (i, guard) in request.guards.iter().enumerate().filter(|(_, guard)| !guard.after()) {
            if !evaluate(guard, &base)? { failed.push(i); }
        }
        if failed.is_empty() {
            let mut then = Candidate::new(base.clone(), scope);
            then.terms_time = terms_time;
            then.execute(&request.ops, &request.key, before)?;
            for (i, guard) in request.guards.iter().enumerate().filter(|(_, guard)| guard.after()) {
                if !evaluate(guard, &then.index)? { failed.push(i); }
            }
            if failed.is_empty() {
                if !request.ops.is_empty() { then.check_constraints()?; }
                return Ok((Outcome::Then, then.effects, then.index));
            }
        }
        let mut otherwise = Candidate::new(base, scope);
        otherwise.terms_time = terms_time;
        otherwise.execute(&request.otherwise, &request.key, before)?;
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
                if receipt.decision_clock.is_some() { tx.validate_admission()?; }
                let expected = if tx.before.is_some() { position.checked_add(1) } else { Some(position) };
                let head = self.index.head();
                let previous = self.receipts.get(&tx.receipt_namespace).and_then(|namespace| namespace.get(&receipt.key));
                let exact_booking = match (receipt.decision_clock, previous) {
                    (Some(offer), previous) if offer.is_appointment() => {
                        offer.appointment_matches_previous(previous.and_then(|entry| entry.receipt.decision_clock))
                            && match previous {
                                Some(entry) => entry.receipt.decision_clock.is_some_and(FundedReceipt::is_appointment),
                                None => self.index.exact(offer.original()).is_ok()
                                    && offer.appointment_address().is_some_and(|address| !self.appointment_addresses.contains(&address)),
                            }
                    }
                    (Some(offer), None) => offer.booked() && self.index.exact(offer.original()).is_ok(),
                    (Some(offer), Some(previous)) => previous.fingerprint == tx.fingerprint
                        && previous.receipt.decision_clock.is_some_and(|booked| offer.terminal_of(booked)),
                    (None, None) => true,
                    (None, Some(_)) => false,
                };
                if self.pending.is_some() || position <= head.basis.position()
                    || Some(receipt.basis.position()) != expected
                    || receipt.basis.incarnation() != head.basis.incarnation() || receipt.system_time < head.system_time
                    || receipt.replayed || validate_key(&receipt.key).is_err()
                    || !exact_booking
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
                    self.publish(tx.receipt, tx.fingerprint, tx.receipt_namespace, tx.workload, tx.workload_digest, tx.effects).map(Some)
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
                    pending.tx.receipt.clone(), pending.tx.fingerprint, pending.tx.receipt_namespace, pending.tx.workload,
                    pending.tx.workload_digest, pending.tx.effects.clone(),
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
                let receipt = self.publish(receipt, pending.tx.fingerprint, pending.tx.receipt_namespace, pending.tx.workload,
                    pending.tx.workload_digest, Vec::new())?;
                self.pending = None;
                Ok(Some(receipt))
            }
        }
    }

    fn publish(&mut self, receipt: Receipt, fingerprint: [u8; 32], receipt_namespace: Option<[u8; 32]>,
               workload: Option<WorkloadFamily>, workload_digest: Option<[u8; 32]>, effects: Vec<BranchEffect>) -> Result<Receipt> {
        let mut candidate = self.index.clone();
        candidate.advance(BranchCut { basis: receipt.basis, system_time: receipt.system_time })?;
        for effect in effects {
            if let BranchEffect::NativeDependentCustody(order) = &effect {
                if receipt_namespace != Some(native_checkpoint_namespace())
                    || receipt.key != dependent_custody_key(order)
                    || receipt.outcome != Outcome::Then || workload.is_some()
                {
                    return Err(invalid_record());
                }
                if let NativeDependentCustodyOrder::Dispose(disposition) = order {
                    self.require_native_custody_fence(&disposition.fence)?;
                }
            }
            if let BranchEffect::ReserveReviewedCustodyPlacement { reviewed, .. } = &effect
                && (receipt_namespace.is_some() && workload != Some(WorkloadFamily::OriginalFamilyWrite)
                    || reviewed.receipt_key != receipt.key)
            {
                return Err(invalid_record());
            }
            if let BranchEffect::ActivateCustodyOccurrence { occurrence, .. }
                | BranchEffect::ActivateDerivedCustodyOccurrence { occurrence, .. } = &effect
                && (receipt_namespace.is_some() && !matches!(workload, Some(WorkloadFamily::CustodyWrite | WorkloadFamily::OriginalFamilyWrite))
                    || !occurrence.receipt_key.is_empty() && occurrence.receipt_key != receipt.key)
            {
                return Err(invalid_record());
            }
            candidate.apply(effect)?;
        }
        self.index = candidate;
        self.store_receipt(StoredReceipt { fingerprint, receipt: receipt.clone(), receipt_namespace, installed: None, workload, workload_digest });
        Ok(receipt)
    }
}

fn native_custody_command_error() -> Anomaly {
    Anomaly::incorrect("rtdb/native-checkpoint-command", "Native transfer requires one private ordered custody command")
}

fn original_custody_family(family: WorkloadFamily) -> bool {
    matches!(family, WorkloadFamily::CustodyWrite | WorkloadFamily::OriginalFamilyWrite)
}

fn erasure_pending() -> Anomaly {
    Anomaly::new(AnomalyCategory::Busy, "rtdb/erasure-pending", "The erase receipt is waiting for durable key retirement")
}

fn unknown_receipt() -> Anomaly {
    Anomaly::new(AnomalyCategory::NotFound, "rtdb/unknown-transaction", "No transaction has committed with this key")
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

/// One store entry: its key as a binary id and its value in the `v` cell.
fn store_row(id: EntityId, value: Vec<u8>) -> Row {
    Row {
        id, valid: None,
        cells: StructFields::from_iter([(VALUE_COLUMN.to_owned(), Cell::Value(Value::Binary(value)))]),
        custody_group: None, protected_columns: Vec::new(), custody_payload: None,
    }
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
    fn evaluate_funded_scope(
        &self, history: &VisibleHistory, reading: Timestamp, claims: &dyn ClaimVerifier,
    ) -> Result<bool> {
        match self {
            Self::Unchanged(claim) => {
                let mut evidence = claims.verify(claim)?;
                if evidence.scope.branch != MAIN_BRANCH || evidence.scope.layer != Layer::Merged {
                    return Err(funding_invalid());
                }
                evidence.scope.basis = history.projected_cut(evidence.scope.basis)?.basis;
                history.index().claim_unchanged(&evidence, reading)
            }
            Self::Version { branch, .. } | Self::CountAtMost { branch, .. } if branch != MAIN_BRANCH =>
                Err(funding_invalid()),
            Self::Unwritten { .. } | Self::RetainedStoreBasis { .. } => Err(funding_invalid()),
            _ => self.evaluate(history.index(), reading, claims, None),
        }
    }

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
            Self::Unwritten { range, since } => Ok(index.view(MAIN_BRANCH, At::Latest, Layer::Merged, time)?
                .store_written_since(range, *since)?.is_none()),
            Self::RetainedStoreBasis { .. } => Err(store_append_shape()),
        }
    }
}

struct Candidate<'a> {
    index: BranchIndex,
    effects: Vec<BranchEffect>,
    touched: BTreeSet<Branch>,
    declared: BTreeSet<TableRef>,
    scope: Option<&'a LabelScope>,
    terms_time: Option<Timestamp>,
    affected_labels: BTreeSet<Vec<u8>>,
    single_label: bool,
}

impl<'a> Candidate<'a> {
    fn new(index: BranchIndex, scope: Option<&'a LabelScope>) -> Self {
        Self {
            index, effects: Vec::new(), touched: BTreeSet::new(), declared: BTreeSet::new(),
            scope, terms_time: None, affected_labels: BTreeSet::new(), single_label: false,
        }
    }

    fn push(&mut self, effect: BranchEffect) -> Result<()> {
        self.index.apply(effect.clone())?;
        self.effects.push(effect);
        Ok(())
    }

    fn terms(&self) -> BranchCut {
        let cut = self.index.head();
        BranchCut { system_time: self.terms_time.unwrap_or(cut.system_time), ..cut }
    }
}

#[cfg(test)]
mod tests;
#[cfg(test)]
mod perf;
