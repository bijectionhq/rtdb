use std::{collections::{BTreeMap, BTreeSet}, sync::Arc};

use bytes::Bytes;
use rtdb_index::branches::{
    BranchCut, BranchEffect, BranchIndex, BranchTable, BranchView, RowImage,
};
pub use rtdb_index::branches::{CountRange, IndexDefinition, Visibility};
use rtdb_types::{
    Anomaly, AnomalyCategory, EntityId, Interval, LogicalType, Result, StructFields, TableRef,
    TimeUnit, Timestamp, Value,
    rtdb_v1::{At, Basis, Branch, Claim, ClaimScope, ClaimSealer, LabelScope, Layer, MAIN_BRANCH, valid_branch_name},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

mod operations;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Window {
    pub not_before: Option<Timestamp>,
    pub before: Option<Timestamp>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Transact {
    pub key: Vec<u8>,
    pub require: Vec<String>,
    pub window: Window,
    pub guards: Vec<Guard>,
    pub ops: Vec<Op>,
    pub otherwise: Vec<Op>,
}

#[derive(Clone, Debug, Serialize)]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum Version { Absent, Present, Written(Basis) }

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum When { Before, After }

#[derive(Clone, Debug, Serialize)]
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

#[derive(Clone, Debug, Serialize)]
pub struct Row {
    pub id: EntityId,
    pub valid: Option<Interval>,
    pub cells: StructFields<Cell>,
}

#[derive(Clone, Debug, Serialize)]
pub enum Op {
    Put { branch: Branch, table: TableRef, rows: Vec<Row>, if_absent: bool },
    Delete { branch: Branch, table: TableRef, ids: Vec<EntityId>, valid: Option<Interval> },
    Erase { branch: Option<Branch>, table: TableRef, ids: Vec<EntityId> },
    Declare { table: TableRef, change: Declaration },
    Fork { child: Branch, from: (Branch, Basis), writable: bool, expires_at: Option<Timestamp> },
    Renew { branch: Branch, expires_at: Timestamp },
    Merge { from: Branch, into: Branch },
    DropBranch { branch: Branch },
}

#[derive(Clone, Debug, Serialize)]
pub enum Declaration {
    Schema(StructFields<LogicalType>),
    Index(IndexDefinition),
    Ttl(Option<u64>),
    Constraint { name: String, range: Option<CountRange> },
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
}

pub const TIMED_TRANSACT: &str = "timed-transact";
pub const LINEARIZABLE: &str = "linearizable";

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

const RECORD_VERSION: u8 = 1;

#[derive(Debug, Serialize, Deserialize)]
pub enum TransactionRecord {
    Tx(ResolvedTransact),
    Settle { tx_lsn: u64 },
    Refuse { tx_lsn: u64 },
}

impl TransactionRecord {
    pub fn encode(&self) -> Result<Bytes> {
        serde_json::to_vec(&(RECORD_VERSION, self)).map(Bytes::from)
            .map_err(|error| Anomaly::fault("rtdb/transaction-encoding", error.to_string()))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let (version, record): (u8, Self) = serde_json::from_slice(bytes)
            .map_err(|error| Anomaly::fault("rtdb/transaction-decoding", error.to_string()))?;
        if version != RECORD_VERSION {
            return Err(Anomaly::unsupported("rtdb/transaction-version", "Unknown resolved transaction version"));
        }
        Ok(record)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResolvedTransact {
    fingerprint: [u8; 32],
    receipt: Receipt,
    effects: Vec<BranchEffect>,
    before: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    receipt_namespace: Option<[u8; 32]>,
}

struct Pending {
    position: u64,
    tx: ResolvedTransact,
}

#[derive(Serialize, Deserialize)]
struct StoredReceipt {
    receipt: Receipt,
    fingerprint: [u8; 32],
    #[serde(default, skip_serializing_if = "Option::is_none")]
    receipt_namespace: Option<[u8; 32]>,
}

/// State folded from a single stream. A caller resolves at its fresh committed prefix.
pub struct TransactState {
    index: BranchIndex,
    receipts: BTreeMap<Option<[u8; 32]>, BTreeMap<Vec<u8>, StoredReceipt>>,
    pending: Option<Pending>,
}

impl TransactState {
    pub fn new(incarnation: [u8; 16]) -> Self {
        Self { index: BranchIndex::new(incarnation), receipts: BTreeMap::new(), pending: None }
    }

    pub fn index(&self) -> &BranchIndex { &self.index }

    /// A checkpoint can replace the settled prefix once the mode installs its manifest.
    pub fn encode_checkpoint(&self) -> Result<Bytes> {
        if self.pending.is_some() { return Err(pending_error()); }
        let receipts = self.receipts.values().flat_map(|namespace| namespace.values()).collect::<Vec<_>>();
        serde_json::to_vec(&(RECORD_VERSION, &self.index, receipts)).map(Bytes::from)
            .map_err(|error| Anomaly::fault("rtdb/transaction-checkpoint", error.to_string()))
    }

    pub fn decode_checkpoint(bytes: &[u8], expected: BranchCut) -> Result<Self> {
        let invalid = || Anomaly::fault("rtdb/transaction-checkpoint", "Checkpoint does not match the expected cut");
        let (version, index, receipts): (u8, BranchIndex, Vec<StoredReceipt>) = serde_json::from_slice(bytes)
            .map_err(|error| Anomaly::fault("rtdb/transaction-checkpoint", error.to_string()))?;
        if version != RECORD_VERSION || index.head() != expected || index.exact(expected.basis)? != expected {
            return Err(invalid());
        }
        let mut state = Self { index, receipts: BTreeMap::new(), pending: None };
        for stored in receipts {
            let receipt = &stored.receipt;
            validate_key(&receipt.key).map_err(|_| invalid())?;
            if receipt.replayed || state.index.exact(receipt.basis)?.system_time != receipt.system_time
                || state.receipts.entry(stored.receipt_namespace).or_default().insert(receipt.key.clone(), stored).is_some()
            {
                return Err(invalid());
            }
        }
        Ok(state)
    }

    pub fn tx_status(&self, key: &[u8]) -> Result<Receipt> {
        self.tx_status_with_namespace(key, None)
    }

    pub fn tx_status_scoped(&self, key: &[u8], authority: ScopedTransact<'_>) -> Result<Receipt> {
        self.tx_status_with_namespace(key, Some(authority.receipt_namespace))
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
        &self, request: &Transact, position: u64, now: Timestamp, features: &[&str], claims: &ClaimSealer,
    ) -> Result<Resolution> {
        self.resolve_with_scope(request, position, now, features, claims, None)
    }

    /// The driver binds claims, written-version selectors, and receipt coordinates to the admitted scope.
    pub fn resolve_scoped(
        &self, request: &Transact, position: u64, now: Timestamp, features: &[&str], claims: &ClaimSealer,
        authority: ScopedTransact<'_>,
    ) -> Result<Resolution> {
        let mut request = request.clone();
        if request.guards.iter().any(|guard| matches!(guard, Guard::Version { expect: Version::Written(_), .. })) {
            let history = self.index.project_main_visibility(authority.labels)?;
            for guard in &mut request.guards {
                if let Guard::Version { branch, expect: Version::Written(basis), .. } = guard {
                    if branch != MAIN_BRANCH {
                        return Err(Anomaly::unsupported("rtdb/scoped-branch", "Scoped written versions require the main branch"));
                    }
                    *basis = history.source_basis(*basis)?;
                }
            }
        }
        self.resolve_with_scope(&request, position, now, features, claims, Some(authority))
    }

    pub fn project_receipt(&self, mut receipt: Receipt, scope: &LabelScope) -> Result<Receipt> {
        let cut = self.index.project_main_visibility(scope)?.projected_cut(receipt.basis)?;
        receipt.basis = cut.basis;
        receipt.system_time = cut.system_time;
        Ok(receipt)
    }

    fn resolve_with_scope(
        &self, request: &Transact, position: u64, now: Timestamp, features: &[&str], claims: &ClaimSealer,
        authority: Option<ScopedTransact<'_>>,
    ) -> Result<Resolution> {
        validate_key(&request.key)?;
        let receipt_namespace = authority.map(|authority| authority.receipt_namespace);
        let fingerprint = serde_json::to_vec(request)
            .map(|bytes| Sha256::digest(bytes).into())
            .map_err(|error| Anomaly::incorrect("rtdb/transaction-encoding", error.to_string()))?;
        if let Some(stored) = self.receipts.get(&receipt_namespace).and_then(|namespace| namespace.get(&request.key)) {
            if stored.fingerprint != fingerprint {
                return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb/idempotency-key", "The key belongs to a different transaction"));
            }
            return Ok(Resolution::Answer(replay(&stored.receipt)));
        }

        if self.pending.is_some() { return Err(pending_error()); }
        let head = self.index.head();
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
                system_time: now.max(head.system_time.next()?),
            }
        };
        let mut base = self.index.clone();
        if !validation { base.advance(cut)?; }
        let decision_time = now.max(cut.system_time);
        let result = self.decide(request, base, now, decision_time, features, claims, authority.map(|authority| authority.labels));
        let (outcome, effects) = match result {
            Ok(decision) => decision,
            Err(error) if error.is_caller_error() => (Outcome::Refused { error }, Vec::new()),
            Err(error) => return Err(error),
        };
        let receipt = Receipt {
            key: request.key.clone(), basis: cut.basis, system_time: cut.system_time, outcome, replayed: false,
        };
        if validation {
            Ok(Resolution::Answer(receipt))
        } else {
            let record = TransactionRecord::Tx(ResolvedTransact { fingerprint, receipt, effects, before, receipt_namespace });
            Ok(Resolution::Append(PreparedAppend { position, record }))
        }
    }

    fn decide(
        &self, request: &Transact, base: BranchIndex, sample: Timestamp, time: Timestamp,
        features: &[&str], claims: &ClaimSealer, scope: Option<&LabelScope>,
    ) -> Result<(Outcome, Vec<BranchEffect>)> {
        if let Some(feature) = request.require.iter().find(|feature| !features.contains(&feature.as_str())) {
            return Err(feature_required(feature));
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

        let mut failed = Vec::new();
        for (i, guard) in request.guards.iter().enumerate().filter(|(_, guard)| !guard.after()) {
            if !guard.evaluate(&base, time, claims, scope)? { failed.push(i); }
        }
        if failed.is_empty() {
            let mut then = Candidate::new(base.clone(), scope);
            then.execute(&request.ops)?;
            for (i, guard) in request.guards.iter().enumerate().filter(|(_, guard)| guard.after()) {
                if !guard.evaluate(&then.index, time, claims, scope)? { failed.push(i); }
            }
            if failed.is_empty() {
                if !request.ops.is_empty() { then.check_constraints()?; }
                return Ok((Outcome::Then, then.effects));
            }
        }
        let mut otherwise = Candidate::new(base, scope);
        otherwise.execute(&request.otherwise)?;
        if !request.otherwise.is_empty() { otherwise.check_constraints()?; }
        Ok((Outcome::Otherwise { failed }, otherwise.effects))
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
        self.receipts.entry(receipt_namespace).or_default().insert(
            receipt.key.clone(), StoredReceipt { fingerprint, receipt: receipt.clone(), receipt_namespace },
        );
        Ok(receipt)
    }
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

    fn evaluate(&self, index: &BranchIndex, time: Timestamp, claims: &ClaimSealer, scope: Option<&LabelScope>) -> Result<bool> {
        let view = |branch| index.view(branch, At::Latest, Layer::Merged, time);
        match self {
            Self::Unchanged(claim) => {
                let ClaimScope { branch, basis, layer } = match scope {
                    Some(scope) => claims.open_scoped(claim, scope)?,
                    None => claims.open(claim)?,
                };
                index.is_unchanged(&branch, basis, layer, time)
            }
            Self::Version { branch, table, id, valid_at, expect } => {
                if let Version::Written(basis) = expect { index.exact(*basis)?; }
                let at = valid_at.unwrap_or(time);
                let row = view(branch)?.row(table, id, at);
                Ok(match expect {
                    Version::Absent => row.is_none(),
                    Version::Present => row.is_some(),
                    Version::Written(basis) => row.is_some_and(|event| event.cut.basis == *basis),
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
