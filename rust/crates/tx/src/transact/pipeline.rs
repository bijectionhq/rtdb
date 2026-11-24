//! The writer's commit pipeline (DESIGN.md, "Write-path performance rules").
//!
//! Execution never waits for durability; only replies do. The writer decides each
//! `Transact` against a speculative index: the durable prefix plus every decision made
//! but not yet acknowledged. It hands the driver one record per transaction at increasing
//! positions, which the driver appends in position order and may group into one journal
//! request. When the journal acknowledges a position, every decision through it becomes
//! durable and its receipt is released. If an append fails or the writer's epoch is in
//! doubt, the unacknowledged suffix is withdrawn: its callers learn that the outcome is
//! unknown and recover with `TxStatus` by key. Nothing acknowledged depends on a
//! withdrawn decision, because decisions are made, acknowledged and withdrawn in position
//! order.
//!
//! # Timed writes
//!
//! A timed write (`window.before`) at position `p` publishes at its terminal record at
//! `p + 1`: `Settle` when the original record was durable before the cutoff, else
//! `Refuse`. The terminal record can only be produced after `p` is acknowledged, so the
//! pipeline reserves `p + 1` and keeps deciding: later decisions assume the write will
//! settle, and their positions start at `p + 2`. The driver appends in position order, so
//! nothing after the reserved position is appended before [`TransactPipeline::terminal`]
//! fills it; the terminal record and the waiting decisions then go out in one group. On
//! the rare `Refuse`, every decision after `p + 1` assumed effects that will not publish.
//! None of them can have been appended (the hole blocked them), so the pipeline drops them
//! and returns their requests' keys to be decided again: their outcome is not unknown.
use std::collections::{BTreeMap, VecDeque};

use super::*;

/// What the pipeline asks the driver to do with one request.
pub enum Decision {
    /// Reply now: a replayed receipt for an acknowledged key, or a validation answered at
    /// the durable prefix. Nothing is appended.
    Answer(Receipt),
    /// Append `record` at `position`, in position order. Reply with the receipt that
    /// [`TransactPipeline::acknowledge`] releases once `position` is durable.
    Append { position: u64, record: Bytes },
}

/// A transaction's identity: its receipt namespace (scoped transacts) and idempotency key.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TxKey {
    pub namespace: Option<[u8; 32]>,
    pub key: Vec<u8>,
}

/// The terminal record of a timed write, from [`TransactPipeline::terminal`].
pub struct Terminal {
    /// Append `record` at `position`; it fills the reserved slot and unblocks later appends.
    pub position: u64,
    pub record: Bytes,
    /// On `Refuse`: later decisions that assumed the write would settle. They were never
    /// appended; decide their requests again. Empty on `Settle`.
    pub redecide: Vec<TxKey>,
}

enum Entry {
    /// An ordinary decision; `after` is the index once it applies.
    Untimed { after: BranchIndex, receipt: Receipt, fingerprint: [u8; 32] },
    /// A timed write's original record. It publishes at its terminal record.
    TimedTx { record: TransactionRecord },
    /// A timed write's `Settle` or `Refuse`.
    Terminal { record: TransactionRecord },
}

struct InFlight {
    position: u64,
    key: TxKey,
    entry: Entry,
}

pub struct TransactPipeline {
    durable: TransactState,
    /// Index after the last decision; equals `durable.index` when nothing is in flight.
    speculative: BranchIndex,
    in_flight: VecDeque<InFlight>,
    /// Keys decided but not yet durable.
    deciding: BTreeMap<TxKey, u64>,
    /// Timed writes awaiting their terminal record, by original position, with the index
    /// once each settles.
    timed: BTreeMap<u64, (PreparedAppend, BranchIndex)>,
    next_position: u64,
    max_in_flight: usize,
}

impl TransactPipeline {
    /// `durable` is the folded state through the journal's committed end, and the next
    /// append goes at `next_position`. `max_in_flight` bounds the speculative suffix.
    pub fn new(durable: TransactState, next_position: u64, max_in_flight: usize) -> Result<Self> {
        if durable.pending.is_some() {
            return Err(Anomaly::fault("rtdb/transaction-pending",
                "Order the recovered timed write's refusal before opening the pipeline"));
        }
        if next_position <= durable.index.head().basis.position() {
            return Err(order_error("The pipeline must start after the durable prefix"));
        }
        Ok(Self {
            speculative: durable.index.clone(),
            durable,
            in_flight: VecDeque::new(),
            deciding: BTreeMap::new(),
            timed: BTreeMap::new(),
            next_position,
            max_in_flight: max_in_flight.max(1),
        })
    }

    /// The acknowledged state. Reads, `TxStatus` and checkpoints use only this.
    pub fn durable(&self) -> &TransactState { &self.durable }

    pub fn into_durable(self) -> TransactState { self.durable }

    pub fn next_position(&self) -> u64 { self.next_position }

    pub fn in_flight(&self) -> usize { self.in_flight.len() }

    /// Original positions of timed writes still waiting for their terminal record.
    pub fn awaiting_terminal(&self) -> impl Iterator<Item = u64> + '_ { self.timed.keys().copied() }

    pub fn is_deciding(&self, key: &[u8], namespace: Option<[u8; 32]>) -> bool {
        self.deciding.contains_key(&TxKey { namespace, key: key.to_vec() })
    }

    /// `TxStatus`: a key still in flight reports `rtdb/transaction-pending`.
    pub fn tx_status(&self, key: &[u8]) -> Result<Receipt> {
        if self.deciding.contains_key(&TxKey { namespace: None, key: key.to_vec() }) { return Err(pending_error()); }
        self.durable.tx_status(key)
    }

    /// `TxStatus` within an admitted scope's receipt namespace.
    pub fn tx_status_scoped(&self, key: &[u8], authority: ScopedTransact<'_>) -> Result<Receipt> {
        let identity = TxKey { namespace: Some(authority.receipt_namespace), key: key.to_vec() };
        if self.deciding.contains_key(&identity) { return Err(pending_error()); }
        self.durable.tx_status_scoped(key, authority)
    }

    /// Decide one request against the speculative index.
    pub fn decide(
        &mut self, request: &Transact, now: Timestamp, features: &[&str], claims: &dyn ClaimVerifier,
    ) -> Result<Decision> {
        self.decide_with(request, now, features, claims, None)
    }

    /// Decide one request in an admitted scope (see [`TransactState::resolve_scoped`]).
    pub fn decide_scoped(
        &mut self, request: &Transact, now: Timestamp, features: &[&str], claims: &dyn ClaimVerifier,
        authority: ScopedTransact<'_>,
    ) -> Result<Decision> {
        self.decide_with(request, now, features, claims, Some(authority))
    }

    fn decide_with(
        &mut self, request: &Transact, now: Timestamp, features: &[&str], claims: &dyn ClaimVerifier,
        authority: Option<ScopedTransact<'_>>,
    ) -> Result<Decision> {
        let identity = TxKey { namespace: request.receipt_namespace(authority.map(|authority| authority.receipt_namespace)), key: request.key.clone() };
        if self.deciding.contains_key(&identity) { return Err(pending_error()); }
        if request.ops.is_empty() && request.otherwise.is_empty() {
            // A validation answers at the durable prefix: never reveal an unacknowledged decision.
            let view = View {
                index: &self.durable.index,
                durable_index: &self.durable.index,
                receipts: &self.durable.receipts,
                pending: false,
            };
            let position = self.durable.index.head().basis.position();
            return match TransactState::resolve_in(view, request, position, now, features, claims, authority)?.0 {
                Resolution::Answer(receipt) => Ok(Decision::Answer(receipt)),
                Resolution::Append(_) => Err(order_error("A validation cannot append")),
            };
        }
        if self.in_flight.len() >= self.max_in_flight {
            return Err(Anomaly::new(AnomalyCategory::Busy, "rtdb/pipeline-full",
                "The writer has the maximum number of unacknowledged transactions"));
        }
        let position = self.next_position;
        let view = View {
            index: &self.speculative,
            durable_index: &self.durable.index,
            receipts: &self.durable.receipts,
            pending: false,
        };
        let (resolution, after) = TransactState::resolve_in(view, request, position, now, features, claims, authority)?;
        let prepared = match resolution {
            Resolution::Answer(receipt) => return Ok(Decision::Answer(receipt)),
            Resolution::Append(prepared) => prepared,
        };
        let after = after.ok_or_else(|| order_error("An append carries its resulting index"))?;
        let bytes = prepared.record.encode()?;
        let TransactionRecord::Tx(tx) = &prepared.record else { return Err(order_error("A decision prepares a Tx record")) };
        let timed = tx.before.is_some();
        let next = position.checked_add(if timed { 2 } else { 1 }).ok_or_else(exhausted)?;
        let entry = if timed {
            // Kept to derive the terminal record; the in-flight copy is folded on acknowledgement.
            let entry = Entry::TimedTx { record: TransactionRecord::Tx(tx.clone()) };
            self.timed.insert(position, (prepared, after.clone()));
            entry
        } else {
            let TransactionRecord::Tx(tx) = prepared.record else { unreachable!("checked above") };
            Entry::Untimed { after: after.clone(), receipt: tx.receipt, fingerprint: tx.fingerprint }
        };
        // A timed write's overlay assumes it settles at `position + 1`.
        self.speculative = after;
        self.next_position = next;
        self.deciding.insert(identity.clone(), position);
        self.in_flight.push_back(InFlight { position, key: identity, entry });
        Ok(Decision::Append { position, record: bytes })
    }

    /// Decide queued requests in order, as one group the driver may append in one journal
    /// request. Each result is independent; a full pipeline refuses the rest.
    pub fn decide_batch(
        &mut self, requests: &[Transact], now: Timestamp, features: &[&str], claims: &dyn ClaimVerifier,
    ) -> Vec<Result<Decision>> {
        requests.iter().map(|request| self.decide(request, now, features, claims)).collect()
    }

    /// For the timed write at `tx_position`, once its original record is durable under
    /// this writer and `clock_upper` bounds the writer's clock after that acknowledgement:
    /// its terminal record. See the module documentation for `Refuse`.
    pub fn terminal(&mut self, tx_position: u64, clock_upper: Timestamp) -> Result<Terminal> {
        let position = tx_position.checked_add(1).ok_or_else(exhausted)?;
        let (record, key, incarnation, system_time) = {
            let (prepared, _) = self.timed.get(&tx_position).ok_or_else(|| order_error("No timed write waits at that position"))?;
            if self.in_flight.iter().any(|entry| entry.position == tx_position + 1) {
                return Err(order_error("The timed write's terminal record is already in flight"));
            }
            if self.durable.pending.as_ref().is_none_or(|pending| pending.position != tx_position) {
                return Err(order_error("Acknowledge the timed write before its terminal record"));
            }
            let record = prepared.after_original_ack(clock_upper, position)
                .ok_or_else(|| order_error("Only a timed write has a terminal record"))?;
            let TransactionRecord::Tx(tx) = &prepared.record else { unreachable!("checked by after_original_ack") };
            let key = TxKey { namespace: tx.receipt_namespace, key: tx.receipt.key.clone() };
            (record, key, tx.receipt.basis.incarnation(), tx.receipt.system_time)
        };
        let refused = matches!(record, TransactionRecord::Refuse { .. });
        let mut redecide = Vec::new();
        if refused {
            // Later decisions assumed the effects; none passed the reserved slot.
            let kept = self.in_flight.iter().take_while(|entry| entry.position < position).count();
            for dropped in self.in_flight.drain(kept..) {
                self.deciding.remove(&dropped.key);
                redecide.push(dropped.key);
            }
            self.timed.retain(|at, _| *at <= tx_position);
            // `Refuse` publishes the original receipt's time at `position` with no effects.
            let mut index = self.durable.index.clone();
            index.advance(BranchCut { basis: Basis::new(incarnation, position), system_time })?;
            self.speculative = index;
            self.next_position = position + 1;
        } else {
            // After a withdrawal the slot may be the next free position; never reuse it.
            self.next_position = self.next_position.max(position + 1);
        }
        let bytes = record.encode()?;
        self.in_flight.insert(
            self.in_flight.iter().take_while(|entry| entry.position < position).count(),
            InFlight { position, key, entry: Entry::Terminal { record } },
        );
        Ok(Terminal { position, record: bytes, redecide })
    }

    /// The journal has made every position through `through` durable under this writer.
    /// Folds those decisions into the durable state and returns the receipts to release,
    /// in position order. A timed write's receipt is released with its terminal record.
    pub fn acknowledge(&mut self, through: u64) -> Result<Vec<Receipt>> {
        let mut released = Vec::new();
        while self.in_flight.front().is_some_and(|entry| entry.position <= through) {
            let InFlight { position, key, entry } = self.in_flight.pop_front().expect("checked front");
            match entry {
                Entry::Untimed { after, receipt, fingerprint } => {
                    if self.durable.pending.is_some() {
                        return Err(order_error("A timed write's terminal record must be durable before later records"));
                    }
                    // Adopt the decided index: identical to applying the record, without re-applying it.
                    self.durable.index = after;
                    self.durable.receipts.entry(key.namespace).or_default().insert(
                        key.key.clone(), StoredReceipt { receipt: receipt.clone(), fingerprint, receipt_namespace: key.namespace },
                    );
                    self.deciding.remove(&key);
                    released.push(receipt);
                }
                Entry::TimedTx { record } => {
                    if self.durable.apply(position, record)?.is_some() {
                        return Err(order_error("A timed write publishes only at its terminal record"));
                    }
                }
                Entry::Terminal { record } => {
                    self.timed.remove(&(position - 1));
                    let receipt = self.durable.apply(position, record)?
                        .ok_or_else(|| order_error("A terminal record publishes its receipt"))?;
                    self.deciding.remove(&key);
                    released.push(receipt);
                }
            }
        }
        if self.in_flight.is_empty() && self.timed.is_empty() { self.speculative = self.durable.index.clone(); }
        Ok(released)
    }

    /// Reserves the next position for a record that is not a transaction (`Lease`,
    /// `Checkpoint`, `Pin`, …), keeping the driver's appends in position order.
    pub fn reserve(&mut self) -> Result<u64> {
        let position = self.next_position;
        self.next_position = position.checked_add(1).ok_or_else(exhausted)?;
        Ok(position)
    }

    /// The append failed or this writer's epoch is in doubt. Drops every unacknowledged
    /// decision and returns their keys: those callers get "outcome unknown" and recover
    /// with `TxStatus`. Then fold any records the journal committed beyond the acknowledged
    /// prefix with [`Self::recover`], and continue with [`Self::resume`].
    pub fn withdraw(&mut self) -> Vec<TxKey> {
        self.deciding.clear();
        self.in_flight.drain(..).map(|entry| entry.key).collect()
    }

    /// After [`Self::withdraw`]: folds one record the journal committed at `position` beyond
    /// the acknowledged prefix, in position order. A returned receipt is final and may be
    /// released to a caller that is asking `TxStatus`.
    pub fn recover(&mut self, position: u64, record: &[u8]) -> Result<Option<Receipt>> {
        if !self.in_flight.is_empty() { return Err(order_error("Withdraw before recovering committed records")); }
        let record = TransactionRecord::decode(record)?;
        let terminal_of = match &record {
            TransactionRecord::Settle { tx_lsn } | TransactionRecord::Refuse { tx_lsn } => Some(*tx_lsn),
            TransactionRecord::Tx(_) => None,
        };
        let receipt = self.durable.apply(position, record)?;
        if let Some(tx_lsn) = terminal_of { self.timed.remove(&tx_lsn); }
        Ok(receipt)
    }

    /// After [`Self::withdraw`] and any [`Self::recover`]: decide again from the durable
    /// prefix, appending from `next_position` (after the journal's committed end). A timed
    /// write whose original record is durable keeps its reserved terminal slot: decisions
    /// resume after it, assuming it settles, and [`Self::terminal`] fills the slot.
    pub fn resume(&mut self, next_position: u64) -> Result<()> {
        if !self.in_flight.is_empty() { return Err(order_error("Withdraw before resuming")); }
        if next_position <= self.durable.index.head().basis.position() {
            return Err(order_error("The pipeline must resume after the durable prefix"));
        }
        self.next_position = next_position;
        let pending = self.durable.pending.as_ref().map(|pending| {
            (pending.position, TxKey { namespace: pending.tx.receipt_namespace, key: pending.tx.receipt.key.clone() })
        });
        self.timed.retain(|at, _| pending.as_ref().is_some_and(|(position, _)| position == at));
        match pending {
            Some((position, key)) => {
                let (_, settled) = self.timed.get(&position).ok_or_else(|| {
                    Anomaly::fault("rtdb/transaction-pending",
                        "A recovered timed write without this writer's decision must be refused; reopen the pipeline")
                })?;
                self.speculative = settled.clone();
                self.next_position = self.next_position.max(position + 2);
                self.deciding.insert(key, position);
            }
            None => self.speculative = self.durable.index.clone(),
        }
        Ok(())
    }
}

fn order_error(message: &str) -> Anomaly { Anomaly::fault("rtdb/transaction-order", message) }

fn exhausted() -> Anomaly { order_error("The journal position space is exhausted") }

#[cfg(test)]
mod tests {
    use rtdb_types::rtdb_v1::ClaimSealer;

    use super::*;

    const INCARNATION: [u8; 16] = [9; 16];
    const FEATURES: &[&str] = &[LINEARIZABLE, TIMED_TRANSACT, TRANSACT_REQUIRES];

    fn claims() -> ClaimSealer { ClaimSealer::new(INCARNATION, [4; 32]) }
    fn table() -> TableRef { TableRef::new("public", "items") }
    fn key(n: u8) -> Vec<u8> { vec![n; 16] }
    fn keys(keys: Vec<TxKey>) -> Vec<Vec<u8>> { keys.into_iter().map(|key| key.key).collect() }

    fn put(n: u8, id: i64, if_absent: bool) -> Transact {
        let row = Row { id: EntityId::Integer(id), valid: None, cells: StructFields::from([("v".into(), Cell::Value(Value::Utf8(format!("{n}"))))]), custody_group: None, protected_columns: Vec::new(), custody_payload: None };
        Transact {
            custody_request_digest: None,
            request_memory: None,
            key: key(n), require: Vec::new(), window: Window::default(), guards: Vec::new(),
            ops: vec![Op::Put { branch: MAIN_BRANCH.into(), table: table(), rows: vec![row], if_absent }],
            otherwise: Vec::new(),
            requires: Vec::new(),
        }
    }

    fn timed(n: u8, id: i64, before: i64) -> Transact {
        let mut request = put(n, id, false);
        request.window.before = Some(Timestamp(before));
        request
    }

    fn pipeline() -> TransactPipeline { TransactPipeline::new(TransactState::new(INCARNATION), 1, 8).unwrap() }

    fn append(pipeline: &mut TransactPipeline, request: &Transact, now: i64) -> (u64, Bytes) {
        match pipeline.decide(request, Timestamp(now), FEATURES, &claims()).unwrap() {
            Decision::Append { position, record } => (position, record),
            Decision::Answer(_) => panic!("expected an append"),
        }
    }

    fn outcome(record: &Bytes) -> Outcome {
        let TransactionRecord::Tx(tx) = TransactionRecord::decode(record).unwrap() else { panic!("expected a Tx") };
        tx.receipt.outcome
    }

    /// The durable state must equal a follower folding the same records in position order.
    fn assert_matches_follower(pipeline: &TransactPipeline, records: &[(u64, Bytes)]) {
        let mut follower = TransactState::new(INCARNATION);
        let mut records = records.to_vec();
        records.sort_by_key(|(position, _)| *position);
        for (position, record) in records {
            follower.apply(position, TransactionRecord::decode(&record).unwrap()).unwrap();
        }
        assert_eq!(pipeline.durable().encode_checkpoint().unwrap(), follower.encode_checkpoint().unwrap());
    }

    #[test]
    fn acknowledged_state_equals_sequential_apply() {
        let mut pipeline = pipeline();
        let mut records = Vec::new();
        for n in 1..=5u8 {
            records.push(append(&mut pipeline, &put(n, i64::from(n % 3), false), 10 + i64::from(n)));
        }
        assert_eq!(pipeline.in_flight(), 5);
        let first = pipeline.acknowledge(2).unwrap();
        let rest = pipeline.acknowledge(5).unwrap();
        let released = first.into_iter().chain(rest).map(|receipt| receipt.basis.position()).collect::<Vec<_>>();
        assert_eq!(released, vec![1, 2, 3, 4, 5]);
        assert_matches_follower(&pipeline, &records);
    }

    #[test]
    fn decisions_see_unacknowledged_earlier_decisions() {
        let mut pipeline = pipeline();
        append(&mut pipeline, &put(1, 7, true), 10);
        let (_, record) = append(&mut pipeline, &put(2, 7, true), 11);
        assert!(matches!(outcome(&record), Outcome::Refused { ref error } if error.code == "rtdb/already-present"));
    }

    #[test]
    fn requires_refusal_uses_the_speculative_prefix_and_replays() {
        let mut pipeline = pipeline();
        let original = append(&mut pipeline, &put(1, 7, false), 10);
        pipeline.acknowledge(original.0).unwrap();
        let replacement = append(&mut pipeline, &put(2, 7, false), 11);

        let mut stale = put(3, 8, false);
        stale.requires.push(Guard::Version {
            branch: MAIN_BRANCH.into(), table: table(), id: EntityId::Integer(7), valid_at: None,
            expect: Version::Written(Basis::new(INCARNATION, original.0)),
        });
        stale.guards.push(Guard::Unchanged(Claim::from_bytes(vec![0])));
        stale.otherwise = put(4, 9, false).ops;
        let refusal = append(&mut pipeline, &stale, 12);
        assert_eq!(pipeline.tx_status(&stale.key).unwrap_err().code, "rtdb/transaction-pending");

        let receipt = pipeline.acknowledge(refusal.0).unwrap().pop().unwrap();
        assert_eq!(receipt.outcome, Outcome::Refused {
            error: Anomaly::new(AnomalyCategory::Conflict, REQUIREMENT_FAILED, "Requirement 0 failed"),
        });
        let view = pipeline.durable().index().view(MAIN_BRANCH, At::Latest, Layer::Merged, Timestamp(20)).unwrap();
        assert_eq!(view.ids(&table()).into_iter().collect::<Vec<_>>(), [&EntityId::Integer(7)]);
        let replayed = Receipt { replayed: true, ..receipt };
        assert_eq!(pipeline.tx_status(&stale.key).unwrap(), replayed);
        let Decision::Answer(retry) = pipeline.decide(&stale, Timestamp(20), FEATURES, &claims()).unwrap()
            else { panic!("expected replay"); };
        assert_eq!(retry, replayed);
        stale.requires.clear();
        assert_eq!(pipeline.decide(&stale, Timestamp(20), FEATURES, &claims()).err().unwrap().code, "rtdb/idempotency-key");
        assert_matches_follower(&pipeline, &[original, replacement, refusal]);
    }

    #[test]
    fn requires_refusal_survives_a_missed_timed_cutoff() {
        let mut pipeline = pipeline();
        let mut request = timed(1, 1, 100);
        request.requires.push(Guard::Version {
            branch: MAIN_BRANCH.into(), table: table(), id: EntityId::Integer(2), valid_at: None,
            expect: Version::Present,
        });
        request.otherwise = put(2, 3, false).ops;
        let original = append(&mut pipeline, &request, 10);
        assert!(pipeline.acknowledge(original.0).unwrap().is_empty());
        let terminal = pipeline.terminal(original.0, Timestamp(100)).unwrap();
        assert!(matches!(TransactionRecord::decode(&terminal.record).unwrap(), TransactionRecord::Refuse { .. }));
        let receipt = pipeline.acknowledge(terminal.position).unwrap().pop().unwrap();
        assert!(matches!(&receipt.outcome, Outcome::Refused { error } if error.code == REQUIREMENT_FAILED));
        assert_eq!(pipeline.tx_status(&request.key).unwrap(), Receipt { replayed: true, ..receipt });
        assert_matches_follower(&pipeline, &[original, (terminal.position, terminal.record)]);
    }

    #[test]
    fn withdrawal_releases_nothing_and_allows_redeciding() {
        let mut pipeline = pipeline();
        let first = append(&mut pipeline, &put(1, 1, false), 10);
        append(&mut pipeline, &put(2, 2, false), 11);
        assert_eq!(pipeline.acknowledge(1).unwrap().len(), 1);
        assert_eq!(pipeline.tx_status(&key(2)).unwrap_err().code, "rtdb/transaction-pending");
        assert_eq!(keys(pipeline.withdraw()), vec![key(2)]);
        pipeline.resume(2).unwrap();
        assert_eq!(pipeline.tx_status(&key(2)).unwrap_err().code, "rtdb/unknown-transaction");
        let durable = pipeline.durable().index().view(MAIN_BRANCH, At::Latest, Layer::Merged, Timestamp(20)).unwrap();
        assert!(durable.row(&table(), &EntityId::Integer(2), Timestamp(20)).is_none());
        let again = append(&mut pipeline, &put(2, 2, false), 12);
        assert_eq!(again.0, 2);
        assert_eq!(pipeline.acknowledge(2).unwrap()[0].basis.position(), 2);
        assert_matches_follower(&pipeline, &[first, again]);
    }

    #[test]
    fn keys_in_flight_wait_and_acknowledged_keys_replay() {
        let mut pipeline = pipeline();
        append(&mut pipeline, &put(1, 1, false), 10);
        assert_eq!(pipeline.decide(&put(1, 1, false), Timestamp(11), FEATURES, &claims()).err().unwrap().code, "rtdb/transaction-pending");
        pipeline.acknowledge(1).unwrap();
        let Decision::Answer(receipt) = pipeline.decide(&put(1, 1, false), Timestamp(12), FEATURES, &claims()).unwrap() else { panic!() };
        assert!(receipt.replayed);
    }

    #[test]
    fn validation_reads_only_the_durable_prefix() {
        let mut pipeline = pipeline();
        append(&mut pipeline, &put(1, 1, false), 10);
        let mut validate = put(2, 1, false);
        validate.ops.clear();
        validate.guards = vec![Guard::Version { branch: MAIN_BRANCH.into(), table: table(), id: EntityId::Integer(1), valid_at: None, expect: Version::Absent }];
        let Decision::Answer(receipt) = pipeline.decide(&validate, Timestamp(11), FEATURES, &claims()).unwrap() else { panic!() };
        assert_eq!(receipt.outcome, Outcome::Then, "the unacknowledged put is not visible to a validation");
    }

    #[test]
    fn later_decisions_proceed_past_a_settling_timed_write() {
        let mut pipeline = pipeline();
        let original = append(&mut pipeline, &timed(1, 1, 100), 10);
        assert_eq!(original.0, 1);
        // The overlay assumes the timed write settles: an if_absent put on its row conflicts.
        let later = append(&mut pipeline, &put(2, 1, true), 11);
        assert_eq!(later.0, 3, "position 2 is reserved for the terminal record");
        assert!(matches!(outcome(&later.1), Outcome::Refused { .. }));
        assert!(pipeline.terminal(1, Timestamp(50)).is_err(), "the original record must be durable first");
        assert!(pipeline.acknowledge(1).unwrap().is_empty());
        let terminal = pipeline.terminal(1, Timestamp(50)).unwrap();
        assert_eq!(terminal.position, 2);
        assert!(terminal.redecide.is_empty());
        assert!(matches!(TransactionRecord::decode(&terminal.record).unwrap(), TransactionRecord::Settle { tx_lsn: 1 }));
        let released = pipeline.acknowledge(3).unwrap().into_iter().map(|receipt| receipt.basis.position()).collect::<Vec<_>>();
        assert_eq!(released, vec![2, 3]);
        assert_matches_follower(&pipeline, &[original, (terminal.position, terminal.record), later]);
    }

    #[test]
    fn a_refused_timed_write_hands_later_decisions_back() {
        let mut pipeline = pipeline();
        let original = append(&mut pipeline, &timed(1, 1, 20), 10);
        append(&mut pipeline, &put(2, 1, true), 11);
        pipeline.acknowledge(1).unwrap();
        let terminal = pipeline.terminal(1, Timestamp(25)).unwrap();
        assert!(matches!(TransactionRecord::decode(&terminal.record).unwrap(), TransactionRecord::Refuse { tx_lsn: 1 }));
        assert_eq!(keys(terminal.redecide), vec![key(2)]);
        assert_eq!(pipeline.acknowledge(2).unwrap()[0].basis.position(), 2);
        // Decided again without the refused write's row, the put now applies.
        let again = append(&mut pipeline, &put(2, 1, true), 30);
        assert_eq!(again.0, 3);
        assert_eq!(outcome(&again.1), Outcome::Then);
        pipeline.acknowledge(3).unwrap();
        assert_matches_follower(&pipeline, &[original, (terminal.position, terminal.record), again]);
    }

    #[test]
    fn withdrawal_after_a_durable_timed_write_keeps_its_terminal_slot() {
        let mut pipeline = pipeline();
        let original = append(&mut pipeline, &timed(1, 1, 100), 10);
        pipeline.acknowledge(1).unwrap();
        append(&mut pipeline, &put(2, 2, false), 11);
        // The journal's committed end is 1: the driver resumes at 2, the reserved slot.
        assert_eq!(keys(pipeline.withdraw()), vec![key(2)]);
        pipeline.resume(2).unwrap();
        // Decisions resume after the slot and still assume the timed write settles.
        let again = append(&mut pipeline, &put(2, 1, true), 12);
        assert_eq!(again.0, 3, "the terminal slot is never reused");
        assert!(matches!(outcome(&again.1), Outcome::Refused { .. }));
        let terminal = pipeline.terminal(1, Timestamp(50)).unwrap();
        assert_eq!(terminal.position, 2);
        pipeline.acknowledge(3).unwrap();
        assert_matches_follower(&pipeline, &[original, (terminal.position, terminal.record), again]);
    }

    #[test]
    fn records_committed_before_a_withdrawal_are_recovered_with_their_receipts() {
        let mut pipeline = pipeline();
        let first = append(&mut pipeline, &put(1, 1, false), 10);
        let second = append(&mut pipeline, &put(2, 2, false), 11);
        append(&mut pipeline, &put(3, 3, false), 12);
        // The reply for 1 and 2 was lost, but the journal committed both; 3 is unknown.
        assert_eq!(keys(pipeline.withdraw()), vec![key(1), key(2), key(3)]);
        assert!(pipeline.resume(1).is_ok(), "resuming before recovery is allowed");
        assert!(pipeline.withdraw().is_empty());
        assert_eq!(pipeline.recover(first.0, &first.1).unwrap().unwrap().key, key(1));
        assert_eq!(pipeline.recover(second.0, &second.1).unwrap().unwrap().key, key(2));
        assert!(pipeline.resume(2).is_err(), "resume after the committed end");
        pipeline.resume(3).unwrap();
        assert!(pipeline.tx_status(&key(2)).is_ok());
        let again = append(&mut pipeline, &put(3, 3, false), 14);
        assert_eq!(again.0, 3);
        pipeline.acknowledge(3).unwrap();
        assert_matches_follower(&pipeline, &[first, second, again]);
    }

    #[test]
    fn backpressure_bounds_the_unacknowledged_suffix() {
        let mut pipeline = TransactPipeline::new(TransactState::new(INCARNATION), 1, 2).unwrap();
        append(&mut pipeline, &put(1, 1, false), 10);
        append(&mut pipeline, &put(2, 2, false), 11);
        assert_eq!(pipeline.decide(&put(3, 3, false), Timestamp(12), FEATURES, &claims()).err().unwrap().code, "rtdb/pipeline-full");
        pipeline.acknowledge(1).unwrap();
        append(&mut pipeline, &put(3, 3, false), 13);
    }

    #[test]
    fn records_carry_the_admission_witness() {
        let mut pipeline = pipeline();
        let request = put(1, 1, false);
        let (_, record) = append(&mut pipeline, &request, 42);
        let TransactionRecord::Tx(tx) = TransactionRecord::decode(&record).unwrap() else { panic!() };
        let (identity, submitted_at, commitment) = tx.admission();
        assert_eq!(identity, request.key.as_slice());
        assert_eq!(submitted_at, Timestamp(42));
        assert_eq!(commitment, request.logical_sha256().unwrap());
        assert_eq!(tx.canonical_request().unwrap().get(), request.canonical_request().unwrap().get());
    }

    #[test]
    fn canonical_admission_survives_recovery_without_changing_retry_identity() {
        let mut pipeline = pipeline();
        let request = put(1, 1, false);
        let (position, record) = append(&mut pipeline, &request, 42);
        let TransactionRecord::Tx(tx) = TransactionRecord::decode(&record).unwrap() else { panic!() };
        assert_eq!(tx.admission(), (request.key.as_slice(), Timestamp(42), request.logical_sha256().unwrap()));
        pipeline.withdraw();
        pipeline.recover(position, &record).unwrap();
        pipeline.resume(position + 1).unwrap();
        let Decision::Answer(receipt) = pipeline.decide(&request, Timestamp(43), FEATURES, &claims()).unwrap() else {
            panic!("a retry should recover the admitted decision");
        };
        assert_eq!(receipt.basis.position(), position);
        assert!(receipt.replayed);
    }

    #[test]
    fn canonical_admission_sorts_named_cells_and_required_features() {
        let mut request = put(1, 1, false);
        request.require = vec![TRANSACT_REQUIRES.into(), LINEARIZABLE.into(), LINEARIZABLE.into()];
        let Op::Put { rows, .. } = &mut request.ops[0] else { panic!("expected a Put"); };
        rows[0].cells.insert("first".into(), Cell::Value(Value::I64(17)));

        let mut equivalent = request.clone();
        equivalent.require = vec![LINEARIZABLE.into(), TRANSACT_REQUIRES.into()];
        let Op::Put { rows, .. } = &mut equivalent.ops[0] else { panic!("expected a Put"); };
        rows[0].cells.sort_keys();

        let (_, first) = append(&mut pipeline(), &request, 42);
        let (_, second) = append(&mut pipeline(), &equivalent, 42);
        assert_eq!(first, second);
        let mut follower = TransactState::new(INCARNATION);
        follower.apply(1, TransactionRecord::decode(&first).unwrap()).unwrap();
        let checkpoint = follower.encode_checkpoint().unwrap();
        let follower = TransactState::decode_checkpoint(&checkpoint, follower.index().head()).unwrap();
        let receipt = follower.tx_status(&request.key).unwrap();
        assert!(matches!(follower.resolve(&equivalent, 2, Timestamp(99), FEATURES, &claims()).unwrap(),
            Resolution::Answer(replay) if replay == receipt));
    }

    #[test]
    fn canonical_admission_checks_the_request_key_and_replays_existing_v2_records() {
        let request = put(1, 1, false);
        let (_, record) = append(&mut pipeline(), &request, 42);
        let mut legacy: serde_json::Value = serde_json::from_slice(&record).unwrap();
        legacy[1]["Tx"]["request"]["key"] = serde_json::json!(key(2));
        let mismatched = TransactionRecord::decode(&serde_json::to_vec(&legacy).unwrap()).unwrap_err();
        assert_eq!(mismatched.code, "rtdb/invalid-resolved-transaction");

        let tx = legacy[1]["Tx"].as_object_mut().unwrap();
        tx.remove("request");
        let admitted_at = tx.remove("admitted_at").unwrap();
        tx.insert("submitted_at".into(), admitted_at);
        let record = TransactionRecord::decode(&serde_json::to_vec(&legacy).unwrap()).unwrap();
        assert!(record.encode().is_err());
        let mut follower = TransactState::new(INCARNATION);
        follower.apply(1, record).unwrap();
        assert!(matches!(follower.resolve(&request, 2, Timestamp(99), FEATURES, &claims()).unwrap(),
            Resolution::Answer(receipt) if receipt.replayed));
    }
}
