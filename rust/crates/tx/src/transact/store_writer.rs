use super::*;

pub(super) fn receipt_namespace() -> [u8; 32] {
    Sha256::digest(b"rtdb.store/writer-fence/receipt/v1").into()
}

impl Transact {
    pub fn set_store_writer(incarnation: [u8; 16], fence: StoreWriterFence) -> Self {
        let mut hash = Sha256::new();
        hash.update(b"rtdb.store/writer-fence/v1\0");
        hash.update(incarnation);
        hash.update(fence.epoch.to_be_bytes());
        if let Some(writer) = fence.writer { hash.update(writer); }
        Self {
            key: hash.finalize().to_vec(), require: Vec::new(), window: Window::default(),
            guards: vec![Guard::SetStoreWriter { incarnation, fence }],
            ops: vec![Op::Store(StoreAppend { writes: Vec::new(), bounds: Vec::new(), derives: Vec::new(), window: Window::default() })],
            otherwise: Vec::new(), requires: Vec::new(), custody_request_digest: None, request_memory: None,
        }
    }

    pub(super) fn store_writer_change(&self) -> Option<([u8; 16], StoreWriterFence)> {
        match self.guards.as_slice() {
            [Guard::SetStoreWriter { incarnation, fence }] => Some((*incarnation, *fence)),
            _ => None,
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn resolve_change(
    view: View<'_>, request: &Transact, incarnation: [u8; 16], fence: StoreWriterFence,
    position: u64, now: Timestamp, fingerprint: [u8; 32], canonical_request: CanonicalRequest,
) -> Result<(Resolution, Option<BranchIndex>)> {
    if view.pending { return Err(pending_error()); }
    let head = view.index.head();
    let namespace = Some(receipt_namespace());
    let receipt = |cut: BranchCut| Receipt {
        key: request.key.clone(), basis: cut.basis, system_time: cut.system_time, outcome: Outcome::Then,
        replayed: false, erasure: None, decision_clock: None,
    };
    if !view.index.check_store_writer_fence(incarnation, fence)? {
        let reply = view.receipts.get(&namespace).and_then(|receipts| receipts.get(&request.key))
            .map_or_else(|| receipt(head), |prior| replay(&prior.receipt));
        return Ok((Resolution::Answer(reply), None));
    }
    if position <= head.basis.position() || view.receipts.get(&namespace).is_some_and(|receipts| receipts.contains_key(&request.key)) {
        return Err(invalid_record());
    }
    let cut = BranchCut { basis: Basis::new(incarnation, position), system_time: now.max(head.system_time.next()?) };
    let effect = BranchEffect::StoreWriterFence { incarnation, fence };
    let mut after = view.index.clone();
    after.advance(cut)?;
    after.apply(effect.clone())?;
    let record = TransactionRecord::Tx(ResolvedTransact {
        fingerprint, receipt: receipt(cut), effects: vec![effect], before: None, receipt_namespace: namespace,
        admitted_at: now, workload: None, workload_digest: None, request: Some(canonical_request),
        timed_store: None,
    });
    Ok((Resolution::Append(PreparedAppend { position, record }), Some(after)))
}
