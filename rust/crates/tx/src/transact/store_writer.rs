use super::*;

pub(super) fn receipt_namespace() -> [u8; 32] {
    Sha256::digest(b"rtdb.store/writer-fence/receipt/v1").into()
}

impl Transact {
    pub fn set_store_writer(incarnation: [u8; 16], fence: StoreWriterFence) -> Self {
        WriterCommand { incarnation, fence, attempt: None }.transaction()
    }

    pub fn claim_store_writer(incarnation: [u8; 16], fence: StoreWriterFence, attempt: [u8; 16]) -> Self {
        WriterCommand { incarnation, fence, attempt: Some(attempt) }.transaction()
    }

    pub(super) fn store_writer_change(&self) -> Option<WriterCommand> {
        match self.guards.as_slice() {
            [Guard::SetStoreWriter { incarnation, fence }] =>
                Some(WriterCommand { incarnation: *incarnation, fence: *fence, attempt: None }),
            [Guard::ClaimStoreWriter { incarnation, fence, attempt }] =>
                Some(WriterCommand { incarnation: *incarnation, fence: *fence, attempt: Some(*attempt) }),
            _ => None,
        }
    }
}

pub(super) struct WriterCommand {
    incarnation: [u8; 16],
    fence: StoreWriterFence,
    attempt: Option<[u8; 16]>,
}

impl WriterCommand {
    pub(super) fn transaction(&self) -> Transact {
        let Self { incarnation, fence, attempt } = *self;
        let mut hash = Sha256::new();
        if attempt.is_some() {
            hash.update(b"rtdb.store/writer-claim/v1\0");
        } else {
            hash.update(b"rtdb.store/writer-fence/v1\0");
        }
        hash.update(incarnation);
        hash.update(fence.epoch.to_be_bytes());
        if let Some(writer) = fence.writer { hash.update(writer); }
        let guard = if let Some(attempt) = attempt {
            hash.update(attempt);
            Guard::ClaimStoreWriter { incarnation, fence, attempt }
        } else {
            Guard::SetStoreWriter { incarnation, fence }
        };
        Transact {
            key: hash.finalize().to_vec(), require: Vec::new(), window: Window::default(),
            guards: vec![guard],
            ops: vec![Op::Store(StoreAppend { writes: Vec::new(), bounds: Vec::new(), derives: Vec::new(), window: Window::default() })],
            otherwise: Vec::new(), requires: Vec::new(), custody_request_digest: None, request_memory: None,
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn resolve_change(
    view: View<'_>, request: &Transact, command: WriterCommand,
    position: u64, now: Timestamp, fingerprint: [u8; 32], canonical_request: CanonicalRequest,
) -> Result<(Resolution, Option<BranchIndex>)> {
    if view.pending { return Err(pending_error()); }
    let WriterCommand { incarnation, fence, attempt } = command;
    let head = view.index.head();
    let namespace = Some(receipt_namespace());
    let receipt = |cut: BranchCut| Receipt {
        key: request.key.clone(), basis: cut.basis, system_time: cut.system_time, outcome: Outcome::Then,
        replayed: false, erasure: None, decision_clock: None,
    };
    let prior = view.receipts.get(&namespace).and_then(|receipts| receipts.get(&request.key));
    let effect = if let Some(attempt) = attempt {
        if attempt == [0; 16] {
            return Err(Anomaly::incorrect("rtdb.store/writer-attempt", "A store writer claim needs a nonzero attempt identity"));
        }
        if let Some(prior) = prior {
            view.index.require_store_writer(Some((incarnation, fence, prior.receipt.basis.position(), attempt)))?;
            return Ok((Resolution::Answer(replay(&prior.receipt)), None));
        }
        view.index.check_store_writer_claim(incarnation, fence)?;
        BranchEffect::ClaimStoreWriter { incarnation, fence, attempt }
    } else {
        if !view.index.check_store_writer_fence(incarnation, fence)? {
            let reply = prior.map_or_else(|| receipt(head), |prior| replay(&prior.receipt));
            return Ok((Resolution::Answer(reply), None));
        }
        BranchEffect::StoreWriterFence { incarnation, fence }
    };
    if position <= head.basis.position() || prior.is_some() {
        return Err(invalid_record());
    }
    let cut = BranchCut { basis: Basis::new(incarnation, position), system_time: now.max(head.system_time.next()?) };
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
