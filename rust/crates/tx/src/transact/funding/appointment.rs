use super::*;

pub const FUNDED_APPOINTMENT: &str = "transact.funded-appointment";

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum AppointmentCommand {
    Prepare { predecessor: Basis, booking: [u8; 16], intent: [u8; 32] },
    Activate { booking: [u8; 16] },
    Consume { booking: [u8; 16], intent: [u8; 32] },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppointmentSample {
    observation: DecisionObservation,
    decision_before: Timestamp,
    journal_before: Timestamp,
    certificate: [u8; 32],
}

impl FundingInput {
    pub fn appointment_observed_at(self) -> Option<Timestamp> {
        match self {
            Self::Appointment { sample: Some(sample), .. } => Some(sample.observation.instant),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StageOrder {
    cut: BranchCut,
    fingerprint: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Appointment {
    intent: [u8; 32],
    address: [u8; 32],
    prepared: StageOrder,
    stage: AppointmentStage,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum AppointmentStage {
    Prepared,
    Activated(Activation),
    Consumed { activation: Activation, sample: AppointmentSample },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Activation {
    order: StageOrder,
    sample: AppointmentSample,
}

pub(in crate::transact) enum AppointmentDecision {
    Replay(Receipt),
    Prepare(FundedReceipt),
    Activate { appointment: FundedReceipt, sample: Timestamp },
    Consume { appointment: FundedReceipt, sample: Timestamp },
}

struct ReceiptPoint<'a> {
    table: &'a TableRef,
    id: &'a EntityId,
    at: Timestamp,
}

impl<'a> ReceiptPoint<'a> {
    fn decode(guard: &'a Guard) -> Option<Self> {
        match guard {
            Guard::Version { branch, table, id, valid_at: Some(at), expect: Version::Absent }
                if branch == MAIN_BRANCH => Some(Self { table, id, at: *at }),
            _ => None,
        }
    }

    fn address(&self) -> Result<[u8; 32]> {
        let coordinate = serde_json::to_vec(&(self.table, self.id, self.at)).map_err(|_| appointment_invalid())?;
        Ok(Sha256::digest(coordinate).into())
    }

    fn absent(&self, index: &BranchIndex) -> Result<()> {
        let view = index.view(MAIN_BRANCH, At::Latest, Layer::Merged, self.at)?;
        if view.row(self.table, self.id, self.at).is_some() {
            return Err(appointment_conflict("The Original receipt point already has an ordered result"));
        }
        Ok(())
    }

    fn check_ops(&self, ops: &[Op], consuming: bool) -> Result<()> {
        let mut writes = 0;
        for op in ops {
            match op {
                Op::Put { table, rows, if_absent, .. } if table == self.table => {
                    for row in rows.iter().filter(|row| &row.id == self.id) {
                        if !*if_absent { return Err(appointment_invalid()); }
                        writes += 1;
                    }
                }
                Op::Delete { table, ids, .. } if table == self.table && ids.contains(self.id) =>
                    return Err(appointment_invalid()),
                _ => {}
            }
        }
        if writes != usize::from(consuming) { return Err(appointment_invalid()); }
        Ok(())
    }

    fn check_effects(&self, effects: &[BranchEffect], consuming: bool) -> Result<()> {
        let mut writes = 0;
        for effect in effects {
            let BranchEffect::Write { branch, table, event } = effect else { return Err(appointment_invalid()); };
            if branch == MAIN_BRANCH && table == self.table && &event.id == self.id {
                if !event.valid.contains(self.at) || event.image_at(self.at).is_none() { return Err(appointment_invalid()); }
                writes += 1;
            }
        }
        if writes != usize::from(consuming) { return Err(appointment_invalid()); }
        Ok(())
    }
}

impl Transact {
    pub(in crate::transact) fn check_appointment_address(&self, point: &workload::Point) -> Result<()> {
        self.appointment_point(ReceiptPoint { table: &point.table, id: &point.id, at: point.valid_at }.address()?)?;
        Ok(())
    }

    pub fn prepare_funded_appointment(key: Vec<u8>, absent: Guard, before: Timestamp) -> Result<Self> {
        validate_key(&key)?;
        if ReceiptPoint::decode(&absent).is_none() || before.0 <= 0 { return Err(appointment_invalid()); }
        Ok(Self {
            key, require: vec![FUNDED_APPOINTMENT.into()], window: Window { not_before: None, before: Some(before) },
            guards: Vec::new(), ops: Vec::new(), otherwise: Vec::new(), requires: vec![absent],
            custody_request_digest: None, request_memory: None,
        })
    }

    fn appointment_point(&self, address: [u8; 32]) -> Result<ReceiptPoint<'_>> {
        let mut point = None;
        for candidate in self.requires.iter().filter_map(ReceiptPoint::decode) {
            if candidate.address()? == address {
                if point.is_some() { return Err(appointment_invalid()); }
                point = Some(candidate);
            }
        }
        point.ok_or_else(appointment_invalid)
    }

    fn check_activation_fences(&self, activation: Basis) -> Result<()> {
        let controls = self.requires.iter().filter_map(|guard| match guard {
            Guard::Version { branch, table, id, valid_at: Some(_), expect: Version::Written(basis) }
                if branch == MAIN_BRANCH && *basis == activation => Some((table, id)),
            _ => None,
        }).collect::<BTreeSet<_>>();
        if controls.len() < 2 { return Err(appointment_invalid()); }
        Ok(())
    }

    fn check_activation_writes(&self, effects: &[BranchEffect]) -> Result<()> {
        let controls = effects.iter().filter_map(|effect| {
            let BranchEffect::Write { branch, table, event } = effect else { return None; };
            let guarded = self.requires.iter().any(|guard| matches!(guard,
                Guard::Version { branch: target, table: relation, id, valid_at: Some(at),
                    expect: Version::Absent | Version::Written(_) }
                    if target == branch && relation == table && id == &event.id
                        && event.valid.contains(*at) && event.image_at(*at).is_some()
            ));
            guarded.then_some((table, &event.id))
        }).collect::<BTreeSet<_>>();
        if controls.len() < 2 { return Err(appointment_invalid()); }
        Ok(())
    }

    pub fn check_appointment_shape(&self, command: AppointmentCommand) -> Result<()> {
        if !self.require.iter().any(|feature| feature == FUNDED_APPOINTMENT)
            || self.require.iter().any(|feature| feature == DECISION_CLOCK_OFFER)
            || self.custody_request_digest.is_some()
            || self.window.not_before.zip(self.window.before).is_some_and(|(start, end)| start >= end)
            || self.ops.iter().chain(&self.otherwise).any(|op| {
                !matches!(op, Op::Put { branch, .. } | Op::Delete { branch, .. } if branch == MAIN_BRANCH)
                    || op.uses_custody()
            })
        {
            return Err(appointment_invalid());
        }
        match command {
            AppointmentCommand::Prepare { .. }
                if self.ops.is_empty() && self.otherwise.is_empty() && self.guards.is_empty()
                    && self.require.len() == 1 && self.window.not_before.is_none()
                    && self.window.before.is_some_and(|before| before.0 > 0)
                    && matches!(self.requires.as_slice(), [guard] if ReceiptPoint::decode(guard).is_some()) =>
                Ok(()),
            AppointmentCommand::Activate { .. }
                if !self.ops.is_empty() && self.otherwise.is_empty() && self.requires.len() > 1 =>
                Ok(()),
            AppointmentCommand::Consume { .. } if !self.ops.is_empty() => Ok(()),
            _ => Err(appointment_invalid()),
        }
    }
}

impl AppointmentCommand {
    fn booking(self) -> [u8; 16] {
        match self {
            Self::Prepare { booking, .. } | Self::Activate { booking } | Self::Consume { booking, .. } => booking,
        }
    }

    fn admits(self, owner: [u8; 32], offer: FundedReceipt, state: Appointment) -> bool {
        offer.belongs_to(owner) && match self {
            Self::Prepare { predecessor, intent, .. } => offer.predecessor == predecessor && state.intent == intent,
            Self::Activate { booking } => booking == offer.booking,
            Self::Consume { booking, intent } => booking == offer.booking && intent == state.intent,
        }
    }

    pub(super) async fn verify(
        self, owner: [u8; 32], request: &Transact, state: &TransactState, clock: &dyn DecisionClock,
        managed_until: Option<Timestamp>,
    ) -> Option<AppointmentSample> {
        if request.check_appointment_shape(self).is_err() { return None; }
        let namespace = managed_until.map(|_| owner);
        let previous = state.receipts.get(&namespace).and_then(|receipts| receipts.get(&request.key));
        match self {
            Self::Prepare { predecessor, .. } if previous.is_none() => {
                let original = state.index().exact(predecessor).ok()?.basis;
                let observation = clock.observe().await.ok()?;
                let reading = clock_slot_reading(observation.instant)?;
                let end = Timestamp(reading.0.checked_add(1_000_000)?);
                let before = request.window.before?.min(end).min(managed_until.unwrap_or(Timestamp::MAX));
                authorize(clock, original, observation, before).await
            }
            Self::Activate { .. } | Self::Consume { .. } => {
                let offer = previous?.receipt.decision_clock?;
                let Phase::Appointment(state) = offer.phase else { return None; };
                if !self.admits(owner, offer, state) { return None; }
                let before = match (self, state.stage) {
                    (Self::Activate { .. }, AppointmentStage::Prepared) => offer.reading().min(offer.before()),
                    (Self::Consume { .. }, AppointmentStage::Activated(_)) => offer.clock.journal_before,
                    _ => return None,
                };
                let observation = clock.observe().await.ok()?;
                if !observation.covers(offer.clock.issued)
                    || clock.funded_certificate(offer.original, observation) != Some(offer.clock.certificate)
                {
                    return None;
                }
                if matches!(self, Self::Consume { .. }) && managed_until.is_none_or(|until| until >= before) && (
                    request.window.before.is_none_or(|limit| limit >= before)
                        || observation.instant >= offer.before().min(request.window.before.unwrap_or(before))
                ) {
                    return Some(AppointmentSample {
                        observation, decision_before: offer.before(),
                        journal_before: before, certificate: offer.clock.certificate,
                    });
                }
                let bound = before.min(request.window.before.unwrap_or(Timestamp::MAX))
                    .min(managed_until.unwrap_or(Timestamp::MAX));
                authorize(clock, offer.original, observation, bound).await
            }
            _ => None,
        }
    }
}

async fn authorize(
    clock: &dyn DecisionClock, predecessor: Basis, observation: DecisionObservation, before: Timestamp,
) -> Option<AppointmentSample> {
    let cutoff = clock.authorize_offer(predecessor, observation, before).await.ok()?;
    if clock.funded_certificate(predecessor, observation) != Some(cutoff.journal.certificate_sha256())
        || cutoff.decision_before <= observation.instant || cutoff.journal.before() < cutoff.decision_before
        || cutoff.journal.before() > before
    {
        return None;
    }
    Some(AppointmentSample {
        observation, decision_before: cutoff.decision_before, journal_before: cutoff.journal.before(),
        certificate: cutoff.journal.certificate_sha256(),
    })
}

impl AppointmentSample {
    fn covers(self, offer: FundedReceipt) -> bool {
        DecisionObservation::decode(&self.observation.encode()).is_ok()
            && self.observation.covers(offer.clock.issued)
            && self.certificate == offer.clock.certificate
    }

    fn valid(self, offer: FundedReceipt, latest_decision: Timestamp, latest_journal: Timestamp) -> bool {
        self.covers(offer) && self.observation.instant < self.decision_before
            && self.decision_before <= latest_decision && self.decision_before <= self.journal_before
            && self.journal_before <= latest_journal
    }

    fn journal_guard(self) -> Result<(Timestamp, Timestamp, TimedJournalCutoff)> {
        Ok((self.observation.instant, self.decision_before, TimedJournalCutoff::new(self.journal_before, self.certificate)?))
    }
}

impl FundedReceipt {
    pub fn is_appointment(self) -> bool { matches!(self.phase, Phase::Appointment(_)) }

    pub(in crate::transact) fn check_managed_appointment(self, scope: &workload::FundedAppointmentScope) -> Result<()> {
        let Phase::Appointment(appointment) = self.phase else { return Err(unknown_receipt()); };
        let receipt = &scope.receipt;
        let address = ReceiptPoint { table: &receipt.table, id: &receipt.id, at: receipt.valid_at }.address()?;
        if appointment.address != address || appointment.intent != scope.intent_sha256 { return Err(unknown_receipt()); }
        Ok(())
    }

    pub(super) fn is_pending_appointment(self) -> bool {
        matches!(self.phase, Phase::Appointment(Appointment { stage: AppointmentStage::Prepared | AppointmentStage::Activated(_), .. }))
    }

    pub fn appointment_cut(self) -> Option<BranchCut> {
        let Phase::Appointment(state) = self.phase else { return None; };
        Some(match state.stage {
            AppointmentStage::Prepared => state.prepared.cut,
            AppointmentStage::Activated(activation) | AppointmentStage::Consumed { activation, .. } => activation.order.cut,
        })
    }

    pub fn appointment_journal_cutoff(self) -> Option<TimedJournalCutoff> {
        self.is_appointment().then(|| TimedJournalCutoff::new(self.clock.journal_before, self.clock.certificate).ok()).flatten()
    }

    pub(in crate::transact) fn appointment_address(self) -> Option<[u8; 32]> {
        match self.phase { Phase::Appointment(state) => Some(state.address), _ => None }
    }

    pub(in crate::transact) fn appointment_guard(self) -> Result<Option<(Timestamp, Timestamp, TimedJournalCutoff)>> {
        let Phase::Appointment(state) = self.phase else { return Err(invalid_record()); };
        match state.stage {
            AppointmentStage::Prepared => Ok(None),
            AppointmentStage::Activated(activation) => activation.sample.journal_guard().map(Some),
            AppointmentStage::Consumed { sample, .. } => sample.journal_guard().map(Some),
        }
    }

    pub(in crate::transact) fn appointment_matches_previous(self, previous: Option<FundedReceipt>) -> bool {
        let Phase::Appointment(state) = self.phase else { return false; };
        let phase = match state.stage {
            AppointmentStage::Prepared => return previous.is_none(),
            AppointmentStage::Activated(_) => AppointmentStage::Prepared,
            AppointmentStage::Consumed { activation, .. } => AppointmentStage::Activated(activation),
        };
        previous == Some(Self { phase: Phase::Appointment(Appointment { stage: phase, ..state }), ..self })
    }

    pub(in crate::transact) fn check_appointment_admission(self, tx: &ResolvedTransact, request: &Transact) -> Result<()> {
        let Phase::Appointment(state) = self.phase else { return Err(invalid_record()); };
        let receipt = &tx.receipt;
        let cut = BranchCut { basis: receipt.basis, system_time: receipt.system_time };
        let current = StageOrder { cut, fingerprint: tx.fingerprint };
        let (command, order) = match state.stage {
            AppointmentStage::Prepared => (AppointmentCommand::Prepare {
                predecessor: self.predecessor, booking: self.booking, intent: state.intent,
            }, state.prepared),
            AppointmentStage::Activated(activation) => (AppointmentCommand::Activate { booking: self.booking }, activation.order),
            AppointmentStage::Consumed { .. } => (AppointmentCommand::Consume { booking: self.booking, intent: state.intent }, current),
        };
        if tx.receipt_namespace.is_some_and(|namespace| namespace != self.owner)
            || tx.before.is_some() || tx.workload.is_some() || receipt.erasure.is_some()
            || receipt.key != request.key || request.logical_sha256()? != tx.fingerprint || order != current
            || !self.appointment_valid(cut) || matches!(receipt.outcome, Outcome::Refused { .. })
            || tx.receipt_namespace.is_some() && !self.managed_appointment_time_valid(cut)
        {
            return Err(invalid_record());
        }
        request.check_appointment_shape(command).map_err(|_| invalid_record())?;
        let point = request.appointment_point(state.address).map_err(|_| invalid_record())?;
        match state.stage {
            AppointmentStage::Prepared if receipt.outcome == Outcome::Then && tx.effects.is_empty()
                && request.window.before.is_some_and(|before| self.clock.journal_before <= before) => Ok(()),
            AppointmentStage::Activated(activation) if receipt.outcome == Outcome::Then && !tx.effects.is_empty()
                && request.window.before.is_none_or(|before| activation.sample.journal_before <= before)
                && request.window.not_before.is_none_or(|start| activation.sample.observation.instant >= start) => {
                point.check_ops(&request.ops, false)
                    .and_then(|()| point.check_effects(&tx.effects, false))
                    .and_then(|()| request.check_activation_writes(&tx.effects))
                    .map_err(|_| invalid_record())
            }
            AppointmentStage::Consumed { activation, sample } if request.window.before.is_none_or(|before| sample.journal_before <= before)
                && request.window.not_before.is_none_or(|start| sample.observation.instant >= start) => {
                request.check_activation_fences(activation.order.cut.basis).map_err(|_| invalid_record())?;
                let ops = match receipt.outcome {
                    Outcome::Then => &request.ops,
                    Outcome::Otherwise { .. } => &request.otherwise,
                    Outcome::Refused { .. } => return Err(invalid_record()),
                };
                point.check_ops(ops, true).and_then(|()| point.check_effects(&tx.effects, true)).map_err(|_| invalid_record())
            }
            _ => Err(invalid_record()),
        }
    }

    pub(in crate::transact) fn appointment_valid(self, cut: BranchCut) -> bool {
        let Phase::Appointment(state) = self.phase else { return false; };
        let original = self.original;
        let clock = self.clock;
        let ordered = |left: Basis, right: Basis| left.incarnation() == right.incarnation() && left < right;
        let prepared = state.prepared.cut;
        if self.booking == [0; 16] || self.owner == [0; 32] || state.intent == [0; 32] || state.address == [0; 32]
            || original != self.predecessor || !ordered(original, prepared.basis)
            || DecisionObservation::decode(&clock.issued.encode()).is_err()
            || clock_slot_reading(clock.issued.instant) != Some(clock.reading)
            || clock.reading >= clock.before || clock.before > clock.journal_before
            || clock.reading.0.checked_add(1_000_000).is_none_or(|end| clock.journal_before.0 > end)
            || TimedJournalCutoff::new(clock.journal_before, clock.certificate).is_err()
        {
            return false;
        }
        let activation_valid = |activation: Activation| {
            ordered(prepared.basis, activation.order.cut.basis)
                && prepared.system_time <= activation.order.cut.system_time
                && activation.sample.valid(self, clock.reading, clock.reading)
        };
        match state.stage {
            AppointmentStage::Prepared => cut == prepared,
            AppointmentStage::Activated(activation) => cut == activation.order.cut && activation_valid(activation),
            AppointmentStage::Consumed { activation, sample } => activation_valid(activation)
                && ordered(activation.order.cut.basis, cut.basis)
                && activation.order.cut.system_time <= cut.system_time
                && sample.observation.covers(activation.sample.observation)
                && sample.observation.instant >= clock.reading
                && sample.valid(self, clock.before, clock.journal_before),
        }
    }

    pub(in crate::transact) fn managed_appointment_time_valid(self, cut: BranchCut) -> bool {
        let Phase::Appointment(state) = self.phase else { return false; };
        let reading = self.reading();
        if !(self.clock.issued.instant..reading).contains(&state.prepared.cut.system_time) { return false; }
        let activation = match state.stage {
            AppointmentStage::Prepared => return true,
            AppointmentStage::Activated(activation) | AppointmentStage::Consumed { activation, .. } => activation,
        };
        if !(activation.sample.observation.instant..activation.sample.decision_before).contains(&activation.order.cut.system_time) {
            return false;
        }
        match state.stage {
            AppointmentStage::Consumed { sample, .. } =>
                (sample.observation.instant..sample.decision_before).contains(&cut.system_time),
            _ => true,
        }
    }

    fn replay_appointment(self, key: &[u8], order: StageOrder, stage: AppointmentStage) -> Receipt {
        let Phase::Appointment(state) = self.phase else { unreachable!("appointment phase") };
        Receipt {
            key: key.to_vec(), basis: order.cut.basis, system_time: order.cut.system_time,
            outcome: Outcome::Then, replayed: true, erasure: None,
            decision_clock: Some(Self { phase: Phase::Appointment(Appointment { stage, ..state }), ..self }),
        }
    }

    pub(in crate::transact) fn appointment_checkpoint(self, stored: &StoredReceipt, cut: Basis) -> Option<StoredReceipt> {
        let Phase::Appointment(state) = self.phase else { return None; };
        let activation = match state.stage {
            AppointmentStage::Prepared => None,
            AppointmentStage::Activated(activation) | AppointmentStage::Consumed { activation, .. } => Some(activation),
        };
        let (order, stage) = match activation.filter(|activation| activation.order.cut.basis <= cut) {
            Some(activation) => (activation.order, AppointmentStage::Activated(activation)),
            None if state.prepared.cut.basis <= cut => (state.prepared, AppointmentStage::Prepared),
            None => return None,
        };
        let mut receipt = self.replay_appointment(&stored.receipt.key, order, stage);
        receipt.replayed = false;
        Some(StoredReceipt { receipt, fingerprint: order.fingerprint, installed: None, ..stored.clone() })
    }
}

pub(in crate::transact) fn resolve_appointment(
    view: &View<'_>, request: &Transact, owner: [u8; 32], command: AppointmentCommand,
    sample: Option<AppointmentSample>, previous: Option<&StoredReceipt>, cut: BranchCut, fingerprint: [u8; 32],
) -> Result<AppointmentDecision> {
    request.check_appointment_shape(command)?;
    if owner == [0; 32] || command.booking() == [0; 16] { return Err(appointment_invalid()); }
    let order = StageOrder { cut, fingerprint };
    if let Some(previous) = previous {
        let offer = previous.receipt.decision_clock.filter(|offer| offer.is_appointment()).ok_or_else(conflicting_key)?;
        let Phase::Appointment(state) = offer.phase else { return Err(invalid_record()); };
        if !command.admits(owner, offer, state) { return Err(conflicting_key()); }
        let replay_stage = |order: StageOrder, stage| {
            if order.fingerprint != fingerprint { return Err(conflicting_key()); }
            Ok(AppointmentDecision::Replay(offer.replay_appointment(&request.key, order, stage)))
        };
        match (command, state.stage) {
            (AppointmentCommand::Prepare { .. }, _) =>
                replay_stage(state.prepared, AppointmentStage::Prepared),
            (AppointmentCommand::Activate { .. }, AppointmentStage::Activated(activation) | AppointmentStage::Consumed { activation, .. }) =>
                replay_stage(activation.order, AppointmentStage::Activated(activation)),
            (AppointmentCommand::Consume { .. }, AppointmentStage::Consumed { .. }) => {
                if previous.fingerprint != fingerprint { return Err(conflicting_key()); }
                Ok(AppointmentDecision::Replay(replay(&previous.receipt)))
            }
            (AppointmentCommand::Activate { .. }, AppointmentStage::Prepared) => {
                let point = request.appointment_point(state.address)?;
                point.absent(view.index)?;
                point.check_ops(&request.ops, false)?;
                let sample = sample.ok_or_else(appointment_unavailable)?;
                let start = request.window.not_before.unwrap_or(Timestamp::MIN);
                if sample.observation.instant < start { return Err(window_pending()); }
                if !sample.valid(offer, offer.reading(), offer.reading()) {
                    return Err(appointment_unavailable());
                }
                let activation = Activation { order, sample };
                let appointment = FundedReceipt {
                    phase: Phase::Appointment(Appointment { stage: AppointmentStage::Activated(activation), ..state }),
                    ..offer
                };
                Ok(AppointmentDecision::Activate { appointment, sample: sample.observation.instant })
            }
            (AppointmentCommand::Consume { .. }, AppointmentStage::Activated(activation)) => {
                request.check_activation_fences(activation.order.cut.basis)?;
                let point = request.appointment_point(state.address)?;
                point.absent(view.index)?;
                point.check_ops(&request.ops, true)?;
                if !request.otherwise.is_empty() { point.check_ops(&request.otherwise, true)?; }
                let sample = sample.ok_or_else(appointment_unavailable)?;
                let start = request.window.not_before.unwrap_or(offer.reading()).max(offer.reading());
                if sample.observation.instant < start { return Err(window_pending()); }
                let decision_before = offer.before().min(sample.decision_before)
                    .min(request.window.before.unwrap_or(Timestamp::MAX));
                if sample.observation.instant >= decision_before {
                    return Err(appointment_conflict("The funded window is closed; resolve the Original receipt point"));
                }
                if !sample.valid(offer, offer.before(), offer.clock.journal_before)
                    || !sample.observation.covers(activation.sample.observation)
                {
                    return Err(appointment_unavailable());
                }
                let appointment = FundedReceipt {
                    phase: Phase::Appointment(Appointment { stage: AppointmentStage::Consumed { activation, sample }, ..state }),
                    ..offer
                };
                Ok(AppointmentDecision::Consume { appointment, sample: sample.observation.instant })
            }
            (AppointmentCommand::Consume { .. }, AppointmentStage::Prepared) =>
                Err(appointment_conflict("The appointment has no ordered activation")),
        }
    } else {
        let AppointmentCommand::Prepare { predecessor, booking, intent } = command else { return Err(appointment_invalid()); };
        if intent == [0; 32] { return Err(appointment_invalid()); }
        let original = view.durable_index.exact(predecessor)?.basis;
        let point = ReceiptPoint::decode(&request.requires[0]).ok_or_else(appointment_invalid)?;
        let address = point.address()?;
        if view.appointment_addresses.contains(&address) {
            return Err(appointment_conflict("The Original receipt point already has a retained appointment"));
        }
        point.absent(view.index)?;
        let sample = sample.ok_or_else(appointment_unavailable)?;
        let reading = clock_slot_reading(sample.observation.instant).ok_or_else(appointment_invalid)?;
        let clock = OfferClock {
            issued: sample.observation, reading, before: sample.decision_before,
            journal_before: sample.journal_before, certificate: sample.certificate,
        };
        let state = Appointment { intent, address, prepared: order, stage: AppointmentStage::Prepared };
        let offer = FundedReceipt { booking, owner, predecessor, original, clock, phase: Phase::Appointment(state) };
        if !offer.appointment_valid(cut) { return Err(appointment_unavailable()); }
        Ok(AppointmentDecision::Prepare(offer))
    }
}

pub(in crate::transact) fn check_appointment_effects(
    request: &Transact, offer: FundedReceipt, outcome: &Outcome, effects: &[BranchEffect],
) -> Result<()> {
    let Phase::Appointment(state) = offer.phase else { return Err(invalid_record()); };
    match state.stage {
        AppointmentStage::Prepared => Ok(()),
        AppointmentStage::Activated(_) if *outcome == Outcome::Then && !effects.is_empty() => {
            request.appointment_point(state.address)?.check_effects(effects, false)?;
            request.check_activation_writes(effects)
        }
        AppointmentStage::Activated(_) => Err(appointment_conflict("The activation guards did not select the writes")),
        AppointmentStage::Consumed { .. } =>
            request.appointment_point(state.address)?.check_effects(effects, true),
    }
}

fn window_pending() -> Anomaly {
    Anomaly::new(AnomalyCategory::Busy, "rtdb/window-pending", "The funded window has not opened")
}

pub fn appointment_invalid() -> Anomaly {
    Anomaly::incorrect("rtdb/funded-appointment", "The funded appointment requires its exact Original receipt and command")
}

pub fn appointment_unavailable() -> Anomaly {
    Anomaly::unsupported("rtdb/funded-appointment", "The route has no eligible certified funded appointment")
}

fn appointment_conflict(message: &str) -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb/funded-appointment", message)
}

#[cfg(test)]
mod tests;
