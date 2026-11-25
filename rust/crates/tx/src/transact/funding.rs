use super::*;
use crate::{DecisionClock, DecisionObservation, clock_slot_reading};
use rtdb_types::TimedJournalCutoff;

pub(super) mod appointment;
use appointment::{Appointment, AppointmentCommand, AppointmentSample};

pub const DECISION_CLOCK_OFFER: &str = "transact.decision-clock-offer";

#[derive(Clone, Copy, Debug)]
pub enum FundingCommand {
    Book { predecessor: Basis, booking: [u8; 16] },
    Consume { booking: [u8; 16] },
    Appointment(AppointmentCommand),
}

impl FundingCommand {
    pub fn is_appointment(self) -> bool { matches!(self, Self::Appointment(_)) }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum FundingInput {
    Book {
        owner: [u8; 32],
        predecessor: Basis,
        issued: Option<OfferClock>,
        booking: [u8; 16],
    },
    Consume {
        owner: [u8; 32],
        booking: [u8; 16],
        observed: Option<DecisionObservation>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        certificate: Option<[u8; 32]>,
    },
    Appointment {
        owner: [u8; 32],
        command: AppointmentCommand,
        sample: Option<AppointmentSample>,
        managed_until: Option<Timestamp>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OfferClock {
    issued: DecisionObservation,
    reading: Timestamp,
    before: Timestamp,
    journal_before: Timestamp,
    certificate: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FundedReceipt {
    booking: [u8; 16],
    owner: [u8; 32],
    predecessor: Basis,
    original: Basis,
    clock: OfferClock,
    phase: Phase,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Phase {
    Booked,
    Terminal { observed: DecisionObservation },
    Appointment(Appointment),
}

impl FundingInput {
    pub fn new(command: FundingCommand, owner: [u8; 32]) -> Self {
        match command {
            FundingCommand::Book { predecessor, booking } => Self::Book { owner, predecessor, booking, issued: None },
            FundingCommand::Consume { booking } => Self::Consume { owner, booking, observed: None, certificate: None },
            FundingCommand::Appointment(command) => Self::Appointment { owner, command, sample: None, managed_until: None },
        }
    }

    pub fn managed_appointment(command: AppointmentCommand, context: &workload::ManagedAppointment) -> Self {
        Self::Appointment {
            owner: context.namespace(), command, sample: None, managed_until: Some(context.expires_at()),
        }
    }

    pub(super) fn managed_appointment_namespace(self) -> Option<[u8; 32]> {
        match self {
            Self::Appointment { owner, managed_until: Some(_), .. } => Some(owner),
            _ => None,
        }
    }

    pub fn is_appointment(self) -> bool { matches!(self, Self::Appointment { .. }) }

    pub async fn verify(
        &mut self, request: &Transact, state: &TransactState, scope: Option<ScopedTransact<'_>>, clock: &dyn DecisionClock,
    ) {
        let namespace = scope.map(|scope| scope.receipt_namespace);
        let stored = state.receipts.get(&namespace).and_then(|receipts| receipts.get(&request.key));
        let original = stored.and_then(|stored| stored.receipt.decision_clock)
            .filter(|offer| offer.matches(*self) && offer.booked());
        match self {
            Self::Book { predecessor, issued, .. } => {
                *issued = None;
                if stored.is_none() && let Ok(original) = original_predecessor(state.index(), *predecessor, scope) {
                    *issued = request.offer_clock(original, clock).await.ok();
                }
            }
            Self::Consume { observed, certificate, .. } => {
                *observed = None;
                *certificate = None;
                if let Some(offer) = original && let Ok(sample) = clock.observe().await {
                    *observed = Some(sample);
                    *certificate = clock.funded_certificate(offer.original, sample);
                }
            }
            Self::Appointment { owner, command, sample, managed_until } => {
                *sample = if scope.is_none() { command.verify(*owner, request, state, clock, *managed_until).await } else { None };
            }
        }
    }
}

impl Transact {
    pub fn check_funded_shape(&self, scoped: bool) -> Result<()> {
        let validation = self.ops.is_empty() && self.otherwise.is_empty();
        if scoped != validation || !self.require.iter().any(|feature| feature == DECISION_CLOCK_OFFER)
            || self.custody_request_digest.is_some() || self.uses_custody()
            || self.uses_erasure() || self.contains_native_custody_transfer()
            || self.ops.iter().chain(&self.otherwise).any(|op| matches!(op, Op::ScopedHold(_)))
        {
            return Err(funding_invalid());
        }
        Ok(())
    }

    async fn offer_clock(&self, predecessor: Basis, clock: &dyn DecisionClock) -> Result<OfferClock> {
        let issued = clock.observe().await?;
        let reading = clock_slot_reading(issued.instant).ok_or_else(funding_invalid)?;
        let end = Timestamp(reading.0.checked_add(1_000_000).ok_or_else(funding_invalid)?);
        let before = self.window.before.map_or(end, |before| before.min(end));
        let lower = self.window.not_before.map_or(reading, |start| start.max(reading));
        let cutoff = clock.authorize_offer(predecessor, issued, before).await?;
        if clock.funded_certificate(predecessor, issued) != Some(cutoff.journal.certificate_sha256())
            || cutoff.decision_before <= lower || cutoff.decision_before > before
            || cutoff.journal.before() < cutoff.decision_before || cutoff.journal.before() > before
        {
            return Err(funding_invalid());
        }
        Ok(OfferClock {
            issued, reading, before: cutoff.decision_before,
            journal_before: cutoff.journal.before(), certificate: cutoff.journal.certificate_sha256(),
        })
    }
}

impl FundedReceipt {
    pub fn booking(&self) -> [u8; 16] { self.booking }
    pub fn predecessor(&self) -> Basis { self.predecessor }
    pub fn reading(&self) -> Timestamp { self.clock.reading }
    pub fn before(&self) -> Timestamp { self.clock.before }
    pub fn belongs_to(&self, owner: [u8; 32]) -> bool { self.owner == owner }

    pub(super) fn booked(&self) -> bool {
        matches!(self.phase, Phase::Booked) || self.is_pending_appointment()
    }

    pub(super) fn original(&self) -> Basis { self.original }

    fn matches(&self, input: FundingInput) -> bool {
        if self.is_appointment() { return false; }
        match input {
            FundingInput::Book { owner, predecessor, .. } => self.owner == owner && self.predecessor == predecessor,
            FundingInput::Consume { owner, booking, .. } => self.owner == owner && self.booking == booking,
            FundingInput::Appointment { .. } => false,
        }
    }

    fn same_booking(self, other: Self) -> bool {
        Self { phase: other.phase, ..self } == other
    }

    pub(super) fn terminal_of(self, original: Self) -> bool {
        matches!(original.phase, Phase::Booked) && !self.booked() && self.same_booking(original)
    }

    pub(super) fn acceptance_is_valid(&self) -> bool {
        matches!(self.phase, Phase::Terminal { observed }
            if observed.covers(self.clock.issued) && self.clock.reading <= observed.instant
                && observed.instant < self.clock.before)
    }

    pub(super) fn check_admission(
        self, request: &Transact, receipt: &Receipt, position: u64, namespace: Option<[u8; 32]>,
    ) -> Result<()> {
        let clock = self.clock;
        let end = clock.reading.0.checked_add(1_000_000).map(Timestamp).ok_or_else(invalid_record)?;
        if self.booking == [0; 16] || self.owner == [0; 32] || self.original.incarnation() != receipt.basis.incarnation()
            || self.original.position() >= position || (namespace.is_none() && self.predecessor != self.original)
            || (namespace.is_some() && self.predecessor.incarnation() == self.original.incarnation())
            || namespace.is_some_and(|namespace| namespace != self.owner)
            || DecisionObservation::decode(&clock.issued.encode()).is_err()
            || clock_slot_reading(clock.issued.instant) != Some(clock.reading)
            || clock.before <= request.window.not_before.unwrap_or(clock.reading).max(clock.reading)
            || clock.before > clock.journal_before || clock.journal_before > request.window.before.unwrap_or(end).min(end)
            || TimedJournalCutoff::new(clock.journal_before, clock.certificate).is_err()
        {
            return Err(invalid_record());
        }
        if let Phase::Terminal { observed } = self.phase
            && (DecisionObservation::decode(&observed.encode()).is_err() || !observed.covers(clock.issued)
                || observed.instant < request.window.not_before.unwrap_or(clock.reading).max(clock.reading))
        {
            return Err(invalid_record());
        }
        Ok(())
    }

    pub(super) fn journal_guard(self) -> Result<Option<(Timestamp, Timestamp, TimedJournalCutoff)>> {
        match self.phase {
            Phase::Terminal { observed } if self.acceptance_is_valid() =>
                Ok(Some((observed.instant, self.clock.before, TimedJournalCutoff::new(self.clock.journal_before, self.clock.certificate)?))),
            _ => Ok(None),
        }
    }
}

pub(super) enum FundingDecision {
    Replay(Receipt),
    Book(FundedReceipt),
    Consume { receipt: FundedReceipt, sample: Timestamp, refusal: Option<Anomaly> },
}

pub(super) fn resolve_funding(
    view: &View<'_>, request: &Transact, input: FundingInput, stored: Option<&StoredReceipt>,
    scope: Option<ScopedTransact<'_>>,
) -> Result<FundingDecision> {
    request.check_funded_shape(scope.is_some())?;
    if let Some(stored) = stored {
        let offer = stored.receipt.decision_clock.filter(|offer| offer.matches(input)).ok_or_else(conflicting_key)?;
        return match input {
            FundingInput::Book { .. } => Ok(FundingDecision::Replay(replay(&stored.receipt))),
            FundingInput::Consume { .. } if !offer.booked() => Ok(FundingDecision::Replay(replay(&stored.receipt))),
            FundingInput::Consume { observed, certificate, .. } => {
                let observed = observed.ok_or_else(funding_unsupported)?;
                if !observed.covers(offer.clock.issued)
                    || DecisionObservation::decode(&observed.encode()).is_err()
                {
                    return Err(funding_invalid());
                }
                let start = request.window.not_before.unwrap_or(offer.clock.reading).max(offer.clock.reading);
                if observed.instant < start {
                    return Err(Anomaly::new(AnomalyCategory::Busy, "rtdb/window-pending", "The transaction window has not opened"));
                }
                let refusal = (observed.instant >= offer.clock.before).then(|| {
                    Anomaly::new(AnomalyCategory::Conflict, "rtdb/window-expired", "The transaction window has expired")
                });
                if refusal.is_none() && certificate != Some(offer.clock.certificate) {
                    return Err(funding_unsupported());
                }
                Ok(FundingDecision::Consume {
                    sample: observed.instant, refusal,
                    receipt: FundedReceipt { phase: Phase::Terminal { observed }, ..offer },
                })
            }
            FundingInput::Appointment { .. } => Err(funding_invalid()),
        };
    }
    let FundingInput::Book { owner, predecessor, booking, issued } = input else {
        return Err(funding_invalid());
    };
    if owner == [0; 32] || booking == [0; 16] || scope.is_some_and(|scope| owner != scope.receipt_namespace) {
        return Err(funding_invalid());
    }
    let original = original_predecessor(view.durable_index, predecessor, scope)?;
    let clock = issued.ok_or_else(funding_unsupported)?;
    Ok(FundingDecision::Book(FundedReceipt { booking, owner, predecessor, original, clock, phase: Phase::Booked }))
}

fn original_predecessor(index: &BranchIndex, predecessor: Basis, scope: Option<ScopedTransact<'_>>) -> Result<Basis> {
    match scope {
        Some(scope) => index.project_main_visibility(scope.labels)?.source_basis(predecessor),
        None => Ok(index.exact(predecessor)?.basis),
    }
}

pub fn funding_unsupported() -> Anomaly {
    Anomaly::unsupported("rtdb/decision-clock-offer", "The route has no certified DecisionClock offer authority")
}

pub fn funding_invalid() -> Anomaly {
    Anomaly::incorrect("rtdb/decision-clock-offer", "The DecisionClock offer does not match the original transaction")
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_types::rtdb_v1::ClaimSealer;

    const INCARNATION: [u8; 16] = [1; 16];
    const BOOKING: [u8; 16] = [4; 16];
    const OWNER: [u8; 32] = [9; 32];
    const FEATURES: &[&str] = &[TRANSACT_REQUIRES, RELATIVE_VALID_TIME];
    const READING: Timestamp = Timestamp(1_000_000);

    fn observation(sequence: u64, micros: i64) -> DecisionObservation {
        DecisionObservation { domain: [3; 16], epoch: 1, sequence, instant: Timestamp(micros) }
    }

    fn book(predecessor: Basis, booking: [u8; 16]) -> FundingInput {
        FundingInput::Book {
            owner: OWNER, predecessor, booking,
            issued: Some(OfferClock {
                issued: observation(1, 100_000),
                reading: READING,
                before: Timestamp(1_800_000),
                journal_before: Timestamp(1_900_000),
                certificate: [8; 32],
            }),
        }
    }

    fn consume(micros: i64) -> FundingInput {
        FundingInput::Consume { owner: OWNER, booking: BOOKING, observed: Some(observation(2, micros)), certificate: Some([8; 32]) }
    }

    fn request(key: u8, ops: Vec<Op>) -> Transact {
        Transact {
            key: vec![key; 16], require: vec![], window: Window::default(), guards: vec![], ops,
            otherwise: vec![], requires: vec![], custody_request_digest: None, request_memory: None,
        }
    }

    fn put(table: TableRef, valid: ValidTime, cells: StructFields<Cell>) -> Op {
        Op::Put { branch: MAIN_BRANCH.into(), table, if_absent: false, rows: vec![Row {
            id: EntityId::Integer(1), valid: Some(valid), cells, custody_group: None,
            protected_columns: vec![], custody_payload: None,
        }] }
    }

    fn funded(
        pipeline: &mut TransactPipeline, request: &Transact, scope: Option<ScopedTransact<'_>>, input: FundingInput,
    ) -> Result<Decision> {
        let claims = ClaimSealer::new(INCARNATION, [2; 32]);
        pipeline.decide_funded(request, Timestamp(3_000_000), FEATURES, &claims, scope, None, input, usize::MAX)
    }

    fn append(pipeline: &mut TransactPipeline, decision: Decision) -> (Receipt, ResolvedTransact) {
        let Decision::Append { position, record } = decision else { panic!("expected append") };
        let TransactionRecord::Tx(resolved) = TransactionRecord::decode(&record).unwrap() else { panic!("expected transaction") };
        (pipeline.acknowledge(position).unwrap().pop().unwrap(), resolved)
    }

    #[test]
    fn exact_booking_and_terminal_recover_with_the_canonical_request() {
        let mut state = TransactState::new(INCARNATION);
        let mut pipeline = TransactPipeline::new(state.clone(), 1, 4).unwrap();
        let table = TableRef::new("public", "items");
        let valid = ValidTime::new(ValidTimeBound::TransactionMicros(0), ValidTimeBound::TransactionMicros(10)).unwrap();
        let mut request = request(1, vec![put(table.clone(), valid, StructFields::from([("reading".into(), Cell::TxTime)]))]);
        request.require.push(DECISION_CLOCK_OFFER.into());
        request.window.before = Some(Timestamp(1_950_000));
        let predecessor = state.index().head().basis;
        let booked = funded(&mut pipeline, &request, None, book(predecessor, BOOKING)).unwrap();
        let (booked, record) = append(&mut pipeline, booked);
        assert!(record.effects().is_empty());
        assert_eq!(record.admission().2, request.logical_sha256().unwrap());
        assert_eq!(pipeline.tx_status(&request.key).unwrap_err().code, "rtdb/transaction-pending");

        state = TransactState::decode_checkpoint(&pipeline.durable().encode_checkpoint().unwrap(), pipeline.durable().index().head()).unwrap();
        pipeline = TransactPipeline::new(state, 2, 4).unwrap();
        let Decision::Answer(replayed) = funded(&mut pipeline, &request, None, book(predecessor, [5; 16])).unwrap()
            else { panic!("expected original booking") };
        assert_eq!(replayed.decision_clock, booked.decision_clock);
        assert!(funded(&mut pipeline, &request, None, consume(999_999)).is_err());
        let mut foreign = request.clone();
        foreign.window.before = Some(Timestamp(1_960_000));
        assert!(funded(&mut pipeline, &foreign, None, consume(1_200_000)).is_err());
        assert!(funded(&mut pipeline, &request, None,
            FundingInput::Consume { owner: [7; 32], booking: BOOKING, observed: Some(observation(2, 1_200_000)), certificate: Some([8; 32]) }).is_err());
        let foreign_receiver = DecisionObservation { domain: [7; 16], ..observation(2, 1_200_000) };
        assert!(funded(&mut pipeline, &request, None,
            FundingInput::Consume { owner: OWNER, booking: BOOKING, observed: Some(foreign_receiver), certificate: Some([8; 32]) }).is_err());
        assert!(funded(&mut pipeline, &request, None,
            FundingInput::Consume { owner: OWNER, booking: BOOKING, observed: Some(observation(2, 1_200_000)), certificate: Some([7; 32]) }).is_err());

        let terminal = funded(&mut pipeline, &request, None, consume(1_200_000)).unwrap();
        let (receipt, record) = append(&mut pipeline, terminal);
        assert_eq!(receipt.outcome, Outcome::Then);
        assert!(receipt.system_time > request.window.before.unwrap());
        assert_eq!(record.funded_guard().unwrap().unwrap().2.certificate_sha256(), [8; 32]);
        assert_eq!(record.canonical_request().unwrap().get(), request.canonical_request().unwrap().get());
        let view = pipeline.durable().index().view(MAIN_BRANCH, At::Latest, Layer::Merged, READING).unwrap();
        let row = view.row(&table, &EntityId::Integer(1), READING).unwrap();
        assert_eq!(row.image.as_ref().unwrap().cells.get("reading"), Some(&Value::Timestamp {
            value: READING.0, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()),
        }));

        let state = pipeline.durable();
        let recovered = TransactState::decode_checkpoint(&state.encode_checkpoint().unwrap(), state.index().head()).unwrap();
        assert_eq!(recovered.tx_status(&request.key).unwrap(), replay(&receipt));
        let mut recovered = TransactPipeline::new(recovered, 3, 4).unwrap();
        let retry = FundingInput::new(FundingCommand::Consume { booking: BOOKING }, OWNER);
        let Decision::Answer(retry) = funded(&mut recovered, &request, None, retry).unwrap()
            else { panic!("expected terminal receipt") };
        assert_eq!(retry, replay(&receipt));
    }

    #[test]
    fn scoped_guards_and_window_ignore_hidden_clock_and_row() {
        let labels = LabelScope { read: [vec![1]].into(), write: Default::default() };
        let scope = ScopedTransact { labels: &labels, receipt_namespace: OWNER };
        let table = TableRef::new("public", "items");
        let claims = ClaimSealer::new(INCARNATION, [2; 32]);
        let mut pipeline = TransactPipeline::new(TransactState::new(INCARNATION), 1, 4).unwrap();
        let declare = request(2, vec![Op::Declare { table: table.clone(), change: Declaration::Visibility(Visibility { column: "label".into() }) }]);
        let declaration = pipeline.decide(&declare, Timestamp(1), FEATURES, &claims).unwrap();
        append(&mut pipeline, declaration);
        let initial = pipeline.into_durable();
        let predecessor = initial.index().project_main_visibility(&labels).unwrap().index().head().basis;
        let mut validation = request(1, vec![]);
        validation.require.push(DECISION_CLOCK_OFFER.into());
        validation.requires.push(Guard::Version {
            branch: MAIN_BRANCH.into(), table: table.clone(), id: EntityId::Integer(1),
            valid_at: None, expect: Version::Absent,
        });
        let run = |hidden, decision_time| {
            let mut pipeline = TransactPipeline::new(initial.clone(), 2, 4).unwrap();
            if hidden {
                let insert = request(3, vec![put(table.clone(), Interval::ALL.into(),
                    StructFields::from([("label".into(), Cell::Value(Value::Binary(vec![2])))]))]);
                let staged = pipeline.decide(&insert, Timestamp(5_000_000), FEATURES, &claims).unwrap();
                append(&mut pipeline, staged);
            }
            let staged = funded(&mut pipeline, &validation, Some(scope), book(predecessor, BOOKING)).unwrap();
            append(&mut pipeline, staged);
            let staged = funded(&mut pipeline, &validation, Some(scope), consume(decision_time)).unwrap();
            let (receipt, _) = append(&mut pipeline, staged);
            pipeline.durable().project_receipt(receipt, &labels).unwrap()
        };
        let accepted = run(false, 1_200_000);
        assert_eq!(accepted.outcome, Outcome::Then);
        assert_eq!(accepted, run(true, 1_200_000));
        let refused = run(false, 1_800_000);
        assert!(matches!(&refused.outcome, Outcome::Refused { error } if error.code == "rtdb/window-expired"));
        assert_eq!(refused, run(true, 1_800_000));
    }
}
