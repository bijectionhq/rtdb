//! Private custody state for an already ordered assertion erasure.
//!
//! The source order owns the fence and exact roster. This journal resumes
//! destruction outside payload checkpoints. Only the installed custody
//! authority may call it with authenticated source orders or hold decisions.

use std::{collections::BTreeMap, time::Duration};

use ring::{
    digest::{SHA256, digest},
    signature::{ED25519, Ed25519KeyPair, KeyPair, UnparsedPublicKey},
};
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{AssertionStage, FamilyFence};

pub const READER_DRAIN_LIMIT_MS: u64 = 120_000;
pub const DESTRUCTION_TARGET_MS: u64 = 86_400_000;
pub const FIXED_DEADLINE_MS: u64 = 30 * DESTRUCTION_TARGET_MS;

const ATTEMPTS: usize = 8;
const HOLDER_TIMEOUT: Duration = Duration::from_secs(30);

/// The requester projection is derived solely from the original order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ErasureAcceptance {
    pub logical_commit: [u8; 32],
    pub physical_by_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CustodyKind {
    ReleaseDrain,
    AssertionVault,
    KeyServiceBackup,
    Derivative,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Obligation {
    pub kind: CustodyKind,
    pub holder_key: [u8; 32],
    pub inventory: [u8; 32],
}

/// A verified source-order projection. The source must persist this data in
/// the same order as the logical fence before delivering it to the journal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ErasureOrder {
    pub domain: Uuid,
    pub id: Uuid,
    pub logical_commit: [u8; 32],
    pub committed_at_ms: u64,
    /// Active covering holds selected by the source in the fence's order.
    pub initial_holds: Vec<Uuid>,
    /// Canonically sorted, exact closed roster, including the backup boundary
    /// even when the backup holder certifies exclusion.
    pub obligations: Vec<Obligation>,
}

impl ErasureOrder {
    pub fn acceptance(&self) -> Result<ErasureAcceptance> {
        Ok(ErasureAcceptance {
            logical_commit: self.logical_commit,
            physical_by_ms: plus(self.committed_at_ms, FIXED_DEADLINE_MS)?,
        })
    }

    fn validate(&self) -> Result<()> {
        self.acceptance()?;
        if self.domain.is_nil() || self.id.is_nil() || self.logical_commit == [0; 32]
            || self.initial_holds.iter().any(Uuid::is_nil)
            || self.initial_holds.windows(2).any(|pair| pair[0] >= pair[1])
            || self.obligations.windows(2).any(|pair| pair[0] >= pair[1])
            || self.obligations.iter().any(|item| {
                item.holder_key == [0; 32] || item.inventory == [0; 32]
            })
            || [CustodyKind::ReleaseDrain, CustodyKind::AssertionVault, CustodyKind::KeyServiceBackup]
                .into_iter().any(|kind| !self.obligations.iter().any(|item| item.kind == kind))
        {
            return Err(invalid("The erasure order requires an exact closed custody roster"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settlement {
    pub obligation: Obligation,
    pub observed_at_ms: u64,
    pub disposition: [u8; 32],
    pub signature: Vec<u8>,
}

impl Settlement {
    pub fn signing_bytes(&self, order: &ErasureOrder, ordered_at_ms: u64) -> Result<Vec<u8>> {
        let mut message = b"rtdb.multi/assertion-destruction-settlement/v1\0".to_vec();
        message.extend_from_slice(digest(&SHA256, &encode(order)?).as_ref());
        message.extend_from_slice(&ordered_at_ms.to_be_bytes());
        message.extend_from_slice(&encode(&(self.obligation, self.observed_at_ms, self.disposition))?);
        Ok(message)
    }

    fn verify(&self, order: &ErasureOrder, ordered_at_ms: u64, now_ms: u64) -> Result<()> {
        if self.observed_at_ms < order.committed_at_ms
            || (self.obligation.kind != CustodyKind::ReleaseDrain && self.observed_at_ms < ordered_at_ms)
            || self.observed_at_ms > now_ms || self.disposition == [0; 32]
        {
            return Err(invalid("The custody disposition is outside its ordered observation window"));
        }
        UnparsedPublicKey::new(&ED25519, self.obligation.holder_key)
            .verify(&self.signing_bytes(order, ordered_at_ms)?, &self.signature)
            .map_err(|_| invalid("The original custodian did not sign the disposition"))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Hold {
    placed_at_ms: u64,
    released_at_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
enum Phase {
    Waiting,
    Destroying { ordered_at_ms: u64, settlements: Vec<Settlement> },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    order: ErasureOrder,
    holds: BTreeMap<Uuid, Hold>,
    suspended_at_ms: Option<u64>,
    suspended_ms: u64,
    missed_before_hold: bool,
    last_hold_change_ms: u64,
    phase: Phase,
    attestation: Option<SignedDestruction>,
}

impl Record {
    fn new(order: ErasureOrder) -> Self {
        Self {
            last_hold_change_ms: order.committed_at_ms,
            holds: order.initial_holds.iter().map(|&id| (id, Hold {
                placed_at_ms: order.committed_at_ms,
                released_at_ms: None,
            })).collect(),
            suspended_at_ms: (!order.initial_holds.is_empty()).then_some(order.committed_at_ms),
            order,
            suspended_ms: 0,
            missed_before_hold: false,
            phase: Phase::Waiting,
            attestation: None,
        }
    }

    fn effective_by_ms(&self) -> Result<u64> {
        plus(self.order.acceptance()?.physical_by_ms, self.suspended_ms)
    }

    fn place_hold(&mut self, hold: Uuid, at_ms: u64) -> Result<()> {
        if let Some(prior) = self.holds.get(&hold) {
            return if prior.placed_at_ms == at_ms { Ok(()) } else { Err(changed()) };
        }
        if hold.is_nil() || at_ms < self.last_hold_change_ms || self.phase != Phase::Waiting {
            return Err(changed());
        }
        if self.suspended_at_ms.is_none() {
            self.missed_before_hold |= at_ms > self.effective_by_ms()?;
            self.suspended_at_ms = Some(at_ms);
        }
        self.last_hold_change_ms = at_ms;
        self.holds.insert(hold, Hold { placed_at_ms: at_ms, released_at_ms: None });
        Ok(())
    }

    fn release_hold(&mut self, hold: Uuid, at_ms: u64) -> Result<()> {
        let entry = self.holds.get_mut(&hold).ok_or_else(changed)?;
        if let Some(prior) = entry.released_at_ms {
            return if prior == at_ms { Ok(()) } else { Err(changed()) };
        }
        if at_ms < self.last_hold_change_ms || self.phase != Phase::Waiting {
            return Err(changed());
        }
        entry.released_at_ms = Some(at_ms);
        self.last_hold_change_ms = at_ms;
        if self.holds.values().all(|hold| hold.released_at_ms.is_some()) {
            let since = self.suspended_at_ms.take().ok_or_else(changed)?;
            self.suspended_ms = plus(self.suspended_ms, at_ms.checked_sub(since).ok_or_else(changed)?)?;
            self.effective_by_ms()?;
        }
        Ok(())
    }

    fn begin(&mut self, now_ms: u64) -> Result<()> {
        if self.phase == Phase::Waiting && self.suspended_at_ms.is_none() {
            let earliest = plus(self.order.committed_at_ms, READER_DRAIN_LIMIT_MS)?
                .max(self.last_hold_change_ms);
            if now_ms >= earliest {
                self.phase = Phase::Destroying { ordered_at_ms: now_ms, settlements: Vec::new() };
            }
        }
        Ok(())
    }

    fn join(&mut self, settlement: Settlement, now_ms: u64) -> Result<()> {
        let Phase::Destroying { ordered_at_ms, settlements } = &mut self.phase else {
            return Err(changed());
        };
        if self.order.obligations.binary_search(&settlement.obligation).is_err() {
            return Err(invalid("The disposition is outside the original custody roster"));
        }
        if settlement.obligation.kind != CustodyKind::ReleaseDrain {
            let drain = self.order.obligations.iter()
                .take_while(|item| item.kind == CustodyKind::ReleaseDrain);
            if drain.into_iter().any(|item| !settlements.iter().any(|joined| {
                joined.obligation == *item && joined.observed_at_ms <= settlement.observed_at_ms
            })) {
                return Err(invalid("Destruction requires every original release drain"));
            }
        }
        settlement.verify(&self.order, *ordered_at_ms, now_ms)?;
        match settlements.binary_search_by_key(&settlement.obligation, |item| item.obligation) {
            Ok(index) if settlements[index] == settlement => Ok(()),
            Ok(_) => Err(changed()),
            Err(index) => {
                settlements.insert(index, settlement);
                Ok(())
            }
        }
    }

    fn report(&self, signer: &Ed25519KeyPair) -> Result<Option<SignedDestruction>> {
        let Phase::Destroying { ordered_at_ms, settlements } = &self.phase else {
            return Ok(None);
        };
        if settlements.len() != self.order.obligations.len() {
            return Ok(None);
        }
        let destroyed_at_ms = settlements.iter().map(|item| item.observed_at_ms).max()
            .ok_or_else(changed)?;
        let statement = DestructionStatement {
            domain: self.order.domain,
            order: self.order.id,
            logical_commit: self.order.logical_commit,
            original_by_ms: self.order.acceptance()?.physical_by_ms,
            effective_by_ms: self.effective_by_ms()?,
            destroyed_at_ms,
            met_deadline: !self.missed_before_hold && destroyed_at_ms <= self.effective_by_ms()?,
            ordered_at_ms: *ordered_at_ms,
            roster_digest: hash(&self.order.obligations)?,
            settlement_digest: hash(settlements)?,
        };
        let signature = signer.sign(&statement.signing_bytes()?).as_ref().to_vec();
        Ok(Some(SignedDestruction { statement, signature }))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DestructionStatement {
    pub domain: Uuid,
    pub order: Uuid,
    pub logical_commit: [u8; 32],
    pub original_by_ms: u64,
    pub effective_by_ms: u64,
    pub destroyed_at_ms: u64,
    pub met_deadline: bool,
    pub ordered_at_ms: u64,
    pub roster_digest: [u8; 32],
    pub settlement_digest: [u8; 32],
}

impl DestructionStatement {
    pub fn signing_bytes(&self) -> Result<Vec<u8>> {
        signed_message(b"rtdb.multi/assertion-destruction-attestation/v1\0", self)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignedDestruction {
    pub statement: DestructionStatement,
    pub signature: Vec<u8>,
}

/// The closed population for one original deadline in one custody domain.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeadlineCohort {
    pub domain: Uuid,
    pub original_by_ms: u64,
    pub orders: Vec<Uuid>,
}

impl DeadlineCohort {
    pub fn signing_bytes(&self) -> Result<Vec<u8>> {
        if self.domain.is_nil() || self.original_by_ms < FIXED_DEADLINE_MS
            || self.orders.is_empty() || self.orders.iter().any(Uuid::is_nil)
            || self.orders.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(invalid("The deadline cohort requires a closed, ordered set of original orders"));
        }
        signed_message(b"rtdb.multi/assertion-deadline-cohort/v1\0", self)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignedDeadlineCohort {
    pub cohort: DeadlineCohort,
    pub signature: Vec<u8>,
}

/// A private audit report. Every member has a durable, independently
/// checkable destruction statement including the effective deadline.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeadlineAttestation {
    pub cohort: SignedDeadlineCohort,
    pub dispositions: Vec<SignedDestruction>,
    pub signature: Vec<u8>,
}

impl DeadlineAttestation {
    pub fn signing_bytes(&self) -> Result<Vec<u8>> {
        signed_message(
            b"rtdb.multi/assertion-deadline-attestation/v1\0",
            &(&self.cohort, &self.dispositions),
        )
    }
}

/// Original authority storage. Compare-and-set and reads must survive failover
/// and live outside every payload or key-service backup that can be restored.
#[async_trait::async_trait]
pub trait DeadlineJournal: Send + Sync {
    async fn read(&self, order: Uuid) -> Result<Option<(Vec<u8>, String)>>;
    async fn compare_and_set(&self, order: Uuid, previous: Option<&str>, bytes: Vec<u8>) -> Result<bool>;
    /// A bounded, strictly increasing page of original order IDs.
    async fn page(&self, after: Option<Uuid>) -> Result<Vec<Uuid>>;
}

/// Configured original holders. A timeout or lost reply is retried by exact
/// order and obligation. A holder must return the same signed terminal result.
#[async_trait::async_trait]
pub trait DestructionCustody: Send + Sync {
    fn now_ms(&self) -> Result<u64>;
    async fn settle(
        &self, order: &ErasureOrder, ordered_at_ms: u64, obligation: Obligation,
    ) -> Result<Option<Settlement>>;
}

/// Resolve the original vault for each enrolled stage and confirm its permanent
/// tombstone. The installed holder owns its credential and the signed receipt.
#[async_trait::async_trait]
pub trait AssertionVaultRoutes: Send + Sync {
    async fn retire_original(&self, stage: AssertionStage) -> Result<()>;
}

pub struct AssertionDeadline<'a> {
    journal: &'a dyn DeadlineJournal,
}

impl<'a> AssertionDeadline<'a> {
    pub fn new(journal: &'a dyn DeadlineJournal) -> Self {
        Self { journal }
    }

    pub async fn publish(&self, order: ErasureOrder) -> Result<ErasureAcceptance> {
        order.validate()?;
        for _ in 0..ATTEMPTS {
            if let Some((prior, _)) = self.read(order.id).await? {
                return if prior.order == order { order.acceptance() } else { Err(changed()) };
            }
            let bytes = encode(&Record::new(order.clone()))?;
            if self.journal.compare_and_set(order.id, None, bytes).await? {
                return order.acceptance();
            }
        }
        Err(changed())
    }

    pub async fn place_hold(&self, order: Uuid, hold: Uuid, ordered_at_ms: u64) -> Result<()> {
        self.update(order, |record| record.place_hold(hold, ordered_at_ms)).await?;
        Ok(())
    }

    pub async fn release_hold(&self, order: Uuid, hold: Uuid, ordered_at_ms: u64) -> Result<()> {
        self.update(order, |record| record.release_hold(hold, ordered_at_ms)).await?;
        Ok(())
    }

    /// Executes the vault part of an installed holder's dispatched obligation.
    /// The holder signs its disposition only after all exact stages settle.
    pub async fn retire_family_capabilities(
        &self, fence: &FamilyFence, obligation: Obligation, vaults: &dyn AssertionVaultRoutes,
    ) -> Result<()> {
        let record = self.family_order(fence).await?;
        if obligation.kind != CustodyKind::AssertionVault
            || obligation.inventory != fence.vault_inventory()
            || !record.order.obligations.contains(&obligation)
        {
            return Err(changed());
        }
        let Phase::Destroying { settlements, .. } = &record.phase else { return Err(changed()); };
        if record.order.obligations.iter()
            .filter(|item| item.kind == CustodyKind::ReleaseDrain)
            .any(|item| !settlements.iter().any(|receipt| receipt.obligation == *item))
        {
            return Err(changed());
        }
        for &stage in fence.assertion_capabilities.iter().chain(&fence.link_capabilities) {
            vaults.retire_original(stage).await?;
        }
        Ok(())
    }

    async fn family_order(&self, fence: &FamilyFence) -> Result<Record> {
        let (record, _) = self.read(fence.order).await?.ok_or_else(changed)?;
        let inventory = fence.vault_inventory();
        if !record.order.obligations.iter().any(|item| {
            item.kind == CustodyKind::AssertionVault && item.inventory == inventory
        }) {
            return Err(changed());
        }
        Ok(record)
    }

    pub(super) async fn require_settled_family(&self, fence: &FamilyFence, custody_key: &[u8; 32]) -> Result<()> {
        let record = self.family_order(fence).await?;
        let report = record.attestation.ok_or_else(changed)?;
        let Phase::Destroying { ordered_at_ms, settlements } = &record.phase else { return Err(changed()); };
        if report.statement.order != record.order.id
            || report.statement.domain != record.order.domain
            || report.statement.logical_commit != record.order.logical_commit
            || report.statement.ordered_at_ms != *ordered_at_ms
            || report.statement.roster_digest != hash(&record.order.obligations)?
            || report.statement.settlement_digest != hash(settlements)?
            || settlements.len() != record.order.obligations.len()
        {
            return Err(changed());
        }
        UnparsedPublicKey::new(&ED25519, custody_key)
            .verify(&report.statement.signing_bytes()?, &report.signature)
            .map_err(|_| invalid("The original custody authority did not sign the disposition"))
    }

    /// Resume the durable irreversible order before calling an original holder.
    /// Callers revisit pending orders and isolate their failures across the scan.
    pub async fn reconcile(&self, id: Uuid, custody: &dyn DestructionCustody) -> Result<()> {
        let record = self.update(id, |record| record.begin(custody.now_ms()?)).await?;
        let Phase::Destroying { ordered_at_ms, mut settlements } = record.phase else {
            return Ok(());
        };
        let mut first_error = None;
        for &obligation in &record.order.obligations {
            if settlements.iter().any(|item| item.obligation == obligation) {
                continue;
            }
            if obligation.kind != CustodyKind::ReleaseDrain
                && record.order.obligations.iter().take_while(|item| item.kind == CustodyKind::ReleaseDrain)
                    .any(|item| !settlements.iter().any(|joined| joined.obligation == *item))
            {
                break;
            }
            let attempt = async {
                let receipt = tokio::time::timeout(
                    HOLDER_TIMEOUT, custody.settle(&record.order, ordered_at_ms, obligation),
                ).await.map_err(|_| Anomaly::new(
                    AnomalyCategory::Unavailable, "rtdb.multi/assertion-custodian-timeout",
                    "The original custodian has no confirmed disposition",
                ))??;
                if let Some(receipt) = receipt {
                    if receipt.obligation != obligation {
                        return Err(invalid("The custodian returned a different obligation"));
                    }
                    let updated = self.update(id, |stored| stored.join(receipt.clone(), custody.now_ms()?)).await?;
                    if let Phase::Destroying { settlements: joined, .. } = updated.phase {
                        settlements = joined;
                    }
                }
                Ok(())
            };
            if let Err(error) = attempt.await {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// An original authority worker resumes the journal from its first page
    /// on replacement. One failed holder leaves its order pending while the
    /// cursor moves past it. No independent completeness or timing claim comes
    /// from this sweep.
    pub async fn run(
        &self, custody: &dyn DestructionCustody, signer: &Ed25519KeyPair,
        stop: &tokio_util::sync::CancellationToken,
    ) {
        let mut after = None;
        loop {
            let step = async {
                let page = self.journal.page(after).await?;
                if page.is_empty() {
                    return Ok::<_, Anomaly>(None);
                }
                if page.windows(2).any(|pair| pair[0] >= pair[1])
                    || after.is_some_and(|previous| page[0] <= previous)
                {
                    return Err(invalid("The custody journal returned an invalid order page"));
                }
                let next = page.last().copied();
                for id in page {
                    if let Err(error) = self.reconcile(id, custody).await {
                        tracing::warn!(order = %id, code = %error.code, "Assertion custody remains pending");
                        continue;
                    }
                    if let Err(error) = self.update(id, |record| {
                        if record.attestation.is_none() {
                            record.attestation = record.report(signer)?;
                        }
                        Ok(())
                    }).await {
                        tracing::warn!(order = %id, code = %error.code, "Could not retain assertion destruction evidence");
                    }
                }
                Ok(next)
            };
            let result = tokio::select! {
                _ = stop.cancelled() => return,
                result = step => result,
            };
            match result {
                Ok(Some(next)) => {
                    after = Some(next);
                    continue;
                }
                Ok(None) => after = None,
                Err(error) => tracing::warn!(code = %error.code, "Could not scan assertion erasure journal"),
            }
            tokio::select! {
                _ = stop.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_secs(10)) => (),
            }
        }
    }

    /// This read belongs exclusively to the authorized audit route.
    pub async fn attest(&self, id: Uuid) -> Result<Option<SignedDestruction>> {
        Ok(self.read(id).await?.ok_or_else(changed)?.0.attestation)
    }

    /// The installed source chooses the closed cohort in its own order.
    /// The caller supplies its configured verification key, and the original
    /// custody signing key held by the audit authority.
    pub async fn attest_deadline(
        &self, cohort: SignedDeadlineCohort, source_key: &[u8; 32],
        custody_signer: &Ed25519KeyPair,
    ) -> Result<Option<DeadlineAttestation>> {
        UnparsedPublicKey::new(&ED25519, source_key)
            .verify(&cohort.cohort.signing_bytes()?, &cohort.signature)
            .map_err(|_| invalid("The original source did not authenticate the deadline cohort"))?;

        let mut dispositions = Vec::with_capacity(cohort.cohort.orders.len());
        for &id in &cohort.cohort.orders {
            let Some(report) = self.attest(id).await? else { return Ok(None); };
            if report.statement.order != id
                || report.statement.domain != cohort.cohort.domain
                || report.statement.original_by_ms != cohort.cohort.original_by_ms
            {
                return Err(invalid("The disposition belongs to a different original deadline or order"));
            }
            UnparsedPublicKey::new(&ED25519, custody_signer.public_key().as_ref())
                .verify(&report.statement.signing_bytes()?, &report.signature)
                .map_err(|_| invalid("The original custody authority did not sign the disposition"))?;
            dispositions.push(report);
        }
        let mut report = DeadlineAttestation { cohort, dispositions, signature: Vec::new() };
        report.signature = custody_signer.sign(&report.signing_bytes()?).as_ref().to_vec();
        Ok(Some(report))
    }

    async fn read(&self, id: Uuid) -> Result<Option<(Record, String)>> {
        let Some((bytes, version)) = self.journal.read(id).await? else { return Ok(None); };
        let record: Record = serde_json::from_slice(&bytes)
            .map_err(|error| Anomaly::fault("rtdb.multi/assertion-deadline-journal", error.to_string()))?;
        if record.order.id != id {
            return Err(changed());
        }
        Ok(Some((record, version)))
    }

    async fn update(&self, id: Uuid, apply: impl Fn(&mut Record) -> Result<()>) -> Result<Record> {
        for _ in 0..ATTEMPTS {
            let (previous, version) = self.read(id).await?.ok_or_else(changed)?;
            let mut successor = previous.clone();
            apply(&mut successor)?;
            if successor == previous
                || self.journal.compare_and_set(id, Some(&version), encode(&successor)?).await?
            {
                return Ok(successor);
            }
        }
        Err(changed())
    }
}

fn encode(value: &impl Serialize) -> Result<Vec<u8>> {
    serde_json::to_vec(value)
        .map_err(|error| Anomaly::fault("rtdb.multi/assertion-deadline-journal", error.to_string()))
}

fn signed_message(domain: &[u8], value: &impl Serialize) -> Result<Vec<u8>> {
    let mut message = domain.to_vec();
    message.extend_from_slice(&encode(value)?);
    Ok(message)
}

fn hash(value: &impl Serialize) -> Result<[u8; 32]> {
    Ok(digest(&SHA256, &encode(value)?).as_ref().try_into().expect("SHA-256 width"))
}

fn plus(left: u64, right: u64) -> Result<u64> {
    left.checked_add(right).ok_or_else(|| invalid("The erasure deadline exceeds the clock's range"))
}

fn invalid(message: &'static str) -> Anomaly {
    Anomaly::incorrect("rtdb.multi/assertion-deadline", message)
}

fn changed() -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/assertion-deadline-changed",
        "The original erasure order or hold state has changed")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signer() -> Ed25519KeyPair {
        Ed25519KeyPair::from_seed_unchecked(&[73; 32]).unwrap()
    }

    fn order(signer: &Ed25519KeyPair) -> ErasureOrder {
        let holder_key = signer.public_key().as_ref().try_into().unwrap();
        ErasureOrder {
            domain: Uuid::from_u128(1),
            id: Uuid::from_u128(2),
            logical_commit: [3; 32],
            committed_at_ms: 1_000,
            initial_holds: Vec::new(),
            obligations: [CustodyKind::ReleaseDrain, CustodyKind::AssertionVault, CustodyKind::KeyServiceBackup]
                .into_iter().map(|kind| Obligation { kind, holder_key, inventory: [4; 32] }).collect(),
        }
    }

    #[test]
    fn overlap_suspends_one_deadline_and_preserves_requester_projection() -> Result<()> {
        let mut order = order(&signer());
        let existing_hold = Uuid::from_u128(5);
        let later_hold = Uuid::from_u128(6);
        order.initial_holds.push(existing_hold);
        let acceptance = order.acceptance()?;
        let mut record = Record::new(order);
        record.place_hold(later_hold, 3_000)?;
        record.release_hold(existing_hold, 4_000)?;
        record.begin(READER_DRAIN_LIMIT_MS + 1_000)?;
        assert_eq!(record.phase, Phase::Waiting);
        record.release_hold(later_hold, 6_000)?;
        record.release_hold(later_hold, 6_000)?;
        assert_eq!(record.effective_by_ms()?, acceptance.physical_by_ms + 5_000);
        assert_eq!(record.order.acceptance()?, acceptance);
        record.begin(READER_DRAIN_LIMIT_MS + 1_000)?;
        assert!(record.place_hold(Uuid::from_u128(7), READER_DRAIN_LIMIT_MS + 2_000).is_err());
        Ok(())
    }

    #[test]
    fn resumed_order_requires_original_drain_and_all_signed_dispositions() -> Result<()> {
        let signer = signer();
        let mut record = Record::new(order(&signer));
        let ordered_at_ms = READER_DRAIN_LIMIT_MS + record.order.committed_at_ms;
        record.begin(ordered_at_ms)?;
        let mut record: Record = serde_json::from_slice(&encode(&record)?).unwrap();
        let receipts: Vec<_> = record.order.obligations.iter().map(|&obligation| {
            let mut receipt = Settlement {
                obligation, observed_at_ms: ordered_at_ms + 1, disposition: [9; 32],
                signature: Vec::new(),
            };
            receipt.signature = signer.sign(&receipt.signing_bytes(&record.order, ordered_at_ms)?)
                .as_ref().to_vec();
            Ok(receipt)
        }).collect::<Result<_>>()?;
        assert!(record.join(receipts[1].clone(), ordered_at_ms + 1).is_err());
        for receipt in &receipts[..2] {
            record.join(receipt.clone(), ordered_at_ms + 1)?;
        }
        assert!(record.report(&signer)?.is_none());
        let mut wrong = receipts[2].clone();
        wrong.disposition = [8; 32];
        assert!(record.join(wrong, ordered_at_ms + 1).is_err());
        record.join(receipts[2].clone(), ordered_at_ms + 1)?;
        record.join(receipts[2].clone(), ordered_at_ms + 1)?;
        let signed = record.report(&signer)?.unwrap();
        assert!(signed.statement.met_deadline);
        assert_eq!(signed.statement.original_by_ms, record.order.acceptance()?.physical_by_ms);
        assert_eq!(signed.statement.destroyed_at_ms, ordered_at_ms + 1);
        assert!(UnparsedPublicKey::new(&ED25519, signer.public_key().as_ref())
            .verify(&signed.statement.signing_bytes()?, &signed.signature).is_ok());
        Ok(())
    }

    struct RetainedEvidence(BTreeMap<Uuid, Vec<u8>>);

    #[async_trait::async_trait]
    impl DeadlineJournal for RetainedEvidence {
        async fn read(&self, order: Uuid) -> Result<Option<(Vec<u8>, String)>> {
            Ok(self.0.get(&order).map(|bytes| (bytes.clone(), "retained".into())))
        }

        async fn compare_and_set(&self, _: Uuid, _: Option<&str>, _: Vec<u8>) -> Result<bool> {
            unreachable!("audit reads do not change custody")
        }

        async fn page(&self, _: Option<Uuid>) -> Result<Vec<Uuid>> {
            unreachable!("audit reads the exact signed cohort")
        }
    }

    #[tokio::test]
    async fn family_retirement_waits_for_drains_and_keeps_independent_denials() -> Result<()> {
        use std::{num::NonZeroU64, sync::Arc};

        use bytes::Bytes;
        use rtdb_storage::IoRegion;
        use rtdb_types::{AssertionId, Position};
        use crate::keys::{AssertionAuthoring, AssertionKeyService, FamilyOccurrence, KeyService,
            LandingReservation, LocalKeyService};

        struct LocalVault<'a>(&'a LocalKeyService, &'a Arc<IoRegion>);
        #[async_trait::async_trait]
        impl AssertionVaultRoutes for LocalVault<'_> {
            async fn retire_original(&self, stage: AssertionStage) -> Result<()> {
                self.0.retire_assertion_stage(stage, self.1).await
            }
        }

        let directory = tempfile::tempdir().unwrap();
        let vault = LocalKeyService::with_archive_broker(directory.path())?;
        let region = IoRegion::new();
        let local = LocalVault(&vault, &region);
        let families = vault.assertion_families(Arc::clone(&region)).unwrap();
        let family = Uuid::new_v4();
        let signer = signer();
        let custody_key = signer.public_key().as_ref().try_into().unwrap();
        let mut order = order(&signer);
        let stage = || AssertionStage::new(order.domain, NonZeroU64::MIN, Uuid::new_v4());
        let original = FamilyOccurrence {
            position: Position { line_generation: [3; 16], publication: 1, ordinal: NonZeroU64::MIN },
            assertion: AssertionId::from_bytes([3; 32]),
            capability: stage()?,
        };
        let pending = LandingReservation {
            source: original.position,
            destination: AssertionId::from_bytes([4; 32]),
            capability: stage()?,
            link_capability: stage()?,
            review_commitment: [5; 32],
        };
        families.register(family, original).await?;
        families.reserve_landing(family, pending).await?;
        let form = AssertionAuthoring { assertion_id: original.assertion, scope_commitment: [9; 32], codec: 1 };
        let body = Bytes::from_static(b"family original");
        let sealed = vault.seal_assertion_stage(original.capability, form, &region, body.clone()).await?;
        let fence = families.fence(family, original.position, order.id).await?;
        assert!(families.require_live_occurrence(family, original).await.is_err());
        order.obligations[1].inventory = fence.vault_inventory();
        let vault_obligation = order.obligations[1];
        let mut record = Record::new(order);
        let observed_at_ms = READER_DRAIN_LIMIT_MS + record.order.committed_at_ms;
        record.begin(observed_at_ms)?;
        let journal = RetainedEvidence(BTreeMap::from([(fence.order, encode(&record)?)]));
        let deadline = AssertionDeadline::new(&journal);
        assert!(deadline.retire_family_capabilities(&fence, vault_obligation, &local).await.is_err());
        assert!(families.settle(family, &deadline, &custody_key).await.is_err());

        let obligations = record.order.obligations.clone();
        let sign = |order: &ErasureOrder, obligation| -> Result<Settlement> {
            let mut settlement = Settlement {
                obligation, observed_at_ms, disposition: [6; 32], signature: Vec::new(),
            };
            settlement.signature = signer.sign(&settlement.signing_bytes(order, observed_at_ms)?)
                .as_ref().to_vec();
            Ok(settlement)
        };
        let drain = sign(&record.order, obligations[0])?;
        record.join(drain, observed_at_ms)?;
        let journal = RetainedEvidence(BTreeMap::from([(fence.order, encode(&record)?)]));
        let deadline = AssertionDeadline::new(&journal);
        deadline.retire_family_capabilities(&fence, vault_obligation, &local).await?;
        deadline.retire_family_capabilities(&fence, vault_obligation, &local).await?;
        assert!(vault.open_assertion_for_resolution(original.capability, form, &region, sealed).await.is_err());
        assert!(vault.seal_assertion_stage(pending.capability, form, &region, body.clone()).await.is_err());
        assert!(vault.seal_assertion_stage(pending.link_capability, form, &region, body).await.is_err());

        for obligation in obligations.into_iter().skip(1) {
            let settled = sign(&record.order, obligation)?;
            record.join(settled, observed_at_ms)?;
        }
        record.attestation = record.report(&signer)?;
        let journal = RetainedEvidence(BTreeMap::from([(fence.order, encode(&record)?)]));
        let deadline = AssertionDeadline::new(&journal);
        assert!(families.settle(family, &deadline, &[0; 32]).await.is_err());
        families.settle(family, &deadline, &custody_key).await?;
        families.settle(family, &deadline, &custody_key).await?;
        let reloaded = LocalKeyService::with_archive_broker(directory.path())?;
        let reloaded = reloaded.assertion_families(region).unwrap();
        assert!(reloaded.require_position_open(original.position).await.is_err());
        assert!(reloaded.register(Uuid::new_v4(), original).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn deadline_report_requires_every_member_of_the_signed_cohort() -> Result<()> {
        let source = Ed25519KeyPair::from_seed_unchecked(&[41; 32]).unwrap();
        let source_key = source.public_key().as_ref().try_into().unwrap();
        let custody = signer();
        let mut settled = Record::new(order(&custody));
        let ordered_at_ms = READER_DRAIN_LIMIT_MS + settled.order.committed_at_ms;
        settled.begin(ordered_at_ms)?;
        for obligation in settled.order.obligations.clone() {
            let mut receipt = Settlement {
                obligation, observed_at_ms: ordered_at_ms, disposition: [19; 32], signature: Vec::new(),
            };
            receipt.signature = custody.sign(&receipt.signing_bytes(&settled.order, ordered_at_ms)?)
                .as_ref().to_vec();
            settled.join(receipt, ordered_at_ms)?;
        }
        settled.attestation = settled.report(&custody)?;

        let mut later = order(&custody);
        later.id = Uuid::from_u128(7);
        let journal = RetainedEvidence(BTreeMap::from([
            (settled.order.id, encode(&settled)?),
            (later.id, encode(&Record::new(later.clone()))?),
        ]));
        let deadline = AssertionDeadline::new(&journal);
        let cohort = DeadlineCohort {
            domain: settled.order.domain, original_by_ms: settled.order.acceptance()?.physical_by_ms,
            orders: vec![settled.order.id, later.id],
        };
        let signature = source.sign(&cohort.signing_bytes()?).as_ref().to_vec();
        let mut selection = SignedDeadlineCohort { cohort, signature };
        assert!(deadline.attest_deadline(selection.clone(), &source_key, &custody).await?.is_none());
        selection.cohort.orders.pop();
        assert!(deadline.attest_deadline(selection, &source_key, &custody).await.is_err());
        Ok(())
    }

}
