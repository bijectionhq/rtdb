//! Current authority for reserving assertion occurrences before source append.

use std::{collections::BTreeMap, num::NonZeroU64, sync::Arc};

use bytes::Bytes;
use ring::{digest, hmac};
use rtdb_storage::IoRegion;
use rtdb_types::{Anomaly, AnomalyCategory, AssertionId, PlacedAttemptScope, Result, Timestamp};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::family::{AssertionVaultJournal, AssertionVaultRecord, MAX_RECORD_BYTES};
use crate::keys::{AssertionMintKey, new_key};

const MAX_EFFECTS: usize = 4096;
const MAX_TICKET_MICROS: i64 = 3_600_000_000;
const CAS_ATTEMPTS: usize = 8;
const TICKET_BYTES: usize = 64;

#[derive(Clone, Copy)]
pub struct AssertionIssuerScope {
    history: [u8; 32],
    disclosure: [u8; 32],
}

impl AssertionIssuerScope {
    pub fn new(writer: PlacedAttemptScope, disclosure: [u8; 32]) -> Self {
        Self {
            history: writer.history_fingerprint(),
            disclosure,
        }
    }

    pub(super) fn address(self) -> digest::Digest {
        let mut hash = digest::Context::new(&digest::SHA256);
        hash.update(b"rtdb/assertion-issuer/v1\0");
        hash.update(&self.history);
        hash.update(&self.disclosure);
        hash.finish()
    }
}

/// A receiver-authenticated retry handle. Its bytes are opaque to the author.
pub struct AssertionIngressTicket([u8; TICKET_BYTES]);

impl AssertionIngressTicket {
    const CLAIM_BYTES: usize = 32;
    pub const WIDTH: usize = TICKET_BYTES;

    pub fn to_bytes(&self) -> [u8; Self::WIDTH] {
        self.0
    }

    pub fn from_bytes(bytes: [u8; Self::WIDTH]) -> Self {
        Self(bytes)
    }

    fn issue(epoch: NonZeroU64, until: Timestamp, key: &hmac::Key) -> Self {
        let mut bytes = [0; Self::WIDTH];
        bytes[..16].copy_from_slice(Uuid::new_v4().as_bytes());
        bytes[16..24].copy_from_slice(&epoch.get().to_be_bytes());
        bytes[24..32].copy_from_slice(&until.0.to_be_bytes());
        let mac = hmac::sign(key, &bytes[..Self::CLAIM_BYTES]);
        bytes[Self::CLAIM_BYTES..].copy_from_slice(mac.as_ref());
        Self(bytes)
    }

    fn open(&self, key: &hmac::Key) -> Result<(Uuid, u64, Timestamp)> {
        let (claim, mac) = self.0.split_at(Self::CLAIM_BYTES);
        hmac::verify(key, claim, mac).map_err(|_| refused())?;
        let attempt = Uuid::from_bytes(self.0[..16].try_into().expect("ticket attempt width"));
        let epoch = u64::from_be_bytes(self.0[16..24].try_into().expect("ticket epoch width"));
        let until = i64::from_be_bytes(self.0[24..32].try_into().expect("ticket cutoff width"));
        Ok((attempt, epoch, Timestamp(until)))
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Reservation {
    commitment: [u8; 32],
    first: u128,
    effects: usize,
    until: i64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Issuer {
    namespace: Uuid,
    mint_key: [u8; 32],
    ticket_key: [u8; 32],
    writer: NonZeroU64,
    next: u128,
    observed: i64,
    reservations: BTreeMap<Uuid, Reservation>,
}

impl Issuer {
    fn advance_time(&mut self, observed: i64) {
        self.observed = observed;
        self.reservations.retain(|_, prior| prior.until > observed);
    }

    fn attempt_key(&self, scope: AssertionIssuerScope, principal: &str) -> hmac::Key {
        let key = hmac::Key::new(hmac::HMAC_SHA256, &self.ticket_key);
        let mut binding = hmac::Context::with_key(&key);
        binding.update(b"rtdb/assertion-ingress-ticket/v1\0");
        binding.update(scope.address().as_ref());
        binding.update(principal.as_bytes());
        hmac::Key::new(hmac::HMAC_SHA256, binding.sign().as_ref())
    }

    fn ids(&self, reservation: &Reservation) -> Result<Vec<AssertionId>> {
        let end = reservation
            .first
            .checked_add(reservation.effects as u128)
            .ok_or_else(invalid_record)?;
        if reservation.effects == 0 || reservation.effects > MAX_EFFECTS || end > self.next {
            return Err(invalid_record());
        }
        let key = AssertionMintKey::new(*self.namespace.as_bytes(), self.mint_key);
        (reservation.first..end)
            .map(|counter| key.id_for_counter(counter))
            .collect()
    }
}

/// Only the original authority installs writers and issues tickets. The source
/// append owner must independently enforce the installed writer fence.
pub struct AssertionIssuers<'a> {
    journal: &'a dyn AssertionVaultJournal,
    region: Arc<IoRegion>,
}

impl<'a> AssertionIssuers<'a> {
    pub(in crate::keys) fn new(
        journal: &'a dyn AssertionVaultJournal,
        region: Arc<IoRegion>,
    ) -> Self {
        Self { journal, region }
    }

    /// Install a registered namespace, or advance the same issuer after the
    /// installed writer authority fences its exact predecessor.
    pub async fn install_writer(
        &self,
        scope: AssertionIssuerScope,
        namespace: Uuid,
        writer: PlacedAttemptScope,
        expected: Option<NonZeroU64>,
    ) -> Result<()> {
        let epoch = writer_epoch(scope, writer)?;
        if namespace.is_nil() || expected.is_some_and(|prior| prior >= epoch) {
            return Err(refused());
        }
        for _ in 0..CAS_ATTEMPTS {
            let (record, previous) = match self.read(scope).await? {
                Some((mut record, etag)) => {
                    if record.namespace != namespace {
                        return Err(refused());
                    }
                    if record.writer == epoch {
                        return Ok(());
                    }
                    if expected != Some(record.writer) {
                        return Err(refused());
                    }
                    record.writer = epoch;
                    (record, Some(etag))
                }
                None if expected.is_none() => (
                    Issuer {
                        namespace,
                        mint_key: new_key()?,
                        ticket_key: new_key()?,
                        writer: epoch,
                        next: 0,
                        observed: i64::MIN,
                        reservations: BTreeMap::new(),
                    },
                    None,
                ),
                None => return Err(refused()),
            };
            if self.write(scope, &record, previous.as_deref()).await? {
                return Ok(());
            }
        }
        Err(contention())
    }

    pub async fn issue_ticket(
        &self,
        scope: AssertionIssuerScope,
        writer: PlacedAttemptScope,
        principal: &str,
        now: Timestamp,
        until: Timestamp,
    ) -> Result<AssertionIngressTicket> {
        let epoch = writer_epoch(scope, writer)?;
        let (record, _) = self.read(scope).await?.ok_or_else(refused)?;
        if record.writer != epoch
            || principal.is_empty()
            || until <= now
            || until.0 <= record.observed
            || until.0.saturating_sub(now.0) > MAX_TICKET_MICROS
        {
            return Err(refused());
        }
        Ok(AssertionIngressTicket::issue(
            epoch,
            until,
            &record.attempt_key(scope, principal),
        ))
    }

    /// One conditional update reserves every dense effect slot and burns its
    /// counter. A slot commitment must cover the canonical authored candidate.
    pub async fn reserve(
        &self,
        scope: AssertionIssuerScope,
        writer: PlacedAttemptScope,
        principal: &str,
        ticket: &AssertionIngressTicket,
        now: Timestamp,
        candidates: &[[u8; 32]],
    ) -> Result<Vec<AssertionId>> {
        let epoch = writer_epoch(scope, writer)?;
        if principal.is_empty() || !(1..=MAX_EFFECTS).contains(&candidates.len()) {
            return Err(refused());
        }
        let mut hash = digest::Context::new(&digest::SHA256);
        hash.update(b"rtdb/assertion-ingress-candidates/v1\0");
        for candidate in candidates {
            hash.update(candidate);
        }
        let commitment = hash.finish().as_ref().try_into().expect("SHA-256 width");
        for _ in 0..CAS_ATTEMPTS {
            let (mut record, etag) = self.read(scope).await?.ok_or_else(refused)?;
            let (attempt, admitted_writer, until) =
                ticket.open(&record.attempt_key(scope, principal))?;
            let observed = record.observed.max(now.0);
            if record.writer != epoch {
                return Err(refused());
            }
            if until.0 <= observed {
                if observed == record.observed {
                    return Err(refused());
                }
                record.advance_time(observed);
                if self.write(scope, &record, Some(&etag)).await? {
                    return Err(refused());
                }
                continue;
            }
            if let Some(prior) = record.reservations.get(&attempt) {
                if prior.commitment != commitment
                    || prior.effects != candidates.len()
                    || prior.until != until.0
                {
                    return Err(refused());
                }
                return record.ids(prior);
            }
            if admitted_writer != epoch.get() {
                return Err(refused());
            }
            let reservation = Reservation {
                commitment,
                first: record.next,
                effects: candidates.len(),
                until: until.0,
            };
            record.next = record
                .next
                .checked_add(candidates.len() as u128)
                .ok_or_else(exhausted)?;
            let ids = record.ids(&reservation)?;
            record.advance_time(observed);
            record.reservations.insert(attempt, reservation);
            if self.write(scope, &record, Some(&etag)).await? {
                return Ok(ids);
            }
        }
        Err(contention())
    }

    async fn read(&self, scope: AssertionIssuerScope) -> Result<Option<(Issuer, String)>> {
        self.journal
            .read_record(AssertionVaultRecord::Issuer(scope), &self.region)
            .await?
            .map(|(bytes, etag)| {
                Ok((
                    serde_json::from_slice(&bytes).map_err(|_| invalid_record())?,
                    etag,
                ))
            })
            .transpose()
    }

    async fn write(
        &self,
        scope: AssertionIssuerScope,
        issuer: &Issuer,
        previous: Option<&str>,
    ) -> Result<bool> {
        let bytes = serde_json::to_vec(issuer).map_err(|_| invalid_record())?;
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(exhausted());
        }
        self.journal
            .write_record(
                AssertionVaultRecord::Issuer(scope),
                &self.region,
                previous,
                Bytes::from(bytes),
            )
            .await
    }
}

fn writer_epoch(scope: AssertionIssuerScope, writer: PlacedAttemptScope) -> Result<NonZeroU64> {
    if scope.history != writer.history_fingerprint() {
        return Err(refused());
    }
    NonZeroU64::new(writer.writer_epoch()).ok_or_else(refused)
}

fn refused() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict,
        "rtdb.multi/assertion-ingress-refused",
        "The assertion attempt is unavailable",
    )
}

fn exhausted() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb.multi/assertion-issuer-exhausted",
        "The assertion issuer cannot reserve more effects",
    )
}

fn contention() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict,
        "rtdb.multi/assertion-issuer-contention",
        "The original assertion issuer changed concurrently",
    )
}

fn invalid_record() -> Anomaly {
    Anomaly::fault(
        "rtdb.multi/assertion-issuer-record",
        "Invalid current assertion issuer record",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::{KeyService, LocalKeyService};

    #[tokio::test]
    async fn slots_survive_retry_and_writer_succession() -> Result<()> {
        let directory = tempfile::tempdir().unwrap();
        let vault = LocalKeyService::with_archive_broker(directory.path())?;
        let region = IoRegion::new();
        let first = PlacedAttemptScope::new("original", 1)?;
        let second = PlacedAttemptScope::new("original", 2)?;
        let scope = AssertionIssuerScope::new(first, [2; 32]);
        let namespace = Uuid::new_v4();
        let issuer = vault.assertion_issuers(Arc::clone(&region)).unwrap();
        issuer.install_writer(scope, namespace, first, None).await?;

        let candidates = [[3; 32], [4; 32]];
        let issue = || issuer.issue_ticket(scope, first, "owner", Timestamp(10), Timestamp(100));
        let original = issue().await?;
        let unused = issue().await?;
        let left = issuer.reserve(scope, first, "owner", &original, Timestamp(11), &candidates);
        let right = issuer.reserve(scope, first, "owner", &original, Timestamp(11), &candidates);
        let (left, right) = tokio::join!(left, right);
        let ids = left?;
        assert_eq!(right?, ids);
        assert_ne!(ids[0], ids[1]);
        assert_eq!(&ids[0].to_bytes()[..16], namespace.as_bytes());
        assert!(
            issuer
                .reserve(
                    scope,
                    first,
                    "owner",
                    &original,
                    Timestamp(11),
                    &[[4; 32], [3; 32]]
                )
                .await
                .is_err()
        );
        assert!(
            issuer
                .reserve(scope, first, "other", &original, Timestamp(11), &candidates)
                .await
                .is_err()
        );

        let reopened = LocalKeyService::with_archive_broker(directory.path())?;
        let issuer = reopened.assertion_issuers(region).unwrap();
        assert!(
            issuer
                .install_writer(scope, namespace, second, None)
                .await
                .is_err()
        );
        issuer
            .install_writer(scope, namespace, second, Some(NonZeroU64::MIN))
            .await?;
        assert!(
            issuer
                .reserve(scope, first, "owner", &original, Timestamp(12), &candidates)
                .await
                .is_err()
        );
        let retry = AssertionIngressTicket::from_bytes(original.to_bytes());
        assert_eq!(
            issuer
                .reserve(scope, second, "owner", &retry, Timestamp(12), &candidates)
                .await?,
            ids
        );
        assert!(
            issuer
                .reserve(scope, second, "owner", &unused, Timestamp(12), &candidates)
                .await
                .is_err()
        );

        let mut tampered = original.to_bytes();
        tampered[31] ^= 1;
        let tampered = AssertionIngressTicket::from_bytes(tampered);
        assert!(
            issuer
                .reserve(
                    scope,
                    second,
                    "owner",
                    &tampered,
                    Timestamp(12),
                    &candidates
                )
                .await
                .is_err()
        );
        assert!(
            issuer
                .reserve(scope, second, "owner", &retry, Timestamp(100), &candidates)
                .await
                .is_err()
        );
        assert!(
            issuer
                .reserve(scope, second, "owner", &retry, Timestamp(99), &candidates)
                .await
                .is_err()
        );
        let landing = issuer
            .issue_ticket(scope, second, "owner", Timestamp(101), Timestamp(200))
            .await?;
        let landed = issuer
            .reserve(
                scope,
                second,
                "owner",
                &landing,
                Timestamp(101),
                &candidates,
            )
            .await?;
        assert!(landed.iter().all(|id| !ids.contains(id)));
        Ok(())
    }
}
