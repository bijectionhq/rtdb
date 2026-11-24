use std::sync::Arc;

use aes_siv::{KeyInit, siv::Aes256Siv};
use async_trait::async_trait;
use ring::{hkdf, rand::{SecureRandom, SystemRandom}};
use rtdb_types::{
    Anomaly, AnomalyCategory, Result, Timestamp,
    rtdb_v1::{
        Basis, Claim, ClaimEvidence, ClaimScope, ClaimVerifier, LabelScope, Layer, MAIN_BRANCH,
        MAX_CLAIM_PAYLOAD_BYTES, valid_branch_name,
    },
};
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::{DataKey, Generation, Kms, aead};

const FORMAT: &[u8] = b"RTDBT\x01";
const WRAPPING_CONTEXT: &[u8] = b"rtdb.v1/token-key/1";
const DERIVATION_CONTEXT: &[u8] = b"rtdb.v1/aes-siv-512/1";
const TAG_BYTES: usize = 16;
const HEADER_BYTES: usize = FORMAT.len() + 16 + 8 + 1;
const MAX_WRAPPED_KEY_BYTES: usize = 64 * 1024;

#[derive(Clone)]
pub struct TokenKey {
    generation: Generation,
    wrapped: Vec<u8>,
}

impl TokenKey {
    pub async fn create(kms: &dyn Kms, generation: Generation) -> Result<Self> {
        kms.create(generation).await?;
        let key = aead::random_key()?;
        let wrapped = kms
            .wrap(generation, &key, &wrapping_context(generation))
            .await?;
        if wrapped.is_empty() || wrapped.len() > MAX_WRAPPED_KEY_BYTES {
            return Err(invalid_metadata());
        }
        Ok(Self {
            generation,
            wrapped,
        })
    }

    pub fn generation(&self) -> Generation {
        self.generation
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = FORMAT.to_vec();
        bytes.extend_from_slice(self.generation.database.as_bytes());
        bytes.extend_from_slice(&self.generation.number.to_be_bytes());
        bytes.extend_from_slice(&self.wrapped);
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let bytes = bytes.strip_prefix(FORMAT).ok_or_else(invalid_metadata)?;
        let (database, bytes) = bytes
            .split_first_chunk::<16>()
            .ok_or_else(invalid_metadata)?;
        let (number, wrapped) = bytes
            .split_first_chunk::<8>()
            .ok_or_else(invalid_metadata)?;
        if wrapped.is_empty() || wrapped.len() > MAX_WRAPPED_KEY_BYTES {
            return Err(invalid_metadata());
        }
        let generation = Generation {
            database: Uuid::from_bytes(*database),
            number: u64::from_be_bytes(*number),
        };
        Ok(Self {
            generation,
            wrapped: wrapped.to_vec(),
        })
    }
}

fn wrapping_context(generation: Generation) -> Vec<u8> {
    let mut context = WRAPPING_CONTEXT.to_vec();
    context.extend_from_slice(generation.database.as_bytes());
    context.extend_from_slice(&generation.number.to_be_bytes());
    context
}

#[async_trait]
pub trait TokenKeys: Send + Sync {
    async fn current(&self) -> Result<TokenKey>;
    async fn get(&self, number: u64) -> Result<Option<TokenKey>>;
}

#[derive(Clone)]
pub struct TokenAuthority {
    incarnation: Uuid,
    kms: Arc<dyn Kms>,
    keys: Arc<dyn TokenKeys>,
}

pub struct TokenIssuer {
    authority: TokenAuthority,
    key: TokenKey,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PageToken {
    pub basis: Basis,
    pub clock: Timestamp,
    pub offset: u64,
    pub request: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExportTicket {
    pub pin: Uuid,
    pub basis: Basis,
    pub manifest_sha256: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReceiverClockObservation {
    pub basis: Basis,
    pub system_time: Timestamp,
    pub observed_at: Timestamp,
    pub request: [u8; 32],
    pub session: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CustodyReadProof {
    pub basis: Basis,
    pub key_generation: Option<u64>,
    pub expires_at: Timestamp,
    pub contributors: Vec<([u8; 16], [u8; 32])>,
}

const CUSTODY_READ_HEADER: usize = Basis::WIDTH + 1 + 8 + 8 + 32 + 32;
const MAX_CUSTODY_CONTRIBUTORS: usize = 512;

const PAGE_BYTES: usize = Basis::WIDTH + 8 + 8 + 32;
const EXPORT_BYTES: usize = Basis::WIDTH + 16 + 32;
const OBSERVATION_BYTES: usize = Basis::WIDTH + 16 + 64;
const SCOPED_CLAIM_DOMAIN: &[u8; 8] = b"scoped\0\x01";

#[derive(Clone, Copy)]
enum Purpose {
    Claim = 1,
    Page = 2,
    Export = 3,
    ReceiverClock = 4,
    CustodyRead = 5,
}

impl Purpose {
    fn invalid(self) -> Anomaly {
        match self {
            Self::Claim => Anomaly::incorrect("rtdb/invalid-claim", "Invalid query claim"),
            Self::Page => Anomaly::new(
                AnomalyCategory::NotFound,
                "rtdb/cursor-expired",
                "Resume from a fresh query",
            ),
            Self::Export => Anomaly::incorrect("rtdb/invalid-export", "Invalid checkpoint export ticket"),
            Self::ReceiverClock => Anomaly::incorrect("rtdb/invalid-receiver-observation", "Invalid receiver clock observation"),
            Self::CustodyRead => Anomaly::incorrect("rtdb/invalid-custody-read", "Invalid custody read proof"),
        }
    }

    fn max_payload(self) -> usize {
        match self {
            Self::Claim => MAX_CLAIM_PAYLOAD_BYTES,
            Self::Page => PAGE_BYTES,
            Self::Export => EXPORT_BYTES,
            Self::ReceiverClock => OBSERVATION_BYTES,
            Self::CustodyRead => CUSTODY_READ_HEADER + 48 * MAX_CUSTODY_CONTRIBUTORS,
        }
    }
}

impl TokenAuthority {
    pub fn custody_vault(&self) -> crate::CustodyVault {
        crate::CustodyVault::new(self.incarnation, Arc::clone(&self.kms))
    }

    pub fn new(incarnation: Uuid, kms: Arc<dyn Kms>, keys: Arc<dyn TokenKeys>) -> Self {
        Self {
            incarnation,
            kms,
            keys,
        }
    }

    pub fn incarnation(&self) -> Uuid {
        self.incarnation
    }

    pub async fn seal_claim(&self, evidence: &ClaimEvidence) -> Result<Claim> {
        self.issuer().await?.seal_claim(evidence).await
    }

    pub async fn open_claim(&self, claim: &Claim) -> Result<ClaimEvidence> {
        let (bytes, _) = self.open(Purpose::Claim, claim.as_bytes()).await?;
        let evidence: ClaimEvidence =
            serde_json::from_slice(&bytes).map_err(|_| Purpose::Claim.invalid())?;
        if evidence.scope.basis.incarnation() != *self.incarnation.as_bytes()
            || !valid_branch_name(&evidence.scope.branch)
        {
            return Err(Purpose::Claim.invalid());
        }
        Ok(evidence)
    }

    pub async fn verify_claims<'a>(
        &self,
        claims: impl IntoIterator<Item = &'a Claim>,
    ) -> VerifiedClaims {
        self.verify_claims_in(claims, None).await
    }

    pub async fn verify_claims_in<'a>(
        &self, claims: impl IntoIterator<Item = &'a Claim>, scope: Option<&LabelScope>,
    ) -> VerifiedClaims {
        let mut verified = Vec::new();
        for claim in claims {
            if !verified.iter().any(|(known, _)| known == claim) {
                let evidence = match scope {
                    Some(scope) => self.open_scoped_claim(claim, scope).await,
                    None => self.open_claim(claim).await,
                };
                verified.push((claim.clone(), evidence));
            }
        }
        VerifiedClaims(verified)
    }

    pub async fn open_scoped_claim(&self, claim: &Claim, scope: &LabelScope) -> Result<ClaimEvidence> {
        let (payload, _) = self.open(Purpose::Claim, claim.as_bytes()).await?;
        let payload = payload.strip_prefix(SCOPED_CLAIM_DOMAIN).ok_or_else(|| Purpose::Claim.invalid())?;
        let (basis, rest) = payload.split_first_chunk::<{ Basis::WIDTH }>().ok_or_else(|| Purpose::Claim.invalid())?;
        let (namespace, nonce) = rest.split_first_chunk::<16>().ok_or_else(|| Purpose::Claim.invalid())?;
        let basis = Basis::from_bytes(basis).map_err(|_| Purpose::Claim.invalid())?;
        if nonce.len() != 16 || basis.incarnation() != *self.incarnation.as_bytes()
            || *namespace != scope.main_read_incarnation(*self.incarnation.as_bytes())
        { return Err(Purpose::Claim.invalid()); }
        Ok(ClaimEvidence { scope: ClaimScope { basis, branch: MAIN_BRANCH.into(), layer: Layer::Merged }, footprint: Default::default() })
    }

    pub async fn seal_page(&self, page: &PageToken) -> Result<Vec<u8>> {
        self.issuer().await?.seal_page(page).await
    }

    pub async fn open_page(&self, token: &[u8]) -> Result<PageToken> {
        self.resume_page(token).await.map(|(page, _)| page)
    }

    pub async fn seal_export(&self, ticket: ExportTicket) -> Result<Vec<u8>> {
        if ticket.pin.is_nil() || ticket.basis.incarnation() != *self.incarnation.as_bytes() {
            return Err(Purpose::Export.invalid());
        }
        let issuer = self.issuer().await?;
        let mut bytes = Vec::with_capacity(EXPORT_BYTES);
        bytes.extend_from_slice(ticket.pin.as_bytes());
        bytes.extend_from_slice(&ticket.basis.to_bytes());
        bytes.extend_from_slice(&ticket.manifest_sha256);
        self.seal(&issuer.key, Purpose::Export, &bytes).await
    }

    pub async fn open_export(&self, token: &[u8]) -> Result<ExportTicket> {
        let (bytes, _) = self.open(Purpose::Export, token).await?;
        let invalid = || Purpose::Export.invalid();
        let (pin, bytes) = bytes.split_first_chunk::<16>().ok_or_else(invalid)?;
        let (basis, digest) = bytes.split_first_chunk::<{ Basis::WIDTH }>().ok_or_else(invalid)?;
        let pin = Uuid::from_bytes(*pin);
        let basis = Basis::from_bytes(basis).map_err(|_| invalid())?;
        if pin.is_nil() || basis.incarnation() != *self.incarnation.as_bytes() {
            return Err(invalid());
        }
        Ok(ExportTicket { pin, basis, manifest_sha256: digest.try_into().map_err(|_| invalid())? })
    }

    pub async fn open_receiver_clock(&self, token: &[u8], session: [u8; 32]) -> Result<ReceiverClockObservation> {
        let (bytes, _) = self.open(Purpose::ReceiverClock, token).await?;
        let invalid = || Purpose::ReceiverClock.invalid();
        let (basis, bytes) = bytes.split_first_chunk::<{ Basis::WIDTH }>().ok_or_else(invalid)?;
        let (system_time, bytes) = bytes.split_first_chunk::<8>().ok_or_else(invalid)?;
        let (observed_at, bytes) = bytes.split_first_chunk::<8>().ok_or_else(invalid)?;
        let (request, bound_session) = bytes.split_first_chunk::<32>().ok_or_else(invalid)?;
        let basis = Basis::from_bytes(basis).map_err(|_| invalid())?;
        if basis.incarnation() != *self.incarnation.as_bytes() || bound_session != session.as_slice() {
            return Err(invalid());
        }
        Ok(ReceiverClockObservation {
            basis, system_time: Timestamp(i64::from_be_bytes(*system_time)),
            observed_at: Timestamp(i64::from_be_bytes(*observed_at)),
            request: *request, session,
        })
    }

    pub async fn open_custody_read(&self, token: &[u8], owner: [u8; 32], session: [u8; 32]) -> Result<CustodyReadProof> {
        let (bytes, _) = self.open(Purpose::CustodyRead, token).await?;
        let invalid = || Purpose::CustodyRead.invalid();
        let (basis, bytes) = bytes.split_first_chunk::<{ Basis::WIDTH }>().ok_or_else(invalid)?;
        let (tag, bytes) = bytes.split_first().ok_or_else(invalid)?;
        let (generation, bytes) = bytes.split_first_chunk::<8>().ok_or_else(invalid)?;
        let (expiry, bytes) = bytes.split_first_chunk::<8>().ok_or_else(invalid)?;
        let (bound_owner, bytes) = bytes.split_first_chunk::<32>().ok_or_else(invalid)?;
        let (bound_session, bytes) = bytes.split_first_chunk::<32>().ok_or_else(invalid)?;
        let basis = Basis::from_bytes(basis).map_err(|_| invalid())?;
        if basis.incarnation() != *self.incarnation.as_bytes() || *bound_owner != owner || *bound_session != session
            || bytes.len() % 48 != 0 || bytes.len() / 48 > MAX_CUSTODY_CONTRIBUTORS
        { return Err(invalid()); }
        let key_generation = match (*tag, u64::from_be_bytes(*generation)) {
            (0, 0) => None,
            (1, generation) => Some(generation),
            _ => return Err(invalid()),
        };
        let contributors = bytes.chunks_exact(48).map(|entry| {
            (entry[..16].try_into().expect("group width"), entry[16..].try_into().expect("occurrence width"))
        }).collect::<Vec<([u8; 16], [u8; 32])>>();
        if contributors.windows(2).any(|pair| pair[0] >= pair[1])
            || contributors.iter().any(|(group, occurrence)| *group == [0; 16] || *occurrence == [0; 32])
        { return Err(invalid()); }
        Ok(CustodyReadProof { basis, key_generation, expires_at: Timestamp(i64::from_be_bytes(*expiry)), contributors })
    }

    pub async fn issuer(&self) -> Result<TokenIssuer> {
        let key = self.keys.current().await?;
        if key.generation.database != self.incarnation {
            return Err(invalid_metadata());
        }
        Ok(TokenIssuer { authority: self.clone(), key })
    }

    pub async fn resume_page(&self, token: &[u8]) -> Result<(PageToken, TokenIssuer)> {
        self.resume_page_in(token, None).await
    }

    pub async fn resume_page_in(&self, token: &[u8], scope: Option<&LabelScope>) -> Result<(PageToken, TokenIssuer)> {
        let (bytes, key) = self.open(Purpose::Page, token).await?;
        let invalid = || Purpose::Page.invalid();
        let (basis, bytes) = bytes
            .split_first_chunk::<{ Basis::WIDTH }>()
            .ok_or_else(invalid)?;
        let basis = Basis::from_bytes(basis).map_err(|_| invalid())?;
        let (clock, bytes) = bytes.split_first_chunk::<8>().ok_or_else(invalid)?;
        let (offset, request) = bytes.split_first_chunk::<8>().ok_or_else(invalid)?;
        let incarnation = *self.incarnation.as_bytes();
        if basis.incarnation() != scope.map_or(incarnation, |scope| scope.main_read_incarnation(incarnation)) {
            return Err(invalid());
        }
        let page = PageToken {
            basis,
            clock: Timestamp(i64::from_be_bytes(*clock)),
            offset: u64::from_be_bytes(*offset),
            request: request.try_into().map_err(|_| invalid())?,
        };
        Ok((page, TokenIssuer { authority: self.clone(), key }))
    }

    pub async fn check_claim_readable(&self, claim: &Claim) -> Result<()> {
        self.check_readable(Purpose::Claim, claim.as_bytes()).await
    }

    pub async fn check_page_readable(&self, token: &[u8]) -> Result<()> {
        self.check_readable(Purpose::Page, token).await
    }

    async fn check_readable(&self, purpose: Purpose, token: &[u8]) -> Result<()> {
        let (_, number, _) = self.split(purpose, token)?;
        self.kms
            .check_readable(Generation {
                database: self.incarnation,
                number,
            })
            .await
    }

    async fn seal(&self, key: &TokenKey, purpose: Purpose, plaintext: &[u8]) -> Result<Vec<u8>> {
        if plaintext.len() > purpose.max_payload() {
            return Err(purpose.invalid());
        }
        let generation = key.generation;
        let material = self.material(key).await?;
        let mut header = FORMAT.to_vec();
        header.extend_from_slice(generation.database.as_bytes());
        header.extend_from_slice(&generation.number.to_be_bytes());
        header.push(purpose as u8);
        let ciphertext = siv(&material)?
            .encrypt([header.as_slice()], plaintext)
            .map_err(|_| purpose.invalid())?;
        self.kms.check_readable(generation).await?;
        header.extend_from_slice(&ciphertext);
        Ok(header)
    }

    async fn open(&self, purpose: Purpose, token: &[u8]) -> Result<(Zeroizing<Vec<u8>>, TokenKey)> {
        let (header, number, ciphertext) = self.split(purpose, token)?;
        let key = self
            .keys
            .get(number)
            .await?
            .ok_or_else(|| purpose.invalid())?;
        let generation = Generation {
            database: self.incarnation,
            number,
        };
        if key.generation != generation {
            return Err(invalid_metadata());
        }
        let material = self.material(&key).await?;
        let bytes = Zeroizing::new(
            siv(&material)?
                .decrypt([header], ciphertext)
                .map_err(|_| purpose.invalid())?,
        );
        self.kms.check_readable(generation).await?;
        Ok((bytes, key))
    }

    fn split<'a>(&self, purpose: Purpose, token: &'a [u8]) -> Result<(&'a [u8], u64, &'a [u8])> {
        let invalid = || purpose.invalid();
        let (header, ciphertext) = token.split_at_checked(HEADER_BYTES).ok_or_else(invalid)?;
        if !(TAG_BYTES..=TAG_BYTES + purpose.max_payload()).contains(&ciphertext.len()) {
            return Err(invalid());
        }
        let rest = header.strip_prefix(FORMAT).ok_or_else(invalid)?;
        let (database, rest) = rest.split_first_chunk::<16>().ok_or_else(invalid)?;
        let (number, kind) = rest.split_first_chunk::<8>().ok_or_else(invalid)?;
        if database != self.incarnation.as_bytes() || kind != [purpose as u8] {
            return Err(invalid());
        }
        Ok((header, u64::from_be_bytes(*number), ciphertext))
    }

    async fn material(&self, key: &TokenKey) -> Result<DataKey> {
        self.kms
            .unwrap(
                key.generation,
                &key.wrapped,
                &wrapping_context(key.generation),
            )
            .await
    }
}

impl TokenIssuer {
    pub async fn seal_custody_read(&self, proof: &CustodyReadProof, owner: [u8; 32], session: [u8; 32]) -> Result<Vec<u8>> {
        if proof.basis.incarnation() != *self.authority.incarnation.as_bytes() || proof.expires_at.0 <= 0
            || proof.contributors.len() > MAX_CUSTODY_CONTRIBUTORS
            || proof.contributors.windows(2).any(|pair| pair[0] >= pair[1])
            || proof.contributors.iter().any(|(group, occurrence)| *group == [0; 16] || *occurrence == [0; 32])
        { return Err(Purpose::CustodyRead.invalid()); }
        let mut bytes = Vec::with_capacity(CUSTODY_READ_HEADER + 48 * proof.contributors.len());
        bytes.extend_from_slice(&proof.basis.to_bytes());
        bytes.push(u8::from(proof.key_generation.is_some()));
        bytes.extend_from_slice(&proof.key_generation.unwrap_or(0).to_be_bytes());
        bytes.extend_from_slice(&proof.expires_at.0.to_be_bytes());
        bytes.extend_from_slice(&owner);
        bytes.extend_from_slice(&session);
        for (group, occurrence) in &proof.contributors {
            bytes.extend_from_slice(group);
            bytes.extend_from_slice(occurrence);
        }
        self.authority.seal(&self.key, Purpose::CustodyRead, &bytes).await
    }

    pub async fn seal_receiver_clock(&self, observation: ReceiverClockObservation) -> Result<Vec<u8>> {
        if observation.basis.incarnation() != *self.authority.incarnation.as_bytes() {
            return Err(Purpose::ReceiverClock.invalid());
        }
        let mut bytes = Vec::with_capacity(OBSERVATION_BYTES);
        bytes.extend_from_slice(&observation.basis.to_bytes());
        bytes.extend_from_slice(&observation.system_time.0.to_be_bytes());
        bytes.extend_from_slice(&observation.observed_at.0.to_be_bytes());
        bytes.extend_from_slice(&observation.request);
        bytes.extend_from_slice(&observation.session);
        self.authority.seal(&self.key, Purpose::ReceiverClock, &bytes).await
    }

    pub async fn seal_claim(&self, evidence: &ClaimEvidence) -> Result<Claim> {
        if evidence.scope.basis.incarnation() != *self.authority.incarnation.as_bytes()
            || !valid_branch_name(&evidence.scope.branch)
        {
            return Err(Purpose::Claim.invalid());
        }
        let bytes =
            Zeroizing::new(serde_json::to_vec(evidence).map_err(|_| Purpose::Claim.invalid())?);
        Ok(Claim::from_bytes(self.authority.seal(&self.key, Purpose::Claim, &bytes).await?))
    }

    pub async fn seal_page(&self, page: &PageToken) -> Result<Vec<u8>> {
        self.seal_page_in(page, None).await
    }

    pub async fn seal_scoped_claim(&self, basis: Basis, scope: &LabelScope) -> Result<Claim> {
        let incarnation = *self.authority.incarnation.as_bytes();
        if basis.incarnation() != incarnation { return Err(Purpose::Claim.invalid()); }
        let mut nonce = [0; 16];
        SystemRandom::new().fill(&mut nonce).map_err(|_| Anomaly::fault("rtdb/claim-randomness", "Could not generate a claim nonce"))?;
        let mut bytes = Zeroizing::new(SCOPED_CLAIM_DOMAIN.to_vec());
        bytes.extend_from_slice(&basis.to_bytes());
        bytes.extend_from_slice(&scope.main_read_incarnation(incarnation));
        bytes.extend_from_slice(&nonce);
        Ok(Claim::from_bytes(self.authority.seal(&self.key, Purpose::Claim, &bytes).await?))
    }

    pub async fn seal_page_in(&self, page: &PageToken, scope: Option<&LabelScope>) -> Result<Vec<u8>> {
        let incarnation = *self.authority.incarnation.as_bytes();
        if page.basis.incarnation() != scope.map_or(incarnation, |scope| scope.main_read_incarnation(incarnation)) {
            return Err(Purpose::Page.invalid());
        }
        let mut bytes = page.basis.to_bytes().to_vec();
        bytes.extend_from_slice(&page.clock.0.to_be_bytes());
        bytes.extend_from_slice(&page.offset.to_be_bytes());
        bytes.extend_from_slice(&page.request);
        self.authority.seal(&self.key, Purpose::Page, &bytes).await
    }

    pub async fn check_readable(&self) -> Result<()> {
        self.authority.kms.check_readable(self.key.generation).await
    }
}

#[derive(Default, serde::Serialize)]
pub struct VerifiedClaims(Vec<(Claim, Result<ClaimEvidence>)>);

impl VerifiedClaims {
    /// Combines verifications made under the same read scope, for a group resolved in one pass.
    pub fn union<'a>(sets: impl IntoIterator<Item = &'a VerifiedClaims>) -> Self {
        let mut verified: Vec<(Claim, Result<ClaimEvidence>)> = Vec::new();
        for (claim, result) in sets.into_iter().flat_map(|set| set.0.iter()) {
            if !verified.iter().any(|(known, _)| known == claim) {
                verified.push((claim.clone(), result.clone()));
            }
        }
        Self(verified)
    }
}

impl ClaimVerifier for VerifiedClaims {
    fn verify(&self, claim: &Claim) -> Result<ClaimEvidence> {
        self.0
            .iter()
            .find(|(known, _)| known == claim)
            .map(|(_, result)| result.clone())
            .unwrap_or_else(|| Err(Purpose::Claim.invalid()))
    }
}

struct SivKeyLength;

impl hkdf::KeyType for SivKeyLength {
    fn len(&self) -> usize {
        64
    }
}

fn siv(key: &DataKey) -> Result<Aes256Siv> {
    let mut derived = Zeroizing::new([0; 64]);
    let salt = hkdf::Salt::new(hkdf::HKDF_SHA256, DERIVATION_CONTEXT);
    let root = salt.extract(key.as_ref());
    root.expand(&[DERIVATION_CONTEXT], SivKeyLength)
        .map_err(|_| invalid_metadata())?
        .fill(derived.as_mut())
        .map_err(|_| invalid_metadata())?;
    Ok(Aes256Siv::new((&*derived).into()))
}

fn invalid_metadata() -> Anomaly {
    Anomaly::fault(
        "rtdb.crypt/token-key",
        "Invalid token key generation metadata",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FileKms;
    use rtdb_types::{
        EntityId, TableRef,
        rtdb_v1::{ClaimFootprint, ClaimRegion, ClaimScope, Layer},
    };
    use std::sync::atomic::{AtomicI64, Ordering};

    struct Keys(Vec<Vec<u8>>);

    #[async_trait]
    impl TokenKeys for Keys {
        async fn current(&self) -> Result<TokenKey> {
            TokenKey::decode(self.0.last().ok_or_else(invalid_metadata)?)
        }

        async fn get(&self, number: u64) -> Result<Option<TokenKey>> {
            self.0
                .get(number as usize)
                .map(|encoded| TokenKey::decode(encoded))
                .transpose()
        }
    }

    #[tokio::test]
    async fn receiver_observation_authenticates_the_query_cut_and_session() {
        let database = Uuid::from_u128(42);
        let kms: Arc<dyn Kms> = Arc::new(crate::LocalKms::new(|| 0));
        let metadata = TokenKey::create(kms.as_ref(), Generation { database, number: 0 }).await.unwrap().encode();
        let authority = TokenAuthority::new(database, kms, Arc::new(Keys(vec![metadata])));
        let observation = ReceiverClockObservation {
            basis: Basis::new(*database.as_bytes(), 9),
            system_time: Timestamp(67),
            observed_at: Timestamp(120),
            request: [3; 32],
            session: [5; 32],
        };
        let proof = authority.issuer().await.unwrap().seal_receiver_clock(observation).await.unwrap();
        assert_eq!(authority.open_receiver_clock(&proof, observation.session).await.unwrap(), observation);
        assert_eq!(authority.open_receiver_clock(&proof, [6; 32]).await.unwrap_err().code, "rtdb/invalid-receiver-observation");
        assert!(authority.open_page(&proof).await.is_err());
    }

    #[tokio::test]
    async fn custody_read_proof_preserves_the_full_inventory_and_selected_generation() {
        let database = Uuid::from_u128(42);
        let kms: Arc<dyn Kms> = Arc::new(crate::LocalKms::new(|| 0));
        let metadata = TokenKey::create(kms.as_ref(), Generation { database, number: 0 }).await.unwrap().encode();
        let authority = TokenAuthority::new(database, kms, Arc::new(Keys(vec![metadata])));
        let evidence = CustodyReadProof {
            basis: Basis::new(*database.as_bytes(), 77), key_generation: Some(9), expires_at: Timestamp(10_000),
            contributors: vec![([1; 16], [2; 32]), ([3; 16], [4; 32])],
        };
        let issuer = authority.issuer().await.unwrap();
        let token = issuer.seal_custody_read(&evidence, [5; 32], [6; 32]).await.unwrap();
        assert_eq!(authority.open_custody_read(&token, [5; 32], [6; 32]).await.unwrap(), evidence);
        assert!(authority.open_custody_read(&token, [7; 32], [6; 32]).await.is_err());
        assert!(authority.open_custody_read(&token, [5; 32], [8; 32]).await.is_err());
        assert!(authority.open_receiver_clock(&token, [6; 32]).await.is_err());
        let mut duplicate = evidence;
        duplicate.contributors.push(duplicate.contributors[1]);
        assert!(issuer.seal_custody_read(&duplicate, [5; 32], [6; 32]).await.is_err());
    }

    #[tokio::test]
    async fn scoped_tokens_bind_the_read_set_without_exposing_raw_cut_equality() {
        let database = Uuid::from_u128(42);
        let incarnation = *database.as_bytes();
        let generation = Generation { database, number: 0 };
        let kms: Arc<dyn Kms> = Arc::new(crate::LocalKms::new(|| 0));
        let metadata = TokenKey::create(kms.as_ref(), generation).await.unwrap().encode();
        let authority = TokenAuthority::new(database, kms, Arc::new(Keys(vec![metadata])));
        let read = LabelScope { read: [vec![1]].into(), ..Default::default() };
        let other = LabelScope { read: [vec![2]].into(), ..Default::default() };
        let issuer = authority.issuer().await.unwrap();
        let source = Basis::new(incarnation, 129);
        let first = issuer.seal_scoped_claim(source, &read).await.unwrap();
        let second = issuer.seal_scoped_claim(source, &read).await.unwrap();
        assert_eq!(first.as_bytes().len(), second.as_bytes().len());
        assert_ne!(first, second);
        assert_eq!(authority.open_scoped_claim(&first, &read).await.unwrap().scope.basis, source);
        assert!(authority.open_scoped_claim(&first, &other).await.is_err());
        assert!(authority.open_claim(&first).await.is_err());

        let page = PageToken { basis: Basis::new(read.main_read_incarnation(incarnation), 1),
            clock: Timestamp(13), offset: 2, request: [4; 32] };
        let token = issuer.seal_page_in(&page, Some(&read)).await.unwrap();
        assert_eq!(authority.resume_page_in(&token, Some(&read)).await.unwrap().0, page);
        assert!(authority.resume_page_in(&token, Some(&other)).await.is_err());
        assert!(authority.open_page(&token).await.is_err());
    }

    #[tokio::test]
    async fn tokens_are_deterministic_and_survive_restart_routing_and_rotation() {
        let root = tempfile::tempdir().unwrap();
        let vault = root.path().join("vault");
        FileKms::create_directory(&vault).unwrap();
        let now = Arc::new(AtomicI64::new(10));
        let open_kms = || {
            let now = now.clone();
            Arc::new(FileKms::open(&vault, move || now.load(Ordering::Relaxed)).unwrap())
                as Arc<dyn Kms>
        };
        let database = Uuid::from_u128(42);
        let generation = Generation {
            database,
            number: 0,
        };
        let basis = Basis::new(*database.as_bytes(), 128);
        let page = PageToken {
            basis,
            clock: Timestamp(12),
            offset: 2,
            request: [7; 32],
        };
        let evidence = ClaimEvidence {
            scope: ClaimScope {
                branch: "main".into(),
                basis,
                layer: Layer::Merged,
            },
            footprint: ClaimFootprint::from_regions([ClaimRegion::Row {
                table: TableRef::new("public", "events"),
                iid: EntityId::Integer(1).iid(),
            }]),
        };
        let first = TokenKey::create(open_kms().as_ref(), generation)
            .await
            .unwrap()
            .encode();
        let (claim, resume) = {
            let writer =
                TokenAuthority::new(database, open_kms(), Arc::new(Keys(vec![first.clone()])));
            let claim = writer.seal_claim(&evidence).await.unwrap();
            let resume = writer.seal_page(&page).await.unwrap();
            assert_eq!(writer.seal_claim(&evidence).await.unwrap(), claim);
            (claim, resume)
        };

        let other = TokenAuthority::new(database, open_kms(), Arc::new(Keys(vec![first.clone()])));
        assert_eq!(other.open_claim(&claim).await.unwrap(), evidence);
        assert_eq!(other.open_page(&resume).await.unwrap(), page);
        assert_eq!(other.seal_claim(&evidence).await.unwrap(), claim);
        assert_eq!(
            other.open_page(claim.as_bytes()).await.unwrap_err().code,
            "rtdb/cursor-expired"
        );
        let mut forgery = claim.as_bytes().to_vec();
        *forgery.last_mut().unwrap() ^= 1;
        assert_eq!(
            other
                .open_claim(&Claim::from_bytes(forgery))
                .await
                .unwrap_err()
                .code,
            "rtdb/invalid-claim"
        );
        let foreign = TokenAuthority::new(
            Uuid::from_u128(43),
            open_kms(),
            Arc::new(Keys(vec![first.clone()])),
        );
        assert_eq!(
            foreign.open_claim(&claim).await.unwrap_err().code,
            "rtdb/invalid-claim"
        );
        assert_eq!(
            foreign.open_page(&resume).await.unwrap_err().code,
            "rtdb/cursor-expired"
        );

        let kms = open_kms();
        let next = TokenKey::create(
            kms.as_ref(),
            Generation {
                database,
                number: 1,
            },
        )
        .await
        .unwrap()
        .encode();
        kms.retire(generation, 20).await.unwrap();
        assert!(kms.destroy(generation).await.is_err());
        let keys = Arc::new(Keys(vec![first, next]));
        let rotated = TokenAuthority::new(database, open_kms(), keys.clone());
        let replacement = rotated.seal_claim(&evidence).await.unwrap();
        assert_ne!(claim, replacement);
        assert_eq!(rotated.open_claim(&claim).await.unwrap(), evidence);
        assert_eq!(rotated.open_page(&resume).await.unwrap(), page);
        let (resumed, issuer) = rotated.resume_page(&resume).await.unwrap();
        assert_eq!(issuer.seal_claim(&evidence).await.unwrap(), claim);
        let next_page = issuer.seal_page(&PageToken { offset: 4, ..resumed }).await.unwrap();
        let verified = rotated
            .verify_claims([&claim, &replacement, &replacement])
            .await;
        assert_eq!(verified.verify(&claim).unwrap(), evidence);

        now.store(20, Ordering::Relaxed);
        assert_eq!(issuer.check_readable().await.unwrap_err().code, "rtdb/erased-under-view");
        assert_eq!(rotated.open_page(&next_page).await.unwrap_err().code, "rtdb/erased-under-view");
        assert_eq!(
            rotated.open_claim(&claim).await.unwrap_err().code,
            "rtdb/erased-under-view"
        );
        assert_eq!(
            rotated.check_page_readable(&resume).await.unwrap_err().code,
            "rtdb/erased-under-view"
        );
        kms.destroy(generation).await.unwrap();
        let restarted = TokenAuthority::new(database, open_kms(), keys);
        assert_eq!(
            restarted.open_page(&resume).await.unwrap_err().code,
            "rtdb/erased-under-view"
        );
        assert_eq!(restarted.open_claim(&replacement).await.unwrap(), evidence);
        assert!(open_kms().create(generation).await.is_err());
    }
}
