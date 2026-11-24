use std::sync::Arc;

use aes_siv::{KeyInit, siv::Aes256Siv};
use async_trait::async_trait;
use ring::hkdf;
use rtdb_types::{
    Anomaly, AnomalyCategory, Result, Timestamp,
    rtdb_v1::{
        Basis, Claim, ClaimEvidence, ClaimVerifier, MAX_CLAIM_PAYLOAD_BYTES, valid_branch_name,
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

const PAGE_BYTES: usize = Basis::WIDTH + 8 + 8 + 32;

#[derive(Clone, Copy)]
enum Purpose {
    Claim = 1,
    Page = 2,
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
        }
    }

    fn max_payload(self) -> usize {
        match self {
            Self::Claim => MAX_CLAIM_PAYLOAD_BYTES,
            Self::Page => PAGE_BYTES,
        }
    }
}

impl TokenAuthority {
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
        let mut verified = Vec::new();
        for claim in claims {
            if !verified.iter().any(|(known, _)| known == claim) {
                verified.push((claim.clone(), self.open_claim(claim).await));
            }
        }
        VerifiedClaims(verified)
    }

    pub async fn seal_page(&self, page: &PageToken) -> Result<Vec<u8>> {
        self.issuer().await?.seal_page(page).await
    }

    pub async fn open_page(&self, token: &[u8]) -> Result<PageToken> {
        self.resume_page(token).await.map(|(page, _)| page)
    }

    pub async fn issuer(&self) -> Result<TokenIssuer> {
        let key = self.keys.current().await?;
        if key.generation.database != self.incarnation {
            return Err(invalid_metadata());
        }
        Ok(TokenIssuer { authority: self.clone(), key })
    }

    pub async fn resume_page(&self, token: &[u8]) -> Result<(PageToken, TokenIssuer)> {
        let (bytes, key) = self.open(Purpose::Page, token).await?;
        let invalid = || Purpose::Page.invalid();
        let (basis, bytes) = bytes
            .split_first_chunk::<{ Basis::WIDTH }>()
            .ok_or_else(invalid)?;
        let basis = Basis::from_bytes(basis).map_err(|_| invalid())?;
        let (clock, bytes) = bytes.split_first_chunk::<8>().ok_or_else(invalid)?;
        let (offset, request) = bytes.split_first_chunk::<8>().ok_or_else(invalid)?;
        if basis.incarnation() != *self.incarnation.as_bytes() {
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
        if page.basis.incarnation() != *self.authority.incarnation.as_bytes() {
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

pub struct VerifiedClaims(Vec<(Claim, Result<ClaimEvidence>)>);

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
