use std::sync::Arc;

use bytes::Bytes;
use rtdb_crypt::{SourceAdmissionKey, tokens::VerifiedClaims};
use rtdb_storage::{Error, ObjectKey, ObjectStore};
use rtdb_tx::transact::Transact;
use rtdb_types::{Anomaly, Result, rtdb_v1::{Claim, ClaimEvidence, ClaimVerifier}};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use tokio::sync::OnceCell;
use uuid::Uuid;

use super::{NativeRequest, NativeServingScope};

pub(crate) struct SourceAuthority {
    store: Arc<dyn ObjectStore>,
    incarnation: Uuid,
    key: OnceCell<SourceAdmissionKey>,
}

#[derive(Serialize, Deserialize)]
struct Admission<C> {
    request: Box<RawValue>,
    scope: Option<NativeServingScope>,
    claims: C,
    custody_request_digest: Option<[u8; 32]>,
}

pub(crate) type SourceRequest = NativeRequest<SourceClaims>;

#[derive(Default, Deserialize)]
#[serde(transparent)]
pub(crate) struct SourceClaims(Vec<(Claim, Result<ClaimEvidence>)>);

impl ClaimVerifier for SourceClaims {
    fn verify(&self, claim: &Claim) -> Result<ClaimEvidence> {
        self.0.iter().find(|(known, _)| known == claim).map(|(_, result)| result.clone())
            .unwrap_or_else(|| Err(Anomaly::incorrect("rtdb/invalid-claim", "Invalid query claim")))
    }
}

impl SourceAuthority {
    pub(crate) fn new(store: Arc<dyn ObjectStore>, incarnation: [u8; 16]) -> Self {
        Self { store, incarnation: Uuid::from_bytes(incarnation), key: OnceCell::new() }
    }

    async fn key(&self, create: bool) -> Result<&SourceAdmissionKey> {
        self.key.get_or_try_init(|| async {
            let key = ObjectKey::new(format!("~rtdb-v1/{}/source-admission-key", self.incarnation))?;
            let stored = match self.store.get_fresh_exact(&key, 32).await {
                Err(Error::MissingObject(_)) if create => {
                    let generated = SourceAdmissionKey::generate()?;
                    match self.store.put(&key, Bytes::copy_from_slice(generated.as_bytes())).await {
                        Ok(()) | Err(Error::ConflictingObject(_)) => (),
                        Err(error) => return Err(error.into()),
                    }
                    self.store.get_fresh_exact(&key, 32).await?
                }
                result => result?,
            };
            SourceAdmissionKey::from_bytes(&stored)
        }).await
    }

    pub(super) async fn seal(&self, request: NativeRequest) -> Result<Bytes> {
        let admission: Admission<VerifiedClaims> = Admission {
            request: request.transaction.canonical_request()?,
            scope: request.scope,
            claims: request.claims,
            custody_request_digest: request.transaction.custody_request_digest,
        };
        let bytes = serde_json::to_vec(&admission).map_err(super::encoding_error)?;
        Ok(self.key(true).await?.seal(self.incarnation, &bytes)?.into())
    }

    pub(crate) async fn open(&self, bytes: &[u8]) -> Result<Result<SourceRequest>> {
        if let Err(error) = SourceAdmissionKey::check_envelope(bytes) { return Ok(Err(error)); }
        let key = self.key(false).await?;
        let request = (|| {
            let clear = key.open(self.incarnation, bytes)?;
            let admission: Admission<SourceClaims> = serde_json::from_slice(&clear).map_err(super::encoding_error)?;
            let mut transaction = Transact::decode_canonical_request(admission.request.get().as_bytes())?;
            transaction.custody_request_digest = admission.custody_request_digest;
            Ok(NativeRequest { transaction, scope: admission.scope, claims: admission.claims })
        })();
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use rtdb_crypt::{Generation, LocalKms, Kms, tokens::{TokenAuthority, TokenKey, TokenKeys}};
    use rtdb_storage::MemoryStore;
    use rtdb_tx::transact::{Guard, Op, Window};
    use rtdb_types::rtdb_v1::{Basis, ClaimScope, LabelScope, Layer};

    use super::*;

    struct Keys(TokenKey);

    #[async_trait]
    impl TokenKeys for Keys {
        async fn current(&self) -> Result<TokenKey> { Ok(self.0.clone()) }
        async fn get(&self, number: u64) -> Result<Option<TokenKey>> { Ok((number == 0).then(|| self.0.clone())) }
    }

    #[tokio::test]
    async fn truncated_source_admission_is_refused_before_key_publication() {
        let authority = SourceAuthority::new(Arc::new(MemoryStore::default()), [3; 16]);
        assert!(matches!(authority.open(b"truncated").await.unwrap(), Err(error) if error.code == "rtdb/source-admission"));
    }

    #[tokio::test]
    async fn source_reopen_authenticates_the_canonical_request_scope_and_claim_verdict() {
        let database = Uuid::from_bytes([7; 16]);
        let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::default());
        let kms: Arc<dyn Kms> = Arc::new(LocalKms::new(|| 0));
        let key = TokenKey::create(kms.as_ref(), Generation { database, number: 0 }).await.unwrap();
        let tokens = TokenAuthority::new(database, kms, Arc::new(Keys(key)));
        let labels = LabelScope { read: [vec![1]].into_iter().collect(), write: [vec![2]].into_iter().collect() };
        let evidence = ClaimEvidence {
            scope: ClaimScope { branch: "main".into(), basis: Basis::new(*database.as_bytes(), 0), layer: Layer::Merged },
            footprint: Default::default(),
        };
        let claim = tokens.issuer().await.unwrap().seal_scoped_claim(evidence.scope.basis, &labels).await.unwrap();
        let transaction = Transact {
            key: vec![3; 16], require: Vec::new(), window: Window::default(), guards: Vec::new(),
            ops: vec![Op::DropBranch { branch: "child".into() }], otherwise: Vec::new(),
            requires: vec![Guard::Unchanged(claim.clone())], custody_request_digest: None, request_memory: None,
        };
        let canonical = transaction.canonical_request().unwrap();
        let claims = tokens.verify_claims_in(transaction.claims(), Some(&labels)).await;
        let scope = NativeServingScope { labels: labels.clone(), receipt_namespace: [4; 32] };
        let submitter = SourceAuthority::new(Arc::clone(&store), *database.as_bytes());
        let bytes = submitter.seal(NativeRequest { transaction, scope: Some(scope), claims }).await.unwrap();

        let writer = SourceAuthority::new(store, *database.as_bytes());
        let replay = writer.open(&bytes).await.unwrap().unwrap();
        assert_eq!(replay.transaction.canonical_request().unwrap().get(), canonical.get());
        let scope = replay.scope.unwrap();
        assert_eq!(scope.labels, labels);
        assert_eq!(scope.receipt_namespace, [4; 32]);
        assert_eq!(replay.claims.verify(&claim).unwrap(), evidence);

        let mut changed = bytes.to_vec();
        changed[32] ^= 1;
        assert!(matches!(writer.open(&changed).await.unwrap(), Err(error) if error.code == "rtdb/source-admission"));
        let key = writer.key(false).await.unwrap();
        assert!(matches!(key.open(Uuid::from_bytes([8; 16]), &bytes), Err(error) if error.code == "rtdb/source-admission"));
    }
}
