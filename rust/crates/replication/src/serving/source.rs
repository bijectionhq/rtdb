use std::sync::Arc;

use bytes::Bytes;
use rtdb_crypt::{SourceAdmissionKey, tokens::VerifiedClaims};
use rtdb_storage::{Error, ObjectKey, ObjectStore};
use rtdb_tx::transact::{CUSTODY_RELEASE_NAMESPACE, Transact, TrustedCustodyCommand};
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    request: Option<Box<RawValue>>,
    scope: Option<NativeServingScope>,
    claims: C,
    custody_request_digest: Option<[u8; 32]>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    funded: Option<FundedAdmission>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    release: Option<ReleaseAdmission>,
}

#[derive(Serialize, Deserialize)]
struct FundedAdmission {
    request: Box<RawValue>,
    input: rtdb_tx::transact::FundingInput,
}

#[derive(Serialize, Deserialize)]
struct ReleaseAdmission {
    namespace: [u8; 32],
    command: TrustedCustodyCommand,
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
        if request.transaction.contains_response_release()
            || request.scope.as_ref().is_some_and(|scope| scope.receipt_namespace == CUSTODY_RELEASE_NAMESPACE)
        {
            return Err(invalid_release());
        }
        self.seal_request(request, None).await
    }

    pub(super) async fn seal_release(&self, command: TrustedCustodyCommand) -> Result<Bytes> {
        let transaction = command.clone().into_request()?;
        let release = ReleaseAdmission { namespace: CUSTODY_RELEASE_NAMESPACE, command };
        self.seal_request(NativeRequest { transaction, scope: None, claims: VerifiedClaims::default(), funding: None }, Some(release)).await
    }

    async fn seal_request(&self, request: NativeRequest, release: Option<ReleaseAdmission>) -> Result<Bytes> {
        let canonical = request.transaction.canonical_request()?;
        let (canonical, funded) = match request.funding {
            Some(input) => (None, Some(FundedAdmission { request: canonical, input })),
            None => (Some(canonical), None),
        };
        let admission: Admission<VerifiedClaims> = Admission {
            request: canonical,
            scope: request.scope,
            claims: request.claims,
            custody_request_digest: request.transaction.custody_request_digest,
            funded,
            release,
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
            let (request, funding) = match (admission.request, admission.funded) {
                (Some(request), None) => (request, None),
                (None, Some(funded)) if admission.release.is_none() && admission.custody_request_digest.is_none() =>
                    (funded.request, Some(funded.input)),
                _ => return Err(rtdb_tx::transact::funding_invalid()),
            };
            let mut transaction = match admission.release {
                Some(release) => {
                    if release.namespace != CUSTODY_RELEASE_NAMESPACE || admission.scope.is_some()
                        || admission.custody_request_digest.is_some() || !admission.claims.0.is_empty()
                    {
                        return Err(invalid_release());
                    }
                    let transaction = release.command.into_request()?;
                    if transaction.canonical_request()?.get() != request.get() { return Err(invalid_release()); }
                    transaction
                }
                None => {
                    if admission.scope.as_ref().is_some_and(|scope| scope.receipt_namespace == CUSTODY_RELEASE_NAMESPACE) {
                        return Err(invalid_release());
                    }
                    Transact::decode_canonical_request(request.get().as_bytes())?
                }
            };
            transaction.custody_request_digest = admission.custody_request_digest;
            Ok(NativeRequest { transaction, scope: admission.scope, claims: admission.claims, funding })
        })();
        Ok(request)
    }
}

fn invalid_release() -> Anomaly {
    Anomaly::incorrect("rtdb/source-release-admission", "Trusted release admission must preserve its exact canonical command and private namespace")
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
        let bytes = submitter.seal(NativeRequest { transaction, scope: Some(scope), claims, funding: None }).await.unwrap();

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

#[cfg(test)]
mod release_tests {
    use std::num::NonZeroU64;
    use rtdb_index::branches::{CustodyCarrierAuthority, CustodyRelease, CustodyReleasePhase, CustodyReleaseRegistration, CustodyResponseAdmission, CustodyResponsePart};
    use rtdb_storage::MemoryStore;
    use rtdb_tx::transact::{CustodyWriterFence, TrustedCustodyAdmission, TrustedCustodyRelease};
    use rtdb_types::{EntityId, TableRef, Timestamp, rtdb_v1::Basis};

    use super::*;

    #[tokio::test]
    async fn trusted_source_release_reopens_only_with_its_canonical_request_and_private_namespace() {
        let incarnation = [1; 16];
        let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::default());
        let submitter = SourceAuthority::new(Arc::clone(&store), incarnation);
        let writer = SourceAuthority::new(store, incarnation);
        let command = TrustedCustodyRelease {
            release: CustodyRelease {
                registration: CustodyReleaseRegistration {
                    authority: CustodyCarrierAuthority {
                        incarnation, source_history: [2; 32], installation: [3; 16], registry: [4; 32], original_writer: [5; 32],
                    },
                    group: [6; 16], original: [7; 32], occurrence: [8; 32], carrier_receipt_sha256: [15; 32], expires_at: Timestamp(100),
                    part: CustodyResponsePart {
                        transport_history: [9; 16], producer: [10; 16], consumer: [11; 16], response: [12; 16], ordinal: NonZeroU64::MIN,
                    },
                },
                phase: CustodyReleasePhase::Disposed { evidence_sha256: [13; 32] },
            },
            writer: CustodyWriterFence {
                table: TableRef::new("public", "installation"), id: EntityId::Integer(1), valid_at: Timestamp(10), written: Basis::new(incarnation, 2),
            },
        };
        let request = command.clone().into_request();
        let canonical = request.canonical_request().unwrap();
        assert!(Transact::decode_canonical_request(canonical.get().as_bytes()).is_err());
        let bytes = submitter.seal_release(TrustedCustodyCommand::Phase(command.clone())).await.unwrap();
        let reopened = writer.open(&bytes).await.unwrap().unwrap();
        assert!(reopened.scope.is_none());
        assert_eq!(reopened.transaction.receipt_namespace(None), Some(CUSTODY_RELEASE_NAMESPACE));
        assert_eq!(reopened.transaction.canonical_request().unwrap().get(), canonical.get());

        let request = || NativeRequest { transaction: command.clone().into_request(), scope: None, claims: VerifiedClaims::default(), funding: None };
        assert!(submitter.seal(request()).await.is_err());
        let mismatched_namespace = submitter.seal_request(request(), Some(ReleaseAdmission {
            namespace: [0; 32], command: TrustedCustodyCommand::Phase(command.clone()),
        })).await.unwrap();
        assert!(writer.open(&mismatched_namespace).await.unwrap().is_err());
        let mut changed_request = request();
        changed_request.transaction.key = vec![14; 16];
        let changed = submitter.seal_request(changed_request, Some(ReleaseAdmission {
            namespace: CUSTODY_RELEASE_NAMESPACE, command: TrustedCustodyCommand::Phase(command.clone()),
        })).await.unwrap();
        assert!(writer.open(&changed).await.unwrap().is_err());

        let admit = TrustedCustodyCommand::Admit(TrustedCustodyAdmission {
            admission: CustodyResponseAdmission { registrations: vec![command.release.registration],
                frame_sha256: [16; 32], read_proofs_sha256: Some([17; 32]), selected_basis: Basis::new(incarnation, 3), key_generation: 7 },
            writer: command.writer,
        });
        let transaction = admit.clone().into_request().unwrap();
        let canonical = transaction.canonical_request().unwrap();
        assert!(Transact::decode_canonical_request(canonical.get().as_bytes()).is_err());
        assert!(submitter.seal(NativeRequest { transaction, scope: None, claims: VerifiedClaims::default(), funding: None }).await.is_err());
        let sealed = submitter.seal_release(admit).await.unwrap();
        let reopened = writer.open(&sealed).await.unwrap().unwrap();
        assert_eq!(reopened.transaction.receipt_namespace(None), Some(CUSTODY_RELEASE_NAMESPACE));
        assert_eq!(reopened.transaction.canonical_request().unwrap().get(), canonical.get());
    }
}
