use journal_contract::StreamId;
use ring::signature::{ED25519, UnparsedPublicKey};
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{
    ActivationAuthority, Binding, ELAPSED_GRANT_PROTOCOL, FUNDED_PORTABLE_PROTOCOL,
    FUNDED_RECOVERY_PROTOCOL, FUNDED_SETTLEMENT_PROTOCOL, State, conflict, corrupt, unavailable,
};

pub(super) mod settlement;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Enrollment {
    pub issuer: Uuid,
    pub receiver_certificate_sha256: [u8; 32],
    pub verification_key: [u8; 32],
}

impl Enrollment {
    fn key(self) -> (Uuid, [u8; 32]) {
        (self.issuer, self.receiver_certificate_sha256)
    }

    fn valid(self) -> bool {
        !self.issuer.is_nil()
            && self.receiver_certificate_sha256 != [0; 32]
            && self.verification_key != [0; 32]
    }

    fn scope(self, activation: StreamId, predecessor: Binding) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(b"rtdb.activation.funded-scope/v1\0");
        digest.update(activation.0.to_be_bytes());
        digest.update(predecessor.incarnation.as_bytes());
        digest.update(predecessor.revision.to_be_bytes());
        digest.update(self.issuer.as_bytes());
        digest.update(self.receiver_certificate_sha256);
        digest.finalize().into()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Closure {
    scope_sha256: [u8; 32],
    bookings: u64,
    inventory_sha256: [u8; 32],
    signature: Vec<u8>,
}

impl Closure {
    fn statement(&self) -> Vec<u8> {
        let mut bytes = b"rtdb.activation.funded-closure/v1\0".to_vec();
        bytes.extend_from_slice(&self.scope_sha256);
        bytes.extend_from_slice(&self.bookings.to_be_bytes());
        bytes.extend_from_slice(&self.inventory_sha256);
        bytes
    }

    fn valid(&self) -> bool {
        self.signature.len() == 64 && ((self.bookings == 0) == (self.inventory_sha256 == [0; 32]))
    }

    fn verify(&self, enrollment: Enrollment) -> Result<()> {
        if !self.valid()
            || UnparsedPublicKey::new(&ED25519, enrollment.verification_key)
                .verify(&self.statement(), &self.signature)
                .is_err()
        {
            return Err(Anomaly::incorrect(
                "rtdb/activation-funded-closure",
                "The funded closure must be signed by the enrolled original issuer",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Scope {
    enrollment: Enrollment,
    scope_sha256: [u8; 32],
    #[serde(default, skip_serializing_if = "Option::is_none")]
    journal: Option<settlement::JournalTrust>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    closure: Option<Closure>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FundedState {
    pub(super) base_version: u8,
    pub(super) scopes: Vec<Scope>,
}

#[derive(Serialize)]
pub(super) struct ScopePage {
    grants_closed: bool,
    total_scopes: usize,
    next_offset: Option<usize>,
    scope: Option<Scope>,
}

fn scope_catalog(scopes: &[Scope]) -> Option<Uuid> {
    scopes.iter().find_map(|scope| {
        scope
            .journal
            .as_ref()
            .map(settlement::JournalTrust::catalog)
    })
}

fn valid_scopes(activation: StreamId, predecessor: Binding, scopes: &[Scope]) -> bool {
    let catalog = scope_catalog(scopes);
    scopes
        .windows(2)
        .all(|pair| pair[0].enrollment.key() < pair[1].enrollment.key())
        && scopes.iter().all(|scope| {
            scope.enrollment.valid()
                && scope.scope_sha256 == scope.enrollment.scope(activation, predecessor)
                && scope
                    .journal
                    .as_ref()
                    .is_none_or(|journal| journal.valid() && Some(journal.catalog()) == catalog)
                && scope.closure.as_ref().is_none_or(|receipt| {
                    receipt.scope_sha256 == scope.scope_sha256 && receipt.valid()
                })
        })
}

impl State {
    pub(super) fn base_version(&self) -> u8 {
        self.funded
            .as_ref()
            .map_or(self.version, |funded| funded.base_version)
    }

    pub(super) fn set_base_version(&mut self, version: u8) {
        match &mut self.funded {
            Some(funded) => funded.base_version = version,
            None => self.version = version,
        }
    }

    pub(super) fn valid_funded(&self, stream: StreamId) -> bool {
        let closed = self.publication.as_ref().and_then(|publication| {
            publication
                .recovery
                .as_ref()
                .map(|recovery| (publication.predecessor, &recovery.funded_scopes))
        });
        match &self.funded {
            Some(funded) => {
                matches!(
                    self.version,
                    FUNDED_RECOVERY_PROTOCOL | FUNDED_SETTLEMENT_PROTOCOL | FUNDED_PORTABLE_PROTOCOL
                ) && funded.base_version >= ELAPSED_GRANT_PROTOCOL
                    && valid_scopes(stream, self.binding, &funded.scopes)
                    && funded
                        .scopes
                        .iter()
                        .all(|scope| scope.closure.is_none() || self.grants_closed)
                    && closed.is_none_or(|(predecessor, scopes)| {
                        valid_scopes(stream, predecessor, scopes)
                            && scopes.iter().all(|scope| scope.closure.is_some())
                    })
            }
            None => {
                !matches!(
                    self.version,
                    FUNDED_RECOVERY_PROTOCOL | FUNDED_SETTLEMENT_PROTOCOL | FUNDED_PORTABLE_PROTOCOL
                ) && closed.is_none_or(|(_, scopes)| scopes.is_empty())
            }
        }
    }

    pub(super) fn funded_scopes(&self) -> &[Scope] {
        self.funded.as_ref().map_or(&[], |funded| &funded.scopes)
    }

    pub(super) fn funded_catalog(&self) -> Option<Uuid> {
        scope_catalog(self.funded_scopes())
    }

    pub(super) fn require_funded_closures(&self) -> Result<&[Scope]> {
        let scopes = self.funded_scopes();
        if scopes.iter().any(|scope| scope.closure.is_none()) {
            return Err(Anomaly::new(
                AnomalyCategory::Conflict,
                "rtdb/activation-funded-closure-required",
                "Recovery requires admission closure from every enrolled predecessor issuer scope",
            ));
        }
        Ok(scopes)
    }
}

impl ActivationAuthority {
    pub(super) async fn commission_funded(
        &self,
        name: &str,
        binding: Binding,
        enrollment: Enrollment,
        journal: Option<settlement::JournalTrust>,
    ) -> Result<Scope> {
        let stream = self.stream_id(name)?;
        if !enrollment.valid() || journal.as_ref().is_some_and(|journal| !journal.valid()) {
            return Err(conflict());
        }
        let scope_sha256 = enrollment.scope(stream, binding);
        let state = self
            .change(stream, |state| {
                let state = state.ok_or_else(unavailable)?;
                state.require(binding)?;
                let scopes = state.funded_scopes();
                if let (Some(catalog), Some(journal)) = (state.funded_catalog(), journal.as_ref())
                    && catalog != journal.catalog()
                {
                    return Err(conflict());
                }
                let offset = match scopes
                    .binary_search_by_key(&enrollment.key(), |scope| scope.enrollment.key())
                {
                    Ok(index)
                        if scopes[index].enrollment == enrollment
                            && (journal.is_none() || scopes[index].journal == journal) =>
                    {
                        return Ok(None);
                    }
                    Ok(index)
                        if scopes[index].enrollment == enrollment
                            && scopes[index].journal.is_none() =>
                    {
                        if state.grants_closed || state.recovery.is_some() {
                            return Err(conflict());
                        }
                        let mut next = state.clone();
                        next.funded.as_mut().ok_or_else(corrupt)?.scopes[index].journal =
                            journal.clone();
                        next.version = FUNDED_PORTABLE_PROTOCOL;
                        return Ok(Some(next));
                    }
                    Ok(_) => return Err(conflict()),
                    Err(index) => index,
                };
                if state.grants_closed
                    || state.recovery.is_some()
                    || state.base_version() < ELAPSED_GRANT_PROTOCOL
                {
                    return Err(conflict());
                }
                let mut next = state.clone();
                let funded = next.funded.get_or_insert_with(|| FundedState {
                    base_version: state.version,
                    scopes: Vec::new(),
                });
                funded.scopes.insert(
                    offset,
                    Scope {
                        enrollment,
                        scope_sha256,
                        journal: journal.clone(),
                        closure: None,
                    },
                );
                next.version = FUNDED_PORTABLE_PROTOCOL;
                Ok(Some(next))
            })
            .await?;
        state
            .funded_scopes()
            .iter()
            .find(|scope| scope.scope_sha256 == scope_sha256)
            .cloned()
            .ok_or_else(corrupt)
    }

    pub(super) async fn record_funded_closure(
        &self,
        name: &str,
        predecessor: Binding,
        closure: Closure,
    ) -> Result<Scope> {
        let state = self
            .change(self.stream_id(name)?, |state| {
                let state = state.ok_or_else(unavailable)?;
                if state.binding != predecessor {
                    return Ok(None);
                }
                if !state.grants_closed {
                    return Err(conflict());
                }
                let scopes = state.funded_scopes();
                let index = scopes
                    .iter()
                    .position(|scope| scope.scope_sha256 == closure.scope_sha256)
                    .ok_or_else(conflict)?;
                let scope = &scopes[index];
                if let Some(prior) = &scope.closure {
                    return if prior == &closure {
                        Ok(None)
                    } else {
                        Err(conflict())
                    };
                }
                closure.verify(scope.enrollment)?;
                let mut next = state.clone();
                next.funded.as_mut().ok_or_else(corrupt)?.scopes[index].closure =
                    Some(closure.clone());
                next.version = FUNDED_PORTABLE_PROTOCOL;
                Ok(Some(next))
            })
            .await?;
        if state.binding != predecessor {
            let scopes = self
                .archived_funded_scopes(name, &state, predecessor)
                .await?;
            let scope = scopes
                .into_iter()
                .find(|scope| scope.scope_sha256 == closure.scope_sha256)
                .ok_or_else(conflict)?;
            return if scope.closure.as_ref() == Some(&closure) {
                Ok(scope)
            } else {
                Err(conflict())
            };
        }
        state
            .funded_scopes()
            .iter()
            .find(|scope| scope.scope_sha256 == closure.scope_sha256)
            .cloned()
            .ok_or_else(corrupt)
    }

    pub(super) async fn funded_scope_page(
        &self,
        name: &str,
        predecessor: Binding,
        offset: usize,
    ) -> Result<ScopePage> {
        let (_, state) = self.read(self.stream_id(name)?).await?;
        let state = state.ok_or_else(unavailable)?;
        if state.binding == predecessor {
            return scope_page(state.grants_closed, state.funded_scopes(), offset);
        }
        let scopes = self
            .archived_funded_scopes(name, &state, predecessor)
            .await?;
        scope_page(true, &scopes, offset)
    }

    async fn archived_funded_scopes(
        &self,
        name: &str,
        state: &State,
        predecessor: Binding,
    ) -> Result<Vec<Scope>> {
        Ok(self
            .archived_funded_recovery(name, state, predecessor)
            .await?
            .funded_scopes)
    }

    async fn archived_funded_recovery(
        &self,
        name: &str,
        state: &State,
        predecessor: Binding,
    ) -> Result<super::recovery::Completed> {
        if predecessor.revision >= state.binding.revision {
            return Err(conflict());
        }
        let publication = match state.publication.as_ref() {
            Some(publication) if publication.predecessor == predecessor => publication.clone(),
            _ => self
                .archived_publication(name, predecessor)
                .await?
                .ok_or_else(conflict)?,
        };
        let recovery = publication.recovery.ok_or_else(conflict)?;
        if !valid_scopes(self.stream_id(name)?, predecessor, &recovery.funded_scopes)
            || recovery
                .funded_scopes
                .iter()
                .any(|scope| scope.closure.is_none())
        {
            return Err(corrupt());
        }
        Ok(recovery)
    }
}

fn scope_page(grants_closed: bool, scopes: &[Scope], offset: usize) -> Result<ScopePage> {
    if offset > scopes.len() {
        return Err(conflict());
    }
    Ok(ScopePage {
        grants_closed,
        total_scopes: scopes.len(),
        next_offset: (offset + 1 < scopes.len()).then_some(offset + 1),
        scope: scopes.get(offset).cloned(),
    })
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        time::Duration,
    };

    use journal_contract::Journal;
    use ring::signature::{Ed25519KeyPair, KeyPair};
    use rtdb_storage::ranked_admission::ClosedRankedAdmission;
    use stream_logkeeper::stream::consensus::{CLUSTER_VERSION_CURRENT, testing::LocalCluster};

    use super::*;
    use crate::multi::activation::{
        VerifiedSuccessor,
        ranked::RankedClosure,
        recovery::{Intent, LossPolicy, Operation, SourceCut, Status},
    };

    fn open(journal: Arc<dyn Journal>, identity: Uuid) -> Arc<ActivationAuthority> {
        let mut authority = ActivationAuthority::new(journal, identity).unwrap();
        let time = AtomicU64::new(0);
        Arc::get_mut(&mut authority).unwrap().clock =
            Arc::new(move || Some(Duration::from_secs(time.fetch_add(8, Ordering::SeqCst))));
        authority
    }

    fn intent(predecessor: Binding) -> Intent {
        Intent {
            operation: Operation {
                predecessor,
                operation_id: Uuid::new_v4(),
            },
            destination_cell: None,
            loss_policy: LossPolicy::AcceptUnknownTail,
        }
    }

    fn closure(key: &Ed25519KeyPair, scope: &Scope, bookings: u64) -> Closure {
        let mut receipt = Closure {
            scope_sha256: scope.scope_sha256,
            bookings,
            inventory_sha256: if bookings == 0 { [0; 32] } else { [7; 32] },
            signature: Vec::new(),
        };
        receipt.signature = key.sign(&receipt.statement()).as_ref().to_vec();
        receipt
    }

    async fn record_installation(
        authority: &ActivationAuthority,
        identity: Uuid,
        intent: Intent,
        status: Status,
    ) {
        let Status::Pending { candidate, .. } = status else {
            panic!("Expected a pending recovery")
        };
        let installed = VerifiedSuccessor {
            predecessor: intent.operation.predecessor.incarnation,
            successor: candidate,
            manifest_digest: [5; 32],
            activation_scope: Some(rtdb_db::ActivationScope::new(identity, "xtdb").unwrap()),
            destination_cell: None,
            key_hold: None,
            journal_hold: None,
            objects_sha256: None,
            funded_inventory_sha256: None,
            source_cut: SourceCut {
                lsn: 3,
                position: 1,
                system_time: 2,
            },
        };
        authority
            .record_recovery_installation("xtdb", intent.operation, installed)
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn funded_scopes_survive_rotation_supersession_and_publication_without_an_operation_id() {
        const PRINCIPAL: &str = "urn:logkeeper:client:funded-roster";
        let mut keepers = LocalCluster::start_independent_catalog(&[PRINCIPAL])
            .await
            .unwrap();
        keepers
            .finalize_version(PRINCIPAL, CLUSTER_VERSION_CURRENT)
            .await
            .unwrap();
        let config = keepers.journal_config(8 << 20, 1 << 20);
        let connect = || Arc::new(keepers.journal(PRINCIPAL, &config).unwrap());
        let identity = Uuid::new_v4();
        let authority = open(connect(), identity);
        let predecessor = authority.bootstrap("xtdb", Uuid::new_v4()).await.unwrap();
        let issuer = Ed25519KeyPair::from_seed_unchecked(&[1; 32]).unwrap();
        let other = Ed25519KeyPair::from_seed_unchecked(&[2; 32]).unwrap();
        let enrollment = Enrollment {
            issuer: Uuid::new_v4(),
            receiver_certificate_sha256: [3; 32],
            verification_key: issuer.public_key().as_ref().try_into().unwrap(),
        };
        let earlier = authority
            .commission_funded("xtdb", predecessor, enrollment, None)
            .await
            .unwrap();
        let ranked_scope = authority
            .ranked_commissioning_scope("xtdb", predecessor)
            .await
            .unwrap();
        let ranked_provider = [23; 32];
        authority
            .commission_ranked("xtdb", predecessor, ranked_scope, ranked_provider)
            .await
            .unwrap();
        let later = authority
            .commission_funded(
                "xtdb",
                predecessor,
                Enrollment {
                    receiver_certificate_sha256: [4; 32],
                    ..enrollment
                },
                None,
            )
            .await
            .unwrap();
        assert_ne!(earlier.scope_sha256, later.scope_sha256);
        assert_eq!(
            authority
                .commission_funded("xtdb", predecessor, enrollment, None)
                .await
                .unwrap(),
            earlier
        );
        let different_key = Enrollment {
            verification_key: other.public_key().as_ref().try_into().unwrap(),
            ..enrollment
        };
        assert!(
            authority
                .commission_funded("xtdb", predecessor, different_key, None)
                .await
                .is_err()
        );
        assert_eq!(
            authority
                .status("xtdb")
                .await
                .unwrap()
                .grant_protocol_version,
            FUNDED_PORTABLE_PROTOCOL
        );
        assert!(authority.close_grants("xtdb", predecessor).await.is_err());

        let abandoned = intent(predecessor);
        authority
            .begin_recovery("xtdb", abandoned, None)
            .await
            .unwrap();
        let earlier_receipt = closure(&issuer, &earlier, 0);
        assert!(
            authority
                .record_funded_closure("xtdb", predecessor, earlier_receipt.clone())
                .await
                .is_err()
        );
        authority
            .freeze_recovery("xtdb", abandoned.operation)
            .await
            .unwrap();
        let ranked_closure = RankedClosure {
            provider: ranked_provider,
            receipt: ClosedRankedAdmission {
                scope: ranked_scope,
                admitted: 0,
                inventory_sha256: [0; 32],
                staging: Default::default(),
            },
        };
        authority
            .record_ranked_closure("xtdb", abandoned.operation, ranked_closure)
            .await
            .unwrap();
        let wrong_issuer = closure(&other, &earlier, 2);
        assert_eq!(
            authority
                .record_funded_closure("xtdb", predecessor, wrong_issuer)
                .await
                .unwrap_err()
                .code,
            "rtdb/activation-funded-closure"
        );
        let closed_earlier = authority
            .record_funded_closure("xtdb", predecessor, earlier_receipt.clone())
            .await
            .unwrap();
        assert!(
            authority
                .commission_funded(
                    "xtdb",
                    predecessor,
                    Enrollment {
                        receiver_certificate_sha256: [5; 32],
                        ..enrollment
                    },
                    None,
                )
                .await
                .is_err()
        );

        let current = intent(predecessor);
        let pending = authority
            .begin_recovery("xtdb", current, Some(abandoned.operation.operation_id))
            .await
            .unwrap();
        record_installation(&authority, identity, current, pending).await;
        let page = authority
            .funded_scope_page("xtdb", predecessor, 0)
            .await
            .unwrap();
        assert!(page.grants_closed);
        assert_eq!(page.total_scopes, 2);
        assert_eq!(page.next_offset, Some(1));
        assert_eq!(page.scope, Some(closed_earlier.clone()));
        assert_eq!(
            authority
                .publish_recovery("xtdb", current.operation)
                .await
                .unwrap_err()
                .code,
            "rtdb/activation-funded-closure-required"
        );
        let later_receipt = closure(&issuer, &later, 0);
        let closed_later = authority
            .record_funded_closure("xtdb", predecessor, later_receipt)
            .await
            .unwrap();
        assert!(
            authority
                .record_funded_closure("xtdb", predecessor, closure(&issuer, &earlier, 3))
                .await
                .is_err()
        );
        let published = authority
            .publish_recovery("xtdb", current.operation)
            .await
            .unwrap();
        assert!(
            matches!(published, Status::Published { ranked_closure: Some(closure), .. } if closure == ranked_closure)
        );

        let successor = authority.binding("xtdb").await.unwrap();
        assert_eq!(
            authority
                .funded_scope_page("xtdb", successor, 0)
                .await
                .unwrap()
                .total_scopes,
            0
        );
        let newer = intent(successor);
        let pending = authority.begin_recovery("xtdb", newer, None).await.unwrap();
        record_installation(&authority, identity, newer, pending).await;
        authority
            .freeze_recovery("xtdb", newer.operation)
            .await
            .unwrap();
        authority
            .publish_recovery("xtdb", newer.operation)
            .await
            .unwrap();
        drop(authority);

        let authority = open(connect(), identity);
        let page = authority
            .funded_scope_page("xtdb", predecessor, 1)
            .await
            .unwrap();
        assert!(page.grants_closed);
        assert_eq!(page.scope, Some(closed_later));
        assert_eq!(page.next_offset, None);
        assert_eq!(
            authority
                .record_funded_closure("xtdb", predecessor, earlier_receipt)
                .await
                .unwrap(),
            closed_earlier
        );
        keepers.shutdown().await.unwrap();
    }
}
