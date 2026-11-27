use std::collections::BTreeMap;

use rtdb_storage::ranked_admission::{ClosedRankedAdmission, RankedAdmissionScope};
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use super::{
    ActivationAuthority, Binding, ELAPSED_GRANT_PROTOCOL, RANKED_RECOVERY_PROTOCOL,
    RECOVERY_HOSTED_CUSTODY_PROTOCOL, State, conflict,
    recovery::{Operation, Status},
    unavailable,
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RankedState {
    protocol: u8,
    pub(super) provider: Option<[u8; 32]>,
    pub(super) closure: Option<RankedClosure>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RankedClosure {
    pub(super) provider: [u8; 32],
    pub(super) receipt: ClosedRankedAdmission,
}

impl RankedClosure {
    pub(super) fn valid_for(&self, incarnation: Uuid) -> bool {
        self.provider != [0; 32]
            && self.receipt.scope.incarnation == incarnation
            && self.receipt.scope.activation_sha256 != [0; 32]
            && (self.receipt.admitted == 0) == (self.receipt.inventory_sha256 == [0; 32])
    }
}

impl State {
    pub(super) fn recovery_protocol(&self) -> u8 {
        self.ranked.map_or(self.version, |ranked| ranked.protocol)
    }

    pub(super) fn advance_protocol(&mut self, protocol: u8) {
        if let Some(ranked) = &mut self.ranked {
            ranked.protocol = ranked.protocol.max(protocol);
        } else {
            self.version = self.version.max(protocol);
        }
    }

    pub(super) fn valid_ranked(&self) -> bool {
        match self.ranked {
            None => self.version != RANKED_RECOVERY_PROTOCOL,
            Some(ranked) => {
                self.version == RANKED_RECOVERY_PROTOCOL
                    && (ELAPSED_GRANT_PROTOCOL..=RECOVERY_HOSTED_CUSTODY_PROTOCOL)
                        .contains(&ranked.protocol)
                    && ranked.provider != Some([0; 32])
                    && ranked.closure.is_none_or(|closure| {
                        self.grants_closed
                            && self.recovery.is_some()
                            && ranked.provider == Some(closure.provider)
                            && closure.valid_for(self.binding.incarnation)
                    })
            }
        }
    }

    pub(super) fn require_ranked_closure(&self) -> Result<Option<RankedClosure>> {
        match self.ranked {
            Some(ranked) if ranked.provider.is_some() => {
                ranked.closure.map(Some).ok_or_else(missing_closure)
            }
            _ => Ok(None),
        }
    }
}

impl ActivationAuthority {
    pub(super) async fn ranked_commissioning_scope(
        &self,
        name: &str,
        binding: Binding,
    ) -> Result<RankedAdmissionScope> {
        self.access.require_operator()?;
        let (_, state) = self.read(self.stream_id(name)?).await?;
        let state = state.ok_or_else(unavailable)?;
        state.require(binding)?;
        if state.grants_closed || state.recovery.is_some() {
            return Err(conflict());
        }
        self.ranked_scope(name, &state)
    }

    pub(super) async fn commission_ranked(
        &self,
        name: &str,
        binding: Binding,
        scope: RankedAdmissionScope,
        provider: [u8; 32],
    ) -> Result<()> {
        if provider == [0; 32] {
            return Err(conflict());
        }
        self.change(self.stream_id(name)?, |state| {
            let state = state.ok_or_else(unavailable)?;
            state.require(binding)?;
            if state.grants_closed
                || state.recovery.is_some()
                || self.ranked_scope(name, state)? != scope
                || state.recovery_protocol() < ELAPSED_GRANT_PROTOCOL
            {
                return Err(conflict());
            }
            if let Some(existing) = state.ranked.and_then(|ranked| ranked.provider) {
                return if existing == provider {
                    Ok(None)
                } else {
                    Err(conflict())
                };
            }
            let mut next = state.clone();
            next.version = RANKED_RECOVERY_PROTOCOL;
            next.ranked = Some(RankedState {
                protocol: state.recovery_protocol(),
                provider: Some(provider),
                closure: None,
            });
            Ok(Some(next))
        })
        .await?;
        Ok(())
    }

    pub(super) async fn ranked_to_close(
        &self,
        name: &str,
        operation: Operation,
    ) -> Result<Option<([u8; 32], RankedAdmissionScope)>> {
        let (_, state) = self.read(self.stream_id(name)?).await?;
        let state = state.ok_or_else(unavailable)?;
        if state.publication_status(operation)?.is_some() {
            return Ok(None);
        }
        state.pending(operation)?;
        if !state.grants_closed {
            return Err(conflict());
        }
        match state.ranked {
            Some(RankedState {
                provider: Some(provider),
                closure: None,
                ..
            }) => Ok(Some((provider, self.ranked_scope(name, &state)?))),
            _ => Ok(None),
        }
    }

    pub(super) async fn record_ranked_closure(
        &self,
        name: &str,
        operation: Operation,
        closure: RankedClosure,
    ) -> Result<Status> {
        self.change(self.stream_id(name)?, |state| {
            let state = state.ok_or_else(unavailable)?;
            state.pending(operation)?;
            let mut ranked = state.ranked.ok_or_else(conflict)?;
            if !state.grants_closed
                || ranked.provider != Some(closure.provider)
                || self.ranked_scope(name, state)? != closure.receipt.scope
                || !closure.valid_for(state.binding.incarnation)
            {
                return Err(conflict());
            }
            if let Some(prior) = ranked.closure {
                return if prior == closure {
                    Ok(None)
                } else {
                    Err(conflict())
                };
            }
            ranked.closure = Some(closure);
            let mut next = state.clone();
            next.ranked = Some(ranked);
            Ok(Some(next))
        })
        .await?;
        self.recovery_status(name, operation).await
    }

    fn ranked_scope(&self, name: &str, state: &State) -> Result<RankedAdmissionScope> {
        let scope =
            rtdb_db::ActivationScope::for_cell(self.identity, name, state.destination_cell())?;
        Ok(RankedAdmissionScope {
            incarnation: state.binding.incarnation,
            activation_sha256: scope.digest(),
        })
    }
}

fn missing_closure() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict,
        "rtdb/activation-ranked-closure-required",
        "Recovery requires closure from the predecessor's original ranked provider",
    )
}

#[cfg(any(feature = "s3", feature = "backup-object-holds"))]
type ProviderConfig = rtdb_s3::S3Config;
#[cfg(not(any(feature = "s3", feature = "backup-object-holds")))]
type ProviderConfig = Value;

#[derive(Default, Deserialize)]
#[serde(transparent)]
pub(super) struct Providers(BTreeMap<String, ProviderConfig>);

impl Providers {
    pub(super) fn validate(&self) -> Result<()> {
        let mut providers = std::collections::BTreeSet::new();
        for (name, config) in &self.0 {
            if name.is_empty() || !providers.insert(provider_id(config)?) {
                return Err(provider_unavailable());
            }
        }
        Ok(())
    }

    pub(super) async fn commission(
        &self,
        authority: &ActivationAuthority,
        name: &str,
        binding: Binding,
        provider: &str,
    ) -> Result<()> {
        let config = self.0.get(provider).ok_or_else(provider_unavailable)?;
        let scope = authority.ranked_commissioning_scope(name, binding).await?;
        let provider = provider_id(config)?;
        commission_scope(config, scope).await?;
        authority
            .commission_ranked(name, binding, scope, provider)
            .await
    }

    pub(super) async fn freeze(
        &self,
        authority: &ActivationAuthority,
        name: &str,
        operation: Operation,
    ) -> Result<Status> {
        authority.freeze_recovery(name, operation).await?;
        let Some((provider, scope)) = authority.ranked_to_close(name, operation).await? else {
            return authority.recovery_status(name, operation).await;
        };
        let config = self.find(provider)?;
        let receipt = close_scope(config, scope).await?;
        authority
            .record_ranked_closure(name, operation, RankedClosure { provider, receipt })
            .await
    }

    pub(super) async fn outcomes(
        &self,
        authority: &ActivationAuthority,
        name: &str,
        operation: Operation,
        after: u64,
    ) -> Result<Value> {
        let closure = match authority.recovery_status(name, operation).await? {
            Status::Pending { ranked_closure, .. } | Status::Published { ranked_closure, .. } => {
                ranked_closure
            }
        }
        .ok_or_else(missing_closure)?;
        let config = self.find(closure.provider)?;
        read_outcomes(config, closure.receipt, after).await
    }

    fn find(&self, selected: [u8; 32]) -> Result<&ProviderConfig> {
        for config in self.0.values() {
            if provider_id(config)? == selected {
                return Ok(config);
            }
        }
        Err(provider_unavailable())
    }
}

#[cfg(any(feature = "s3", feature = "backup-object-holds"))]
pub(super) fn provider_id(config: &ProviderConfig) -> Result<[u8; 32]> {
    use sha2::{Digest, Sha256};

    let root = rtdb_storage::ObjectKey::new("multi")?;
    let allocation = config.ranked_allocation(&root)?;
    let encoded = serde_json::to_vec(&allocation).map_err(|_| provider_unavailable())?;
    let mut hash = Sha256::new();
    hash.update(b"rtdb.activation/ranked-s3-provider/v1\0");
    hash.update(encoded);
    Ok(hash.finalize().into())
}

#[cfg(any(feature = "s3", feature = "backup-object-holds"))]
pub(super) async fn open_provider(
    config: &ProviderConfig,
    scope: RankedAdmissionScope,
    io: std::sync::Arc<rtdb_storage::IoRegion>,
) -> Result<rtdb_storage::remote::RankedAdmission> {
    let root = rtdb_storage::ObjectKey::new("multi")?;
    config
        .ranked_admission(&root, &BTreeMap::new(), io, scope)
        .await
}

#[cfg(any(feature = "s3", feature = "backup-object-holds"))]
async fn commission_scope(config: &ProviderConfig, scope: RankedAdmissionScope) -> Result<()> {
    let io = rtdb_storage::IoRegion::new();
    let result = async {
        open_provider(config, scope, io.clone())
            .await?
            .commission()
            .await
    }
    .await;
    io.close_and_wait().await;
    result
}

#[cfg(any(feature = "s3", feature = "backup-object-holds"))]
async fn close_scope(
    config: &ProviderConfig,
    scope: RankedAdmissionScope,
) -> Result<ClosedRankedAdmission> {
    let io = rtdb_storage::IoRegion::new();
    let result = async {
        open_provider(config, scope, io.clone())
            .await?
            .close()
            .await
    }
    .await;
    io.close_and_wait().await;
    result
}

#[cfg(any(feature = "s3", feature = "backup-object-holds"))]
async fn read_outcomes(
    config: &ProviderConfig,
    receipt: ClosedRankedAdmission,
    after: u64,
) -> Result<Value> {
    let io = rtdb_storage::IoRegion::new();
    let result = async {
        let page = open_provider(config, receipt.scope, io.clone()).await?
            .outcome_page(receipt, after, 32).await?;
        let catalogues = page.catalogues.into_iter().map(|entry| serde_json::json!({
            "manifest": entry.manifest.to_string(), "position": entry.position,
            "system_time": entry.system_time, "key_generation": entry.key_generation,
        })).collect::<Vec<_>>();
        Ok(serde_json::json!({ "scope": receipt, "catalogues": catalogues, "next_after": page.next_after }))
    }.await;
    io.close_and_wait().await;
    result
}

#[cfg(not(any(feature = "s3", feature = "backup-object-holds")))]
fn provider_id(_: &ProviderConfig) -> Result<[u8; 32]> {
    Err(provider_unavailable())
}

#[cfg(not(any(feature = "s3", feature = "backup-object-holds")))]
async fn commission_scope(_: &ProviderConfig, _: RankedAdmissionScope) -> Result<()> {
    Err(provider_unavailable())
}

#[cfg(not(any(feature = "s3", feature = "backup-object-holds")))]
async fn close_scope(_: &ProviderConfig, _: RankedAdmissionScope) -> Result<ClosedRankedAdmission> {
    Err(provider_unavailable())
}

#[cfg(not(any(feature = "s3", feature = "backup-object-holds")))]
async fn read_outcomes(_: &ProviderConfig, _: ClosedRankedAdmission, _: u64) -> Result<Value> {
    Err(provider_unavailable())
}

fn provider_unavailable() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb/activation-ranked-provider",
        "The original ranked provider requires a matching S3 allocation in the activation broker",
    )
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

    use super::*;
    use crate::multi::activation::{
        VerifiedSuccessor,
        recovery::{Intent, LossPolicy, SourceCut},
    };

    #[cfg(any(feature = "s3", feature = "backup-object-holds"))]
    #[tokio::test]
    #[ignore = "requires a disposable S3-compatible loopback bucket"]
    async fn ranked_broker_reads_the_original_catalogues_after_source_loss() {
        use rtdb_format::journal::{ObjectId, ObjectKind, RankedCatalog};

        let endpoint = std::env::var("RTDB_BACKUP_HOLD_TEST_ENDPOINT").unwrap();
        assert_eq!(url::Url::parse(&endpoint).unwrap().host_str(), Some("127.0.0.1"));
        let bucket = std::env::var("RTDB_BACKUP_HOLD_TEST_BUCKET").unwrap();
        assert!(bucket.starts_with("rtdb-backup-hold-test-"));
        let mut config = rtdb_s3::S3Config::new(bucket);
        config.endpoint = Some(endpoint);
        config.region = Some("us-east-1".into());
        let data = tempfile::tempdir().unwrap();
        let identity = Uuid::new_v4();
        let path = data.path().join("authority");
        let authority = ActivationAuthority::open_local(&path, identity).unwrap();
        let predecessor = authority.bootstrap("xtdb", Uuid::new_v4()).await.unwrap();
        let providers = Providers(BTreeMap::from([("archive".to_owned(), config.clone())]));
        providers.validate().unwrap();
        providers.commission(&authority, "xtdb", predecessor, "archive").await.unwrap();
        let scope = authority.ranked_commissioning_scope("xtdb", predecessor).await.unwrap();
        let io = rtdb_storage::IoRegion::new();
        let writer = open_provider(&config, scope, io.clone()).await.unwrap();
        let catalogue = RankedCatalog {
            manifest: ObjectId {
                database: scope.incarnation,
                kind: ObjectKind::Manifest,
                epoch: 1,
                seq: 1,
                digest: [8; 32],
            },
            position: 3,
            system_time: 4,
            key_generation: 1,
        };
        for position in 3..36 {
            writer.publish(RankedCatalog { position, ..catalogue }).await.unwrap();
        }
        drop(writer);
        io.close_and_wait().await;

        let operation = Operation { predecessor, operation_id: Uuid::new_v4() };
        authority.begin_recovery("xtdb", Intent {
            operation,
            destination_cell: None,
            loss_policy: LossPolicy::AcceptUnknownTail,
        }, None).await.unwrap();
        let wrong_config = rtdb_s3::S3Config { prefix: Some("replacement".into()), ..config };
        let replacement = Providers(BTreeMap::from([("archive".to_owned(), wrong_config)]));
        assert_eq!(
            replacement.freeze(&authority, "xtdb", operation).await.unwrap_err().code,
            "rtdb/activation-ranked-provider",
        );
        assert!(authority.status("xtdb").await.unwrap().grants_closed);
        drop(authority);

        let authority = ActivationAuthority::open_local(&path, identity).unwrap();
        let Status::Pending { ranked_closure: Some(closure), .. } =
            providers.freeze(&authority, "xtdb", operation).await.unwrap()
        else { panic!("expected original-provider closure"); };
        assert_eq!(closure.receipt.admitted, 33);
        let first = providers.outcomes(&authority, "xtdb", operation, 0).await.unwrap();
        let entries = first["catalogues"].as_array().unwrap();
        assert_eq!(entries.len(), 32);
        assert_eq!(entries[0]["position"], 3);
        assert_eq!(entries[0]["manifest"], catalogue.manifest.to_string());
        let next = first["next_after"].as_u64().unwrap();
        let last = providers.outcomes(&authority, "xtdb", operation, next).await.unwrap();
        assert_eq!(last["catalogues"][0]["position"], 35);
        assert!(last["next_after"].is_null());
    }

    #[tokio::test]
    async fn ranked_recovery_requires_the_original_closure_and_retains_it_through_supersession() {
        let data = tempfile::tempdir().unwrap();
        let identity = Uuid::new_v4();
        let mut authority =
            ActivationAuthority::open_local(&data.path().join("authority"), identity).unwrap();
        let time = AtomicU64::new(0);
        Arc::get_mut(&mut authority).unwrap().clock =
            Arc::new(move || Some(Duration::from_secs(time.fetch_add(6, Ordering::SeqCst))));

        let predecessor = authority.bootstrap("xtdb", Uuid::new_v4()).await.unwrap();
        let scope = authority
            .ranked_commissioning_scope("xtdb", predecessor)
            .await
            .unwrap();
        let provider = [29; 32];
        authority
            .commission_ranked("xtdb", predecessor, scope, provider)
            .await
            .unwrap();
        authority
            .commission_ranked("xtdb", predecessor, scope, provider)
            .await
            .unwrap();
        assert!(
            authority
                .commission_ranked("xtdb", predecessor, scope, [30; 32])
                .await
                .is_err()
        );
        assert_eq!(
            authority
                .status("xtdb")
                .await
                .unwrap()
                .grant_protocol_version,
            RANKED_RECOVERY_PROTOCOL
        );
        assert!(authority.close_grants("xtdb", predecessor).await.is_err());

        let intent = |operation_id| Intent {
            operation: Operation {
                predecessor,
                operation_id,
            },
            destination_cell: None,
            loss_policy: LossPolicy::AcceptUnknownTail,
        };
        let first = intent(Uuid::new_v4());
        let Status::Pending { candidate, .. } =
            authority.begin_recovery("xtdb", first, None).await.unwrap()
        else {
            panic!("expected a pending recovery");
        };
        let installation = |successor| VerifiedSuccessor {
            predecessor: predecessor.incarnation,
            successor,
            manifest_digest: [8; 32],
            destination_cell: None,
            key_hold: None,
            journal_hold: None,
            objects_sha256: None,
            activation_scope: Some(rtdb_db::ActivationScope::new(identity, "xtdb").unwrap()),
            source_cut: SourceCut {
                lsn: 3,
                position: 1,
                system_time: 2,
            },
        };
        authority
            .record_recovery_installation("xtdb", first.operation, installation(candidate))
            .await
            .unwrap();
        assert!(
            authority
                .ranked_to_close("xtdb", first.operation)
                .await
                .is_err()
        );
        authority
            .freeze_recovery("xtdb", first.operation)
            .await
            .unwrap();
        assert_eq!(
            authority
                .ranked_to_close("xtdb", first.operation)
                .await
                .unwrap(),
            Some((provider, scope))
        );
        assert_eq!(
            authority
                .publish_recovery("xtdb", first.operation)
                .await
                .unwrap_err()
                .code,
            "rtdb/activation-ranked-closure-required",
        );

        let closure = RankedClosure {
            provider,
            receipt: ClosedRankedAdmission {
                scope,
                admitted: 1,
                inventory_sha256: [31; 32],
            },
        };
        assert!(
            authority
                .record_ranked_closure(
                    "xtdb",
                    first.operation,
                    RankedClosure {
                        provider: [30; 32],
                        ..closure
                    }
                )
                .await
                .is_err()
        );
        authority
            .record_ranked_closure("xtdb", first.operation, closure)
            .await
            .unwrap();

        let second = intent(Uuid::new_v4());
        let Status::Pending {
            candidate,
            ranked_closure: Some(observed),
            ..
        } = authority
            .begin_recovery("xtdb", second, Some(first.operation.operation_id))
            .await
            .unwrap()
        else {
            panic!("expected the replacement recovery to retain the provider close");
        };
        assert_eq!(observed, closure);
        assert!(
            authority
                .ranked_to_close("xtdb", second.operation)
                .await
                .unwrap()
                .is_none()
        );
        authority
            .record_recovery_installation("xtdb", second.operation, installation(candidate))
            .await
            .unwrap();
        let published = authority
            .publish_recovery("xtdb", second.operation)
            .await
            .unwrap();
        assert!(
            matches!(published, Status::Published { ranked_closure: Some(observed), .. } if observed == closure)
        );

        drop(authority);
        let authority =
            ActivationAuthority::open_local(&data.path().join("authority"), identity).unwrap();
        assert!(matches!(
            authority.recovery_status("xtdb", second.operation).await.unwrap(),
            Status::Published { ranked_closure: Some(observed), .. } if observed == closure
        ));
        let successor = authority.binding("xtdb").await.unwrap();
        let (_, state) = authority
            .read(authority.stream_id("xtdb").unwrap())
            .await
            .unwrap();
        assert!(state.unwrap().require_ranked_closure().unwrap().is_none());
        assert_ne!(
            authority
                .ranked_commissioning_scope("xtdb", successor)
                .await
                .unwrap(),
            scope
        );
    }
}
