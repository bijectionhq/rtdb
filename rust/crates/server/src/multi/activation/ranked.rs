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
            && self.receipt.staging.valid()
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
    #[cfg(all(feature = "backup-object-holds", any(feature = "s3", test)))]
    pub(in crate::multi) async fn open_ranked_writer(
        &self,
        name: &str,
        incarnation: Uuid,
        config: &rtdb_s3::S3Config,
        io: std::sync::Arc<rtdb_storage::IoRegion>,
    ) -> Result<Option<std::sync::Arc<dyn rtdb_storage::ranked_admission::RankedAdmissionWriter>>> {
        let (_, state) = self.read(self.stream_id(name)?).await?;
        let Some(state) = state.filter(|state| state.binding.incarnation == incarnation && !state.grants_closed) else {
            return Ok(None);
        };
        let Some(provider) = state.ranked.and_then(|ranked| ranked.provider) else {
            return Ok(None);
        };
        if state.destination_cell() != self.serving_cell || provider_id(config)? != provider {
            return Err(provider_unavailable());
        }
        let admission = open_provider(config, self.ranked_scope(name, &state)?, io).await?;
        Ok(Some(std::sync::Arc::new(admission)))
    }

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

#[cfg(feature = "backup-object-holds")]
type ProviderConfig = rtdb_s3::S3Config;
#[cfg(not(feature = "backup-object-holds"))]
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

#[cfg(feature = "backup-object-holds")]
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

#[cfg(feature = "backup-object-holds")]
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

#[cfg(feature = "backup-object-holds")]
async fn commission_scope(config: &ProviderConfig, scope: RankedAdmissionScope) -> Result<()> {
    let io = rtdb_storage::IoRegion::new();
    let result = async {
        let root = rtdb_storage::ObjectKey::new("multi")?;
        config.versioned_inventory(&root, &BTreeMap::new()).await?.require_versioned().await?;
        open_provider(config, scope, io.clone())
            .await?
            .commission()
            .await
    }
    .await;
    io.close_and_wait().await;
    result
}

#[cfg(feature = "backup-object-holds")]
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

#[cfg(feature = "backup-object-holds")]
async fn read_outcomes(
    config: &ProviderConfig,
    receipt: ClosedRankedAdmission,
    after: u64,
) -> Result<Value> {
    let io = rtdb_storage::IoRegion::new();
    let result = async {
        if after > receipt.admitted.max(receipt.staging.completed) {
            return Err(Anomaly::incorrect("rtdb.storage/ranked-admission", "The ranked outcome cursor is beyond the closed inventories"));
        }
        let provider = open_provider(config, receipt.scope, io.clone()).await?;
        let page = provider.outcome_page(receipt, after.min(receipt.admitted), 32).await?;
        let staging = provider.staging_page(receipt, after.min(receipt.staging.completed), 32).await?;
        let catalogues = page.catalogues.into_iter().map(|entry| serde_json::json!({
            "manifest": entry.manifest.to_string(), "position": entry.position,
            "system_time": entry.system_time, "key_generation": entry.key_generation,
        })).collect::<Vec<_>>();
        let staged = staging.objects.into_iter().map(|entry| serde_json::json!({
            "object": entry.object.to_string(), "attempt": entry.attempt, "physical": entry.physical,
        })).collect::<Vec<_>>();
        ranked_outcome_frame(receipt, after, catalogues, staged)
    }.await;
    io.close_and_wait().await;
    result
}

#[cfg(feature = "backup-object-holds")]
fn ranked_outcome_frame(
    receipt: ClosedRankedAdmission, after: u64, mut catalogues: Vec<Value>, mut staged: Vec<Value>,
) -> Result<Value> {
    let mut count = catalogues.len().max(staged.len());
    loop {
        let position = after.saturating_add(count as u64);
        let next_after = (position < receipt.admitted.max(receipt.staging.completed)).then_some(position);
        let frame = serde_json::json!({ "scope": receipt, "catalogues": &catalogues, "staged": &staged, "next_after": next_after });
        let reply: Result<&Value> = Ok(&frame);
        if serde_json::to_vec(&reply).map_err(|_| provider_unavailable())?.len() <= super::BROKER_FRAME_BYTES {
            return Ok(frame);
        }
        if count <= 1 {
            return Err(Anomaly::fault("rtdb/activation-ranked-provider", "The original ranked outcome exceeds the activation broker's frame limit"));
        }
        count /= 2;
        catalogues.truncate(count);
        staged.truncate(count);
    }
}

#[cfg(not(feature = "backup-object-holds"))]
fn provider_id(_: &ProviderConfig) -> Result<[u8; 32]> {
    Err(provider_unavailable())
}

#[cfg(not(feature = "backup-object-holds"))]
async fn commission_scope(_: &ProviderConfig, _: RankedAdmissionScope) -> Result<()> {
    Err(provider_unavailable())
}

#[cfg(not(feature = "backup-object-holds"))]
async fn close_scope(_: &ProviderConfig, _: RankedAdmissionScope) -> Result<ClosedRankedAdmission> {
    Err(provider_unavailable())
}

#[cfg(not(feature = "backup-object-holds"))]
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

    #[cfg(feature = "backup-object-holds")]
    #[test]
    fn ranked_outcomes_share_a_cursor_and_fit_the_broker_frame() {
        let receipt = ClosedRankedAdmission {
            scope: RankedAdmissionScope { incarnation: Uuid::new_v4(), activation_sha256: [1; 32] },
            admitted: 8,
            inventory_sha256: [2; 32],
            staging: rtdb_storage::ranked_admission::RankedStagingInventory { completed: 32, inventory_sha256: [3; 32] },
        };
        let catalogues = (1..=8).map(|position| serde_json::json!({ "position": position })).collect();
        let staged = (1..=32).map(|sequence| serde_json::json!({
            "sequence": sequence,
            "physical": { "version": "v".repeat(1024) },
        })).collect();
        let frame = ranked_outcome_frame(receipt, 0, catalogues, staged).unwrap();
        let reply: Result<&Value> = Ok(&frame);
        assert!(serde_json::to_vec(&reply).unwrap().len() <= super::super::BROKER_FRAME_BYTES);
        let next = frame["next_after"].as_u64().unwrap();
        assert_eq!(frame["staged"].as_array().unwrap().len(), next as usize);
        assert_eq!(frame["catalogues"].as_array().unwrap().len(), next.min(receipt.admitted) as usize);
        assert_eq!(frame["staged"][next as usize - 1]["sequence"], next);
    }

    #[cfg(feature = "backup-object-holds")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires a disposable S3-compatible loopback bucket"]
    async fn ranked_writer_exports_and_the_broker_reconciles_after_keepers_stop() {
        use std::{collections::BTreeSet, net::Ipv4Addr};

        use rtdb_crypt::FileKms;
        use rtdb_db::{MultiDatabase, MultiOptions, PortableBackup, Role};
        use rtdb_node::{Clock, MultiServing, SystemClock};
        use rtdb_tx::transact::{Cell, Declaration, Op, Outcome, Row, Transact};
        use rtdb_types::{EntityId, Interval, MemoryBudget, TableRef, rtdb_v1::MAIN_BRANCH};
        use stream_logkeeper::stream::consensus::{CLUSTER_VERSION_CURRENT, testing::LocalCluster};
        use tokio_util::sync::CancellationToken;

        let config = test_provider();
        let directory = tempfile::tempdir().unwrap();
        let authority = ActivationAuthority::open_local(&directory.path().join("authority"), Uuid::new_v4()).unwrap();
        let binding = authority.bootstrap("xtdb", Uuid::new_v4()).await.unwrap();
        let providers = Providers(BTreeMap::from([("archive".to_owned(), config.clone())]));
        providers.commission(&authority, "xtdb", binding, "archive").await.unwrap();
        let io = rtdb_storage::IoRegion::new();
        let root = rtdb_storage::ObjectKey::new("multi").unwrap();
        let store = config.open(&root, &BTreeMap::new(), io.clone()).await.unwrap();
        let ranked = authority.open_ranked_writer("xtdb", binding.incarnation, &config, io.clone())
            .await.unwrap().unwrap();
        let vault = directory.path().join("keys");
        FileKms::create_directory(&vault).unwrap();
        let keys: Arc<dyn rtdb_crypt::Kms> = Arc::new(FileKms::open(&vault, || SystemClock.now().0).unwrap());
        const PRINCIPAL: &str = "urn:logkeeper:client:ranked-writer";
        let mut keepers = LocalCluster::start_replicated_catalog(&[PRINCIPAL]).await.unwrap();
        let placement = keepers.journal_config(64 << 20, 4 << 20);
        let journal = Arc::new(keepers.journal(PRINCIPAL, &placement).unwrap());
        journal.finalize_version(CLUSTER_VERSION_CURRENT).await.unwrap();
        let node = Ipv4Addr::LOCALHOST.into();
        let database = MultiDatabase::new(MultiOptions {
            database: binding.incarnation,
            node,
            lease_micros: 10_000_000,
            drift_micros: 1_000,
        }, journal, store, keys.clone(), Arc::new(|| SystemClock.now())).unwrap()
            .with_immutable_journal()
            .with_activation_scope(authority.scope("xtdb").unwrap()).unwrap()
            .with_serving_authority(authority.serving_authority("xtdb", binding.incarnation).unwrap())
            .with_ranked_admission(ranked).unwrap();
        database.create().await.unwrap();
        let serving = MultiServing::new(database, MemoryBudget::new(64 << 20));
        assert!(matches!(serving.tick(&BTreeSet::from([node])).await.unwrap(), Role::Writer(_)));
        let write = |key, ops| Transact {
            custody_request_digest: None,
            request_memory: None,
            key: vec![key; 16],
            require: vec![],
            window: Default::default(),
            guards: vec![],
            requires: vec![],
            otherwise: vec![],
            ops,
        };
        let table = TableRef::new("public", "docs");
        let change: Declaration = serde_json::from_value(serde_json::json!({"RankedIndex": {
            "name": "nearest", "definition": {"columns": ["embedding"], "kind": {"Ivf": {
                "dimensions": 2, "recall_bits": 0.95_f64.to_bits(),
            }}},
        }})).unwrap();
        let declared = serving.transact_local(write(16, vec![Op::Declare { table: table.clone(), change }]), None)
            .await.unwrap();
        assert_eq!(declared.outcome, Outcome::Then);
        let receipt = serving.transact_local(write(17, vec![Op::Put {
                branch: MAIN_BRANCH.into(),
                table,
                if_absent: false,
                rows: vec![Row {
                    id: EntityId::Integer(1),
                    valid: Some(Interval::ALL.into()),
                    custody_group: None,
                    protected_columns: vec![],
                    custody_payload: None,
                    cells: rtdb_types::StructFields::from([("embedding".into(), Cell::Value(rtdb_types::Value::list(vec![
                        rtdb_types::Value::F32(1.0), rtdb_types::Value::F32(0.0),
                    ])))]),
                }],
            }]), None).await.unwrap();
        assert_eq!(receipt.outcome, Outcome::Then);
        assert!(serving.maintenance_schedule().await.ranked_due.is_some());
        serving.maintain_ranked_catalog(&CancellationToken::new()).await.unwrap();
        assert!(serving.maintenance_schedule().await.ranked_due.is_none());
        let archive = serving.portable_backup().await.unwrap();
        let verified = PortableBackup::verify(&archive.into_bytes(), keys).await.unwrap();
        assert_eq!(verified.source_basis(), receipt.basis);
        assert_eq!(verified.source_receipt(&[17; 16]).await.unwrap().basis, receipt.basis);
        serving.shutdown_writer().await.unwrap();
        drop(serving);
        io.close_and_wait().await;
        keepers.shutdown().await.unwrap();

        let operation = Operation { predecessor: binding, operation_id: Uuid::new_v4() };
        authority.begin_recovery("xtdb", Intent {
            operation,
            destination_cell: None,
            loss_policy: LossPolicy::AcceptUnknownTail,
        }, None).await.unwrap();
        providers.freeze(&authority, "xtdb", operation).await.unwrap();
        let outcome = providers.outcomes(&authority, "xtdb", operation, 0).await.unwrap();
        assert_eq!(outcome["scope"]["admitted"], 1);
        assert_eq!(outcome["catalogues"][0]["position"], receipt.basis.position());
        assert_eq!(outcome["catalogues"][0]["system_time"], receipt.system_time.0);
        let staged = outcome["staged"].as_array().unwrap();
        assert!(staged.len() >= 2);
        assert_eq!(outcome["scope"]["staging"]["completed"], staged.len());
        let manifest = staged.iter().find(|entry| entry["object"] == outcome["catalogues"][0]["manifest"]).unwrap();
        assert_eq!(manifest["attempt"]["position"], receipt.basis.position());
        assert_eq!(manifest["attempt"]["key_generation"], outcome["catalogues"][0]["key_generation"]);
        assert_eq!(manifest["physical"]["key"], format!("multi/{}", manifest["object"].as_str().unwrap()));
        assert_eq!(manifest["physical"]["namespace"]["bucket"], config.bucket);
    }

    #[cfg(feature = "backup-object-holds")]
    fn test_provider() -> rtdb_s3::S3Config {
        let endpoint = std::env::var("RTDB_BACKUP_HOLD_TEST_ENDPOINT").unwrap();
        assert_eq!(url::Url::parse(&endpoint).unwrap().host_str(), Some("127.0.0.1"));
        let bucket = std::env::var("RTDB_BACKUP_HOLD_TEST_BUCKET").unwrap();
        assert!(bucket.starts_with("rtdb-backup-hold-test-"));
        let mut config = rtdb_s3::S3Config::new(bucket);
        config.endpoint = Some(endpoint);
        config.region = Some("us-east-1".into());
        config
    }

    #[cfg(feature = "backup-object-holds")]
    #[tokio::test]
    #[ignore = "requires a disposable S3-compatible loopback bucket"]
    async fn ranked_broker_reads_the_original_catalogues_after_source_loss() {
        use rtdb_format::journal::{ObjectId, ObjectKind, RankedCatalog};

        let config = test_provider();
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
                staging: Default::default(),
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
