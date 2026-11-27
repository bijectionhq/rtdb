use super::activation::{ActivationAuthority, VerifiedSuccessor};
use journal_contract::Journal;
use rtdb_crypt::LocalKms;
use rtdb_db::{MultiDatabase, MultiOptions, PortableBackup, Role};
use rtdb_node::{Clock, MultiServing, SystemClock, serving::ServingDriver};
use rtdb_storage::MemoryStore;
use rtdb_tx::transact::{Cell, Op, Outcome, Row, Transact};
use rtdb_types::{EntityId, StructFields, TableRef, Value, rtdb_v1::MAIN_BRANCH};
use std::{
    collections::BTreeSet,
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
    time::Duration,
};
use stream_logkeeper::stream::consensus::{CLUSTER_VERSION_CURRENT, testing::LocalCluster};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const PRINCIPAL: &str = "urn:logkeeper:client:multi-endgame";
const NAME: &str = "xtdb";
const NODE: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

async fn journal() -> (LocalCluster, Arc<dyn Journal>) {
    let keepers = LocalCluster::start_replicated_catalog(&[PRINCIPAL])
        .await
        .unwrap();
    let config = keepers.journal_config(64 << 20, 4 << 20);
    let journal = keepers.journal(PRINCIPAL, &config).unwrap();
    let version = journal
        .client()
        .finalize_version(CLUSTER_VERSION_CURRENT)
        .await
        .unwrap();
    assert_eq!(version, CLUSTER_VERSION_CURRENT);
    (keepers, Arc::new(journal))
}

fn database(
    incarnation: Uuid,
    journal: Arc<dyn Journal>,
    store: Arc<MemoryStore>,
    keys: Arc<LocalKms>,
) -> MultiDatabase {
    MultiDatabase::new(
        MultiOptions {
            database: incarnation,
            node: NODE,
            lease_micros: 60_000_000,
            drift_micros: 0,
        },
        journal,
        store,
        keys,
        Arc::new(|| SystemClock.now()),
    )
    .unwrap()
    .with_immutable_journal()
    .with_binary_checkpoints(true)
}

fn transaction(key: u8) -> Transact {
    Transact {
        custody_request_digest: None,
        request_memory: None,
        key: vec![key; 16],
        require: Vec::new(),
        window: Default::default(),
        guards: Vec::new(),
        requires: Vec::new(),
        otherwise: Vec::new(),
        ops: vec![Op::Put {
            branch: MAIN_BRANCH.into(),
            table: TableRef::new("public", "recovery"),
            rows: vec![Row {
                id: EntityId::Integer(i64::from(key)),
                valid: None,
                custody_group: None,
                protected_columns: Vec::new(),
                custody_payload: None,
                cells: StructFields::from([(
                    "payload".into(),
                    Cell::Value(Value::Utf8("verified".into())),
                )]),
            }],
            if_absent: false,
        }],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn portable_cut_replaces_a_lost_cell_and_fences_its_return() {
    let (mut authority_keepers, control_journal) = journal().await;
    let authority = ActivationAuthority::new(control_journal, Uuid::new_v4()).unwrap();
    let predecessor_id = Uuid::new_v4();
    let successor_id = Uuid::new_v4();
    let predecessor_binding = authority.bootstrap(NAME, predecessor_id).await.unwrap();
    let (mut source_keepers, source_journal) = journal().await;
    let source_keeper_ids = source_keepers.keepers().keys().copied().collect::<Vec<_>>();
    let source_store = Arc::new(MemoryStore::default());
    let source_keys = Arc::new(LocalKms::new(|| SystemClock.now().0));
    let source = database(
        predecessor_id,
        source_journal.clone(),
        source_store.clone(),
        source_keys.clone(),
    )
    .with_serving_authority(authority.serving_authority(NAME, predecessor_id).unwrap());
    source.create().await.unwrap();
    let live = BTreeSet::from([NODE]);
    let source = MultiServing::new(source, rtdb_types::MemoryBudget::new(64 << 20));
    assert!(matches!(source.tick(&live).await.unwrap(), Role::Writer(_)));
    let committed = source.transact_local(transaction(1), None).await.unwrap();
    assert_eq!(committed.outcome, Outcome::Then);
    let archive = source.portable_backup().await.unwrap().into_bytes();

    drop(source);
    source_keepers.shutdown().await.unwrap();
    authority
        .close_grants(NAME, predecessor_binding)
        .await
        .unwrap();

    let (mut destination_keepers, destination_journal) = journal().await;
    let destination_store = Arc::new(MemoryStore::default());
    let destination_keys = Arc::new(LocalKms::new(|| SystemClock.now().0));
    let mut staging = database(
        successor_id,
        destination_journal.clone(),
        destination_store.clone(),
        destination_keys.clone(),
    );
    staging.create().await.unwrap();
    assert!(matches!(
        staging.tick(&live).await.unwrap(),
        Role::Writer(_)
    ));
    let verified = PortableBackup::verify(&archive, source_keys.clone())
        .await
        .unwrap();
    let original_receipt = verified.source_receipt(&transaction(1).key).await.unwrap();
    assert_eq!(original_receipt.basis, committed.basis);
    staging.install_portable_backup(verified).await.unwrap();
    assert_eq!(
        staging
            .tick(&BTreeSet::from([IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2))]))
            .await
            .unwrap(),
        Role::Follower
    );
    drop(staging);

    let mut staged_reopen = database(
        successor_id,
        destination_journal.clone(),
        destination_store.clone(),
        destination_keys.clone(),
    );
    let installed = staged_reopen.installed_backup().await.unwrap().unwrap();
    let successor = VerifiedSuccessor::from_installation(&installed);
    let operation = Uuid::new_v4();
    let active = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match authority
                .publish_successor(NAME, predecessor_binding, operation, successor)
                .await
            {
                Ok(active) => break active,
                Err(error) if error.code == "rtdb/activation-draining" => {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                Err(error) => panic!("successor publication failed: {error:?}"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(active.incarnation, successor_id);
    assert_eq!(active.revision, predecessor_binding.revision + 1);
    assert_eq!(
        authority
            .publish_successor(NAME, predecessor_binding, operation, successor)
            .await
            .unwrap(),
        active
    );

    let destination = MultiServing::new(
        staged_reopen
            .with_serving_authority(authority.serving_authority(NAME, successor_id).unwrap()),
        rtdb_types::MemoryBudget::new(64 << 20),
    );
    let cancellation = CancellationToken::new();
    let recovered = destination
        .tx_status(&transaction(1).key, &cancellation)
        .await
        .unwrap();
    assert_eq!(recovered.outcome, committed.outcome);
    assert_eq!(recovered.basis.incarnation(), *successor_id.as_bytes());
    assert!(matches!(
        destination.tick(&live).await.unwrap(),
        Role::Writer(_)
    ));
    assert_eq!(
        destination
            .transact_local(transaction(3), None)
            .await
            .unwrap()
            .outcome,
        Outcome::Then
    );

    for keeper in source_keeper_ids {
        source_keepers.restart(keeper).await.unwrap();
    }
    let returned = MultiServing::new(
        database(predecessor_id, source_journal, source_store, source_keys)
            .with_serving_authority(authority.serving_authority(NAME, predecessor_id).unwrap()),
        rtdb_types::MemoryBudget::new(64 << 20),
    );
    assert_eq!(
        returned
            .transact_local(transaction(2), None)
            .await
            .unwrap_err()
            .code,
        "rtdb/activation-fenced"
    );
    source_keepers.shutdown().await.unwrap();
    destination_keepers.shutdown().await.unwrap();
    authority_keepers.shutdown().await.unwrap();
}
