#[cfg(not(unix))]
use super::activation::VerifiedSuccessor;
use super::activation::{ActivationAuthority, recovery::{Intent, LossPolicy, Operation, Status}};
use journal_contract::Journal;
use rtdb_crypt::FileKms;
use rtdb_db::{ActivationScope, MultiDatabase, MultiOptions, PortableBackup, Role};
use rtdb_node::{Clock, MultiServing, SystemClock, serving::ServingDriver};
use rtdb_storage::{MemoryStore, ObjectStore};
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
    keys: Arc<FileKms>,
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

fn vault(directory: &tempfile::TempDir, name: &str) -> Arc<FileKms> {
    let path = directory.path().join(name);
    FileKms::create_directory(&path).unwrap();
    Arc::new(FileKms::open(&path, || SystemClock.now().0).unwrap())
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
    let authority_id = Uuid::new_v4();
    let authority = ActivationAuthority::new(control_journal.clone(), authority_id).unwrap();
    let activation_scope = ActivationScope::new(authority_id, NAME).unwrap();
    let predecessor_id = Uuid::new_v4();
    let predecessor_binding = authority.bootstrap(NAME, predecessor_id).await.unwrap();
    let operation = Uuid::new_v4();
    let recovery = Operation { predecessor: predecessor_binding, operation_id: operation };
    let destination_cell = cfg!(unix).then(Uuid::new_v4);
    let successor_id = authority
        .successor_incarnation_in_cell(NAME, predecessor_binding, operation, destination_cell)
        .unwrap();
    let destination_scope = ActivationScope::for_cell(authority.identity(), NAME, destination_cell).unwrap();
    let vaults = tempfile::tempdir().unwrap();
    let (mut source_keepers, source_journal) = journal().await;
    let source_keeper_ids = source_keepers.keepers().keys().copied().collect::<Vec<_>>();
    let source_store = Arc::new(MemoryStore::default());
    let source_keys = vault(&vaults, "source");
    let source = database(
        predecessor_id,
        source_journal.clone(),
        source_store.clone(),
        source_keys.clone(),
    )
    .with_activation_scope(activation_scope)
    .unwrap()
    .with_serving_authority(authority.serving_authority(NAME, predecessor_id).unwrap());
    source.create().await.unwrap();
    let live = BTreeSet::from([NODE]);
    let source = MultiServing::new(source, rtdb_types::MemoryBudget::new(64 << 20));
    assert!(matches!(source.tick(&live).await.unwrap(), Role::Writer(_)));
    let committed = source.transact_local(transaction(1), None).await.unwrap();
    assert_eq!(committed.outcome, Outcome::Then);
    let stream = journal_contract::StreamId(predecessor_id.as_u128());
    let head = source_journal.head(stream).await.unwrap();
    let objects = source_store.list(None).await.unwrap().len();
    assert!(source.maintenance_schedule().await.ranked_due.is_none());
    source.maintain_ranked_catalog(&CancellationToken::new()).await.unwrap();
    assert!(source.maintenance_schedule().await.ranked_cooldown.is_none());
    assert_eq!(source_journal.head(stream).await.unwrap(), head);
    assert_eq!(source_store.list(None).await.unwrap().len(), objects);
    let archive = source.portable_backup().await.unwrap().into_bytes();

    drop(source);
    source_keepers.shutdown().await.unwrap();
    assert!(matches!(
        authority.begin_recovery(NAME, Intent {
            operation: recovery,
            destination_cell,
            loss_policy: LossPolicy::AcceptUnknownTail,
        }, None).await.unwrap(),
        Status::Pending { candidate, grants_closed: false, .. } if candidate == successor_id
    ));
    authority.freeze_recovery(NAME, recovery).await.unwrap();

    let (mut destination_keepers, destination_journal) = journal().await;
    let destination_store = Arc::new(MemoryStore::default());
    let destination_keys = vault(&vaults, "destination");
    let mut staging = database(
        successor_id,
        destination_journal.clone(),
        destination_store.clone(),
        destination_keys.clone(),
    )
    .with_activation_scope(destination_scope)
    .unwrap();
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
    )
    .with_activation_scope(destination_scope)
    .unwrap();
    let installed = staged_reopen.installed_backup().await.unwrap().unwrap();
    let offline = MultiServing::new(
        database(successor_id, destination_journal.clone(), destination_store.clone(), destination_keys.clone())
            .with_activation_scope(destination_scope).unwrap(),
        rtdb_types::MemoryBudget::new(64 << 20),
    );
    assert_eq!(
        offline.tx_status(&transaction(1).key, &CancellationToken::new()).await.unwrap_err().code,
        "rtdb/activation-fenced"
    );
    let destination_stream = journal_contract::StreamId(successor_id.as_u128());
    let head = destination_journal.head(destination_stream).await.unwrap();
    assert!(destination_journal.read_page(
        destination_stream, journal_contract::Lsn(1), head, 1, Duration::ZERO
    ).await.is_err_and(|error| matches!(error, journal_contract::JournalError::Trimmed { .. })));
    let uncommissioned = database(
        successor_id, destination_journal.clone(), destination_store.clone(), destination_keys.clone(),
    );
    assert_eq!(
        uncommissioned.check_activation_scope(head).await.unwrap_err().code,
        "rtdb/activation-scope"
    );
    let wrong_authority = uncommissioned.with_activation_scope(
        ActivationScope::new(Uuid::new_v4(), NAME).unwrap()
    ).unwrap();
    assert_eq!(
        wrong_authority.check_activation_scope(head).await.unwrap_err().code,
        "rtdb/activation-scope"
    );
    #[cfg(not(unix))]
    let successor = VerifiedSuccessor::from_installation(&installed);
    #[cfg(unix)]
    let publication = super::activation::operator::testing::Installation::attest(
        &authority,
        NAME,
        predecessor_binding,
        operation,
        destination_cell.unwrap(),
        &installed,
    );
    #[cfg(unix)]
    assert_eq!(publication.record(&authority).await.unwrap()["stage"], "pending");
    #[cfg(not(unix))]
    authority.record_recovery_installation(NAME, recovery, successor).await.unwrap();
    let destination_keeper_ids = destination_keepers.keepers().keys().copied().collect::<Vec<_>>();
    destination_keepers.shutdown().await.unwrap();

    drop(authority);
    let authority = ActivationAuthority::new(control_journal.clone(), authority_id).unwrap();
    assert!(matches!(
        authority.recovery_status(NAME, recovery).await.unwrap(),
        Status::Pending { grants_closed: true, installation: Some(_), .. }
    ));
    let _active = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            #[cfg(unix)]
            let result = publication.publish_without_proof(&authority).await;
            #[cfg(not(unix))]
            let result = authority.publish_recovery(NAME, recovery).await;
            match result {
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
    #[cfg(unix)]
    {
        assert_eq!(_active["stage"], "published");
        assert_eq!(_active["binding"]["incarnation"], successor_id.to_string());
        assert_eq!(_active["binding"]["revision"], predecessor_binding.revision + 1);
        assert_eq!(_active["intent"]["destination_cell"], destination_cell.unwrap().to_string());
        assert_eq!(_active["intent"]["loss_policy"], "accept-unknown-tail");
        assert!(_active["final_source_lsn"].is_null());
        assert_eq!(publication.publish_without_proof(&authority).await.unwrap(), _active);
        let wrong_cell = Uuid::new_v4();
        assert_ne!(
            authority.successor_incarnation_in_cell(NAME, predecessor_binding, operation, Some(wrong_cell)).unwrap(),
            successor_id,
        );
        for cell in [None, Some(wrong_cell)] {
            assert_eq!(
                authority.serving_authority_for_cell(NAME, successor_id, cell).unwrap()
                    .acquire().await.unwrap_err().code,
                "rtdb/activation-fenced",
            );
        }
    }
    let binding = authority.binding(NAME).await.unwrap();
    assert_eq!(binding.incarnation, successor_id);
    assert_eq!(binding.revision, predecessor_binding.revision + 1);
    let control_stream = authority.commissioning_stream_id(NAME).unwrap();
    let control_head = control_journal.head(control_stream).await.unwrap();
    assert!(control_head.0 > 1);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let page = control_journal
                .read_page(control_stream, control_head, control_head, 1, Duration::ZERO)
                .await
                .unwrap();
            if page.floor == control_head {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("publication keeps just its verified authority checkpoint");
    #[cfg(not(unix))]
    assert!(matches!(
        authority.publish_recovery(NAME, recovery).await.unwrap(),
        Status::Published { binding: published, .. } if published == binding
    ));

    for keeper in destination_keeper_ids {
        destination_keepers.restart(keeper).await.unwrap();
    }
    let destination = MultiServing::new(
        staged_reopen
            .with_serving_authority(authority.serving_authority_for_cell(NAME, successor_id, destination_cell).unwrap()),
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
            .with_activation_scope(activation_scope)
            .unwrap()
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
    let next = Operation { predecessor: binding, operation_id: Uuid::new_v4() };
    authority.begin_recovery(NAME, Intent {
        operation: next,
        destination_cell: None,
        loss_policy: LossPolicy::Lossless,
    }, None).await.unwrap();
    authority.freeze_recovery(NAME, next).await.unwrap();
    let frozen = authority.status(NAME).await.unwrap();
    assert!(frozen.grants_closed);
    assert_eq!(frozen.destination_cell, destination_cell);
    assert_eq!(frozen.grant_protocol_version, 4);
    source_keepers.shutdown().await.unwrap();
    destination_keepers.shutdown().await.unwrap();
    authority_keepers.shutdown().await.unwrap();
}
