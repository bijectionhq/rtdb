use std::process::{Child, Command, Stdio};

use rtdb_db::WritePipeline;
use rtdb_node::DecisionClock;
use rtdb_tx::transact::{Cell, DECISION_CLOCK_OFFER, FundingCommand, Op, Row, Transact};
use rtdb_types::{EntityId, Interval, StructFields, TableRef, rtdb_v1::MAIN_BRANCH};
use stream_logkeeper::stream::consensus::{CLUSTER_VERSION_CURRENT, testing::LocalCluster};
use tokio::sync::Mutex;

use super::*;

struct ClockProcess(Child);

impl Drop for ClockProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn start_receiver(keepers: &LocalCluster, source: Uuid, home: &Path) -> ClockProcess {
    let fixture =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../bench/v1_timed_original_fixture.py");
    let openssl = std::env::var_os("RTDB_FUNDED_TEST_OPENSSL").unwrap_or_else(|| "openssl".into());
    let command = || {
        let mut command = Command::new("python3");
        command.arg(&fixture).arg("--openssl").arg(&openssl);
        command
    };
    let mut initialize = command();
    initialize
        .arg("initialize")
        .arg(home)
        .args(["--source", &source.to_string()]);
    for voter in keepers.voters() {
        initialize
            .arg("--keeper")
            .arg(keepers.write_config(voter).unwrap());
    }
    let initialized = initialize.output().unwrap();
    assert!(
        initialized.status.success(),
        "{}",
        String::from_utf8_lossy(&initialized.stderr)
    );
    let mut process = ClockProcess(
        command()
            .arg("serve")
            .arg(home)
            .stdout(Stdio::null())
            .spawn()
            .unwrap(),
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        while !home.join("keeper-clock.sock").exists() {
            assert!(process.0.try_wait().unwrap().is_none());
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    process
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the loopback receiver fixture, Python 3 and OpenSSL 3"]
async fn commissioned_funded_dispatch_leaves_the_live_receiver_and_journal_untouched() {
    const PRINCIPAL: &str = "urn:logkeeper:client:funded-qualification";
    const DATABASE: &str = "funded-qualification";
    let home = tempfile::Builder::new()
        .prefix("rtdb-fund-")
        .tempdir_in("/tmp")
        .unwrap();
    let source = Uuid::new_v4();
    let mut keepers = LocalCluster::start_timed(&[PRINCIPAL]).await.unwrap();
    let placement = keepers.journal_config(8 << 20, 1 << 20);
    let journal = Arc::new(
        keepers
            .journal(PRINCIPAL, &placement)
            .unwrap()
            .with_timed_original(keepers.timed_verifier(), Some(64 << 10)),
    );
    journal
        .finalize_version(CLUSTER_VERSION_CURRENT)
        .await
        .unwrap();

    let clock_home = home.path().join("clock");
    let receiver = start_receiver(&keepers, source, &clock_home).await;
    let profile = fs::read_to_string(clock_home.join("receiver-profile.yaml")).unwrap();
    let mut configuration =
        NodeConfiguration::from_yaml_with_environment(&profile, &|_| None, false).unwrap();
    let clock = configuration
        .take_native_receiver_clock(true)
        .unwrap()
        .unwrap();
    assert!(clock.funded_offers_for(*source.as_bytes()).await);
    let lineage = || fs::read(clock_home.join("receiver-lineage.json")).unwrap();
    let before_booking = lineage();

    let store = Arc::new(rtdb_storage::MemoryStore::default());
    let keys = Arc::new(rtdb_crypt::LocalKms::new(|| SystemClock.now().0));
    let batch = Arc::new(ConsensusJournalBatch(journal.clone()));
    let node = Ipv4Addr::LOCALHOST.into();
    let live = BTreeSet::from([node]);
    let database = || {
        MultiDatabase::new(
            MultiOptions {
                database: source,
                node,
                lease_micros: 60_000_000,
                drift_micros: 0,
            },
            journal.clone(),
            store.clone(),
            keys.clone(),
            Arc::new(|| SystemClock.now()),
        )
        .unwrap()
        .with_immutable_journal()
        .with_batch_appender(batch.clone())
    };
    let mut unscoped = database();
    unscoped.create().await.unwrap();
    assert!(matches!(
        unscoped.tick(&live).await.unwrap(),
        Role::Writer(_)
    ));
    assert!(unscoped.supports_funded_transact());
    let stream = unscoped.journal_stream();
    let predecessor = Basis::new(*source.as_bytes(), 0);
    let request = Transact {
        key: vec![1; 16],
        require: vec![DECISION_CLOCK_OFFER.into()],
        window: Default::default(),
        guards: vec![],
        requires: vec![],
        otherwise: vec![],
        custody_request_digest: None,
        request_memory: None,
        ops: vec![Op::Put {
            branch: MAIN_BRANCH.into(),
            table: TableRef::new("public", "funded"),
            if_absent: false,
            rows: vec![Row {
                id: EntityId::Integer(1),
                valid: Some(Interval::ALL.into()),
                cells: StructFields::from([("reading".into(), Cell::TxTime)]),
                custody_group: None,
                protected_columns: vec![],
                custody_payload: None,
            }],
        }],
    };
    let original = WritePipeline::start(Arc::new(Mutex::new(unscoped)));
    let booking = [3; 16];
    let booked = original
        .transact_funded(
            request.clone(),
            None,
            [2; 32],
            FundingCommand::Book {
                predecessor,
                booking,
            },
            clock.clone(),
        )
        .await
        .unwrap();
    assert_eq!(booked.decision_clock.unwrap().booking(), booking);
    assert_ne!(lineage(), before_booking);
    original.shutdown().await.unwrap();
    drop(original);

    let mut authority_keepers = LocalCluster::start_independent_catalog(&[PRINCIPAL])
        .await
        .unwrap();
    let control = authority_keepers
        .journal(
            PRINCIPAL,
            &authority_keepers.journal_config(8 << 20, 1 << 20),
        )
        .unwrap();
    control
        .finalize_version(CLUSTER_VERSION_CURRENT)
        .await
        .unwrap();
    let authority = ActivationAuthority::new(Arc::new(control), Uuid::new_v4()).unwrap();
    authority.bootstrap(DATABASE, source).await.unwrap();
    let mut commissioned =
        database().with_serving_authority(authority.serving_authority(DATABASE, source).unwrap());
    assert!(matches!(
        commissioned.tick(&live).await.unwrap(),
        Role::Writer(_)
    ));
    assert!(!commissioned.supports_funded_transact());
    assert_eq!(
        commissioned.tx_status(&request.key).await.unwrap_err().code,
        "rtdb/transaction-pending"
    );
    let head = journal.head(stream).await.unwrap();
    let last_observation = lineage();
    let commissioned = WritePipeline::start(Arc::new(Mutex::new(commissioned)));
    let error = commissioned
        .transact_funded(
            request,
            None,
            [2; 32],
            FundingCommand::Consume { booking },
            clock.clone(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, "rtdb/activation-effects");
    assert_eq!(lineage(), last_observation);
    assert_eq!(journal.head(stream).await.unwrap(), head);
    assert!(clock.funded_offers_for(*source.as_bytes()).await);
    assert_ne!(lineage(), last_observation);

    commissioned.shutdown().await.unwrap();
    drop(commissioned);
    drop(receiver);
    authority_keepers.shutdown().await.unwrap();
    keepers.shutdown().await.unwrap();
}
