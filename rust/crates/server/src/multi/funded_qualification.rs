use std::{
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicUsize, Ordering},
};

use journal_contract::{CheckpointInstallation, Epoch, JournalError, Lsn, WriterSessionId};
use rtdb_db::{TimedOriginal, WritePipeline};
use rtdb_node::DecisionClock;
use rtdb_stream_journal::{KeeperId, TimedOriginalVerifier};
use rtdb_tx::transact::{Cell, DECISION_CLOCK_OFFER, FundingCommand, Op, Row, Transact};
use rtdb_types::{
    EntityId, Interval, StructFields, TableRef, TimedJournalCutoff, Timestamp, rtdb_v1::MAIN_BRANCH,
};
use sha2::{Digest, Sha256};
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn funded_acceptance_proof_survives_replacement_and_expiry_until_checkpoint_trim() {
    const PRINCIPAL: &str = "urn:logkeeper:client:funded-proof-retention";
    let mut keepers = LocalCluster::start_timed(&[PRINCIPAL]).await.unwrap();
    let journal = Arc::new(
        keepers
            .journal(PRINCIPAL, &keepers.journal_config(1 << 20, 4096))
            .unwrap()
            .with_timed_original(keepers.timed_verifier(), Some(2048)),
    );
    journal
        .finalize_version(CLUSTER_VERSION_CURRENT)
        .await
        .unwrap();
    let stream = StreamId(Uuid::new_v4().as_u128());
    journal.create_immutable(stream).await.unwrap();
    let writer = journal
        .seal_session(stream, Epoch(0), WriterSessionId(1))
        .await
        .unwrap();
    let batch = ConsensusJournalBatch(journal.clone());
    let payload = bytes::Bytes::from_static(b"exact funded stage");
    let original = TimedOriginal {
        stream,
        epoch: writer.fence.writer_epoch,
        at: Lsn(1),
        payload_sha256: Sha256::digest(&payload).into(),
        cutoff: TimedJournalCutoff::new(Timestamp(SystemClock.now().0 + 60_000_000), [9; 32]).unwrap(),
    };
    assert!(matches!(
        batch.prove_timed_original(original).await,
        Err(JournalError::Conflict { head: Lsn(0) })
    ));
    let accepted = batch
        .append_session_timed_original(writer.fence, original, payload)
        .await
        .unwrap();
    assert_eq!(accepted.original, original);
    assert!(accepted.quorum_sync_at < original.cutoff.before());

    keepers.shift_timed_clock(120_000_000);
    let replacement = journal
        .seal_session(stream, writer.fence.writer_epoch, WriterSessionId(2))
        .await
        .unwrap();
    assert_eq!(replacement.sealed_head_lsn, original.at);
    assert_eq!(batch.prove_timed_original(original).await.unwrap(), accepted);

    let checkpoint = journal
        .install_checkpoint(
            replacement.fence,
            CheckpointInstallation {
                operation_id: 1,
                checkpoint_lsn: Lsn(2),
                covered_through_lsn: original.at,
                record: bytes::Bytes::from_static(b"checkpoint covering funded stage"),
            },
        )
        .await
        .unwrap();
    assert_eq!(checkpoint.logical_floor_lsn, Lsn(2));
    assert!(matches!(
        batch.prove_timed_original(original).await,
        Err(JournalError::Trimmed { floor: Lsn(2) })
    ));
    keepers.shutdown().await.unwrap();
}

struct WithholdTimedAppend {
    keeper_trust: Arc<dyn TimedOriginalVerifier>,
    verified_samples: AtomicUsize,
}

impl TimedOriginalVerifier for WithholdTimedAppend {
    fn verify(&self, keeper: KeeperId, statement: &[u8], signature: &[u8; 64]) -> bool {
        if self.keeper_trust.verify(keeper, statement, signature) {
            self.verified_samples.fetch_add(1, Ordering::Relaxed);
        }
        false
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_funded_append_can_commit_after_cutoff_until_original_writer_is_fenced() {
    const PRINCIPAL: &str = "urn:logkeeper:client:funded-seal-exclusion";
    let mut keepers = LocalCluster::start_timed(&[PRINCIPAL]).await.unwrap();
    let config = keepers.journal_config(1 << 20, 4096);
    let journal = Arc::new(
        keepers
            .journal(PRINCIPAL, &config)
            .unwrap()
            .with_timed_original(keepers.timed_verifier(), Some(2048)),
    );
    journal
        .finalize_version(CLUSTER_VERSION_CURRENT)
        .await
        .unwrap();
    let withhold = Arc::new(WithholdTimedAppend {
        keeper_trust: keepers.timed_verifier(),
        verified_samples: AtomicUsize::new(0),
    });
    let staging = ConsensusJournalBatch(Arc::new(
        keepers
            .journal(PRINCIPAL, &config)
            .unwrap()
            .with_timed_original(withhold.clone(), Some(2048)),
    ));
    let stage = async |payload: bytes::Bytes| {
        let stream = StreamId(Uuid::new_v4().as_u128());
        journal.create_immutable(stream).await.unwrap();
        let writer = journal
            .seal_session(stream, Epoch(0), WriterSessionId(1))
            .await
            .unwrap();
        let original = TimedOriginal {
            stream,
            epoch: writer.fence.writer_epoch,
            at: Lsn(1),
            payload_sha256: Sha256::digest(&payload).into(),
            cutoff: TimedJournalCutoff::new(Timestamp(SystemClock.now().0 + 60_000_000), [9; 32])
                .unwrap(),
        };
        let verified_before = withhold.verified_samples.load(Ordering::Relaxed);
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match staging
                    .append_session_timed_original(writer.fence, original, payload.clone())
                    .await
                {
                    Err(JournalError::Unavailable)
                        if withhold.verified_samples.load(Ordering::Relaxed) > verified_before =>
                    {
                        break;
                    }
                    Err(JournalError::Unavailable | JournalError::NotFound) => (),
                    Err(JournalError::Fenced { current }) if current < original.epoch => (),
                    unexpected => panic!("unexpected result while staging: {unexpected:?}"),
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(journal.head(stream).await.unwrap(), Lsn(0));
        (writer.fence, original, payload)
    };
    let (open_writer, open_stage, open_payload) = stage(bytes::Bytes::from_static(b"open")).await;
    let (old_writer, closed_stage, closed_payload) =
        stage(bytes::Bytes::from_static(b"closed")).await;

    keepers.shift_timed_clock(120_000_000);
    let batch = ConsensusJournalBatch(journal.clone());
    let committed = batch
        .append_session_timed_original(open_writer, open_stage, open_payload)
        .await
        .unwrap();
    assert!(committed.quorum_sync_at < open_stage.cutoff.before());
    assert_eq!(
        batch.prove_timed_original(open_stage).await.unwrap(),
        committed
    );

    let seal = journal
        .seal_session(
            closed_stage.stream,
            old_writer.writer_epoch,
            WriterSessionId(2),
        )
        .await
        .unwrap();
    assert_eq!(seal.fence.stream, closed_stage.stream);
    assert!(seal.fence.writer_epoch > closed_stage.epoch);
    assert!(seal.sealed_head_lsn < closed_stage.at);
    assert!(matches!(
        batch.append_session_timed_original(old_writer, closed_stage, closed_payload).await,
        Err(JournalError::Fenced { current }) if current == seal.fence.writer_epoch
    ));
    assert!(matches!(
        batch.prove_timed_original(closed_stage).await,
        Err(JournalError::Conflict { head }) if head == seal.sealed_head_lsn
    ));
    keepers.shutdown().await.unwrap();
}
