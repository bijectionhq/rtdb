use std::{collections::BTreeSet, path::Path, sync::Arc, time::Duration};

use bytes::Bytes;
use journal_contract::{
    BoxFuture, Epoch, Identity, Journal, JournalError, Lsn, StreamId, WriterFence, WriterSessionId,
    reference::{FileJournal, Limits},
};
use rtdb_db::{ServingAuthority, ServingPermit};
use rtdb_node::{Clock, SystemClock};
use rtdb_types::{Anomaly, AnomalyCategory, Result, Timestamp};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use uuid::Uuid;

const MAX_GRANT_MICROS: i64 = 5_000_000;
const MAX_CLOCK_OFFSET_MICROS: i64 = 250_000;
const ATTEMPTS: usize = 8;

pub struct ActivationAuthority {
    journal: Arc<dyn Journal>,
    identity: Uuid,
    local: bool,
    clock: Arc<dyn Clock>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Binding {
    pub incarnation: Uuid,
    pub revision: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct Publication {
    operation: Uuid,
    predecessor: Binding,
    manifest_digest: [u8; 32],
}

/// Produced by the destination installer after its reconstruction is committed.
#[derive(Clone, Copy)]
pub struct VerifiedSuccessor {
    predecessor: Uuid,
    successor: Uuid,
    manifest_digest: [u8; 32],
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct State {
    version: u8,
    binding: Binding,
    grants_closed: bool,
    grants_end: Option<i64>,
    retired: BTreeSet<Uuid>,
    publication: Option<Publication>,
}

impl State {
    fn initial(incarnation: Uuid) -> Self {
        Self {
            version: 1,
            binding: Binding {
                incarnation,
                revision: 1,
            },
            grants_closed: false,
            grants_end: None,
            retired: BTreeSet::new(),
            publication: None,
        }
    }

    fn require(&self, expected: Binding) -> Result<()> {
        if self.binding != expected {
            return Err(conflict());
        }
        Ok(())
    }
}

impl ActivationAuthority {
    /// All participants in an authority must use its same independent ordered journal.
    pub fn new(journal: Arc<dyn Journal>, identity: Uuid) -> Result<Arc<Self>> {
        if identity.is_nil() {
            return Err(conflict());
        }
        Ok(Arc::new(Self {
            journal,
            identity,
            local: false,
            clock: Arc::new(SystemClock),
        }))
    }

    pub fn open_local(path: &Path, identity: Uuid) -> Result<Arc<Self>> {
        if identity.is_nil() {
            return Err(conflict());
        }
        let journal = FileJournal::open(path, Limits::default()).map_err(super::io_error)?;
        let journal = Arc::new(journal.handle(Identity::from_static("urn:rtdb:activation:local")));
        Ok(Arc::new(Self {
            journal,
            identity,
            local: true,
            clock: Arc::new(SystemClock),
        }))
    }

    pub async fn bootstrap(&self, name: &str, incarnation: Uuid) -> Result<Binding> {
        if incarnation.is_nil() {
            return Err(conflict());
        }
        let stream = self.stream_id(name)?;
        if self.local {
            self.journal.create(stream).await
        } else {
            self.journal.create_immutable(stream).await
        }
        .map_err(journal_error)?;
        let state = self
            .change(stream, |state| match state {
                None => Ok(Some(State::initial(incarnation))),
                Some(state) if state.binding == State::initial(incarnation).binding => Ok(None),
                Some(_) => Err(conflict()),
            })
            .await?;
        Ok(state.binding)
    }

    pub async fn binding(&self, name: &str) -> Result<Binding> {
        Ok(self
            .read(self.stream_id(name)?)
            .await?
            .1
            .ok_or_else(unavailable)?
            .binding)
    }

    pub async fn close_grants(&self, name: &str, expected: Binding) -> Result<()> {
        self.change(self.stream_id(name)?, |state| {
            let state = state.ok_or_else(unavailable)?;
            state.require(expected)?;
            if state.grants_closed {
                return Ok(None);
            }
            let mut next = state.clone();
            next.grants_closed = true;
            Ok(Some(next))
        })
        .await
        .map(drop)
    }

    pub async fn publish_successor(
        &self,
        name: &str,
        expected: Binding,
        operation: Uuid,
        successor: VerifiedSuccessor,
    ) -> Result<Binding> {
        if operation.is_nil()
            || successor.predecessor != expected.incarnation
            || successor.successor.is_nil()
            || successor.successor == expected.incarnation
        {
            return Err(conflict());
        }
        let publication = Publication {
            operation,
            predecessor: expected,
            manifest_digest: successor.manifest_digest,
        };
        let state = self
            .change(self.stream_id(name)?, |state| {
                let state = state.ok_or_else(unavailable)?;
                if let Some(prior) = state
                    .publication
                    .filter(|prior| prior.operation == operation)
                {
                    if prior == publication && state.binding.incarnation == successor.successor {
                        return Ok(None);
                    }
                    return Err(conflict());
                }
                state.require(expected)?;
                if !state.grants_closed || state.retired.contains(&successor.successor) {
                    return Err(conflict());
                }
                let clock_lower_bound = self.clock.now().0.saturating_sub(MAX_CLOCK_OFFSET_MICROS);
                if state.grants_end.is_some_and(|end| clock_lower_bound < end) {
                    return Err(Anomaly::new(
                        AnomalyCategory::Busy,
                        "rtdb/activation-draining",
                        "The previous incarnation still holds a serving grant",
                    ));
                }
                let mut next = state.clone();
                next.retired.insert(expected.incarnation);
                next.binding = Binding {
                    incarnation: successor.successor,
                    revision: expected.revision.checked_add(1).ok_or_else(conflict)?,
                };
                next.grants_closed = false;
                next.grants_end = None;
                next.publication = Some(publication);
                Ok(Some(next))
            })
            .await?;
        Ok(state.binding)
    }

    pub(super) fn serving_authority(
        self: &Arc<Self>,
        name: &str,
        incarnation: Uuid,
    ) -> Result<Arc<dyn ServingAuthority>> {
        Ok(Arc::new(BoundAuthority {
            authority: Arc::clone(self),
            stream: self.stream_id(name)?,
            incarnation,
            grant: Mutex::new(None),
        }))
    }

    async fn grant(&self, stream: StreamId, incarnation: Uuid) -> Result<i64> {
        let state = self
            .change(stream, |state| {
                let state = state.ok_or_else(unavailable)?;
                if state.grants_closed || state.binding.incarnation != incarnation {
                    return Err(not_active());
                }
                let now = self.clock.now().0;
                if state
                    .grants_end
                    .is_some_and(|end| end.saturating_sub(now) > MAX_GRANT_MICROS / 2)
                {
                    return Ok(None);
                }
                let mut next = state.clone();
                next.grants_end = Some(now.checked_add(MAX_GRANT_MICROS).ok_or_else(unavailable)?);
                Ok(Some(next))
            })
            .await?;
        state.grants_end.ok_or_else(unavailable)
    }

    fn stream_id(&self, name: &str) -> Result<StreamId> {
        super::validate_deployment_name(name)?;
        let mut hash = Sha256::new();
        hash.update(b"rtdb.activation.v1");
        hash.update(self.identity.as_bytes());
        hash.update(name.as_bytes());
        let digest: [u8; 32] = hash.finalize().into();
        let id = StreamId(u128::from_be_bytes(digest[..16].try_into().unwrap()));
        Ok(id)
    }

    async fn read(&self, stream: StreamId) -> Result<(Lsn, Option<State>)> {
        if !self.local {
            self.journal
                .require_immutable(stream)
                .await
                .map_err(journal_error)?;
        }
        for _ in 0..ATTEMPTS {
            let head = self.journal.head(stream).await.map_err(journal_error)?;
            if head == Lsn(0) {
                return Ok((head, None));
            }
            let page = if self.local {
                self.journal.read(stream, head, 1, Duration::ZERO).await
            } else {
                self.journal
                    .read_page(stream, head, head, 1, Duration::ZERO)
                    .await
            };
            let page = match page {
                Ok(page) => page,
                Err(JournalError::Trimmed { .. }) => continue,
                Err(error) => return Err(journal_error(error)),
            };
            let [(at, bytes)] = page.records.as_slice() else {
                continue;
            };
            if *at != head {
                return Err(corrupt());
            }
            let state: State = serde_json::from_slice(bytes).map_err(|_| corrupt())?;
            if state.version != 1
                || state.binding.incarnation.is_nil()
                || state.binding.revision == 0
                || state.retired.contains(&state.binding.incarnation)
            {
                return Err(corrupt());
            }
            return Ok((head, Some(state)));
        }
        Err(unavailable())
    }

    async fn change(
        &self,
        stream: StreamId,
        mut next: impl FnMut(Option<&State>) -> Result<Option<State>>,
    ) -> Result<State> {
        let mut epoch = Epoch(u64::from(self.local));
        for _ in 0..ATTEMPTS {
            let (head, observed) = self.read(stream).await?;
            let Some(state) = next(observed.as_ref())? else {
                return observed.ok_or_else(corrupt);
            };
            let sealed = if self.local {
                self.journal
                    .seal(stream, epoch)
                    .await
                    .map(|head| (head, Fence::Local(epoch)))
            } else {
                let session = WriterSessionId(Uuid::new_v4().as_u128());
                self.journal
                    .seal_session(stream, epoch, session)
                    .await
                    .map(|receipt| (receipt.sealed_head_lsn, Fence::Immutable(receipt.fence)))
            };
            let fence = match sealed {
                Ok((sealed_head, fence)) if sealed_head == head => fence,
                Ok(_) => continue,
                Err(JournalError::Fenced { current }) => {
                    epoch = Epoch(
                        current
                            .0
                            .checked_add(u64::from(self.local))
                            .ok_or_else(unavailable)?,
                    );
                    continue;
                }
                Err(error) => return Err(journal_error(error)),
            };
            let bytes = serde_json::to_vec(&state)
                .map(Bytes::from)
                .map_err(|_| corrupt())?;
            let at = Lsn(head.0.checked_add(1).ok_or_else(unavailable)?);
            let append = match fence {
                Fence::Local(epoch) => self.journal.append(stream, epoch, at, bytes).await,
                Fence::Immutable(fence) => {
                    self.journal
                        .append_session_batch(fence, at, vec![bytes])
                        .await
                }
            };
            match append {
                Ok(committed) if committed == at => {
                    if head != Lsn(0) {
                        let _ = match fence {
                            Fence::Local(epoch) => self.journal.trim(stream, epoch, head).await,
                            Fence::Immutable(fence) => self.journal.trim_session(fence, head).await,
                        };
                    }
                    return Ok(state);
                }
                Err(JournalError::Conflict { .. } | JournalError::Fenced { .. }) => continue,
                Err(error) => return Err(journal_error(error)),
                Ok(_) => return Err(corrupt()),
            }
        }
        Err(unavailable())
    }
}

enum Fence {
    Local(Epoch),
    Immutable(WriterFence),
}

struct BoundAuthority {
    authority: Arc<ActivationAuthority>,
    stream: StreamId,
    incarnation: Uuid,
    grant: Mutex<Option<ServingPermit>>,
}

impl ServingAuthority for BoundAuthority {
    fn acquire(&self) -> BoxFuture<'_, Result<ServingPermit>> {
        Box::pin(async move {
            let mut current = self.grant.lock().await;
            if let Some(grant) = current.as_ref()
                && grant.check().is_ok()
            {
                return Ok(grant.clone());
            }
            let end = self.authority.grant(self.stream, self.incarnation).await?;
            let clock = Arc::clone(&self.authority.clock);
            let permit = ServingPermit::until(
                end,
                Arc::new(move || clock.now().0.saturating_add(MAX_CLOCK_OFFSET_MICROS)),
            )?;
            *current = Some(permit.clone());
            Ok(permit)
        })
    }
}

fn unavailable() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb/activation-unavailable",
        "The database serving authority is unavailable",
    )
}

fn not_active() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb/activation-fenced",
        "The database incarnation may not serve",
    )
}

fn conflict() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict,
        "rtdb/activation-conflict",
        "The database activation does not match the current incarnation",
    )
}

fn corrupt() -> Anomaly {
    Anomaly::fault(
        "rtdb/activation-state",
        "The activation journal contains an invalid binding",
    )
}

fn journal_error(error: JournalError) -> Anomaly {
    match error {
        JournalError::Recovered { .. } | JournalError::Deleted => corrupt(),
        _ => unavailable(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI64, Ordering};

    use super::*;

    struct TestClock(AtomicI64);
    impl Clock for TestClock {
        fn now(&self) -> Timestamp {
            Timestamp(self.0.load(Ordering::Relaxed))
        }
    }

    #[test]
    fn retained_client_handle_cannot_serve_after_successor_publication() {
        std::thread::Builder::new()
            .stack_size(rtdb_node::WORKER_STACK_BYTES)
            .spawn(|| {
                tokio::runtime::Builder::new_multi_thread()
                    .thread_stack_size(rtdb_node::WORKER_STACK_BYTES)
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(guarded_client());
            })
            .unwrap()
            .join()
            .unwrap();
    }

    async fn guarded_client() {
        use rtdb_columnar::MemoryBudget;
        use rtdb_tx::transact::{Outcome, Transact};
        use tokio_util::sync::CancellationToken;

        let data = tempfile::tempdir().unwrap();
        let authority =
            ActivationAuthority::open_local(&data.path().join("authority"), Uuid::new_v4())
                .unwrap();
        let incumbent = Uuid::new_v4();
        let successor = Uuid::new_v4();
        let original = authority.bootstrap("xtdb", incumbent).await.unwrap();
        let limit = 256 << 20;
        let mut config =
            crate::config::NodeConfiguration::from_yaml_with_environment("", &|_| None, false)
                .unwrap();
        config.flight_sql = None;
        let memory = MemoryBudget::new(config.node_options(limit).unwrap().execution_memory_bytes);
        let runtime = super::super::MultiRuntime::development_with_activation(
            &data.path().join("source"),
            &mut config,
            memory.clone(),
            Some(incumbent),
            None,
            Some(authority.clone()),
        )
        .await
        .unwrap();
        let (handle, driver) =
            crate::ServerDriver::open_with_multi(config, limit, memory, &runtime)
                .await
                .unwrap();
        let server = tokio::spawn(driver.run());
        let stop = CancellationToken::new();
        tokio::select! {
            value = runtime.await_development_writer(handle.node(), incumbent) => { value.unwrap(); }
            result = runtime.drive(handle.node(), &stop) => panic!("Multi stopped before readiness: {result:?}"),
        }
        let attached = handle.node().catalog().database("xtdb").unwrap();
        let retained = attached.serving_driver().unwrap();
        let transaction = |key| Transact {
            custody_request_digest: None,
            request_memory: None,
            key: vec![key; 16],
            require: vec![],
            window: Default::default(),
            guards: vec![],
            requires: vec![],
            otherwise: vec![],
            ops: vec![],
        };
        assert_eq!(
            retained
                .transact(transaction(21), &stop)
                .await
                .unwrap()
                .outcome,
            Outcome::Then
        );
        retained
            .check_read_delivery(None, &BTreeSet::new(), &stop)
            .await
            .unwrap();

        authority.close_grants("xtdb", original).await.unwrap();
        let ready = VerifiedSuccessor {
            predecessor: incumbent,
            successor,
            manifest_digest: [3; 32],
        };
        let operation = Uuid::new_v4();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match authority
                    .publish_successor("xtdb", original, operation, ready)
                    .await
                {
                    Ok(binding) => {
                        assert_eq!(binding.incarnation, successor);
                        break;
                    }
                    Err(error) if error.code == "rtdb/activation-draining" => {
                        tokio::time::sleep(Duration::from_millis(25)).await;
                    }
                    Err(error) => panic!("Unexpected successor publication error: {error:?}"),
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(
            retained
                .transact(transaction(22), &stop)
                .await
                .unwrap_err()
                .code,
            "rtdb/activation-fenced",
        );
        assert_eq!(
            retained
                .check_read_delivery(None, &BTreeSet::new(), &stop)
                .await
                .unwrap_err()
                .code,
            "rtdb/activation-fenced",
        );
        drop(attached);
        handle.shutdown();
        server.await.unwrap().unwrap();
        drop(handle);
        runtime.close().await;
    }

    #[tokio::test]
    async fn freezes_grants_and_publishes_exact_successor_after_expiry() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("activation");
        let clock = Arc::new(TestClock(AtomicI64::new(1_000_000)));
        let mut authority = ActivationAuthority::open_local(&path, Uuid::new_v4()).unwrap();
        Arc::get_mut(&mut authority).unwrap().clock = clock.clone();
        let old = Uuid::new_v4();
        let next = Uuid::new_v4();
        let incumbent = authority.bootstrap("xtdb", old).await.unwrap();
        let installed = VerifiedSuccessor {
            predecessor: old,
            successor: next,
            manifest_digest: [4; 32],
        };
        let operation = Uuid::new_v4();
        let old_grant = authority.serving_authority("xtdb", old).unwrap();
        old_grant.acquire().await.unwrap();
        assert_eq!(
            authority
                .serving_authority("xtdb", next)
                .unwrap()
                .acquire()
                .await
                .unwrap_err()
                .code,
            "rtdb/activation-fenced"
        );
        authority.close_grants("xtdb", incumbent).await.unwrap();
        assert_eq!(
            authority
                .serving_authority("xtdb", old)
                .unwrap()
                .acquire()
                .await
                .unwrap_err()
                .code,
            "rtdb/activation-fenced"
        );
        assert_eq!(
            authority
                .publish_successor("xtdb", incumbent, operation, installed)
                .await
                .unwrap_err()
                .code,
            "rtdb/activation-draining"
        );
        drop(old_grant);
        let identity = authority.identity;
        drop(authority);

        let mut authority = ActivationAuthority::open_local(&path, identity).unwrap();
        Arc::get_mut(&mut authority).unwrap().clock = clock.clone();
        assert_eq!(authority.binding("xtdb").await.unwrap(), incumbent);
        assert_eq!(
            authority
                .publish_successor("xtdb", incumbent, operation, installed)
                .await
                .unwrap_err()
                .code,
            "rtdb/activation-draining"
        );
        clock.0.store(6_250_000, Ordering::Relaxed);
        let active = authority
            .publish_successor("xtdb", incumbent, operation, installed)
            .await
            .unwrap();
        assert_eq!(
            active,
            Binding {
                incarnation: next,
                revision: 2
            }
        );
        assert_eq!(
            authority
                .publish_successor("xtdb", incumbent, operation, installed)
                .await
                .unwrap(),
            active
        );
        assert_eq!(
            authority
                .close_grants("xtdb", incumbent)
                .await
                .unwrap_err()
                .code,
            "rtdb/activation-conflict"
        );
        assert_eq!(
            authority
                .serving_authority("xtdb", old)
                .unwrap()
                .acquire()
                .await
                .unwrap_err()
                .code,
            "rtdb/activation-fenced"
        );
        authority
            .serving_authority("xtdb", next)
            .unwrap()
            .acquire()
            .await
            .unwrap();
    }
}

#[cfg(feature = "s3")]
pub(super) async fn installed(database_url: &str) -> Result<Option<Arc<ActivationAuthority>>> {
    let url = match std::env::var("RTDB_ACTIVATION_JOURNAL_URL") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => return Ok(None),
        Err(_) => return Err(configuration()),
    };
    if url == database_url || !url.starts_with(super::CONSENSUS_SCHEME) {
        return Err(configuration());
    }
    let env = |name| std::env::var(name).map_err(|_| configuration());
    let identity =
        Uuid::parse_str(&env("RTDB_ACTIVATION_AUTHORITY_ID")?).map_err(|_| configuration())?;
    let credentials = super::Credentials {
        certificate: env("RTDB_ACTIVATION_CERTIFICATE_PATH")?.into(),
        private_key: env("RTDB_ACTIVATION_PRIVATE_KEY_PATH")?.into(),
        ca: env("RTDB_ACTIVATION_CA_PATH")?.into(),
    };
    let backend = super::connect_stream_journal(&url, &credentials).await?;
    Ok(Some(ActivationAuthority::new(backend.journal, identity)?))
}

#[cfg(feature = "s3")]
fn configuration() -> Anomaly {
    Anomaly::incorrect(
        "rtdb/activation-configuration",
        "Activation requires an independent consensus journal, authority ID and commissioned TLS paths",
    )
}
