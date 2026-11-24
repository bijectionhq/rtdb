//! Exact unit grants for plaintext retained by an execution between reads.

use std::{
    collections::BTreeMap,
    future::{Future, poll_fn},
    io::{self, IoSlice},
    num::NonZeroU64,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use parking_lot::Mutex;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_types::residence_trace::CertifiedRead;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use super::{ObjectCipher, UnitOperation};
use crate::{Error, HeldView, IoRegion, ObjectKey, ReadLease, Result};

/// Sample before consulting the key owner. An issuer cannot extend this window
/// by delaying its reply or by reusing a successful earlier read.
const GRANT_WINDOW: Duration = Duration::from_secs(30);

/// A key owner waits this long after confirming its admission fence before
/// reporting settlement. The margin covers rate differences between monotonic
/// clocks at the reader and the owner. The wait starts again on an uncertain retry.
pub async fn drain_unit_delivery() {
    tokio::time::sleep(GRANT_WINDOW * 2).await;
}

/// A fixed transport cutoff checked by both the scheduling and wall clocks on this host.
#[derive(Clone, Copy)]
pub struct DeliveryDeadline {
    instant: Instant,
    wall: SystemTime,
}

impl DeliveryDeadline {
    fn sampled(now: Instant, wall: SystemTime, until: Instant) -> Self {
        Self { instant: until, wall: wall.checked_add(until.saturating_duration_since(now)).unwrap_or(wall) }
    }

    pub fn answer_window() -> Self {
        let now = Instant::now();
        Self::sampled(now, SystemTime::now(), now + GRANT_WINDOW)
    }

    pub fn earlier(self, other: Self) -> Self {
        Self { instant: self.instant.min(other.instant), wall: self.wall.min(other.wall) }
    }

    pub fn covers(self, other: Self) -> bool {
        self.instant >= other.instant && self.wall >= other.wall
    }

    pub fn is_live(self) -> bool { Instant::now() < self.instant && SystemTime::now() < self.wall }

    pub fn instant(self) -> Instant { self.instant }

    pub fn wire_nanos(self) -> Option<u64> {
        self.wall.duration_since(UNIX_EPOCH).ok()?.as_nanos().try_into().ok()
    }

    pub fn from_wire_nanos(until: u64) -> Option<Self> {
        let now = Instant::now();
        let wall = UNIX_EPOCH.checked_add(Duration::from_nanos(until))?;
        let remaining = wall.duration_since(SystemTime::now()).ok()?.min(GRANT_WINDOW);
        if remaining.is_zero() { return None; }
        Some(Self { instant: now.checked_add(remaining)?, wall })
    }

    pub async fn writing<T>(self, work: impl Future<Output = io::Result<T>>) -> io::Result<T> {
        if !self.is_live() { return Err(handoff_expired()); }
        let handoff = Handoff { deadline: self, lease: None };
        let result = tokio::time::timeout_at(self.instant.into(), ANSWER_HANDOFF.scope(handoff, work))
            .await.map_err(|_| handoff_expired())??;
        if !self.is_live() { return Err(handoff_expired()); }
        Ok(result)
    }
}

tokio::task_local! {
    static PLAINTEXT_VIEWS: Arc<[Arc<PlaintextView>]>;
    static ANSWER_HANDOFF: Handoff;
}

struct Unit {
    cipher: Arc<dyn ObjectCipher>,
    name: ObjectKey,
    expires: Mutex<Option<DeliveryDeadline>>,
    read_lease: Mutex<Option<Arc<ReadLease>>>,
    _allocation: Arc<MemoryReservation>,
}

/// An original authority for plaintext that is not tracked by an object key.
#[async_trait::async_trait]
pub trait PlaintextReleaseCheck: Send + Sync {
    async fn require_current(&self) -> rtdb_types::Result<()>;
}

/// A protected Root member selected at the rank's held cut.
pub struct RootRankSelection<'a> {
    pub family: uuid::Uuid,
    pub assertion: rtdb_types::AssertionId,
    pub stage: [u8; 40],
    pub target: rtdb_types::RootLandingTarget,
    pub link_digest: [u8; 32],
    pub payload_digest: [u8; 32],
    pub publication: u64,
    pub ordinal: NonZeroU64,
    pub relation: &'a str,
    pub selector: &'a str,
}

#[async_trait::async_trait]
pub trait RootRankAuthority: Send + Sync {
    async fn require_derivative(&self, selected: RootRankSelection<'_>) -> rtdb_types::Result<()>;
}

/// A conservative inventory of the exact keys and checks for an execution's answer.
pub struct PlaintextView {
    memory: Arc<MemoryBudget>,
    units: Mutex<BTreeMap<(usize, ObjectKey), Arc<Unit>>>,
    certified_reads: Mutex<BTreeMap<[u8; 16], (Arc<CertifiedRead>, Arc<MemoryReservation>)>>,
    protected_cuts: Mutex<Vec<ProtectedCut>>,
    release_checks: Mutex<BTreeMap<[u8; 32], Arc<CheckedRelease>>>,
    rank: Mutex<Option<RankInventory>>,
}

#[derive(Default)]
struct RankInventory {
    entries: BTreeMap<Vec<u8>, Arc<MemoryReservation>>,
    carrier_reads: usize,
    root_authority: Option<Arc<dyn RootRankAuthority>>,
}

struct CheckedRelease {
    owner: Arc<dyn PlaintextReleaseCheck>,
    cutoff: DeliveryDeadline,
    _allocation: Arc<MemoryReservation>,
}

struct ProtectedCut {
    view: Arc<HeldView>,
    lease: Arc<ReadLease>,
    _allocation: Arc<MemoryReservation>,
}

impl PlaintextView {
    pub fn new(memory: Arc<MemoryBudget>) -> Arc<Self> {
        Arc::new(Self {
            memory,
            units: Mutex::new(BTreeMap::new()),
            certified_reads: Mutex::new(BTreeMap::new()),
            protected_cuts: Mutex::new(Vec::new()),
            release_checks: Mutex::new(BTreeMap::new()),
            rank: Mutex::new(None),
        })
    }

    pub async fn reading<F: Future>(self: &Arc<Self>, work: F) -> F::Output {
        let mut views = PLAINTEXT_VIEWS
            .try_with(|views| views.to_vec())
            .unwrap_or_default();
        if views.iter().any(|view| Arc::ptr_eq(self, view)) {
            return work.await;
        }
        views.push(Arc::clone(self));
        PLAINTEXT_VIEWS.scope(views.into(), work).await
    }

    pub(crate) fn inherit<F: Future>(work: F) -> impl Future<Output = F::Output> {
        let views = PLAINTEXT_VIEWS.try_with(Arc::clone).ok();
        async move {
            match views {
                Some(views) => PLAINTEXT_VIEWS.scope(views, work).await,
                None => work.await,
            }
        }
    }

    pub fn begin_rank_inventory() -> rtdb_types::Result<()> {
        PLAINTEXT_VIEWS.try_with(|views| {
            let owner = views.last().ok_or_else(rank_unavailable)?;
            *owner.rank.lock() = Some(RankInventory::default());
            Ok(())
        }).map_err(|_| rank_unavailable())?
    }

    pub fn bind_root_rank_authority(authority: Arc<dyn RootRankAuthority>) -> rtdb_types::Result<()> {
        PLAINTEXT_VIEWS.try_with(|views| {
            let mut rank = views.last().ok_or_else(rank_unavailable)?.rank.lock();
            rank.as_mut().ok_or_else(rank_unavailable)?.root_authority = Some(authority);
            Ok(())
        }).map_err(|_| rank_unavailable())?
    }

    pub async fn require_root_rank_derivative(selected: RootRankSelection<'_>) -> rtdb_types::Result<()> {
        let authority = PLAINTEXT_VIEWS.try_with(|views| {
            views.iter().rev().find_map(|view| {
                view.rank.lock().as_ref().map(|rank| rank.root_authority.clone())
            }).flatten()
        }).ok().flatten().ok_or_else(rank_unavailable)?;
        authority.require_derivative(selected).await
    }

    pub fn tracks_rank_inventory() -> bool {
        PLAINTEXT_VIEWS.try_with(|views| views.iter().any(|view| view.rank.lock().is_some())).unwrap_or(false)
    }

    pub fn charge_rank_carrier() -> rtdb_types::Result<()> {
        PLAINTEXT_VIEWS.try_with(|views| {
            for view in views.iter() {
                let mut rank = view.rank.lock();
                let Some(rank) = rank.as_mut() else { continue; };
                if rank.carrier_reads == 512 { return Err(rank_unavailable()); }
                rank.carrier_reads += 1;
            }
            Ok(())
        }).unwrap_or(Ok(()))
    }

    pub fn track_rank_contributor(entry: &[u8]) -> rtdb_types::Result<()> {
        if entry.is_empty() || entry.len() > 1024 { return Err(rank_unavailable()); }
        PLAINTEXT_VIEWS.try_with(|views| {
            for view in views.iter() {
                let mut rank = view.rank.lock();
                let Some(rank) = rank.as_mut() else { continue; };
                let entries = &mut rank.entries;
                if entries.contains_key(entry) { continue; }
                if entries.len() == 512 { return Err(rank_unavailable()); }
                let charge = view.memory.reserve(entry.len() + 256)?;
                entries.insert(entry.to_vec(), charge);
            }
            Ok(())
        }).unwrap_or(Ok(()))
    }

    pub fn rank_inventory() -> rtdb_types::Result<(Vec<u8>, Arc<MemoryReservation>)> {
        PLAINTEXT_VIEWS.try_with(|views| {
            let owner = views.last().ok_or_else(rank_unavailable)?;
            let rank = owner.rank.lock();
            let entries = &rank.as_ref().ok_or_else(rank_unavailable)?.entries;
            let size = 2 + entries.keys().map(Vec::len).sum::<usize>();
            let charge = owner.memory.reserve(size.saturating_mul(4))?;
            let mut bytes = Vec::with_capacity(size);
            bytes.extend_from_slice(&(entries.len() as u16).to_be_bytes());
            for entry in entries.keys() { bytes.extend_from_slice(entry); }
            Ok((bytes, charge))
        }).map_err(|_| rank_unavailable())?
    }

    /// Preserve the first cutoff for this subject through response handoff.
    pub fn track_release_check(key: [u8; 32], owner: Arc<dyn PlaintextReleaseCheck>) -> rtdb_types::Result<()> {
        PLAINTEXT_VIEWS.try_with(|views| {
            for view in views.iter() {
                let mut checks = view.release_checks.lock();
                if let std::collections::btree_map::Entry::Vacant(entry) = checks.entry(key) {
                    let allocation = view.memory.reserve(512)?;
                    entry.insert(Arc::new(CheckedRelease {
                        owner: Arc::clone(&owner), cutoff: DeliveryDeadline::answer_window(), _allocation: allocation,
                    }));
                }
            }
            Ok(())
        }).unwrap_or(Ok(()))
    }

    pub fn track_certified_read(read: &Arc<CertifiedRead>) -> rtdb_types::Result<()> {
        PLAINTEXT_VIEWS.try_with(|views| {
            for view in views.iter() {
                let mut reads = view.certified_reads.lock();
                if let std::collections::btree_map::Entry::Vacant(entry) = reads.entry(read.invocation) {
                    let charge = view.memory.reserve(256)?;
                    entry.insert((read.clone(), charge));
                }
            }
            Ok(())
        }).unwrap_or(Ok(()))
    }

    /// Keep the exact selected catalog and its deadline with a bounded action reply.
    pub fn track_protected_cut(view: Arc<HeldView>, lease: Arc<ReadLease>) -> rtdb_types::Result<()> {
        PLAINTEXT_VIEWS.try_with(|views| {
            for owner in views.iter() {
                let allocation = owner.memory.reserve(std::mem::size_of::<ProtectedCut>())?;
                owner.protected_cuts.lock().push(ProtectedCut {
                    view: Arc::clone(&view), lease: Arc::clone(&lease), _allocation: allocation,
                });
            }
            Ok(())
        }).unwrap_or(Ok(()))
    }

    pub fn track(cipher: &Arc<dyn ObjectCipher>, name: &ObjectKey) -> Result<()> {
        Self::track_at(cipher, name, IoRegion::current_read_lease())
    }

    fn track_at(cipher: &Arc<dyn ObjectCipher>, name: &ObjectKey, lease: Option<Arc<ReadLease>>) -> Result<()> {
        let tracked = PLAINTEXT_VIEWS.try_with(|views| {
            for view in views.iter() {
                let key = (Arc::as_ptr(cipher) as *const () as usize, name.clone());
                let mut units = view.units.lock();
                if let Some(unit) = units.get(&key) {
                    if let Some(lease) = &lease {
                        let mut original = unit.read_lease.lock();
                        *original = Some(original.take().map_or_else(|| lease.clone(), |prior| prior.earlier(lease.clone())));
                    }
                } else {
                    let bytes = name.as_str().len().saturating_mul(2).saturating_add(512);
                    let allocation =
                        view.memory.reserve(bytes).map_err(|_| Error::MemoryLimit {
                            requested: bytes,
                            limit: view.memory.limit_bytes(),
                            retained: view.memory.used_bytes(),
                        })?;
                    units.insert(key, Arc::new(Unit {
                        cipher: Arc::clone(cipher),
                        name: name.clone(),
                        expires: Mutex::new(None),
                        read_lease: Mutex::new(lease.clone()),
                        _allocation: allocation,
                    }));
                }
            }
            Ok(())
        });
        tracked.unwrap_or(Ok(()))
    }

    pub async fn grant(&self) -> rtdb_types::Result<PlaintextDelivery> {
        let (allocation, units) = {
            let units = self.units.lock();
            let element = std::mem::size_of::<Arc<Unit>>() + std::mem::size_of::<Arc<dyn Send + Sync>>();
            let bytes = units.len().saturating_mul(element).saturating_add(std::mem::size_of::<PlaintextDelivery>());
            (
                self.memory.reserve(bytes)?,
                units.values().cloned().collect::<Vec<_>>(),
            )
        };
        let mut deadline = None;
        let mut custody = Vec::new();
        for unit in units {
            let now = Instant::now();
            let wall = SystemTime::now();
            let mut cutoff = now + GRANT_WINDOW;
            let read_lease = unit.read_lease.lock().clone();
            if let Some(lease) = &read_lease {
                lease.check()?;
                if let Some(read_cutoff) = now.checked_add(lease.remaining()) {
                    cutoff = cutoff.min(read_cutoff);
                }
            }
            if let Some(provider) = unit.cipher.delivery_deadline() {
                cutoff = cutoff.min(provider);
            }
            if cutoff <= now {
                return Err(expired());
            }
            let sampled = DeliveryDeadline::sampled(now, wall, cutoff);
            let cached = *unit.expires.lock();
            let expires = match cached.filter(|expires| expires.is_live() && expires.instant > now + Duration::from_secs(1)) {
                Some(expires) => expires.earlier(sampled),
                None => {
                    let check = IoRegion::leased(
                        read_lease,
                        unit.cipher.operate(&unit.name, UnitOperation::Grant),
                    );
                    let checked = tokio::time::timeout_at(cutoff.into(), check)
                        .await
                        .map_err(|_| expired())??;
                    let (until, held) = checked.granted()?;
                    let cutoff = until.map_or(sampled, |until| DeliveryDeadline::sampled(now, wall, until.min(cutoff)));
                    if let Some(held) = held {
                        custody.push(held);
                    } else {
                        *unit.expires.lock() = Some(cutoff);
                    }
                    cutoff
                }
            };
            deadline = Some(deadline.map_or(expires, |prior: DeliveryDeadline| prior.earlier(expires)));
        }
        let (_check_charge, checks) = {
            let checks = self.release_checks.lock();
            let charge = self.memory.reserve(checks.len().saturating_mul(std::mem::size_of::<Arc<CheckedRelease>>()))?;
            (charge, checks.values().cloned().collect::<Vec<_>>())
        };
        for check in checks {
            if !check.cutoff.is_live() { return Err(expired()); }
            tokio::time::timeout_at(check.cutoff.instant.into(), check.owner.require_current())
                .await.map_err(|_| expired())??;
            deadline = Some(deadline.map_or(check.cutoff, |prior| prior.earlier(check.cutoff)));
        }
        let certified_reads = self.certified_reads.lock();
        let trace_bytes = self.memory.reserve(certified_reads.len().saturating_mul(std::mem::size_of::<Arc<CertifiedRead>>()))?;
        let mut delivery = PlaintextDelivery {
            certified_reads: certified_reads.values().map(|(read, _)| read.clone()).collect(),
            _trace_bytes: trace_bytes,
            deadline,
            views: Vec::new(),
            lease: None,
            _custody: custody,
            _allocation: allocation,
        };
        for cut in self.protected_cuts.lock().iter() {
            let charge = self.memory.reserve(std::mem::size_of::<Arc<HeldView>>())?;
            delivery._custody.push(charge);
            delivery = delivery.holding(Arc::clone(&cut.view), Some(Arc::clone(&cut.lease)));
        }
        delivery.check()?;
        Ok(delivery)
    }
}

/// The caller's delivery scopes travel with an asynchronous read request.
pub struct PlaintextScopes(Arc<[Arc<PlaintextView>]>);

impl PlaintextScopes {
    pub fn capture() -> Self {
        Self(PLAINTEXT_VIEWS.try_with(Arc::clone).unwrap_or_default())
    }

    pub async fn reading<F: Future>(self, work: F) -> F::Output {
        PLAINTEXT_VIEWS.scope(self.0, work).await
    }
}

/// The original object keys needed by a replayed in-memory snapshot.
pub struct PlaintextDependencies {
    units: Vec<Arc<Unit>>,
    _allocation: Arc<MemoryReservation>,
}

impl PlaintextDependencies {
    pub fn capture(memory: &Arc<MemoryBudget>) -> rtdb_types::Result<Self> {
        let view = PLAINTEXT_VIEWS.try_with(|views| views.last().cloned()).ok().flatten();
        let units = view.as_ref().map(|view| view.units.lock());
        let bytes = units.as_ref().map_or(0, |units| units.len()).saturating_mul(std::mem::size_of::<Arc<Unit>>());
        let allocation = memory.reserve(bytes)?;
        let units = units.map_or_else(Vec::new, |units| units.values().cloned().collect());
        Ok(Self { units, _allocation: allocation })
    }

    pub fn admit(&self) -> Result<()> {
        for unit in &self.units {
            let lease = unit.read_lease.lock().clone();
            PlaintextView::track_at(&unit.cipher, &unit.name, lease)?;
        }
        Ok(())
    }
}

fn rank_unavailable() -> rtdb_types::Anomaly {
    rtdb_types::Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, "rtdb.owned/rank-custody",
        "The held rank cannot establish its complete bounded family inventory")
}

fn expired() -> rtdb_types::Anomaly {
    rtdb_types::Anomaly::new(
        rtdb_types::AnomalyCategory::Unavailable,
        "rtdb.storage/plaintext-delivery-expired",
        "The live unit-key grant expired during answer delivery",
    )
}

pub struct PlaintextDelivery {
    certified_reads: Vec<Arc<CertifiedRead>>,
    _trace_bytes: Arc<MemoryReservation>,
    deadline: Option<DeliveryDeadline>,
    views: Vec<Arc<HeldView>>,
    lease: Option<Arc<ReadLease>>,
    _custody: Vec<Arc<dyn Send + Sync>>,
    _allocation: Arc<MemoryReservation>,
}

impl PlaintextDelivery {
    pub fn observe_answer(&self) {
        for read in &self.certified_reads {
            read.observe("certified-answer");
        }
    }

    pub fn holding(mut self, view: Arc<HeldView>, lease: Option<Arc<ReadLease>>) -> Self {
        if let Some(lease) = lease {
            let sampled = Instant::now();
            let wall = SystemTime::now();
            let cutoff = sampled.checked_add(lease.remaining()).unwrap_or(sampled);
            let cutoff = DeliveryDeadline::sampled(sampled, wall, cutoff);
            self.deadline = Some(self.deadline.map_or(cutoff, |prior| prior.earlier(cutoff)));
            self.lease = Some(self.lease.take().map_or_else(|| lease.clone(), |prior| prior.earlier(lease.clone())));
        }
        self.views.push(view);
        self
    }

    pub fn is_bounded(&self) -> bool { self.deadline.is_some() }

    pub fn transport_deadline(&self) -> Option<DeliveryDeadline> { self.deadline }

    pub fn check(&self) -> rtdb_types::Result<()> {
        if let Some(lease) = &self.lease { lease.check()?; }
        if self.deadline.is_some_and(|deadline| !deadline.is_live()) {
            return Err(expired());
        }
        Ok(())
    }

    /// Keeps custody through the final flush. The transport checks the same
    /// original cutoff on every socket handoff while polling this future.
    pub async fn deliver<T>(
        &self,
        work: impl Future<Output = rtdb_types::Result<T>>,
    ) -> rtdb_types::Result<T> {
        self.check()?;
        let Some(deadline) = self.deadline else { return work.await; };
        let handoff = Handoff { deadline, lease: self.lease.clone() };
        let mut work = std::pin::pin!(work);
        let result = tokio::time::timeout_at(deadline.instant.into(), ANSWER_HANDOFF.scope(handoff, poll_fn(|cx| {
            if let Err(error) = self.check() { return Poll::Ready(Err(error)); }
            work.as_mut().poll(cx)
        }))).await.map_err(|_| expired())??;
        self.check()?;
        Ok(result)
    }
}

struct Handoff {
    deadline: DeliveryDeadline,
    lease: Option<Arc<ReadLease>>,
}

pub fn current_delivery_deadline() -> Option<DeliveryDeadline> {
    ANSWER_HANDOFF.try_with(|handoff| handoff.deadline).ok()
}

fn handoff_expired() -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, "The plaintext delivery grant expired at the socket")
}

fn check_handoff() -> io::Result<()> {
    ANSWER_HANDOFF.try_with(|handoff| {
        if !handoff.deadline.is_live() || handoff.lease.as_ref().is_some_and(|lease| lease.check().is_err()) {
            return Err(handoff_expired());
        }
        Ok(())
    }).unwrap_or(Ok(()))
}

/// Checks a scoped answer grant on each write, including writes resumed inside a buffered protocol or TLS flush.
pub struct DeliveryIo<W>(W);

impl<W> DeliveryIo<W> {
    pub fn new(io: W) -> Self { Self(io) }
    pub fn get_ref(&self) -> &W { &self.0 }
}

impl<W: AsyncRead + Unpin> AsyncRead for DeliveryIo<W> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, bytes: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, bytes)
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for DeliveryIo<W> {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<usize>> {
        check_handoff()?;
        Pin::new(&mut self.0).poll_write(cx, bytes)
    }

    fn is_write_vectored(&self) -> bool { self.0.is_write_vectored() }

    fn poll_write_vectored(mut self: Pin<&mut Self>, cx: &mut Context<'_>, bytes: &[IoSlice<'_>]) -> Poll<io::Result<usize>> {
        check_handoff()?;
        Pin::new(&mut self.0).poll_write_vectored(cx, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        check_handoff()?;
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        check_handoff()?;
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI64, Ordering};

    use super::*;
    use crate::sealing::UnitOutput;

    struct AvailableKey;

    #[async_trait::async_trait]
    impl ObjectCipher for AvailableKey {
        async fn operate(&self, _: &ObjectKey, _: UnitOperation) -> Result<UnitOutput> {
            Ok(UnitOutput::Granted)
        }
    }

    #[tokio::test]
    async fn detached_reply_spends_dependencies_of_the_replayed_snapshot() {
        let memory = MemoryBudget::new(64 << 10);
        let original = PlaintextView::new(memory.clone());
        let key: Arc<dyn ObjectCipher> = Arc::new(AvailableKey);
        let name = ObjectKey::new("tables/data").unwrap();
        let dependencies = original.reading(async {
            PlaintextView::track(&key, &name).unwrap();
            PlaintextDependencies::capture(&memory).unwrap()
        }).await;

        let reply = PlaintextView::new(memory.clone());
        reply.reading(async {
            let caller = PlaintextScopes::capture();
            tokio::spawn(caller.reading(async move { dependencies.admit().unwrap() })).await.unwrap();
        }).await;
        assert!(reply.grant().await.unwrap().is_bounded());
    }

    #[tokio::test]
    async fn materialized_deployment_plaintext_requires_its_original_key() {
        use std::sync::atomic::AtomicBool;
        use bytes::Bytes;
        use crate::{MemoryStore, ObjectStore, sealing::EncryptingStore};

        struct DeploymentKey(AtomicBool);

        #[async_trait::async_trait]
        impl ObjectCipher for DeploymentKey {
            async fn operate(&self, _: &ObjectKey, _: UnitOperation) -> Result<UnitOutput> {
                if self.0.load(Ordering::Relaxed) { Ok(UnitOutput::Granted) }
                else { Err(Error::ErasedUnderView) }
            }
        }

        let key = Arc::new(DeploymentKey(AtomicBool::new(true)));
        let store = EncryptingStore::with_deployment_delivery(
            Arc::new(MemoryStore::default()), &[8; 32], key.clone(),
        ).unwrap();
        let name = ObjectKey::new("tables/data").unwrap();
        let original = Bytes::from_static(b"cached plaintext");
        store.put(&name, original.clone()).await.unwrap();
        let view = PlaintextView::new(MemoryBudget::new(64 << 10));
        let plaintext = view.reading(store.get(&name)).await.unwrap();
        assert_eq!(plaintext, original);

        key.0.store(false, Ordering::Relaxed);
        assert_eq!(view.grant().await.err().unwrap().code, "rtdb/erased-under-view");
    }

    #[tokio::test]
    async fn socket_write_checks_again_after_work_runs_in_the_same_poll() {
        use tokio::io::AsyncWriteExt;

        let view = PlaintextView::new(MemoryBudget::new(64 << 10));
        let now = Arc::new(AtomicI64::new(0));
        let clock: Arc<dyn Fn() -> i64 + Send + Sync> = {
            let now = now.clone();
            Arc::new(move || now.load(Ordering::Relaxed))
        };
        let lease = Arc::new(ReadLease::new(Some(10_000_000), clock));
        let grant = view.grant().await.unwrap().holding(HeldView::recovery(Arc::new(())), Some(lease));
        let mut bytes = Vec::new();
        let error = grant.deliver(async {
            now.store(10_000_000, Ordering::Relaxed);
            let error = DeliveryIo::new(&mut bytes).write_all(b"plaintext").await.unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::TimedOut);
            Err::<(), _>(expired())
        }).await.unwrap_err();
        assert_eq!(error.code, "rtdb.storage/plaintext-delivery-expired");
        assert!(bytes.is_empty());
    }

    #[tokio::test]
    async fn buffered_plaintext_cannot_renew_past_its_read_lease() {
        let memory = MemoryBudget::new(64 << 10);
        let view = PlaintextView::new(memory);
        let key: Arc<dyn ObjectCipher> = Arc::new(AvailableKey);
        let name = ObjectKey::new("table/data").unwrap();
        let now = Arc::new(AtomicI64::new(0));
        let clock = {
            let now = Arc::clone(&now);
            Arc::new(move || now.load(Ordering::Relaxed))
        };
        let lease = Arc::new(ReadLease::new(Some(5_000_000), clock));
        IoRegion::leased(
            Some(lease),
            view.reading(async {
                PlaintextView::track(&key, &name).unwrap();
            }),
        )
        .await;
        assert!(view.grant().await.unwrap().is_bounded());

        now.store(5_000_000, Ordering::Relaxed);
        assert!(view.grant().await.is_err());
    }
}
