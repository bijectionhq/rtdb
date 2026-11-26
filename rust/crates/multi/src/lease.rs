//! A member's registration and lease (`RenewLease`, `ExpireLease`).
//!
//! Liveness is per member: one renewal every `heartbeat`, whatever the member holds.

use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rtdb_directory::{Cas, Directory, LogkeeperDecommission, MemberId, MemberRecord, MemberRole, MemberState, now_ms};
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug)]
pub struct LeaseOptions { pub lease: Duration, pub heartbeat: Duration }

impl Default for LeaseOptions {
    fn default() -> Self { Self { lease: Duration::from_secs(3), heartbeat: Duration::from_millis(500) } }
}

pub struct MemberLease {
    directory: Arc<dyn Directory>,
    record: Mutex<MemberRecord>,
    options: LeaseOptions,
    lost: CancellationToken,
}

pub struct Registration {
    pub role: MemberRole,
    pub address: String,
    pub control_address: Option<String>,
    pub availability_zone: String,
    pub engine_version: String,
    pub pool: rtdb_directory::MemberPool,
    pub storage_namespaces: Option<rtdb_directory::StorageNamespaces>,
}

impl Registration {
    fn record(self, id: MemberId) -> MemberRecord {
        MemberRecord {
            id, role: self.role, address: self.address, control_address: self.control_address,
            availability_zone: self.availability_zone, engine_version: self.engine_version,
            lease_expires_at_ms: 0, state: MemberState::Eligible, drain_target_engine: None, pool: self.pool, storage_namespaces: self.storage_namespaces, decommission: None, eligibility_epoch: 0, version: 0,
        }
    }
}

impl MemberLease {
    pub async fn register(directory: Arc<dyn Directory>, registration: Registration, options: LeaseOptions) -> Result<Arc<Self>> {
        let mut candidate = registration.record(MemberId::fresh());
        candidate.lease_expires_at_ms = now_ms() + options.lease.as_millis() as i64;
        let record = directory.register_member(candidate).await?;
        Ok(Arc::new(Self { directory, record: Mutex::new(record), options, lost: CancellationToken::new() }))
    }

    // This caller retains the exclusive journal lock while reclaiming the
    // directory entry and serving. Only the original journal can recover it.
    pub(crate) async fn reclaim_logkeeper(
        directory: Arc<dyn Directory>,
        registration: Registration,
        id: MemberId,
        stable_address: bool,
        options: LeaseOptions,
    ) -> Result<Arc<Self>> {
        let mut candidate = registration.record(id);
        let conflict = || Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/logkeeper-identity",
            "The durable logkeeper identity or address is already installed for another journal");
        let address_in_use = |record: &MemberRecord, other: &MemberRecord| {
            other.role == MemberRole::Logkeeper && other.id != record.id && other.address == record.address
        };
        if stable_address && directory.members().await?.iter().any(|other| address_in_use(&candidate, other)) {
            return Err(conflict());
        }
        candidate.state = MemberState::Joining;
        let mut draining = false;
        let mut record = tokio::time::timeout(options.lease + options.heartbeat, async {
            loop {
                let previous = directory.member(&candidate.id).await?;
                let now = now_ms();
                candidate.lease_expires_at_ms = now + options.lease.as_millis() as i64;
                let Some(previous) = previous else { return directory.register_member(candidate.clone()).await; };
                if previous.role != MemberRole::Logkeeper || (stable_address && previous.address != candidate.address) {
                    return Err(conflict());
                }
                if matches!(previous.decommission, Some(LogkeeperDecommission::Decommissioned { .. })) {
                    return Err(Anomaly::incorrect("rtdb.multi/logkeeper-decommissioned",
                        format!("Logkeeper {} was decommissioned and cannot rejoin the cell", previous.id)));
                }
                if !previous.is_live(now) {
                    draining = previous.state == MemberState::Draining;
                    candidate.follow(&previous);
                    if let Cas::Applied(record) = directory.replace_member(candidate.clone(), previous.version).await? {
                        return Ok(record);
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }).await.map_err(|_| Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/logkeeper-lease",
            "The logkeeper's previous membership has not expired"))??;
        if stable_address && directory.members().await?.iter().any(|other| address_in_use(&record, other)) {
            return Err(conflict());
        }
        let version = record.version;
        let reclaimed = record.clone();
        // An operator's drain of the journal outlives the process that serves it.
        record.state = if draining { MemberState::Draining } else { MemberState::Eligible };
        record.follow(&reclaimed);
        record.lease_expires_at_ms = now_ms() + options.lease.as_millis() as i64;
        let Cas::Applied(record) = directory.replace_member(record, version).await? else {
            return Err(conflict());
        };
        Ok(Arc::new(Self { directory, record: Mutex::new(record), options, lost: CancellationToken::new() }))
    }

    pub fn id(&self) -> MemberId { self.record.lock().id.clone() }
    pub fn record(&self) -> MemberRecord { self.record.lock().clone() }

    /// Cancelled once this member can no longer be sure its lease holds: it could not renew before expiry, or the cell declared it dead.
    /// Whatever it holds is then someone else's to claim, so it stops serving it.
    pub fn lost(&self) -> CancellationToken { self.lost.clone() }

    /// Renews until `cancellation` or loss.
    pub async fn run(self: Arc<Self>, cancellation: CancellationToken) {
        let mut ticker = tokio::time::interval(self.options.heartbeat);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => return,
                _ = ticker.tick() => {}
            }
            match self.renew().await {
                Ok(true) => {}
                Ok(false) => { self.lost.cancel(); return; }
                Err(_) if self.record.lock().lease_expires_at_ms <= now_ms() => { self.lost.cancel(); return; }
                Err(_) => {}
            }
        }
    }

    /// Ok(false) when the member has been declared dead.
    async fn renew(&self) -> Result<bool> {
        let current = self.record();
        let mut next = current.clone();
        next.lease_expires_at_ms = now_ms() + self.options.lease.as_millis() as i64;
        match self.directory.replace_member(next, current.version).await? {
            Cas::Applied(stored) => { *self.record.lock() = stored; Ok(true) }
            Cas::Conflict(Some(stored)) if current.role == MemberRole::Logkeeper && stored.state == MemberState::Draining
                && stored.address == current.address && stored.lease_expires_at_ms == current.lease_expires_at_ms =>
            {
                let mut next = stored.clone();
                next.lease_expires_at_ms = now_ms() + self.options.lease.as_millis() as i64;
                match self.directory.replace_member(next, stored.version).await? {
                    Cas::Applied(stored) => { *self.record.lock() = stored; Ok(true) }
                    Cas::Conflict(_) => Ok(false),
                }
            }
            Cas::Conflict(_) if current.role == MemberRole::Logkeeper => Ok(false),
            Cas::Conflict(Some(stored)) if stored.state == MemberState::Dead || stored.state == MemberState::Retired => Ok(false),
            Cas::Conflict(Some(stored)) => {
                // The controller moved this member's state (draining, say); keep its state and renew on top of it.
                let mut next = stored.clone();
                next.lease_expires_at_ms = now_ms() + self.options.lease.as_millis() as i64;
                match self.directory.replace_member(next, stored.version).await? {
                    Cas::Applied(stored) => { *self.record.lock() = stored; Ok(true) }
                    _ => Ok(true),
                }
            }
            Cas::Conflict(None) => Ok(false),
        }
    }

    /// Withdraws this node from new placements while it keeps serving what it holds.
    pub(crate) async fn begin_drain(&self) -> Result<()> {
        crate::admin::drain(self.directory.as_ref(), &self.id()).await
    }

    /// Leaves the cell: a retired member holds nothing and is never live again.
    pub async fn retire(&self) -> Result<()> {
        let mut current = self.record();
        for _ in 0..8 {
            let mut next = current.clone();
            if current.role == MemberRole::Logkeeper && current.state == MemberState::Draining {
                next.lease_expires_at_ms = now_ms();
            } else {
                next.state = MemberState::Retired;
            }
            next.follow(&current);
            match self.directory.replace_member(next, current.version).await? {
                Cas::Applied(stored) => { *self.record.lock() = stored; return Ok(()); }
                // A drain moved a node's record after this lease last read it.
                Cas::Conflict(Some(stored)) if stored.role != MemberRole::Logkeeper
                    && !matches!(stored.state, MemberState::Dead | MemberState::Retired) => current = stored,
                Cas::Conflict(_) => return Ok(()),
            }
        }
        Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/retire-contended", "The member's lease could not be retired"))
    }
}
