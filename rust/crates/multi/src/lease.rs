//! A member's registration and lease (`RenewLease`, `ExpireLease`).
//!
//! Liveness is per member: one renewal every `heartbeat`, whatever the member holds.

use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rtdb_directory::{Cas, Directory, MemberId, MemberRecord, MemberRole, MemberState, now_ms};
use rtdb_types::Result;
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

pub struct Registration { pub role: MemberRole, pub address: String, pub control_address: Option<String>, pub availability_zone: String }

impl MemberLease {
    pub async fn register(directory: Arc<dyn Directory>, registration: Registration, options: LeaseOptions) -> Result<Arc<Self>> {
        let record = directory.register_member(MemberRecord {
            id: MemberId::fresh(), role: registration.role, address: registration.address, control_address: registration.control_address,
            availability_zone: registration.availability_zone, engine_version: env!("CARGO_PKG_VERSION").into(),
            lease_expires_at_ms: now_ms() + options.lease.as_millis() as i64, state: MemberState::Eligible, version: 0,
        }).await?;
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

    /// Leaves the cell: a retired member holds nothing and is never live again.
    pub async fn retire(&self) -> Result<()> {
        let current = self.record();
        let mut next = current.clone();
        next.state = MemberState::Retired;
        if let Cas::Applied(stored) = self.directory.replace_member(next, current.version).await? { *self.record.lock() = stored; }
        Ok(())
    }
}
