//! A cell signs access to one frozen stage for one restore worker.

use std::time::Duration;

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::digest::{SHA256, digest};
use rtdb_directory::{DeploymentRecord, Lifecycle, now_ms};
use rtdb_pgwire::CapabilityVerifier;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use uuid::Uuid;

use crate::{archive::CapturePoint, capability::CellSecret};

use super::{Request, valid_manifest};

const SCOPE: &str = "~checkpoint-stage-reader";
const MAXIMUM_LEASE_MS: i64 = 35 * 60 * 1000;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StageGrant {
    pub point: CapturePoint,
    pub destination: Uuid,
    pub stage_operation: Uuid,
    pub staged_manifest_sha256: String,
    pub snapshot_sha256: String,
    pub target: Uuid,
    pub worker_sha256: String,
    pub until_ms: i64,
}

impl StageGrant {
    fn subject(&self, request: &Request) -> String {
        let bytes = serde_json::to_vec(&(&request.name, request.authority, request.capture, self))
            .expect("stage grant fields serialize");
        let digest = digest(&SHA256, &bytes);
        URL_SAFE_NO_PAD.encode(digest.as_ref())
    }

    fn remaining(&self, now: i64) -> Option<Duration> {
        let remaining = self.until_ms.checked_sub(now)?;
        (1..=MAXIMUM_LEASE_MS)
            .contains(&remaining)
            .then(|| Duration::from_millis(remaining as u64))
    }

    pub fn matches(&self, request: &Request, stage: &DeploymentRecord, org: &str) -> bool {
        let Some(held) = &stage.checkpoint_stage else {
            return false;
        };
        self.point.deployment == request.authority
            && self.point.operation == request.capture
            && self.point.incarnation == held.source_incarnation
            && self.point.log_epoch == held.source_log_epoch
            && self.point.checkpoint == held.checkpoint
            && self.point.checkpoint.system_time > 0
            && !self.target.is_nil()
            && self.target != request.authority
            && self.target != self.destination
            && !self.stage_operation.is_nil()
            && !self.destination.is_nil()
            && valid_manifest(&self.staged_manifest_sha256)
            && valid_manifest(&self.worker_sha256)
            && self.snapshot_sha256.len() == 64
            && self
                .snapshot_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            && stage.id == self.destination.to_string()
            && stage.lifecycle == Lifecycle::Restoring
            && held.intent.source == request.authority
            && held.intent.capture == request.capture
            && held.intent.destination == self.destination
            && held.intent.operation == self.stage_operation
            && held.intent.org == org
            && held.staged_manifest_sha256.as_deref() == Some(self.staged_manifest_sha256.as_str())
    }

    pub fn issue(&self, request: &Request, key: &CellSecret) -> Option<String> {
        self.remaining(now_ms())?;
        Some(key.mint_until(SCOPE, &self.subject(request), true, self.until_ms))
    }

    pub fn admit(
        &self,
        request: &Request,
        key: &CellSecret,
        signature: &str,
        worker: &str,
    ) -> Option<Instant> {
        let secret = URL_SAFE_NO_PAD.decode(worker).ok()?;
        if secret.len() != 32
            || URL_SAFE_NO_PAD.encode(digest(&SHA256, &secret).as_ref()) != self.worker_sha256
        {
            return None;
        }
        let claims = key.verify(signature, SCOPE).ok()?;
        if !claims.read_only || claims.principal != self.subject(request) {
            return None;
        }
        Some(Instant::now() + self.remaining(now_ms())?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_directory::Checkpoint;

    #[test]
    fn a_stage_grant_binds_worker_exact_scope_and_expiration() {
        let source = Uuid::new_v4();
        let capture = Uuid::new_v4();
        let request = Request {
            token: String::new(),
            name: "source".into(),
            authority: source,
            capture,
            action: super::super::Action::CaptureCheckpoint,
        };
        let worker = [7; 32];
        let worker_secret = URL_SAFE_NO_PAD.encode(worker);
        let mut grant = StageGrant {
            point: CapturePoint {
                deployment: source,
                operation: capture,
                incarnation: 1,
                log_epoch: 0,
                checkpoint: Checkpoint {
                    block: 1,
                    boundary: 2,
                    source: 3,
                    system_time: 4,
                },
            },
            destination: Uuid::new_v4(),
            stage_operation: Uuid::new_v4(),
            staged_manifest_sha256: URL_SAFE_NO_PAD.encode([0; 32]),
            snapshot_sha256: "a".repeat(64),
            target: Uuid::new_v4(),
            worker_sha256: URL_SAFE_NO_PAD.encode(digest(&SHA256, &worker).as_ref()),
            until_ms: now_ms() + 60_000,
        };
        let key = CellSecret::generate().unwrap();
        let accepted = grant.issue(&request, &key).unwrap();
        assert!(
            grant
                .admit(&request, &key, &accepted, &worker_secret)
                .is_some()
        );
        assert!(
            grant
                .admit(&request, &key, &accepted, &URL_SAFE_NO_PAD.encode([0; 32]))
                .is_none()
        );
        assert!(
            grant
                .admit(
                    &request,
                    &CellSecret::generate().unwrap(),
                    &accepted,
                    &worker_secret
                )
                .is_none()
        );
        grant.snapshot_sha256 = "b".repeat(64);
        assert!(
            grant
                .admit(&request, &key, &accepted, &worker_secret)
                .is_none()
        );
        grant.snapshot_sha256 = "a".repeat(64);
        grant.target = Uuid::new_v4();
        assert!(
            grant
                .admit(&request, &key, &accepted, &worker_secret)
                .is_none()
        );
        grant.until_ms = now_ms();
        assert!(grant.issue(&request, &key).is_none());
    }
}
