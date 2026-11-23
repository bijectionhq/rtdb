//! The RTDB projection of a preinstalled journal route.
//!
//! The route and its credential are immutable installation inputs. The
//! gateway and node agent own their request and activation lifetimes. The
//! journal owns the ordered writer, checkpoint, and replay decisions.

use ordered_logkeeper::ordered::{
    AppendResult, Checkpoint, CheckpointPreparation, Command, Cut, Enrollment, Fingerprint, Grant,
    Hold, Outcome, Placement, Principal, Refusal, Request, RequestId, Window,
    daemon::{ApplicationRoute, Client, Credentials},
    service::{
        ReplayHead, ReplayPage, Route,
        wire::{self, Operation, Reply, SignedBarrier},
    },
};

pub mod custody;
pub mod hosted;
mod log;
pub(crate) use log::{Held, JournalLog};
use ordered_logkeeper::ordered::TrimResult;
use rtdb_directory::DeploymentRecord;
use rtdb_storage::log::LogOffset;
use rtdb_types::{Anomaly, AnomalyCategory, Result, StorageEpoch};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstalledRoute {
    pub incarnation: u64,
    pub log_epoch: i32,
    pub authority: Authority,
    pub route: ApplicationRoute,
    pub credentials: Credentials,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Authority {
    Routing,
    Worker { enrollment: Enrollment },
    Custody,
}

impl Authority {
    fn principal(self) -> Principal {
        match self {
            Self::Routing => Principal::RouteReader,
            Self::Worker { enrollment } => Principal::Worker(enrollment),
            Self::Custody => Principal::Custodian,
        }
    }
}

pub struct InstalledJournal {
    client: Client,
    placement: Placement,
    incarnation: u64,
    log_epoch: StorageEpoch,
    authority: Authority,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct WriterAdmission {
    pub grant: Grant,
    pub recovery: Hold,
    pub position: u64,
}

impl InstalledJournal {
    pub fn open(config: InstalledRoute) -> Result<Arc<Self>> {
        let placement = config.route.placement;
        if config.incarnation == 0 || config.log_epoch < 0 {
            return Err(mismatch(
                "The installed journal has invalid RTDB coordinates",
            ));
        }
        let client = Client::new(config.route, &config.credentials)
            .map_err(|error| mismatch(&error.to_string()))?;
        Ok(Arc::new(Self {
            client,
            placement,
            incarnation: config.incarnation,
            log_epoch: StorageEpoch(config.log_epoch),
            authority: config.authority,
        }))
    }

    pub fn deployment(&self) -> Uuid {
        self.placement.deployment
    }
    pub fn placement(&self) -> Placement {
        self.placement
    }
    pub fn authority(&self) -> Authority {
        self.authority
    }
    pub fn log_epoch(&self) -> StorageEpoch {
        self.log_epoch
    }

    pub fn binds(&self, record: &DeploymentRecord) -> bool {
        Uuid::parse_str(&record.id).ok() == Some(self.deployment())
            && record.incarnation == self.incarnation
            && record.log_epoch == self.log_epoch.0
    }

    pub async fn route(&self) -> Result<Route> {
        let reply = self.call(Operation::Route).await?;
        let Reply::Route(route) = reply else {
            return Err(mismatch(
                "The peer did not return an installed journal route",
            ));
        };
        Ok(route)
    }

    pub fn request(&self, route: &Route, command: Command) -> Result<Request> {
        if route.placement != self.placement {
            return Err(mismatch(
                "The route addresses a different journal placement",
            ));
        }
        let id = RequestId::fresh(route.position, route.retry_positions).map_err(refused)?;
        Ok(Request {
            placement: self.placement,
            principal: self.authority.principal(),
            id,
            command,
        })
    }

    pub fn nomination(&self, route: &Route) -> Result<Request> {
        let Authority::Worker { enrollment } = self.authority else {
            return Err(mismatch("Only an enrolled RTDB worker can nominate itself"));
        };
        self.request(
            route,
            Command::Grant {
                expected: route.writer,
                candidate: enrollment,
            },
        )
    }

    pub async fn grant(&self, nomination: &Request) -> Result<WriterAdmission> {
        let Command::Grant { candidate, .. } = &nomination.command else {
            return Err(mismatch("The request is not a writer nomination"));
        };
        let decision = self.execute(nomination).await?;
        let Outcome::Granted { grant, recovery } = decision.receipt.outcome.map_err(refused)?
        else {
            return Err(mismatch("The journal did not return a writer grant"));
        };
        if grant.enrollment != *candidate
            || recovery.owner != *candidate
            || recovery.id != nomination.id
            || recovery.admitted_at != decision.receipt.position
        {
            return Err(mismatch(
                "The writer grant and its protected recovery basis disagree",
            ));
        }
        Ok(WriterAdmission {
            grant,
            recovery,
            position: decision.receipt.position,
        })
    }

    pub async fn reconcile(&self, grant: Grant, original: Request) -> Result<AppendResult> {
        let reply = self.call(Operation::Reconcile { grant, original }).await?;
        let Reply::Reconciled(result) = reply else {
            return Err(mismatch("The peer did not reconcile the original append"));
        };
        Ok(result)
    }

    pub async fn barrier(&self, invocation: &Request) -> Result<SignedBarrier> {
        if !matches!(&invocation.command, Command::Barrier { .. }) {
            return Err(mismatch("The request is not a fresh read invocation"));
        }
        let decision = self.execute(invocation).await?;
        decision.receipt.outcome.map_err(refused)?;
        decision.barrier.ok_or_else(|| {
            Anomaly::new(
                AnomalyCategory::Unavailable,
                "rtdb.multi/ordered-read-closed",
                "The original read no longer has a protected basis",
            )
        })
    }

    pub async fn replay(&self, hold: Hold, from: Cut, maximum_bytes: u64) -> Result<ReplayPage> {
        let reply = self
            .call(Operation::Replay {
                hold,
                from,
                maximum_bytes,
            })
            .await?;
        let Reply::Replay(page) = reply else {
            return Err(mismatch(
                "The peer did not return the protected replay page",
            ));
        };
        Ok(page)
    }

    pub async fn checkpoint_replay(
        &self,
        preparation: CheckpointPreparation,
        base: Checkpoint,
        from: Cut,
        maximum_bytes: u64,
    ) -> Result<ReplayPage> {
        match self
            .call(Operation::CheckpointReplay {
                preparation,
                base,
                from,
                maximum_bytes,
            })
            .await?
        {
            Reply::Replay(page) => Ok(page),
            _ => Err(mismatch(
                "The peer did not return the prepared checkpoint replay",
            )),
        }
    }

    pub async fn replay_head(&self, hold: Hold, from: Cut) -> Result<Option<ReplayHead>> {
        let reply = self.call(Operation::ReplayHead { hold, from }).await?;
        let Reply::ReplayHead(head) = reply else {
            return Err(mismatch(
                "The peer did not return the protected replay head",
            ));
        };
        Ok(head)
    }

    pub async fn hold(&self, request: &Request) -> Result<Hold> {
        let decision = self.execute(request).await?;
        let Outcome::Protected(hold) = decision.receipt.outcome.map_err(refused)? else {
            return Err(mismatch(
                "The peer did not protect the ordered replay interval",
            ));
        };
        if hold.id != request.id || hold.admitted_at != decision.receipt.position {
            return Err(mismatch("The replay hold addresses a different invocation"));
        }
        Ok(hold)
    }

    pub async fn release(&self, hold: Hold) -> Result<()> {
        match self.call(Operation::Release { hold }).await? {
            Reply::Released(closed) if closed == hold => Ok(()),
            _ => Err(mismatch("The peer released a different replay hold")),
        }
    }

    pub async fn vacate(&self, grant: Grant) -> Result<()> {
        let route = self.route().await?;
        if route.writer.grant != Some(grant) {
            return Ok(());
        }
        let decision = self
            .execute(&self.request(&route, Command::Vacate { grant })?)
            .await?;
        match decision.receipt.outcome {
            Ok(Outcome::Vacated) | Err(Refusal::Stale) => Ok(()),
            Err(reason) => Err(refused(reason)),
            Ok(_) => Err(mismatch(
                "The peer returned a different writer release result",
            )),
        }
    }

    pub async fn append(&self, request: &Request) -> Result<AppendResult> {
        let decision = self.execute(request).await?;
        let Outcome::Appended(result) = decision.receipt.outcome.map_err(refused)? else {
            return Err(mismatch(
                "The peer did not return the ordered append receipt",
            ));
        };
        if result.position != decision.receipt.position {
            return Err(mismatch(
                "The append result names a different command position",
            ));
        }
        Ok(result)
    }

    pub async fn checkpoint_cut(&self, next: u64) -> Result<Cut> {
        match self.call(Operation::CheckpointCut { next }).await? {
            Reply::CheckpointCut(cut) if cut.next_lsn == next => Ok(cut),
            _ => Err(mismatch("The peer selected a different checkpoint cut")),
        }
    }

    pub async fn prepare_checkpoint(
        &self,
        route: &Route,
        preparation: CheckpointPreparation,
    ) -> Result<()> {
        let request = self.request(route, Command::PrepareCheckpoint { preparation })?;
        let decision = self.execute(&request).await?;
        match decision.receipt.outcome.map_err(refused)? {
            Outcome::CheckpointPrepared(chosen) if chosen == preparation => Ok(()),
            _ => Err(mismatch("The journal prepared a different checkpoint")),
        }
    }

    pub async fn close_checkpoint(&self, route: &Route, checkpoint: Checkpoint) -> Result<()> {
        let request = self.request(route, Command::CloseCheckpoint { checkpoint })?;
        let decision = self.execute(&request).await?;
        match decision.receipt.outcome.map_err(refused)? {
            Outcome::CheckpointClosed(closed) if closed == checkpoint => Ok(()),
            _ => Err(mismatch(
                "The journal closed a different checkpoint allocation",
            )),
        }
    }

    pub async fn settle_checkpoint(
        &self,
        route: &Route,
        checkpoint: Checkpoint,
        disposition: Fingerprint,
    ) -> Result<()> {
        let request = self.request(
            route,
            Command::SettleCheckpoint {
                checkpoint,
                disposition,
            },
        )?;
        let decision = self.execute(&request).await?;
        match decision.receipt.outcome.map_err(refused)? {
            Outcome::CheckpointSettled(settled) if settled == checkpoint => Ok(()),
            _ => Err(mismatch(
                "The journal settled a different checkpoint allocation",
            )),
        }
    }

    pub async fn publish(&self, route: &Route, checkpoint: Checkpoint) -> Result<()> {
        let decision = self
            .execute(&self.request(route, Command::Publish { checkpoint })?)
            .await?;
        match decision.receipt.outcome.map_err(refused)? {
            Outcome::Published(published) if published == checkpoint => Ok(()),
            _ => Err(mismatch("The journal published a different checkpoint")),
        }
    }

    pub async fn trim(&self, route: &Route, through: Cut) -> Result<TrimResult> {
        let command = Command::Trim {
            selected: route.checkpoint,
            through,
        };
        let decision = self.execute(&self.request(route, command)?).await?;
        match decision.receipt.outcome.map_err(refused)? {
            Outcome::Trimmed(trimmed) if trimmed.through == through => Ok(trimmed),
            _ => Err(mismatch("The journal trimmed through a different cut")),
        }
    }

    pub async fn fund(&self, route: &Route, window: Window) -> Result<()> {
        let decision = self
            .execute(&self.request(route, Command::FundWindow { window })?)
            .await?;
        match decision.receipt.outcome.map_err(refused)? {
            Outcome::WindowFunded(funded) if funded == window => Ok(()),
            _ => Err(mismatch("The journal funded a different allocation")),
        }
    }

    pub async fn execute(&self, request: &Request) -> Result<wire::Decision> {
        if request.placement != self.placement || request.principal != self.authority.principal() {
            return Err(mismatch(
                "The request addresses a different installed journal credential",
            ));
        }
        let reply = self.call(Operation::Execute(request.clone())).await?;
        let Reply::Decision(decision) = reply else {
            return Err(mismatch("The peer did not return the ordered decision"));
        };
        Ok(decision)
    }

    async fn call(&self, operation: Operation) -> Result<Reply> {
        match self.client.call(operation).await.map_err(|error| {
            Anomaly::new(
                AnomalyCategory::Unavailable,
                "rtdb.multi/ordered-journal",
                error.to_string(),
            )
        })? {
            Reply::Refused(reason) => Err(refused(reason)),
            reply => Ok(reply),
        }
    }
}

pub fn latest(cut: Cut) -> Result<LogOffset> {
    if cut.next_lsn == 0 {
        return Ok(LogOffset(-1));
    }
    i64::try_from(cut.next_lsn - 1)
        .map(LogOffset)
        .map_err(|_| mismatch("The journal position exceeds the RTDB log range"))
}

fn mismatch(message: &str) -> Anomaly {
    Anomaly::incorrect("rtdb.multi/ordered-journal-binding", message.to_owned())
}

fn refused(reason: Refusal) -> Anomaly {
    use AnomalyCategory::{Conflict, Forbidden, Incorrect, Unavailable};
    let (category, code) = match reason {
        Refusal::Unauthorized => (Forbidden, "rtdb.multi/ordered-credentials"),
        Refusal::Stale => (Conflict, "rtdb.multi/ordered-stale-grant"),
        Refusal::Conflict => (Conflict, "rtdb.multi/ordered-conflict"),
        Refusal::Expired => (Conflict, "rtdb.multi/ordered-expired"),
        Refusal::Capacity => (Unavailable, "rtdb.multi/ordered-capacity"),
        Refusal::ReadPending => (Unavailable, "rtdb.multi/ordered-read-pending"),
        Refusal::Unavailable => (Unavailable, "rtdb.multi/ordered-replay-unavailable"),
        Refusal::CohortSealed => (Unavailable, "rtdb.multi/ordered-cohort-sealed"),
        Refusal::Invalid | Refusal::WrongPlacement => {
            (Incorrect, "rtdb.multi/ordered-journal-binding")
        }
    };
    Anomaly::new(
        category,
        code,
        format!("Ordered journal refused the operation ({reason:?})"),
    )
}
