use ordered_logkeeper::ordered::{Command, Outcome, Refusal, Request, Window, service::Route};
use rtdb_directory::now_ms;
use rtdb_types::Result;
use uuid::Uuid;

use super::invalid;
use crate::journal::{InstalledJournal, adapter_command, refused, retention_decision::trim_target};

#[derive(Default)]
pub(super) struct Retention {
    worker_disposal: tokio::sync::Mutex<Option<Request>>,
}

impl Retention {
    pub(super) async fn dispose_withdrawn_workers(
        &self,
        custodian: &InstalledJournal,
    ) -> Result<()> {
        let mut pending = self.worker_disposal.lock().await;
        Self::settle_worker_disposal(custodian, &mut pending).await?;
        let mut after = None;
        loop {
            let workers = custodian.withdrawn_workers(after).await?;
            let Some(last) = workers.last() else {
                return Ok(());
            };
            after = Some(last.enrollment);
            let mut route = custodian.route().await?;
            for worker in workers {
                let now = u64::try_from(now_ms()).unwrap_or_default();
                if adapter_command::disposal_is_due(worker.eligible_at, now, route.time) {
                    *pending = Some(custodian.request(
                        &route,
                        adapter_command::dispose_expired_command(worker.enrollment),
                    )?);
                    Self::settle_worker_disposal(custodian, &mut pending).await?;
                    route = custodian.route().await?;
                }
            }
        }
    }

    async fn settle_worker_disposal(
        custodian: &InstalledJournal,
        pending: &mut Option<Request>,
    ) -> Result<()> {
        let Some(request) = pending.as_ref() else {
            return Ok(());
        };
        let Command::DisposeExpired { enrollment } = request.command else {
            return Err(invalid("The pending request is not an expiry disposal"));
        };
        let result = custodian.execute(request).await;
        if result
            .as_ref()
            .is_err_and(|error| error.code == "rtdb.multi/ordered-journal")
        {
            return result.map(|_| ());
        }
        *pending = None;
        match result?.receipt.outcome {
            Ok(Outcome::WorkerDisposed(disposed)) if disposed == enrollment => Ok(()),
            Err(Refusal::Unavailable | Refusal::Expired) => Ok(()),
            Err(reason) => Err(refused(reason)),
            Ok(_) => Err(invalid(
                "The journal disposed a different worker enrollment",
            )),
        }
    }

    pub(super) async fn retain_selected(
        &self,
        custodian: &InstalledJournal,
        window_bytes: Option<u64>,
    ) -> Result<()> {
        let mut route = custodian.route().await?;
        if let Some(through) = trim_target(route.floor, route.checkpoint.cut)
            && custodian.trim(&route, through).await.is_ok()
        {
            route = custodian.route().await?;
        }
        Self::replenish(custodian, &route, window_bytes).await
    }

    /// Replenish only bytes the installed voters have already paid for. The
    /// ordered decision recomputes the limit against concurrent appends.
    async fn replenish(
        custodian: &InstalledJournal,
        route: &Route,
        window_bytes: Option<u64>,
    ) -> Result<()> {
        let remaining = route.window.bytes - route.window_spent;
        let funded = window_bytes
            .unwrap_or(route.reusable_bytes)
            .min(route.reusable_bytes);
        let watermark = (crate::process::MAXIMUM_LOG_MESSAGE_BYTES as u64)
            .saturating_add(ordered_logkeeper::ordered::APPEND_RECORD_CHARGE_BYTES)
            .max(route.window.bytes / 4);
        if remaining > watermark || funded <= remaining {
            return Ok(());
        }
        let serial = route
            .window
            .serial
            .checked_add(1)
            .ok_or_else(|| invalid("The journal window serial space is exhausted"))?;
        let window = Window {
            allocation: Uuid::new_v4(),
            serial,
            bytes: funded,
        };
        custodian.fund(route, window).await
    }
}
