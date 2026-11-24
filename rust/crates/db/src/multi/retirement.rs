use journal_contract::Lsn;
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use uuid::Uuid;

use super::{MultiDatabase, Record, next, no_writer};

/// The exact terminal in the deployment journal, before catalog and KMS settlement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeploymentRetirement {
    pub incarnation: Uuid,
    pub terminal: Lsn,
    pub generation: u64,
    pub lease_until: i64,
}

impl MultiDatabase {
    pub async fn deployment_retirement(&mut self) -> Result<Option<DeploymentRetirement>> {
        self.refresh().await?;
        Ok(self
            .state()
            .retired
            .map(|(terminal, lease_until)| self.retirement(terminal, lease_until)))
    }

    pub async fn retire_deployment(&mut self) -> Result<DeploymentRetirement> {
        if let Some(retirement) = self.deployment_retirement().await? {
            return Ok(retirement);
        }
        let lease = self.authority()?;
        self.require_destroyed_generation()?;
        if self.durable_transactions().index().has_custody_groups() {
            return Err(Anomaly::unsupported(
                "rtdb/deployment-custody",
                "Deployment retirement requires a complete independent custody key closure",
            ));
        }
        if self.reachability(lease).await?.pins().next().is_some() {
            return Err(Anomaly::new(
                AnomalyCategory::Busy,
                "rtdb/deployment-pins",
                "Release dependent checkpoint pins before retiring the deployment",
            ));
        }
        let head = self.state().head;
        let writer = self.writer.as_ref().ok_or_else(no_writer)?;
        if writer.pipeline.next_position() != self.state().coordinates.position(next(head)?)?
            || self.state().transactions.recover_pending().is_some()
        {
            return Err(Anomaly::new(
                AnomalyCategory::Busy,
                "rtdb/deployment-pending-writes",
                "The deployment writer is settling an admitted transaction",
            ));
        }
        let terminal = next(head)?;
        self.append_control(
            lease,
            Record::DeploymentRetirement {
                lease_until: lease.until,
            },
        )
        .await?;
        self.withdraw_writer();
        Ok(self.retirement(terminal, lease.until))
    }

    fn retirement(&self, terminal: Lsn, lease_until: i64) -> DeploymentRetirement {
        DeploymentRetirement {
            incarnation: self.options.database,
            terminal,
            generation: self.state().generation,
            lease_until,
        }
    }
}
