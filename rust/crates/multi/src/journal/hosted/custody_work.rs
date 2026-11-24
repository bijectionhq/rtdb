//! An enrolled process keeps each broker duty through its last plaintext owner.

use std::{path::PathBuf, sync::Arc, time::Duration};

use ordered_logkeeper::ordered::Placement;
use ring::signature::{Ed25519KeyPair, KeyPair};
use rtdb_types::Result;
use serde::Deserialize;
use tokio::sync::oneshot;
use tokio_util::task::TaskTracker;
use uuid::Uuid;

use crate::replay_broker::{
    Client,
    domain::{self, Action, Duty, WorkJoin},
};

use super::{erasure::CustodyWork, invalid};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Installation {
    pub(super) domain: Uuid,
    pub(super) admission_key: PathBuf,
}

#[derive(Clone)]
pub(super) struct WorkOwner {
    broker: Client,
    domain: Uuid,
    root: Uuid,
    history: [u8; 32],
    admission: Arc<Ed25519KeyPair>,
    process: Arc<Ed25519KeyPair>,
}

impl WorkOwner {
    pub(super) fn open(
        installation: Installation,
        broker: Client,
        placement: Placement,
    ) -> Result<Self> {
        if installation.domain.is_nil() {
            return Err(invalid("The installed custody domain has no identity"));
        }
        let mut bytes = std::fs::read(&installation.admission_key)
            .map_err(|_| invalid("The installed custody admission credential cannot be read"))?;
        let admission = Ed25519KeyPair::from_pkcs8(&bytes);
        bytes.fill(0);
        let admission = admission
            .map_err(|_| invalid("The installed custody admission credential is invalid"))?;
        let random = ring::rand::SystemRandom::new();
        let secret = Ed25519KeyPair::generate_pkcs8(&random)
            .map_err(|_| invalid("The process custody signer cannot be provisioned"))?;
        let process = Ed25519KeyPair::from_pkcs8(secret.as_ref())
            .map_err(|_| invalid("The process custody signer is invalid"))?;
        Ok(Self {
            broker,
            domain: installation.domain,
            root: placement.deployment,
            history: domain::installed_history(placement)?,
            admission: Arc::new(admission),
            process: Arc::new(process),
        })
    }

    pub(super) fn domain(&self) -> Uuid {
        self.domain
    }

    pub(super) async fn record_sql(&self, intent: domain::SqlIntent) -> Result<()> {
        let view = self.broker.custody_domain(self.domain, Action::Status).await?;
        if view.next_sql == Some(intent) { return Ok(()); }
        let generation = view.generation;
        let signature = self.admission.sign(&intent.signing_bytes(self.domain, generation)?).as_ref().to_vec();
        self.broker.custody_domain(self.domain, Action::RecordSql { generation, intent, signature }).await?;
        Ok(())
    }

    pub(super) async fn hold(
        &self,
        kind: CustodyWork,
        tasks: &TaskTracker,
    ) -> Result<Arc<dyn Send + Sync>> {
        let view = self
            .broker
            .custody_domain(self.domain, Action::Status)
            .await?;
        if view.operation.is_some() {
            return Err(rtdb_storage::Error::ErasedUnderView.into());
        }
        let generation = view.generation;
        let id = Uuid::new_v4();
        let duty = Duty {
            root: self.root,
            history: self.history,
            kind,
            verification_key: self
                .process
                .public_key()
                .as_ref()
                .try_into()
                .expect("Ed25519 key width"),
        };
        let statement = domain::admission_bytes(self.domain, generation, id, duty)?;
        let signature = self.admission.sign(&statement).as_ref().to_vec();
        let (send, ready) = oneshot::channel();
        let (lease, drained) = oneshot::channel::<()>();
        let owner = self.clone();
        tasks.spawn(async move {
            let admitted = owner
                .broker
                .custody_domain(
                    owner.domain,
                    Action::AdmitWorker {
                        generation,
                        id,
                        duty,
                        signature,
                    },
                )
                .await;
            let _ = send.send(admitted.map(|_| Arc::new(lease) as Arc<dyn Send + Sync>));
            let _ = drained.await;
            // A lost admission response can leave a duty in the roster too.
            owner.join(generation, id, duty).await;
        });
        ready
            .await
            .map_err(|_| invalid("The custody work admission ended"))?
    }

    async fn join(&self, generation: u64, id: Uuid, duty: Duty) {
        let observation: [u8; 32] = ring::digest::digest(&ring::digest::SHA256, id.as_bytes())
            .as_ref()
            .try_into()
            .expect("SHA-256 width");
        let Ok(statement) = WorkJoin::signing_bytes(self.domain, generation, id, duty, observation)
        else {
            return;
        };
        let join = WorkJoin {
            observation,
            signature: self.process.sign(&statement).as_ref().to_vec(),
        };
        loop {
            match self
                .broker
                .custody_domain(
                    self.domain,
                    Action::Join {
                        generation,
                        id,
                        join: join.clone(),
                    },
                )
                .await
            {
                Ok(_) => return,
                Err(error) => {
                    tracing::warn!(code = %error.code, "Installed custody drain awaits broker settlement")
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}
