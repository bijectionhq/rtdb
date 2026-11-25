use std::collections::BTreeSet;

use journal_contract::{JournalError, StreamId};
use rtdb_db::{ClassicInstallation, Role, discover_nodes};
use rtdb_tx::transact::TransactState;
use rtdb_types::{Anomaly, AnomalyCategory, Result, TransactionKey, rtdb_v1::Basis};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    DatabaseOpen, MultiRuntime,
    catalog::{Deployment, Status},
    classic_ingress::{HeldReady, ReadySelection},
    open_database, validate_deployment_name,
};

#[derive(Serialize, Deserialize)]
pub struct ClassicCandidate {
    pub reserved_at: Basis,
    pub projection_sha256: [u8; 32],
    pub installation: Option<ClassicInstallation>,
}

impl MultiRuntime {
    /// Reserves a closed native catalog entry for a verified Classic source.
    pub(super) async fn stage_classic_candidate(
        &self,
        name: &str,
        incarnation: Uuid,
        request: [u8; 32],
        projection: [u8; 32],
    ) -> Result<ClassicCandidate> {
        self.require_classic_candidate(name, incarnation, request)?;
        if projection == [0; 32] {
            return Err(conflict());
        }
        if let Some(existing) = self
            .classic_candidate_status(name, incarnation, request)
            .await?
        {
            return if existing.projection_sha256 == projection {
                Ok(existing)
            } else {
                Err(conflict())
            };
        }
        match self
            .backends
            .journal
            .head(StreamId(incarnation.as_u128()))
            .await
        {
            Err(JournalError::NotFound) => (),
            Ok(_) => {
                let existing = self
                    .classic_candidate_status(name, incarnation, request)
                    .await?
                    .ok_or_else(conflict)?;
                return if existing.projection_sha256 == projection {
                    Ok(existing)
                } else {
                    Err(conflict())
                };
            }
            Err(error) => {
                return Err(Anomaly::new(
                    AnomalyCategory::Unavailable,
                    "rtdb/classic-candidate-stream",
                    error.to_string(),
                ));
            }
        }

        if let (Some(host), Some(peer)) = (&self.backends.peers, &self.peer) {
            peer.observe(&discover_nodes(host).await?);
        } else {
            self.catalog
                .tick(&BTreeSet::from([self.backends.node]))
                .await?;
        }
        let selected = self
            .catalog
            .reserve_classic_candidate(name, incarnation, request, projection)
            .await?;
        self.inspect_classic_candidate(selected, request, projection)
            .await
    }

    /// A closed candidate has no serving driver. This operation uses only its private writer lease.
    pub(super) async fn install_classic_candidate(
        &self,
        name: &str,
        incarnation: Uuid,
        request: [u8; 32],
        projection: [u8; 32],
        transactions: &TransactState,
    ) -> Result<ClassicCandidate> {
        let (selected, expected) = self
            .classic_candidate(name, incarnation, request)
            .await?
            .ok_or_else(conflict)?;
        if expected != projection {
            return Err(conflict());
        }
        let mut candidate = self
            .inspect_classic_candidate(selected, request, projection)
            .await?;
        if candidate.installation.is_some() {
            return Ok(candidate);
        }
        let mut database = open_database(
            &self.backends,
            &self.kms,
            incarnation,
            DatabaseOpen::Required,
        )
        .await?;
        let live = match &self.backends.peers {
            Some(host) => discover_nodes(host).await?,
            None => BTreeSet::from([self.backends.node]),
        };
        if !matches!(database.tick(&live).await?, Role::Writer(_)) {
            return Err(Anomaly::new(
                AnomalyCategory::Unavailable,
                "rtdb/classic-candidate-writer",
                "The exact private candidate lease is still settling",
            ));
        }
        candidate.installation = Some(
            database
                .install_classic_checkpoint(request, transactions)
                .await?,
        );
        Ok(candidate)
    }

    /// Reads the closed reservation and committed restore without acquiring a writer lease.
    pub(super) async fn classic_candidate_status(
        &self,
        name: &str,
        incarnation: Uuid,
        request: [u8; 32],
    ) -> Result<Option<ClassicCandidate>> {
        let Some((selected, projection)) =
            self.classic_candidate(name, incarnation, request).await?
        else {
            return Ok(None);
        };
        Ok(Some(
            self.inspect_classic_candidate(selected, request, projection)
                .await?,
        ))
    }

    /// Checks an exact historical cut against the closed native stream and its reservation.
    pub(super) async fn classic_candidate_cut(
        &self,
        name: &str,
        incarnation: Uuid,
        request: [u8; 32],
        original: TransactionKey,
    ) -> Result<Basis> {
        let (selected, _) = self
            .classic_candidate(name, incarnation, request)
            .await?
            .ok_or_else(conflict)?;
        let mut database = open_database(
            &self.backends,
            &self.kms,
            selected.database,
            DatabaseOpen::Prepared,
        )
        .await?;
        Ok(database
            .classic_checkpoint_cut(request, original)
            .await?
            .basis)
    }

    pub(super) async fn classic_held_ready(
        &self,
        name: &str,
        incarnation: Uuid,
        binding: [u8; 32],
        candidate: &ClassicCandidate,
        ready: &ReadySelection,
        record: bool,
    ) -> Result<Option<HeldReady>> {
        if candidate.reserved_at != ready.reserved_at
            || candidate.installation.as_ref() != Some(&ready.installation)
            || self
                .classic_candidate_cut(name, incarnation, binding, ready.classic_cut)
                .await?
                != ready.native_basis
        {
            return Err(conflict());
        }
        self.catalog
            .classic_held_ready(name, incarnation, binding, ready, record)
            .await
    }

    async fn classic_candidate(
        &self,
        name: &str,
        incarnation: Uuid,
        request: [u8; 32],
    ) -> Result<Option<(Deployment, [u8; 32])>> {
        self.require_classic_candidate(name, incarnation, request)?;
        let Some(selected) = self.catalog.read().await?.remove(name) else {
            return Ok(None);
        };
        match selected.status {
            Status::ClassicCandidate {
                request: prior,
                projection,
                ..
            } if selected.database == incarnation && prior == request => {
                Ok(Some((selected, projection)))
            }
            _ => Err(conflict()),
        }
    }

    async fn inspect_classic_candidate(
        &self,
        selected: Deployment,
        request: [u8; 32],
        projection: [u8; 32],
    ) -> Result<ClassicCandidate> {
        let reserved_at = match &selected.status {
            Status::ClassicCandidate {
                held: Some(held), ..
            } => held.reserved_at,
            _ => selected.written,
        };
        let installation = match self
            .backends
            .journal
            .head(StreamId(selected.database.as_u128()))
            .await
        {
            Ok(head) if head.0 > 0 => {
                let mut database = open_database(
                    &self.backends,
                    &self.kms,
                    selected.database,
                    DatabaseOpen::Prepared,
                )
                .await?;
                database.classic_checkpoint_status(request).await?
            }
            Ok(_) | Err(JournalError::NotFound) => None,
            Err(error) => {
                return Err(Anomaly::new(
                    AnomalyCategory::Unavailable,
                    "rtdb/classic-candidate-stream",
                    error.to_string(),
                ));
            }
        };
        Ok(ClassicCandidate {
            reserved_at,
            projection_sha256: projection,
            installation,
        })
    }

    fn require_classic_candidate(
        &self,
        name: &str,
        incarnation: Uuid,
        request: [u8; 32],
    ) -> Result<()> {
        validate_deployment_name(name)?;
        if self.follower.is_some()
            || self.bootstrap.contains_key(name)
            || self.bootstrap.values().any(|id| *id == incarnation)
            || incarnation.is_nil()
            || request == [0; 32]
        {
            return Err(conflict());
        }
        Ok(())
    }
}

fn conflict() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict,
        "rtdb/classic-candidate",
        "The native catalog differs from the exact Classic migration reservation",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NodeConfiguration;
    use crate::multi::classic_ingress::HeldAssignment;
    use rtdb_db::classic_import_basis;
    use rtdb_types::{MemoryBudget, Timestamp, TxId};

    #[tokio::test]
    async fn held_ready_classic_candidate_recovers_the_same_ordered_record_and_stays_closed()
    -> Result<()> {
        let files = tempfile::tempdir().unwrap();
        let mut configuration =
            NodeConfiguration::from_yaml_with_environment("", &|_| None, false)?;
        let runtime = MultiRuntime::development(
            files.path(),
            &mut configuration,
            MemoryBudget::new(32 << 20),
        )
        .await?;
        let incarnation = Uuid::new_v4();
        let selected = runtime
            .stage_classic_candidate("private", incarnation, [1; 32], [2; 32])
            .await?;
        assert!(selected.installation.is_none());
        assert!(
            runtime
                .stage_classic_candidate("private", incarnation, [3; 32], [2; 32])
                .await
                .is_err()
        );
        assert!(
            runtime
                .stage_classic_candidate("other", incarnation, [1; 32], [2; 32])
                .await
                .is_err()
        );

        let cut = rtdb_types::TransactionKey {
            tx_id: TxId(10),
            system_time: Timestamp(11),
        };
        let mut transactions = TransactState::new(*incarnation.as_bytes());
        let mut index = transactions.index().clone();
        let application_cut = TransactionKey {
            tx_id: TxId(7),
            system_time: Timestamp(5),
        };
        let application_basis =
            classic_import_basis(*incarnation.as_bytes(), application_cut.tx_id)?;
        let mut application = index.head();
        application.basis = application_basis;
        application.system_time = application_cut.system_time;
        index.advance_classic(application)?;
        let basis = classic_import_basis(*incarnation.as_bytes(), cut.tx_id)?;
        let mut native_cut = index.head();
        native_cut.basis = basis;
        native_cut.system_time = cut.system_time;
        index.advance_classic(native_cut)?;
        transactions.replace_applied_index(index)?;
        let installed = runtime
            .install_classic_candidate("private", incarnation, [1; 32], [2; 32], &transactions)
            .await?;
        assert_eq!(installed.reserved_at, selected.reserved_at);
        assert_eq!(installed.installation.as_ref().unwrap().basis, basis);
        assert_eq!(
            runtime
                .classic_candidate_status("private", incarnation, [1; 32])
                .await?
                .unwrap()
                .installation,
            installed.installation
        );
        assert_eq!(
            runtime
                .classic_candidate_cut("private", incarnation, [1; 32], cut)
                .await?,
            basis
        );
        let ready = ReadySelection {
            endpoint: "https://managed.internal".into(),
            placement: HeldAssignment {
                epoch: 9,
                assignment: [3; 16],
                holder: [4; 16],
            },
            classic_cut: application_cut,
            native_basis: application_basis,
            reserved_at: installed.reserved_at,
            installation: installed.installation.clone().unwrap(),
        };
        assert!(
            runtime
                .classic_held_ready("private", incarnation, [1; 32], &selected, &ready, true)
                .await
                .is_err()
        );
        assert!(
            runtime
                .classic_held_ready("private", incarnation, [1; 32], &installed, &ready, false)
                .await?
                .is_none()
        );
        let mut wrong = ready.clone();
        wrong.classic_cut.system_time.0 += 1;
        assert!(
            runtime
                .classic_held_ready("private", incarnation, [1; 32], &installed, &wrong, true)
                .await
                .is_err()
        );
        wrong = ready.clone();
        wrong.installation.manifest_ciphertext_sha256[0] ^= 1;
        assert!(
            runtime
                .classic_held_ready("private", incarnation, [1; 32], &installed, &wrong, true)
                .await
                .is_err()
        );
        let ordered = runtime
            .classic_held_ready("private", incarnation, [1; 32], &installed, &ready, true)
            .await?
            .unwrap();
        assert_eq!(ordered.ready, ready);
        assert!(ordered.recorded_at > ready.reserved_at);
        runtime
            .stage_classic_candidate("other", Uuid::new_v4(), [3; 32], [4; 32])
            .await?;
        let reopened = runtime
            .classic_candidate_status("private", incarnation, [1; 32])
            .await?
            .unwrap();
        assert_eq!(reopened.reserved_at, ready.reserved_at);
        assert_eq!(
            runtime
                .classic_held_ready("private", incarnation, [1; 32], &reopened, &ready, false)
                .await?,
            Some(ordered.clone())
        );
        assert_eq!(
            runtime
                .classic_held_ready("private", incarnation, [1; 32], &reopened, &ready, true)
                .await?,
            Some(ordered)
        );
        wrong = ready;
        wrong.placement.holder = [5; 16];
        assert!(
            runtime
                .classic_held_ready("private", incarnation, [1; 32], &reopened, &wrong, true)
                .await
                .is_err()
        );
        assert!(
            runtime
                .classic_held_ready("private", incarnation, [1; 32], &reopened, &wrong, false)
                .await
                .is_err()
        );
        assert!(!runtime.catalog.read().await?["private"].status.ready());
        assert!(
            runtime
                .provision("private", Some(incarnation))
                .await
                .is_err()
        );
        assert!(runtime.ready_driver("private", incarnation).await.is_err());
        assert!(!runtime.databases.read().unwrap().contains_key("private"));
        runtime.close().await;
        Ok(())
    }
}
