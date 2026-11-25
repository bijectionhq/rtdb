use std::{collections::BTreeSet, net::IpAddr};

use rtdb_db::{Role, preferred};
use rtdb_tx::transact::workload::{self, Fence};
use rtdb_types::{Anomaly, AnomalyCategory, Result, rtdb_v1::Basis};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{DatabaseOpen, MultiRuntime, classic_ingress::HeldReady, open_database};

pub(crate) mod operator;
mod transport;
pub(super) use transport::Ingress;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Scope {
    domain: String,
    authority: Uuid,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Attempt {
    command_id: [u8; 16],
    command_digest: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AppliedAt {
    history_id: [u8; 16],
    sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Decision {
    scope: Scope,
    attempt: Attempt,
    begin_sha256: [u8; 32],
    applied_at: AppliedAt,
    decision_sha256: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Witness {
    decision: Decision,
    pub(super) held_ready: HeldReady,
}

impl Witness {
    pub(super) fn name(&self) -> &str {
        &self.decision.scope.domain
    }

    pub(super) fn database(&self) -> Uuid {
        self.decision.scope.authority
    }

    pub(super) fn valid(&self, catalog: Uuid) -> bool {
        let witness = &self.decision;
        let held = &self.held_ready;
        super::validate_deployment_name(self.name()).is_ok()
            && !self.database().is_nil()
            && self.database() != catalog
            && witness.attempt.command_id != [0; 16]
            && witness.attempt.command_digest != [0; 32]
            && witness.begin_sha256 != [0; 32]
            && witness.applied_at.history_id != [0; 16]
            && witness.applied_at.sequence > 0
            && witness.decision_sha256 != [0; 32]
            && held.binding_sha256 != [0; 32]
            && held.ready.valid_for(catalog, self.database())
            && held.recorded_at.incarnation() == *catalog.as_bytes()
            && held.recorded_at > held.ready.reserved_at
    }

    fn placement(&self) -> workload::Placement {
        let placement = &self.held_ready.ready.placement;
        workload::Placement {
            epoch: placement.epoch,
            assignment: Some(workload::Holder {
                assignment_id: placement.assignment,
                holder: Uuid::from_bytes(placement.holder).to_string(),
            }),
        }
    }

    pub(super) fn valid_fence(&self, fence: &Fence) -> bool {
        let install = &self.held_ready.ready.installation;
        fence.placement == self.placement()
            && fence.applied.namespace != [0; 32]
            && fence.applied.key.as_slice() == self.decision.decision_sha256.as_slice()
            && Basis::from_bytes(&fence.applied.basis).is_ok_and(|basis| {
                basis.incarnation() == install.basis.incarnation() && basis > install.basis
            })
            && fence.applied.system_time_micros >= install.system_time.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Record {
    pub(super) witness: Witness,
    pub(super) phase: Phase,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Phase {
    Opening {
        profile: [u8; 32],
    },
    Opened {
        profile: [u8; 32],
        workload_fence: Fence,
    },
    Excluded,
}

pub(super) enum Transition {
    Observe,
    Reserve([u8; 32]),
    Exclude,
    Finish { profile: [u8; 32], fence: Fence },
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
enum Reply {
    Opened {
        decision: Decision,
        held_ready: HeldReady,
        ordered_at: Basis,
        workload_fence: Fence,
    },
    Excluded {
        decision: Decision,
        held_ready: HeldReady,
        ordered_at: Basis,
    },
    Redirect {
        writer: IpAddr,
    },
    Unresolved,
    Refused,
}

impl Record {
    fn reply(self, ordered_at: Basis) -> Reply {
        let Witness {
            decision,
            held_ready,
        } = self.witness;
        match self.phase {
            Phase::Opening { .. } => Reply::Unresolved,
            Phase::Opened { workload_fence, .. } => Reply::Opened {
                decision,
                held_ready,
                ordered_at,
                workload_fence,
            },
            Phase::Excluded => Reply::Excluded {
                decision,
                held_ready,
                ordered_at,
            },
        }
    }
}

impl MultiRuntime {
    async fn open_classic_workload(
        &self,
        witness: &Witness,
        live: &BTreeSet<IpAddr>,
    ) -> Result<Reply> {
        let profile = self
            .peer
            .as_ref()
            .and_then(|peer| peer.workload_profile())
            .ok_or_else(denied)?;
        let point = profile.point.clone();
        let writer = preferred(witness.database(), live).ok_or_else(unavailable)?;
        if writer != self.backends.node {
            return Ok(Reply::Redirect { writer });
        }
        let (reserved, basis) = self
            .catalog
            .classic_opening(witness, Transition::Reserve(profile.identity))
            .await?
            .ok_or_else(denied)?;
        if !matches!(&reserved.phase, Phase::Opening { .. }) {
            return Ok(reserved.reply(basis));
        }
        let mut database = open_database(
            &self.backends,
            &self.kms,
            witness.database(),
            DatabaseOpen::Required,
        )
        .await?;
        if !matches!(database.tick(live).await?, Role::Writer(_)) {
            return Err(unavailable());
        }
        let placement = workload::ImportedPlacement::new(
            point,
            witness.placement(),
            witness.decision.decision_sha256,
            witness.held_ready.ready.native_basis,
            witness.held_ready.ready.classic_cut.system_time,
        );
        let fence = database
            .open_classic_placement(
                witness.held_ready.binding_sha256,
                &witness.held_ready.ready.installation,
                &placement,
            )
            .await?;
        let (record, basis) = self
            .catalog
            .classic_opening(
                witness,
                Transition::Finish {
                    profile: profile.identity,
                    fence,
                },
            )
            .await?
            .ok_or_else(denied)?;
        Ok(record.reply(basis))
    }
}

pub(super) fn denied() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Forbidden,
        "rtdb/classic-opening",
        "The exact Classic opening is not authorized",
    )
}

fn unavailable() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb/classic-opening",
        "The exact Classic opening is unresolved",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NodeConfiguration;
    use crate::multi::classic_ingress::{HeldAssignment, ReadySelection};
    use rtdb_db::classic_import_basis;
    use rtdb_format::data::{StoredEvent, StoredOperation};
    use rtdb_tx::transact::TransactState;
    use rtdb_types::{
        EntityId, EventPosition, Interval, MemoryBudget, StructFields, Timestamp, TransactionKey,
        TxId, Value,
    };

    async fn held(
        runtime: &MultiRuntime,
        name: &str,
        decision_sha256: [u8; 32],
    ) -> Result<Witness> {
        let database = Uuid::new_v4();
        let binding = [1; 32];
        let candidate = runtime
            .stage_classic_candidate(name, database, binding, [2; 32])
            .await?;
        let point =
            workload::Point::new("public.workload_installation", "authority".into(), Timestamp(0))?;
        let mut state = TransactState::new(*database.as_bytes());
        let mut index = state.index().clone();
        let application_cut = TransactionKey {
            tx_id: TxId(9),
            system_time: Timestamp(10),
        };
        let mut cut = index.head();
        cut.basis = classic_import_basis(*database.as_bytes(), application_cut.tx_id)?;
        cut.system_time = application_cut.system_time;
        index.advance_classic(cut)?;
        let id = EntityId::String("authority".into());
        index.apply_classic_event(
            &point.table,
            StoredEvent {
                position: Some(EventPosition {
                    transaction: application_cut.tx_id,
                    ordinal: 0,
                }),
                origin: None,
                entity_key: None,
                iid: id.iid(),
                system_from: cut.system_time,
                commit_position: 0,
                valid_time: Interval::new(point.valid_at, Timestamp::MAX).unwrap(),
                recency: None,
                operation: StoredOperation::Put(StructFields::from([
                    ("_id".into(), id.as_value()),
                    (
                        "incarnation".into(),
                        Value::Binary(database.as_bytes().to_vec()),
                    ),
                    ("kind".into(), Value::Utf8("hosted".into())),
                    ("placement_epoch".into(), Value::Utf8(format!("{:020}", 7))),
                    ("placement_assignment".into(), Value::Binary(vec![3; 16])),
                    ("writer".into(), Value::Utf8(String::new())),
                ])),
            },
            Some(application_cut.tx_id),
        )?;
        state.replace_applied_index(index)?;
        let installed = runtime
            .install_classic_candidate(name, database, binding, [2; 32], &state)
            .await?;
        let ready = ReadySelection {
            endpoint: "https://managed.internal".into(),
            placement: HeldAssignment {
                epoch: 8,
                assignment: [4; 16],
                holder: [5; 16],
            },
            classic_cut: application_cut,
            native_basis: cut.basis,
            reserved_at: candidate.reserved_at,
            installation: installed.installation.clone().unwrap(),
        };
        let held_ready = runtime
            .classic_held_ready(name, database, binding, &installed, &ready, true)
            .await?
            .unwrap();
        Ok(Witness {
            decision: Decision {
                scope: Scope {
                    domain: name.into(),
                    authority: database,
                },
                attempt: Attempt {
                    command_id: [6; 16],
                    command_digest: [7; 32],
                },
                begin_sha256: [8; 32],
                applied_at: AppliedAt {
                    history_id: [9; 16],
                    sequence: 12,
                },
                decision_sha256,
            },
            held_ready,
        })
    }

    #[tokio::test]
    async fn classic_opening_recovers_one_ordered_assignment_and_excludes_only_before_reservation()
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
        let witness = held(&runtime, "candidate", [10; 32]).await?;
        let profile = [11; 32];
        let catalog = &runtime.catalog;
        assert!(
            catalog
                .classic_opening(&witness, Transition::Observe)
                .await?
                .is_none()
        );
        let (reserved, _) = catalog
            .classic_opening(&witness, Transition::Reserve(profile))
            .await?
            .unwrap();
        assert!(matches!(reserved.phase, Phase::Opening { .. }));
        assert!(
            catalog
                .classic_opening(&witness, Transition::Exclude)
                .await
                .is_err()
        );
        let mut database = open_database(
            &runtime.backends,
            &runtime.kms,
            witness.database(),
            DatabaseOpen::Required,
        )
        .await?;
        assert!(matches!(
            database
                .tick(&BTreeSet::from([runtime.backends.node]))
                .await?,
            Role::Writer(_)
        ));
        let ready = &witness.held_ready.ready;
        let placement = workload::ImportedPlacement::new(
            workload::Point::new("public.workload_installation", "authority".into(), Timestamp(0))?,
            witness.placement(),
            witness.decision.decision_sha256,
            ready.native_basis,
            ready.classic_cut.system_time,
        );
        let ordered_fence = database
            .open_classic_placement(
                witness.held_ready.binding_sha256,
                &ready.installation,
                &placement,
            )
            .await?;
        assert_eq!(
            ordered_fence,
            database
                .open_classic_placement(
                    witness.held_ready.binding_sha256,
                    &ready.installation,
                    &placement
                )
                .await?
        );
        let terminal = catalog
            .classic_opening(
                &witness,
                Transition::Finish {
                    profile,
                    fence: ordered_fence,
                },
            )
            .await?;
        assert_eq!(
            terminal,
            catalog
                .classic_opening(&witness, Transition::Observe)
                .await?
        );
        assert!(
            runtime
                .ready_driver(witness.name(), witness.database())
                .await
                .is_err()
        );

        let excluded = held(&runtime, "excluded", [12; 32]).await?;
        let terminal = catalog
            .classic_opening(&excluded, Transition::Exclude)
            .await?
            .unwrap();
        assert!(matches!(terminal.0.phase, Phase::Excluded));
        assert_eq!(
            Some(terminal),
            catalog
                .classic_opening(&excluded, Transition::Reserve(profile))
                .await?
        );
        assert!(
            runtime
                .ready_driver(excluded.name(), excluded.database())
                .await
                .is_err()
        );
        drop(database);
        runtime.close().await;
        Ok(())
    }
}
