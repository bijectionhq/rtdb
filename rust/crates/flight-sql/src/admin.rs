use bytes::Bytes;
use prost::Message;
use rtdb_auth::Principal;
use rtdb_types::{Timestamp, rtdb_v1::Basis};
use tonic::Status;
use uuid::Uuid;

use crate::{
    rtdb::{admin::v1 as proto, v1 as data},
    service::{Call, Service},
    v1::error,
};

pub(crate) const EXPORT: &str = "rtdb.admin.v1.ExportCheckpoint";
pub(crate) const RESTORE: &str = "rtdb.admin.v1.RestoreDeployment";
pub(crate) const FORK: &str = "rtdb.admin.v1.ForkDeployment";
pub(crate) const CREATE: &str = "rtdb.admin.v1.CreateDeployment";
pub(crate) const DELETE: &str = "rtdb.admin.v1.DeleteDeployment";
pub(crate) const ACTIONS: [&str; 5] = [CREATE, DELETE, FORK, EXPORT, RESTORE];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeploymentAdminAction<'a> {
    Create {
        deployment: &'a str,
    },
    Delete {
        deployment: &'a str,
    },
    Export {
        deployment: &'a str,
    },
    Restore {
        source: &'a str,
        destination: &'a str,
    },
    Fork {
        source: &'a str,
        destination: &'a str,
    },
}

/// Deployment authorization supplied by the host that binds the Flight listener.
pub trait DeploymentAdminAuthority: std::fmt::Debug + Send + Sync {
    fn authorize(
        &self,
        principal: &Principal,
        action: DeploymentAdminAction<'_>,
    ) -> rtdb_types::Result<()>;
}

impl Service {
    pub(crate) fn supports_admin_action(&self, call: &Call, name: &str) -> bool {
        if !self.has_admin_route(call) {
            return false;
        }
        let catalog = self.0.node.catalog();
        match name {
            CREATE | DELETE => catalog.is_managed(),
            RESTORE | FORK if catalog.is_durable_default() => {
                self.0.config.default_checkpoint_restore_directory.is_some()
            }
            _ => true,
        }
    }

    pub(crate) fn has_admin_route(&self, call: &Call) -> bool {
        let principal = &call.session.caller.principal;
        self.0.config.deployment_admin.is_some()
            && call.session.caller.verified_credential
            && principal.user_id().is_some()
            && principal.label_scope().is_none()
            && call.session.audience_source().is_none()
            && !call.session.fresh_replica
    }

    pub(crate) async fn admin_action(
        &self,
        call: &Call,
        name: &str,
        body: Bytes,
    ) -> Result<Vec<u8>, Status> {
        let authority = self
            .0
            .config
            .deployment_admin
            .as_ref()
            .ok_or_else(|| error::unsupported("admin"))?;
        if !self.has_admin_route(call) {
            return Err(Status::permission_denied(
                "The session cannot administer deployments",
            ));
        }
        let principal = &call.session.caller.principal;
        let catalog = self.0.node.catalog();
        match name {
            CREATE => {
                let request: proto::CreateDeploymentRequest = decode(body)?;
                authority
                    .authorize(
                        principal,
                        DeploymentAdminAction::Create {
                            deployment: &request.deployment,
                        },
                    )
                    .map_err(error::anomaly)?;
                let expected = request.incarnation.as_deref().map(deployment_incarnation).transpose()?;
                let incarnation = self.0
                    .node
                    .create_managed_deployment(request.deployment.clone(), expected)
                    .await
                    .map_err(error::anomaly)?;
                Ok(proto::Deployment {
                    deployment: request.deployment,
                    basis: None,
                    incarnation: incarnation.as_bytes().to_vec(),
                }
                .encode_to_vec())
            }
            DELETE => {
                let request: proto::DeleteDeploymentRequest = decode(body)?;
                authority
                    .authorize(
                        principal,
                        DeploymentAdminAction::Delete {
                            deployment: &request.deployment,
                        },
                    )
                    .map_err(error::anomaly)?;
                let (proof, unreadable_by) = self
                    .0
                    .node
                    .erase_managed_deployment(&request.deployment)
                    .await
                    .map_err(error::anomaly)?;
                Ok(proto::ErasureProof {
                    deployment: request.deployment,
                    proof,
                    unreadable_by_micros: unreadable_by.0,
                }
                .encode_to_vec())
            }
            EXPORT => {
                let request: proto::ExportCheckpointRequest = decode(body)?;
                authority
                    .authorize(
                        principal,
                        DeploymentAdminAction::Export {
                            deployment: &request.deployment,
                        },
                    )
                    .map_err(error::anomaly)?;
                let basis = basis(request.basis)?;
                let pin = pin(&request.pin)?;
                let retain_until = request.retain_until_micros.map(Timestamp);
                let checkpoint = if catalog.is_managed() {
                    self.0
                        .node
                        .export_managed_checkpoint(
                            &request.deployment,
                            pin,
                            basis,
                            retain_until,
                            &call.cancellation,
                        )
                        .await
                } else if catalog.is_durable_default() {
                    self.0
                        .node
                        .export_default_checkpoint(
                            &request.deployment,
                            pin,
                            basis,
                            retain_until,
                            None,
                            &call.cancellation,
                        )
                        .await
                } else {
                    catalog
                        .export_deployment_checkpoint(
                            &request.deployment,
                            pin,
                            basis,
                            retain_until,
                            &call.cancellation,
                        )
                        .await
                }
                .map_err(error::anomaly)?;
                Ok(proto::CheckpointExport {
                    checkpoint,
                    basis: Some(basis_wire(basis)),
                }
                .encode_to_vec())
            }
            RESTORE => {
                let request: proto::RestoreDeploymentRequest = decode(body)?;
                authority
                    .authorize(
                        principal,
                        DeploymentAdminAction::Restore {
                            source: &request.from,
                            destination: &request.deployment,
                        },
                    )
                    .map_err(error::anomaly)?;
                let basis = if catalog.is_managed() {
                    self.0
                        .node
                        .transfer_managed_deployment(
                            &request.from,
                            &request.deployment,
                            rtdb_node::DeploymentTransfer::Restore {
                                ticket: request.checkpoint,
                            },
                            &call.cancellation,
                        )
                        .await
                } else if catalog.is_durable_default() {
                    let parent = self.default_restore_directory()?;
                    self.0
                        .node
                        .restore_default_checkpoint(
                            &request.from,
                            &request.deployment,
                            &request.checkpoint,
                            parent,
                            &call.cancellation,
                        )
                        .await
                } else {
                    catalog
                        .restore_deployment_checkpoint(
                            &request.from,
                            &request.deployment,
                            request.checkpoint,
                            &call.cancellation,
                        )
                        .await
                }
                .map_err(error::anomaly)?;
                Ok(proto::Deployment {
                    deployment: request.deployment,
                    basis: Some(basis_wire(basis)),
                    incarnation: Vec::new(),
                }
                .encode_to_vec())
            }
            FORK => {
                let request: proto::ForkDeploymentRequest = decode(body)?;
                authority
                    .authorize(
                        principal,
                        DeploymentAdminAction::Fork {
                            source: &request.from,
                            destination: &request.deployment,
                        },
                    )
                    .map_err(error::anomaly)?;
                let pin = pin(&request.pin)?;
                let basis = basis(request.basis)?;
                let retain_until = request.retain_until_micros.map(Timestamp);
                let basis = if catalog.is_managed() {
                    self.0
                        .node
                        .transfer_managed_deployment(
                            &request.from,
                            &request.deployment,
                            rtdb_node::DeploymentTransfer::Fork {
                                pin,
                                basis,
                                retain_until,
                            },
                            &call.cancellation,
                        )
                        .await
                } else if catalog.is_durable_default() {
                    let parent = self.default_restore_directory()?;
                    if request.from == request.deployment || request.deployment == "xtdb" {
                        return Err(error::incorrect(
                            "Fork needs a distinct Default destination",
                        ));
                    }
                    let checkpoint = self
                        .0
                        .node
                        .export_default_checkpoint(
                            &request.from,
                            pin,
                            basis,
                            retain_until,
                            None,
                            &call.cancellation,
                        )
                        .await
                        .map_err(error::anomaly)?;
                    self.0
                        .node
                        .restore_default_checkpoint(
                            &request.from,
                            &request.deployment,
                            &checkpoint,
                            parent,
                            &call.cancellation,
                        )
                        .await
                } else {
                    catalog
                        .fork_deployment_checkpoint(
                            &request.from,
                            &request.deployment,
                            pin,
                            basis,
                            retain_until,
                            &call.cancellation,
                        )
                        .await
                }
                .map_err(error::anomaly)?;
                Ok(proto::Deployment {
                    deployment: request.deployment,
                    basis: Some(basis_wire(basis)),
                    incarnation: Vec::new(),
                }
                .encode_to_vec())
            }
            _ => Err(error::unsupported("admin")),
        }
    }

    fn default_restore_directory(&self) -> Result<&std::path::Path, Status> {
        self.0
            .config
            .default_checkpoint_restore_directory
            .as_deref()
            .ok_or_else(|| error::unsupported("default-checkpoint-restore"))
    }
}

fn decode<M: Message + Default>(bytes: Bytes) -> Result<M, Status> {
    M::decode(bytes).map_err(|_| error::incorrect("Invalid deployment administration request"))
}

fn basis(basis: Option<data::Basis>) -> Result<Basis, Status> {
    let basis = basis.ok_or_else(|| error::incorrect("An exact source basis is required"))?;
    Basis::from_bytes(&basis.value).map_err(error::anomaly)
}

fn basis_wire(basis: Basis) -> data::Basis {
    data::Basis {
        value: basis.to_bytes().to_vec(),
    }
}

fn pin(bytes: &[u8]) -> Result<Uuid, Status> {
    Uuid::from_slice(bytes)
        .ok()
        .filter(|pin| !pin.is_nil())
        .ok_or_else(|| error::incorrect("A nonzero checkpoint pin ID is required"))
}

fn deployment_incarnation(bytes: &[u8]) -> Result<Uuid, Status> {
    Uuid::from_slice(bytes)
        .ok()
        .filter(|incarnation| !incarnation.is_nil())
        .ok_or_else(|| error::incorrect("A nonzero deployment incarnation UUID is required"))
}
