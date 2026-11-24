use std::sync::Arc;

use bytes::Bytes;
use prost::Message;
use rtdb_auth::VerifiedWorkloadGrant;
use rtdb_node::Database;
use rtdb_tx::transact::workload::{
    self, Applied, Authority, Evidence, Family, Fence, Holder, Operation, Placement, Predecessor,
    Previous,
};
use sha2::{Digest, Sha256};
use tonic::Status;

use super::{Call, Service, check_key, decode, error, proto, transact};

impl Service {
    pub(super) async fn managed_workload_action(
        &self,
        call: &Call,
        body: Bytes,
    ) -> Result<Vec<u8>, Status> {
        let (database, grant) = self.managed_workload_attachment(call)?;
        let driver = database.serving_driver().ok_or_else(denied)?;
        let (authority, point, expires_at) = grant
            .assignment(&call.database, driver.incarnation(), self.0.node.now())
            .map_err(error::anomaly)?;
        let digest = attempt_digest(&body);
        let request: proto::ManagedWorkloadRequest = decode(body)?;
        let (operation, namespace) =
            request_operation(call, request, authority, grant.receipt_namespace(), digest)?;
        let reply = driver
            .managed_workload(
                workload::Request {
                    point: point.clone(),
                    namespace,
                    expires_at,
                    operation,
                },
                &call.cancellation,
            )
            .await
            .map_err(error::anomaly)?;
        self.check_attachment(call, &database)?;
        grant
            .assignment(&call.database, driver.incarnation(), self.0.node.now())
            .map_err(error::anomaly)?;
        Ok(response(reply, namespace).encode_to_vec())
    }

    pub(super) async fn managed_workload_status(
        &self,
        call: &Call,
        key: Vec<u8>,
    ) -> Result<Vec<u8>, Status> {
        let (database, grant) = self.managed_workload_attachment(call)?;
        let driver = database.serving_driver().ok_or_else(denied)?;
        let (authority, point, expires_at) = grant
            .assignment(&call.database, driver.incarnation(), self.0.node.now())
            .map_err(error::anomaly)?;
        let Authority::ReceiptRecovery {
            original_namespace,
            family,
        } = authority
        else {
            return Err(denied());
        };
        let reply = driver
            .managed_workload(
                workload::Request {
                    point: point.clone(),
                    namespace: *original_namespace,
                    expires_at,
                    operation: Operation::Status {
                        key,
                        family: *family,
                    },
                },
                &call.cancellation,
            )
            .await
            .map_err(error::anomaly)?;
        self.check_attachment(call, &database)?;
        grant
            .assignment(&call.database, driver.incarnation(), self.0.node.now())
            .map_err(error::anomaly)?;
        let workload::Reply::Receipt(receipt) = reply else {
            return Err(denied());
        };
        Ok(transact::receipt(receipt).encode_to_vec())
    }

    fn managed_workload_attachment<'a>(
        &self,
        call: &'a Call,
    ) -> Result<(Arc<Database>, &'a VerifiedWorkloadGrant), Status> {
        let grant = call
            .session
            .caller
            .principal
            .workload_grant()
            .ok_or_else(denied)?;
        if !call.session.caller.verified_credential
            || !self.0.node.catalog().is_managed()
            || self.follower_only()
            || call.session.audience_source().is_some()
            || call.session.fresh_replica
        {
            return Err(denied());
        }
        let database = self
            .0
            .node
            .catalog()
            .database(&call.database)
            .map_err(error::anomaly)?;
        self.check_attachment(call, &database)?;
        Ok((database, grant))
    }
}

fn request_operation(
    call: &Call,
    request: proto::ManagedWorkloadRequest,
    granted: &Authority,
    namespace: [u8; 32],
    digest: [u8; 32],
) -> Result<(Operation, [u8; 32]), Status> {
    use proto::managed_workload_request::Operation as Wire;
    let key = request.key;
    let (authority, transaction) = match required(request.operation)? {
        Wire::AdvancePlacement(value) => {
            use proto::workload_advance_placement::Previous as WirePrevious;
            let previous = match required(value.previous)? {
                WirePrevious::Empty(_) => Previous::Empty,
                WirePrevious::Vacant(value) => Previous::Vacant {
                    fence: fence(value)?,
                },
                WirePrevious::Closed(value) => Previous::Closed {
                    predecessor: predecessor(value)?,
                },
            };
            (
                Authority::AdvancePlacement {
                    installation_intent: fixed(value.installation_intent)?,
                    previous,
                    next: placement(required(value.next)?)?,
                },
                None,
            )
        }
        Wire::ClaimWriter(value) => (
            Authority::ClaimWriter {
                fence: fence(required(value.fence)?)?,
            },
            None,
        ),
        Wire::Write(value) => (
            Authority::Write {
                fence: fence(required(value.fence)?)?,
                writer: applied(required(value.writer)?)?,
            },
            Some(required(value.transaction)?),
        ),
        Wire::CloseWriter(value) => (
            Authority::CloseWriter {
                fence: fence(required(value.fence)?)?,
                writer: applied(required(value.writer)?)?,
                closure: fixed(value.closure)?,
            },
            None,
        ),
        Wire::ObservePlacement(value) => (
            Authority::ObservePlacement {
                fence: fence(required(value.fence)?)?,
            },
            None,
        ),
        Wire::PredecessorEvidence(value) => (
            Authority::PredecessorEvidence {
                predecessor: predecessor(value)?,
            },
            None,
        ),
        Wire::SettleAttempt(value) => (
            Authority::AttemptSettlement {
                original_namespace: fixed(value.original_namespace)?,
                family: family(value.family)?,
                key: key.clone(),
                request_digest: fixed(value.request_digest)?,
            },
            None,
        ),
    };
    if &authority != granted {
        return Err(denied());
    }
    match authority {
        Authority::ObservePlacement { fence } if key.is_empty() => {
            Ok((Operation::Observe(fence), namespace))
        }
        Authority::PredecessorEvidence { predecessor } if key.is_empty() => {
            Ok((Operation::Predecessor(predecessor), namespace))
        }
        Authority::AttemptSettlement {
            original_namespace,
            family,
            request_digest,
            ..
        } => {
            check_key(&key)?;
            Ok((
                Operation::Settle {
                    key,
                    family,
                    digest: request_digest,
                },
                original_namespace,
            ))
        }
        authority if authority.family().is_some() => {
            check_key(&key)?;
            let transaction = transaction
                .map(|request| {
                    if request.key != key {
                        return Err(denied());
                    }
                    let mut tx = transact::request(request, call.memory())?;
                    tx.request_memory = Some(call.request_reservation());
                    Ok(tx)
                })
                .transpose()?;
            Ok((
                Operation::Submit {
                    key,
                    authority,
                    transaction,
                    digest,
                },
                namespace,
            ))
        }
        _ => Err(denied()),
    }
}

fn required<T>(value: Option<T>) -> Result<T, Status> {
    value.ok_or_else(|| error::incorrect("Managed workload evidence is incomplete"))
}

fn fixed<const N: usize>(bytes: Vec<u8>) -> Result<[u8; N], Status> {
    bytes
        .try_into()
        .map_err(|_| error::incorrect("Invalid managed workload identifier length"))
}

fn placement(value: proto::WorkloadPlacement) -> Result<Placement, Status> {
    let assignment = value
        .assignment
        .map(|value| -> Result<Holder, Status> {
            Ok(Holder {
                assignment_id: fixed(value.assignment_id)?,
                holder: value.holder,
            })
        })
        .transpose()?;
    Ok(Placement {
        epoch: value.epoch,
        assignment,
    })
}

fn applied(value: proto::WorkloadApplied) -> Result<Applied, Status> {
    check_key(&value.key)?;
    Ok(Applied {
        namespace: fixed(value.namespace)?,
        key: value.key,
        basis: fixed(required(value.basis)?.value)?,
        system_time_micros: value.system_time_micros,
    })
}

fn fence(value: proto::WorkloadFence) -> Result<Fence, Status> {
    Ok(Fence {
        placement: placement(required(value.placement)?)?,
        applied: applied(required(value.applied)?)?,
    })
}

fn predecessor(value: proto::WorkloadPredecessor) -> Result<Predecessor, Status> {
    Ok(Predecessor {
        fence: fence(required(value.fence)?)?,
        writer: applied(required(value.writer)?)?,
        close: applied(required(value.close)?)?,
        closure: fixed(value.closure)?,
    })
}

fn family(value: i32) -> Result<Family, Status> {
    match proto::WorkloadFamily::try_from(value) {
        Ok(proto::WorkloadFamily::AdvancePlacement) => Ok(Family::AdvancePlacement),
        Ok(proto::WorkloadFamily::ClaimWriter) => Ok(Family::ClaimWriter),
        Ok(proto::WorkloadFamily::Write) => Ok(Family::Write),
        Ok(proto::WorkloadFamily::CloseWriter) => Ok(Family::CloseWriter),
        _ => Err(error::incorrect(
            "Invalid managed workload operation family",
        )),
    }
}

fn evidence(value: Evidence) -> proto::WorkloadEvidence {
    let Applied {
        namespace,
        key,
        basis,
        system_time_micros,
    } = value.applied;
    proto::WorkloadEvidence {
        applied: Some(proto::WorkloadApplied {
            namespace: namespace.to_vec(),
            key,
            basis: Some(proto::Basis {
                value: basis.to_vec(),
            }),
            system_time_micros,
        }),
        placement: Some(proto::WorkloadPlacement {
            epoch: value.placement.epoch,
            assignment: value
                .placement
                .assignment
                .map(|assignment| proto::WorkloadHolder {
                    assignment_id: assignment.assignment_id.to_vec(),
                    holder: assignment.holder,
                }),
        }),
        writer: value.writer,
        predecessor_retirement_basis: value.predecessor_retirement_basis.map(|basis| {
            proto::Basis {
                value: basis.to_bytes().to_vec(),
            }
        }),
    }
}

fn response(reply: workload::Reply, namespace: [u8; 32]) -> proto::ManagedWorkloadResponse {
    use proto::managed_workload_response::Result as Wire;
    let (result, receipt_namespace) = match reply {
        workload::Reply::Receipt(receipt) => (
            Wire::Receipt(transact::receipt(receipt)),
            namespace.to_vec(),
        ),
        workload::Reply::Placement(placement) => (Wire::Placement(evidence(placement)), Vec::new()),
        workload::Reply::Predecessor(predecessor) => {
            let workload::PredecessorEvidence {
                fence,
                writer,
                close,
            } = *predecessor;
            (
                Wire::Predecessor(proto::WorkloadPredecessorEvidence {
                    fence: Some(evidence(fence)),
                    writer: Some(evidence(writer)),
                    close: Some(evidence(close)),
                }),
                Vec::new(),
            )
        }
    };
    proto::ManagedWorkloadResponse {
        result: Some(result),
        receipt_namespace,
    }
}

fn attempt_digest(body: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"rtdb.v1/managed-workload-attempt\0");
    hash.update(body);
    hash.finalize().into()
}

fn denied() -> Status {
    error::anomaly(workload::denied())
}
