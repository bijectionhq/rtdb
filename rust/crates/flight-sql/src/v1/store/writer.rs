use prost::Message;
use rtdb_index::branches::store::{StoreWriterClaim, StoreWriterFence};
use rtdb_auth::PlacementFence;
use rtdb_node::{DatabaseAccess, SessionIdentity};
use rtdb_tx::transact::{Outcome, Receipt, Transact};
use tonic::Status;

use super::{Call, Service, error, incarnation, proto};

pub(crate) const FENCE_WRITER: &str = "rtdb.store.v1.FenceWriter";
pub(crate) const CLAIM_WRITER: &str = "rtdb.store.v1.ClaimWriter";
pub(crate) const WRITER_FENCE_HEADER: &str = "x-rtdb-store-writer-fence";
pub(crate) const CLOSE_RECEIPT_HEADER: &str = "x-rtdb-store-close-receipt";

pub(super) fn fence(epoch: u64, writer: Option<Vec<u8>>) -> Result<StoreWriterFence, Status> {
    if epoch == 0 {
        return Err(error::incorrect("A store writer epoch must be positive"));
    }
    let writer = writer.map(|writer| writer.try_into()
        .map_err(|_| error::incorrect("A store writer identity has 16 bytes"))).transpose()?;
    Ok(StoreWriterFence { epoch, writer })
}

pub(super) fn ordered_claim(fence: StoreWriterFence, generation: u64, claim: &[u8]) -> Result<StoreWriterClaim, Status> {
    let claim: [u8; 16] = claim.try_into()
        .map_err(|_| error::incorrect("A store writer claim has 16 bytes"))?;
    if generation == 0 || claim == [0; 16] || fence.writer.is_none_or(|writer| writer == [0; 16]) {
        return Err(error::incorrect("A store writer claim requires its ordered generation and claim identity"));
    }
    Ok(StoreWriterClaim { fence, generation, claim })
}

impl Service {
    pub(super) async fn store_fence_writer(
        &self, call: &Call, request: proto::FenceWriterRequest,
    ) -> Result<Vec<u8>, Status> {
        if request.writer.is_some() {
            return Err(error::incorrect("Use ClaimWriter to obtain an ordered writer generation"));
        }
        let expected = incarnation(&request.incarnation)?;
        let fence = fence(request.epoch, None)?;
        let receipt = self.order_store_writer(call, expected, PlacementFence::Close { epoch: fence.epoch },
            Transact::set_store_writer(expected, fence)).await?;
        let Outcome::StoreWriterClosed { epoch, final_cut, previous } = receipt.outcome else {
            return Err(Status::internal("The fence did not return its ordered close receipt"));
        };
        if epoch != fence.epoch || final_cut.basis.incarnation() != expected
            || final_cut.basis.position() >= receipt.basis.position()
        {
            return Err(Status::internal("The ordered closure belongs to another request or source"));
        }
        let previous = previous.map(|claim| -> Result<proto::WriterFence, Status> {
            let writer = claim.fence.writer.ok_or_else(|| Status::internal("The previous writer claim has no assignment"))?;
            Ok(proto::WriterFence { epoch: claim.fence.epoch, writer: writer.to_vec(),
                generation: claim.generation, claim: claim.claim.to_vec() })
        }).transpose()?;
        Ok(proto::FenceWriterReply {
            incarnation: request.incarnation, epoch,
            fenced: Some(proto::Stamp { version: receipt.basis.position(), micros: receipt.system_time.0 }),
            final_cut: Some(proto::Stamp { version: final_cut.basis.position(), micros: final_cut.system_time.0 }),
            previous,
        }.encode_to_vec())
    }

    pub(super) async fn store_claim_writer(
        &self, call: &Call, request: proto::ClaimWriterRequest,
    ) -> Result<Vec<u8>, Status> {
        let expected = incarnation(&request.incarnation)?;
        let assignment: [u8; 16] = request.assignment.as_slice().try_into()
            .map_err(|_| error::incorrect("A store placement assignment has 16 bytes"))?;
        let attempt: [u8; 16] = request.attempt.as_slice().try_into()
            .map_err(|_| error::incorrect("A store writer claim attempt has 16 bytes"))?;
        let fence = StoreWriterFence { epoch: request.epoch, writer: Some(assignment) };
        let permission = PlacementFence::Claim { epoch: request.epoch, assignment };
        let mut command = Transact::claim_store_writer(expected, fence, attempt);
        if let Some(floor) = request.reading_floor_micros {
            let first = floor.checked_add(1).filter(|_| floor >= 0)
                .ok_or_else(|| error::incorrect("The Store reading floor must allow a later clock sample"))?;
            command.window.not_before = Some(rtdb_types::Timestamp(first));
        }
        let receipt = self.order_store_writer(call, expected, permission, command).await?;
        if receipt.outcome != Outcome::Then {
            return Err(Status::internal("The Store did not confirm the exact writer claim"));
        }
        Ok(proto::ClaimWriterReply { claim: Some(request), generation: receipt.basis.position() }.encode_to_vec())
    }

    async fn order_store_writer(
        &self, call: &Call, expected: [u8; 16], permission: PlacementFence, mut command: Transact,
    ) -> Result<Receipt, Status> {
        if self.follower_only() || call.session.fresh_replica {
            return Err(error::unsupported(FENCE_WRITER));
        }
        let caller = &call.session.caller;
        if !caller.verified_credential || caller.principal.label_scope().is_some() {
            return Err(Status::permission_denied("A store writer fence requires verified placement authority"));
        }
        let database = if let Some(grant) = caller.principal.workload_grant() {
            grant.admit_placement_fence(&call.database, expected, permission, self.0.node.now()).map_err(error::anomaly)?;
            let (_, attachment) = call.session.resolve_database(Some(&call.database), &self.0.node)?;
            attachment.ok_or_else(|| error::unsupported(FENCE_WRITER))?
        } else {
            self.store_database(call, DatabaseAccess::Write, Some(&expected)).await?.0
        };
        if database.read_only() {
            return Err(error::unsupported(FENCE_WRITER));
        }
        let driver = database.serving_driver().ok_or_else(|| error::unsupported(FENCE_WRITER))?;
        if driver.incarnation() != expected {
            return Err(Status::failed_precondition("The store incarnation changed"));
        }
        caller.principal.revalidate(self.0.node.now()).await.map_err(error::anomaly)?;
        self.check_attachment(call, &database)?;
        command.request_memory = Some(call.request_reservation());
        let receipt = driver.transact(command, &call.cancellation).await.map_err(error::anomaly)?;
        self.check_attachment(call, &database)?;
        caller.principal.revalidate(self.0.node.now()).await.map_err(error::anomaly)?;
        match &receipt.outcome {
            Outcome::Refused { error: refusal } => Err(error::anomaly(refusal.clone())),
            _ if receipt.basis.incarnation() == expected => Ok(receipt),
            _ => Err(Status::internal("The store writer fence did not confirm its ordered request")),
        }
    }
}
