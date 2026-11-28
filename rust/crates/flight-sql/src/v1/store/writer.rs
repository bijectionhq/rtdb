use prost::Message;
use rtdb_index::branches::store::StoreWriterFence;
use rtdb_node::{DatabaseAccess, SessionIdentity};
use rtdb_tx::transact::{Outcome, Transact};
use tonic::Status;

use super::{Call, Service, error, incarnation, proto};

pub(super) const FENCE_WRITER: &str = "rtdb.store.v1.FenceWriter";
pub(crate) const WRITER_FENCE_HEADER: &str = "x-rtdb-store-writer-fence";

pub(super) fn fence(epoch: u64, writer: Option<Vec<u8>>) -> Result<StoreWriterFence, Status> {
    if epoch == 0 {
        return Err(error::incorrect("A store writer epoch must be positive"));
    }
    let writer = writer.map(|writer| writer.try_into()
        .map_err(|_| error::incorrect("A store writer identity has 16 bytes"))).transpose()?;
    Ok(StoreWriterFence { epoch, writer })
}

impl Service {
    pub(super) async fn store_fence_writer(
        &self, call: &Call, request: proto::FenceWriterRequest,
    ) -> Result<Vec<u8>, Status> {
        if self.follower_only() || call.session.fresh_replica {
            return Err(error::unsupported(FENCE_WRITER));
        }
        let caller = &call.session.caller;
        if !caller.verified_credential || caller.principal.label_scope().is_some() || caller.principal.workload_grant().is_some() {
            return Err(Status::permission_denied("A store writer fence requires verified installation-wide write access"));
        }
        let expected = incarnation(&request.incarnation)?;
        let fence = fence(request.epoch, request.writer)?;
        let (database, _, _) = self.store_database(call, DatabaseAccess::Write, Some(&expected)).await?;
        if database.read_only() {
            return Err(error::unsupported(FENCE_WRITER));
        }
        let driver = database.serving_driver().ok_or_else(|| error::unsupported(FENCE_WRITER))?;
        caller.principal.revalidate(self.0.node.now()).await.map_err(error::anomaly)?;
        self.check_attachment(call, &database)?;
        let mut command = Transact::set_store_writer(expected, fence);
        command.request_memory = Some(call.request_reservation());
        let receipt = driver.transact(command, &call.cancellation).await.map_err(error::anomaly)?;
        self.check_attachment(call, &database)?;
        caller.principal.revalidate(self.0.node.now()).await.map_err(error::anomaly)?;
        match receipt.outcome {
            Outcome::Then if receipt.basis.incarnation() == expected => Ok(proto::FenceWriterReply {
                incarnation: expected.to_vec(), epoch: fence.epoch, writer: fence.writer.map(|writer| writer.to_vec()),
            }.encode_to_vec()),
            Outcome::Refused { error: refusal } => Err(error::anomaly(refusal)),
            _ => Err(Status::internal("The store writer fence did not confirm its ordered request")),
        }
    }
}
