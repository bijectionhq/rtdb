use prost::Message;
use rtdb_node::{DatabaseAccess, SessionIdentity};
use tonic::Status;

use super::{Call, Service, error, proto, within};

pub(in crate::v1) const STORAGE_SIZES: &str = "rtdb.store.v1.StorageSizes";

impl Service {
    pub(super) async fn store_storage_sizes(&self, call: &Call, request: proto::StorageSizesRequest) -> Result<Vec<u8>, Status> {
        let (database, incarnation, scope) = self.store_database(call, DatabaseAccess::Read, Some(&request.incarnation)).await?;
        // A partial rollup would be read as the installation's complete balance.
        within(scope, |_| false)?;
        let driver = database.serving_driver().ok_or_else(|| error::unsupported(STORAGE_SIZES))?;
        let sizes = driver.store_sizes(request.reconcile_epoch, self.0.node.now(), &call.cancellation).await.map_err(error::anomaly)?;
        if sizes.at.incarnation() != incarnation {
            return Err(Status::failed_precondition("The store incarnation changed"));
        }
        call.session.caller.principal.revalidate(self.0.node.now()).await.map_err(error::anomaly)?;
        self.check_attachment(call, &database)?;
        let buckets = sizes.buckets().map(|(bucket, balance)| proto::SizeBucket {
            family: bucket.family as u32,
            tablet: bucket.tablet.to_vec(),
            key_bytes: balance.key_bytes,
            value_bytes: balance.value_bytes,
            presences: balance.presences,
        }).collect();
        Ok(proto::StorageSizesReply {
            incarnation: incarnation.to_vec(),
            snapshot: Some(proto::SizeSnapshot { at: sizes.at.position(), buckets }),
        }.encode_to_vec())
    }
}
