use std::sync::Arc;

use prost::Message;
use rtdb_index::branches::store::{KeyRange, StoreWriterClaim};
use rtdb_node::{Database, DatabaseAccess, SessionIdentity};
use rtdb_types::{Anomaly, AnomalyCategory, rtdb_v1::Basis};
use tonic::Status;

use super::{Call, Service, error, incarnation, proto, ranges, writer};
use crate::store_release::StoreRelease;

pub(crate) const PURGE_FLOOR: &str = "rtdb.store.v1.PurgeFloor";
pub(crate) const PURGE_WRITER_HEADER: &str = "x-rtdb-store-purge-writer";

struct PurgePlan {
    since: Basis,
    keep_from: u64,
    reads: Vec<KeyRange>,
    writer: StoreWriterClaim,
}

impl TryFrom<proto::PurgeFloorRequest> for PurgePlan {
    type Error = Status;

    fn try_from(request: proto::PurgeFloorRequest) -> Result<Self, Self::Error> {
        let incarnation = incarnation(&request.incarnation)?;
        let since = request.since.ok_or_else(|| error::incorrect("A purge floor states its basis version"))?;
        let keep_from = request.keep_from.ok_or_else(|| error::incorrect("A purge floor states its minimum version"))?;
        if keep_from > since {
            return Err(error::purge_floor(Anomaly::incorrect(
                "rtdb.store/purge-floor-ahead", "A purge floor cannot exceed its basis",
            )));
        }
        if request.reads.is_empty() {
            return Err(error::incorrect("A purge floor names its writer and retention guards"));
        }
        let binding = request.writer_fence.ok_or_else(|| error::incorrect("A purge floor requires its ordered writer claim"))?;
        let fence = writer::fence(binding.epoch, Some(binding.writer))?;
        let writer = writer::ordered_claim(fence, binding.generation, &binding.claim)?;
        Ok(Self { since: Basis::new(incarnation, since), keep_from, reads: ranges(request.reads)?, writer })
    }
}

fn require_purge_credential(call: &Call) -> Result<(), Status> {
    let caller = &call.session.caller;
    if !caller.verified_credential {
        return Err(Status::permission_denied("Purging store history requires a verified credential"));
    }
    if caller.principal.label_scope().is_some() || caller.principal.workload_grant().is_some() {
        return Err(error::purge_floor(Anomaly::new(AnomalyCategory::Forbidden, "rtdb.store/outside-grant",
            "A store purge floor requires installation-wide write authority")));
    }
    Ok(())
}

impl Service {
    pub(crate) async fn supports_store_purge_floor(&self, call: &Call) -> bool {
        if self.follower_only() || call.session.fresh_replica
            || require_purge_credential(call).is_err()
            || StoreRelease::require(call.session.store_release).is_err()
            || call.session.caller.principal.authorize_database(&call.database, DatabaseAccess::Write).is_err()
        {
            return false;
        }
        let Ok(database) = self.0.node.catalog().database(&call.database) else { return false; };
        let Some(driver) = database.serving_driver() else { return false; };
        driver.supports_store_purge_floor().await && self.check_purge_attachment(call, &database).is_ok()
    }

    fn check_purge_attachment(&self, call: &Call, database: &Arc<Database>) -> Result<(), Status> {
        self.check_attachment(call, database)?;
        if call.session.placed_database().is_some() && !call.session.holds_attachment(database) {
            return Err(error::unavailable("The Flight session belongs to a different database attachment"));
        }
        if database.read_only() {
            return Err(error::unsupported(PURGE_FLOOR));
        }
        Ok(())
    }

    pub(super) async fn store_purge_floor(&self, call: &Call, request: proto::PurgeFloorRequest) -> Result<Vec<u8>, Status> {
        if self.follower_only() || call.session.fresh_replica {
            return Err(error::unsupported(PURGE_FLOOR));
        }
        require_purge_credential(call)?;
        let PurgePlan { since, keep_from, reads, writer } = request.try_into()?;
        let (database, incarnation, _) = self.store_database(call, DatabaseAccess::Write, Some(&since.incarnation())).await?;
        let driver = database.serving_driver().ok_or_else(|| error::unsupported(PURGE_FLOOR))?;
        call.session.caller.principal.revalidate(self.0.node.now()).await.map_err(error::anomaly)?;
        self.check_purge_attachment(call, &database)?;
        driver.publish_store_purge_floor(since, keep_from, reads, writer, call.request_reservation(), &call.cancellation)
            .await.map_err(error::purge_floor)?;
        self.check_purge_attachment(call, &database)?;
        Ok(proto::PurgeFloorReply { incarnation: incarnation.to_vec(), keep_from: Some(keep_from) }.encode_to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan() -> proto::PurgeFloorRequest {
        proto::PurgeFloorRequest {
            incarnation: vec![9; 16], since: Some(0), keep_from: Some(0),
            reads: vec![
                proto::KeyRange { start: vec![9], end: None },
                proto::KeyRange { start: vec![1], end: Some(vec![2]) },
            ],
            writer_fence: Some(proto::WriterFence { epoch: 4, writer: vec![2; 16], generation: 11, claim: vec![3; 16] }),
        }
    }

    #[test]
    fn checked_purge_plan_preserves_incarnation_zero_and_exact_guards() {
        let checked = PurgePlan::try_from(plan()).unwrap();
        assert_eq!(checked.since, Basis::new([9; 16], 0));
        assert_eq!(checked.keep_from, 0);
        assert_eq!(checked.writer.fence.epoch, 4);
        assert_eq!(checked.writer.fence.writer, Some([2; 16]));
        assert_eq!(checked.writer.generation, 11);
        assert_eq!(checked.writer.claim, [3; 16]);
        assert_eq!(checked.reads, [
            KeyRange { start: vec![9], end: None },
            KeyRange { start: vec![1], end: Some(vec![2]) },
        ]);
    }

    #[test]
    fn purge_plan_requires_incarnation_versions_and_nonempty_guard_ranges() {
        for request in [
            proto::PurgeFloorRequest { incarnation: vec![], ..plan() },
            proto::PurgeFloorRequest { incarnation: vec![1; 15], ..plan() },
            proto::PurgeFloorRequest { since: None, ..plan() },
            proto::PurgeFloorRequest { keep_from: None, ..plan() },
            proto::PurgeFloorRequest { keep_from: Some(1), ..plan() },
            proto::PurgeFloorRequest { reads: vec![], ..plan() },
            proto::PurgeFloorRequest { writer_fence: None, ..plan() },
            proto::PurgeFloorRequest {
                writer_fence: Some(proto::WriterFence { generation: 0, ..plan().writer_fence.unwrap() }),
                ..plan()
            },
            proto::PurgeFloorRequest {
                writer_fence: Some(proto::WriterFence { claim: vec![1; 15], ..plan().writer_fence.unwrap() }),
                ..plan()
            },
            proto::PurgeFloorRequest {
                reads: vec![proto::KeyRange { start: vec![1], end: Some(vec![1]) }],
                ..plan()
            },
        ] {
            assert_eq!(PurgePlan::try_from(request).err().unwrap().code(), tonic::Code::InvalidArgument);
        }
    }
}
