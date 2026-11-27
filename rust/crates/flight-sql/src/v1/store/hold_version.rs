use std::sync::Arc;

use prost::Message;
use rtdb_node::{Database, DatabaseAccess, SessionIdentity, serving::{
    StoreVersionHoldKey, StoreVersionHoldReply, StoreVersionHoldRequest,
}};
use rtdb_types::{Timestamp, rtdb_v1::Basis};
use sha2::{Digest, Sha256};
use tonic::Status;

use super::{Call, Service, error, incarnation, proto::{self, hold_version_reply::Outcome, hold_version_request::Operation}};

pub(super) const ACTION: &str = "rtdb.store.v1.HoldVersion";
pub(super) const AVAILABLE: bool = false;

fn exact(incarnation: [u8; 16], at: Option<u64>) -> Result<Basis, Status> {
    Ok(Basis::new(incarnation, at.ok_or_else(|| error::incorrect("A Store hold states its exact Version"))?))
}

fn hold_key(incarnation: [u8; 16], key: proto::HeldVersionKey) -> Result<StoreVersionHoldKey, Status> {
    let id = key.hold_id.try_into().map_err(|_| error::incorrect("A Store hold identity has 16 bytes"))?;
    Ok(StoreVersionHoldKey { basis: exact(incarnation, key.at)?, id })
}

fn encode_key(key: StoreVersionHoldKey) -> proto::HeldVersionKey {
    proto::HeldVersionKey { hold_id: key.id.to_vec(), at: Some(key.basis.position()) }
}

fn decode_request(request: proto::HoldVersionRequest) -> Result<([u8; 16], StoreVersionHoldRequest), Status> {
    let incarnation = incarnation(&request.incarnation)?;
    let operation = match request.operation.ok_or_else(|| error::incorrect("A Store hold names an operation"))? {
        Operation::Retain(retain) => StoreVersionHoldRequest::Retain {
            basis: exact(incarnation, retain.at)?,
            expires_at: Timestamp(retain.expires_at_micros.ok_or_else(|| error::incorrect("A Store hold states its deadline"))?),
        },
        Operation::Reopen(key) => StoreVersionHoldRequest::Reopen(hold_key(incarnation, key)?),
        Operation::Release(key) => StoreVersionHoldRequest::Release(hold_key(incarnation, key)?),
    };
    Ok((incarnation, operation))
}

impl Service {
    pub(super) async fn store_hold_version(&self, call: &Call, request: proto::HoldVersionRequest) -> Result<Vec<u8>, Status> {
        if self.follower_only() || call.session.fresh_replica {
            return Err(error::unsupported(ACTION));
        }
        let caller = &call.session.caller;
        let principal = &caller.principal;
        let credential = caller.mac.filter(|_| caller.verified_credential
            && principal.label_scope().is_none() && principal.workload_grant().is_none())
            .ok_or_else(|| Status::permission_denied("Store Version holds require verified database reader authority"))?;
        let (stated, operation) = decode_request(request)?;
        let (database, incarnation, _) = self.store_database(call, DatabaseAccess::Read, Some(&stated)).await?;
        let driver = database.serving_driver().ok_or_else(|| error::unsupported(ACTION))?;
        let mut reader = Sha256::new();
        reader.update(b"rtdb.store.v1.HoldVersion");
        reader.update(credential);
        reader.update(database.attachment().to_be_bytes());
        let reader = reader.finalize().into();
        principal.revalidate(self.0.node.now()).await.map_err(error::anomaly)?;
        self.check_hold_attachment(call, &database)?;
        let reply = driver.store_version_hold(reader, operation, &call.cancellation).await.map_err(error::anomaly)?;
        principal.revalidate(self.0.node.now()).await.map_err(error::anomaly)?;
        self.check_hold_attachment(call, &database)?;
        let outcome = match reply {
            StoreVersionHoldReply::Held(hold) => {
                if self.0.node.now() >= hold.expires_at {
                    return Err(error::not_found("rtdb.store/hold-unavailable", "The Store Version hold expired before delivery"));
                }
                Outcome::Held(proto::HeldVersion { key: Some(encode_key(hold.key)), expires_at_micros: Some(hold.expires_at.0) })
            }
            StoreVersionHoldReply::Released(key) => Outcome::Released(encode_key(key)),
        };
        Ok(proto::HoldVersionReply { incarnation: incarnation.to_vec(), outcome: Some(outcome) }.encode_to_vec())
    }

    fn check_hold_attachment(&self, call: &Call, database: &Arc<Database>) -> Result<(), Status> {
        if database.read_only() {
            return Err(error::unsupported(ACTION));
        }
        if call.session.closed() || call.session.placed_database().is_some() && !call.session.holds_attachment(database) {
            return Err(error::unavailable("The Flight session cannot use this database attachment"));
        }
        self.check_attachment(call, database)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retain_requires_an_explicit_version_including_zero() {
        let request = |at| proto::HoldVersionRequest {
            incarnation: vec![1; 16],
            operation: Some(Operation::Retain(proto::RetainVersion { at, expires_at_micros: Some(100) })),
        };
        assert!(decode_request(request(None)).is_err());
        let (incarnation, operation) = decode_request(request(Some(0))).unwrap();
        assert_eq!(incarnation, [1; 16]);
        assert!(matches!(operation, StoreVersionHoldRequest::Retain { basis, expires_at: Timestamp(100) }
            if basis == Basis::new(incarnation, 0)));
    }
}
