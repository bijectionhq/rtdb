use prost::Message;
use rtdb_node::{Database, DatabaseAccess, SessionIdentity};
use tonic::{Status, metadata::{MetadataMap, MetadataValue}};

use super::{error, proto};
use crate::state::Caller;

const PROBE: &str = "x-rtdb-store-access-probe-bin";
const DENIAL: &str = "x-rtdb-store-access-denial-bin";

pub(crate) fn probe(headers: &MetadataMap, handshake: bool) -> Result<Option<[u8; 16]>, Status> {
    let values = headers.get_all_bin(PROBE);
    let mut values = values.iter();
    let Some(value) = values.next() else { return Ok(None); };
    if !handshake || values.next().is_some() {
        return Err(error::incorrect("A Store access probe belongs on a new handshake and names one incarnation"));
    }
    let bytes = value.to_bytes().map_err(|_| error::incorrect("Invalid Store access probe"))?;
    Ok(Some(super::incarnation(&bytes)?))
}

pub(crate) fn store_denial(caller: &Caller, database: &Database, expected: [u8; 16]) -> Status {
    let Some(principal) = caller.principal.user_id().filter(|_| caller.verified_credential) else {
        return Status::unauthenticated("A Store access probe requires a verified principal");
    };
    let Some(driver) = database.serving_driver() else { return error::unsupported("store.access-probe"); };
    if driver.incarnation() != expected {
        return Status::failed_precondition("The store incarnation changed");
    }
    match caller.principal.authorize_database(database.name(), DatabaseAccess::Read) {
        Err(error) if error.code == "xtdb.authz/database-not-granted" => {
            let denial = proto::StoreAccessDenial {
                database: database.name().into(), incarnation: expected.to_vec(), principal: principal.into(),
            };
            let mut status = Status::permission_denied("The verified principal was denied access to the placed store");
            status.metadata_mut().insert_bin(DENIAL, MetadataValue::from_bytes(&denial.encode_to_vec()));
            status
        }
        Err(refusal) => error::anomaly(refusal),
        Ok(()) => Status::failed_precondition("The verified principal can read the placed store"),
    }
}
