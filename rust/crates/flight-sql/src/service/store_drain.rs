use super::{CLOSE_SESSION, Credentials, Output, Service, auth, cancelled, flight, status, wire};
use crate::protocol::CloseResult;
use prost::Message;
use tonic::{Request, Response, Status};
use uuid::Uuid;

pub const REQUEST_STORE_DRAIN_HEADER: &str = "x-rtdb-request-store-drain";
pub const STORE_DRAIN_PROOF_HEADER: &str = "x-rtdb-store-drain-proof";

fn refused() -> Status {
    Status::permission_denied("The Store drain proof must name its original Flight receiver and session")
}

pub(super) struct VerifiedStoreDrain(Uuid);

impl Service {
    pub(super) fn store_drain_proof(
        &self, request: &Request<flight::Action>,
    ) -> Result<Option<VerifiedStoreDrain>, Status> {
        let proofs = request.metadata().get_all(STORE_DRAIN_PROOF_HEADER);
        let mut proofs = proofs.iter();
        let Some(proof) = proofs.next() else { return Ok(None); };
        if proofs.next().is_some() || request.get_ref().r#type != CLOSE_SESSION || !request.get_ref().body.is_empty() {
            return Err(refused());
        }
        let credentials = Credentials::parse(request)?;
        let bearer = credentials.bearer().ok_or_else(refused)?;
        let session = Uuid::parse_str(bearer).map_err(|_| refused())?;
        let session_key = session.to_string();
        if session_key != bearer || auth::cookie(request).is_some_and(|cookie| cookie != bearer) {
            return Err(refused());
        }
        let databases = request.metadata().get_all("x-xtdb-database");
        let mut databases = databases.iter();
        let database = databases.next().and_then(|value| value.to_str().ok())
            .filter(|database| !database.is_empty() && databases.next().is_none()).ok_or_else(refused)?;
        if !self.0.auth.verify_store_drain(session, database, proof.as_encoded_bytes()) {
            return Err(refused());
        }
        Ok(Some(VerifiedStoreDrain(session)))
    }

    pub(super) fn confirm_forgotten_store_drain(
        &self, proof: &VerifiedStoreDrain,
    ) -> Result<Option<Response<Output<flight::Result>>>, Status> {
        // A signed session starts registered. Sweep forgets an enrolled
        // session only after fencing it and observing no outstanding reads.
        // Shutdown discards the registry and cancels the service first.
        if self.0.sessions.contains(&proof.0.to_string()) {
            return Ok(None);
        }
        if self.0.cancellation.is_cancelled() {
            return Err(cancelled());
        }
        let permit = self.0.permits.clone().try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("Flight concurrent-call limit reached"))?;
        let store_watch_drain = self.0.auth.store_watch_signer(proof.0)?.drained();
        let result = CloseResult { status: 1, store_reads_drained: true, store_watch_drain };
        let charge = self.memory().reserve(wire::add(result.encoded_len(), 256)?).map_err(status)?;
        let body = wire::retain(result.encode_to_vec(), charge);
        let stream: Output<_> = Box::pin(futures::stream::once(async move {
            let _permit = permit;
            Ok(flight::Result { body })
        }));
        Ok(Some(Response::new(stream)))
    }
}
