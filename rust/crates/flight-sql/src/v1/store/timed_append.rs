use std::{sync::Arc, time::Duration};

use arrow_flight::FlightData;
use bytes::Bytes;
use prost::Message as _;
use rtdb_index::branches::store::StoreWriterFence;
use rtdb_node::{
    CertifiedDecisionClock, Database, DatabaseAccess, SessionIdentity,
    serving::{CatalogRoot, TimedStoreAppend, TimedStoreAuthorizer, TimedStoreCommitment, TimedStoreReceipt},
};
use rtdb_types::{Anomaly, AnomalyCategory, TimedJournalCutoff, Timestamp};
use tokio::sync::oneshot;
use tonic::{Response, Status, Streaming, metadata::MetadataMap};

use super::{StoreAppendInput, TIMED_APPEND_CAPABILITY, append_reply, error, proto, writer};
use crate::{
    service::{Call, Output, Service},
    state::Caller,
    wire,
};

pub(crate) const INCARNATION_HEADER: &str = "x-rtdb-store-timed-incarnation-bin";
const MAX_BODY_BYTES: usize = (64 << 20) - 1024;
const BEGIN_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) fn requested_incarnation(metadata: &MetadataMap) -> Result<Option<[u8; 16]>, Status> {
    let values = metadata.get_all(TIMED_APPEND_CAPABILITY);
    let mut values = values.iter();
    let pins = metadata.get_all_bin(INCARNATION_HEADER);
    let mut pins = pins.iter();
    match (values.next(), pins.next()) {
        (None, None) => Ok(None),
        (Some(version), Some(pin)) if version == "1" && values.next().is_none() && pins.next().is_none() => {
            let bytes = pin.to_bytes().map_err(|_| error::incorrect("Invalid timed Store incarnation header"))?;
            let incarnation = super::incarnation(&bytes)?;
            if incarnation == [0; 16] {
                return Err(error::incorrect("Timed Store requires a nonzero incarnation"));
            }
            Ok(Some(incarnation))
        },
        _ => Err(error::incorrect("A timed Store capability offer requires one version and one incarnation")),
    }
}

struct Input {
    stream: Streaming<FlightData>,
    remaining: usize,
}

impl Input {
    async fn message(
        &mut self, call: &Call,
    ) -> Result<(proto::timed_append_input::Message, Arc<rtdb_columnar::MemoryReservation>), Status> {
        let frame = self.stream.message().await?
            .ok_or_else(|| error::incorrect("The timed Store exchange ended before its next message"))?;
        if frame.flight_descriptor.is_some() || !frame.data_header.is_empty() || !frame.app_metadata.is_empty()
            || frame.data_body.is_empty()
        {
            return Err(error::incorrect("A timed Store frame contains only a protobuf body"));
        }
        let bytes = frame.data_body;
        self.remaining = self.remaining.checked_sub(bytes.len())
            .ok_or_else(|| Status::resource_exhausted("The timed Store exchange exceeds its byte limit"))?;
        let memory = call.reserve(wire::add(wire::mul(bytes.len(), 32)?, 4096)?)?;
        Ok((decode_input(bytes)?, memory))
    }

    async fn end(&mut self) -> Result<(), Status> {
        if self.stream.message().await?.is_some() {
            return Err(error::incorrect("The timed Store exchange has an extra message after confirmation"));
        }
        Ok(())
    }
}

fn decode_input(bytes: Bytes) -> Result<proto::timed_append_input::Message, Status> {
    let message = proto::TimedAppendInput::decode(bytes.clone())
        .map_err(|_| error::incorrect("Invalid timed Store protobuf"))?;
    if message.encode_to_vec().as_slice() != bytes.as_ref() {
        return Err(error::incorrect("The timed Store protobuf is not canonical"));
    }
    message.message.ok_or_else(|| error::incorrect("Missing timed Store message"))
}

impl Service {
    fn timed_store_clock(&self, caller: &Caller) -> Result<Arc<CertifiedDecisionClock>, Status> {
        let (Some(clock), Some(policy)) = (&self.0.config.timed_store_clock, &self.0.config.hosted_controls)
        else { return Err(unavailable()) };
        if self.follower_only() || self.0.config.playground || !caller.verified_credential || caller.mac.is_none()
            || caller.principal.label_scope().is_some() || caller.principal.workload_grant().is_some()
            || caller.principal.user_id() != Some(policy.owner.as_str())
        {
            return Err(Status::permission_denied("Timed Store requires the installed Kernel writer"));
        }
        Ok(Arc::clone(clock))
    }

    async fn require_timed_store_attachment(
        &self,
        database: &Database,
        incarnation: [u8; 16],
        clock: &CertifiedDecisionClock,
    ) -> Result<(), Status> {
        let driver = database.serving_driver().ok_or_else(unavailable)?;
        if driver.incarnation() != incarnation {
            return Err(Status::failed_precondition("The timed Store incarnation changed"));
        }
        if database.read_only() || !clock.supports_timed_store(incarnation)
            || self.classic_flight_excluded(Some(database)).await?
        {
            return Err(unavailable());
        }
        if driver.classic_workload_fence().is_some() || !driver.supports_timed_store_append().await {
            return Err(unavailable());
        }
        Ok(())
    }

    pub(crate) async fn admit_timed_store_handshake(
        &self, caller: &Caller, database: &str, incarnation: [u8; 16],
    ) -> Result<Arc<Database>, Status> {
        let clock = self.timed_store_clock(caller)?;
        let catalog = self.0.node.catalog();
        if !catalog.is_managed() && !catalog.is_placed() {
            return Err(unavailable());
        }
        if database.contains('@') {
            return Err(error::incorrect("The timed Store handshake names its database and incarnation separately"));
        }
        caller.principal.authorize_database(database, DatabaseAccess::Write).map_err(error::anomaly)?;
        let database = catalog.database(database).map_err(error::anomaly)?;
        self.require_timed_store_attachment(&database, incarnation, &clock).await?;
        Ok(database)
    }

    async fn timed_store_target(
        &self, call: &Call,
    ) -> Result<(Arc<Database>, [u8; 16], Arc<CertifiedDecisionClock>), Status> {
        let expected = call.session.timed_store_incarnation
            .ok_or_else(|| Status::permission_denied("Timed Store requires its own writer handshake"))?;
        if call.session.fresh_replica || call.session.placed_database() != Some(call.database.as_str()) {
            return Err(Status::permission_denied("Timed Store requires the installed Kernel writer"));
        }
        let clock = self.timed_store_clock(&call.session.caller)?;
        let (database, incarnation, scope) = self.store_database(call, DatabaseAccess::Write, Some(&expected)).await?;
        if scope.is_some() {
            return Err(unavailable());
        }
        self.require_timed_store_attachment(&database, incarnation, &clock).await?;
        self.check_attachment(call, &database)?;
        Ok((database, incarnation, clock))
    }

    pub(crate) async fn timed_store_exchange(
        &self,
        call: Call,
        stream: Streaming<FlightData>,
    ) -> Result<Response<Output<FlightData>>, Status> {
        if call.minted {
            return Err(Status::permission_denied("Timed Store requires an authenticated Store session"));
        }
        let (database, incarnation, clock) = self.timed_store_target(&call).await.map_err(|status| call.error(status))?;
        let mut input = Input { stream, remaining: self.0.config.maximum_message_bytes.min(MAX_BODY_BYTES) };
        let first = call.cancellation.run_until_cancelled(tokio::time::timeout(BEGIN_TIMEOUT, input.message(&call))).await
            .ok_or_else(crate::error::cancelled)?
            .map_err(|_| Status::deadline_exceeded("Timed Store did not send its initial message"))??;
        let (proto::timed_append_input::Message::Begin(begin), memory) = first else {
            return Err(error::incorrect("The timed Store exchange must begin with its two append plans"));
        };
        let preview = begin.preview.ok_or_else(|| error::incorrect("Missing Kernel authorization preview"))?;
        let effect = begin.effect.ok_or_else(|| error::incorrect("Missing timed Store effect"))?;
        let fence = append_fence(&preview, incarnation)?;
        if append_fence(&effect, incarnation)? != fence || preview.key == effect.key {
            return Err(error::incorrect("The timed Store pair needs matching writer fences and different keys"));
        }
        let shape = companion_shape(preview.clone());
        let (preview, validation) = StoreAppendInput::parse(preview)?.transaction(incarnation, None, true, Arc::clone(&memory))?;
        let (effect, effect_validation) = StoreAppendInput::parse(effect)?.transaction(incarnation, None, true, memory)?;
        if validation || effect_validation {
            return Err(error::incorrect("The timed Store pair requires two appends with effects"));
        }
        let application_root = CatalogRoot(fixed_bytes(begin.application_root, "application catalog root")?);
        if application_root.0 == [0; 32] {
            return Err(error::incorrect("Missing pinned application catalog root"));
        }
        let decision_before = Timestamp(begin.decision_before_micros);
        let journal_cutoff = TimedJournalCutoff::new(
            Timestamp(begin.journal_before_micros),
            fixed_bytes(begin.receiver_certificate_sha256, "receiver certificate digest")?,
        ).map_err(error::anomaly)?;
        clock.admit_timed_store(incarnation, decision_before, journal_cutoff).await.map_err(error::anomaly)?;
        self.check_attachment(&call, &database)?;

        let call = Arc::new(call);
        let service = self.clone();
        let (send, mut prepared) = oneshot::channel();
        let authorize: TimedStoreAuthorizer = {
            let call = Arc::clone(&call);
            let database = Arc::clone(&database);
            let service = service.clone();
            Box::new(move |commitment| Box::pin(async move {
                call.cancellation.run_until_cancelled(async {
                    if commitment.original.stream.0.to_be_bytes() != incarnation
                        || commitment.original.cutoff != journal_cutoff
                    {
                        return Err(Anomaly::fault("rtdb/timed-store-authorization", "The prepared effect differs from its Store and clock binding"));
                    }
                    service.check_attachment(&call, &database).map_err(protocol_error)?;
                    send.send(commitment).map_err(|_| disconnected())?;
                    let (message, memory) = input.message(&call).await.map_err(protocol_error)?;
                    let proto::timed_append_input::Message::Confirm(confirmed) = message else {
                        return Err(protocol_error(error::incorrect("Expected the Kernel authorization confirmation")));
                    };
                    if append_fence(&confirmed, incarnation).map_err(protocol_error)? != fence
                        || companion_shape(confirmed.clone()) != shape
                    {
                        return Err(protocol_error(error::incorrect("The Kernel confirmation changed its prepared append shape")));
                    }
                    let (confirmed, _) = StoreAppendInput::parse(confirmed).map_err(protocol_error)?
                        .transaction(incarnation, None, true, memory).map_err(protocol_error)?;
                    input.end().await.map_err(protocol_error)?;
                    call.session.caller.principal.revalidate(service.0.node.now()).await?;
                    service.check_attachment(&call, &database).map_err(protocol_error)?;
                    Ok(confirmed)
                }).await.ok_or_else(disconnected)?
            }))
        };
        let headers = call.response(())?.metadata().clone();
        let drop_guard = call.cancellation.clone().drop_guard();
        let output: Output<FlightData> = Box::pin(async_stream::try_stream! {
            let _cancel_on_drop = drop_guard;
            let driver = database.serving_driver().ok_or_else(unavailable)?;
            let append = driver.append_timed_store(
                TimedStoreAppend { preview, effect, application_root, decision_before, journal_cutoff, authorize },
                &call.cancellation,
            );
            tokio::pin!(append);
            let (commitment, completed) = tokio::select! {
                biased;
                first = &mut prepared => (first.ok(), None),
                receipt = &mut append => (None, Some(receipt)),
            };
            if let Some(commitment) = commitment {
                yield frame(proto::timed_append_output::Message::Prepared(wire_commitment(commitment)));
            }
            let committed = match completed {
                Some(result) => result,
                None => append.await,
            }.map_err(error::timed_append)?;
            let commitment = commitment.ok_or_else(|| Status::internal("The timed Store committed without its preparation"))?;
            service.check_attachment(&call, &database)?;
            let receipt = committed_reply(incarnation, commitment, committed)?;
            service.publish();
            yield frame(proto::timed_append_output::Message::Committed(receipt));
        });
        let mut response = Response::new(output);
        *response.metadata_mut() = headers;
        Ok(response)
    }
}

fn append_fence(request: &proto::AppendRequest, incarnation: [u8; 16]) -> Result<Option<StoreWriterFence>, Status> {
    if request.incarnation.as_slice() != incarnation {
        return Err(Status::failed_precondition("The timed Store incarnation changed"));
    }
    request.writer_fence.as_ref()
        .map(|binding| writer::fence(binding.epoch, Some(binding.writer.clone())))
        .transpose()
}

fn companion_shape(mut request: proto::AppendRequest) -> proto::AppendRequest {
    for write in &mut request.writes {
        if let Some(value) = &mut write.value {
            value.clear();
        }
    }
    request
}

fn fixed_bytes<const N: usize>(bytes: Vec<u8>, field: &str) -> Result<[u8; N], Status> {
    bytes.try_into().map_err(|_| error::incorrect(&format!("A timed Store {field} has {N} bytes")))
}

fn wire_commitment(commitment: TimedStoreCommitment) -> proto::TimedAppendCommitment {
    let original = commitment.original;
    proto::TimedAppendCommitment {
        catalog: commitment.catalog.as_bytes().to_vec(),
        stream: original.stream.0.to_be_bytes().to_vec(),
        writer_epoch: original.epoch.0,
        lsn: original.at.0,
        payload_sha256: original.payload_sha256.to_vec(),
        journal_before_micros: original.cutoff.before().0,
        receiver_certificate_sha256: original.cutoff.certificate_sha256().to_vec(),
    }
}

fn committed_reply(
    incarnation: [u8; 16],
    commitment: TimedStoreCommitment,
    committed: TimedStoreReceipt,
) -> Result<proto::TimedAppendReceipt, Status> {
    if committed.receipt.basis.incarnation() != incarnation
        || committed.evidence.original != commitment.original
        || committed.evidence.evidence_sha256 == [0; 32]
        || committed.evidence.quorum_sync_at.0 <= 0
        || committed.evidence.quorum_sync_at >= commitment.original.cutoff.before()
    {
        return Err(Status::internal("The timed Store acknowledgement differs from its prepared effect"));
    }
    let reply = append_reply(incarnation, Ok(committed.receipt), false)?;
    if !matches!(reply.outcome, Some(proto::append_reply::Outcome::Committed(_))) {
        return Err(Status::internal("The timed Store did not commit its prepared effect"));
    }
    Ok(proto::TimedAppendReceipt {
        reply: Some(reply),
        original: Some(wire_commitment(commitment)),
        quorum_sync_at_micros: committed.evidence.quorum_sync_at.0,
        evidence_sha256: committed.evidence.evidence_sha256.to_vec(),
    })
}

fn frame(message: proto::timed_append_output::Message) -> FlightData {
    let body = proto::TimedAppendOutput { message: Some(message) }.encode_to_vec();
    FlightData { data_body: body.into(), ..Default::default() }
}

fn unavailable() -> Status {
    error::anomaly(Anomaly::new(
        AnomalyCategory::Unavailable, "rtdb/timed-store-unavailable",
        "Timed Store requires a commissioned Kernel writer and installed receiver clock",
    ))
}

fn disconnected() -> Anomaly {
    Anomaly::new(AnomalyCategory::Interrupted, "rtdb.store/timed-append-disconnected",
        "The Kernel disconnected from the timed Store exchange")
}

fn protocol_error(status: Status) -> Anomaly {
    Anomaly::incorrect("rtdb.store/timed-append-protocol", status.message().to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn companion() -> proto::AppendRequest {
        proto::AppendRequest {
            key: vec![1; 16],
            incarnation: vec![2; 16],
            since: 7,
            writes: vec![proto::Write {
                key: b"authorization".to_vec(),
                value: Some(vec![0; 32]),
                ..Default::default()
            }],
            writer_fence: Some(proto::WriterFence { epoch: 5, writer: vec![3; 16] }),
            ..Default::default()
        }
    }

    #[test]
    fn timed_append_capability_requires_one_known_version_and_exact_incarnation() {
        let mut metadata = MetadataMap::new();
        assert_eq!(requested_incarnation(&metadata).unwrap(), None);
        metadata.insert_bin(INCARNATION_HEADER, tonic::metadata::MetadataValue::from_bytes(&[4; 16]));
        assert!(requested_incarnation(&metadata).is_err());
        metadata.insert(TIMED_APPEND_CAPABILITY, "0".parse().unwrap());
        assert!(requested_incarnation(&metadata).is_err());
        metadata.insert(TIMED_APPEND_CAPABILITY, "1".parse().unwrap());
        assert_eq!(requested_incarnation(&metadata).unwrap(), Some([4; 16]));
        metadata.append_bin(INCARNATION_HEADER, tonic::metadata::MetadataValue::from_bytes(&[4; 16]));
        assert!(requested_incarnation(&metadata).is_err());
        metadata.remove_bin(INCARNATION_HEADER);
        assert!(requested_incarnation(&metadata).is_err());
        metadata.insert_bin(INCARNATION_HEADER, tonic::metadata::MetadataValue::from_bytes(&[0; 16]));
        assert!(requested_incarnation(&metadata).is_err());
        metadata.insert_bin(INCARNATION_HEADER, tonic::metadata::MetadataValue::from_bytes(&[4; 16]));
        metadata.append(TIMED_APPEND_CAPABILITY, "1".parse().unwrap());
        assert!(requested_incarnation(&metadata).is_err());
    }

    #[test]
    fn timed_append_confirmation_preserves_fence_and_append_shape() {
        let tentative = companion();
        let fence = append_fence(&tentative, [2; 16]).unwrap();
        assert!(fence.is_some());
        let original = companion_shape(tentative.clone());
        let mut confirmed = tentative;
        confirmed.writes[0].value = Some(vec![4; 32]);
        assert_eq!(companion_shape(confirmed.clone()), original);
        assert_eq!(append_fence(&confirmed, [2; 16]).unwrap(), fence);
        assert!(append_fence(&confirmed, [3; 16]).is_err());
        confirmed.since += 1;
        assert_ne!(companion_shape(confirmed.clone()), original);
        confirmed.writer_fence = None;
        assert_ne!(append_fence(&confirmed, [2; 16]).unwrap(), fence);
    }

    #[test]
    fn timed_append_without_transport_fence_still_requires_the_exact_incarnation() {
        let mut preview = companion();
        preview.writer_fence = None;
        assert_eq!(append_fence(&preview, [2; 16]).unwrap(), None);
        assert!(append_fence(&preview, [3; 16]).is_err());
        preview.incarnation.clear();
        assert!(append_fence(&preview, [2; 16]).is_err());
    }

    #[test]
    fn timed_append_refuses_unknown_or_duplicate_protobuf_fields() {
        let message = proto::TimedAppendInput {
            message: Some(proto::timed_append_input::Message::Confirm(companion())),
        };
        let canonical = message.encode_to_vec();
        assert!(decode_input(canonical.clone().into()).is_ok());
        let mut extra = canonical.clone();
        extra.extend([24, 1]);
        assert!(decode_input(extra.into()).is_err());
        let mut duplicate = canonical.clone();
        duplicate.extend(canonical);
        assert!(decode_input(duplicate.into()).is_err());
    }
}
