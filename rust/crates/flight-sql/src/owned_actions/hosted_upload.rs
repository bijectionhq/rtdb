use arrow_flight::FlightData;
use tonic::{Response, Streaming};

use super::{HostedDecisionInput, packed_groups::PackedGroups, *};
use crate::{error::cancelled, service::{Output, handoff_action_results}};
use rtdb_storage::sealing::PlaintextView;

pub(crate) const DECIDE_HOSTED_STREAM_V1: &str = "rtdb.owned.DecideHostedStream.v1";
pub(crate) const HOSTED_UPLOAD_HEADER: &str = "x-rtdb-owned-write";
const MAX_UPLOAD_BODY: usize = (64 << 20) - 1024;
const MAX_UPLOAD_FRAMES: usize = 16_384;
const UPLOAD_TIMEOUT: Duration = Duration::from_secs(30);

pub(super) struct HostedUpload<'a> {
    call: &'a Call,
    input: Streaming<FlightData>,
    bytes_left: usize,
    frames_left: usize,
}

impl<'a> HostedUpload<'a> {
    async fn receive(call: &'a Call, input: Streaming<FlightData>) -> Result<(Bytes, Bytes, PackedGroups<'static>), Status> {
        let mut upload = Self {
            call, input, bytes_left: MAX_UPLOAD_BODY - 4, frames_left: MAX_UPLOAD_FRAMES,
        };
        let prefix = upload.required().await?;
        Service::hosted_decision_parts(&prefix)?;
        let controls = upload.required().await?;
        let packed = PackedGroups::receive(call, &mut upload).await?;
        if !upload.next().await?.is_empty() || upload.input.message().await?.is_some() {
            return Err(invalid("Unexpected data at hosted upload end"));
        }
        Ok((prefix, controls, packed))
    }

    pub(super) async fn required(&mut self) -> Result<Bytes, Status> {
        let body = self.next().await?;
        if body.is_empty() { return Err(invalid("Hosted upload ended before its declared schedule")); }
        Ok(body)
    }

    async fn next(&mut self) -> Result<Bytes, Status> {
        self.frames_left = self.frames_left.checked_sub(1)
            .ok_or_else(|| invalid("Hosted upload exceeds its frame limit"))?;
        let data = self.input.message().await?.ok_or_else(|| invalid("Truncated hosted upload"))?;
        if data.flight_descriptor.is_some() || !data.data_header.is_empty() || !data.app_metadata.is_empty() {
            return Err(invalid("Invalid hosted upload frame"));
        }
        let body = data.data_body;
        self.bytes_left = self.bytes_left.checked_sub(body.len())
            .ok_or_else(|| invalid("Hosted upload exceeds its byte limit"))?;
        let charge = self.call.reserve(wire::add(body.len(), 256)?)?;
        Ok(wire::retain(body, charge))
    }
}

impl Service {
    pub(crate) fn supports_hosted_upload(&self, call: &Call) -> bool {
        !call.session.fresh_replica
            && self.hosted_control_policy(call).is_ok()
    }

    pub(crate) async fn hosted_upload(
        &self, call: Call, input: Streaming<FlightData>,
    ) -> Result<Response<Output<FlightData>>, Status> {
        let plaintext = PlaintextView::new(Arc::clone(call.memory()));
        let work = async {
            let (prefix, controls, packed) = tokio::time::timeout(UPLOAD_TIMEOUT, HostedUpload::receive(&call, input))
                .await.map_err(|_| Status::deadline_exceeded("Hosted upload did not finish"))??;
            let receipt = self.decide_hosted(&call, &prefix, HostedDecisionInput::Streamed {
                packed, controls: &controls,
            }).await?;
            Ok(FlightData { data_body: receipt.body, ..Default::default() })
        };
        let receipt = call.cancellation.run_until_cancelled(plaintext.reading(work)).await
            .ok_or_else(cancelled).and_then(|result| result).map_err(|error| call.error(error))?;
        self.publish();
        handoff_action_results(call, [Ok(receipt)], plaintext, false).await
    }
}
