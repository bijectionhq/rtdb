use std::sync::Arc;

use arrow_flight::Result as ActionResult;
use arrow_ipc::MessageHeader;
use bytes::{Buf, Bytes};
use rtdb_storage::sealing::{PlaintextDelivery, PlaintextView};
use tonic::{Response, Status, metadata::MetadataValue};

use crate::{
    error::{arrow, cancelled, invalid, status},
    owned_actions::{HeldRangeFormat, PointTime, WorldOrder},
    service::{Call, Output, Service},
};

pub(crate) const OWNED_IPC_HEADER: &str = "x-rtdb-owned-ipc";
pub(super) const READ_POINTS: &str = "rtdb.owned.ReadPoints.v1";
pub(super) const READ_RANGES: &str = "rtdb.owned.ReadRanges.v1";
const MAX_REQUEST_BYTES: usize = 2 << 20;
const MAX_POINTS: u16 = 256;
const MAX_RANGES: u16 = 32;
const IPC_EOS: [u8; 8] = [255, 255, 255, 255, 0, 0, 0, 0];

#[derive(Clone, Copy)]
pub(super) enum BatchKind {
    Point,
    Range,
}

impl BatchKind {
    pub(super) fn for_action(action: &str) -> Option<Self> {
        match action {
            READ_POINTS => Some(Self::Point),
            READ_RANGES => Some(Self::Range),
            _ => None,
        }
    }

    fn header(self) -> &'static str {
        match self {
            Self::Point => "x-rtdb-owned-points",
            Self::Range => "x-rtdb-owned-ranges",
        }
    }

    fn request(self, body: Bytes) -> Result<Bytes, Status> {
        let (count, mut remaining) = body.split_first_chunk::<2>()
            .ok_or_else(|| invalid("Missing held read batch count"))?;
        let max = match self {
            Self::Point => MAX_POINTS,
            Self::Range => MAX_RANGES,
        };
        if !(2..=max).contains(&u16::from_be_bytes(*count)) || body.len() > MAX_REQUEST_BYTES {
            return Err(invalid("Held read batch exceeds its request bound"));
        }
        let max_selector = match self {
            Self::Point => 7,
            Self::Range => 7,
        };
        for _ in 0..u16::from_be_bytes(*count) {
            let (selector, request, rest) = entry(remaining)?;
            if selector > max_selector || request.is_empty() {
                return Err(invalid("Invalid held read batch entry"));
            }
            remaining = rest;
        }
        if !remaining.is_empty() {
            return Err(invalid("Trailing held read batch bytes"));
        }
        Ok(body.slice(2..))
    }
}

fn entry(input: &[u8]) -> Result<(u8, &[u8], &[u8]), Status> {
    let (header, tail) = input.split_first_chunk::<5>()
        .ok_or_else(|| invalid("Truncated held read batch entry"))?;
    let size = u32::from_be_bytes(header[1..].try_into().expect("four length bytes")) as usize;
    let (request, rest) = tail.split_at_checked(size)
        .ok_or_else(|| invalid("Truncated held read batch request"))?;
    Ok((header[0], request, rest))
}

impl Service {
    pub(super) async fn held_read_batch(
        &self, call: Call, kind: BatchKind, body: Bytes,
    ) -> Result<Response<Output<ActionResult>>, Status> {
        let mut body = kind.request(body).map_err(|error| call.error(error))?;
        let first = self.read_batch_entry(&call, kind, &mut body).await;
        let (mut headers, cutoff) = match &first {
            Ok((_, grant)) => call.delivery_headers(grant, true)?,
            Err(_) => (call.response(())?.metadata().clone(), None),
        };
        headers.insert(kind.header(), MetadataValue::from_static("v1"));

        let service = self.clone();
        let stream: Output<ActionResult> = Box::pin(async_stream::try_stream! {
            let mut next = first;
            loop {
                let (reply, grant) = next.map_err(|error| call.error(error))?;
                if call.cancellation.is_cancelled() {
                    Err(call.error(cancelled()))?;
                }
                call.handoff_action(grant, cutoff).map_err(|error| call.error(error))?;
                service.publish();
                yield reply;
                if body.is_empty() {
                    break;
                }
                next = service.read_batch_entry(&call, kind, &mut body).await;
            }
        });
        let mut response = Response::new(stream);
        *response.metadata_mut() = headers;
        Ok(response)
    }

    async fn read_batch_entry(
        &self, call: &Call, kind: BatchKind, body: &mut Bytes,
    ) -> Result<(ActionResult, PlaintextDelivery), Status> {
        let (selector, request, rest) = entry(body)?;
        let consumed = body.len() - rest.len();
        let result = call.cancellation.run_until_cancelled(async {
            let plaintext = PlaintextView::new(Arc::clone(call.memory()));
            let reply = plaintext.reading(self.batch_read(call, kind, selector, request)).await?;
            let grant = plaintext.grant().await.map_err(status)?;
            Ok((reply, grant))
        }).await.unwrap_or_else(|| Err(cancelled()))?;
        body.advance(consumed);
        Ok(result)
    }

    async fn batch_read(
        &self, call: &Call, kind: BatchKind, selector: u8, request: &[u8],
    ) -> Result<ActionResult, Status> {
        match kind {
            BatchKind::Point => {
                if call.session.fresh_replica && matches!(selector, 4..=6) {
                    return Err(Status::permission_denied("The fresh Flight route only admits Root point reads"));
                }
                match selector {
                    0 => self.held_owned_point_action(call, request, PointTime::Optional).await,
                    1 => self.held_owned_point_action(call, request, PointTime::Required).await,
                    2 => self.held_owned_historical_point_action(call, request).await,
                    3 => self.held_owned_position_point_action(call, request).await,
                    4 => self.held_world_point_action(call, request).await,
                    5 => self.held_world_point_at_action(call, request, false).await,
                    6 => self.held_world_point_at_action(call, request, true).await,
                    7 => self.held_root_cut_point_action(call, request).await,
                    _ => unreachable!("validated point selector"),
                }
            }
            BatchKind::Range => {
                if selector <= 3 {
                    let version = match selector {
                        0 => HeldRangeFormat::Leading,
                        1 => HeldRangeFormat::Full,
                        2 => HeldRangeFormat::FixedValid,
                        _ => HeldRangeFormat::Historical,
                    };
                    return self.held_owned_range_action(call, request, version).await;
                }
                if call.session.fresh_replica {
                    return Err(Status::permission_denied("The fresh Flight route only admits Root range reads"));
                }
                let order = match selector {
                    4 => WorldOrder::ParentRange,
                    5 => WorldOrder::OverlayRange,
                    6 => WorldOrder::ParentRangeAt,
                    7 => WorldOrder::OverlayRangeAt,
                    _ => unreachable!("validated range selector"),
                };
                self.held_world_ordered_action(call, request, order).await
            }
        }
    }
}

pub(super) fn is_ipc_stream(body: &[u8]) -> bool {
    body.ends_with(&IPC_EOS)
        && ipc_message(body).is_ok_and(|(_, kind)| kind == MessageHeader::Schema)
}

fn ipc_message(body: &[u8]) -> Result<(usize, MessageHeader), Status> {
    let (prefix, tail) = body.split_first_chunk::<8>()
        .ok_or_else(|| invalid("Truncated owned IPC prefix"))?;
    if prefix[..4] != [255; 4] {
        return Err(invalid("Owned IPC requires its continuation prefix"));
    }
    let length = u32::from_le_bytes(prefix[4..].try_into().expect("four length bytes")) as usize;
    if length == 0 {
        return if tail.is_empty() {
            Ok((IPC_EOS.len(), MessageHeader::NONE))
        } else {
            Err(invalid("Trailing bytes after owned IPC terminator"))
        };
    }
    let (metadata, following) = tail.split_at_checked(length)
        .ok_or_else(|| invalid("Truncated owned IPC metadata"))?;
    let message = arrow_ipc::root_as_message(metadata).map_err(arrow)?;
    let body_size = usize::try_from(message.bodyLength()).map_err(arrow)?;
    if body_size > following.len() {
        return Err(invalid("Truncated owned IPC message body"));
    }
    Ok((8 + length + body_size, message.header_type()))
}

pub(super) struct IpcFrames(pub Bytes);

impl Iterator for IpcFrames {
    type Item = Result<ActionResult, Status>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.0.is_empty() {
            return None;
        }
        let (length, _) = match ipc_message(&self.0) {
            Ok(message) => message,
            Err(error) => {
                self.0 = Bytes::new();
                return Some(Err(error));
            }
        };
        Some(Ok(ActionResult { body: self.0.split_to(length) }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn held_read_batch_has_exact_framing_and_selectors() {
        let bytes = Bytes::from_static(&[0, 2, 0, 0, 0, 0, 2, 1, 2, 7, 0, 0, 0, 1, 3]);
        let body = BatchKind::Point.request(bytes.clone()).unwrap();
        let (selector, request, rest) = entry(&body).unwrap();
        assert_eq!((selector, request), (0, &[1, 2][..]));
        assert_eq!(entry(rest).unwrap(), (7, &[3][..], &[][..]));
        assert!(BatchKind::Range.request(bytes.clone()).is_ok());
        let invalid_selector = Bytes::from_static(&[0, 2, 0, 0, 0, 0, 1, 1, 8, 0, 0, 0, 1, 2]);
        assert!(BatchKind::Range.request(invalid_selector).is_err());
        assert!(BatchKind::Point.request(bytes.slice(..bytes.len() - 1)).is_err());
        let mut trailing = bytes.to_vec();
        trailing.push(0);
        assert!(BatchKind::Point.request(trailing.into()).is_err());
        assert!(BatchKind::Range.request(Bytes::from_static(&[0, 33])).is_err());
    }

    #[test]
    fn held_read_batch_ipc_frames_keep_arrow_stream_bytes_and_boundaries() {
        let input = rtdb_format::ipc::write_arrow_stream(
            &arrow_schema::Schema::empty(), &[], arrow_ipc::writer::IpcWriteOptions::default(),
        ).unwrap();
        assert!(is_ipc_stream(&input));
        let frames = IpcFrames(Bytes::copy_from_slice(&input)).collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[1].body.as_ref(), IPC_EOS);
        assert_eq!([frames[0].body.as_ref(), frames[1].body.as_ref()].concat(), input);
    }
}
