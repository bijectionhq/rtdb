//! Preserve Prost's encoding order while retaining its Bytes fields as spans.

use bytes::{Buf, BufMut, Bytes, BytesMut, buf::UninitSlice};
use prost::Message;
use std::io::{self, IoSlice, Write};
use tokio::io::{AsyncWrite, AsyncWriteExt};

const SPANS_PER_WRITE: usize = 32;
pub(crate) const WIRE_BATCH_FRAMES: usize = 32;
const WIRE_CONTROL_BYTES: usize = 64 * 1024;

#[derive(Default)]
pub(crate) struct FrameParts {
    control: BytesMut,
    payloads: Vec<(usize, Bytes)>,
    payload_bytes: usize,
}

impl FrameParts {
    pub(crate) fn len(&self) -> usize {
        self.control.len() + self.payload_bytes
    }

    pub(crate) fn clear(&mut self) {
        self.control.clear();
        self.payloads.clear();
        self.payload_bytes = 0;
    }

    pub(crate) fn push_wire(&mut self, message: &impl Message) {
        self.put_u32(message.encoded_len() as u32);
        message.encode_raw(self);
    }

    pub(crate) fn push_durable(&mut self, message: &impl Message) {
        self.put_u32(message.encoded_len() as u32);
        let checksum = self.control.len();
        self.put_u32(0);
        let body = self.control.len();
        let payload = self.payloads.len();
        message.encode_raw(self);
        let mut crc = crc32fast::Hasher::new();
        for part in self.parts_from(body, payload) {
            crc.update(part);
        }
        self.control[checksum..body].copy_from_slice(&crc.finalize().to_be_bytes());
    }

    fn parts_from(&self, mut start: usize, payload: usize) -> impl Iterator<Item = &[u8]> {
        let payloads = &self.payloads[payload..];
        let tail = payloads.last().map_or(start, |(at, _)| *at);
        payloads.iter().flat_map(move |(end, body)| {
            let header = &self.control[start..*end];
            start = *end;
            [header, body.as_ref()]
        }).chain(std::iter::once(&self.control[tail..]))
          .filter(|part| !part.is_empty())
    }

    pub(crate) fn write_to(&self, mut writer: impl Write) -> io::Result<()> {
        let mut parts = self.parts_from(0, 0).peekable();
        while parts.peek().is_some() {
            let (mut spans, count) = next_spans(&mut parts);
            let mut remaining = &mut spans[..count];
            while !remaining.is_empty() {
                match writer.write_vectored(remaining) {
                    Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                    Ok(n) => IoSlice::advance_slices(&mut remaining, n),
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => (),
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(())
    }

    pub(crate) fn wire_batch_full(&self) -> bool {
        self.control.len() >= WIRE_CONTROL_BYTES || self.payloads.len() >= SPANS_PER_WRITE
    }

    pub(crate) async fn send_wire(&mut self, writer: &mut (impl AsyncWrite + Unpin)) -> io::Result<()> {
        {
            let mut parts = self.parts_from(0, 0).peekable();
            while parts.peek().is_some() {
                let (mut spans, count) = next_spans(&mut parts);
                let mut remaining = &mut spans[..count];
                while !remaining.is_empty() {
                    let n = writer.write_vectored(remaining).await?;
                    if n == 0 {
                        return Err(io::Error::new(io::ErrorKind::WriteZero, "write zero"));
                    }
                    IoSlice::advance_slices(&mut remaining, n);
                }
            }
        }
        if self.control.capacity() > WIRE_CONTROL_BYTES || self.payloads.capacity() > SPANS_PER_WRITE {
            *self = Self::default();
        } else {
            self.clear();
        }
        Ok(())
    }
}

fn next_spans<'a>(
    parts: &mut impl Iterator<Item = &'a [u8]>,
) -> ([IoSlice<'a>; SPANS_PER_WRITE], usize) {
    let mut spans = std::array::from_fn(|_| IoSlice::new(&[]));
    let mut count = 0;
    for (slot, part) in spans.iter_mut().zip(parts) {
        *slot = IoSlice::new(part);
        count += 1;
    }
    (spans, count)
}

// SAFETY: The writable chunk and initialized-byte accounting are both delegated
// to the same BytesMut. A retained span never borrows its writable memory.
unsafe impl BufMut for FrameParts {
    fn remaining_mut(&self) -> usize {
        self.control.remaining_mut()
    }

    unsafe fn advance_mut(&mut self, count: usize) {
        // SAFETY: The caller initialized these bytes in the delegated chunk.
        unsafe { self.control.advance_mut(count) };
    }

    fn chunk_mut(&mut self) -> &mut UninitSlice {
        self.control.chunk_mut()
    }

    fn put_slice(&mut self, bytes: &[u8]) {
        self.control.put_slice(bytes);
    }

    fn put<T: Buf>(&mut self, mut bytes: T)
    where
        Self: Sized,
    {
        let len = bytes.remaining();
        if len != 0 {
            // Prost writes Bytes through put and strings through put_slice.
            // Bytes::copy_to_bytes lends the original backing.
            self.payloads.push((self.control.len(), bytes.copy_to_bytes(len)));
            self.payload_bytes += len;
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, PartialEq, Message)]
    struct Envelope {
        #[prost(uint64, tag = "1")]
        id: u64,
        #[prost(bytes = "bytes", repeated, tag = "2")]
        payloads: Vec<Bytes>,
        #[prost(string, tag = "3")]
        tail: String,
    }

    #[test]
    fn framed_prost_output_keeps_payload_owners_and_exact_bytes() {
        let payload = Bytes::from(vec![0xaa; 128 * 1024]);
        let message = Envelope {
            id: 800,
            payloads: vec![Bytes::new(), payload.clone(), Bytes::from_static(b"second")],
            tail: "trailing control".into(),
        };
        let encoded = message.encode_to_vec();
        let mut expected = Vec::new();
        expected.put_u32(encoded.len() as u32);
        expected.put_slice(&encoded);
        expected.put_u32(encoded.len() as u32);
        expected.put_u32(crc32fast::hash(&encoded));
        expected.put_slice(&encoded);

        let mut frames = FrameParts::default();
        frames.push_wire(&message);
        frames.push_durable(&message);
        assert!(frames.payloads.iter().any(|(_, span)| span.as_ptr() == payload.as_ptr()));
        assert_eq!(frames.len(), expected.len());
        let mut actual = Vec::new();
        frames.write_to(&mut actual).unwrap();
        assert_eq!(actual, expected);
    }
}
