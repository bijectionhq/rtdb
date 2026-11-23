use std::sync::Arc;

use bytes::Bytes;
use rtdb_columnar::MemoryBudget;
use rtdb_types::Timestamp;
use tonic::Status;
use uuid::Uuid;

use crate::{error::{invalid, status}, wire};

const VERSION: &[u8] = b"RTDBFT\x02";

pub(crate) enum Ticket {
    Read(Uuid),
    Empty,
}

impl Ticket {
    pub fn encode(&self, memory: &Arc<MemoryBudget>) -> Result<Bytes, Status> {
        let charge = memory.reserve(64).map_err(status)?;
        let mut bytes = Vec::with_capacity(VERSION.len() + 16);
        bytes.extend_from_slice(VERSION);
        if let Self::Read(id) = self {
            bytes.extend_from_slice(id.as_bytes());
        }
        Ok(wire::retain(bytes, charge))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, Status> {
        let body = bytes.strip_prefix(VERSION).ok_or_else(|| invalid("invalid ticket version"))?;
        if body.is_empty() {
            return Ok(Self::Empty);
        }
        let id = Uuid::from_slice(body).map_err(|_| invalid("invalid read ticket"))?;
        Ok(Self::Read(id))
    }
}

pub(crate) fn timestamp(time: Timestamp) -> Result<String, Status> {
    rtdb_types::Instant::from(time).to_iso_string().map_err(status)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_ticket_carries_only_the_versioned_opaque_handle() {
        let memory = MemoryBudget::new(4096);
        let id = Uuid::from_bytes([1; 16]);
        let encoded = Ticket::Read(id).encode(&memory).unwrap();
        assert!(matches!(Ticket::decode(&encoded), Ok(Ticket::Read(decoded)) if decoded == id));
        assert_eq!(&encoded[VERSION.len()..], id.as_bytes());
        assert!(Ticket::decode(&encoded[..encoded.len() - 1]).is_err());
        assert!(Ticket::decode(b"RTDBFT\x01").is_err());
        assert!(matches!(Ticket::decode(&Ticket::Empty.encode(&memory).unwrap()), Ok(Ticket::Empty)));
    }
}
