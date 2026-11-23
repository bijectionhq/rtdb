use rtdb_types::Timestamp;
use uuid::Uuid;

// Kind 7 can append a record an older resolver would apply without a marker.
pub const GATEWAY_REPLAY_PERMIT_KIND: u8 = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum GatewayReplayMode { Dispatch = 0, ConfirmDispatch = 1, Replay = 2 }

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GatewayReplayPermit {
    pub nonce: Uuid,
    pub not_after: Timestamp,
    pub mode: GatewayReplayMode,
}

impl GatewayReplayPermit {
    pub fn encode(self) -> [u8; 26] {
        let mut bytes = [0; 26];
        bytes[0] = GATEWAY_REPLAY_PERMIT_KIND;
        bytes[1] = self.mode as u8;
        bytes[2..10].copy_from_slice(&self.not_after.0.to_be_bytes());
        bytes[10..].copy_from_slice(self.nonce.as_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let &[GATEWAY_REPLAY_PERMIT_KIND, mode, ref fields @ ..] = bytes else { return None };
        let mode = match mode {
            0 => GatewayReplayMode::Dispatch,
            1 => GatewayReplayMode::ConfirmDispatch,
            2 => GatewayReplayMode::Replay,
            _ => return None,
        };
        let (time, id) = fields.split_at_checked(8)?;
        let not_after = Timestamp(i64::from_be_bytes(time.try_into().ok()?));
        let nonce = Uuid::from_bytes(id.try_into().ok()?);
        (!nonce.is_nil() && not_after != Timestamp::MAX).then_some(Self { nonce, not_after, mode })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_gateway_permit_preserves_the_exact_nonce_mode_and_cutoff() {
        for mode in [GatewayReplayMode::Dispatch, GatewayReplayMode::ConfirmDispatch, GatewayReplayMode::Replay] {
            let permit = GatewayReplayPermit { nonce: Uuid::from_u128(9), not_after: Timestamp(-123), mode };
            let encoded = permit.encode();
            assert_eq!(GatewayReplayPermit::decode(&encoded), Some(permit));
            let mut legacy = encoded;
            legacy[0] = 7;
            assert_eq!(GatewayReplayPermit::decode(&legacy), None);
            assert_eq!(GatewayReplayPermit::decode(&encoded[..25]), None);
            let mut unknown_mode = encoded;
            unknown_mode[1] = 3;
            assert_eq!(GatewayReplayPermit::decode(&unknown_mode), None);
        }
    }
}
