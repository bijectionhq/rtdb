use std::{error::Error, fmt, num::NonZeroU64};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AssertionId([u8; 32]);

impl AssertionId {
    pub const WIDTH: usize = 32;

    pub fn from_bytes(bytes: [u8; Self::WIDTH]) -> Self {
        Self(bytes)
    }

    pub fn to_bytes(self) -> [u8; Self::WIDTH] {
        self.0
    }

    pub fn from_mint_block(namespace: [u8; 16], encrypted_counter: [u8; 16]) -> Self {
        let mut bytes = [0; Self::WIDTH];
        bytes[..16].copy_from_slice(&namespace);
        bytes[16..].copy_from_slice(&encrypted_counter);
        Self(bytes)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PlacedId {
    pub route: [u8; 32],
    pub assertion: AssertionId,
}

impl PlacedId {
    pub const WIDTH: usize = 64;

    pub fn from_bytes(bytes: [u8; Self::WIDTH]) -> Self {
        Self {
            route: field(&bytes, 0),
            assertion: AssertionId::from_bytes(field(&bytes, 32)),
        }
    }

    pub fn to_bytes(self) -> [u8; Self::WIDTH] {
        let mut bytes = [0; Self::WIDTH];
        bytes[..32].copy_from_slice(&self.route);
        bytes[32..].copy_from_slice(&self.assertion.to_bytes());
        bytes
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Position {
    pub line_generation: [u8; 16],
    pub publication: u64,
    pub ordinal: NonZeroU64,
}

impl Position {
    pub const WIDTH: usize = 32;

    pub fn from_bytes(bytes: [u8; Self::WIDTH]) -> Result<Self, AssertionWireError> {
        Ok(Self {
            line_generation: field(&bytes, 0),
            publication: u64::from_be_bytes(field(&bytes, 16)),
            ordinal: NonZeroU64::new(u64::from_be_bytes(field(&bytes, 24)))
                .ok_or(AssertionWireError)?,
        })
    }

    pub fn to_bytes(self) -> [u8; Self::WIDTH] {
        let mut bytes = [0; Self::WIDTH];
        bytes[..16].copy_from_slice(&self.line_generation);
        bytes[16..24].copy_from_slice(&self.publication.to_be_bytes());
        bytes[24..].copy_from_slice(&self.ordinal.get().to_be_bytes());
        bytes
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShadowBound {
    MinusInfinity,
    Fixed(i64),
    PendingOffset(i64),
    PlusInfinity,
}

impl ShadowBound {
    const WIDTH: usize = 9;

    fn from_bytes(bytes: [u8; Self::WIDTH]) -> Result<Self, AssertionWireError> {
        match (bytes[0], i64::from_be_bytes(field(&bytes, 1))) {
            (0, 0) => Ok(Self::MinusInfinity),
            (1, value) => Ok(Self::Fixed(value)),
            (2, value) => Ok(Self::PendingOffset(value)),
            (3, 0) => Ok(Self::PlusInfinity),
            _ => Err(AssertionWireError),
        }
    }

    fn to_bytes(self) -> [u8; Self::WIDTH] {
        let (tag, payload) = match self {
            Self::MinusInfinity => (0, 0),
            Self::Fixed(value) => (1, value),
            Self::PendingOffset(value) => (2, value),
            Self::PlusInfinity => (3, 0),
        };
        let mut bytes = [tag; Self::WIDTH];
        bytes[1..].copy_from_slice(&payload.to_be_bytes());
        bytes
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShadowV1 {
    position: Position,
    lower: ShadowBound,
    upper: ShadowBound,
    padded_size: u64,
    route: [u8; 32],
}

impl ShadowV1 {
    pub const WIDTH: usize = 90;

    pub fn new(
        position: Position,
        lower: ShadowBound,
        upper: ShadowBound,
        padded_size: u64,
        route: [u8; 32],
    ) -> Result<Self, AssertionWireError> {
        use ShadowBound::{Fixed, MinusInfinity, PendingOffset, PlusInfinity};
        let ordered = match (lower, upper) {
            (PlusInfinity, _) | (_, MinusInfinity) => false,
            (MinusInfinity, _) | (_, PlusInfinity) => true,
            (Fixed(a), Fixed(b)) | (PendingOffset(a), PendingOffset(b)) => a < b,
            (Fixed(_), PendingOffset(_)) | (PendingOffset(_), Fixed(_)) => true,
        };
        if !ordered {
            return Err(AssertionWireError);
        }
        Ok(Self { position, lower, upper, padded_size, route })
    }

    pub fn position(self) -> Position {
        self.position
    }

    pub fn bounds(self) -> (ShadowBound, ShadowBound) {
        (self.lower, self.upper)
    }

    pub fn padded_size(self) -> u64 {
        self.padded_size
    }

    pub fn route(self) -> [u8; 32] {
        self.route
    }

    pub fn from_bytes(bytes: [u8; Self::WIDTH]) -> Result<Self, AssertionWireError> {
        Self::new(
            Position::from_bytes(field(&bytes, 0))?,
            ShadowBound::from_bytes(field(&bytes, 32))?,
            ShadowBound::from_bytes(field(&bytes, 41))?,
            u64::from_be_bytes(field(&bytes, 50)),
            field(&bytes, 58),
        )
    }

    pub fn to_bytes(self) -> [u8; Self::WIDTH] {
        let mut bytes = [0; Self::WIDTH];
        bytes[..32].copy_from_slice(&self.position.to_bytes());
        bytes[32..41].copy_from_slice(&self.lower.to_bytes());
        bytes[41..50].copy_from_slice(&self.upper.to_bytes());
        bytes[50..58].copy_from_slice(&self.padded_size.to_be_bytes());
        bytes[58..].copy_from_slice(&self.route);
        bytes
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExactEraseSelector {
    Placed(PlacedId),
    Position(Position),
}

impl ExactEraseSelector {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, AssertionWireError> {
        match bytes.split_first() {
            Some((0, body)) => Ok(Self::Placed(PlacedId::from_bytes(
                body.try_into().map_err(|_| AssertionWireError)?,
            ))),
            Some((1, body)) => Ok(Self::Position(Position::from_bytes(
                body.try_into().map_err(|_| AssertionWireError)?,
            )?)),
            _ => Err(AssertionWireError),
        }
    }

    pub fn to_bytes(self) -> Vec<u8> {
        match self {
            Self::Placed(placed) => [&[0], placed.to_bytes().as_slice()].concat(),
            Self::Position(position) => [&[1], position.to_bytes().as_slice()].concat(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlacementBinding {
    pub assertion: AssertionId,
    pub stage: [u8; 16],
    pub capsule_digest: [u8; 32],
    pub position: Position,
    pub clock_commitment: [u8; 32],
    pub address_extent_commitment: [u8; 32],
    pub disclosure_scope: [u8; 32],
    pub membership_commitment: [u8; 32],
}

impl PlacementBinding {
    pub const WIDTH: usize = 241;

    pub fn from_bytes(bytes: [u8; Self::WIDTH]) -> Result<Self, AssertionWireError> {
        if bytes[0] != 1 {
            return Err(AssertionWireError);
        }
        Ok(Self {
            assertion: AssertionId::from_bytes(field(&bytes, 1)),
            stage: field(&bytes, 33),
            capsule_digest: field(&bytes, 49),
            position: Position::from_bytes(field(&bytes, 81))?,
            clock_commitment: field(&bytes, 113),
            address_extent_commitment: field(&bytes, 145),
            disclosure_scope: field(&bytes, 177),
            membership_commitment: field(&bytes, 209),
        })
    }

    pub fn to_bytes(self) -> [u8; Self::WIDTH] {
        let mut bytes = [0; Self::WIDTH];
        bytes[0] = 1;
        bytes[1..33].copy_from_slice(&self.assertion.to_bytes());
        bytes[33..49].copy_from_slice(&self.stage);
        bytes[49..81].copy_from_slice(&self.capsule_digest);
        bytes[81..113].copy_from_slice(&self.position.to_bytes());
        bytes[113..145].copy_from_slice(&self.clock_commitment);
        bytes[145..177].copy_from_slice(&self.address_extent_commitment);
        bytes[177..209].copy_from_slice(&self.disclosure_scope);
        bytes[209..].copy_from_slice(&self.membership_commitment);
        bytes
    }
}

/// The destination slot fixed by the reviewed landing before RTDB assigns
/// the publication position.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RootLandingTarget {
    pub receipt: [u8; 32],
    pub review_slot: u64,
    pub address_extent: [u8; 32],
    pub disclosure: [u8; 32],
}

impl RootLandingTarget {
    pub const WIDTH: usize = 104;

    pub fn from_bytes(bytes: [u8; Self::WIDTH]) -> Result<Self, AssertionWireError> {
        let target = Self {
            receipt: field(&bytes, 0),
            review_slot: u64::from_be_bytes(field(&bytes, 32)),
            address_extent: field(&bytes, 40),
            disclosure: field(&bytes, 72),
        };
        if [target.receipt, target.address_extent, target.disclosure].contains(&[0; 32]) {
            return Err(AssertionWireError);
        }
        Ok(target)
    }

    pub fn to_bytes(self) -> [u8; Self::WIDTH] {
        let mut bytes = [0; Self::WIDTH];
        bytes[..32].copy_from_slice(&self.receipt);
        bytes[32..40].copy_from_slice(&self.review_slot.to_be_bytes());
        bytes[40..72].copy_from_slice(&self.address_extent);
        bytes[72..].copy_from_slice(&self.disclosure);
        bytes
    }
}

/// The original ordered Root occurrence and the exact frames its landing
/// receipt accepted. The installed release owner authenticates the order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RootLandingBinding {
    pub position: Position,
    pub assertion: AssertionId,
    pub stage: [u8; 16],
    pub target: RootLandingTarget,
    pub payload_digest: [u8; 32],
    pub link_digest: [u8; 32],
}

impl RootLandingBinding {
    pub const WIDTH: usize = 248;

    pub fn from_bytes(bytes: [u8; Self::WIDTH]) -> Result<Self, AssertionWireError> {
        let binding = Self {
            position: Position::from_bytes(field(&bytes, 0))?,
            assertion: AssertionId::from_bytes(field(&bytes, 32)),
            stage: field(&bytes, 64),
            target: RootLandingTarget::from_bytes(field(&bytes, 80))?,
            payload_digest: field(&bytes, 184),
            link_digest: field(&bytes, 216),
        };
        if binding.position.line_generation == [0; 16] || binding.stage == [0; 16]
            || [binding.payload_digest, binding.link_digest].contains(&[0; 32])
        {
            return Err(AssertionWireError);
        }
        Ok(binding)
    }

    pub fn to_bytes(self) -> [u8; Self::WIDTH] {
        let mut bytes = [0; Self::WIDTH];
        bytes[..32].copy_from_slice(&self.position.to_bytes());
        bytes[32..64].copy_from_slice(&self.assertion.to_bytes());
        bytes[64..80].copy_from_slice(&self.stage);
        bytes[80..184].copy_from_slice(&self.target.to_bytes());
        bytes[184..216].copy_from_slice(&self.payload_digest);
        bytes[216..].copy_from_slice(&self.link_digest);
        bytes
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AudienceFrontier {
    pub line_generation: [u8; 16],
    pub disclosure_scope: [u8; 32],
    pub publication: u64,
}

impl AudienceFrontier {
    const WIDTH: usize = 56;

    fn lane(&self) -> ([u8; 16], [u8; 32]) {
        (self.line_generation, self.disclosure_scope)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AudienceCut(Vec<AudienceFrontier>);

impl AudienceCut {
    pub fn new(frontiers: Vec<AudienceFrontier>) -> Result<Self, AssertionWireError> {
        if !frontiers.windows(2).all(|w| w[0].lane() < w[1].lane()) {
            return Err(AssertionWireError);
        }
        Ok(Self(frontiers))
    }

    pub fn frontiers(&self) -> &[AudienceFrontier] {
        &self.0
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, AssertionWireError> {
        let Some((1, body)) = bytes.split_first() else {
            return Err(AssertionWireError);
        };
        let components = body.chunks_exact(AudienceFrontier::WIDTH);
        if !components.remainder().is_empty() {
            return Err(AssertionWireError);
        }
        Self::new(components.map(|component| AudienceFrontier {
            line_generation: field(component, 0),
            disclosure_scope: field(component, 16),
            publication: u64::from_be_bytes(field(component, 48)),
        }).collect())
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![1];
        for frontier in &self.0 {
            bytes.extend_from_slice(&frontier.line_generation);
            bytes.extend_from_slice(&frontier.disclosure_scope);
            bytes.extend_from_slice(&frontier.publication.to_be_bytes());
        }
        bytes
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AssertionWireError;

impl fmt::Display for AssertionWireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid assertion wire value")
    }
}

impl Error for AssertionWireError {}

fn field<const N: usize>(bytes: &[u8], at: usize) -> [u8; N] {
    let mut result = [0; N];
    result.copy_from_slice(&bytes[at..at + N]);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shadow_has_canonical_layout_and_bounds() {
        let position = Position {
            line_generation: [0xab; 16],
            publication: 19,
            ordinal: NonZeroU64::new(7).unwrap(),
        };
        let shadow = ShadowV1::new(
            position,
            ShadowBound::Fixed(-10),
            ShadowBound::PlusInfinity,
            4096,
            [0xcd; 32],
        ).unwrap();
        let mut bytes = shadow.to_bytes();
        assert_eq!(&bytes[..16], &[0xab; 16]);
        assert_eq!(&bytes[16..24], &19_u64.to_be_bytes());
        assert_eq!(&bytes[24..32], &7_u64.to_be_bytes());
        assert_eq!(bytes[32], 1);
        assert_eq!(&bytes[33..41], &(-10_i64).to_be_bytes());
        assert_eq!(&bytes[41..50], &[3, 0, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(&bytes[50..58], &4096_u64.to_be_bytes());
        assert_eq!(&bytes[58..], &[0xcd; 32]);
        assert_eq!(ShadowV1::from_bytes(bytes), Ok(shadow));
        bytes[49] = 1;
        assert_eq!(ShadowV1::from_bytes(bytes), Err(AssertionWireError));
        bytes[49] = 0;
        bytes[31] = 0;
        assert_eq!(ShadowV1::from_bytes(bytes), Err(AssertionWireError));
    }

    #[test]
    fn placed_id_starts_with_the_route() {
        let assertion = AssertionId::from_mint_block([1; 16], [2; 16]);
        let placed = PlacedId { route: [3; 32], assertion };
        let bytes = placed.to_bytes();
        assert_eq!(&bytes[..32], &[3; 32]);
        assert_eq!(&bytes[32..48], &[1; 16]);
        assert_eq!(&bytes[48..], &[2; 16]);
        assert_eq!(PlacedId::from_bytes(bytes), placed);
    }
}
