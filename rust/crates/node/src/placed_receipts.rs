use rtdb_storage::log::LogOffset;
use rtdb_types::{Anomaly, BlockIndex, PlacedAttempt, PlacedAttemptScope, ReplicaPosition, Result, Timestamp};

use crate::ReplicaReadIndex;

const TOKEN_PREFIX: &str = "rtdb1.";

pub enum AwaitAuthority {
    SourcePosition,
    Live {
        codec: Box<dyn PlacedReceiptCodec>,
        recovery: ClosedEpochRecovery,
        placed_receipt_retirement: bool,
        direct_sql_commits: bool,
    },
    Checkpoint {
        codec: Box<dyn PlacedReceiptCodec>,
        block: BlockIndex,
        through: ReplicaPosition,
        root: [u8; 32],
    },
    Frozen,
}

impl AwaitAuthority {
    pub(crate) fn codec(&self) -> Option<&dyn PlacedReceiptCodec> {
        match self {
            Self::Live { codec, .. } | Self::Checkpoint { codec, .. } => Some(codec.as_ref()),
            _ => None,
        }
    }
}

pub enum ClosedEpochRecovery {
    Opened(LogOffset),
    ReadIndex(Box<dyn ReplicaReadIndex>),
}

impl ClosedEpochRecovery {
    pub(crate) async fn confirm(&self) -> Result<LogOffset> {
        match self {
            Self::Opened(position) => Ok(*position),
            Self::ReadIndex(index) => index.confirm().await,
        }
    }
}

pub trait PlacedReceiptCodec: Send + Sync {
    fn scope(&self) -> PlacedAttemptScope;
    fn seal(&self, value: &[u8]) -> Result<String>;
    fn open(&self, token: &str) -> Result<Vec<u8>>;
    fn seal_snapshot(&self, value: &[u8]) -> Result<String>;
    fn open_snapshot(&self, token: &str) -> Result<Vec<u8>>;
    fn seal_range(&self, value: &[u8]) -> Result<String> { self.seal(value) }
    fn open_range(&self, token: &str) -> Result<Vec<u8>> { self.open(token) }
}

pub(crate) enum PlacedToken {
    Pending { attempt: PlacedAttempt, submitted_at: Timestamp },
    Applied { attempt: PlacedAttempt, through: ReplicaPosition },
}

pub(crate) fn is_placed_token(token: &str) -> bool { token.starts_with(TOKEN_PREFIX) }

pub(crate) fn invalid_token() -> Anomaly {
    Anomaly::incorrect("rtdb/invalid-await-token", "Await token does not belong to this database history and principal")
}

impl PlacedToken {
    pub(crate) fn seal(&self, authority: &dyn PlacedReceiptCodec) -> Result<String> {
        let (tag, attempt, coordinate) = match *self {
            Self::Pending { attempt, submitted_at } => (0, attempt, submitted_at.0),
            Self::Applied { attempt, through } => (1, attempt, through.0),
        };
        let mut bytes = Vec::with_capacity(105);
        bytes.push(tag);
        bytes.extend_from_slice(&attempt.encode());
        bytes.extend_from_slice(&coordinate.to_be_bytes());
        Ok(format!("{TOKEN_PREFIX}{}", authority.seal(&bytes)?))
    }

    pub(crate) fn open(token: &str, authority: &dyn PlacedReceiptCodec, principal: Option<&str>) -> Result<Self> {
        let sealed = token.strip_prefix(TOKEN_PREFIX).ok_or_else(invalid_token)?;
        let bytes = authority.open(sealed)?;
        let (fields, coordinate) = bytes.split_last_chunk::<8>().ok_or_else(invalid_token)?;
        let (&tag, attempt) = fields.split_first().ok_or_else(invalid_token)?;
        let attempt = PlacedAttempt::decode(attempt).map_err(|_| invalid_token())?;
        if !attempt.scope().same_history(authority.scope()) || !attempt.belongs_to(principal) {
            return Err(invalid_token());
        }
        let coordinate = i64::from_be_bytes(*coordinate);
        match tag {
            0 => Ok(Self::Pending { attempt, submitted_at: Timestamp(coordinate) }),
            1 if coordinate >= 0 => Ok(Self::Applied { attempt, through: ReplicaPosition(coordinate) }),
            _ => Err(invalid_token()),
        }
    }
}
