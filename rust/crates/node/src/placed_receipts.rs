use async_trait::async_trait;
use rtdb_storage::log::LogOffset;
use rtdb_types::{Anomaly, PlacedAttempt, PlacedAttemptScope, ReplicaPosition, Result, Timestamp};

const TOKEN_PREFIX: &str = "rtdb1.";

pub enum AwaitAuthority {
    SourcePosition,
    Placed(Box<dyn PlacedReceiptAuthority>),
    Frozen,
}

impl AwaitAuthority {
    pub(crate) fn placed(&self) -> Option<&dyn PlacedReceiptAuthority> {
        match self { Self::Placed(authority) => Some(authority.as_ref()), _ => None }
    }
}

#[async_trait]
pub trait PlacedReceiptAuthority: Send + Sync {
    fn scope(&self) -> PlacedAttemptScope;
    fn seal(&self, value: &[u8]) -> Result<String>;
    fn open(&self, token: &str) -> Result<Vec<u8>>;
    async fn recovered_through(&self) -> Result<LogOffset>;
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
    pub(crate) fn seal(&self, authority: &dyn PlacedReceiptAuthority) -> Result<String> {
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

    pub(crate) fn open(token: &str, authority: &dyn PlacedReceiptAuthority, principal: Option<&str>) -> Result<Self> {
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
