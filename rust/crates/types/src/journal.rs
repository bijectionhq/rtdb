use serde::{Deserialize, Serialize};

/// A route to a durable journal, with its expected identity when the caller knows it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(untagged)]
pub enum JournalEndpoint {
    Member { address: String, journal: String },
    Address(String),
}

impl JournalEndpoint {
    pub fn address(&self) -> &str {
        match self { Self::Member { address, .. } | Self::Address(address) => address }
    }

    pub fn journal(&self) -> Option<&str> {
        match self { Self::Member { journal, .. } => Some(journal), Self::Address(_) => None }
    }
}

impl From<String> for JournalEndpoint {
    fn from(address: String) -> Self { Self::Address(address) }
}

impl From<&str> for JournalEndpoint {
    fn from(address: &str) -> Self { address.to_owned().into() }
}

pub fn valid_journal_identity(journal: &str) -> bool {
    uuid::Uuid::try_parse(journal).is_ok_and(|id| !id.is_nil() && id.hyphenated().encode_lower(&mut uuid::Uuid::encode_buffer()) == journal)
}
