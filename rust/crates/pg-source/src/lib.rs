mod config;
mod decoder;
mod driver;
mod indexer;
mod snapshot;
mod stream;
mod text;
mod token;
mod wire;

pub use config::{CONFIG_TYPE, DIRECT_MIRROR_TYPE, Remote, SourceConfig, SslMode};
pub use decoder::{Decoder, PgTransaction, RowKind, RowOperation};
pub use driver::{ReconnectPolicy, SourceDriver, SourceObserver, SourcePhase, SourceStatus};
pub use indexer::{DirectMirror, PgIndexer};
pub use text::read_text;
pub use token::{ConfirmationState, Lsn, ResumeToken};

#[cfg(test)]
mod integration;
