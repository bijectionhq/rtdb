mod config;
mod decoder;
mod driver;
mod indexer;
mod metrics;
mod snapshot;
mod stream;
mod token;
mod wire;

pub use config::{CONFIG_TYPE, DIRECT_MIRROR_TYPE, Remote, SourceConfig, SslMode};
pub use decoder::{ArrowChanges, ArrowPgTransaction, Decoder, PgTransaction, RowKind, RowOperation};
pub use driver::{ReconnectPolicy, SourceDriver, SourceObserver, SourcePhase, SourceStatus};
pub use indexer::{DirectMirror, PgIndexer};
pub use metrics::{SourceAssignmentMetrics, SourceMetrics, SourceMetricsFactory};
pub use rtdb_pg_codec::read_text;
pub use token::{ConfirmationState, Lsn, ResumeToken};

#[cfg(test)]
mod integration;
