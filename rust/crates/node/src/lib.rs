mod ordered_index;
mod session;
mod resources;
mod catalog;
mod config;
mod database;
mod read_currency;
mod node;
mod backends;
mod runtime;
mod metrics;
mod health;
pub use health::{HealthChecks, HealthResponse};
#[cfg(feature = "pg-source")]
mod postgres;
#[cfg(feature = "pg-source")]
pub use postgres::{PostgresIndexerFactory, PostgresSourceFactory};
#[cfg(feature = "kafka-connect")]
mod kafka_connect;
#[cfg(feature = "kafka-connect")]
pub use kafka_connect::{KafkaConnectSourceFactory, KafkaIndexerFactory};

pub use session::*;
pub use resources::*;
pub use catalog::{DatabaseCatalog, Membership};
pub use config::*;
pub use database::{CompactionRelay, Database};
pub use node::{IDEMPOTENCY_KEY_PARAMETER, NOT_HELD, REPLICA_BEHIND, NativeSession, Node, NodeDriver, NodeServices, ReadFence};

mod statement;
pub use statement::{PreparedSql, ResultSignaturePolicy};
mod control;
mod inspection;
mod inspection_render;
pub use control::{ControlExecutionOptions, ControlFrontend, ControlResult, control_schema, transaction_settings};
mod operations;
pub use operations::{CompactorReset, SnapshotExport};

/// The stack each runtime worker gets: a query's operators and expressions nest with the depth of the statement,
/// and stack overflow aborts the process rather than failing the query. It is reserved, not committed, up front.
pub const WORKER_STACK_BYTES: usize = 16 << 20;
