mod config;
mod converter;
mod data;
mod docs;
mod driver;
mod plugins;
mod transforms;
mod avro;
mod json_schema;
mod json_value;
mod json_decimal;
mod registry;
mod header;
mod schema_graph;
use rtdb_types::java_date as legacy_calendar;
mod locale_data;
mod date_locale;
mod date_names;
mod date_number;
mod date_zone_names;
mod date_environment;
mod date_pattern;
mod date_platform;
mod date_calendar;

pub use config::{ConnectConfig, PluginSpec, ResumeToken, SourceConfig, TransformSpec, DOCS_TYPE, SOURCE_TYPE, proto};
pub use data::{ConnectData, ConnectField, ConnectSchema, SchemaKind, SinkRecord};
pub use schema_graph::SchemaReference;
pub use date_environment::{DateClock,DateEnvironment,DateEnvironmentInput};
pub use docs::{DocsIndexer, RecordIndexer, SourceTransactions};
pub use driver::{SourceDriver, SourceIdentity, SourceObserver, SourceOptions, SourceRegistration, SourceStatus};
pub use plugins::{ConversionContext, Converter, ConverterFactory, PluginRegistry, Predicate, PredicateFactory, Transform, TransformFactory, TransformChain};

/// SHA-256 of the retained Temurin25 CLDR resource bundle used by native Connect date formatting.
/// The build verifies these exact bytes before generating immutable locale tables.
pub const LOCALE_DATA_SHA256: &str = locale_data::LOCALE_DATA_SHA256;

fn error(code: &str, message: impl std::fmt::Display) -> rtdb_types::Anomaly {
    rtdb_types::Anomaly::incorrect(format!("xtdb.kafka-connect-source/{code}"), message.to_string())
}
