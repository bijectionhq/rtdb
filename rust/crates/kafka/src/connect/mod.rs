mod config;
mod converter;
mod numeric;
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
use rtdb_java_date::DatePattern;
#[cfg(test)]
mod date_environment;

pub use config::{ConnectConfig, PluginSpec, ResumeToken, SourceConfig, TransformSpec, DOCS_TYPE, SOURCE_TYPE, proto};
pub use data::{ConnectData, ConnectField, ConnectSchema, SchemaKind, SinkRecord};
pub use schema_graph::SchemaReference;
pub use rtdb_java_date::{DateClock,DateEnvironment,DateEnvironmentInput,LOCALE_DATA_SHA256};
pub use docs::{DocsIndexer, RecordIndexer, SourceTransactions};
pub use driver::{SourceDriver, SourceIdentity, SourceObserver, SourceOptions, SourceRegistration, SourceStatus};
pub use plugins::{ConversionContext, Converter, ConverterFactory, PluginRegistry, Predicate, PredicateFactory, Transform, TransformFactory, TransformChain};

fn error(code: &str, message: impl std::fmt::Display) -> rtdb_types::Anomaly {
    rtdb_types::Anomaly::incorrect(format!("xtdb.kafka-connect-source/{code}"), message.to_string())
}
