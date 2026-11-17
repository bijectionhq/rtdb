mod aggregate;
mod context;
mod parameters;
mod cursor;
mod runtime_schema;
mod operators;
mod list;
#[cfg(test)]
mod list_tests;
mod join;
mod join_key;
mod pushdown;
mod sort;
mod window;
mod scan;
mod scan_tasks;
mod spill;
mod patch;
mod system_schema;
mod system;
mod inspection;
mod trace;
mod settings;
mod pages;
mod analysis;
mod explain;
mod optimize;
mod materialized;
mod cross;
mod join_storage;
mod mark;

pub use context::{CatalogSnapshot, DatabaseSnapshot, QueryContext, QueryOptions};
pub use cursor::{PreparedQuery, QueryCursor};

fn invalid(code: &str, message: impl std::fmt::Display) -> rtdb_types::Anomaly {
    rtdb_types::Anomaly::incorrect(format!("rtdb.query/{code}"), message.to_string())
}
