mod aggregate;
mod context;
mod cursor;
mod operators;
mod join;
mod sort;
mod window;
mod scan;
mod spill;
mod patch;
mod system_schema;
mod system;
mod inspection;
mod trace;

pub use context::{CatalogSnapshot, DatabaseSnapshot, QueryContext, QueryOptions};
pub use cursor::{PreparedQuery, QueryCursor};

fn invalid(code: &str, message: impl std::fmt::Display) -> rtdb_types::Anomaly {
    rtdb_types::Anomaly::incorrect(format!("rtdb.query/{code}"), message.to_string())
}
