//! Immutable database objects, writer garbage collection and generation erasure.

mod erasure;
mod gc;
mod objects;

pub use erasure::{ErasurePlan, PreparedErasure, Retirement};
pub use gc::{GcPlan, Reachability};
pub use objects::{PreparedObject, VerifiedManifest, WriterObjects, read_file, read_manifest};
pub use rtdb_storage::{LocalStore, MemoryStore, ObjectKey, ObjectStore, StoredObject};
