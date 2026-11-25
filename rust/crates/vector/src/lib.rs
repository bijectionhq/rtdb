//! Opt-in approximate nearest-neighbour search: immutable IVF generations over current tries, built by the artefact lane,
//! and a scan accelerator that probes them, ranks the frontier exactly and leaves the final ranking to the query.
//! Exact `COSINE_DISTANCE` ranking stays the default and the fallback.
//! Behavioural spec: dev/doc/vector.allium.

mod accelerate;
mod definition;
mod file;
mod kmeans;
mod lane;
mod probe;
mod readiness;
mod shortlist;
mod rows;

pub use accelerate::{PREDICATE, VectorAccelerator};
pub use definition::{Approximate, CatalogDefinition, DEFINITIONS, Definitions, KIND, Method, VectorDefinition, definitions_in, owned_definition};
pub use lane::{VectorArtefacts, stage_native_generation};
pub use readiness::{committed_native_ready, mounted_native_definition, mounted_native_ready};
pub use rows::{Partition, UnitProbe};
pub use shortlist::{Budget, Cut, Round, Shortlist, declined};
