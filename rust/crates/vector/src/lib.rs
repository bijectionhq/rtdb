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
mod rows;

pub use accelerate::{PREDICATE, VectorAccelerator};
pub use definition::{Approximate, DEFINITIONS, KIND, Method, VectorDefinition, definitions_in};
pub use lane::VectorArtefacts;
pub use rows::Partition;
