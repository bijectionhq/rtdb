mod bijection_geometry;
mod definition;
mod kernel;
mod lane;
mod nearest;
mod postings;
mod read;

pub use kernel::{Classification, Predicate};
pub use lane::SpatialArtefacts;
pub use nearest::PreparedNearest;
pub use read::{Hit, NearestPosition, Page, PageOrder, PreparedRegion, Request, path_ready, read_held, read_prepared, require_path_ready};
pub use rtdb_query::OwnedSpatialProjection as Projection;

#[cfg(test)]
mod tests;
