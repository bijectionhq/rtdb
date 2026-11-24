mod definition;
mod lane;
mod nearest;
mod postings;
mod probe;
mod range_matches;
mod read;

pub use definition::path_geometry_encoding;
use rtdb_expr::spatial as kernel;
#[cfg(test)]
use rtdb_expr::spatial::bijection_geometry;
pub use rtdb_expr::spatial::{Classification, Predicate};
pub use lane::SpatialArtefacts;
pub use nearest::PreparedNearest;
pub use probe::Probe;
pub use read::{NearestPosition, Page, PageOrder, PreparedProbe, PreparedRange, PreparedRegion, Request, committed_native_ready, gather_document, open_probe, path_ready, read_held, read_prepared, read_probe, require_path_ready};
pub use rtdb_query::OwnedSpatialProjection as Projection;

#[cfg(test)]
mod tests;
