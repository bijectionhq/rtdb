// Generated from
// npm-packages/geometry/src/bounds.json. Do not edit.
//! The bounds and shared constants of the geometry profile, its operations,
//! movement, formats, cells and XZ indexing; the TypeScript and Python twins
//! are generated from the same source.

pub const GEOMETRY_STANDARD_BYTES: usize = 65_536;
pub const GEOMETRY_STANDARD_COORDINATES: usize = 512;
pub const GEOMETRY_STANDARD_RINGS: usize = 32;
pub const GEOMETRY_STANDARD_RING_COORDINATES: usize = 512;
pub const GEOMETRY_LARGE_BYTES: usize = 2_097_152;
pub const GEOMETRY_LARGE_COORDINATES: usize = 40_960;
pub const GEOMETRY_LARGE_RINGS: usize = 1024;
pub const GEOMETRY_LARGE_RING_COORDINATES: usize = 8192;
pub const GEOMETRY_HEIGHT_LIMIT_METRES: usize = 50_000_000;
pub const GEOMETRY_EARTH_RADIUS_METRES: f64 = 6_371_008.8;
pub const GEOMETRY_TOLERANCE_DEGREES: f64 = 1e-10;
pub const GEOMETRY_OPS_MEMBERS: usize = 256;
pub const GEOMETRY_OPS_INPUT_COORDINATES: usize = 16_384;
pub const GEOMETRY_OPS_VERTICES: usize = 510;
pub const GEOMETRY_OPS_SEGMENTS_PER_QUADRANT: usize = 32;
pub const GEOMETRY_OPS_WORKING_COORDINATES: usize = 65_536;
pub const GEOMETRY_OPS_LOCAL_FRAME_METRES: usize = 490_000;
pub const GEOMETRY_OPS_SUBDIVIDE_PARTS: usize = 4096;
pub const GEOMETRY_WORK_STEPS: usize = 8_000_000;
pub const GEOMETRY_WORK_ALLOCATED_BYTES: usize = 16_777_216;
pub const MOVEMENT_TRACK_FIXES: usize = 10_000;
pub const MOVEMENT_MAX_GAP_MS: usize = 86_400_000;
pub const MOVEMENT_SIMPLIFIED_POINTS: usize = 10_000;
pub const FORMATS_TEXT_BYTES: usize = 8_388_608;
pub const FORMATS_KMZ_BYTES: usize = 8_388_608;
pub const FORMATS_KML_BYTES: usize = 16_777_216;
pub const FORMATS_GEOMETRIES: usize = 10_000;
pub const FORMATS_DEPTH: usize = 64;
pub const FORMATS_GEOBUF_BYTES: usize = 1_048_576;
pub const CELLS_GEOHASH_MAX_PRECISION: usize = 9;
pub const CELLS_H3_MAX_RESOLUTION: usize = 15;
pub const CELLS_H3_MAX_AREA_RESOLUTION: usize = 14;
pub const XZ_MIN_RESOLUTION: usize = 1;
pub const XZ_MAX_RESOLUTION: usize = 30;
