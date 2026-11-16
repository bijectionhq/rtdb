use rtdb_types::{Result, ZoneId};

pub(crate) type Zone = ZoneId;
pub(crate) fn zone(timezone: &str) -> Result<Zone> { ZoneId::parse(timezone) }

/// Validates configured named zones and Java fixed-offset spellings, normalizing the latter for session output.
/// Invalid zones return `xtdb/invalid-timezone`.
pub fn normalize_timezone(timezone: &str) -> Result<String> { Ok(zone(timezone)?.to_string()) }
