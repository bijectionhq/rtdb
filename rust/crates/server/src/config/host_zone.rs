use std::{path::Path, time::{SystemTime, UNIX_EPOCH}};
use rtdb_types::{LegacyTimeZone, Result};

/// The host's time zone, as XTDB's pgwire server takes it from `Clock/systemDefaultZone`:
/// the startup environment's `TZ`, else the zone `/etc/localtime` links to or is a copy of under `/usr/share/zoneinfo`.
pub(crate) fn host_timezone(environment: &impl Fn(&str) -> Option<String>) -> Result<String> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX));
    host_timezone_from(environment("TZ").as_deref(), Path::new("/etc/localtime"), Path::new("/usr/share/zoneinfo"), now)
}

/// The session zone the JVM's default `TimeZone` converts to with `toZoneId`, for the given `TZ`, `localtime` file and `zoneinfo` directory.
/// No source gives `GMT`; an ID the JVM does not know gives the host's `GMT±hh:mm` offset at `now` (epoch milliseconds).
/// - Errors: zone files beyond the bounded capture limits, or a legacy custom offset outside ±18 hours.
pub fn host_timezone_from(tz: Option<&str>, localtime: &Path, zoneinfo: &Path, now: i64) -> Result<String> {
    let id = rtdb_java_date::default_timezone_id(tz, localtime, zoneinfo, now)
        .map_err(|error| super::invalid(format!("Cannot determine the host time zone: {error}")))?;
    Ok(LegacyTimeZone::parse(&id)?.to_zone_id()?.to_string())
}
