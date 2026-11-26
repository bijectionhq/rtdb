use std::path::Path;
use rtdb_types::Result;

/// The host's time zone, found where the JVM's `ZoneId.systemDefault()` looks for it on Unix:
/// the startup environment's `TZ`, then `/etc/timezone`, then the zone `/etc/localtime` links to.
pub(crate) fn host_timezone(environment: &impl Fn(&str) -> Option<String>) -> Result<String> {
    let tz = environment("TZ");
    let etc_timezone = if cfg!(target_os = "linux") { std::fs::read_to_string("/etc/timezone").ok() } else { None };
    let localtime = std::fs::read_link("/etc/localtime").ok();
    host_timezone_from(tz.as_deref(), etc_timezone.as_deref(), localtime.as_deref())
}

/// The zone a non-empty `TZ` names, normalized as a session zone; without one, the first host file that names a zone, else `UTC`.
/// - Errors: a `TZ` that names no zone, as an unknown zone ID or a path outside a `zoneinfo` directory.
pub fn host_timezone_from(tz: Option<&str>, etc_timezone: Option<&str>, localtime: Option<&Path>) -> Result<String> {
    if let Some(tz) = tz.map(|tz| tz.strip_prefix(':').unwrap_or(tz)).filter(|tz| !tz.is_empty()) {
        let zone = match tz.starts_with('/') {
            true => zoneinfo_name(Path::new(tz)),
            false => Some(tz.to_owned()),
        };
        return zone.and_then(|zone| rtdb_expr::normalize_timezone(&zone).ok())
            .ok_or_else(|| super::invalid(format!("TZ names no time zone: '{tz}'")));
    }
    let etc_timezone = etc_timezone.and_then(|text| text.lines().next()).map(str::trim).map(str::to_owned);
    Ok([etc_timezone, localtime.and_then(zoneinfo_name)].into_iter().flatten()
        .filter(|zone| !zone.is_empty())
        .find_map(|zone| rtdb_expr::normalize_timezone(&zone).ok())
        .unwrap_or_else(|| "UTC".into()))
}

fn zoneinfo_name(path: &Path) -> Option<String> {
    let path = path.to_str()?;
    let (_, name) = path.rsplit_once("zoneinfo/")?;
    Some(name.to_owned())
}
