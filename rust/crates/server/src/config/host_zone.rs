use std::path::Path;

/// The host's time zone, found where the JVM's `ZoneId.systemDefault()` looks for it on Unix:
/// the `TZ` environment variable, then `/etc/timezone`, then the zone `/etc/localtime` links to.
pub(crate) fn host_timezone() -> String {
    let tz = std::env::var("TZ").ok();
    let etc_timezone = if cfg!(target_os = "linux") { std::fs::read_to_string("/etc/timezone").ok() } else { None };
    let localtime = std::fs::read_link("/etc/localtime").ok();
    host_timezone_from(tz.as_deref(), etc_timezone.as_deref(), localtime.as_deref())
}

/// The first of the candidates that names a zone, normalized as a session zone; `UTC` when none does.
pub fn host_timezone_from(tz: Option<&str>, etc_timezone: Option<&str>, localtime: Option<&Path>) -> String {
    let tz = tz.map(|tz| tz.strip_prefix(':').unwrap_or(tz)).and_then(|tz| match tz.starts_with('/') {
        true => zoneinfo_name(Path::new(tz)),
        false => Some(tz.to_owned()),
    });
    let etc_timezone = etc_timezone.and_then(|text| text.lines().next()).map(str::trim).map(str::to_owned);
    [tz, etc_timezone, localtime.and_then(zoneinfo_name)].into_iter().flatten()
        .filter(|zone| !zone.is_empty())
        .find_map(|zone| rtdb_expr::normalize_timezone(&zone).ok())
        .unwrap_or_else(|| "UTC".into())
}

fn zoneinfo_name(path: &Path) -> Option<String> {
    let path = path.to_str()?;
    let (_, name) = path.rsplit_once("zoneinfo/")?;
    Some(name.to_owned())
}
