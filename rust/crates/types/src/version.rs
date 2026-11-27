use std::sync::OnceLock;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct BuildInfo {
    pub schema_version: u32,
    pub engine: &'static str,
    pub version: &'static str,
    pub revision: Option<&'static str>,
    pub tzdb_version: &'static str,
    pub tzdb_sha256: &'static str,
}

/// Identifies the compiled artifact and selected time-zone data, independently of runtime version overrides.
/// The full revision is absent when the build did not supply `RTDB_REVISION`.
pub fn compiled_build_info() -> BuildInfo {
    BuildInfo { schema_version: 1, engine: "rust", version: env!("CARGO_PKG_VERSION"), revision: option_env!("RTDB_REVISION"), tzdb_version: crate::TZDB_VERSION, tzdb_sha256: crate::TZDB_SHA256 }
}

macro_rules! xtdb_compatible_version { () => { "2.x-SNAPSHOT" } }

/// The reference version this engine is compatible with: what the reference's own build of the tracked `main` reports.
pub const XTDB_COMPATIBLE_VERSION: &str = xtdb_compatible_version!();

/// The version reported without an `XTDB_VERSION` override: the compatible reference version, with this engine's package version as build metadata so that version comparisons ignore it.
pub const DEFAULT_RUNTIME_VERSION: &str = concat!(xtdb_compatible_version!(), "+rtdb.", env!("CARGO_PKG_VERSION"));

struct Version { number: String, revision: Option<String> }

impl Version {
    fn from_sources(number: Option<&str>, revision: Option<&str>, embedded_revision: Option<&str>) -> Self {
        Self {
            number: number.map(str::trim).filter(|value| !value.is_empty()).unwrap_or(DEFAULT_RUNTIME_VERSION).to_owned(),
            revision: [revision, embedded_revision].into_iter().flatten().map(str::trim).find(|value| !value.is_empty() && *value != "unknown").map(|value| value.chars().take(7).collect()),
        }
    }
}

static VERSION: OnceLock<Version> = OnceLock::new();

fn selected() -> &'static Version { VERSION.get_or_init(|| Version::from_sources(None, None, option_env!("RTDB_REVISION"))) }

/// Installs the process version from the server's startup `XTDB_VERSION` and `GIT_SHA` overrides, before anything reports it.
/// - Errors: the version was already reported or installed with different values.
pub fn install_runtime_version(xtdb_version: Option<&str>, git_sha: Option<&str>) -> crate::Result<()> {
    let version = Version::from_sources(xtdb_version, git_sha, option_env!("RTDB_REVISION"));
    let installed = VERSION.get_or_init(|| Version { number: version.number.clone(), revision: version.revision.clone() });
    if installed.number == version.number && installed.revision == version.revision { Ok(()) }
    else { Err(crate::Anomaly::fault("rtdb.version/already-selected", "The process version was reported before its overrides were installed")) }
}

/// Returns the process version, shared by SQL version functions and the native command line:
/// the installed `XTDB_VERSION` override, else `DEFAULT_RUNTIME_VERSION`.
pub fn runtime_version() -> &'static str { &selected().number }

/// Returns up to seven characters of the installed `GIT_SHA` override or build-time `RTDB_REVISION`.
/// Absent, blank and `unknown` candidates are skipped.
pub fn runtime_revision() -> Option<&'static str> { selected().revision.as_deref() }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_version_is_the_compatible_reference_version_with_the_engine_as_build_metadata() {
        assert_eq!(DEFAULT_RUNTIME_VERSION, format!("{XTDB_COMPATIBLE_VERSION}+rtdb.{}", env!("CARGO_PKG_VERSION")));
    }

    #[test]
    fn runtime_metadata_uses_authoritative_overrides_and_native_package_fallback() {
        let selected = Version::from_sources(Some(" release-version \n"), Some(" c84f9334eb060ceb92d28ecdd4a06e6b625932fd "), Some("embedded"));
        assert_eq!(selected.number, "release-version");
        assert_eq!(selected.revision.as_deref(), Some("c84f933"));
        for empty in [None, Some(""), Some(" \n")] {
            assert_eq!(Version::from_sources(empty, None, None).number, DEFAULT_RUNTIME_VERSION);
        }
        for missing in [None, Some(""), Some(" \n"), Some("unknown")] {
            assert!(Version::from_sources(None, missing, None).revision.is_none());
            assert_eq!(Version::from_sources(None, missing, Some("embedded")).revision.as_deref(), Some("embedde"));
        }
        assert_eq!(Version::from_sources(None, Some("abc"), None).revision.as_deref(), Some("abc"));
    }

    #[test]
    fn the_installed_overrides_are_the_reported_version_and_cannot_change() {
        install_runtime_version(Some("installed-version"), Some("0123456789")).unwrap();
        install_runtime_version(Some(" installed-version "), Some("0123456")).unwrap();
        assert_eq!(runtime_version(), "installed-version");
        assert_eq!(runtime_revision(), Some("0123456"));
        assert!(install_runtime_version(None, None).is_err());
    }
}
