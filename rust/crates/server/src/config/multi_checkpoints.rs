use rtdb_format::config::{Yaml, YamlFields};
use rtdb_types::Result;

use super::{empty, invalid};

/// How Multi writers checkpoint their journals, from `multiCheckpoints`.
/// Both modes write checkpoints that only this release's replay installs, so
/// a deployment enables them once every node runs it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MultiCheckpointSettings {
    /// `deferred`: builds journal-bound checkpoints off the commit path.
    pub deferred: bool,
    /// `binary`: writes a checkpoint's transaction state in CBOR.
    pub binary: bool,
}

/// The variables that set the modes before they were configuration keys.
/// Deprecated: read only when the YAML leaves the key out.
const DEFERRED_SETTING: &str = "RTDB_MULTI_DEFERRED_CHECKPOINTS";
const BINARY_SETTING: &str = "RTDB_MULTI_BINARY_CHECKPOINTS";

impl MultiCheckpointSettings {
    /// - Errors: an unknown key, a non-boolean value, or a deprecated variable
    ///   that is not `1`, `true`, `0`, `false` or empty.
    pub(crate) fn decode(value: Option<Yaml>, environment: &impl Fn(&str) -> Option<String>) -> Result<Self> {
        let mut fields = YamlFields::new(value.unwrap_or_else(empty), "multiCheckpoints")?;
        let deferred = fields.boolean("deferred")?;
        let binary = fields.boolean("binary")?;
        fields.finish()?;
        Ok(Self {
            deferred: deferred.map_or_else(|| deprecated_flag(DEFERRED_SETTING, environment), Ok)?,
            binary: binary.map_or_else(|| deprecated_flag(BINARY_SETTING, environment), Ok)?,
        })
    }
}

fn deprecated_flag(name: &str, environment: &impl Fn(&str) -> Option<String>) -> Result<bool> {
    let value = environment(name);
    let enabled = match value.as_deref() {
        None | Some("" | "0" | "false") => false,
        Some("1" | "true") => true,
        Some(_) => return Err(invalid(format!("{name} must be 1, true, 0 or false"))),
    };
    if value.is_some() {
        tracing::warn!(setting = name, "Deprecated: set the Multi checkpoint mode with the multiCheckpoints configuration key");
    }
    Ok(enabled)
}

#[cfg(test)]
mod tests {
    use rtdb_format::config::parse_yaml;

    use super::*;

    fn decode(yaml: &str, variables: &[(&str, &str)]) -> Result<MultiCheckpointSettings> {
        let environment = |name: &str| variables.iter().find(|(key, _)| *key == name).map(|(_, value)| (*value).to_owned());
        MultiCheckpointSettings::decode(Some(parse_yaml(yaml)?), &environment)
    }

    #[test]
    fn the_configuration_key_sets_the_modes_and_wins_over_the_environment() {
        assert_eq!(decode("{}", &[]).unwrap(), MultiCheckpointSettings::default());
        assert_eq!(decode("{deferred: true, binary: true}", &[]).unwrap(), MultiCheckpointSettings { deferred: true, binary: true });
        let settings = decode("{deferred: false}", &[(DEFERRED_SETTING, "1"), (BINARY_SETTING, "true")]).unwrap();
        assert_eq!(settings, MultiCheckpointSettings { deferred: false, binary: true });
    }

    #[test]
    fn malformed_settings_are_refused() {
        assert!(decode("{deferred: maybe}", &[]).is_err());
        assert!(decode("{compressed: true}", &[]).is_err());
        assert!(decode("{}", &[(BINARY_SETTING, "yes")]).is_err());
    }
}
