use rtdb_format::config::{Yaml, YamlFields};
use rtdb_types::{Anomaly, Result};

use super::empty;

/// How Multi writers treat their journals, from `multiJournal`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MultiJournalSettings {
    /// `holdWritesAfterRecovery`: after an operator recovers a journal that
    /// lost two of its three voters, refuse the writer role until a writer or
    /// an administrator acknowledges the recovery. By default the writer
    /// acknowledges it, records it in the journal and resumes.
    pub hold_writes_after_recovery: bool,
    pub immutable_streams: bool,
}

impl MultiJournalSettings {
    /// - Errors: an unknown key or a non-boolean value.
    pub(crate) fn decode(value: Option<Yaml>) -> Result<Self> {
        let mut fields = YamlFields::new(value.unwrap_or_else(empty), "multiJournal")?;
        let hold_writes_after_recovery = fields.boolean("holdWritesAfterRecovery")?.unwrap_or(false);
        let immutable_streams = fields.boolean("immutableStreams")?.unwrap_or(false);
        fields.finish()?;
        if hold_writes_after_recovery && immutable_streams {
            return Err(Anomaly::incorrect(
                "rtdb/multi-journal",
                "Immutable streams cannot be configured to acknowledge an in-place recovery",
            ));
        }
        Ok(Self { hold_writes_after_recovery, immutable_streams })
    }
}

#[cfg(test)]
mod tests {
    use rtdb_format::config::parse_yaml;

    use super::*;

    fn decode(yaml: &str) -> Result<MultiJournalSettings> {
        MultiJournalSettings::decode(Some(parse_yaml(yaml)?))
    }

    #[test]
    fn selects_immutable_streams_explicitly() {
        assert!(!decode("{}").unwrap().immutable_streams);
        assert!(decode("{immutableStreams: true}").unwrap().immutable_streams);
        assert!(decode("{immutableStreams: maybe}").is_err());
        assert!(decode("{immutableStreams: true, holdWritesAfterRecovery: true}").is_err());
    }

    #[test]
    fn writers_resume_after_a_recovery_unless_configured_to_hold() {
        assert_eq!(MultiJournalSettings::decode(None).unwrap(), MultiJournalSettings::default());
        assert!(!decode("{}").unwrap().hold_writes_after_recovery);
        assert!(decode("{holdWritesAfterRecovery: true}").unwrap().hold_writes_after_recovery);
        assert!(decode("{holdWritesAfterRecovery: maybe}").is_err());
        assert!(decode("{holdWrites: true}").is_err());
    }
}
