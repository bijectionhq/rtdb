use std::{collections::BTreeMap, fmt::Write, sync::Arc};

use rtdb_types::{LegacyTimeZone, MemoryBudget, Result, java_date::{DateFields, MILLIS_PER_DAY}};

use super::{date_locale::{LocaleData, LocaleId}, date_platform, date_zone_names::ZoneDisplay};

const DAYS: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const MONTHS: [&str; 12] = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

/// Immutable `Date.toString` calendar and short timezone names for one default-zone observation.
pub struct DateDisplay {
    zone: LegacyTimeZone,
    names: ZoneDisplay,
}

impl DateDisplay {
    /// Captures the host default timezone, using raw epoch milliseconds for the fallback observation.
    /// The caller owns this blocking operation and the finite memory budget for locale resources.
    pub fn capture_process(observed_at: i64, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let _scratch = memory.reserve(4 << 20)?;
        let default_zone = date_platform::host_timezone(observed_at)?;
        Self::from_zone(LegacyTimeZone::parse(&default_zone)?, observed_at, memory)
    }

    /// Prepares short US-English display names for the selected legacy timezone.
    /// `observed_at` is the epoch millisecond observation used for CLDR fallback names.
    pub fn from_zone(zone: LegacyTimeZone, observed_at: i64, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let _locale = memory.reserve(1 << 20)?;
        let locale = LocaleData::new(LocaleId::from_parts("en", "", "US", "", BTreeMap::new())?)?;
        let names = ZoneDisplay::new(&locale, zone, &|| observed_at, memory)?;
        Ok(Self { zone, names })
    }

    /// Returns a bound in UTF-8 bytes for caller admission before formatting.
    pub fn maximum_output_bytes(&self) -> usize {
        64 + self.names.display(false, false).len().max(self.names.display(true, false).len())
    }

    /// Formats signed Date milliseconds in the captured default zone.
    /// The caller admits the output using `maximum_output_bytes`.
    pub fn format(&self, millis: i64) -> Result<String> {
        let (raw, daylight) = self.zone.offsets_at_millis(millis)?;
        let offset = raw + daylight;
        let clock = millis.rem_euclid(MILLIS_PER_DAY) + i64::from(offset);
        let day = millis.div_euclid(MILLIS_PER_DAY) + clock.div_euclid(MILLIS_PER_DAY);
        let fields = DateFields::from_instant(millis, offset);
        let weekday = DAYS[(day + 4).rem_euclid(7) as usize];
        let month = MONTHS[usize::from(fields.month) - 1];
        let name = self.names.display(daylight != 0, false);
        let mut output = String::with_capacity(self.maximum_output_bytes());
        write!(output, "{weekday} {month} {:02} {:02}:{:02}:{:02} {name} {}",
            fields.day, fields.hour, fields.minute, fields.second, fields.year_of_era())
            .expect("formatting into a String is infallible");
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_text_uses_english_names_both_sides_of_the_hybrid_cutover() {
        let memory = MemoryBudget::new(16 << 20);
        let printer = DateDisplay::from_zone(LegacyTimeZone::gmt(), 1_600_000_000_000, &memory).unwrap();
        assert_eq!(printer.format(-12_219_292_800_001).unwrap(), "Thu Oct 04 23:59:59 GMT 1582");
        assert_eq!(printer.format(-12_219_292_800_000).unwrap(), "Fri Oct 15 00:00:00 GMT 1582");
        assert_eq!(printer.format(-1).unwrap(), "Wed Dec 31 23:59:59 GMT 1969");
        assert_eq!(printer.format(0).unwrap(), "Thu Jan 01 00:00:00 GMT 1970");
        drop(printer);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn default_zone_names_follow_each_dates_daylight_state() {
        let memory = MemoryBudget::new(16 << 20);
        let printer = DateDisplay::from_zone(LegacyTimeZone::parse("America/New_York").unwrap(), 1_600_000_000_000, &memory).unwrap();
        assert_eq!(printer.format(0).unwrap(), "Wed Dec 31 19:00:00 EST 1969");
        let summer = printer.format(1_593_561_600_000).unwrap();
        assert_eq!(summer, "Tue Jun 30 20:00:00 EDT 2020");
        assert!(summer.len() <= printer.maximum_output_bytes());
    }

    #[test]
    fn readable_pattern_uses_format_locale_years_with_utc_clock_fields() {
        let memory = MemoryBudget::new(16 << 20);
        for (language, region, expected) in [
            ("en", "US", "1970-01-01T00:00:00.000-00:00"),
            ("th", "TH", "2513-01-01T00:00:00.000-00:00"),
        ] {
            let input = crate::DateEnvironmentInput {
                language: language.into(), script: String::new(), region: region.into(),
                variant: String::new(), unicode: BTreeMap::new(), timezone: "GMT+08:00".into(),
                initialized_at: 1_600_000_000_000, observed_at: 1_600_000_000_000, constructed_at: 1_600_000_000_000,
            };
            let environment = crate::DateEnvironment::from_inputs(input, &memory).unwrap();
            let mut pattern = crate::DatePattern::compile("yyyy-MM-dd'T'HH:mm:ss.SSS-00:00", environment, true).unwrap();
            assert_eq!(pattern.format(0).unwrap(), expected);
        }
        assert_eq!(memory.used_bytes(), 0);
    }
}
