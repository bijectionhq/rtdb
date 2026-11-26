//! Native OpenJDK legacy Date formatting and process locale inputs.

mod date_calendar;
mod date_display;
mod date_environment;
mod date_locale;
mod date_names;
mod date_number;
mod date_pattern;
mod date_platform;
mod date_zone_names;
mod locale_data;

use rtdb_types::java_date as legacy_calendar;

pub use date_environment::{DateClock, DateEnvironment, DateEnvironmentInput};
pub use date_display::DateDisplay;
pub use date_pattern::DatePattern;
pub use date_platform::process_language;

/// SHA-256 of the verified Temurin 25 CLDR resource archive used to build the locale tables.
pub const LOCALE_DATA_SHA256: &str = locale_data::LOCALE_DATA_SHA256;

fn error(code: &str, message: impl std::fmt::Display) -> rtdb_types::Anomaly {
    rtdb_types::Anomaly::incorrect(format!("xtdb.kafka-connect-source/{code}"), message.to_string())
}
