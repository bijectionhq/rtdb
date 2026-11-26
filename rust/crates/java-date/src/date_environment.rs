use std::{collections::BTreeMap,sync::Arc};
use rtdb_types::{LegacyTimeZone,MemoryBudget,MemoryReservation,Result};
use super::{date_locale::{LocaleData,LocaleId},date_names::{FieldNames,NameField},date_number::DateNumber,error};

/// Caller-owned wall clock used synchronously when a formatter or timezone name is constructed.
/// Implementations return signed epoch milliseconds immediately, are thread-safe, and must not block or start asynchronous work.
pub trait DateClock: Send + Sync { fn now_millis(&self) -> i64; }
enum DateClocks { Fixed { observed_at: i64,constructed_at: i64 },Live(Arc<dyn DateClock>) }

/// Explicit process-format locale and timezone inputs for legacy date objects.
/// Locale fields correspond to Java's FORMAT category; Unicode keys are the two-character keys such as `ca`, `nu`, `fw`, `rg`, and `tz`.
/// All clocks are raw epoch milliseconds: process initialization, CLDR name observation, and formatter construction.
/// No field is derived from the SQL session timezone.
pub struct DateEnvironmentInput {
    pub language: String,
    pub script: String,
    pub region: String,
    pub variant: String,
    pub unicode: BTreeMap<String,String>,
    pub timezone: String,
    pub initialized_at: i64,
    pub observed_at: i64,
    pub constructed_at: i64,
}

/// Immutable locale/provider data shared by a group of passive formatters.
/// Construction reserves retained and temporary native allocations before resolving locale data.
/// Formatting reads these captured values without changing process locale or timezone state.
pub struct DateEnvironment {
    pub(super) locale: LocaleData,
    pub(super) number: DateNumber,
    pub(super) names: [FieldNames;5],
    pub(super) default_zone: LegacyTimeZone,
    pub(super) calendar_zone: LegacyTimeZone,
    clocks: DateClocks,
    charge: Arc<MemoryReservation>,
}
impl DateEnvironment {
    /// Captures the process FORMAT locale and default timezone inside the supplied owner's joined I/O region.
    /// No process locale is changed; cancelled callers must leave region draining to the database owner before releasing that database's name.
    /// `initialized_at` is the provider's initialization time; `clock` is retained for subsequent formatter pivots and CLDR observations.
    /// Capture samples the clock inside admitted I/O for host timezone fallback; errors release all capture/decoding reservations.
    /// Linux and macOS capture are supported; other hosts must supply explicit inputs.
    pub async fn capture(io: &Arc<rtdb_storage::IoRegion>,memory: &Arc<MemoryBudget>,initialized_at: i64,clock: Arc<dyn DateClock>) -> Result<Arc<Self>> {
        let memory = Arc::clone(memory);
        io.run_blocking(move || Ok(Self::capture_process(&memory,initialized_at,clock)))
            .await.map_err(rtdb_types::Anomaly::from)?
    }
    /// Captures the process defaults synchronously, with the same locale and clock policy as `capture`.
    /// The caller owns the blocking call. Capture and construction are admitted to `memory`.
    pub fn capture_process(memory: &Arc<MemoryBudget>,initialized_at: i64,clock: Arc<dyn DateClock>) -> Result<Arc<Self>> {
        let _scratch = memory.reserve(4 << 20)?;
        let observed_at = clock.now_millis();
        let input = super::date_platform::capture(initialized_at,observed_at,observed_at)?;
        Self::build(input,memory,DateClocks::Live(clock))
    }
    /// Consumes explicitly captured inputs and returns a shared environment retaining its allocation charges.
    /// Locale/provider errors and exhausted memory are returned before publishing any environment.
    /// Invalid legacy timezone identifiers follow Java's GMT fallback; unsupported Unicode timezone keys leave the process default unchanged.
    /// Combined text input is limited to 4096 bytes; locale components and extensions together are limited to 1024 ASCII bytes.
    /// Fixed observation and construction timestamps are reused for every formatter built from this environment.
    pub fn from_inputs(input: DateEnvironmentInput,memory: &Arc<MemoryBudget>) -> Result<Arc<Self>> {
        let clocks = DateClocks::Fixed { observed_at: input.observed_at,constructed_at: input.constructed_at };
        Self::build(input,memory,clocks)
    }
    fn build(input: DateEnvironmentInput,memory: &Arc<MemoryBudget>,clocks: DateClocks) -> Result<Arc<Self>> {
        let size = [&input.language,&input.script,&input.region,&input.variant,&input.timezone].into_iter()
            .chain(input.unicode.iter().flat_map(|(key,value)|[key,value])).try_fold(0usize,|size,value|size.checked_add(value.len()).ok_or_else(overflow))?;
        if size > 4096 { return Err(error("invalid-date-locale","Date environment exceeds its input byte bound")); }
        let bound = size.checked_mul(size).and_then(|size|size.checked_mul(128)).and_then(|size|size.checked_add(1 << 20)).ok_or_else(overflow)?;
        let locale_charge = memory.reserve(bound)?;
        let locale = LocaleData::new(LocaleId::from_parts(&input.language,&input.script,&input.region,&input.variant,input.unicode)?)?;
        let number = DateNumber::new(&locale)?;
        let names = [NameField::Era,NameField::Year,NameField::Month,NameField::Weekday,NameField::AmPm].map(|field|FieldNames::new(&locale,field));
        let default_zone = LegacyTimeZone::parse(&input.timezone)?;
        let calendar_zone = locale.locale.extensions.get("tz").and_then(|key|super::locale_data::canonical_zone(key)).map_or(Ok(default_zone),LegacyTimeZone::parse)?;
        Ok(Arc::new(Self { locale,number,names,default_zone,calendar_zone,clocks,charge: locale_charge }))
    }
    pub(super) fn names(&self,field: NameField) -> &FieldNames {
        &self.names[match field { NameField::Era => 0,NameField::Year => 1,NameField::Month => 2,NameField::Weekday => 3,NameField::AmPm => 4 }]
    }
    pub(super) fn memory(&self) -> &Arc<MemoryBudget> { self.charge.budget() }

    /// Reserves memory in the environment's budget for state retained by a caller.
    pub fn reserve(&self, bytes: usize) -> Result<Arc<MemoryReservation>> {
        self.charge.budget().reserve(bytes)
    }
    pub(super) fn constructed_at(&self) -> i64 { match &self.clocks { DateClocks::Fixed { constructed_at,.. } => *constructed_at,DateClocks::Live(clock) => clock.now_millis() } }
    pub(super) fn observed_at(&self) -> i64 { match &self.clocks { DateClocks::Fixed { observed_at,.. } => *observed_at,DateClocks::Live(clock) => clock.now_millis() } }
}
fn overflow() -> rtdb_types::Anomaly { error("allocation-overflow","Date environment allocation exceeds range") }

#[cfg(test)]
mod tests {
    use super::*;
    fn input() -> DateEnvironmentInput { DateEnvironmentInput { language: "en".into(),script: String::new(),region: "US".into(),variant: String::new(),unicode: BTreeMap::new(),timezone: "Europe/Paris".into(),initialized_at: 1_600_000_000_000,observed_at: 1_600_000_000_000,constructed_at: 1_600_000_000_000 } }
    #[test]
    fn environment_clones_share_retained_provider_data_and_release_their_charge() {
        let memory = MemoryBudget::new(256 << 20);
        let environment = DateEnvironment::from_inputs(input(),&memory).unwrap();
        let retained = memory.used_bytes();assert!(retained > 0);
        let clone = Arc::clone(&environment);drop(environment);assert_eq!(memory.used_bytes(),retained);
        assert_eq!(clone.default_zone.to_string(),"Europe/Paris");
        drop(clone);assert_eq!(memory.used_bytes(),0);
    }
    #[test]
    fn locale_calendar_timezone_override_keeps_process_default_separate() {
        let memory = MemoryBudget::new(256 << 20);let mut input = input();input.unicode.insert("tz".into(),"usnyc".into());
        let environment = DateEnvironment::from_inputs(input,&memory).unwrap();
        assert_eq!(environment.default_zone.to_string(),"Europe/Paris");
        assert_eq!(environment.calendar_zone.to_string(),"America/New_York");
    }
    #[test]
    fn live_clock_is_observed_when_each_formatter_is_constructed() {
        struct Clock(std::sync::atomic::AtomicI64);
        impl DateClock for Clock { fn now_millis(&self) -> i64 { self.0.load(std::sync::atomic::Ordering::SeqCst) } }
        let midnight = |year|rtdb_types::CivilDate::from_ymd(year,6,18).unwrap().epoch_day() * 86_400_000;
        let clock = Arc::new(Clock(std::sync::atomic::AtomicI64::new(midnight(2020))));
        let mut input = input();input.timezone = "UTC".into();
        let environment = DateEnvironment::build(input,&MemoryBudget::new(256 << 20),DateClocks::Live(clock.clone())).unwrap();
        let mut first = super::super::date_pattern::DatePattern::compile("yy-MM-dd",Arc::clone(&environment),true).unwrap();
        clock.0.store(midnight(2021),std::sync::atomic::Ordering::SeqCst);
        let mut second = super::super::date_pattern::DatePattern::compile("yy-MM-dd",environment,true).unwrap();
        assert_eq!(first.read("40-06-18").unwrap(),midnight(1940));
        assert_eq!(second.read("40-06-18").unwrap(),midnight(2040));
    }
}
