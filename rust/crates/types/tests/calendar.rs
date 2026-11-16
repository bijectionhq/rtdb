use rtdb_types::{CivilDate as Date, CivilDateTime as DateTime, FixedOffset, Instant, ZoneId, MAX_EPOCH_DAY, MIN_EPOCH_DAY, NANOS_PER_DAY, TZDB_SHA256, TZDB_VERSION};

#[test]
fn calendar_serde_uses_validated_iso_dates_and_retains_wide_admission_values() {
    let date = Date::from_ymd(280_000,2,29).unwrap();
    assert_eq!(serde_json::to_string(&date).unwrap(),"\"+280000-02-29\"");
    assert_eq!(serde_json::from_str::<Date>("\"+280000-02-29\"").unwrap(),date);
    assert!(serde_json::from_str::<Date>("\"+280001-02-29\"").is_err());
    let value = rtdb_types::Value::calendar_date(Date::MAX,rtdb_types::DateUnit::Day);
    let encoded = serde_json::to_string(&value).unwrap();
    assert_eq!(serde_json::from_str::<rtdb_types::Value>(&encoded).unwrap(),value);
}

fn local(year: i32, month: u32, day: u32, hour: u64, minute: u64) -> DateTime {
    DateTime::new(Date::from_ymd(year, month, day).unwrap(), (hour * 3600 + minute * 60) * 1_000_000_000).unwrap()
}

#[test]
fn java_calendar_extrema_and_four_hundred_year_cycles() {
    assert_eq!(Date::from_ymd(-999_999_999, 1, 1).unwrap().epoch_day(), MIN_EPOCH_DAY);
    assert_eq!(Date::from_ymd(999_999_999, 12, 31).unwrap().epoch_day(), MAX_EPOCH_DAY);
    assert!(Date::from_epoch_day(MIN_EPOCH_DAY - 1).is_err());
    assert!(Date::from_epoch_day(MAX_EPOCH_DAY + 1).is_err());
    for year in [-999_999_999, -999_999_600, -400_000, -400, -1, 0, 1, 400, 300_000, 999_999_600, 999_999_999] {
        for month in 1..=12 {
            for day in [1, Date::month_length(year, month)] {
                let date = Date::from_ymd(year, month, day).unwrap();
                assert_eq!(Date::from_epoch_day(date.epoch_day()).unwrap().ymd(), (year, month, day));
                assert_eq!(Date::parse_iso(&date.to_string()).unwrap(), date);
            }
        }
    }
    assert_eq!(Date::from_ymd(300_000, 2, 29).unwrap().plus_months(12).unwrap().ymd(), (300_001, 2, 28));
    assert!(Date::from_ymd(300_100, 2, 29).is_err());
}

#[test]
fn strict_extended_year_spelling_and_subsecond_epoch_flooring() {
    for text in ["0000-01-01", "-0001-01-01", "+300000-02-29", "-999999999-01-01", "+999999999-12-31"] {
        assert_eq!(Date::parse_iso(text).unwrap().to_string(), text);
    }
    for text in ["-0000-01-01", "+0001-01-01", "300000-01-01", "+1000000000-01-01", "2001-02-29"] { assert!(Date::parse_iso(text).is_err(), "{text}"); }
    let previous = DateTime::from_epoch_nanos(-1).unwrap();
    assert_eq!(previous.date().to_string(), "1969-12-31");
    assert_eq!(previous.nano_of_day(), NANOS_PER_DAY - 1);
    assert_eq!(previous.epoch_nanos(), -1);
    assert_eq!(DateTime::from_epoch_nanos(local(300_000, 7, 1, 12, 0).epoch_nanos()).unwrap(), local(300_000, 7, 1, 12, 0));
}

#[test]
fn pinned_tzdb_preserves_future_recurring_rules_and_pre_history_offset() {
    if std::option_env!("RTDB_TZDB_PATH").is_none() {
        assert_eq!(TZDB_VERSION, "2025b");
        assert_eq!(TZDB_SHA256, "7be2a3adf10ff841579629f7725b15f0db41e8856527ec72847b5477073a3d29");
    }
    let paris = ZoneId::parse("Europe/Paris").unwrap();
    for year in [2100, 2400, 300_000, 999_999_999] {
        assert_eq!(paris.offset_at(local(year, 7, 1, 0, 0).epoch_second()).unwrap().seconds(), 7200);
        assert_eq!(paris.offset_at(local(year, 1, 1, 0, 0).epoch_second()).unwrap().seconds(), 3600);
    }
    assert_eq!(paris.offset_at(local(-300_000, 1, 1, 0, 0).epoch_second()).unwrap().seconds(), 561);
}

#[test]
fn gaps_overlaps_and_skipped_days_follow_java_zone_resolution() {
    let paris = ZoneId::parse("Europe/Paris").unwrap();
    let gap = local(2024, 3, 31, 2, 30);
    let resolved = paris.resolve_local(gap, None).unwrap();
    assert_eq!(paris.local_datetime(resolved).unwrap(), local(2024, 3, 31, 3, 30));
    let overlap = local(2024, 10, 27, 2, 30);
    let early = paris.resolve_local(overlap, None).unwrap();
    let late = paris.resolve_local(overlap, Some(FixedOffset::from_seconds(3600).unwrap())).unwrap();
    assert_eq!(late.seconds() - early.seconds(), 3600);
    let apia = ZoneId::parse("Pacific/Apia").unwrap();
    let start = apia.start_of_day(Date::from_ymd(2011, 12, 30).unwrap()).unwrap();
    assert_eq!(apia.local_datetime(Instant::new(start, 0).unwrap()).unwrap(), local(2011, 12, 31, 0, 0));
    let kathmandu = ZoneId::parse("Asia/Kathmandu").unwrap();
    let start = kathmandu.start_of_day(Date::from_ymd(1986, 1, 1).unwrap()).unwrap();
    assert_eq!(kathmandu.local_datetime(Instant::new(start, 0).unwrap()).unwrap(), local(1986, 1, 1, 0, 15));
}

#[test]
fn fixed_offsets_preserve_seconds_and_normalized_prefix_identity() {
    for (input, normalized, seconds) in [("+1", "+01:00", 3600), ("-013045", "-01:30:45", -5445), ("GMT+013045", "GMT+01:30:45", 5445), ("UTC-00", "UTC", 0), ("UT+18", "UT+18:00", 64800)] {
        let zone = ZoneId::parse(input).unwrap();
        assert_eq!(zone.to_string(), normalized);
        assert_eq!(zone.offset_at(i64::MAX).unwrap().seconds(), seconds);
    }
    for text in ["+18:00:01", "-19", "Europe/paris", "UTC+1:00", "PST"] { assert!(ZoneId::parse(text).is_err(), "{text}"); }
}
