use rtdb_types::{CivilDate,LegacyTimeZone,ZoneId};

fn millis(year: i32,month: u32,day: u32,hour: i64,minute: i64) -> i64 {
    CivilDate::from_ymd(year,month,day).unwrap().epoch_day() * 86_400_000 + hour * 3_600_000 + minute * 60_000
}

#[test]
fn legacy_timezones_use_final_raw_offset_before_the_historical_table() {
    let date = millis(1800,1,1,0,0);
    let paris = LegacyTimeZone::parse("Europe/Paris").unwrap();
    assert_eq!(paris.offset_at_millis(date).unwrap(),3_600_000);
    assert_eq!(ZoneId::parse("Europe/Paris").unwrap().offset_at(date / 1000).unwrap().seconds(),561);
    assert_eq!(paris.offset_at_millis(millis(1900,1,1,0,0)).unwrap(),561_000);
}

#[test]
fn legacy_wall_resolution_uses_standard_overlap_and_gap_offsets() {
    let paris = LegacyTimeZone::parse("Europe/Paris").unwrap();
    assert_eq!(paris.resolve_wall_millis(millis(2020,10,25,2,30)).unwrap(),millis(2020,10,25,1,30));
    assert_eq!(paris.resolve_wall_millis(millis(2020,3,29,2,30)).unwrap(),millis(2020,3,29,1,30));
    assert_eq!(paris.offsets_by_wall_millis(millis(2020,3,29,2,30)).unwrap(),(3_600_000,0));
    assert_eq!(paris.offsets_at_millis(millis(2020,3,29,1,30)).unwrap(),(3_600_000,3_600_000));
    assert_eq!(paris.resolve_wall_millis(millis(2200,10,26,2,30)).unwrap(),millis(2200,10,26,1,30));
}

#[test]
fn recurring_tail_and_signed64_date_endpoints_are_resolved_without_chrono() {
    let paris = LegacyTimeZone::parse("Europe/Paris").unwrap();
    assert_eq!(paris.offset_at_millis(millis(300_000,7,1,0,0)).unwrap(),7_200_000);
    assert_eq!(paris.offset_at_millis(millis(300_000,1,1,0,0)).unwrap(),3_600_000);
    assert_eq!(paris.offset_at_millis(i64::MIN).unwrap(),3_600_000);
    assert_eq!(paris.offset_at_millis(i64::MAX).unwrap(),3_600_000);
    let fixed = LegacyTimeZone::parse("GMT+23:59:59").unwrap();
    assert_eq!(fixed.offset_at_millis(i64::MAX).unwrap(),86_399_000);
    assert_eq!(fixed.resolve_wall_millis(i64::MIN).unwrap(),i64::MIN.wrapping_sub(86_399_000));
}

#[test]
fn custom_timezone_spelling_and_java25_short_aliases_preserve_reference_fallback() {
    for text in ["GMT+1","GMT+0100","GMT+1:00"] { assert_eq!(LegacyTimeZone::parse(text).unwrap().to_string(),"GMT+01:00"); }
    assert_eq!(LegacyTimeZone::parse("GMT-0").unwrap().to_string(),"GMT-00:00");
    assert_eq!(LegacyTimeZone::parse("GMT+1:02:03").unwrap().to_string(),"GMT+01:02:03");
    for text in ["GMT+","GMT+24","GMT+1:2","+01:00","unknown"] { assert_eq!(LegacyTimeZone::parse(text).unwrap().to_string(),"GMT"); }
    assert_eq!(LegacyTimeZone::parse("CST").unwrap().offset_at_millis(millis(2020,7,1,0,0)).unwrap(),-18_000_000);
    assert_eq!(LegacyTimeZone::parse("EST").unwrap().offset_at_millis(millis(1900,1,1,0,0)).unwrap(),LegacyTimeZone::parse("America/Panama").unwrap().offset_at_millis(millis(1900,1,1,0,0)).unwrap());
    assert_eq!(LegacyTimeZone::parse("EST").unwrap().to_zone_id().unwrap().to_string(),"-05:00");
    assert_eq!(LegacyTimeZone::parse("CST").unwrap().to_zone_id().unwrap().to_string(),"America/Chicago");
    assert!(LegacyTimeZone::parse("GMT+23:00").unwrap().to_zone_id().is_err());
}

#[test]
fn cairo_future_tail_keeps_the_last_thursday_midnight_transition() {
    let cairo = LegacyTimeZone::parse("Africa/Cairo").unwrap();
    assert_eq!(cairo.offset_at_millis(millis(2200,10,30,20,30)).unwrap(),10_800_000);
    assert_eq!(cairo.offset_at_millis(millis(2200,10,30,21,30)).unwrap(),7_200_000);
}

#[test]
fn legacy_offset_components_and_display_savings_use_distinct_time_bases() {
    let paris = LegacyTimeZone::parse("Europe/Paris").unwrap();
    assert_eq!(paris.offsets_at_millis(millis(1900,1,1,0,0)).unwrap(),(561_000,0));
    assert_eq!(paris.offsets_at_millis(millis(2020,7,1,0,0)).unwrap(),(3_600_000,3_600_000));
    assert_eq!(paris.offsets_at_millis(millis(2200,7,1,0,0)).unwrap(),(3_600_000,3_600_000));
    assert_eq!(paris.daylight_savings_millis(),3_600_000);
    assert_eq!(paris.raw_offset_at_millis(millis(1900,1,1,0,0),millis(1900,1,1,0,0)).unwrap(),561_000);
    assert_eq!(paris.raw_offset_at_millis(millis(2020,1,1,0,0),millis(1900,1,1,0,0)).unwrap(),3_600_000);
    let fixed = LegacyTimeZone::parse("GMT+01:02:03").unwrap();
    assert_eq!(fixed.offsets_at_millis(i64::MIN).unwrap(),(3_723_000,0));
    assert_eq!(fixed.daylight_savings_millis(),0);
}

#[test]
fn modern_display_offsets_retain_historical_savings_after_daylight_rules_end() {
    let paris = ZoneId::parse("Europe/Paris").unwrap();
    assert_eq!(paris.standard_offset_at(millis(1900,1,1,0,0) / 1000).unwrap().seconds(),561);
    assert_eq!(paris.standard_offset_at(millis(2020,7,1,0,0) / 1000).unwrap().seconds(),3600);
    assert_eq!(paris.historical_daylight_savings_before(millis(1900,1,1,0,0) / 1000).unwrap(),0);
    assert_eq!(paris.historical_daylight_savings_before(millis(2020,7,1,0,0) / 1000).unwrap(),3600);
    let phoenix = ZoneId::parse("America/Phoenix").unwrap();
    assert_eq!(phoenix.historical_daylight_savings_before(millis(2020,1,1,0,0) / 1000).unwrap(),3600);
    assert_eq!(phoenix.standard_offset_at(millis(2020,1,1,0,0) / 1000).unwrap().seconds(),-25_200);
    assert!(paris.standard_offset_at(i64::MAX).is_err());
    assert_eq!(ZoneId::utc().historical_daylight_savings_before(i64::MAX).unwrap(),0);
}
