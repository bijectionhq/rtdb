use rtdb_expr::{EvalContext,query_clock_instant,scalar_call};
use rtdb_types::{Instant,TimeUnit as U,Timestamp,Value as V};

#[test]
fn exact_query_clock_drives_all_temporal_precisions_without_microsecond_roundtrip() {
    let clock = Instant::new(1,123_456_789).unwrap();
    let context = EvalContext { current_time: clock,timezone: "+01:00",..Default::default() };
    for (precision,unit,expected) in [(0,U::Second,1),(1,U::Millisecond,1100),(2,U::Millisecond,1120),(3,U::Millisecond,1123),(4,U::Microsecond,1_123_400),(5,U::Microsecond,1_123_450),(6,U::Microsecond,1_123_456),(7,U::Nanosecond,1_123_456_700),(8,U::Nanosecond,1_123_456_780),(9,U::Nanosecond,1_123_456_789)] {
        assert_eq!(scalar_call("current_timestamp",&[V::I64(precision)],&context).unwrap(),V::Timestamp { value: expected,unit,timezone: Some("UTC".into()) });
        assert_eq!(scalar_call("local_timestamp",&[V::I64(precision)],&context).unwrap(),V::Timestamp { value: expected+3600*unit.units_per_second(),unit,timezone: None });
        assert_eq!(scalar_call("current_time",&[V::I64(precision)],&context).unwrap(),V::Time { value: expected,unit,bit_width: if precision<=3 {32}else{64} });
        assert_eq!(scalar_call("local_time",&[V::I64(precision)],&context).unwrap(),V::Time { value: expected+3600*unit.units_per_second(),unit,bit_width: if precision<=3 {32}else{64} });
    }
    let context = EvalContext { current_time: Instant::new(-1,999_999_999).unwrap(),..Default::default() };
    assert_eq!(scalar_call("current_timestamp",&[V::I64(9)],&context).unwrap(),V::Timestamp { value: -1,unit: U::Nanosecond,timezone: Some("UTC".into()) });
    assert_eq!(scalar_call("current_time",&[V::I64(9)],&context).unwrap(),V::Time { value: 86_399_999_999_999,unit: U::Nanosecond,bit_width: 64 });
    assert_eq!(context.current_time.as_micros().unwrap(),Timestamp(-1));
}

#[test]
fn query_clock_parameters_keep_units_and_distinguish_local_objects_from_string_fallback() {
    let instant = Instant::new(-1,999_999_999).unwrap();
    assert_eq!(query_clock_instant(&V::Timestamp { value: -1,unit: U::Nanosecond,timezone: Some("UTC".into()) },"Europe/Paris").unwrap(),instant);
    assert_eq!(query_clock_instant(&V::Timestamp { value: 3_600_000_000_001,unit: U::Nanosecond,timezone: None },"Europe/Paris").unwrap(),Instant::new(0,1).unwrap());
    assert_eq!(query_clock_instant(&V::Utf8("1970-01-01T01:00:00.000000001[Europe/Paris]".into()),"UTC").unwrap(),Instant::new(0,1).unwrap());
    assert_eq!(query_clock_instant(&V::Utf8("1970-01-01T01:00:00.000000001".into()),"UTC").unwrap(),Instant::default());
    assert_eq!(query_clock_instant(&V::DateDay(0),"+01:00").unwrap(),Instant::new(-3600,0).unwrap());
}

#[test]
fn object_dates_resolve_midnight_through_a_gap_instead_of_using_first_valid_time() {
    let date = rtdb_types::CivilDate::parse_iso("1919-03-31").unwrap();
    let expected = rtdb_types::parse_sql_timestamp_instant("1919-03-31T05:00Z","UTC").unwrap();
    for value in [V::calendar_date(date,rtdb_types::DateUnit::Day),V::DateDay(date.epoch_day() as i32),V::DateMilli(date.epoch_day()*86_400_000)] {
        assert_eq!(query_clock_instant(&value,"America/Toronto").unwrap(),expected);
        assert_eq!(rtdb_expr::timestamp_micros(&value,"America/Toronto").unwrap(),expected.as_micros().unwrap());
    }
}
