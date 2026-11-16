use rtdb_types::{Instant, Timestamp};

#[test]
fn iso_instant_covers_java_boundaries_and_full_microsecond_values() {
    for text in [
        "-1000000000-01-01T00:00:00Z", "+1000000000-12-31T23:59:59.999999999Z",
        "-999999999-01-01T00:00:00Z", "+999999999-12-31T23:59:59Z",
        "+300000-02-29T12:34:56.123456Z", "-300000-07-01T00:00:00.123Z",
        "0000-01-01T00:00:00Z", "1969-12-31T23:59:59.999999999Z",
    ] { assert_eq!(Instant::parse_iso(text).unwrap().to_iso_string().unwrap(), text); }
    for value in [i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX - 1, i64::MAX] {
        let instant = Instant::from(Timestamp(value));
        assert_eq!(Instant::parse_iso(&instant.to_iso_string().unwrap()).unwrap().as_micros().unwrap(), Timestamp(value));
    }
    for text in ["-1000000000-01-01T00:00:00+00:01", "+1000000001-01-01T00:00:00Z", "+1000000000-12-31T23:59:59-00:01"] { assert!(Instant::parse_iso(text).is_err(), "{text}"); }
}

#[test]
fn iso_instant_keeps_seconds_offset_case_and_leap_second_rules() {
    for (text, normalized) in [
        ("2020-12-31T24:00:00Z", "2021-01-01T00:00:00Z"),
        ("2016-12-31t23:59:60.123z", "2016-12-31T23:59:59.123Z"),
        ("1970-01-01T00:00:00.1+01:00:15", "1969-12-31T22:59:45.100Z"),
        ("1970-01-01T00:00:00.Z", "1970-01-01T00:00:00Z"),
        ("+1000000001-01-01T00:00:00+18:00", "+1000000000-12-31T06:00:00Z"),
    ] { assert_eq!(Instant::parse_iso(text).unwrap().to_iso_string().unwrap(), normalized); }
    for text in ["1970-01-01T00:00Z", "1970-01-01 00:00:00Z", "1970-01-01T00:00:00+01", "1970-01-01T00:00:60Z", "1970-01-01T24:00:00.1Z", "1970-01-01T00:00:00.1234567890Z"] { assert!(Instant::parse_iso(text).is_err(), "{text}"); }
}
