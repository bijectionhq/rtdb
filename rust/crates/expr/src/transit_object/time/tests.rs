use super::*;

fn read(tag: &str,text: &str) -> Time { Time::read(tag,&Object::String(text.into())).unwrap().unwrap() }

#[test]
fn canonical_temporal_text_retains_registered_object_kinds() {
    for (tag,input,output) in [
        ("time/period","p1y13m2w-1d","P1Y13M13D"),
        ("time/period","-P-1Y2M","P1Y-2M"),("time/period","P0Y","P0D"),
        ("time/duration","P1DT2H","PT26H"),("time/duration","PT-0.10S","PT-0.1S"),("time/duration","P1Dt","PT24H"),
        ("time/date","+10000-02-29","+10000-02-29"),
        ("time/time","01:02:00.100000","01:02:00.100"),("time/time","01:02:00.000100","01:02:00.000100"),
        ("time/time","01:02:00","01:02"),
        ("time/date-time","2023-02-31 24:00","2023-03-01T00:00"),
        ("time/date-time","2024-01-02T03:04+02[Europe/Paris]","2024-01-02T03:04"),
        ("time/offset-time","01:02:00z","01:02Z"),("time/offset-time","01:02:03+02:00:01","01:02:03+02:00:01"),
        ("time/instant","2024-01-02T03:04+02","2024-01-02T01:04:00Z"),
        ("time/instant","2024-01-02[UTC]","2024-01-02T00:00:00Z"),
        ("time/offset-date-time","2024-01-02T03:04+02[Europe/Paris]","2024-01-02T02:04+01:00"),
        ("time/zoned-date-time","2024-01-02T03:04+02[Europe/Paris]","2024-01-02T02:04+01:00[Europe/Paris]"),
        ("time/zoned-date-time","2024-01-02T03:04Z","2024-01-02T03:04Z"),
        ("time/zoned-date-time","2024-01-02[UTC]","2024-01-02T00:00Z[UTC]"),
        ("time/zone","UTC+00:00","UTC"),("time/zone","+1","+01:00"),
        ("time/year","+0001","1"),("time/year","-0","0"),("time/year-month","+10000-01","10000-01"),
        ("time/year-month","-0001-02","-0001-02"),("time/month-day","--02-29","--02-29"),
        ("time/day-of-week","MONDAY","MONDAY"),("time/month","JANUARY","JANUARY"),
        ("xtdb/interval","P1Y13M2W","P2Y1M14D"),("xtdb/interval","PT","P"),
        ("xtdb/interval","-P1Y2DT0.000000001S","P-1Y-2DT-0.000000001S"),
    ] {
        assert!(Time::recognizes(tag));
        assert_eq!(read(tag,input).wire().unwrap(),tagged(tag,T::String(output.into())),"{tag}: {input}");
    }
}

#[test]
fn strict_and_smart_temporal_handlers_reject_their_distinct_invalid_inputs() {
    for (tag,input) in [
        ("time/period","P"),("time/period","P2147483648Y"),("time/period","-P-2147483648Y"),("time/period","P306783379W"),
        ("time/duration","PT9223372036854775808S"),("time/duration","-PT-9223372036854775808S"),("time/duration","PT1ſ"),
        ("time/date","2023-02-29"),("time/date","10000-01-01"),("time/date","-0000-01-01"),
        ("time/time","24:00"),("time/time","23:59:60"),("time/time","01:02Z"),
        ("time/date-time","2024-01-02"),("time/date-time","2024-01-02t03:04"),
        ("time/instant","2024-01-02T03:04"),("time/instant","2024-01-02T03:04z"),
        ("time/offset-time","03:04+02"),("time/offset-time","03:04+0200"),("time/offset-time","24:00Z"),
        ("time/year","1234567890"),("time/year"," 1"),("time/year-month","2024-13"),("time/year-month","+0001-02"),
        ("time/month-day","--02-30"),("time/day-of-week","monday"),("time/month","January"),
        ("xtdb/interval","P2147483647Y"),("xtdb/interval","PT9223372037S"),("xtdb/interval","P1YY"),
    ] { assert!(Time::read(tag,&Object::String(input.into())).is_err(),"{tag}: {input}"); }
    for tag in ["time/date","time/year","time/month","xtdb/interval"] { assert!(Time::read(tag,&Object::Null).is_err()); }
    for input in ["garbage","p1d","P+1Y","P1DT0,1S"] { assert_eq!(Time::read("xtdb/interval",&Object::String(input.into())).unwrap(),None); }
}

#[test]
fn temporal_values_preserve_full_ranges_and_non_normalized_equality() {
    assert_ne!(read("time/period","P1Y"),read("time/period","P12M"));
    assert_eq!(read("time/duration","PT60S"),read("time/duration","PT1M"));
    assert_ne!(read("time/time","00:00"),read("time/offset-time","00:00Z"));
    assert_ne!(read("time/zoned-date-time","2024-01-01T00:00Z"),read("time/zoned-date-time","2024-01-01T00:00[UTC]"));
    assert_ne!(read("time/offset-date-time","2024-01-01T00:00Z"),read("time/offset-date-time","2024-01-01T01:00+01"));
    for (tag,text) in [("time/date","-999999999-01-01"),("time/date","+999999999-12-31"),("time/duration","PT-9223372036854775808S"),("time/duration","PT9223372036854775807.999999999S"),("time/period","P-2147483648Y2147483647M-2147483648D")] {
        let value = read(tag,text);let T::Tagged { tag,representation } = value.wire().unwrap() else { panic!("tag") };
        let T::String(text) = *representation else { panic!("text") };
        assert_eq!(read(&tag,&text),value);
    }
}

#[test]
fn legacy_date_decode_accepts_java_radices_and_preserves_signed_milliseconds() {
    for (text,millis) in [("0",0),("-0x8000000000000000",i64::MIN),("0x7fffffffffffffff",i64::MAX),("+077",63),("-#FF",-255),("１２",12),("0xＦＦ",255)] {
        assert_eq!(read("m",text).wire().unwrap(),tagged("m",T::Integer(millis)));
    }
    for text in ["0x8000000000000000","-9223372036854775809","08","0x-1","#"," 1"] { assert!(Time::read("m",&Object::String(text.into())).is_err()); }
}

#[test]
fn range_endpoints_follow_time_conversion_protocol_and_keep_open_bounds() {
    let instant = read("time/instant","2024-01-02T03:04Z");
    let values = Object::Vector(vec![Object::Time(instant),Object::Time(Time::LegacyDate(-1)),Object::String("ignored".into())]);
    assert_eq!(Time::read("xtdb/tstz-range",&values).unwrap().unwrap().wire().unwrap(),tagged("xtdb/tstz-range",T::Array(vec![
        tagged("time/zoned-date-time",T::String("2024-01-02T03:04Z[UTC]".into())),
        tagged("time/zoned-date-time",T::String("1969-12-31T23:59:59.999Z[UTC]".into())),
    ])));
    assert_eq!(Time::read("xtdb/tstz-range",&Object::Null).unwrap().unwrap().wire().unwrap(),tagged("xtdb/tstz-range",T::Array(vec![T::Null,T::Null])));
    assert!(Time::read("xtdb/tstz-range",&Object::Vector(vec![Object::Time(read("time/offset-date-time","2024-01-01T00:00Z"))])).is_err());
}

#[test]
fn temporal_java_hashes_use_object_components() {
    for (tag,text,hash) in [
        ("time/period","P1Y2M3D",1 + (2 << 8) + (3 << 16)),("time/duration","PT1.000000002S",103),
        ("time/date","2024-01-02",(2024 << 11) + (1 << 6) + 2),
        ("time/time","00:00:00.000000001",1),("time/instant","1970-01-01T00:00:01.000000002Z",103),
        ("time/year","-1",-1),("time/year-month","2024-02",2024 ^ (2 << 27)),
        ("time/month-day","--02-29",(2 << 6) + 29),("time/zone","+01",3600),("time/zone","UTC",84356),
        ("xtdb/interval","P1M2DT0.000000003S",(31 + 2) * 31 + 3),("m","-1",0),
    ] { assert_eq!(read(tag,text).java_hash().unwrap(),hash,"{tag}: {text}"); }
}
