use rtdb_types::{CalendarDate, CivilDate, DateUnit, Value};

#[test]
fn calendar_objects_roundtrip_their_validated_date_and_selected_unit() {
    for date in [CivilDate::MIN, CivilDate::from_ymd(1970, 1, 1).unwrap(), CivilDate::MAX] {
        for unit in [DateUnit::Day, DateUnit::Millisecond] {
            let value = CalendarDate::new(date, unit);
            let encoded = serde_json::to_string(&value).unwrap();
            assert_eq!(serde_json::from_str::<CalendarDate>(&encoded).unwrap(), value);
        }
    }
}

#[test]
fn calendar_day_object_admission_truncates_while_expression_admission_checks() {
    for day in [i64::from(i32::MIN) - 1, i64::from(i32::MAX) + 1] {
        let value = CalendarDate::new(CivilDate::from_epoch_day(day).unwrap(), DateUnit::Day);
        assert_eq!(value.object_value(), Value::DateDay(day as i32));
        assert_eq!(value.expression_value().unwrap_err().code, "xtdb.expression/overflow-error");
    }
    for day in [i32::MIN, 0, i32::MAX] {
        let value = CalendarDate::new(CivilDate::from_epoch_day(i64::from(day)).unwrap(), DateUnit::Day);
        assert_eq!(value.object_value(), Value::DateDay(day));
        assert_eq!(value.expression_value().unwrap(), Value::DateDay(day));
    }
}

#[test]
fn calendar_millisecond_admission_wraps_at_both_boundaries() {
    for date in [CivilDate::MIN, CivilDate::MAX] {
        let value = CalendarDate::new(date, DateUnit::Millisecond);
        let expected = Value::DateMilli(date.epoch_day().wrapping_mul(86_400_000));
        assert_eq!(value.object_value(), expected);
        assert_eq!(value.expression_value().unwrap(), expected);
    }
}
