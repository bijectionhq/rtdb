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
fn calendar_day_admission_checks_the_vector_range() {
    for day in [i64::from(i32::MIN) - 1, i64::from(i32::MAX) + 1] {
        let value = CalendarDate::new(CivilDate::from_epoch_day(day).unwrap(), DateUnit::Day);
        assert_eq!(value.object_value().unwrap_err().code, "xtdb/date-out-of-range");
        assert_eq!(value.expression_value().unwrap_err().code, "xtdb.expression/overflow-error");
    }
    for day in [i32::MIN, 0, i32::MAX] {
        let value = CalendarDate::new(CivilDate::from_epoch_day(i64::from(day)).unwrap(), DateUnit::Day);
        assert_eq!(value.object_value().unwrap(), Value::DateDay(day));
        assert_eq!(value.expression_value().unwrap(), Value::DateDay(day));
    }
}

#[test]
fn calendar_millisecond_admission_checks_the_vector_range() {
    for date in [CivilDate::MIN, CivilDate::MAX] {
        let value = CalendarDate::new(date, DateUnit::Millisecond);
        assert_eq!(value.object_value().unwrap_err().code, "xtdb/date-out-of-range");
        assert_eq!(value.expression_value().unwrap_err().code, "xtdb.expression/overflow-error");
    }
}

#[test]
fn calendar_objects_and_admitted_dates_report_the_same_invalid_identifier_type() {
    let date = CivilDate::from_ymd(2020, 1, 6).unwrap();
    for unit in [DateUnit::Day, DateUnit::Millisecond] {
        let object = CalendarDate::new(date, unit);
        for value in [Value::Date(object), object.object_value().unwrap(), object.expression_value().unwrap()] {
            let error = value.entity_id().unwrap_err();
            assert_eq!(error.code, "xtdb/invalid-id");
            assert_eq!(error.message.as_deref(), Some("Invalid ID type: java.time.LocalDate"));
            assert_eq!(error.context["type"], "java.time.LocalDate");
        }
    }
}
