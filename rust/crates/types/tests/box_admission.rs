use rtdb_types::{CivilDate,DateUnit,Value};

#[test]
fn box_date_admission_checks_days_and_retains_millisecond_calendar_days() {
    let mut day = Value::calendar_date(CivilDate::MAX,DateUnit::Day);
    assert!(day.admit_box_date().is_err());
    let mut millis = Value::calendar_date(CivilDate::MAX,DateUnit::Millisecond);
    let before = millis.clone();
    millis.admit_box_date().unwrap();
    assert_eq!(millis,before);
    assert_eq!(millis.admit_expression_dates().unwrap_err().code, "xtdb.expression/overflow-error");
}

#[test]
fn box_admission_is_shallow_and_physical_millisecond_dates_keep_their_storage_value() {
    let mut list = Value::list(vec![Value::calendar_date(CivilDate::MAX,DateUnit::Day)]);
    list.admit_box_date().unwrap();
    assert!(list.admit_expression_dates().is_err());
    for millis in [1,-1,86_400_001,-86_400_001,i64::MIN,i64::MAX] {
        let mut value = Value::DateMilli(millis);
        value.admit_box_date().unwrap();
        assert_eq!(value,Value::DateMilli(millis));
    }
}
