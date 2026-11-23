use rtdb_types::StructFields;
use rtdb_columnar::{admit_execution_value, build_array, copy_array, hash_value, read_value, to_field};
use rtdb_types::{CalendarDate, CivilDate, DateUnit, Value};

#[test]
fn object_writers_reject_out_of_range_dates_in_both_units() {
    for date in [CivilDate::MIN, CivilDate::MAX] {
        for unit in [DateUnit::Day, DateUnit::Millisecond] {
            let wide = Value::Date(CalendarDate::new(date, unit));
            let logical = wide.logical_type();
            assert!(copy_array(&logical, &[wide.clone()]).is_err());
            assert!(hash_value(&wide, 0).is_err());
            assert_eq!(build_array(&logical, &[wide.clone()]).unwrap_err().code, "xtdb/date-out-of-range");
            let mut admitted = wide;
            assert_eq!(admit_execution_value(&mut admitted).unwrap_err().code, "xtdb/date-out-of-range");
        }
    }
}

#[test]
fn nested_calendar_values_follow_the_same_explicit_admission_boundary() {
    let wide = Value::Date(CalendarDate::new(CivilDate::from_epoch_day(-1).unwrap(), DateUnit::Day));
    let mut nested = Value::Struct(StructFields::from([("dates".into(),Value::List(vec![wide]))]));
    let logical = nested.logical_type();
    assert!(copy_array(&logical,&[nested.clone()]).is_err());
    let array = build_array(&logical,&[nested.clone()]).unwrap();
    admit_execution_value(&mut nested).unwrap();
    assert_eq!(read_value(&to_field("nested",&logical).unwrap(),array.as_ref(),0).unwrap(),nested);
    assert!(!nested.has_calendar_object());
}
