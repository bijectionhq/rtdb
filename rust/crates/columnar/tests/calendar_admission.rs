use rtdb_types::StructFields;
use rtdb_columnar::{admit_execution_value, build_array, copy_array, hash_value, read_value, to_field};
use rtdb_types::{CalendarDate, CivilDate, DateUnit, Value};

#[test]
fn object_writers_narrow_wide_dates_while_stored_cell_copies_reject_them() {
    for date in [CivilDate::MIN,CivilDate::MAX] {
        for unit in [DateUnit::Day,DateUnit::Millisecond] {
            let wide = Value::Date(CalendarDate::new(date,unit));
            let logical = wide.logical_type();
            assert!(copy_array(&logical,&[wide.clone()]).is_err());
            assert!(hash_value(&wide,0).is_err());
            let array = build_array(&logical,&[wide.clone()]).unwrap();
            let expected = CalendarDate::new(date,unit).object_value();
            assert_eq!(read_value(&to_field("d",&logical).unwrap(),array.as_ref(),0).unwrap(),expected);
            let mut admitted = wide;
            admit_execution_value(&mut admitted);
            assert_eq!(admitted,expected);
            copy_array(&logical,&[admitted]).unwrap();
        }
    }
}

#[test]
fn nested_calendar_values_follow_the_same_explicit_admission_boundary() {
    let wide = Value::Date(CalendarDate::new(CivilDate::MAX,DateUnit::Day));
    let mut nested = Value::Struct(StructFields::from([("dates".into(),Value::list(vec![wide]))]));
    let logical = nested.logical_type();
    assert!(copy_array(&logical,&[nested.clone()]).is_err());
    let array = build_array(&logical,&[nested.clone()]).unwrap();
    admit_execution_value(&mut nested);
    assert_eq!(read_value(&to_field("nested",&logical).unwrap(),array.as_ref(),0).unwrap(),nested);
    assert!(!nested.has_calendar_object());
}
