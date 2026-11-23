use rtdb_columnar::MemoryBudget;
use rtdb_tx::{Documents, SqlArguments, UserMetadata};
use rtdb_types::{AnomalyCategory, CivilDate, DateUnit, StructFields, Value};

#[test]
fn transaction_object_owners_reject_wide_dates_before_durable_copying() {
    let memory = MemoryBudget::new(1 << 20);
    for unit in [DateUnit::Day, DateUnit::Millisecond] {
        let value = Value::calendar_date(CivilDate::MAX, unit);
        let document = StructFields::from([("d".into(), value.clone())]);
        for error in [
            Documents::new(vec![document.clone()], &memory).unwrap_err(),
            SqlArguments::from_rows(vec![vec![Value::list(vec![value])]], &memory).unwrap_err(),
            UserMetadata::new(Value::Struct(document), &memory).unwrap_err(),
        ] {
            assert_eq!(error.category, AnomalyCategory::Incorrect);
            assert_eq!(error.code, "xtdb/date-out-of-range");
        }
        assert_eq!(memory.used_bytes(), 0);
    }
}
