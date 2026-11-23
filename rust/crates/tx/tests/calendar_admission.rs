use rtdb_types::StructFields;
use rtdb_columnar::MemoryBudget;
use rtdb_tx::{Documents,SqlArguments,UserMetadata};
use rtdb_types::{CivilDate,DateUnit,Value};

#[test]
fn transaction_object_owners_admit_wide_dates_before_durable_copying() {
    let memory = MemoryBudget::new(1 << 20);
    let date = CivilDate::MAX;
    let value = Value::calendar_date(date,DateUnit::Day);
    let expected = Value::DateDay(date.epoch_day() as i32);
    let documents = Documents::new(vec![StructFields::from([("d".into(),value.clone())])],&memory).unwrap();
    assert_eq!(documents.rows()[0]["d"],expected);
    let arguments = SqlArguments::from_rows(vec![vec![Value::list(vec![value.clone()])]],&memory).unwrap();
    assert_eq!(arguments.rows().next().unwrap(),[Value::list(vec![expected.clone()])]);
    let metadata = UserMetadata::new(Value::Struct(StructFields::from([("d".into(),value)])),&memory).unwrap();
    assert_eq!(metadata.value(),&Value::Struct(StructFields::from([("d".into(),expected)])));
    drop(documents);drop(arguments);drop(metadata);
    assert_eq!(memory.used_bytes(),0);
}
