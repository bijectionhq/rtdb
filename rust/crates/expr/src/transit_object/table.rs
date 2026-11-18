use rtdb_format::transit::TransitValue as T;
use rtdb_types::{Result,TableRef};
use super::{Object,render,tagged};
use crate::admission::Admission;

pub(super) fn read(representation: Object,allocation: Admission<'_>) -> Result<TableRef> {
    let representation = representation.destructure_map()?;
    let schema = representation.lookup_keyword("schema-name")?;
    let table = representation.lookup_keyword("table-name")?;
    Ok(TableRef::new(if matches!(schema,Object::Null) { allocation.reserve(6)?; "public".into() } else { render::string(&schema,allocation)? },render::string(&table,allocation)?))
}

pub(super) fn wire(value: &TableRef) -> T {
    tagged("xt/table",T::Map(vec![(T::Keyword("schema-name".into()),T::String(value.schema.clone())),(T::Keyword("table-name".into()),T::String(value.table.clone()))]))
}
