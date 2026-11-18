use rtdb_types::{Result,TableRef};
use super::{Object,render};
use crate::admission::Admission;

pub(super) fn read(representation: Object,allocation: Admission<'_>) -> Result<TableRef> {
    let representation = representation.destructure_map()?;
    let schema = representation.lookup_keyword("schema-name")?;
    let table = representation.lookup_keyword("table-name")?;
    Ok(TableRef::new(if matches!(schema,Object::Null) { allocation.reserve(6)?; "public".into() } else { render::string(&schema,allocation)? },render::string(&table,allocation)?))
}
