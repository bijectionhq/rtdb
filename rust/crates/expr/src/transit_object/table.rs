use rtdb_format::transit::TransitValue as T;
use rtdb_types::{Result,TableRef};
use super::{Map,Object,render,tagged};

pub(super) fn read(representation: Object,allocation: &crate::EvaluationScope) -> Result<TableRef> {
    let representation = match representation {
        Object::List(mut values) => match values.len() {
            0 => Object::Map(Map::new()),1 => values.pop().unwrap(),_ => Object::Map(Map::from_flat_sequence(values)?),
        },
        value => value,
    };
    let schema = lookup(&representation,"schema-name")?;
    let table = lookup(&representation,"table-name")?;
    Ok(TableRef::new(if matches!(schema,Object::Null) { allocation.reserve(6)?; "public".into() } else { render::string(&schema,allocation)? },render::string(&table,allocation)?))
}

fn lookup(value: &Object,name: &str) -> Result<Object> {
    let key = Object::Keyword(name.into());
    Ok(match value {
        Object::Map(map) => map.get(&key)?.cloned().unwrap_or(Object::Null),
        Object::TxKey(record) => record.get(&key)?.cloned().unwrap_or(Object::Null),
        Object::Set(set) => if set.get(&key)?.is_some() { key } else { Object::Null },
        _ => Object::Null,
    })
}

pub(super) fn wire(value: &TableRef) -> T {
    tagged("xt/table",T::Map(vec![(T::Keyword("schema-name".into()),T::String(value.schema.clone())),(T::Keyword("table-name".into()),T::String(value.table.clone()))]))
}
