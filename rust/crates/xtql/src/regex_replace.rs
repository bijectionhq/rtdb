use rtdb_plan::{RegexForm as F,RegexReplacement};
use rtdb_types::{Keyword,Value};
use crate::{Edn,EdnKind};

pub(crate) fn metadata(arguments: &[Edn]) -> RegexReplacement {
    let field = |index| arguments.get(index).map(form).unwrap_or_default();
    RegexReplacement { pattern: field(1),replacement: field(2),start: field(3),occurrence: field(4),flags: field(5) }
}

fn form(value: &Edn) -> F {
    match &value.kind {
        EdnKind::Nil => F::default(),
        EdnKind::Bool(value) => F::Literal(Value::Bool(*value)),
        EdnKind::Integer(value) => F::Literal(Value::I64(*value)),
        EdnKind::Float(value) => F::Literal(Value::F64(*value)),
        EdnKind::String(value) => F::Literal(Value::Utf8(value.clone())),
        EdnKind::Keyword(value) => F::Literal(Value::Keyword(Keyword::from_name(value))),
        EdnKind::Symbol(value) => F::Symbol(value.clone()),
        EdnKind::Character(value) => F::Character(*value),
        EdnKind::BigInteger(value) => F::BigInteger(value.clone()),
        EdnKind::Decimal(value) => F::Decimal(value.clone()),
        EdnKind::List(values) => F::List(values.iter().map(form).collect()),
        EdnKind::Vector(values) => F::Vector(values.iter().map(form).collect()),
        EdnKind::Set(values) => F::Set(values.iter().map(form).collect()),
        EdnKind::Map(values) => F::Map(values.iter().map(|(key,value)|(form(key),form(value))).collect()),
        EdnKind::Tagged { tag,value } => F::Tagged { tag: tag.clone(),value: Box::new(form(value)) },
        EdnKind::AnonymousFunction(value) => F::AnonymousFunction(Box::new(form(value))),
    }
}
