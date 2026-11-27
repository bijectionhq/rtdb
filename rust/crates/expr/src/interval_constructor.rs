use rtdb_plan::IntervalConstructor;
use rtdb_types::{Result, Value};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IntervalConstructorFunction {
    SingleField,
    MultiField,
}

impl IntervalConstructorFunction {
    pub(crate) fn from_canonical(name: &str) -> Option<Self> {
        Some(match name {
            "single_field_interval" => Self::SingleField,
            "multi_field_interval" => Self::MultiField,
            _ => return None,
        })
    }

    pub(crate) fn canonical_name(self) -> &'static str {
        match self {
            Self::SingleField => "single_field_interval",
            Self::MultiField => "multi_field_interval",
        }
    }

    pub(crate) fn eval(self, args: &[Value]) -> Result<Value> {
        if args.iter().any(crate::compare::nullish) { return Ok(Value::Null); }
        let name = self.canonical_name();
        let text = |index| match &args[index] { Value::Utf8(value) => Ok(value.as_str()), _ => Err(crate::numeric::inapplicable(name, &[&args[index]])) };
        let number = |index| crate::numeric::integer(&args[index]).and_then(|value| i64::try_from(value).ok()).ok_or_else(|| crate::numeric::inapplicable(name, &[&args[index]]));
        let recipe = if self == Self::SingleField {
            let unit = text(1)?;
            IntervalConstructor::single(unit, number(2)?, if unit == "SECOND" { Some(number(3)?) } else { None })?
        } else {
            IntervalConstructor::multi(text(1)?, text(3)?, number(2)?, number(4)?)?
        };
        construct(&args[0], &recipe)
    }
}

pub(crate) fn construct(value: &Value,recipe: &IntervalConstructor) -> Result<Value> {
    crate::interval_cast::cast(value, recipe.qualifier())
}
