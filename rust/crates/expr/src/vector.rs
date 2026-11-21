use rtdb_types::{Anomaly, Result, Value};

use crate::{compare::nullish, numeric::number};

pub(crate) fn cosine_distance(left: &Value, right: &Value) -> Result<Value> {
    if nullish(left) || nullish(right) {
        return Ok(Value::Null);
    }
    let left = components(left)?;
    let right = components(right)?;
    if left.is_empty() || left.len() != right.len() {
        return Err(Anomaly::incorrect("rtdb.vector/dimensions", "Cosine distance requires vectors of the same positive dimension"));
    }

    // Scale independently so that both the products and the squared norms stay finite.
    let left_scale = scale(left)?;
    let right_scale = scale(right)?;
    let mut dot = 0.0;
    let mut left_squared = 0.0;
    let mut right_squared = 0.0;
    for (left, right) in left.iter().zip(right) {
        let left = component(left)? / left_scale;
        let right = component(right)? / right_scale;
        dot += left * right;
        left_squared += left * left;
        right_squared += right * right;
    }
    let similarity = dot / (left_squared * right_squared).sqrt();
    Ok(Value::F64(1.0 - similarity.clamp(-1.0, 1.0)))
}

fn components(vector: &Value) -> Result<&[Value]> {
    match vector {
        Value::List(values) | Value::FixedSizeList(values) => Ok(values),
        _ => Err(Anomaly::incorrect("rtdb.vector/type", "Cosine distance requires numeric lists")),
    }
}

fn scale(values: &[Value]) -> Result<f64> {
    let mut scale = 0.0_f64;
    for value in values {
        scale = scale.max(component(value)?.abs());
    }
    if scale == 0.0 {
        return Err(Anomaly::incorrect("rtdb.vector/zero-norm", "Cosine distance requires a nonzero vector"));
    }
    Ok(scale)
}

fn component(value: &Value) -> Result<f64> {
    number(value)
        .filter(|value| value.is_finite())
        .ok_or_else(|| Anomaly::incorrect("rtdb.vector/component", "Vector components must be finite numbers"))
}
