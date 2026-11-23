use rtdb_types::{Anomaly, ListView, Result, Value};

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
    for index in 0..left.len() {
        let left = component_at(left, index)? / left_scale;
        let right = component_at(right, index)? / right_scale;
        dot += left * right;
        left_squared += left * left;
        right_squared += right * right;
    }
    Ok(Value::F64(distance(dot, left_squared, right_squared)))
}

fn distance(dot: f64, left_squared: f64, right_squared: f64) -> f64 {
    let similarity = dot / (left_squared * right_squared).sqrt();
    1.0 - similarity.clamp(-1.0, 1.0)
}

pub struct CosineVector {
    components: Vec<f64>,
    squared: f64,
}

impl CosineVector {
    pub fn prepare<'a>(values: impl Iterator<Item = &'a Value> + Clone, dimensions: usize) -> Result<Self> {
        Self::prepare_components(values.map(component), dimensions)
    }

    pub fn prepare_value(value: &Value) -> Result<Self> {
        let values = components(value)?;
        Self::prepare_components((0..values.len()).map(|index| component_at(values, index)), values.len())
    }

    pub fn prepare_components(values: impl Iterator<Item = Result<f64>>, dimensions: usize) -> Result<Self> {
        let mut scale = 0.0_f64;
        let mut components = Vec::with_capacity(dimensions);
        for value in values {
            let value = value?;
            if !value.is_finite() {
                return Err(Anomaly::incorrect("rtdb.vector/component", "Vector components must be finite numbers"));
            }
            scale = scale.max(value.abs());
            components.push(value);
        }
        if scale == 0.0 {
            return Err(Anomaly::incorrect("rtdb.vector/zero-norm", "Cosine distance requires a nonzero vector"));
        }
        let mut squared = 0.0;
        for value in &mut components {
            *value /= scale;
            squared += *value * *value;
        }
        Ok(Self { components, squared })
    }

    pub fn dimensions(&self) -> usize { self.components.len() }

    pub fn score(&self, values: impl ExactSizeIterator<Item = f64> + Clone) -> Option<f64> {
        if values.len() != self.dimensions() { return None; }
        let scale = values.clone().try_fold(0.0_f64, |scale, value| value.is_finite().then(|| scale.max(value.abs())))?;
        if scale == 0.0 { return None; }
        let mut dot = 0.0;
        let mut squared = 0.0;
        for (value, prepared) in values.zip(&self.components) {
            let value = value / scale;
            dot += value * prepared;
            squared += value * value;
        }
        Some(distance(dot, squared, self.squared))
    }
}

fn components(vector: &Value) -> Result<ListView<'_>> {
    match vector {
        Value::List(_) | Value::FixedSizeList(_) => Ok(vector.list_view().unwrap()),
        _ => Err(Anomaly::incorrect("rtdb.vector/type", "Cosine distance requires numeric lists")),
    }
}

fn scale(values: ListView<'_>) -> Result<f64> {
    let mut scale = 0.0_f64;
    for index in 0..values.len() {
        scale = scale.max(component_at(values, index)?.abs());
    }
    if scale == 0.0 {
        return Err(Anomaly::incorrect("rtdb.vector/zero-norm", "Cosine distance requires a nonzero vector"));
    }
    Ok(scale)
}

fn component_at(values: ListView<'_>, index: usize) -> Result<f64> {
    if let ListView::Values(values) = values { return component(&values[index]); }
    let value = values.float_component(index);
    value.filter(|value| value.is_finite())
        .ok_or_else(|| Anomaly::incorrect("rtdb.vector/component", "Vector components must be finite numbers"))
}

fn component(value: &Value) -> Result<f64> {
    number(value)
        .filter(|value| value.is_finite())
        .ok_or_else(|| Anomaly::incorrect("rtdb.vector/component", "Vector components must be finite numbers"))
}

#[cfg(test)]
mod tests {
    use super::{CosineVector, cosine_distance};
    use rtdb_types::Value;

    #[test]
    fn prepared_distance_has_the_scalar_bits() {
        let query = [1e290, -1e-210, 0.0, 1e290].map(Value::F64);
        let prepared = CosineVector::prepare(query.iter(), query.len()).unwrap();
        for row in [[1e-290, 1e-290, 0.0, 1e-290], [1.0, -2.0, -0.0, 3.0], [-10.0, -100.0, 5.0, 0.01]] {
            let Value::F64(expected) = cosine_distance(&Value::list(row.map(Value::F64).to_vec()), &Value::list(query.to_vec())).unwrap() else { unreachable!() };
            assert_eq!(prepared.score(row.into_iter()).unwrap().to_bits(), expected.to_bits());
        }
        assert_eq!(prepared.score([0.0; 4].into_iter()), None);
        assert_eq!(prepared.score([1.0, f64::NAN, 0.0, 1.0].into_iter()), None);
    }
}
