use arrow_array::{Array, Float32Array, Float64Array, cast::AsArray, types::{Float16Type, Float32Type, Float64Type}};
use arrow_schema::{DataType, Field};
use rtdb_columnar::ArrowCell;
use rtdb_types::{Anomaly, ListView, Result, Value};

use crate::{compare::nullish, numeric::number};

pub(crate) fn cosine_distance(left: &Value, right: &Value) -> Result<Value> {
    if nullish(left) || nullish(right) {
        return Ok(Value::Null);
    }
    score_pair(VectorInput::Stored(components(left)?), VectorInput::Stored(components(right)?))
}

#[derive(Clone, Copy)]
pub(crate) enum VectorInput<'a> {
    Stored(ListView<'a>),
    Floats { values: ListView<'a>, offset: usize, len: usize },
    Arrow { field: &'a Field, array: &'a dyn Array, offset: usize, len: usize },
}

impl<'a> VectorInput<'a> {
    pub(crate) fn from_arrow(cell: ArrowCell<'a>) -> Option<Self> {
        if let Some((values, range)) = cell.float_list_span() {
            return Some(Self::Floats { values, offset: range.start, len: range.len() });
        }
        let (field, array, range) = cell.sequence_range()?;
        Some(Self::Arrow { field, array, offset: range.start, len: range.len() })
    }

    pub(crate) fn prefix(self, length: usize) -> Option<Self> {
        if length > self.len() { return None; }
        Some(match self {
            Self::Stored(ListView::Values(values)) => Self::Stored(ListView::Values(&values[..length])),
            Self::Stored(ListView::Numeric(values)) => Self::Arrow { field: values.field(), array: values.array().as_ref(), offset: 0, len: length },
            Self::Stored(ListView::Scalars(_)) => return None,
            Self::Stored(values) => Self::Floats { values, offset: 0, len: length },
            Self::Floats { values, offset, .. } => Self::Floats { values, offset, len: length },
            Self::Arrow { field, array, offset, .. } => Self::Arrow { field, array, offset, len: length },
        })
    }

    pub(crate) fn float_cells(self) -> Option<(ListView<'a>, usize, usize)> {
        match self {
            Self::Stored(values) if values.is_float() => Some((values, 0, values.len())),
            Self::Floats { values, offset, len } => Some((values, offset, len)),
            _ => None,
        }
    }

    pub(crate) fn len(self) -> usize {
        match self { Self::Stored(values) => values.len(), Self::Floats { len, .. } | Self::Arrow { len, .. } => len }
    }

    pub(crate) fn component(self, index: usize) -> Result<f64> {
        match self {
            Self::Stored(values) => component_at(values, index),
            Self::Floats { values, offset, .. } => values.float_component(offset + index)
                .filter(|value| value.is_finite()).ok_or_else(invalid_component),
            Self::Arrow { field, array, offset, .. } => arrow_number(ArrowCell { field, array, row: offset + index }),
        }
    }
}

pub(crate) fn score_pair(left: VectorInput<'_>, right: VectorInput<'_>) -> Result<Value> {
    if left.len() == 0 || left.len() != right.len() {
        return Err(Anomaly::incorrect("rtdb.vector/dimensions", "Cosine distance requires vectors of the same positive dimension"));
    }

    match (float_slice(left), float_slice(right)) {
        (Some(FloatSlice::F32(a, offset_a, len)), Some(FloatSlice::F32(b, offset_b, _))) =>
            score_components(float_components(a, offset_a, len, f64::from), float_components(b, offset_b, len, f64::from)),
        (Some(FloatSlice::F32(a, offset_a, len)), Some(FloatSlice::F64(b, offset_b, _))) =>
            score_components(float_components(a, offset_a, len, f64::from), float_components(b, offset_b, len, |v| v)),
        (Some(FloatSlice::F64(a, offset_a, len)), Some(FloatSlice::F32(b, offset_b, _))) =>
            score_components(float_components(a, offset_a, len, |v| v), float_components(b, offset_b, len, f64::from)),
        (Some(FloatSlice::F64(a, offset_a, len)), Some(FloatSlice::F64(b, offset_b, _))) =>
            score_components(float_components(a, offset_a, len, |v| v), float_components(b, offset_b, len, |v| v)),
        _ => score_components((0..left.len()).map(|index| left.component(index)), (0..right.len()).map(|index| right.component(index))),
    }
}

enum FloatSlice<'a> {
    F32(&'a Float32Array, usize, usize),
    F64(&'a Float64Array, usize, usize),
}

fn float_slice(input: VectorInput<'_>) -> Option<FloatSlice<'_>> {
    let (values, offset, len) = match input {
        VectorInput::Stored(view) => (view, 0, view.len()),
        VectorInput::Floats { values, offset, len } => (values, offset, len),
        VectorInput::Arrow { .. } => return None,
    };
    match values {
        ListView::F32(values) => Some(FloatSlice::F32(values, offset, len)),
        ListView::F64(values) => Some(FloatSlice::F64(values, offset, len)),
        _ => None,
    }
}

fn float_components<T: arrow_array::types::ArrowPrimitiveType>(
    array: &arrow_array::PrimitiveArray<T>, offset: usize, len: usize, to_double: impl Fn(T::Native) -> f64 + Clone,
) -> impl Iterator<Item = Result<f64>> + Clone {
    array.values()[offset..offset + len].iter().enumerate().map(move |(index, &value)| {
        let value = to_double(value);
        if array.is_null(offset + index) || !value.is_finite() { Err(invalid_component()) } else { Ok(value) }
    })
}

fn score_components(
    left: impl Iterator<Item = Result<f64>> + Clone, right: impl Iterator<Item = Result<f64>> + Clone,
) -> Result<Value> {
    // Scale independently so that both the products and the squared norms stay finite.
    let left_scale = scale(left.clone())?;
    let right_scale = scale(right.clone())?;
    let mut dot = 0.0;
    let mut left_squared = 0.0;
    let mut right_squared = 0.0;
    for (left, right) in left.zip(right) {
        let left = left? / left_scale;
        let right = right? / right_scale;
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

pub(crate) fn components(vector: &Value) -> Result<ListView<'_>> {
    match vector {
        Value::List(_) | Value::FixedSizeList(_) => Ok(vector.list_view().unwrap()),
        _ => Err(invalid_vector_type()),
    }
}

pub(crate) fn invalid_vector_type() -> Anomaly {
    Anomaly::incorrect("rtdb.vector/type", "Cosine distance requires numeric lists")
}

fn scale(mut values: impl Iterator<Item = Result<f64>>) -> Result<f64> {
    let scale = values.try_fold(0.0_f64, |scale, value| Ok::<_, Anomaly>(scale.max(value?.abs())))?;
    if scale == 0.0 {
        return Err(Anomaly::incorrect("rtdb.vector/zero-norm", "Cosine distance requires a nonzero vector"));
    }
    Ok(scale)
}

fn component_at(values: ListView<'_>, index: usize) -> Result<f64> {
    match values {
        ListView::Values(values) => return component(&values[index]),
        ListView::Numeric(values) => return packed_number(ArrowCell { field: values.field(), array: values.array().as_ref(), row: index }),
        _ => {}
    }
    let value = values.float_component(index);
    value.filter(|value| value.is_finite())
        .ok_or_else(|| Anomaly::incorrect("rtdb.vector/component", "Vector components must be finite numbers"))
}

fn invalid_component() -> Anomaly {
    Anomaly::incorrect("rtdb.vector/component", "Vector components must be finite numbers")
}

fn packed_number(cell: ArrowCell<'_>) -> Result<f64> {
    let cell = cell.physical()?;
    if let Some(value) = cell.integer() { return Ok(value as f64); }
    arrow_number(cell)
}

fn arrow_number(cell: ArrowCell<'_>) -> Result<f64> {
    let cell = cell.physical()?;
    if cell.is_null()? { return Err(invalid_component()); }
    if let Some(value) = cell.signed_integer() { return Ok(value as f64); }
    let value = match cell.field.data_type() {
        DataType::Float16 if cell.field.metadata().is_empty() => cell.array.as_primitive::<Float16Type>().value(cell.row).to_f64(),
        DataType::Float32 if cell.field.metadata().is_empty() => f64::from(cell.array.as_primitive::<Float32Type>().value(cell.row)),
        DataType::Float64 if cell.field.metadata().is_empty() => cell.array.as_primitive::<Float64Type>().value(cell.row),
        ty if rtdb_columnar::decimal_parameters(ty).is_some() => rtdb_columnar::decimal_cell_as_f64(cell.field, cell.array, cell.row)?,
        _ => return Err(invalid_component()),
    };
    value.is_finite().then_some(value).ok_or_else(invalid_component)
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
    fn packed_text_has_no_float_prefix() {
        let array = std::sync::Arc::new(arrow_array::StringArray::from(vec!["text"]));
        let field = std::sync::Arc::new(arrow_schema::Field::new("v", arrow_schema::DataType::Utf8, false));
        let list = rtdb_types::ListValue::from_scalar_arrow(field, array).unwrap();
        assert!(super::VectorInput::Stored(list.view()).prefix(1).is_none());
    }

    #[test]
    fn wave4_cosine_slice_has_identical_bits_and_left_first_errors() {
        use super::{VectorInput, score_pair};
        use arrow_array::{Float32Array, Float64Array};
        use rtdb_types::ListView;

        let left = Float32Array::from(vec![None, Some(-0.0), Some(1e30), Some(-1e-30)]);
        let right = Float64Array::from(vec![Some(2e300), Some(3e-300), Some(-1.0), None]);
        let a = VectorInput::Floats { values: ListView::F32(&left), offset: 1, len: 3 };
        let b = VectorInput::Floats { values: ListView::F64(&right), offset: 0, len: 3 };
        let Value::F64(score) = score_pair(a, b).unwrap() else { unreachable!() };
        let Value::F64(reference) = super::score_components((0..3).map(|index| a.component(index)), (0..3).map(|index| b.component(index))).unwrap() else { unreachable!() };
        assert_eq!(score.to_bits(), reference.to_bits());
        let zero = Float32Array::from(vec![-0.0, 0.0, 0.0]);
        let a = VectorInput::Stored(ListView::F32(&zero));
        let b = VectorInput::Floats { values: ListView::F64(&right), offset: 1, len: 3 };
        assert_eq!(score_pair(a, b).unwrap_err().code, "rtdb.vector/zero-norm");
        assert_eq!(score_pair(b, a).unwrap_err().code, "rtdb.vector/component");
    }

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
