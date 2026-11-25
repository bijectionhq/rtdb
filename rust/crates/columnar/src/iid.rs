use std::sync::Arc;

use arrow_array::{Array, ArrayRef, FixedSizeBinaryArray, Int16Array, Int32Array, Int64Array, RunArray, builder::FixedSizeBinaryBuilder, make_array, types::{Int16Type, Int32Type, Int64Type}};
use arrow_schema::DataType;
use rtdb_types::{Anomaly, Iid, LogicalType, Result, RunEndWidth, ScalarType};

pub enum IidView<'a> {
    Plain(&'a FixedSizeBinaryArray),
    Run16(&'a RunArray<Int16Type>),
    Run32(&'a RunArray<Int32Type>),
    Run64(&'a RunArray<Int64Type>),
}

impl<'a> IidView<'a> {
    /// Accepts non-null fixed-size binary(16) values in plain or signed run-end layouts.
    pub fn new(array: &'a dyn Array) -> Result<Self> {
        match array.data_type() {
            DataType::FixedSizeBinary(16) => {
                let values = downcast::<FixedSizeBinaryArray>(array)?;
                validate_values(values)?;
                Ok(Self::Plain(values))
            }
            DataType::RunEndEncoded(ends, _) => match ends.data_type() {
                DataType::Int16 => {
                    let runs = downcast::<RunArray<Int16Type>>(array)?;
                    validate_values(downcast::<FixedSizeBinaryArray>(runs.values().as_ref())?)?;
                    Ok(Self::Run16(runs))
                }
                DataType::Int32 => {
                    let runs = downcast::<RunArray<Int32Type>>(array)?;
                    validate_values(downcast::<FixedSizeBinaryArray>(runs.values().as_ref())?)?;
                    Ok(Self::Run32(runs))
                }
                DataType::Int64 => {
                    let runs = downcast::<RunArray<Int64Type>>(array)?;
                    validate_values(downcast::<FixedSizeBinaryArray>(runs.values().as_ref())?)?;
                    Ok(Self::Run64(runs))
                }
                _ => Err(Anomaly::incorrect("rtdb/iid-run-width", "IID run ends must be i16, i32, or i64")),
            },
            _ => Err(Anomaly::incorrect("rtdb/iid-layout", "IID values must use fixed-size binary(16)")),
        }
    }

    pub fn len(&self) -> usize {
        match self { Self::Plain(a) => a.len(), Self::Run16(a) => a.len(), Self::Run32(a) => a.len(), Self::Run64(a) => a.len() }
    }

    pub fn is_empty(&self) -> bool { self.len() == 0 }

    pub fn get(&self, index: usize) -> Result<Iid> {
        if index >= self.len() { return Err(Anomaly::incorrect("rtdb/iid-index", "IID row index exceeds array length")); }
        let bytes = match self {
            Self::Plain(a) => a.value(index),
            Self::Run16(a) => downcast::<FixedSizeBinaryArray>(a.values().as_ref())?.value(a.get_physical_index(index)),
            Self::Run32(a) => downcast::<FixedSizeBinaryArray>(a.values().as_ref())?.value(a.get_physical_index(index)),
            Self::Run64(a) => downcast::<FixedSizeBinaryArray>(a.values().as_ref())?.value(a.get_physical_index(index)),
        };
        let mut iid = [0; 16];
        iid.copy_from_slice(bytes);
        Ok(Iid(iid))
    }
}

fn downcast<T: 'static>(array: &dyn Array) -> Result<&T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| Anomaly::incorrect("rtdb/arrow-array-type", "Arrow array implementation disagrees with its declared type"))
}

fn validate_values(values: &FixedSizeBinaryArray) -> Result<()> {
    if values.value_length() != 16 || values.null_count() != 0 {
        Err(Anomaly::incorrect("rtdb/iid-values", "Every IID must be exactly sixteen non-null bytes"))
    } else { Ok(()) }
}

pub fn iid_data_type(encoding: Option<RunEndWidth>) -> Result<DataType> {
    let Some(width) = encoding else { return Ok(DataType::FixedSizeBinary(16)); };
    let value = Box::new(LogicalType::Scalar(ScalarType::FixedSizeBinary(16)));
    let field = crate::to_field("_iid", &LogicalType::RunEndEncoded { width, value })?;
    Ok(field.data_type().clone())
}

/// Encodes consecutive equal IIDs as runs when a width is supplied.
/// Logical length must fit the selected signed run-end width.
pub fn build_iids(iids: &[Iid], encoding: Option<RunEndWidth>) -> Result<ArrayRef> {
    let mut builder = FixedSizeBinaryBuilder::with_capacity(iids.len(), 16);
    if encoding.is_none() {
        for iid in iids { builder.append_value(iid.0).map_err(super::arrow_error)?; }
        return Ok(Arc::new(builder.finish()));
    }
    let mut ends = Vec::new();
    for (index, iid) in iids.iter().enumerate() {
        if iids.get(index + 1) != Some(iid) {
            builder.append_value(iid.0).map_err(super::arrow_error)?;
            ends.push(index + 1);
        }
    }
    let values = builder.finish();
    let runs: ArrayRef = match encoding {
        Some(RunEndWidth::I16) => {
            let ends = ends.into_iter().map(i16::try_from).collect::<std::result::Result<Vec<_>, _>>().map_err(|_| run_overflow())?;
            Arc::new(RunArray::<Int16Type>::try_new(&Int16Array::from(ends), &values).map_err(super::arrow_error)?)
        }
        Some(RunEndWidth::I32) => {
            let ends = ends.into_iter().map(i32::try_from).collect::<std::result::Result<Vec<_>, _>>().map_err(|_| run_overflow())?;
            Arc::new(RunArray::<Int32Type>::try_new(&Int32Array::from(ends), &values).map_err(super::arrow_error)?)
        }
        Some(RunEndWidth::I64) => {
            let ends = ends.into_iter().map(i64::try_from).collect::<std::result::Result<Vec<_>, _>>().map_err(|_| run_overflow())?;
            Arc::new(RunArray::<Int64Type>::try_new(&Int64Array::from(ends), &values).map_err(super::arrow_error)?)
        }
        None => return Err(Anomaly::fault("rtdb/iid-encoding", "Plain IID encoding was not handled")),
    };
    let data = runs.to_data().into_builder().data_type(iid_data_type(encoding)?).build().map_err(super::arrow_error)?;
    Ok(make_array(data))
}

fn run_overflow() -> Anomaly { Anomaly::incorrect("rtdb/run-end-overflow", "IID array exceeds its run-end width") }
