use std::sync::Arc;

use arrow_array::{
    ListArray,
    builder::{ArrayBuilder, ListBuilder, PrimitiveBuilder},
    types::{ArrowPrimitiveType, Float32Type, Float64Type},
};
use arrow_schema::FieldRef;
use rtdb_types::{ListValue, PgType, Result, Value};

use crate::{
    Format, invalid,
    text::{float, float32, visit_array_elements},
};

pub(crate) fn read(ty: PgType, format: Format, bytes: &[u8], item: FieldRef) -> Result<ListArray> {
    match ty {
        PgType::Float4s => read_list::<Float32Type>(format, bytes, item, 700, float32, |bytes| {
            Ok(f32::from_be_bytes(bytes.try_into().map_err(invalid)?))
        }),
        PgType::Float8s => read_list::<Float64Type>(format, bytes, item, 701, float, |bytes| {
            Ok(f64::from_be_bytes(bytes.try_into().map_err(invalid)?))
        }),
        _ => unreachable!("float array codec requires a float array type"),
    }
}

fn read_list<T: ArrowPrimitiveType>(
    format: Format,
    bytes: &[u8],
    item: FieldRef,
    oid: i32,
    parse: impl Fn(&str) -> Result<T::Native>,
    decode: impl Fn(&[u8]) -> Result<T::Native>,
) -> Result<ListArray> {
    let values = PrimitiveBuilder::<T>::with_capacity(0);
    let mut list = ListBuilder::with_capacity(values, 1).with_field(item);
    match format {
        Format::Text => {
            let text = std::str::from_utf8(bytes).map_err(invalid)?.trim();
            let (text, count) = if let Some(bounds) = text.strip_prefix('[') {
                let (bounds, contents) = bounds
                    .split_once(']')
                    .ok_or_else(|| invalid("Invalid PostgreSQL array bounds"))?;
                let contents = contents
                    .trim_start()
                    .strip_prefix('=')
                    .ok_or_else(|| invalid("PostgreSQL array must be one-dimensional"))?;
                let (lower, upper) = bounds
                    .split_once(':')
                    .ok_or_else(|| invalid("Invalid PostgreSQL array bounds"))?;
                let lower: i32 = lower.trim().parse().map_err(invalid)?;
                let upper: i32 = upper.trim().parse().map_err(invalid)?;
                let count =
                    usize::try_from(i64::from(upper) - i64::from(lower) + 1).map_err(invalid)?;
                (contents.trim(), Some(count))
            } else {
                (text, None)
            };
            let inner = text
                .strip_prefix('{')
                .and_then(|text| text.strip_suffix('}'))
                .ok_or_else(|| invalid("PostgreSQL array must be one-dimensional"))?;
            if !inner.trim().is_empty() {
                visit_array_elements(text, true, |value| {
                    list.values()
                        .append_option(value.map(|value| parse(value.trim())).transpose()?);
                    Ok(())
                })?;
            }
            if count.is_some_and(|count| count != list.values().len()) {
                return Err(invalid(
                    "PostgreSQL array bounds do not match its element count",
                ));
            }
        }
        Format::Binary => visit_binary(bytes, oid, |bytes| {
            list.values().append_option(bytes.map(&decode).transpose()?);
            Ok(())
        })?,
    }
    i32::try_from(list.values().len()).map_err(invalid)?;
    list.append(true);
    Ok(list.finish())
}

fn visit_binary(
    mut input: &[u8],
    oid: i32,
    mut visit: impl FnMut(Option<&[u8]>) -> Result<()>,
) -> Result<()> {
    fn integer(input: &mut &[u8]) -> Result<i32> {
        let (bytes, rest) = input
            .split_first_chunk()
            .ok_or_else(|| invalid("Truncated PostgreSQL binary value"))?;
        *input = rest;
        Ok(i32::from_be_bytes(*bytes))
    }
    let dimensions = integer(&mut input)?;
    let flags = integer(&mut input)?;
    if integer(&mut input)? != oid {
        return Err(invalid("Unsupported binary array element OID"));
    }
    if !matches!(flags, 0 | 1) {
        return Err(invalid("Invalid binary array flags"));
    }
    let count = match dimensions {
        0 => 0,
        1 => {
            let count = integer(&mut input)?;
            let lower = integer(&mut input)?;
            lower
                .checked_add(count)
                .ok_or_else(|| invalid("Invalid PostgreSQL array bounds"))?;
            usize::try_from(count).map_err(invalid)?
        }
        _ => return Err(invalid("PostgreSQL array must be one-dimensional")),
    };
    if count > input.len() / 4 {
        return Err(invalid("Binary array count exceeds remaining bytes"));
    }
    for _ in 0..count {
        let length = integer(&mut input)?;
        if length == -1 {
            visit(None)?;
        } else {
            let length = usize::try_from(length).map_err(invalid)?;
            let (bytes, rest) = input
                .split_at_checked(length)
                .ok_or_else(|| invalid("Truncated PostgreSQL binary value"))?;
            visit(Some(bytes))?;
            input = rest;
        }
    }
    if !input.is_empty() {
        return Err(invalid("Trailing bytes in PostgreSQL binary value"));
    }
    Ok(())
}

pub(crate) fn read_value(ty: PgType, format: Format, bytes: &[u8]) -> Result<Value> {
    let field = rtdb_columnar::to_field("", &ty.logical_type().expect("float array type"))?;
    let arrow_schema::DataType::List(item) = field.data_type() else {
        unreachable!()
    };
    let array = read(ty, format, bytes, Arc::clone(item))?;
    Ok(Value::List(
        ListValue::from_numeric_arrow(Arc::clone(item), Arc::clone(array.values()))
            .expect("float array elements use numeric buffers"),
    ))
}

#[cfg(test)]
mod tests {
    use super::read;
    use crate::Format;
    use arrow_array::{Array, cast::AsArray, types::Float32Type};
    use arrow_schema::DataType;
    use rtdb_columnar::{ArrowCell, MemoryBudget};
    use rtdb_types::PgType;
    use std::sync::Arc;

    #[test]
    fn float_array_codecs_keep_nulls_and_exact_binary_components() {
        let memory = MemoryBudget::new(1 << 20);
        let ty = PgType::Float4s;
        let logical = ty.logical_type().unwrap();
        let field = rtdb_columnar::to_field("vector", &logical).unwrap();
        let DataType::List(item) = field.data_type() else {
            panic!()
        };
        let text = read(
            ty,
            Format::Text,
            b"[0:2]={ -0, NULL, NaN }",
            Arc::clone(item),
        )
        .unwrap();
        let values = text.values().as_primitive::<Float32Type>();
        assert_eq!(values.value(0).to_bits(), (-0.0f32).to_bits());
        assert!(values.is_null(1));
        assert!(values.value(2).is_nan());
        let binary = [1, 1, 700, 3, 1, 4, i32::MIN, -1, 4, 0x7fa0_0001];
        let bytes = binary
            .into_iter()
            .flat_map(i32::to_be_bytes)
            .collect::<Vec<_>>();
        let array = read(ty, Format::Binary, &bytes, Arc::clone(item)).unwrap();
        let cell = ArrowCell {
            field: &field,
            array: &array,
            row: 0,
        };
        let encoded = crate::encode_arrow(
            ty,
            Format::Binary,
            &logical,
            cell,
            &Default::default(),
            &memory,
        )
        .unwrap()
        .unwrap();
        assert_eq!(encoded.as_slice(), bytes);
        for malformed in [
            b"{{1,2}}".as_slice(),
            b"[1:3]={1}",
            b"{1,}",
            br#"{1"2"}"#,
            br#"{"1"2}"#,
            b"",
        ] {
            assert!(read(ty, Format::Text, malformed, Arc::clone(item)).is_err());
        }
        let mut multidimensional = bytes;
        multidimensional[3] = 2;
        assert!(read(ty, Format::Binary, &multidimensional, Arc::clone(item)).is_err());
    }

    #[test]
    fn float8_array_uses_standard_empty_binary_header() {
        let memory = MemoryBudget::new(1 << 20);
        let ty = PgType::Float8s;
        let logical = ty.logical_type().unwrap();
        let field = rtdb_columnar::to_field("vector", &logical).unwrap();
        let DataType::List(item) = field.data_type() else {
            panic!()
        };
        let array = read(ty, Format::Text, b"{}", Arc::clone(item)).unwrap();
        assert_eq!(array.values().data_type(), &DataType::Float64);
        let cell = ArrowCell {
            field: &field,
            array: &array,
            row: 0,
        };
        let bytes = crate::encode_arrow(
            ty,
            Format::Binary,
            &logical,
            cell,
            &Default::default(),
            &memory,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            bytes.as_slice(),
            [0i32, 0, 701]
                .into_iter()
                .flat_map(i32::to_be_bytes)
                .collect::<Vec<_>>()
        );
        assert!(
            read(ty, Format::Binary, bytes.as_slice(), Arc::clone(item))
                .unwrap()
                .values()
                .is_empty()
        );
    }
}
