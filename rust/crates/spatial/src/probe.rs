use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BinaryArray, StringArray, StructArray, UInt32Array};
use arrow_schema::{DataType, Field, Fields};
use rtdb_columnar::{MemoryBudget, MemoryReservation, selected_cell};

use crate::read::SelectedHit;

/// The full projection is already admitted by the read. These fields may be
/// represented by their exact clear value size until the caller selects a row.
#[derive(Clone, Copy)]
pub struct Probe<'a> {
    pub fields: &'a [&'a str],
    pub point_column: Option<&'a str>,
    pub certify_point: bool,
}

pub(super) struct Evidence {
    pub(super) omitted: Option<StructArray>,
    pub(super) point_intersects: bool,
    _reservation: Option<Arc<MemoryReservation>>,
}

impl Evidence {
    pub(super) fn new(point_intersects: bool) -> Self {
        Self { omitted: None, point_intersects, _reservation: None }
    }
}

pub(super) fn summarize(
    hits: &[SelectedHit], columns: &[usize], probe: Probe<'_>, evidence: &mut Evidence,
    memory: &Arc<MemoryBudget>,
) -> Vec<usize> {
    if probe.fields.is_empty() {
        return columns.to_vec();
    }
    let schema = hits[0].source.batch().schema_ref();
    let Ok(reservation) = memory.reserve(4096 + columns.len() * (256 + hits.len() * 8)) else {
        return columns.to_vec();
    };
    let mut omitted = Vec::new();
    let mut output = Vec::with_capacity(columns.len());
    for &index in columns {
        let field = schema.field(index);
        if probe.fields.contains(&field.name().as_str())
            && (evidence.point_intersects || probe.point_column != Some(field.name().as_str()))
        {
            let sizes = hits.iter().map(|hit| {
                let batch = hit.source.batch();
                cell_size(batch.schema_ref().field(index), batch.column(index).as_ref(), hit.row)
            }).collect::<Option<Vec<_>>>();
            if let Some(sizes) = sizes {
                omitted.push((
                    Arc::new(Field::new(field.name(), DataType::UInt32, true)),
                    Arc::new(UInt32Array::from(sizes)) as ArrayRef,
                ));
                continue;
            }
        }
        output.push(index);
    }
    if !omitted.is_empty() {
        let (fields, sizes): (Vec<_>, Vec<_>) = omitted.into_iter().unzip();
        evidence.omitted = Some(StructArray::new(Fields::from(fields), sizes, None));
        evidence._reservation = Some(reservation);
    }
    output
}

fn cell_size(field: &Field, array: &dyn Array, row: usize) -> Option<Option<u32>> {
    let Some((field, array, row)) = selected_cell(field, array, row).ok()? else {
        return Some(None);
    };
    if field.metadata().contains_key(rtdb_columnar::EXTENSION_NAME_KEY) {
        return None;
    }
    let size = match field.data_type() {
        DataType::Binary => clear_image_size(array.as_any().downcast_ref::<BinaryArray>()?.value(row))?,
        // Bijection's owned-fact v2 wrapper distinguishes an absent field
        // (null struct) from a present logical null (valid struct).
        DataType::Struct(fields) if fields.len() == 1 && fields[0].name() == "v2_value"
            && !fields[0].metadata().contains_key(rtdb_columnar::EXTENSION_NAME_KEY) =>
        {
            let value = array.as_any().downcast_ref::<StructArray>()?.column(0);
            let Some((field, value, row)) = selected_cell(&fields[0], value.as_ref(), row).ok()? else {
                return Some(Some(1));
            };
            if field.metadata().contains_key(rtdb_columnar::EXTENSION_NAME_KEY) {
                return None;
            }
            match field.data_type() {
                DataType::Null | DataType::Boolean => 1,
                DataType::Int64 | DataType::Float64 => 9,
                DataType::Utf8 => add(2, u32::try_from(value.as_any()
                    .downcast_ref::<StringArray>()?.value(row).len()).ok()?)?,
                DataType::Binary => add(2, u32::try_from(value.as_any()
                    .downcast_ref::<BinaryArray>()?.value(row).len()).ok()?)?,
                _ => return None,
            }
        }
        _ => return None,
    };
    Some(Some(size))
}

// Unsupported images are projected verbatim so Bijection retains their
// admission and original errors. The accepted subset follows the clear
// canonical value size and depth limits of Bijection document fields.
const MAX_SIZE: u32 = 32 << 20;
const MAX_DEPTH: usize = 15;

fn clear_image_size(mut image: &[u8]) -> Option<u32> {
    if image.len() > 3 * MAX_SIZE as usize {
        return None;
    }
    let size = value_size(&mut image, 0)?;
    image.is_empty().then_some(size)
}

fn take<'a>(image: &mut &'a [u8], count: usize) -> Option<&'a [u8]> {
    let (value, rest) = image.split_at_checked(count)?;
    *image = rest;
    Some(value)
}

fn payload_size(image: &mut &[u8], text: bool) -> Option<u32> {
    let mut size = 2;
    loop {
        let end = image.iter().position(|byte| *byte == 0)?;
        let segment = take(image, end)?;
        if text {
            std::str::from_utf8(segment).ok()?;
        }
        size = add(size, u32::try_from(end).ok()?)?;
        match take(image, 2)? {
            [0, 1] => return Some(size),
            [0, 255] => size = add(size, 1)?,
            _ => return None,
        }
    }
}

fn add(left: u32, right: u32) -> Option<u32> {
    left.checked_add(right).filter(|size| *size <= MAX_SIZE)
}

fn value_size(image: &mut &[u8], depth: usize) -> Option<u32> {
    let tag = take(image, 1)?[0];
    match tag {
        2 => Some(1),
        3 if matches!(take(image, 1)?, [0 | 1]) => Some(1),
        4 => { take(image, 8)?; Some(9) }
        5 => {
            let word = u64::from_be_bytes(take(image, 8)?.try_into().ok()?);
            let bits = if word < (1 << 63) { !word } else { word ^ (1 << 63) };
            (!f64::from_bits(bits).is_nan() || bits == 0x7ff8_0000_0000_0000).then_some(9)
        }
        9 | 10 => payload_size(image, tag == 9),
        12 | 13 if depth < MAX_DEPTH => {
            let record = tag == 13;
            let mut previous = None;
            let mut size = 2;
            let mut count = 0;
            while !image.starts_with(&[0, 0]) {
                count += 1;
                if count > if record { 1024 } else { 8192 } {
                    return None;
                }
                if record {
                    let end = image.iter().position(|byte| *byte == 0)?;
                    let key = take(image, end)?;
                    if key.len() > 1024 || key.first() == Some(&b'$')
                        || !key.iter().all(|byte| (32..127).contains(byte))
                        || previous.is_some_and(|prior: &[u8]| prior >= key)
                        || take(image, 2)? != [0, 1]
                    {
                        return None;
                    }
                    size = add(size, u32::try_from(key.len()).ok()? + 1)?;
                    previous = Some(key);
                }
                size = add(size, value_size(image, depth + 1)?)?;
            }
            take(image, 2)?;
            Some(size)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_clear_fact_preserves_absent_and_explicit_null() {
        let field = Field::new("v2_value", DataType::Utf8, true);
        let wrapped = StructArray::new(
            vec![Arc::new(field)].into(),
            vec![Arc::new(StringArray::from(vec![Some("text"), None, None]))],
            Some(arrow_buffer::NullBuffer::from(vec![true, true, false])),
        );
        let column = Field::new("c_text", wrapped.data_type().clone(), true);
        assert_eq!(cell_size(&column, &wrapped, 0), Some(Some(6)));
        assert_eq!(cell_size(&column, &wrapped, 1), Some(Some(1)));
        assert_eq!(cell_size(&column, &wrapped, 2), Some(None));
    }

    #[test]
    fn measures_the_active_typed_fact_union_leg() {
        use arrow_array::{NullArray, UnionArray};
        use arrow_schema::UnionFields;

        let union = UnionArray::try_new(
            UnionFields::try_new(
                [0, 1],
                [Field::new("null", DataType::Null, true), Field::new("utf8", DataType::Utf8, false)],
            ).unwrap(),
            vec![1_i8, 0].into(),
            Some(vec![0_i32, 0].into()),
            vec![Arc::new(NullArray::new(1)), Arc::new(StringArray::from(vec!["text"]))],
        ).unwrap();
        let wrapped = StructArray::new(
            vec![Arc::new(Field::new("v2_value", union.data_type().clone(), false))].into(),
            vec![Arc::new(union)],
            None,
        );
        let column = Field::new("c_text", wrapped.data_type().clone(), true);
        assert_eq!(cell_size(&column, &wrapped, 0), Some(Some(6)));
        assert_eq!(cell_size(&column, &wrapped, 1), Some(Some(1)));
    }

    #[test]
    fn measures_clear_images_and_declines_unadmitted_values() {
        assert_eq!(clear_image_size(&[2]), Some(1));
        assert_eq!(clear_image_size(&[9, b'a', 0, 255, b'b', 0, 1]), Some(5));
        assert_eq!(clear_image_size(&[13, b'a', 0, 1, 12, 2, 3, 0, 0, 0, 0, 0]), Some(8));
        assert_eq!(clear_image_size(&[9, 0xc2, 0, 255, 0x80, 0, 1]), None);
        assert_eq!(clear_image_size(&[13, b'$', 0, 1, 2, 0, 0]), None);
        assert_eq!(clear_image_size(&[3, 2]), None);
        assert_eq!(clear_image_size(&[2, 2]), None);
    }
}
