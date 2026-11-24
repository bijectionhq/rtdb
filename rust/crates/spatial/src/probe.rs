use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BinaryArray, BooleanArray, ListArray, StringArray, StructArray, UInt32Array};
use arrow_schema::{DataType, Field, Fields};
use rtdb_columnar::{MemoryBudget, MemoryReservation, selected_cell};
use rtdb_types::Result;

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
) -> Result<Vec<usize>> {
    if probe.fields.is_empty() {
        return Ok(columns.to_vec());
    }
    let schema = hits[0].source.batch().schema_ref();
    let reservation = memory.reserve(4096 + columns.len() * (256 + hits.len() * 8))?;
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
    Ok(output)
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
            typed_value_size(&fields[0], value.as_ref(), row, 0)?.0
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

fn typed_value_size(field: &Field, array: &dyn Array, row: usize, depth: usize) -> Option<(u32, bool)> {
    let Some((field, array, row)) = selected_cell(field, array, row).ok()? else {
        return Some((1, true));
    };
    if field.metadata().contains_key(rtdb_columnar::EXTENSION_NAME_KEY) {
        return None;
    }
    let size = match field.data_type() {
        DataType::Null => return Some((1, true)),
        DataType::Boolean => 1,
        DataType::Int64 | DataType::Float64 => 9,
        DataType::Utf8 => add(2, u32::try_from(array.as_any().downcast_ref::<StringArray>()?.value(row).len()).ok()?)?,
        DataType::Binary => add(2, u32::try_from(array.as_any().downcast_ref::<BinaryArray>()?.value(row).len()).ok()?)?,
        DataType::List(item) if depth < MAX_DEPTH => {
            let list = array.as_any().downcast_ref::<ListArray>()?;
            let offsets = list.value_offsets();
            let mut entries = usize::try_from(offsets[row]).ok()?..usize::try_from(offsets[row + 1]).ok()?;
            if entries.len() > 8192 { return None; }
            entries.try_fold(2, |size, index| add(size, typed_value_size(item, list.values().as_ref(), index, depth + 1)?.0))?
        },
        DataType::Struct(fields) if depth < MAX_DEPTH && fields.len() <= 2048 => {
            let value = array.as_any().downcast_ref::<StructArray>()?;
            let mut size = 2;
            let mut previous = None;
            let mut used = 0;
            let mut members = 0;
            for (index, member) in fields.iter().enumerate() {
                let Some(name) = member.name().strip_prefix("value_") else { continue };
                if !clear_field_name(name.as_bytes()) || previous.is_some_and(|prior| prior >= name) { return None; }
                previous = Some(name);
                used += 1;
                members += 1;
                if members > 1024 { return None; }
                let (bytes, null) = typed_value_size(member, value.column(index).as_ref(), row, depth + 1)?;
                if let Some((position, presence)) = fields.iter().enumerate().find(|(_, field)| {
                    field.name().strip_prefix("is_present_") == Some(name)
                }) {
                    used += 1;
                    let flags = value.column(position).as_any().downcast_ref::<BooleanArray>()?;
                    if flags.is_null(row) || presence.metadata().contains_key(rtdb_columnar::EXTENSION_NAME_KEY) { return None; }
                    if !flags.value(row) {
                        if !null { return None; }
                        continue;
                    }
                }
                size = add(add(size, u32::try_from(name.len()).ok()? + 1)?, bytes)?;
            }
            if used != fields.len() { return None; }
            size
        },
        _ => return None,
    };
    Some((size, false))
}

fn clear_field_name(key: &[u8]) -> bool {
    key.len() <= 1024 && key.first() != Some(&b'$') && key.iter().all(|byte| (32..127).contains(byte))
}

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
                    if !clear_field_name(key)
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
    fn typed_compound_sizes_match_canonical_missing_and_null_members() {
        let object = StructArray::new(
            vec![Field::new("value_note", DataType::Utf8, true), Field::new("is_present_note", DataType::Boolean, false)].into(),
            vec![
                Arc::new(StringArray::from(vec![None, None, Some("x"), Some("hidden")])),
                Arc::new(BooleanArray::from(vec![false, true, true, false])),
            ], None,
        );
        let items = Arc::new(Field::new("item", object.data_type().clone(), false));
        let list = ListArray::new(items, arrow_buffer::OffsetBuffer::new(vec![0, 1, 2, 3, 4].into()), Arc::new(object), None);
        let wrapped = StructArray::new(
            vec![Field::new("v2_value", list.data_type().clone(), true)].into(), vec![Arc::new(list)], None,
        );
        let field = Field::new("c_list", wrapped.data_type().clone(), true);
        for (row, canonical) in [
            &[12, 13, 0, 0, 0, 0][..],
            &[12, 13, b'n', b'o', b't', b'e', 0, 1, 2, 0, 0, 0, 0],
            &[12, 13, b'n', b'o', b't', b'e', 0, 1, 9, b'x', 0, 1, 0, 0, 0, 0],
        ].into_iter().enumerate() {
            assert_eq!(cell_size(&field, &wrapped, row), Some(clear_image_size(canonical)));
        }
        assert_eq!(cell_size(&field, &wrapped, 3), None);
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
