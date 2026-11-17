use rtdb_types::java_text::JavaString;
use super::{ConversionContext, Result, beans::{Properties, StringSet, Tags}, cursor::Cursor, owned::Values, scalars};

pub(super) fn list<T>(cursor: &mut Cursor<'_>, context: &mut ConversionContext,
    mut read: impl FnMut(&mut Cursor<'_>, &mut ConversionContext) -> Result<T>) -> Result<Option<Values<T>>> {
    if cursor.take_null(context)? { return Ok(None); }
    cursor.begin_array(context)?;
    let mut values = Values::new();
    while cursor.element()? { values.push(read(cursor, context)?, context)?; }
    Ok(Some(values))
}

pub(super) fn strings(cursor: &mut Cursor<'_>, context: &mut ConversionContext) -> Result<Option<StringSet>> {
    if cursor.take_null(context)? { return Ok(None); }
    cursor.begin_array(context)?;
    let mut values: Values<Option<JavaString>> = Values::new();
    while cursor.element()? {
        let value = scalars::text(cursor, context)?;
        if !values.as_slice().iter().any(|old| match (old, &value) {
            (None, None) => true,
            (Some(old), Some(value)) => old.view().units().eq(value.view().units()),
            _ => false,
        }) { values.push(value, context)?; }
    }
    Ok(Some(StringSet { values, sorted: false }))
}

pub(super) fn properties(cursor: &mut Cursor<'_>, context: &mut ConversionContext) -> Result<Option<Properties>> {
    if cursor.take_null(context)? { return Ok(None); }
    cursor.begin_object(context)?;
    let mut values: Values<(JavaString, Option<JavaString>)> = Values::new();
    while let Some(name) = cursor.field()? {
        let key = name.copy(context)?;
        let value = scalars::text(cursor, context)?;
        if let Some(old) = values.as_mut_slice().iter_mut().find(|(old, _)| old.view().units().eq(key.view().units())) {
            old.1 = value;
        } else { values.push((key, value), context)?; }
    }
    Ok(Some(Properties { values, sorted: false }))
}

pub(super) fn tags(cursor: &mut Cursor<'_>, context: &mut ConversionContext) -> Result<Option<Tags>> {
    if cursor.take_null(context)? { return Ok(None); }
    cursor.begin_object(context)?;
    let mut values: Values<(JavaString, Option<StringSet>)> = Values::new();
    while let Some(name) = cursor.field()? {
        let key = name.copy(context)?;
        let value = strings(cursor, context)?;
        if let Some(old) = values.as_mut_slice().iter_mut().find(|(old, _)| old.view().units().eq(key.view().units())) {
            old.1 = value;
        } else { values.push((key, value), context)?; }
    }
    Ok(Some(Tags { values, sorted: false }))
}

pub(super) fn sort_strings(value: &mut StringSet, cursor: &Cursor<'_>) -> Result<()> {
    if value.values.as_slice().iter().any(Option::is_none) { return Err(cursor.constructor("Null element in a sorted string set")); }
    value.values.as_mut_slice().sort_unstable_by(|a, b| a.as_ref().unwrap().view().units().cmp(b.as_ref().unwrap().view().units()));
    value.sorted = true;
    Ok(())
}

pub(super) fn sort_properties(value: &mut Properties) {
    value.values.as_mut_slice().sort_unstable_by(|a, b| a.0.view().units().cmp(b.0.view().units()));
    value.sorted = true;
}

pub(super) fn sort_tags(value: &mut Tags, cursor: &Cursor<'_>) -> Result<()> {
    for (_, tags) in value.values.as_mut_slice() {
        sort_strings(tags.as_mut().ok_or_else(|| cursor.constructor("Null tag set in Metadata constructor"))?, cursor)?;
    }
    value.values.as_mut_slice().sort_unstable_by(|a, b| a.0.view().units().cmp(b.0.view().units()));
    value.sorted = true;
    Ok(())
}
