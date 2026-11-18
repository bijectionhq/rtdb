use std::{collections::{BTreeMap, BTreeSet}, fs::File, path::Path};

use rtdb_types::{Anomaly, Result};

use super::{Values, add, invalid, product, read_text_file, super::{unescape_units, visit_properties_units}};

mod reader;

pub(super) fn selected_file<'a>(path: &Path, keys: &BTreeSet<&'a str>, reserve: &mut impl FnMut(usize) -> Result<()>) -> Result<Values<'a>> {
    let file = File::open(path).map_err(|_| invalid("Kafka ConfigProvider file could not be opened"))?;
    if !file.metadata().is_ok_and(|metadata| metadata.is_file()) {
        let text = read_text_file(file, reserve)?;
        let mut input = text.encode_utf16();
        return selected_units(|| Ok(input.next()), keys, reserve);
    }
    reserve(reader::SCRATCH_BYTES)?;
    let mut reader = reader::Reader::new(file);
    selected_units(|| reader.next_unit(), keys, reserve)
}

#[cfg(test)]
pub(super) fn selected<'a>(text: &str, keys: &BTreeSet<&'a str>) -> Result<Values<'a>> {
    let mut input = text.encode_utf16();
    selected_units(|| Ok(input.next()), keys, &mut |_| Ok(()))
}

fn selected_units<'a>(input: impl FnMut() -> Result<Option<u16>>, keys: &BTreeSet<&'a str>, reserve: &mut impl FnMut(usize) -> Result<()>) -> Result<Values<'a>> {
    let mut selected = BTreeMap::new();
    visit_properties_units(input, reserve, |key, value, reserve| {
        reserve(add(product(add(key.len(), value.len())?, 16)?, 512)?)?;
        let name = String::from_utf16(&unescape_units(key)?);
        let units = unescape_units(value)?;
        if let Some(&key) = name.ok().as_deref().and_then(|name| keys.get(name)) {
            selected.insert(key, units);
        }
        Ok(())
    })?;
    selected.into_iter().map(|(key, units)| {
        let value = String::from_utf16(&units).map_err(|_| Anomaly::unsupported(
            "xtdb.kafka/config-provider-utf16", "Selected Kafka ConfigProvider value requires a native UTF-16 setting",
        ))?;
        Ok((key, value))
    }).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn properties_decode_all_escapes_before_projecting_only_final_requested_values() {
        let keys = BTreeSet::from(["needed"]);
        let text = "outside=\\uD800\n\\uD801=unknown\nneeded=\\uD802\nneeded=final\n";
        assert_eq!(selected(text, &keys).unwrap()["needed"], "final");
        assert_eq!(selected("needed=\\uD800", &keys).unwrap_err().code, "xtdb.kafka/config-provider-utf16");
        for invalid in ["unused=\\u+001", "unused=\\u012Z", "unused=\\u1"] {
            assert_eq!(selected(invalid, &keys).unwrap_err().code, "xtdb.kafka/properties-file");
        }
    }
}
