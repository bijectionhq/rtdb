use std::collections::BTreeSet;

use rtdb_types::{Anomaly, Result};

use super::{Values, super::{unescape_units, visit_properties_text}};

pub(super) fn selected<'a>(text: &str, keys: &BTreeSet<&'a str>) -> Result<Values<'a>> {
    let mut selected = std::collections::BTreeMap::new();
    visit_properties_text(text, |key, value| {
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
