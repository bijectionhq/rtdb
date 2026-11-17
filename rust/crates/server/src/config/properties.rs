use std::collections::BTreeMap;
use std::sync::Arc;
use rtdb_columnar::MemoryBudget;
use rtdb_types::{ObjectSort,Result};

const MAX_BYTES: usize = 4096;

/// Decodes a UTF-8 JSON object of supported property names to string values under the startup budget.
/// Only `java.util.Arrays.useLegacyMergeSort` is supported; absent selects TimSort and duplicate keys use their last value.
/// Values use [`ObjectSort::from_property`]: only case-insensitive `true`, without trimming, selects legacy sorting.
/// Input above 4096 bytes, unknown properties, nonstring values, invalid JSON and exhausted startup memory are errors.
/// The returned setting owns no strings and performs no subsequent process lookup.
pub fn object_sort_properties(text: &str,memory: &Arc<MemoryBudget>) -> Result<ObjectSort> {
    if text.len() > MAX_BYTES { return Err(super::invalid("Native system properties exceed 4096 bytes")); }
    let _reservation = memory.reserve(text.len() * 128 + 4096)?;
    let properties: BTreeMap<String,String> = serde_json::from_str(text).map_err(|_|super::invalid("Native system properties must be a JSON object of strings"))?;
    if properties.keys().any(|key|key != ObjectSort::PROPERTY) { return Err(super::invalid("Unsupported native system property")); }
    Ok(ObjectSort::from_property(properties.get(ObjectSort::PROPERTY).map(String::as_str)))
}

pub(crate) fn command_property(argument: &str) -> Result<ObjectSort> {
    if argument.len() > MAX_BYTES { return Err(super::invalid("Native system property exceeds 4096 bytes")); }
    let property = argument.strip_prefix("-D").ok_or_else(||super::invalid("Native system property must start with -D"))?;
    let (name,value) = property.split_once('=').unwrap_or((property,""));
    if name != ObjectSort::PROPERTY { return Err(super::invalid("Unsupported native system property")); }
    Ok(ObjectSort::from_property(Some(value)))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn property_decode_is_bounded_known_only_and_releases_its_scratch() {
        let memory = MemoryBudget::new(1 << 20);
        assert_eq!(object_sort_properties(r#"{"java.util.Arrays.useLegacyMergeSort":"TRUE"}"#,&memory).unwrap(),ObjectSort::LegacyMergeSort);
        assert_eq!(object_sort_properties(r#"{"java.util.Arrays.useLegacyMergeSort":"true","java.util.Arrays.useLegacyMergeSort":""}"#,&memory).unwrap(),ObjectSort::TimSort);
        for input in [r#"{"xtdb.vector.null-checks":"false"}"#,r#"{"java.util.Arrays.useLegacyMergeSort":true}"#,"[]"] { assert!(object_sort_properties(input,&memory).is_err()); }
        assert!(object_sort_properties(&" ".repeat(MAX_BYTES + 1),&memory).is_err());
        assert!(object_sort_properties("{}",&MemoryBudget::new(1)).is_err());
        assert_eq!(memory.used_bytes(),0);
    }
}
