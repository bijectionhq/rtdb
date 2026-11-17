use rtdb_types::Result;
use super::MapKey;

pub(super) fn indices<'a>(initial: usize, keys: impl IntoIterator<Item = &'a MapKey>) -> Result<Vec<usize>> {
    if initial > i32::MAX as usize { return Err(super::super::super::error("avro-json", "Map initial block exceeds Java map capacity")); }
    crate::java_map::indices(initial, keys, |key| match key {
        MapKey::String(key) => key.encode_utf16().fold(0i32, |hash, unit| hash.wrapping_mul(31).wrapping_add(i32::from(unit))),
        MapKey::Utf8(key) => key.iter().fold(0i32, |hash, byte| hash.wrapping_mul(31).wrapping_add(i32::from(*byte as i8))),
    }, |left, right| match (left, right) {
        (MapKey::String(left), MapKey::String(right)) => left.encode_utf16().cmp(right.encode_utf16()),
        (MapKey::Utf8(left), MapKey::Utf8(right)) => left.cmp(right),
        _ => unreachable!("one Java key class per map"),
    }).ok_or_else(super::super::super::resolution::overflow)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn first_key<'a>(initial: usize, keys: impl IntoIterator<Item = &'a str>, strings: bool) -> Result<Option<String>> {
        let keys = keys.into_iter().map(|key| if strings { MapKey::String(key.into()) } else { MapKey::Utf8(key.as_bytes().to_vec()) }).collect::<Vec<_>>();
        Ok(indices(initial, keys.iter())?.first().map(|index| keys[*index].clone().into_text()))
    }
    #[test]
    fn bucket_order_precedes_insertion_order_and_duplicates_keep_position() {
        let keys = ["z", "a", "a", "b"].map(str::to_owned);
        assert_eq!(first_key(4, keys.iter().map(String::as_str), false).unwrap().as_deref(), Some("a"));
        assert_eq!(first_key(1, keys.iter().map(String::as_str), false).unwrap().as_deref(), Some("a"));
    }
    #[test]
    fn collision_tree_root_moves_to_bucket_front() {
        let keys = ["AaAaAaAa", "AaAaAaBB", "AaAaBBAa", "AaAaBBBB", "AaBBAaAa", "AaBBAaBB", "AaBBBBAa", "AaBBBBBB", "BBAaAaAa"].map(str::to_owned);
        assert_eq!(first_key(64, keys.iter().map(String::as_str), false).unwrap().as_deref(), Some("AaAaBBBB"));
    }
    #[test]
    fn utf8_hash_uses_signed_bytes_while_string_hash_uses_utf16() {
        let keys = ["â", "a"].map(str::to_owned);
        assert_eq!(first_key(16, keys.iter().map(String::as_str), false).unwrap().as_deref(), Some("â"));
        assert_eq!(first_key(16, keys.iter().map(String::as_str), true).unwrap().as_deref(), Some("a"));
    }
    #[test]
    fn tree_lookup_replaces_large_collision_families_without_moving_entries() {
        let keys = (0..4096).map(|index| MapKey::String((0..12).map(|bit| if index & (1 << bit) == 0 { "Aa" } else { "BB" }).collect())).collect::<Vec<_>>();
        let original = indices(64, keys.iter()).unwrap();
        assert_eq!(original.len(), keys.len());
        let replaced = indices(64, keys.iter().chain(keys.iter().rev())).unwrap();
        assert_eq!(replaced, original.into_iter().map(|index| keys.len() * 2 - 1 - index).collect::<Vec<_>>());
    }
    #[test]
    fn split_tree_buckets_keep_every_entry_and_replace_by_key() {
        let mut keys = (0..16).map(|index| MapKey::String(char::from_u32(index * 64 + 1).unwrap().to_string())).collect::<Vec<_>>();
        keys.extend((2..36).map(|value| MapKey::String(char::from_u32(value).unwrap().to_string())));
        let original = indices(64, keys.iter()).unwrap();
        let mut sorted = original.clone(); sorted.sort_unstable(); assert_eq!(sorted, (0..keys.len()).collect::<Vec<_>>());
        assert_eq!(indices(64, keys.iter().chain(keys.iter())).unwrap(), original.into_iter().map(|index| index + keys.len()).collect::<Vec<_>>());
    }
}
