#![allow(unexpected_cfgs)]

pub(crate) fn memory_allows_insert(reserved: bool, replacing_stage: bool, exists: bool) -> bool {
    !(reserved || (!replacing_stage && exists))
}

pub(crate) fn memory_allows_reservation(record_exists: bool) -> bool {
    !record_exists
}

#[cfg(any(feature = "dynamodb", verus_keep_ghost))]
pub(crate) mod dynamo {
    pub(crate) const ID: &str = "id";
    pub(crate) const VERSION: &str = "version";
    pub(crate) const INSTALLED_JOURNAL: &str = "installed_journal";
    pub(crate) const NEW_ITEM: &str = "attribute_not_exists(#id)";
    pub(crate) const EXACT_VERSION: &str = "#v = :expected";

    pub(crate) fn reservation_fields() -> [&'static str; 2] {
        [ID, INSTALLED_JOURNAL]
    }
}

#[cfg(test)]
mod tests {
    use super::memory_allows_insert;

    #[test]
    fn reservation_refuses_even_a_staged_replacement() {
        assert!(!memory_allows_insert(true, true, true));
        assert!(memory_allows_insert(false, true, true));
        assert!(!memory_allows_insert(false, false, true));
    }
}
