#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == !(reserved || (!replacing_stage && exists)),
))]
pub(crate) fn memory_allows_insert(reserved: bool, replacing_stage: bool, exists: bool) -> bool {
    !(reserved || (!replacing_stage && exists))
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == !record_exists,
))]
pub(crate) fn memory_allows_reservation(record_exists: bool) -> bool {
    !record_exists
}

#[cfg(any(feature = "dynamodb", verus_keep_ghost))]
pub(crate) mod dynamo {
    #[cfg(verus_keep_ghost)]
    use vstd::prelude::*;

    /// The deployments and members partition key and the names table's deployment pointer.
    pub(crate) const ID: &str = "id";
    pub(crate) const VERSION: &str = "version";
    pub(crate) const INSTALLED_JOURNAL: &str = "installed_journal";
    pub(crate) const NEW_ITEM: &str = "attribute_not_exists(#id)";
    pub(crate) const EXACT_VERSION: &str = "#v = :expected";

    #[cfg(verus_keep_ghost)]
    verus! {
    proof fn dynamo_condition_names_match_backend_contract()
        ensures VERSION@ == "version"@, NEW_ITEM@ == "attribute_not_exists(#id)"@,
            EXACT_VERSION@ == "#v = :expected"@,
    {}
    }

    #[cfg_attr(verus_keep_ghost, verus_spec(out =>
        ensures out@[0]@ == "id"@, out@[1]@ == "installed_journal"@,
    ))]
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
