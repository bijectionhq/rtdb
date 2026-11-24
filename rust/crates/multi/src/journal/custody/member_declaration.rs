pub(crate) fn fresh_survivor_allocation(candidate: u128, previous: u128) -> bool {
    candidate != previous
}

pub(crate) fn declares_written_member(
    address_matches: bool,
    observed_bytes: u64,
    expected_bytes: u64,
) -> bool {
    address_matches && observed_bytes == expected_bytes
}
