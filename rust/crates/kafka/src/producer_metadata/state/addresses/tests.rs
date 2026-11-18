use super::*;

const FIRST: ConnectionAddress = ConnectionAddress::V4([192, 0, 2, 1]);
const SECOND: ConnectionAddress = ConnectionAddress::V4([192, 0, 2, 2]);

const fn scoped(last: u8, scope_id: u32) -> ConnectionAddress {
    let mut address = [0; 16];
    address[0] = 0xfe;
    address[1] = 0x80;
    address[15] = last;
    ConnectionAddress::V6 { address, scope_id }
}

#[test]
fn preferred_family_keeps_encounter_order_and_resolves_again_only_after_exhaustion() {
    let memory = MemoryBudget::new(1 << 20);
    let mut state = ConnectionAddresses::new(memory.clone());
    state.advance();
    let family = [FIRST, scoped(1, 1), SECOND, scoped(2, 2)];
    assert_eq!(state.current(|| Ok(family.as_slice())).unwrap(), FIRST);
    assert!(memory.used_bytes() > 0);
    state.advance();
    assert_eq!(state.current(|| panic!("an available preferred address is already retained")).unwrap(), SECOND);
    state.advance();
    assert_eq!(memory.used_bytes(), 0);

    let replacement = [scoped(2, 4), FIRST, scoped(1, 4)];
    assert_eq!(state.current(|| Ok(replacement.as_slice())).unwrap(), scoped(2, 4));
    state.advance();
    assert_eq!(state.current(|| panic!("the IPv6 result remains selected")).unwrap(), scoped(1, 4));
}

#[test]
fn re_resolution_skips_only_the_first_equal_ipv6_address_regardless_of_scope() {
    let memory = MemoryBudget::new(1 << 20);
    let mut state = ConnectionAddresses::new(memory.clone());
    let original = [scoped(1, 3)];
    assert_eq!(state.current(|| Ok(original.as_slice())).unwrap(), scoped(1, 3));
    state.clear();
    assert_eq!(memory.used_bytes(), 0);

    let replacement = [scoped(1, 4), scoped(1, 5), scoped(2, 6)];
    assert_eq!(state.current(|| Ok(replacement.as_slice())).unwrap(), scoped(1, 5));
    state.advance();
    assert_eq!(state.current(|| panic!("only the first equal result was skipped")).unwrap(), scoped(2, 6));
}

#[test]
fn unavailable_empty_and_denied_results_preserve_the_last_attempt() {
    let memory = MemoryBudget::new(1 << 20);
    let mut state = ConnectionAddresses::new(memory.clone());
    let original = [FIRST];
    assert_eq!(state.current(|| Ok(original.as_slice())).unwrap(), FIRST);
    state.clear();
    let error = state.current(|| Err(Anomaly::incorrect("fixture/dns", "Name was unavailable"))).unwrap_err();
    assert_eq!(error.code, "fixture/dns");
    assert_eq!(state.current(|| Ok(&[])).unwrap_err().code, "rtdb/kafka-empty-address-list");

    let replacement = [FIRST, SECOND];
    let full = memory.reserve(memory.limit_bytes()).unwrap();
    assert!(state.current(|| Ok(replacement.as_slice())).is_err());
    drop(full);
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(state.current(|| Ok(replacement.as_slice())).unwrap(), SECOND);
}

#[test]
fn one_preferred_address_can_repeat_when_the_other_family_is_filtered_out() {
    let mut state = ConnectionAddresses::new(MemoryBudget::new(1 << 20));
    let family = [FIRST, scoped(1, 1)];
    assert_eq!(state.current(|| Ok(family.as_slice())).unwrap(), FIRST);
    state.advance();
    assert_eq!(state.current(|| Ok(family.as_slice())).unwrap(), FIRST);
}
