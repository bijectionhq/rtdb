use super::*;

fn der(tag: u8, bytes: &[u8]) -> Vec<u8> {
    let mut encoded = vec![tag];
    if bytes.len() < 128 { encoded.push(bytes.len() as u8); }
    else { let length = bytes.len().to_be_bytes(); let first = length.iter().position(|byte| *byte != 0).unwrap(); encoded.push(0x80 | (length.len() - first) as u8); encoded.extend_from_slice(&length[first..]); }
    encoded.extend_from_slice(bytes); encoded
}
fn assertion(oid: &[u8], tag: u8, value: &[u8]) -> Vec<u8> { der(0x30, &[der(6, oid), der(tag, value)].concat()) }
fn cn(tag: u8, value: &[u8]) -> Vec<u8> { assertion(&[0x55, 4, 3], tag, value) }
fn name(rdns: Vec<Vec<Vec<u8>>>) -> Vec<u8> { der(0x30, &rdns.into_iter().flat_map(|values| der(0x31, &values.concat())).collect::<Vec<_>>()) }
fn decode(bytes: &[u8]) -> DistinguishedName { DistinguishedName::decode(bytes, &MemoryBudget::new(16 << 20)).unwrap() }

#[test]
fn names_match_unicode_case_expansion_and_compatibility_decomposition() {
    for (left, right) in [("ÉCOLE CA", "e\u{301}cole ca"), ("Straße CA", "STRASSE CA"), ("ﬃ CA", "ffi ca"), ("K CA", "k ca"), ("\u{10400} CA", "\u{10428} ca")] {
        assert_eq!(decode(&name(vec![vec![cn(12, left.as_bytes())]])), decode(&name(vec![vec![cn(12, right.as_bytes())]])), "{left:?}");
    }
}

#[test]
fn only_printable_and_utf8_values_with_standard_keywords_share_text_semantics() {
    let utf8 = decode(&name(vec![vec![cn(12, "ÉCOLE".as_bytes())]]));
    assert_eq!(utf8, decode(&name(vec![vec![cn(19, b"\xc9COLE")]])));
    for tag in [20, 22, 27, 30] {
        let value = decode(&name(vec![vec![cn(tag, b"CA")]]));
        assert_ne!(value, decode(&name(vec![vec![cn(12, b"CA")]])));
        assert!(value.canonical().starts_with("cn=#"));
    }
    let unknown = |value: &[u8]| name(vec![vec![assertion(&[0x55, 4, 5], 12, value)]]);
    assert_ne!(decode(&unknown(b"CA")), decode(&unknown(b"ca")));
    assert_eq!(decode(&unknown(b"CA")).canonical(), "2.5.4.5=#0c024341");
    assert_eq!(decode(&name(vec![vec![assertion(&[9, 0x92, 0x26, 0x89, 0x93, 0xf2, 0x2c, 0x64, 1, 0x19], 22, b"EXAMPLE")]])).canonical(), "dc=#16074558414d504c45");
}

#[test]
fn canonical_names_preserve_rdn_order_and_duplicate_assertions() {
    let country = assertion(&[0x55, 4, 6], 19, b"FR");
    let common = cn(12, b"Example");
    let left = decode(&name(vec![vec![country.clone(), common.clone()]]));
    assert_eq!(left.canonical(), "c=fr+cn=example");
    assert_eq!(left, decode(&name(vec![vec![common.clone(), country.clone()]])));
    assert_ne!(left, decode(&name(vec![vec![common.clone(), country.clone(), country.clone()]])));
    assert_ne!(decode(&name(vec![vec![country.clone()], vec![common.clone()]])), decode(&name(vec![vec![common], vec![country]])));
}

#[test]
fn trimming_escaping_and_space_collapse_follow_ava_canonicalization_order() {
    let canonical = |value: &str| decode(&name(vec![vec![cn(12, value.as_bytes())]])).canonical;
    assert_eq!(canonical("\t  A   B  \r\n"), "cn=a b");
    assert_eq!(canonical("A\t\tB"), "cn=a\t\tb");
    assert_eq!(canonical("#A,+<>;\"\\"), "cn=\\#a\\,\\+\\<\\>\\;\\\"\\\\");
    assert_eq!(canonical(" #A "), "cn=#a");
    assert_ne!(canonical("#A"), canonical(" #A"));
    assert_eq!(canonical("\u{a0}A\u{a0}"), "cn= a ");
    assert_ne!(canonical("\u{a0}A\u{a0}"), canonical(" A "));
}

#[test]
fn malformed_utf8_uses_java_replacement_and_bmp_validation_uses_decoded_characters() {
    let surrogate = decode(&name(vec![vec![cn(12, &[0xed, 0xa0, 0x80])]]));
    assert_eq!(surrogate.canonical(), "cn=\u{fffd}");
    assert_eq!(decode(&name(vec![vec![cn(30, &[0xd8, 0])]])).canonical(), "cn=#1e02d800");
    assert_eq!(decode(&name(vec![vec![cn(30, &[0xd8, 0, 0xd8, 0, 0xdc, 0])]])).canonical(), "cn=#1e06d800d800dc00");
    let supplementary = name(vec![vec![cn(30, &[0xd8, 0, 0xdc, 0])]]);
    assert!(DistinguishedName::decode(&supplementary, &MemoryBudget::new(1 << 20)).is_err());
}

#[test]
fn arbitrary_oid_arcs_and_java_oid_limits_are_preserved() {
    assert_eq!(dotted_oid(&[0x88, 0x37, 3]).unwrap(), "2.999.3");
    assert_eq!(dotted_oid(&[0x82, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0]).unwrap(), "2.18446744073709551536");
    for bytes in [vec![], vec![0x80, 0], vec![0x81], vec![1; 4097]] { assert!(dotted_oid(&bytes).is_err()); }
}

#[test]
fn empty_unwrapped_and_ber_names_follow_principal_input_framing() {
    assert_eq!(decode(&[]).canonical(), "");
    assert_eq!(decode(&[0x30, 0]).canonical(), "");
    let assertion = cn(12, b"CA");
    let unwrapped = der(0x31, &assertion);
    let expected = decode(&der(0x30, &unwrapped));
    assert_eq!(decode(&unwrapped), expected);
    assert_eq!(decode(&[vec![0x30, 0x80], unwrapped.clone(), vec![0, 0]].concat()), expected);
    assert_eq!(decode(&[vec![0x30, 0x81, unwrapped.len() as u8], unwrapped].concat()), expected);
    for bytes in [vec![0x30, 0x80], vec![0x30, 0xff], vec![0x30, 2, 0x31, 1], name(vec![vec![der(0x30, &[der(6, &[0x55, 4, 3]), der(12, b"x"), der(5, &[])].concat())]])] {
        assert!(DistinguishedName::decode(&bytes, &MemoryBudget::new(1 << 20)).is_err());
    }
}

fn unknown_value(value: &[u8]) -> Vec<u8> { name(vec![vec![der(0x30, &[der(6, &[0x55, 4, 5]), value.to_vec()].concat())]]) }

#[test]
fn nested_indefinite_values_normalize_scanned_headers_and_preserve_opaque_bodies() {
    let nested = [0x30, 0x80, 0x30, 0x80, 4, 1, b'x', 0, 0, 0, 0];
    assert_eq!(decode(&unknown_value(&nested)).canonical(), "2.5.4.5=#30053003040178");
    let opaque = [0x30, 7, 0x30, 0x80, 4, 1, b'x', 0, 0];
    assert_eq!(decode(&unknown_value(&opaque)).canonical(), "2.5.4.5=#300730800401780000");
    let outer = [vec![0x30, 0x80], opaque.to_vec(), vec![0, 0]].concat();
    assert_eq!(decode(&unknown_value(&outer)).canonical(), "2.5.4.5=#3009300730800401780000");
    let opaque_invalid = [0x30, 0x80, 0x30, 1, 0xff, 0, 0];
    assert_eq!(decode(&unknown_value(&opaque_invalid)).canonical(), "2.5.4.5=#30033001ff");
    let opaque_primitive = [0x30, 0x80, 4, 0x80, 4, 1, b'x', 0, 0, 0, 0];
    assert_eq!(decode(&unknown_value(&opaque_primitive)).canonical(), "2.5.4.5=#30050403040178");
}

#[test]
fn indefinite_conversion_preserves_java_padding_and_ignored_trailing_input() {
    let redundant_length = [0x30, 0x80, 4, 0x81, 1, b'x', 0, 0];
    assert_eq!(decode(&unknown_value(&redundant_length)).canonical(), "2.5.4.5=#300404017800");
    let unwrapped = der(0x31, &cn(12, b"CA"));
    let trailing = [vec![0x30, 0x80], unwrapped.clone(), vec![0, 0, 0xff, 0x80]].concat();
    assert_eq!(decode(&trailing), decode(&unwrapped));
}

#[test]
fn deeply_nested_indefinite_values_use_linear_scratch_without_a_depth_cutoff() {
    let mut indefinite = Vec::new();
    for _ in 0..1024 { indefinite.extend_from_slice(&[0x30, 0x80]); }
    indefinite.extend_from_slice(&[4, 1, b'x']);
    for _ in 0..1024 { indefinite.extend_from_slice(&[0, 0]); }
    let mut definite = vec![4, 1, b'x'];
    for _ in 0..1024 { definite = der(0x30, &definite); }
    assert_eq!(decode(&unknown_value(&indefinite)), decode(&unknown_value(&definite)));
    indefinite.truncate(indefinite.len() - 1);
    assert!(DistinguishedName::decode(&unknown_value(&indefinite), &MemoryBudget::new(16 << 20)).is_err());
}

#[test]
fn name_reservations_are_admitted_before_parsing_and_released_with_each_value() {
    let encoded = name(vec![vec![cn(12, "ÉCOLE CA".as_bytes())]]);
    let memory = MemoryBudget::new(1 << 20);
    let first = DistinguishedName::decode(&encoded, &memory).unwrap();
    let retained = memory.used_bytes(); assert!(retained > encoded.len());
    let second = DistinguishedName::decode(&encoded, &memory).unwrap();
    assert_eq!(memory.used_bytes(), retained * 2);
    drop(first); assert_eq!(memory.used_bytes(), retained);
    drop(second); assert_eq!(memory.used_bytes(), 0);
    assert_eq!(DistinguishedName::decode(&[0xff], &MemoryBudget::new(0)).err().unwrap().code(), "rtdb/memory-budget-exceeded");
}

#[test]
fn common_ancestor_distance_uses_highest_first_structural_rdns() {
    let country = assertion(&[0x55, 4, 6], 19, b"FR");
    let organization = assertion(&[0x55, 4, 10], 12, b"Example");
    let anchor = decode(&name(vec![vec![country.clone()], vec![organization.clone()], vec![cn(12, b"root")]]));
    let sibling = decode(&name(vec![vec![country.clone()], vec![organization.clone()], vec![cn(12, b"issuer")]]));
    let ancestor = decode(&name(vec![vec![country.clone()], vec![organization.clone()]]));
    assert_eq!(anchor.distance_to_common_non_geographical_ancestor(&sibling), Some(1));
    assert_eq!(anchor.distance_to_common_non_geographical_ancestor(&ancestor), Some(0));
    assert_eq!(ancestor.distance_to_common_non_geographical_ancestor(&anchor), Some(1));
    assert_eq!(anchor.distance_to_common_non_geographical_ancestor(&anchor), Some(0));
    assert_eq!(anchor.distance_to_common_non_geographical_ancestor(&decode(&name(vec![vec![country], vec![cn(12, b"other")]]))), None);
    assert_eq!(anchor.distance_to_common_non_geographical_ancestor(&decode(&[])), None);
}

#[test]
fn geography_uses_every_original_oid_in_the_last_common_rdn() {
    for oid in [6, 7, 8, 9] {
        let rdn = vec![assertion(&[0x55, 4, oid], 12, b"place"), assertion(&[0x55, 4, 10], 12, b"Example")];
        let anchor = decode(&name(vec![rdn.clone()]));
        let issuer = decode(&name(vec![rdn, vec![cn(12, b"issuer")]]));
        assert_eq!(anchor.distance_to_common_non_geographical_ancestor(&issuer), None);
    }
    let geographical_parent = vec![assertion(&[0x55, 4, 6], 19, b"FR")];
    let organization = vec![assertion(&[0x55, 4, 10], 12, b"Example")];
    let anchor = decode(&name(vec![geographical_parent.clone(), organization.clone()]));
    let issuer = decode(&name(vec![geographical_parent, organization, vec![cn(12, b"issuer")]]));
    assert_eq!(anchor.distance_to_common_non_geographical_ancestor(&issuer), Some(1));
}

#[test]
fn compatibility_delimiters_do_not_create_rdns_or_geographical_attributes() {
    for value in ["Org，cn=child", "Org＋c=FR", "Org,cn=child", "Org+c=FR"] {
        let rdn = vec![assertion(&[0x55, 4, 10], 12, value.as_bytes())];
        let anchor = decode(&name(vec![rdn.clone()]));
        let issuer = decode(&name(vec![rdn, vec![cn(12, b"issuer")]]));
        assert_eq!(anchor.distance_to_common_non_geographical_ancestor(&issuer), Some(1), "{value}");
        assert_eq!(anchor.distance_to_common_non_geographical_ancestor(&anchor), Some(0), "{value}");
    }
}

#[test]
fn ancestor_comparison_preserves_rdn_equality_and_name_reservations() {
    let memory = MemoryBudget::new(2 << 20);
    let left = name(vec![vec![assertion(&[0x55, 4, 10], 12, "ÉCOLE".as_bytes()), cn(12, b"CA")]]);
    let right = name(vec![vec![cn(19, b"ca"), assertion(&[0x55, 4, 10], 12, "e\u{301}cole".as_bytes())], vec![cn(12, b"issuer")]]);
    let anchor = DistinguishedName::decode(&left, &memory).unwrap();
    let issuer = DistinguishedName::decode(&right, &memory).unwrap();
    assert_eq!(anchor.distance_to_common_non_geographical_ancestor(&issuer), Some(1));
    assert!(memory.used_bytes() > 0);
    drop(anchor); drop(issuer); assert_eq!(memory.used_bytes(), 0);
    assert!(DistinguishedName::decode(&left, &MemoryBudget::new(1)).is_err());
}

#[test]
fn common_ancestor_does_not_merge_distinct_ava_counts_after_nfkd() {
    let single = decode(&name(vec![vec![assertion(&[0x55, 4, 10], 12, "Org＋ou=Team".as_bytes())]]));
    let multiple = decode(&name(vec![vec![assertion(&[0x55, 4, 10], 12, b"Org"), assertion(&[0x55, 4, 11], 12, b"Team")]]));
    assert_eq!(single.canonical(), multiple.canonical());
    assert_eq!(single.distance_to_common_non_geographical_ancestor(&multiple), None);
}
