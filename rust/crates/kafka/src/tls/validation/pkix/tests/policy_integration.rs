use super::*;

const P: &[u8] = &[0x2a, 3, 4];
const Q: &[u8] = &[0x2a, 3, 5];
const R: &[u8] = &[0x2a, 3, 6];
const ANY: &[u8] = &[0x55, 0x1d, 0x20, 0];

fn der(tag: u8, bytes: &[u8]) -> Vec<u8> {
    assert!(bytes.len() < 128);
    let mut encoded = vec![tag, bytes.len() as u8]; encoded.extend_from_slice(bytes); encoded
}
fn extension(oid: &str, critical: bool, bytes: &[u8]) -> X509Extension {
    X509Extension::new_from_der(&Asn1Object::from_str(oid).unwrap(), critical, &Asn1OctetString::new_from_bytes(bytes).unwrap()).unwrap()
}
fn policies(oid: &[u8], qualified: bool) -> X509Extension {
    let mut fields = der(6, oid);
    if qualified {
        let qualifier = [der(6, &[0x2b, 6, 1, 5, 5, 7, 2, 1]), der(0x16, b"x")].concat();
        fields.extend(der(0x30, &der(0x30, &qualifier)));
    }
    extension("2.5.29.32", true, &der(0x30, &der(0x30, &fields)))
}
fn require(count: u8) -> X509Extension { extension("2.5.29.36", true, &der(0x30, &der(0x80, &[count]))) }
fn mapping(from: &[u8], to: &[u8]) -> X509Extension {
    extension("2.5.29.33", true, &der(0x30, &der(0x30, &[der(6, from), der(6, to)].concat())))
}

#[test]
fn native_pkix_ignores_malformed_noncritical_policy_extensions_and_rejects_critical_ones() {
    let root_key = key(1024); let root = certificate("root", &root_key, None, true, VALID, MessageDigest::sha256(), vec![]);
    let issuer_key = key(1024); let issuer = certificate("issuer", &issuer_key, Some((&root, &root_key)), true, VALID, MessageDigest::sha256(), vec![]);
    let leaf_key = key(1024);
    let memory = MemoryBudget::new(1 << 24); let charge = memory.reserve(1024).unwrap();
    for oid in ["2.5.29.32", "2.5.29.33", "2.5.29.36", "2.5.29.54"] {
        for critical in [false, true] {
            let leaf = certificate("server", &leaf_key, Some((&issuer, &issuer_key)), false, VALID, MessageDigest::sha256(), vec![extension(oid, critical, &[5, 0])]);
            assert_eq!(verify_store(&store(&[root.clone()]), &[leaf.clone(), issuer.clone()], &now(), &charge).is_ok(), !critical, "{oid}, critical={critical}");
            assert!(verify_store(&store(&[leaf.clone()]), &[leaf], &now(), &charge).is_ok());
            assert_eq!(memory.used_bytes(), 1024);
        }
    }
    for critical in [false, true] {
        let leaf = certificate("server", &leaf_key, Some((&root, &root_key)), false, VALID, MessageDigest::sha256(), vec![extension("1.2.3.99", critical, &[5, 0])]);
        assert_eq!(verify_store(&store(&[root.clone()]), &[leaf], &now(), &charge).is_ok(), !critical);
    }
    let wrong_signer = key(1024);
    let leaf = certificate("server", &leaf_key, Some((&root, &wrong_signer)), false, VALID, MessageDigest::sha256(), vec![extension("2.5.29.32", false, &[5, 0])]);
    assert!(verify_store(&store(&[root]), &[leaf], &now(), &charge).is_err());
    drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn native_pkix_uses_java_ber_normalization_before_policy_tree_processing() {
    let root_key = key(1024); let root = certificate("root", &root_key, None, true, VALID, MessageDigest::sha256(), vec![]);
    let issuer_key = key(1024); let leaf_key = key(1024);
    let bytes = [0x30, 0x80, 0x30, 0x81, 5, 6, 3, 0x2a, 3, 4, 0, 0];
    let memory = MemoryBudget::new(1 << 24); let charge = memory.reserve(1024).unwrap();
    for critical in [false, true] {
        let issuer = certificate("issuer", &issuer_key, Some((&root, &root_key)), true, VALID, MessageDigest::sha256(), vec![extension("2.5.29.32", critical, &bytes)]);
        let leaf = certificate("server", &leaf_key, Some((&issuer, &issuer_key)), false, VALID, MessageDigest::sha256(), vec![policies(P, true)]);
        assert_eq!(verify_store(&store(&[root.clone()]), &[leaf, issuer], &now(), &charge).is_ok(), !critical);
    }
    drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn native_pkix_keeps_one_parsed_policy_among_unparseable_noncritical_duplicates() {
    let root_key = key(1024); let root = certificate("root", &root_key, None, true, VALID, MessageDigest::sha256(), vec![]);
    let issuer_key = key(1024); let leaf_key = key(1024);
    let memory = MemoryBudget::new(1 << 24); let charge = memory.reserve(1024).unwrap();
    for malformed_first in [false, true] {
        let mut extensions = vec![policies(P, false), extension("2.5.29.32", false, &[5, 0])];
        if malformed_first { extensions.reverse(); }
        let issuer = certificate("issuer", &issuer_key, Some((&root, &root_key)), true, VALID, MessageDigest::sha256(), extensions);
        let leaf = certificate("server", &leaf_key, Some((&issuer, &issuer_key)), false, VALID, MessageDigest::sha256(), vec![require(0), policies(P, false)]);
        assert!(verify_store(&store(&[root.clone()]), &[leaf, issuer], &now(), &charge).is_ok());
    }
    let issuer = certificate("issuer", &issuer_key, Some((&root, &root_key)), true, VALID, MessageDigest::sha256(), vec![policies(P, false), policies(P, false)]);
    let leaf = certificate("server", &leaf_key, Some((&issuer, &issuer_key)), false, VALID, MessageDigest::sha256(), vec![policies(P, false)]);
    assert!(verify_store(&store(&[root.clone()]), &[leaf, issuer], &now(), &charge).is_err());
    let issuer = certificate("issuer", &issuer_key, Some((&root, &root_key)), true, VALID, MessageDigest::sha256(), vec![extension("2.5.29.32", false, &[5, 0]), extension("2.5.29.32", false, &[5, 0])]);
    let leaf = certificate("server", &leaf_key, Some((&issuer, &issuer_key)), false, VALID, MessageDigest::sha256(), vec![policies(P, true)]);
    assert!(verify_store(&store(&[root]), &[leaf, issuer], &now(), &charge).is_ok());
    drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn native_pkix_enforces_explicit_policy_on_final_and_intermediate_certificates() {
    let root_key = key(1024); let root = certificate("root", &root_key, None, true, VALID, MessageDigest::sha256(), vec![]);
    let issuer_key = key(1024); let leaf_key = key(1024);
    let memory = MemoryBudget::new(1 << 24); let charge = memory.reserve(1024).unwrap();
    for (count, accepted) in [(0, false), (1, true), (0xff, true), (0xfe, true)] {
        let leaf = certificate("server", &leaf_key, Some((&root, &root_key)), false, VALID, MessageDigest::sha256(), vec![require(count)]);
        assert_eq!(verify_store(&store(&[root.clone()]), &[leaf], &now(), &charge).is_ok(), accepted);
    }
    for (count, accepted) in [(0, false), (1, false), (2, true)] {
        let issuer = certificate("issuer", &issuer_key, Some((&root, &root_key)), true, VALID, MessageDigest::sha256(), vec![require(count)]);
        let leaf = certificate("server", &leaf_key, Some((&issuer, &issuer_key)), false, VALID, MessageDigest::sha256(), vec![policies(P, true)]);
        assert_eq!(verify_store(&store(&[root.clone()]), &[leaf, issuer], &now(), &charge).is_ok(), accepted);
    }
    drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn native_pkix_enforces_mapped_policies_and_inhibited_any_policy() {
    let root_key = key(1024); let root = certificate("root", &root_key, None, true, VALID, MessageDigest::sha256(), vec![]);
    let issuer_key = key(1024); let leaf_key = key(1024);
    let issuer = certificate("issuer", &issuer_key, Some((&root, &root_key)), true, VALID, MessageDigest::sha256(), vec![require(0), policies(P, false), mapping(P, Q)]);
    let memory = MemoryBudget::new(1 << 24); let charge = memory.reserve(1024).unwrap();
    for (policy, accepted) in [(P, false), (Q, true), (R, false), (ANY, true)] {
        let leaf = certificate("server", &leaf_key, Some((&issuer, &issuer_key)), false, VALID, MessageDigest::sha256(), vec![policies(policy, false)]);
        assert_eq!(verify_store(&store(&[root.clone()]), &[leaf, issuer.clone()], &now(), &charge).is_ok(), accepted);
    }
    for critical in [false, true] {
        let issuer = certificate("issuer", &issuer_key, Some((&root, &root_key)), true, VALID, MessageDigest::sha256(), vec![require(0), policies(P, false), extension("2.5.29.54", critical, &[2, 1, 0])]);
        let leaf = certificate("server", &leaf_key, Some((&issuer, &issuer_key)), false, VALID, MessageDigest::sha256(), vec![policies(ANY, false)]);
        assert_eq!(verify_store(&store(&[root.clone()]), &[leaf, issuer], &now(), &charge).is_ok(), !critical);
    }
    drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn native_pkix_retries_an_alternate_issuer_after_java_policy_rejection() {
    let root_key = key(1024); let root = certificate("root", &root_key, None, true, VALID, MessageDigest::sha256(), vec![]);
    let issuer_key = key(1024);
    let rejected = certificate("issuer", &issuer_key, Some((&root, &root_key)), true, VALID, MessageDigest::sha256(), vec![require(0), policies(P, false)]);
    let accepted = certificate("issuer", &issuer_key, Some((&root, &root_key)), true, VALID, MessageDigest::sha256(), vec![require(0), policies(Q, false)]);
    let leaf = certificate("server", &key(1024), Some((&accepted, &issuer_key)), false, VALID, MessageDigest::sha256(), vec![policies(Q, false)]);
    let memory = MemoryBudget::new(1 << 24); let charge = memory.reserve(1024).unwrap();
    let verified = verify_store(&store(&[root]), &[leaf, rejected, accepted.clone()], &now(), &charge).unwrap();
    assert_eq!(verified.certificates[1].to_der().unwrap(), accepted.to_der().unwrap());
    drop(verified); drop(charge); assert_eq!(memory.used_bytes(), 0);
}
