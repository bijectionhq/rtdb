use super::*;

fn der(tag: u8, value: &[u8]) -> Vec<u8> {
    let mut result = vec![tag];
    if value.len() < 128 { result.push(value.len() as u8); }
    else { let bytes = value.len().to_be_bytes(); let start = bytes.iter().position(|byte| *byte != 0).unwrap(); result.push(0x80 | (bytes.len() - start) as u8); result.extend_from_slice(&bytes[start..]); }
    result.extend_from_slice(value); result
}
fn extension(oid: &str, critical: bool, bytes: &[u8]) -> X509Extension {
    X509Extension::new_from_der(&Asn1Object::from_str(oid).unwrap(), critical, &Asn1OctetString::new_from_bytes(bytes).unwrap()).unwrap()
}
fn issued(subject: &str, issuer: &str, subject_key: &PKey<Private>, signing_key: &PKey<Private>, version: i32, ca: bool, extras: Vec<X509Extension>) -> X509 {
    issued_at(subject, issuer, subject_key, signing_key, version, ca, VALID, extras)
}
fn issued_at(subject: &str, issuer: &str, subject_key: &PKey<Private>, signing_key: &PKey<Private>, version: i32, ca: bool, validity: (i64, i64), extras: Vec<X509Extension>) -> X509 {
    let name = |value: &str| { let mut name = X509NameBuilder::new().unwrap(); name.append_entry_by_text("CN", value).unwrap(); name.build() };
    let mut builder = X509::builder().unwrap(); builder.set_version(version).unwrap();
    builder.set_serial_number(&BigNum::from_u32(1).unwrap().to_asn1_integer().unwrap()).unwrap();
    builder.set_subject_name(&name(subject)).unwrap(); builder.set_issuer_name(&name(issuer)).unwrap();
    builder.set_pubkey(subject_key).unwrap(); builder.set_not_before(&Asn1Time::from_unix(validity.0).unwrap()).unwrap(); builder.set_not_after(&Asn1Time::from_unix(validity.1).unwrap()).unwrap();
    if version == 2 {
        if ca { builder.append_extension(BasicConstraints::new().critical().ca().build().unwrap()).unwrap(); }
        builder.append_extension(if ca { KeyUsage::new().key_cert_sign().build().unwrap() } else { KeyUsage::new().digital_signature().build().unwrap() }).unwrap();
    }
    for extension in extras { builder.append_extension(extension).unwrap(); }
    builder.sign(signing_key, MessageDigest::sha256()).unwrap(); builder.build()
}
fn aki(key: Option<&[u8]>, serial: Option<&[u8]>) -> Vec<u8> {
    der(0x30, &[key.map(|key| der(0x80, key)).unwrap_or_default(), serial.map(|serial| der(0x82, serial)).unwrap_or_default()].concat())
}

#[test]
fn selected_pkix_names_use_java_unicode_equivalence_without_native_issuer_prefilter() {
    let root_key = key(1024); let root = issued("ÉCOLE", "ÉCOLE", &root_key, &root_key, 2, true, vec![]);
    let leaf_key = key(1024);
    let leaf = issued("server", "e\u{301}cole", &leaf_key, &root_key, 2, false, vec![]);
    let memory = MemoryBudget::new(16 << 20); let charge = memory.reserve(1024).unwrap();
    let verified = verify_store(&store(&[root.clone()]), &[leaf.clone()], &now(), &charge).unwrap();
    assert_eq!(verified.certificates[0].to_der().unwrap(), leaf.to_der().unwrap());
    assert_eq!(verified.certificates[1].to_der().unwrap(), root.to_der().unwrap());
    let bad = issued("server", "different", &leaf_key, &root_key, 2, false, vec![]);
    assert!(verify_store(&store(&[root]), &[bad], &now(), &charge).is_err());
    drop(verified); drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn selected_pkix_verifies_original_signatures_after_identifier_metadata_is_removed() {
    let root_key = key(1024); let root = issued("CA", "CA", &root_key, &root_key, 2, true, vec![]);
    let leaf_key = key(1024); let wrong_key = key(1024);
    let malformed = || extension("2.5.29.35", false, &der(0x30, &der(0x82, &[])));
    let good = issued("server", "CA", &leaf_key, &root_key, 2, false, vec![malformed()]);
    let bad = issued("server", "CA", &leaf_key, &wrong_key, 2, false, vec![malformed()]);
    let memory = MemoryBudget::new(16 << 20); let charge = memory.reserve(1024).unwrap();
    assert!(verify_store(&store(&[root.clone()]), &[good], &now(), &charge).is_ok());
    assert!(verify_store(&store(&[root]), &[bad], &now(), &charge).is_err());
    drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn selected_pkix_ignores_malformed_noncritical_identifier_caches_on_intermediates() {
    let root_key = key(1024); let root = issued("root", "root", &root_key, &root_key, 2, true, vec![]);
    let issuer_key = key(1024); let leaf_key = key(1024);
    let leaf = issued("server", "issuer", &leaf_key, &issuer_key, 2, false, vec![]);
    let memory = MemoryBudget::new(16 << 20); let charge = memory.reserve(1024).unwrap();
    for extensions in [
        vec![extension("2.5.29.35", false, &[5, 0])],
        vec![extension("2.5.29.14", false, &[5, 0])],
        vec![extension("2.5.29.14", false, &der(4, b"key")), extension("2.5.29.14", false, &[5, 0])],
        vec![extension("2.5.29.35", false, &aki(None, None)), extension("2.5.29.35", false, &[5, 0])],
    ] {
        let issuer = issued("issuer", "root", &issuer_key, &root_key, 2, true, extensions);
        assert!(verify_store(&store(&[root.clone()]), &[leaf.clone(), issuer], &now(), &charge).is_ok());
    }
    for oid in ["2.5.29.14", "2.5.29.35"] {
        let value = if oid.ends_with("14") { der(4, b"key") } else { aki(None, None) };
        let issuer = issued("issuer", "root", &issuer_key, &root_key, 2, true, vec![extension(oid, false, &value), extension(oid, false, &value)]);
        assert!(verify_store(&store(&[root.clone()]), &[leaf.clone(), issuer], &now(), &charge).is_err());
    }
    drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn selected_pkix_rejects_critical_identifiers_unknown_extensions_and_other_malformed_extensions() {
    let root_key = key(1024); let root = issued("root", "root", &root_key, &root_key, 2, true, vec![]);
    let issuer_key = key(1024); let leaf_key = key(1024);
    let leaf = issued("server", "issuer", &leaf_key, &issuer_key, 2, false, vec![]);
    let memory = MemoryBudget::new(16 << 20); let charge = memory.reserve(1024).unwrap();
    for extension in [
        extension("2.5.29.14", true, &der(4, b"key")), extension("2.5.29.14", true, &[5, 0]),
        extension("2.5.29.35", true, &aki(None, None)), extension("2.5.29.35", true, &[5, 0]),
        extension("1.2.3.4.5", true, &[5, 0]), extension("2.5.29.17", true, &[5, 0]),
        extension("2.5.29.31", true, &der(0x30, &[])),
    ] {
        let issuer = issued("issuer", "root", &issuer_key, &root_key, 2, true, vec![extension]);
        assert!(verify_store(&store(&[root.clone()]), &[leaf.clone(), issuer], &now(), &charge).is_err());
    }
    drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn ordered_pkix_uses_raw_skid_canonical_aki_and_v3_anchor_serial_screening() {
    let root_key = key(1024); let leaf_key = key(1024);
    let memory = MemoryBudget::new(16 << 20); let charge = memory.reserve(1024).unwrap();
    for (skid, authority_serial, valid) in [(Some(der(4, b"key")), vec![1], true), (None, vec![1], true), (Some(vec![4, 0x81, 3, b'k', b'e', b'y']), vec![1], false), (None, vec![2], false), (None, vec![0, 1], true), (None, vec![0xff], false)] {
        let root = issued("root", "root", &root_key, &root_key, 2, true, skid.into_iter().map(|bytes| extension("2.5.29.14", false, &bytes)).collect());
        let leaf = issued("server", "root", &leaf_key, &root_key, 2, false, vec![extension("2.5.29.35", false, &aki(Some(b"key"), Some(&authority_serial)))]);
        assert_eq!(verify_store(&store(&[root]), &[leaf], &now(), &charge).is_ok(), valid);
    }
    for version in [0, 1] {
        let root = issued("root", "root", &root_key, &root_key, version, true, vec![]);
        let leaf = issued("server", "root", &leaf_key, &root_key, 2, false, vec![extension("2.5.29.35", false, &aki(Some(b"key"), Some(&[2])))]);
        assert!(verify_store(&store(&[root]), &[leaf], &now(), &charge).is_ok());
    }
    drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn ordered_pkix_does_not_filter_intermediate_edges_by_aki_or_authority_issuer_name() {
    let root_key = key(1024); let root = issued("root", "root", &root_key, &root_key, 2, true, vec![]);
    let issuer_key = key(1024);
    let mut other = X509NameBuilder::new().unwrap(); other.append_entry_by_text("CN", "different authority issuer").unwrap(); let other = other.build();
    let identifier = der(0x30, &[der(0xa1, &der(0xa4, &other.to_der().unwrap())), der(0x82, &[1])].concat());
    let issuer = issued("issuer", "root", &issuer_key, &root_key, 2, true, vec![extension("2.5.29.35", false, &identifier)]);
    let leaf = issued("server", "issuer", &key(1024), &issuer_key, 2, false, vec![extension("2.5.29.35", false, &aki(Some(b"different"), Some(&[99])))]);
    let memory = MemoryBudget::new(16 << 20); let charge = memory.reserve(1024).unwrap();
    assert!(verify_store(&store(&[root]), &[leaf, issuer], &now(), &charge).is_ok());
    drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn builder_pkix_does_not_apply_ordered_anchor_aki_serial_filter() {
    let root_key = key(1024); let root = issued("root", "root", &root_key, &root_key, 2, true, vec![]);
    let issuer_key = key(1024);
    let issuer = issued("issuer", "root", &issuer_key, &root_key, 2, true, vec![extension("2.5.29.35", false, &aki(None, Some(&[99])))]);
    let leaf = issued("server", "issuer", &key(1024), &issuer_key, 2, false, vec![]);
    let memory = MemoryBudget::new(16 << 20); let charge = memory.reserve(1024).unwrap();
    let store = store(&[root.clone()]);
    assert!(verify_store(&store, &[leaf.clone(), issuer.clone()], &now(), &charge).is_err());
    assert!(verify_store(&store, &[leaf, root, issuer], &now(), &charge).is_ok());
    drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn java_self_issued_rollover_preserves_path_length_and_name_constraint_exceptions() {
    let root_key = key(1024); let root = issued("root", "root", &root_key, &root_key, 2, true, vec![]);
    let issuer_key = key(1024); let rollover_key = key(1024);
    let limited = extension("2.5.29.19", true, &der(0x30, &[der(1, &[0xff]), der(2, &[0])].concat()));
    let constraint = extension("2.5.29.30", true, &der(0x30, &der(0xa0, &der(0x30, &der(0x82, b"allowed.example")))));
    let mut issuer = X509::builder().unwrap();
    let mut subject = X509NameBuilder::new().unwrap(); subject.append_entry_by_text("CN", "ÉCOLE").unwrap(); let subject = subject.build();
    issuer.set_version(2).unwrap(); issuer.set_serial_number(&BigNum::from_u32(1).unwrap().to_asn1_integer().unwrap()).unwrap();
    issuer.set_subject_name(&subject).unwrap(); issuer.set_issuer_name(root.subject_name()).unwrap(); issuer.set_pubkey(&issuer_key).unwrap();
    issuer.set_not_before(&Asn1Time::from_unix(VALID.0).unwrap()).unwrap(); issuer.set_not_after(&Asn1Time::from_unix(VALID.1).unwrap()).unwrap();
    issuer.append_extension(limited).unwrap(); issuer.append_extension(KeyUsage::new().key_cert_sign().build().unwrap()).unwrap(); issuer.append_extension(constraint).unwrap();
    issuer.sign(&root_key, MessageDigest::sha256()).unwrap(); let issuer = issuer.build();
    let rollover = issued("e\u{301}cole", "ÉCOLE", &rollover_key, &issuer_key, 2, true, vec![extension("2.5.29.17", false, &der(0x30, &der(0x82, b"outside.example")))]);
    let leaf = issued("server", "e\u{301}cole", &key(1024), &rollover_key, 2, false, vec![extension("2.5.29.17", false, &der(0x30, &der(0x82, b"allowed.example")))]);
    let memory = MemoryBudget::new(16 << 20); let charge = memory.reserve(1024).unwrap();
    let verified = verify_store(&store(&[root]), &[leaf, rollover.clone(), issuer], &now(), &charge).unwrap();
    assert_eq!(verified.certificates[1].to_der().unwrap(), rollover.to_der().unwrap());
    drop(verified); drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn ordered_pkix_allows_v1_v2_rollover_only_directly_below_the_anchor() {
    let root_key = key(1024); let root = issued("ÉCOLE", "ÉCOLE", &root_key, &root_key, 2, true, vec![]);
    let rollover_key = key(1024); let leaf_key = key(1024);
    let memory = MemoryBudget::new(16 << 20); let charge = memory.reserve(1024).unwrap();
    for version in [0, 1] {
        for (subject, valid) in [("e\u{301}cole", true), ("other", false)] {
            let rollover = issued(subject, "ÉCOLE", &rollover_key, &root_key, version, true, vec![]);
            let leaf = issued("server", subject, &leaf_key, &rollover_key, 2, false, vec![]);
            assert_eq!(verify_store(&store(&[root.clone()]), &[leaf.clone(), rollover.clone()], &now(), &charge).is_ok(), valid);
            assert!(verify_store(&store(&[root.clone()]), &[leaf, root.clone(), rollover], &now(), &charge).is_err());
        }
        let upper = issued("upper", "ÉCOLE", &root_key, &root_key, 2, true, vec![]);
        let rollover = issued("upper", "upper", &rollover_key, &root_key, version, true, vec![]);
        let leaf = issued("server", "upper", &leaf_key, &rollover_key, 2, false, vec![]);
        assert!(verify_store(&store(&[root.clone()]), &[leaf, rollover, upper], &now(), &charge).is_err());
    }
    drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn selected_pkix_callbacks_observe_original_chain_certificates() {
    unsafe extern "C" fn original_only(verified: i32, context: *mut openssl_sys::X509_STORE_CTX) -> i32 {
        let context = unsafe { X509StoreContextRef::from_ptr(context) };
        i32::from(verified == 1 && context.current_cert().is_some_and(|current| context.chain().is_some_and(|chain| {
            chain.iter().nth(context.error_depth() as usize).is_some_and(|original| original.as_ptr() == current.as_ptr())
        })))
    }
    let root_key = key(1024); let root = issued("root", "root", &root_key, &root_key, 2, true, vec![]);
    let leaf = issued("server", "root", &key(1024), &root_key, 2, false, vec![extension("2.5.29.35", false, &[5, 0])]);
    let original = leaf.to_der().unwrap();
    let memory = MemoryBudget::new(16 << 20); let charge = memory.reserve(1024).unwrap();
    let mut context = X509StoreContext::new().unwrap(); let untrusted = Stack::new().unwrap();
    let verified = context.init(&store(&[root]), &leaf, &untrusted, |context| {
        unsafe { X509_STORE_CTX_set_verify_cb(context.as_ptr(), Some(original_only)); }
        Ok(verify_context(context, &now(), &charge))
    }).unwrap().unwrap();
    assert_eq!(verified[0].to_der().unwrap(), original);
    assert_eq!(leaf.to_der().unwrap(), original);
    drop(verified); drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn native_default_verifier_retains_its_own_issuer_matching() {
    let root_key = key(1024); let root = issued("ÉCOLE", "ÉCOLE", &root_key, &root_key, 2, true, vec![]);
    let leaf = issued("server", "e\u{301}cole", &key(1024), &root_key, 2, false, vec![]);
    let mut context = X509StoreContext::new().unwrap(); let untrusted = Stack::new().unwrap();
    assert!(!context.init(&store(&[root.clone()]), &leaf, &untrusted, |context| {
        configure_parameters(context, &now()).unwrap();
        context.verify_cert()
    }).unwrap());
    let memory = MemoryBudget::new(16 << 20); let charge = memory.reserve(1024).unwrap();
    assert!(verify_store(&store(&[root]), &[leaf], &now(), &charge).is_ok());
    drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn builder_legacy_anchor_screening_requires_the_child_validity_period() {
    let root_key = key(1024); let issuer_key = key(1024); let leaf_key = key(1024);
    let memory = MemoryBudget::new(16 << 20); let charge = memory.reserve(1024).unwrap();
    for version in [0, 1, 2] {
        let root = issued_at("root", "root", &root_key, &root_key, version, true, (1_400_000_000, 1_500_000_000), vec![]);
        let issuer = issued("issuer", "root", &issuer_key, &root_key, 2, true, vec![]);
        let leaf = issued("server", "issuer", &leaf_key, &issuer_key, 2, false, vec![]);
        let store = store(&[root.clone()]);
        assert!(verify_store(&store, &[leaf.clone(), issuer.clone()], &now(), &charge).is_ok());
        assert_eq!(verify_store(&store, &[leaf, root, issuer], &now(), &charge).is_ok(), version == 2);
    }
    drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn selected_pkix_anchor_malformed_noncritical_extensions_do_not_enter_native_metadata() {
    let root_key = key(1024); let issuer_key = key(1024);
    let root = issued("root", "root", &root_key, &root_key, 2, true, vec![
        extension("2.5.29.17", false, &[5, 0]), extension("1.2.3.4.5", true, &[5, 0]),
    ]);
    let issuer = issued("issuer", "root", &issuer_key, &root_key, 2, true, vec![]);
    let leaf = issued("server", "issuer", &key(1024), &issuer_key, 2, false, vec![]);
    let memory = MemoryBudget::new(16 << 20); let charge = memory.reserve(1024).unwrap();
    let verified = verify_store(&store(&[root.clone()]), &[leaf, issuer], &now(), &charge).unwrap();
    assert_eq!(verified.certificates.last().unwrap().to_der().unwrap(), root.to_der().unwrap());
    drop(verified); drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn selected_native_anchor_key_usage_is_owned_by_the_java_policy_caller() {
    let root_key = key(1024);
    let root = issued("root", "root", &root_key, &root_key, 2, false,
        vec![BasicConstraints::new().critical().ca().build().unwrap()]);
    let leaf = issued("server", "root", &key(1024), &root_key, 2, false, vec![]);
    let mut originals = Stack::new().unwrap(); originals.push(leaf.clone()).unwrap(); originals.push(root.clone()).unwrap();
    let empty_store = store(&[]); let untrusted = Stack::new().unwrap(); let mut context = X509StoreContext::new().unwrap();
    assert!(context.init(&empty_store, &leaf, &untrusted, |context| {
        configure_parameters(context, &now()).unwrap();
        Ok(verify_with_dates(context, &now(), &originals, &[0, 1]).unwrap())
    }).unwrap());
    let memory = MemoryBudget::new(16 << 20); let charge = memory.reserve(1024).unwrap();
    assert!(verify_store(&store(&[root]), &[leaf], &now(), &charge).is_err());
    drop(charge); assert_eq!(memory.used_bytes(), 0);
}

mod java_constraints;
