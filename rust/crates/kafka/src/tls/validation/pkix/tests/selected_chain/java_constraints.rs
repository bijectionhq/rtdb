use super::*;

fn raw_issued(subject: &str, issuer: &str, subject_key: &PKey<Private>, signing_key: &PKey<Private>, extensions: Vec<X509Extension>) -> X509 {
    let name = |value: &str| { let mut name = X509NameBuilder::new().unwrap(); name.append_entry_by_text("CN", value).unwrap(); name.build() };
    let mut builder = X509::builder().unwrap(); builder.set_version(2).unwrap();
    builder.set_serial_number(&BigNum::from_u32(1).unwrap().to_asn1_integer().unwrap()).unwrap();
    builder.set_subject_name(&name(subject)).unwrap(); builder.set_issuer_name(&name(issuer)).unwrap();
    builder.set_pubkey(subject_key).unwrap(); builder.set_not_before(&Asn1Time::from_unix(VALID.0).unwrap()).unwrap(); builder.set_not_after(&Asn1Time::from_unix(VALID.1).unwrap()).unwrap();
    for extension in extensions { builder.append_extension(extension).unwrap(); }
    builder.sign(signing_key, MessageDigest::sha256()).unwrap(); builder.build()
}

fn ca(limit: Option<i32>) -> X509Extension {
    let limit = limit.map(|limit| der(2, &limit.to_be_bytes())).unwrap_or_default();
    extension("2.5.29.19", true, &der(0x30, &[der(1, &[0xff]), limit].concat()))
}

fn parsed(oid: &str) -> Vec<u8> {
    match oid {
        "2.5.29.19" => der(0x30, &der(1, &[0xff])),
        "2.5.29.15" => der(3, &[2, 4]),
        "2.5.29.37" => der(0x30, &der(6, &[0x2b, 6, 1, 5, 5, 7, 3, 2])),
        _ => unreachable!(),
    }
}

#[test]
fn java_selected_extensions_keep_parsed_copies_beside_malformed_noncritical_copies() {
    let root_key = key(1024); let root = issued("root", "root", &root_key, &root_key, 2, true, vec![]);
    let issuer_key = key(1024); let leaf = issued("server", "issuer", &key(1024), &issuer_key, 2, false, vec![]);
    let memory = MemoryBudget::new(16 << 20); let charge = memory.reserve(1024).unwrap();
    for oid in ["2.5.29.19", "2.5.29.15", "2.5.29.37"] {
        for malformed_first in [false, true] {
            let mut extensions = if oid == "2.5.29.19" { vec![] } else { vec![ca(None)] };
            let mut pair = vec![extension(oid, false, &parsed(oid)), extension(oid, false, &[5, 0])];
            if malformed_first { pair.reverse(); }
            extensions.extend(pair);
            let issuer = raw_issued("issuer", "root", &issuer_key, &root_key, extensions);
            assert!(verify_store(&store(&[root.clone()]), &[leaf.clone(), issuer], &now(), &charge).is_ok(), "{oid} {malformed_first}");
        }
        for (values, critical) in [(vec![parsed(oid), parsed(oid)], false), (vec![vec![5, 0]], true)] {
            let mut extensions = if oid == "2.5.29.19" { vec![] } else { vec![ca(None)] };
            extensions.extend(values.iter().map(|value| extension(oid, critical, value)));
            let issuer = raw_issued("issuer", "root", &issuer_key, &root_key, extensions);
            assert!(verify_store(&store(&[root.clone()]), &[leaf.clone(), issuer], &now(), &charge).is_err(), "{oid} {critical}");
        }
    }
    drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn java_selected_malformed_only_extensions_follow_each_java_getter() {
    let root_key = key(1024); let root = issued("root", "root", &root_key, &root_key, 2, true, vec![]);
    let issuer_key = key(1024); let leaf = issued("server", "issuer", &key(1024), &issuer_key, 2, false, vec![]);
    let memory = MemoryBudget::new(16 << 20); let charge = memory.reserve(1024).unwrap();
    for (oid, valid) in [("2.5.29.19", false), ("2.5.29.15", true), ("2.5.29.37", true)] {
        let mut extensions = if oid == "2.5.29.19" { vec![] } else { vec![ca(None)] };
        extensions.push(extension(oid, false, &[5, 0]));
        let issuer = raw_issued("issuer", "root", &issuer_key, &root_key, extensions);
        assert_eq!(verify_store(&store(&[root.clone()]), &[leaf.clone(), issuer], &now(), &charge).is_ok(), valid, "{oid}");
    }
    for (oid, valid) in [("2.5.29.19", true), ("2.5.29.15", true), ("2.5.29.37", false)] {
        let leaf = raw_issued("server", "root", &key(1024), &root_key, vec![extension(oid, false, &[5, 0])]);
        assert_eq!(verify_store(&store(&[root.clone()]), &[leaf], &now(), &charge).is_ok(), valid, "leaf {oid}");
    }
    drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn java_selected_ku_accepts_wrapping_and_ber_lengths_and_masks_undeclared_bits() {
    let root_key = key(1024); let root = issued("root", "root", &root_key, &root_key, 2, true, vec![]);
    let issuer_key = key(1024); let leaf = issued("server", "issuer", &key(1024), &issuer_key, 2, false, vec![]);
    let memory = MemoryBudget::new(16 << 20); let charge = memory.reserve(1024).unwrap();
    for (value, valid) in [(der(4, &der(3, &[2, 4])), true), (vec![3, 0x81, 2, 2, 4], true),
        (der(3, &[3, 0x84]), false), (der(3, &[0, 0x80]), false), (der(3, &[0, 1]), false)] {
        let issuer = raw_issued("issuer", "root", &issuer_key, &root_key, vec![ca(None), extension("2.5.29.15", false, &value)]);
        assert_eq!(verify_store(&store(&[root.clone()]), &[leaf.clone(), issuer], &now(), &charge).is_ok(), valid, "{value:?}");
    }
    drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn java_selected_signed_path_limits_distinguish_minus_one_and_non_self_issued_descendants() {
    let root_key = key(1024); let root = issued("root", "root", &root_key, &root_key, 2, true, vec![]);
    let upper_key = key(1024); let lower_key = key(1024); let leaf_key = key(1024);
    let memory = MemoryBudget::new(16 << 20); let charge = memory.reserve(1024).unwrap();
    for limit in [-1, -2, i32::MIN, 0, 1] {
        let upper = raw_issued("ÉCOLE", "root", &upper_key, &root_key, vec![ca(Some(limit))]);
        let direct = issued("server", "ÉCOLE", &leaf_key, &upper_key, 2, false, vec![]);
        assert_eq!(verify_store(&store(&[root.clone()]), &[direct, upper.clone()], &now(), &charge).is_ok(), limit != -1, "direct {limit}");
        for (subject, self_issued) in [("e\u{301}cole", true), ("different", false)] {
            let lower = raw_issued(subject, "ÉCOLE", &lower_key, &upper_key, vec![ca(None)]);
            let leaf = issued("server", subject, &leaf_key, &lower_key, 2, false, vec![]);
            assert_eq!(verify_store(&store(&[root.clone()]), &[leaf, lower, upper.clone()], &now(), &charge).is_ok(),
                limit != -1 && (self_issued || limit > 0), "{limit} {self_issued}");
        }
    }
    drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn java_selected_leaf_eku_uses_java_parsing_and_all_server_authentication_oids() {
    let root_key = key(1024); let root = issued("root", "root", &root_key, &root_key, 2, true, vec![]);
    let leaf_key = key(1024); let memory = MemoryBudget::new(16 << 20); let charge = memory.reserve(1024).unwrap();
    for oid in [vec![0x2b, 6, 1, 5, 5, 7, 3, 1], vec![0x55, 0x1d, 0x25, 0],
        vec![0x2b, 6, 1, 4, 1, 0x82, 0x37, 0xa, 3, 3], vec![0x60, 0x86, 0x48, 1, 0x86, 0xf8, 0x42, 4, 1]] {
        for malformed_first in [false, true] {
            let mut extensions = vec![extension("2.5.29.37", false, &der(0x30, &der(6, &oid))), extension("2.5.29.37", false, &[5, 0])];
            if malformed_first { extensions.reverse(); }
            let leaf = raw_issued("server", "root", &leaf_key, &root_key, extensions);
            assert!(verify_store(&store(&[root.clone()]), &[leaf], &now(), &charge).is_ok());
        }
    }
    for value in [der(0x30, &[]), parsed("2.5.29.37"), der(0x30, &der(6, &[0x80]))] {
        let leaf = raw_issued("server", "root", &leaf_key, &root_key, vec![extension("2.5.29.37", false, &value)]);
        assert!(verify_store(&store(&[root.clone()]), &[leaf], &now(), &charge).is_err());
    }
    drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn java_selected_leaf_usage_failure_occurs_after_selection_without_retry() {
    std::thread_local! { static LEAF_SUCCESSES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }
    unsafe extern "C" fn count_leaf_successes(verified: i32, context: *mut openssl_sys::X509_STORE_CTX) -> i32 {
        let context = unsafe { X509StoreContextRef::from_ptr(context) };
        if verified == 1 && context.error_depth() == 0 { LEAF_SUCCESSES.with(|count| count.set(count.get() + 1)); }
        verified
    }
    let root_key = key(1024);
    let root = issued("root", "root", &root_key, &root_key, 2, true, vec![]);
    let alternate = issued("root", "root", &root_key, &root_key, 2, true, vec![extension("1.2.3.4", false, &[5, 0])]);
    let memory = MemoryBudget::new(16 << 20); let charge = memory.reserve(1024).unwrap();
    for extension in [extension("2.5.29.15", false, &der(3, &[0, 0x20])), extension("2.5.29.37", false, &[5, 0])] {
        let leaf = raw_issued("server", "root", &key(1024), &root_key, vec![extension]);
        LEAF_SUCCESSES.with(|count| count.set(0));
        let mut context = X509StoreContext::new().unwrap(); let untrusted = Stack::new().unwrap();
        let result = context.init(&store(&[root.clone(), alternate.clone()]), &leaf, &untrusted, |context| {
            unsafe { X509_STORE_CTX_set_verify_cb(context.as_ptr(), Some(count_leaf_successes)); }
            Ok(verify_context(context, &now(), &charge))
        }).unwrap();
        assert!(result.is_err());
        LEAF_SUCCESSES.with(|count| assert_eq!(count.get(), 1));
    }
    drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn java_selected_stripped_usage_keeps_original_signatures_and_legacy_native_rejection() {
    let root_key = key(1024); let wrong_key = key(1024); let root = issued("root", "root", &root_key, &root_key, 2, true, vec![]);
    let leaf_key = key(1024); let memory = MemoryBudget::new(16 << 20); let charge = memory.reserve(1024).unwrap();
    for (signing_key, valid) in [(&root_key, true), (&wrong_key, false)] {
        let leaf = raw_issued("server", "root", &leaf_key, signing_key, vec![extension("2.5.29.15", false, &[5, 0])]);
        let mut originals = Stack::new().unwrap(); originals.push(leaf.clone()).unwrap(); originals.push(root.clone()).unwrap();
        let empty = Stack::new().unwrap(); let mut context = X509StoreContext::new().unwrap();
        assert!(!context.init(&store(&[]), &leaf, &empty, |context| {
            configure_parameters(context, &now()).unwrap();
            Ok(verify_with_dates(context, &now(), &originals, &[0, 1]).unwrap())
        }).unwrap());
        let mut context = X509StoreContext::new().unwrap();
        assert!(!context.init(&store(&[root.clone()]), &leaf, &empty, |context| {
            configure_parameters(context, &now()).unwrap(); context.verify_cert()
        }).unwrap());
        let result = verify_store(&store(&[root.clone()]), &[leaf.clone()], &now(), &charge);
        assert_eq!(result.is_ok(), valid);
        if let Ok(verified) = result { assert_eq!(verified.certificates[0].to_der().unwrap(), leaf.to_der().unwrap()); }
    }
    drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn java_selected_exact_trusted_leaf_skips_critical_usage_parsing() {
    let key = key(1024); let leaf = raw_issued("trusted", "trusted", &key, &key,
        vec![extension("2.5.29.15", true, &[5, 0]), extension("2.5.29.37", true, &[5, 0])]);
    let memory = MemoryBudget::new(16 << 20); let charge = memory.reserve(1024).unwrap();
    let verified = verify_store(&store(&[leaf.clone()]), &[leaf.clone()], &now(), &charge).unwrap();
    assert_eq!(verified.certificates.len(), 1);
    assert_eq!(verified.certificates[0].to_der().unwrap(), leaf.to_der().unwrap());
    drop(verified); drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn java_selected_callback_override_of_minimum_signed_limit_keeps_original_certificates() {
    std::thread_local! { static PATH_ERRORS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }
    unsafe extern "C" fn permit_path_length(verified: i32, context: *mut openssl_sys::X509_STORE_CTX) -> i32 {
        let context = unsafe { X509StoreContextRef::from_ptr(context) };
        let original = context.current_cert().is_some_and(|current| context.chain().is_some_and(|chain| {
            chain.iter().nth(context.error_depth() as usize).is_some_and(|original| original.as_ptr() == current.as_ptr())
        }));
        if !original { return 0; }
        if verified == 0 && context.error().as_raw() == openssl_sys::X509_V_ERR_PATH_LENGTH_EXCEEDED && context.error_depth() == 1 {
            PATH_ERRORS.with(|count| count.set(count.get() + 1));
            return 1;
        }
        verified
    }
    let root_key = key(1024); let upper_key = key(1024); let lower_key = key(1024);
    let root = issued("root", "root", &root_key, &root_key, 2, true, vec![]);
    let upper = raw_issued("upper", "root", &upper_key, &root_key, vec![ca(Some(i32::MIN))]);
    let lower = raw_issued("lower", "upper", &lower_key, &upper_key, vec![ca(None)]);
    let leaf = issued("server", "lower", &key(1024), &lower_key, 2, false, vec![]);
    let memory = MemoryBudget::new(16 << 20);
    let chain = [leaf.as_ref(), lower.as_ref(), upper.as_ref(), root.as_ref()];
    let metadata = crate::tls::security::PathMetadata::decode(&chain, &memory).unwrap();
    let mut originals = Stack::new().unwrap(); for certificate in chain { originals.push(certificate.to_owned()).unwrap(); }
    let empty = Stack::new().unwrap(); let mut context = X509StoreContext::new().unwrap();
    PATH_ERRORS.with(|count| count.set(0));
    assert!(context.init(&store(&[]), &leaf, &empty, |context| {
        configure_parameters(context, &now()).unwrap();
        unsafe { X509_STORE_CTX_set_verify_cb(context.as_ptr(), Some(permit_path_length)); }
        let verified = verify_java_with_dates(context, &now(), &originals, &metadata).unwrap();
        assert_eq!(context.error().as_raw(), openssl_sys::X509_V_ERR_PATH_LENGTH_EXCEEDED);
        Ok(verified)
    }).unwrap());
    PATH_ERRORS.with(|count| assert_eq!(count.get(), 1));
    drop(metadata); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn java_selected_abi_rejects_wrong_fact_counts_before_retaining_the_chain() {
    let root_key = key(1024); let root = issued("root", "root", &root_key, &root_key, 2, true, vec![]);
    let leaf = issued("server", "root", &key(1024), &root_key, 2, false, vec![]);
    let memory = MemoryBudget::new(16 << 20); let chain = [leaf.as_ref(), root.as_ref()];
    let metadata = crate::tls::security::PathMetadata::decode(&chain, &memory).unwrap();
    assert_eq!(metadata.facts().len(), 1);
    let mut originals = Stack::new().unwrap(); for certificate in chain { originals.push(certificate.to_owned()).unwrap(); }
    let empty = Stack::new().unwrap();
    for count in [0, 2, i32::MAX as usize, usize::MAX] {
        let mut context = X509StoreContext::new().unwrap();
        context.init(&store(&[]), &leaf, &empty, |context| {
            configure_parameters(context, &now()).unwrap();
            let result = unsafe { X509_STORE_CTX_verify_selected_java_chain(context.as_ptr(), originals.as_ptr(), metadata.facts().as_ptr(), count) };
            assert_eq!(result, -1);
            assert!(context.chain().is_none());
            assert_eq!(context.error().as_raw(), openssl_sys::X509_V_ERR_INVALID_CALL);
            Ok(())
        }).unwrap();
    }
    drop(metadata); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn java_selected_key_checker_precedes_basic_constraints_failure() {
    let root_key = key(1024); let root = issued("root", "root", &root_key, &root_key, 2, true, vec![]);
    let issuer_key = key(1024); let issuer = raw_issued("issuer", "root", &issuer_key, &root_key,
        vec![extension("2.5.29.15", false, &der(3, &[0, 0x80]))]);
    let leaf = issued("server", "issuer", &key(1024), &issuer_key, 2, false, vec![]);
    let memory = MemoryBudget::new(16 << 20); let chain = [leaf.as_ref(), issuer.as_ref(), root.as_ref()];
    let metadata = crate::tls::security::PathMetadata::decode(&chain, &memory).unwrap();
    let mut originals = Stack::new().unwrap(); for certificate in chain { originals.push(certificate.to_owned()).unwrap(); }
    let empty = Stack::new().unwrap(); let mut context = X509StoreContext::new().unwrap();
    context.init(&store(&[]), &leaf, &empty, |context| {
        configure_parameters(context, &now()).unwrap();
        assert!(!verify_java_with_dates(context, &now(), &originals, &metadata).unwrap());
        assert_eq!(context.error().as_raw(), openssl_sys::X509_V_ERR_KEYUSAGE_NO_CERTSIGN);
        assert_eq!(context.error_depth(), 1);
        assert_eq!(context.current_cert().unwrap().as_ptr(), issuer.as_ptr());
        Ok(())
    }).unwrap();
    drop(metadata); assert_eq!(memory.used_bytes(), 0);
}
