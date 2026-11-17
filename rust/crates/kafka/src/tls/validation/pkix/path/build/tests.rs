use super::*;
use openssl::{asn1::{Asn1Object, Asn1OctetString, Asn1Time}, bn::BigNum, hash::MessageDigest, pkey::{PKey, Private}, rsa::Rsa,
    x509::{X509Extension, X509Name, X509NameBuilder, extension::{BasicConstraints, KeyUsage}}};
use rtdb_columnar::MemoryBudget;

const VALID: (i64, i64) = (1_600_000_000, 2_000_000_000);
fn key() -> PKey<Private> { PKey::from_rsa(Rsa::generate(1024).unwrap()).unwrap() }
fn now() -> Asn1Time { Asn1Time::from_unix(1_800_000_000).unwrap() }
fn name(value: &str) -> X509Name {
    let mut name = X509NameBuilder::new().unwrap();
    for entry in value.split('/') { let (kind, value) = entry.split_once('=').unwrap_or(("CN", entry)); name.append_entry_by_text(kind, value).unwrap(); }
    name.build()
}
fn extension(oid: &str, critical: bool, value: &[u8]) -> X509Extension {
    X509Extension::new_from_der(&Asn1Object::from_str(oid).unwrap(), critical, &Asn1OctetString::new_from_bytes(value).unwrap()).unwrap()
}
fn der(tag: u8, value: &[u8]) -> Vec<u8> {
    let mut encoded = vec![tag];
    if value.len() < 128 { encoded.push(value.len() as u8); }
    else {
        let length = value.len().to_be_bytes();
        let first = length.iter().position(|byte| *byte != 0).unwrap();
        encoded.push(0x80 | (length.len() - first) as u8);
        encoded.extend_from_slice(&length[first..]);
    }
    encoded.extend_from_slice(value);
    encoded
}
fn skid(value: &[u8]) -> X509Extension { extension("2.5.29.14", false, &der(4, value)) }
fn aki(value: &[u8]) -> X509Extension { extension("2.5.29.35", false, &der(0x30, &der(0x80, value))) }
fn san(value: &[u8]) -> X509Extension { extension("2.5.29.17", false, &der(0x30, &der(0x82, value))) }
fn certificate(subject: &str, issuer: &str, public: &PKey<Private>, signer: &PKey<Private>, serial: u32, ca: bool, extras: Vec<X509Extension>) -> X509 {
    let mut certificate = X509::builder().unwrap(); certificate.set_version(2).unwrap();
    certificate.set_serial_number(&BigNum::from_u32(serial).unwrap().to_asn1_integer().unwrap()).unwrap();
    certificate.set_subject_name(&name(subject)).unwrap(); certificate.set_issuer_name(&name(issuer)).unwrap(); certificate.set_pubkey(public).unwrap();
    certificate.set_not_before(&Asn1Time::from_unix(VALID.0).unwrap()).unwrap(); certificate.set_not_after(&Asn1Time::from_unix(VALID.1).unwrap()).unwrap();
    if ca { certificate.append_extension(BasicConstraints::new().critical().ca().build().unwrap()).unwrap(); }
    certificate.append_extension(if ca { KeyUsage::new().key_cert_sign().build().unwrap() } else { KeyUsage::new().digital_signature().build().unwrap() }).unwrap();
    for extension in extras { certificate.append_extension(extension).unwrap(); }
    certificate.sign(signer, MessageDigest::sha256()).unwrap(); certificate.build()
}
fn signed_with_public_key_parameters(certificate: &X509, signer: &PKey<Private>, parameters: &[u8]) -> X509 {
    fn element<'a>(input: &mut &'a [u8], tag: u8) -> &'a [u8] {
        assert_eq!(input[0], tag);
        let (header, length) = if input[1] < 128 { (2, usize::from(input[1])) } else {
            let count = usize::from(input[1] & 127);
            (2 + count, input[2..2 + count].iter().fold(0usize, |length, byte| (length << 8) | usize::from(*byte)))
        };
        let value = &input[header..header + length];
        *input = &input[header + length..];
        value
    }
    let encoded = certificate.to_der().unwrap();
    let mut outer = element(&mut encoded.as_slice(), 0x30);
    let signed = element(&mut outer, 0x30);
    let signature_algorithm = outer;
    element(&mut outer, 0x30);
    let signature_algorithm = &signature_algorithm[..signature_algorithm.len() - outer.len()];
    let mut fields = signed;
    for tag in [0xa0, 2, 0x30, 0x30, 0x30, 0x30] { element(&mut fields, tag); }
    let prefix = &signed[..signed.len() - fields.len()];
    let mut public_key = element(&mut fields, 0x30);
    let mut algorithm = element(&mut public_key, 0x30);
    let identifier = algorithm;
    element(&mut algorithm, 6);
    let identifier = &identifier[..identifier.len() - algorithm.len()];
    let algorithm = der(0x30, &[identifier, parameters].concat());
    let public_key = der(0x30, &[algorithm.as_slice(), public_key].concat());
    let signed = der(0x30, &[prefix, public_key.as_slice(), fields].concat());
    let mut signature = openssl::sign::Signer::new(MessageDigest::sha256(), signer).unwrap();
    signature.update(&signed).unwrap();
    let signature = der(3, &[vec![0], signature.sign_to_vec().unwrap()].concat());
    let encoded = der(0x30, &[signed.as_slice(), signature_algorithm, signature.as_slice()].concat());
    let certificate = X509::from_der(&encoded).unwrap();
    assert_eq!(certificate.to_der().unwrap(), encoded);
    assert!(certificate.verify(signer).unwrap());
    certificate
}
fn store(anchors: &[X509]) -> openssl::x509::store::X509Store {
    let mut store = X509StoreBuilder::new().unwrap(); for anchor in anchors { store.add_cert(anchor.clone()).unwrap(); } store.build()
}
fn with_builder<T>(peer: &[X509], anchors: &[X509], operation: impl FnOnce(&mut Builder<'_, '_>) -> T) -> T {
    let memory = MemoryBudget::new(16 << 20); let charge = memory.reserve(1024).unwrap();
    let mut context = X509StoreContext::new().unwrap(); let untrusted = Stack::new().unwrap();
    let result = context.init(&store(anchors), &peer[0], &untrusted, |context| {
        let mut trusted = Stack::new().unwrap(); for anchor in anchors { trusted.push(anchor.clone()).unwrap(); }
        let date = now();
        let source = Search { peer: peer.iter().map(X509::as_ref).collect(), anchors: trusted, source: context, now: &date, charge: &charge };
        Ok(operation(&mut Builder::new(&source).unwrap()))
    }).unwrap();
    drop(charge); assert_eq!(memory.used_bytes(), 0); result
}
fn verify(peer: &[X509], anchors: &[X509]) -> Result<super::super::super::VerifiedChain> {
    let memory = MemoryBudget::new(16 << 20); let charge = memory.reserve(1024).unwrap();
    super::super::super::verify_store(&store(anchors), peer, &now(), &charge)
}

#[test]
fn certificate_store_hash_order_precedes_asymmetric_tree_insertion() {
    let key = key();
    let peer = (0..4).map(|index| certificate("issuer", "root", &key, &key, index + 1, true, vec![])).collect::<Vec<_>>();
    let root = certificate("root", "root", &key, &key, 10, true, vec![]);
    with_builder(&peer, &[root], |builder| {
        for (candidate, hash) in builder.candidates.iter_mut().zip([7, 1, 17, 2]) { candidate.hash = hash; }
        let order = builder.certificate_order(16, [0, 1, 2, 3, 1]);
        assert_eq!(order, [1, 2, 3, 0]);
        assert_eq!(tree::ordered(order, |left, right| builder.compare(None, left, right)).unwrap(), [0, 3, 2, 1]);
    });
}

#[test]
fn comparator_prefers_strict_raw_skid_match_before_trusted_issuer() {
    let key = key();
    let peer = [certificate("issuer", "other", &key, &key, 1, true, vec![skid(b"key")]),
        certificate("issuer", "root", &key, &key, 2, true, vec![]),
        certificate("issuer", "other", &key, &key, 3, true, vec![extension("2.5.29.14", false, &[4, 0x81, 3, b'k', b'e', b'y'])]),
        certificate("issuer", "root", &key, &key, 4, true, vec![skid(b"key")])];
    let root = certificate("root", "root", &key, &key, 10, true, vec![]);
    with_builder(&peer, &[root], |builder| {
        let key = der(4, b"key");
        assert_eq!(builder.compare(Some(&key), 0, 1).unwrap(), Ordering::Less);
        assert_eq!(builder.compare(Some(&key), 1, 0).unwrap(), Ordering::Greater);
        assert_eq!(builder.compare(Some(&key), 2, 1).unwrap(), Ordering::Greater);
        assert_eq!(builder.compare(Some(&key), 0, 3).unwrap(), Ordering::Less);
        assert_eq!(builder.compare(Some(&key), 3, 0).unwrap(), Ordering::Less);
        assert_eq!(builder.compare(Some(&key), 0, 0).unwrap(), Ordering::Equal);
    });
}

#[test]
fn namespace_preference_uses_the_first_trusted_common_nongeographical_ancestor() {
    let key = key();
    let peer = [certificate("issuer", "C=FR/O=A/OU=far/CN=ca", &key, &key, 1, true, vec![]),
        certificate("issuer", "C=FR/O=B", &key, &key, 2, true, vec![])];
    let roots = [certificate("C=FR/O=A/CN=root", "C=FR/O=A/CN=root", &key, &key, 10, true, vec![]),
        certificate("C=FR/O=B/CN=root", "C=FR/O=B/CN=root", &key, &key, 11, true, vec![])];
    with_builder(&peer, &roots, |builder| {
        builder.subjects.sort_by(|left, right| left.canonical.cmp(&right.canonical));
        assert_eq!(builder.compare(None, 0, 1).unwrap(), Ordering::Less);
        builder.subjects.reverse();
        assert_eq!(builder.compare(None, 0, 1).unwrap(), Ordering::Greater);
    });
}

#[test]
fn repeated_candidates_compare_java_subject_public_key_and_original_parsed_san() {
    let key = key();
    let peer = [certificate("ÉCOLE", "root", &key, &key, 1, true, vec![]),
        certificate("e\u{301}cole", "other", &key, &key, 2, true, vec![]),
        certificate("ÉCOLE", "root", &key, &key, 3, true, vec![extension("2.5.29.17", false, &[5, 0])]),
        certificate("ÉCOLE", "root", &key, &key, 4, true, vec![san(b"A.example")]),
        certificate("ÉCOLE", "root", &key, &key, 5, true, vec![san(b"a.example")]),
        certificate("ÉCOLE", "other", &key, &key, 6, true, vec![san(b"A.example")])];
    with_builder(&peer, &[], |builder| {
        assert!(builder.repeated(0, 0).unwrap()); assert!(builder.repeated(0, 1).unwrap()); assert!(builder.repeated(0, 2).unwrap());
        assert!(!builder.repeated(0, 3).unwrap()); assert!(!builder.repeated(3, 4).unwrap()); assert!(builder.repeated(3, 5).unwrap());
    });
}

#[test]
fn builder_rejects_repeated_keys_with_ber_null_parameters_on_a_signed_bridge() {
    let root_key = key(); let repeated_key = key(); let issuer_key = key();
    let root = certificate("root", "root", &root_key, &root_key, 10, true, vec![]);
    let target = certificate("subject", "issuer", &repeated_key, &issuer_key, 1, false,
        vec![BasicConstraints::new().critical().ca().build().unwrap()]);
    let decoy = certificate("decoy", "root", &root_key, &root_key, 20, true, vec![]);
    let issuer = certificate("issuer", "subject", &issuer_key, &repeated_key, 2, true, vec![]);
    let distinct = certificate("subject", "root", &repeated_key, &root_key, 3, true, vec![san(b"bridge.example")]);
    let bridge = certificate("subject", "root", &repeated_key, &root_key, 4, true, vec![]);
    assert!(verify(&[target.clone(), decoy.clone(), issuer.clone(), distinct], &[root.clone()]).is_ok());
    for parameters in [&[5, 0x81, 0][..], &[5, 0x84, 0, 0, 0, 0][..]] {
        let bridge = signed_with_public_key_parameters(&bridge, &root_key, parameters);
        let peer = [target.clone(), decoy.clone(), issuer.clone(), bridge];
        with_builder(&peer, &[root.clone()], |builder| {
            assert!(builder.repeated(0, 3).unwrap());
            assert!(builder.frame(&[0, 2], 1).unwrap().candidates.is_empty());
            assert!(builder.verify().is_err());
        });
        assert!(verify(&peer, &[root.clone()]).is_err());
    }
}

#[test]
fn builder_merges_trusted_and_peer_candidates_before_applying_repetition() {
    let key = key();
    let root = certificate("root", "root", &key, &key, 10, true, vec![]);
    let peer = [certificate("issuer", "root", &key, &key, 1, true, vec![]), root.clone()];
    with_builder(&peer, &[root], |builder| {
        let frame = builder.frame(&[0], 0).unwrap();
        assert_eq!(frame.candidates.len(), 1);
        assert!(builder.anchor(frame.candidates[0]).is_some());
        assert!(builder.frame(&[0, frame.candidates[0]], 0).unwrap().candidates.is_empty());
    });
}

#[test]
fn builder_depth_limit_keeps_anchors_and_excludes_peer_candidates_at_five() {
    let root_key = key(); let other_key = key();
    let root = certificate("root", "root", &root_key, &root_key, 10, true, vec![]);
    let peer = [certificate("issuer", "root", &root_key, &root_key, 1, true, vec![]),
        certificate("root", "root", &other_key, &root_key, 2, true, vec![])];
    with_builder(&peer, &[root], |builder| {
        assert!(builder.frame(&[0], 4).unwrap().candidates.iter().any(|index| *index == 1));
        let limited = builder.frame(&[0], 5).unwrap();
        assert!(!limited.candidates.is_empty());
        assert!(limited.candidates.iter().all(|index| builder.anchor(*index).is_some()));
    });
}

#[test]
fn selected_anchor_ca_failure_does_not_retry_a_valid_second_anchor() {
    let root_key = key(); let issuer_key = key();
    for ca_extension in [false, true] {
        let mut extra = vec![skid(b"chosen")];
        if ca_extension { extra.push(BasicConstraints::new().critical().ca().build().unwrap()); }
        let bad = certificate("root", "root", &root_key, &root_key, 10, false, extra);
        let good = certificate("root", "root", &root_key, &root_key, 11, true, vec![skid(b"other")]);
        let issuer = certificate("issuer", "root", &issuer_key, &root_key, 2, true, vec![aki(b"chosen")]);
        let leaf = certificate("server", "issuer", &key(), &issuer_key, 1, false, vec![]);
        let peer = [leaf, good.clone(), issuer];
        assert!(verify(&peer, &[good.clone()]).is_ok());
        with_builder(&peer, &[good.clone(), bad.clone()], |builder| {
            let selected = builder.verify().unwrap();
            assert_eq!(selected.last().unwrap().to_der().unwrap(), bad.to_der().unwrap());
        });
        assert!(verify(&peer, &[good, bad]).is_err());
    }
}

#[test]
fn successive_self_issued_candidate_returns_the_branch_before_later_anchor() {
    let root_key = key(); let first_key = key(); let second_key = key();
    let root = certificate("CA", "CA", &root_key, &root_key, 10, true, vec![skid(b"root")]);
    let first = certificate("CA", "CA", &first_key, &root_key, 2, true, vec![skid(b"first"), aki(b"second")]);
    let leaf = certificate("server", "CA", &key(), &first_key, 1, false, vec![aki(b"first")]);
    let decoy = certificate("decoy", "CA", &root_key, &root_key, 20, true, vec![]);
    assert!(verify(&[leaf.clone(), decoy.clone(), first.clone()], &[root.clone()]).is_ok());
    let second = certificate("CA", "CA", &second_key, &root_key, 3, true, vec![skid(b"second")]);
    assert!(verify(&[leaf.clone(), decoy.clone(), first.clone(), second], &[root.clone()]).is_err());
    let rejected = certificate("CA", "CA", &second_key, &root_key, 4, true,
        vec![skid(b"second"), extension("1.2.3.4.5", true, &[5, 0])]);
    assert!(verify(&[leaf, decoy, first, rejected], &[root]).is_ok());
}

#[test]
fn leaf_tls_usage_is_checked_after_a_path_is_selected() {
    let root_key = key(); let issuer_key = key();
    let root = certificate("root", "root", &root_key, &root_key, 10, true, vec![]);
    let issuer = certificate("issuer", "root", &issuer_key, &root_key, 2, true, vec![]);
    let leaf = certificate("server", "issuer", &key(), &issuer_key, 1, false,
        vec![extension("2.5.29.37", false, &der(0x30, &der(6, &[0x2b, 6, 1, 5, 5, 7, 3, 2])))]);
    let peer = [leaf, root.clone(), issuer];
    with_builder(&peer, &[root.clone()], |builder| { assert!(builder.verify().is_ok()); });
    assert!(verify(&peer, &[root]).is_err());
}

#[test]
fn default_sized_trust_sets_retain_compact_metadata_and_release_comparison_scratch() {
    let key = key();
    let roots = (1..=150).map(|serial| certificate("root", "root", &key, &key, serial, true, vec![])).collect::<Vec<_>>();
    let leaf = certificate("server", "root", &key, &key, 200, false, vec![]);
    with_builder(&[leaf], &roots, |builder| {
        let retained = builder.source.charge.budget().used_bytes();
        assert!(retained < 2 << 20);
        for _ in 0..3 { let _ = builder.frame(&[0], 0).unwrap(); assert_eq!(builder.source.charge.budget().used_bytes(), retained); }
    });
}

#[test]
fn selector_filters_path_length_before_tree_insertion_and_forward_checks_key_usage() {
    let root_key = key();
    let constraints = |length| extension("2.5.29.19", true, &der(0x30, &[der(1, &[1]), der(2, &[length])].concat()));
    let peer = [certificate("leaf", "issuer", &root_key, &root_key, 1, false, vec![]),
        certificate("issuer", "root", &root_key, &root_key, 2, false, vec![constraints(0)]),
        certificate("issuer", "root", &root_key, &root_key, 3, false, vec![constraints(1)])];
    with_builder(&peer, &[], |builder| {
        assert!(!builder.matches(1, 0, Some(1)).unwrap());
        assert!(builder.matches(2, 0, Some(1)).unwrap());
        assert!(builder.forward(2, false).is_err());
        assert!(builder.forward(2, true).is_ok());
    });
}

#[test]
fn failed_search_releases_attempt_and_frame_reservations_before_another_call() {
    let root_key = key(); let wrong_key = key();
    let root = certificate("root", "root", &root_key, &root_key, 10, true, vec![]);
    let leaf = certificate("server", "issuer", &key(), &wrong_key, 1, false, vec![]);
    let mut peer = vec![leaf];
    for serial in 2..20 { peer.push(certificate("issuer", "root", &root_key, &root_key, serial, true, vec![])); }
    with_builder(&peer, &[root], |builder| {
        let retained = builder.source.charge.budget().used_bytes();
        for _ in 0..3 { assert!(builder.verify().is_err()); assert_eq!(builder.source.charge.budget().used_bytes(), retained); }
    });
}
