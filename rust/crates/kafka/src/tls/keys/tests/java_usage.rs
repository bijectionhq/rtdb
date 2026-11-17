use super::*;
use super::super::client_status as checked_status;
use openssl::{asn1::{Asn1Object, Asn1OctetString}, dh::Dh, dsa::Dsa, pkey::Private, rsa::Rsa, sign::Signer};
use std::cell::Cell;

const CLIENT: &[u8] = &[0x2b, 6, 1, 5, 5, 7, 3, 2];
const SERVER: &[u8] = &[0x2b, 6, 1, 5, 5, 7, 3, 1];
const ANY: &[u8] = &[0x55, 0x1d, 0x25, 0];
const VALID: (i64, i64) = (1_600_000_000, 1_800_000_000);

fn der(tag: u8, content: &[u8]) -> Vec<u8> {
    let mut result = vec![tag];
    if content.len() < 128 { result.push(content.len() as u8); }
    else {
        let length = content.len().to_be_bytes();
        let start = length.iter().position(|byte| *byte != 0).unwrap();
        result.push(0x80 | (length.len() - start) as u8);
        result.extend_from_slice(&length[start..]);
    }
    result.extend_from_slice(content);
    result
}

fn extension(oid: &str, critical: bool, value: &[u8]) -> X509Extension {
    X509Extension::new_from_der(&Asn1Object::from_str(oid).unwrap(), critical, &Asn1OctetString::new_from_bytes(value).unwrap()).unwrap()
}

fn eku(oids: &[&[u8]]) -> Vec<u8> { der(0x30, &oids.iter().flat_map(|oid| der(6, oid)).collect::<Vec<_>>()) }
fn now() -> Asn1Time { Asn1Time::from_unix(1_700_000_000).unwrap() }
fn status(extensions: Vec<X509Extension>) -> Status { client_status(&certificate(extensions, VALID.0, VALID.1), &now()) }

#[test]
fn client_native_extensions_use_java_malformed_and_duplicate_semantics() {
    let good = [("2.5.29.15", der(3, &[0, 0x80])), ("2.5.29.37", eku(&[CLIENT]))];
    for (oid, value) in good {
        for malformed_first in [false, true] {
            let mut extensions = vec![extension(oid, false, &value), extension(oid, false, &[5, 0])];
            if malformed_first { extensions.reverse(); }
            assert_eq!(status(extensions), Status::Valid, "{oid} {malformed_first}");
        }
        assert_eq!(status(vec![extension(oid, false, &value), extension(oid, false, &value)]), Status::ExtensionMismatch, "{oid}");
        assert_eq!(status(vec![extension(oid, false, &value), extension(oid, true, &[5, 0])]), Status::ExtensionMismatch, "{oid}");
    }
    assert_eq!(status(vec![extension("2.5.29.15", false, &[5, 0])]), Status::Valid);
    assert_eq!(status(vec![extension("2.5.29.15", true, &[5, 0])]), Status::ExtensionMismatch);
    assert_eq!(status(vec![extension("2.5.29.37", false, &[5, 0])]), Status::ExtensionMismatch);
    assert_eq!(status(vec![extension("2.5.29.37", true, &[5, 0])]), Status::ExtensionMismatch);
}

#[test]
fn client_native_usage_accepts_java_octet_wrappers_ber_lengths_and_declared_bits() {
    for value in [der(4, &der(3, &[7, 0xff])), vec![3, 0x81, 2, 7, 0xff], der(3, &[7, 0x80])] {
        assert_eq!(status(vec![extension("2.5.29.15", true, &value)]), Status::Valid);
    }
    for value in [der(3, &[0]), der(3, &[0, 0x08]), der(3, &[7, 0x01])] {
        assert_eq!(status(vec![extension("2.5.29.15", false, &value)]), Status::ExtensionMismatch);
    }
    for value in [
        [vec![0x30, 0x81, 10], der(6, CLIENT)].concat(),
        [vec![0x30, 0x80], der(6, CLIENT), vec![0, 0]].concat(),
    ] {
        assert_eq!(status(vec![extension("2.5.29.37", true, &value)]), Status::Valid);
    }
}

#[test]
fn client_native_eku_accepts_only_client_or_any_before_expiration_ranking() {
    for (oids, expected) in [(vec![], false), (vec![CLIENT], true), (vec![ANY], true), (vec![SERVER], false),
        (vec![&[0x2b, 6, 1, 4, 1, 0x82, 0x37, 0x0a, 3, 3][..]], false),
        (vec![&[0x60, 0x86, 0x48, 1, 0x86, 0xf8, 0x42, 4, 1][..]], false)] {
        for validity in [VALID, (1_500_000_000, 1_600_000_000)] {
            let certificate = certificate(vec![extension("2.5.29.37", false, &eku(&oids))], validity.0, validity.1);
            let expected = if !expected { Status::ExtensionMismatch } else if validity == VALID { Status::Valid } else { Status::Expired };
            assert_eq!(client_status(&certificate, &now()), expected, "{oids:?} {validity:?}");
        }
    }
    let value = der(0x30, &[der(6, CLIENT), der(6, &[0x2a, 0x81])].concat());
    assert_eq!(status(vec![extension("2.5.29.37", false, &value)]), Status::ExtensionMismatch);
}

fn certificate_with_key(signer: &PKey<Private>, public_key: &[u8], usage: Option<&[u8]>) -> X509 {
    let algorithm = der(0x30, &[der(6, &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 1, 11]), vec![5, 0]].concat());
    let name = X509NameBuilder::new().unwrap().build().to_der().unwrap();
    let extensions = usage.map(|usage| der(0xa3, &der(0x30, &der(0x30, &[der(6, &[0x55, 0x1d, 15]), der(4, usage)].concat())))).unwrap_or_default();
    let signed = der(0x30, &[der(0xa0, &der(2, &[2])), der(2, &[1]), algorithm.clone(), name.clone(),
        der(0x30, &[der(0x17, b"200101000000Z"), der(0x17, b"331231235959Z")].concat()), name, public_key.to_vec(), extensions].concat());
    let mut signature = Signer::new(MessageDigest::sha256(), signer).unwrap();
    signature.update(&signed).unwrap();
    let encoded = der(0x30, &[signed, algorithm, der(3, &[vec![0], signature.sign_to_vec().unwrap()].concat())].concat());
    let certificate = X509::from_der(&encoded).unwrap();
    assert_eq!(certificate.to_der().unwrap(), encoded);
    assert!(certificate.verify(signer).unwrap());
    certificate
}

#[test]
fn client_native_key_algorithms_preserve_pss_and_dh_usage_rules() {
    let signer = PKey::from_rsa(Rsa::generate(2048).unwrap()).unwrap();
    let rsa = signer.public_key_to_der().unwrap();
    let pss = der(0x30, &[der(0x30, &der(6, &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 1, 10])),
        der(3, &[vec![0], signer.rsa().unwrap().public_key_to_der_pkcs1().unwrap()].concat())].concat());
    let dsa = PKey::from_dsa(Dsa::generate(2048).unwrap()).unwrap().public_key_to_der().unwrap();
    let ec = PKey::from_ec_key(EcKey::generate(&EcGroup::from_curve_name(Nid::X9_62_PRIME256V1).unwrap()).unwrap()).unwrap().public_key_to_der().unwrap();
    let dh = Dh::get_2048_256().unwrap().generate_key().unwrap();
    let integer = |value: &openssl::bn::BigNumRef| {
        let mut bytes = value.to_vec();
        if bytes.first().is_some_and(|byte| byte & 0x80 != 0) { bytes.insert(0, 0); }
        der(2, &bytes)
    };
    let algorithm = der(0x30, &[der(6, &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 3, 1]), der(0x30, &[integer(dh.prime_p()), integer(dh.generator())].concat())].concat());
    let dh = der(0x30, &[algorithm, der(3, &[vec![0], integer(dh.public_key())].concat())].concat());
    let dhx = PKey::from_dhx(Dh::get_2048_256().unwrap().generate_key().unwrap()).unwrap().public_key_to_der().unwrap();
    let ed = PKey::generate_ed25519().unwrap().public_key_to_der().unwrap();
    for (public_key, kind, required) in [(rsa, Id::RSA, 0x80), (pss, Id::RSA_PSS, 0), (dsa, Id::DSA, 0x80), (ec, Id::EC, 0x80),
        (dh, Id::DH, 8), (dhx, Id::DHX, 8), (ed, Id::ED25519, 0)] {
        for (padding, bits) in [(0, 0), (0, 0x80), (0, 8), (0, 0xff), (4, 8), (3, 8), (7, 0xff)] {
            let certificate = certificate_with_key(&signer, &public_key, Some(&der(3, &[padding, bits])));
            assert_eq!(certificate.public_key().unwrap().id(), kind);
            let permits = required == 0 || bits & (0xff << padding) & required != 0;
            assert_eq!(client_status(&certificate, &now()), if permits { Status::Valid } else { Status::ExtensionMismatch }, "{kind:?} {padding} {bits}");
        }
    }
    let unknown_key = der(0x30, &[der(0x30, &der(6, &[0x2a, 3, 4])), der(3, &[0, 1])].concat());
    let unknown = certificate_with_key(&signer, &unknown_key, Some(&der(3, &[0, 0x80])));
    assert!(unknown.public_key().is_err());
    assert_eq!(client_status(&unknown, &now()), Status::ExtensionMismatch);
    for usage in [None, Some(vec![5, 0]), Some(der(3, &[0, 0x88]))] {
        let unknown = certificate_with_key(&signer, &unknown_key, usage.as_deref());
        assert!(unknown.public_key().is_err());
        assert_eq!(client_status(&unknown, &now()), Status::Valid);
    }
}

#[test]
fn client_status_admits_encoding_before_parsing_and_releases_every_error_path() {
    let certificate = certificate(vec![], VALID.0, VALID.1);
    let bound = certificate.to_der().unwrap().len() + 64;
    for (limit, peak) in [(bound - 1, 0), (bound, bound)] {
        let memory = MemoryBudget::new(limit);
        assert_eq!(checked_status(&certificate, &now(), &memory).unwrap_err().code, "rtdb/memory-budget-exceeded");
        assert_eq!(memory.used_bytes(), 0);
        assert_eq!(memory.peak_bytes(), peak);
    }
    let memory = MemoryBudget::new(1 << 20);
    assert_eq!(checked_status(&certificate, &now(), &memory).unwrap(), Status::Valid);
    assert_eq!(memory.used_bytes(), 0);
    for critical in [false, true] {
        for oid in ["2.5.29.15", "2.5.29.37"] {
            let certificate = super::certificate(vec![extension(oid, critical, &[5, 0])], VALID.0, VALID.1);
            assert!(checked_status(&certificate, &now(), &memory).is_ok());
            assert_eq!(memory.used_bytes(), 0);
        }
    }
}

#[test]
fn new_sunx509_budget_failure_stops_before_later_candidates_or_fallbacks() {
    let manager = manager(Algorithm::NewSunX509);
    let denied = MemoryBudget::new(0).reserve(1).unwrap_err();
    for first in [Status::Expired, Status::ExtensionMismatch, Status::Valid] {
        let calls = Cell::new(0);
        let result = manager.try_choose(&[KeyType::Rsa, KeyType::Ec], |_, kind| kind == KeyType::Ec, |index| {
            calls.set(calls.get() + 1);
            match index { 0 => Ok(first), 1 => Err(denied.clone()), _ => panic!("selection continued after admission failure") }
        });
        assert_eq!(result.unwrap_err(), denied);
        assert_eq!(calls.get(), 2);
    }
    assert_eq!(manager.try_choose(&[KeyType::Rsa], |_, _| true, |_| Ok(Status::Valid)).unwrap(), Some(0));
    assert_eq!(manager.try_choose(&[KeyType::Rsa], |_, _| false, |_| panic!("incompatible identity was ranked")).unwrap(), None);
}

#[test]
fn sunx509_does_not_request_usage_admission_or_status_after_matching_an_alias() {
    let manager = manager(Algorithm::SunX509);
    assert_eq!(manager.try_choose(&[KeyType::Rsa], |index, _| index == 1, |_| panic!("SunX509 ranked client usage")).unwrap(), Some(1));
    assert_eq!(manager.try_choose(&[KeyType::Rsa], |_, _| false, |_| panic!("SunX509 ranked client usage")).unwrap(), None);
}

fn credential_with_usage(usages: &[(bool, &[u8])]) -> crate::tls::tests::support::Credential {
    use crate::tls::tests::support::Credential;
    let original = Credential::new("client usage", true, None);
    let mut certificate = X509::builder().unwrap(); certificate.set_version(2).unwrap();
    certificate.set_serial_number(original.certificate.serial_number()).unwrap();
    certificate.set_subject_name(original.certificate.subject_name()).unwrap(); certificate.set_issuer_name(original.certificate.issuer_name()).unwrap();
    certificate.set_pubkey(&original.key).unwrap();
    certificate.set_not_before(&Asn1Time::from_unix(VALID.0).unwrap()).unwrap(); certificate.set_not_after(&Asn1Time::from_unix(VALID.1).unwrap()).unwrap();
    for &(critical, value) in usages { certificate.append_extension(extension("2.5.29.15", critical, value)).unwrap(); }
    certificate.sign(&original.key, MessageDigest::sha256()).unwrap();
    Credential { key: original.key, certificate: certificate.build(), chain: vec![] }
}

fn deferred(credential: &crate::tls::tests::support::Credential, memory: &Arc<MemoryBudget>) -> crate::tls::identities::Material {
    use crate::tls::identities::Material;
    let bytes = crate::tls::tests::support::jks(&[("client", credential)], "store", "key");
    let charge = memory.reserve(Material::allocation_bound(bytes.len(), StoreFormat::Java, StoreUse::OptionalIdentity, Algorithm::NewSunX509, Some("store"), Some("key")).unwrap()).unwrap();
    Material::decode(&bytes, StoreFormat::Java, StoreUse::OptionalIdentity, Algorithm::NewSunX509, Some("store"), Some("key"), charge).unwrap()
}

#[test]
fn native_new_sun_material_and_selection_keep_java_key_usage_decisions() {
    for usages in [vec![(false, &[5, 0][..])], vec![(false, &[3, 2, 0, 0x80][..]), (false, &[5, 0][..])],
        vec![(false, &[5, 0][..]), (false, &[3, 2, 0, 0x80][..])], vec![(false, &[4, 4, 3, 2, 0, 0x80][..])]] {
        let credential = credential_with_usage(&usages);
        let memory = MemoryBudget::new(32 << 20);
        let material = deferred(&credential, &memory);
        let charge = memory.reserve(65_536).unwrap();
        let random = crate::tls::random::Context::new(None, &memory).unwrap();
        let native = material.native(Algorithm::NewSunX509, &random, &charge).unwrap();
        assert_eq!(native.entries().len(), 1);
        assert!(native.entries()[0].eligible);
        let selected = native.manager().try_choose(&[KeyType::Ec], |index, kind| native.entries()[index].eligible && native.entries()[index].kind == kind,
            |index| checked_status(&native.entries()[index].certificate, &now(), &memory)).unwrap();
        assert_eq!(selected, Some(0));
        drop(native); drop(material); drop(charge); drop(random);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn native_new_sun_material_propagates_chain_usage_admission_instead_of_recording_ineligibility() {
    let credential = credential_with_usage(&[]);
    let memory = MemoryBudget::new(32 << 20);
    let material = deferred(&credential, &memory);
    let charge = memory.reserve(65_536).unwrap();
    let random = crate::tls::random::Context::new(None, &memory).unwrap();
    let native_bound = 65_536 + credential.certificate.to_der().unwrap().len() * 32;
    let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes() - native_bound).unwrap();
    let retained = memory.used_bytes();
    assert_eq!(material.native(Algorithm::NewSunX509, &random, &charge).err().unwrap().code, "rtdb/memory-budget-exceeded");
    assert_eq!(memory.used_bytes(), retained);
    drop(blocker);
    let native = material.native(Algorithm::NewSunX509, &random, &charge).unwrap();
    assert!(native.entries()[0].eligible);
    drop(native); drop(material); drop(charge); drop(random);
    assert_eq!(memory.used_bytes(), 0);
}
