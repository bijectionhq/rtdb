use super::*;
use openssl::{asn1::Asn1Time, bn::BigNum, ec::{EcGroup, EcKey}, hash::MessageDigest, pkey::{PKey, Private}, x509::{X509Extension, X509NameBuilder, extension::{BasicConstraints, ExtendedKeyUsage, KeyUsage, SubjectAlternativeName}}};
use rtdb_columnar::MemoryBudget;

fn key() -> PKey<Private> { PKey::from_ec_key(EcKey::generate(&EcGroup::from_curve_name(Nid::X9_62_PRIME256V1).unwrap()).unwrap()).unwrap() }
fn certificate(name: &str, key: &PKey<Private>, issuer: Option<(&X509, &PKey<Private>)>, extensions: Vec<X509Extension>, validity: (i64, i64)) -> X509 {
    let mut subject = X509NameBuilder::new().unwrap(); subject.append_entry_by_text("CN", name).unwrap(); let subject = subject.build();
    let mut certificate = X509::builder().unwrap(); certificate.set_version(2).unwrap();
    certificate.set_serial_number(&BigNum::from_u32(1).unwrap().to_asn1_integer().unwrap()).unwrap();
    certificate.set_subject_name(&subject).unwrap(); certificate.set_issuer_name(issuer.map_or(subject.as_ref(), |(issuer, _)| issuer.subject_name())).unwrap();
    certificate.set_pubkey(key).unwrap();
    certificate.set_not_before(&Asn1Time::from_unix(validity.0).unwrap()).unwrap(); certificate.set_not_after(&Asn1Time::from_unix(validity.1).unwrap()).unwrap();
    for extension in extensions { certificate.append_extension(extension).unwrap(); }
    certificate.sign(issuer.map_or(key, |(_, key)| key), MessageDigest::sha256()).unwrap(); certificate.build()
}
fn ca() -> Vec<X509Extension> { vec![BasicConstraints::new().critical().ca().build().unwrap(), KeyUsage::new().key_cert_sign().build().unwrap()] }
fn server() -> Vec<X509Extension> { vec![KeyUsage::new().digital_signature().build().unwrap(), ExtendedKeyUsage::new().server_auth().build().unwrap()] }
const VALID: (i64, i64) = (1_600_000_000, 1_800_000_000);
fn now() -> openssl::asn1::Asn1Time { Asn1Time::from_unix(1_700_000_000).unwrap() }
fn trust(anchors: Vec<X509>) -> SimpleTrust {
    let memory = MemoryBudget::new(1 << 24);
    SimpleTrust::new(anchors, memory.reserve(1 << 20).unwrap()).unwrap()
}

#[test]
fn simple_validation_requires_the_supplied_chain_order_and_a_matching_anchor() {
    let root_key = key(); let root = certificate("root", &root_key, None, ca(), VALID);
    let intermediate_key = key(); let intermediate = certificate("intermediate", &intermediate_key, Some((&root, &root_key)), ca(), VALID);
    let leaf = certificate("server", &key(), Some((&intermediate, &intermediate_key)), server(), VALID);
    let trust = trust(vec![root.clone()]);
    for chain in [vec![leaf.clone(), intermediate.clone()], vec![leaf.clone(), intermediate.clone(), root.clone()]] {
        let result = trust.verify(&chain, &now(), |_| Ok(())).unwrap();
        assert_eq!(result.len(), 3); assert_eq!(result[2].to_der().unwrap(), root.to_der().unwrap());
    }
    for chain in [vec![leaf.clone()], vec![leaf, root, intermediate]] { assert!(trust.verify(&chain, &now(), |_| Ok(())).is_err()); }
    assert!(trust.verify(&[], &now(), |_| Ok(())).is_err());
}

#[test]
fn reissued_anchor_uses_the_trusted_key_and_anchor_validity_is_not_checked() {
    let root_key = key(); let expired = certificate("root", &root_key, None, ca(), (1_500_000_000, 1_550_000_000));
    let reissued = certificate("root", &root_key, None, ca(), VALID);
    let leaf = certificate("server", &key(), Some((&reissued, &root_key)), server(), VALID);
    let trust = trust(vec![expired.clone()]);
    let chain = [leaf.clone(), reissued];
    let verified = trust.verify(&chain, &now(), |_| Ok(())).unwrap();
    assert_eq!(verified[1].to_der().unwrap(), expired.to_der().unwrap());
    assert!(trust.verify(&[leaf], &now(), |_| Ok(())).is_ok());
}

#[test]
fn authority_key_identifier_selects_a_matching_anchor_before_subject_fallback() {
    use openssl::asn1::{Asn1Object, Asn1OctetString};
    let identifier = |authority: bool, value: u8| {
        let mut bytes = if authority { vec![0x30, 0x16, 0x80, 0x14] } else { vec![0x04, 0x14] };
        bytes.extend_from_slice(&[value; 20]);
        X509Extension::new_from_der(&Asn1Object::from_str(if authority { "2.5.29.35" } else { "2.5.29.14" }).unwrap(), false, &Asn1OctetString::new_from_bytes(&bytes).unwrap()).unwrap()
    };
    let first_key = key(); let mut extensions = ca(); extensions.push(identifier(false, 1));
    let first = certificate("root", &first_key, None, extensions, VALID);
    let second_key = key(); let mut extensions = ca(); extensions.push(identifier(false, 2));
    let second = certificate("root", &second_key, None, extensions, VALID);
    let trust = trust(vec![first.clone(), second.clone()]);
    let (issuer, issuer_key, id) = if trust.anchors[0].public_key().unwrap().public_eq(&first_key) { (&second, &second_key, 2) } else { (&first, &first_key, 1) };
    let mut extensions = server(); extensions.push(identifier(true, id));
    let leaf = certificate("server", &key(), Some((issuer, issuer_key)), extensions, VALID);
    let peer = [leaf]; let verified = trust.verify(&peer, &now(), |_| Ok(())).unwrap();
    assert_eq!(verified[1].to_der().unwrap(), issuer.to_der().unwrap());
    let leaf = certificate("server", &key(), Some((issuer, issuer_key)), server(), VALID);
    assert!(trust.verify(&[leaf], &now(), |_| Ok(())).is_err());
}

#[test]
fn directly_trusted_leaf_skips_usage_and_dates_but_still_checks_security_policy() {
    let leaf = certificate("trusted leaf", &key(), None, vec![ExtendedKeyUsage::new().client_auth().build().unwrap()], (1_500_000_000, 1_550_000_000));
    let trust = trust(vec![leaf.clone()]);
    assert_eq!(trust.verify(&[leaf.clone()], &now(), |_| Ok(())).unwrap().len(), 1);
    assert!(trust.verify(&[leaf], &now(), |_| Err(invalid("distrusted"))).is_err());
}

#[test]
fn intermediate_eku_is_ignored_but_unknown_critical_extensions_are_rejected() {
    let root_key = key(); let root = certificate("root", &root_key, None, ca(), VALID);
    let trust = trust(vec![root.clone()]);
    let intermediate_key = key(); let mut extensions = ca(); extensions.push(ExtendedKeyUsage::new().critical().client_auth().build().unwrap());
    let intermediate = certificate("intermediate", &intermediate_key, Some((&root, &root_key)), extensions, VALID);
    let leaf = certificate("server", &key(), Some((&intermediate, &intermediate_key)), server(), VALID);
    assert!(trust.verify(&[leaf, intermediate], &now(), |_| Ok(())).is_ok());
    let mut extensions = ca();
    let builder = X509::builder().unwrap();
    extensions.push(SubjectAlternativeName::new().critical().dns("intermediate").build(&builder.x509v3_context(Some(&root), None)).unwrap());
    let intermediate = certificate("intermediate", &intermediate_key, Some((&root, &root_key)), extensions, VALID);
    let leaf = certificate("server", &key(), Some((&intermediate, &intermediate_key)), server(), VALID);
    assert!(trust.verify(&[leaf, intermediate], &now(), |_| Ok(())).is_err());
}

#[test]
fn intermediate_path_length_key_usage_and_signature_are_enforced() {
    let root_key = key(); let root = certificate("root", &root_key, None, ca(), VALID); let trust = trust(vec![root.clone()]);
    let top_key = key();
    let top = certificate("top", &top_key, Some((&root, &root_key)), vec![BasicConstraints::new().ca().pathlen(0).build().unwrap()], VALID);
    let lower_key = key(); let lower = certificate("lower", &lower_key, Some((&top, &top_key)), ca(), VALID);
    let leaf = certificate("server", &key(), Some((&lower, &lower_key)), server(), VALID);
    assert!(trust.verify(&[leaf, lower, top], &now(), |_| Ok(())).is_err());
    let intermediate_key = key();
    let intermediate = certificate("intermediate", &intermediate_key, Some((&root, &root_key)), vec![BasicConstraints::new().ca().build().unwrap(), KeyUsage::new().digital_signature().build().unwrap()], VALID);
    let leaf = certificate("server", &key(), Some((&intermediate, &intermediate_key)), server(), VALID);
    assert!(trust.verify(&[leaf, intermediate], &now(), |_| Ok(())).is_err());
    let forged = certificate("server", &key(), Some((&root, &key())), server(), VALID);
    assert!(trust.verify(&[forged], &now(), |_| Ok(())).is_err());
}

#[test]
fn server_usage_validity_and_empty_trust_are_enforced() {
    let root_key = key(); let root = certificate("root", &root_key, None, ca(), VALID); let trusted = trust(vec![root.clone()]);
    for (extensions, validity) in [(vec![ExtendedKeyUsage::new().client_auth().build().unwrap()], VALID), (server(), (1_500_000_000, 1_550_000_000)), (vec![KeyUsage::new().key_encipherment().build().unwrap()], VALID)] {
        let leaf = certificate("server", &key(), Some((&root, &root_key)), extensions, validity);
        assert!(trusted.verify(&[leaf], &now(), |_| Ok(())).is_err());
    }
    let leaf = certificate("server", &key(), Some((&root, &root_key)), server(), VALID);
    assert!(trust(vec![]).verify(&[leaf], &now(), |_| Ok(())).is_err());
}

#[test]
fn trust_algorithms_preserve_jca_aliases_and_release_retained_material() {
    for name in [None, Some("PKIX"), Some("sunPKIX"), Some("X509"), Some("X.509")] { assert_eq!(Algorithm::parse(name).unwrap(), Algorithm::Pkix); }
    assert_eq!(Algorithm::parse(Some("sunx509")).unwrap(), Algorithm::SunX509);
    for name in ["", "NewSunX509", "custom.TrustManager"] { assert!(Algorithm::parse(Some(name)).is_err()); }
    let memory = MemoryBudget::new(1 << 24);
    let root = certificate("root", &key(), None, ca(), VALID);
    let trust = SimpleTrust::new(vec![root.clone(), root], memory.reserve(1 << 20).unwrap()).unwrap();
    assert_eq!(trust.anchors.len(), 1); assert!(memory.used_bytes() > 0);
    drop(trust); assert_eq!(memory.used_bytes(), 0);
}
