use super::*;
use openssl::{asn1::Asn1Time, bn::BigNum, ec::{EcGroup, EcKey}, hash::MessageDigest, nid::Nid, pkey::PKey, x509::{X509, X509Extension, X509NameBuilder, extension::{ExtendedKeyUsage, KeyUsage}}};
use rtdb_columnar::MemoryBudget;
use rtdb_tls::{KeyStore, StoreFormat, StoreUse};

fn manager(algorithm: Algorithm) -> KeyManager { KeyManager { algorithm, order: vec![0, 1, 2] } }

#[test]
fn sunx509_uses_requested_key_priority_without_usage_or_expiration_ranking() {
    let manager = manager(Algorithm::SunX509);
    let kinds = [KeyType::Ec, KeyType::Rsa, KeyType::Rsa];
    assert_eq!(manager.choose(&[KeyType::Rsa, KeyType::Ec], |index, kind| kinds[index] == kind, |_| panic!("SunX509 does not check usage or dates")), Some(1));
    assert_eq!(manager.choose(&[KeyType::Rsa], |index, kind| index != 1 && kinds[index] == kind, |_| unreachable!()), Some(2));
    assert_eq!(manager.choose(&[KeyType::Dsa], |index, kind| kinds[index] == kind, |_| unreachable!()), None);
}

#[test]
fn new_sunx509_prefers_the_first_requested_type_with_valid_extensions_and_dates() {
    let manager = manager(Algorithm::NewSunX509);
    let kinds = [KeyType::Ec, KeyType::Rsa, KeyType::Rsa];
    assert_eq!(manager.choose(&[KeyType::Rsa, KeyType::Ec], |index, kind| kinds[index] == kind, |_| Status::Valid), Some(1));
    let statuses = [Status::Valid, Status::Expired, Status::ExtensionMismatch];
    assert_eq!(manager.choose(&[KeyType::Rsa, KeyType::Ec], |index, kind| kinds[index] == kind, |index| statuses[index]), Some(0));
    let kinds = [KeyType::Ec, KeyType::Rsa, KeyType::Ec];
    assert_eq!(manager.choose(&[KeyType::Dsa, KeyType::Rsa, KeyType::Ec], |index, kind| kinds[index] == kind, |_| Status::Valid), Some(0));
}

#[test]
fn new_sunx509_fallback_prefers_expired_then_key_priority_then_store_order() {
    let manager = manager(Algorithm::NewSunX509);
    let kinds = [KeyType::Rsa, KeyType::Ec, KeyType::Rsa];
    let statuses = [Status::ExtensionMismatch, Status::Expired, Status::Expired];
    assert_eq!(manager.choose(&[KeyType::Rsa, KeyType::Ec], |index, kind| kinds[index] == kind, |index| statuses[index]), Some(2));
    assert_eq!(manager.choose(&[KeyType::Rsa, KeyType::Ec], |index, kind| kinds[index] == kind, |_| Status::Expired), Some(0));
    assert_eq!(manager.choose(&[KeyType::Dsa], |index, kind| kinds[index] == kind, |_| Status::Expired), None);
}

#[test]
fn manager_alias_order_distinguishes_java_hashmap_from_keystore_enumeration() {
    let fixture = include_bytes!("../../../../tls/tests/fixtures/rsa1024.jks");
    let alias_length = usize::from(u16::from_be_bytes(fixture[16..18].try_into().unwrap()));
    let payload = &fixture[18 + alias_length..fixture.len() - 20];
    let mut bytes = fixture[..8].to_vec(); bytes.extend_from_slice(&3u32.to_be_bytes());
    for alias in [b"a", b"b", b"z"] {
        bytes.extend_from_slice(&1u32.to_be_bytes()); bytes.extend_from_slice(&1u16.to_be_bytes());
        bytes.extend_from_slice(alias); bytes.extend_from_slice(payload);
    }
    bytes.extend_from_slice(&[0; 20]);
    let memory = MemoryBudget::new(1 << 24);
    let charge = memory.reserve(KeyStore::allocation_bound_with_passwords(bytes.len(), None, Some("12345678")).unwrap()).unwrap();
    let store = KeyStore::decode(&bytes, StoreFormat::Java, StoreUse::Identity, None, Some("12345678"), charge).unwrap();
    let chosen = |algorithm| {
        let manager = KeyManager::new(algorithm, store.identities());
        let index = manager.choose(&[KeyType::Rsa], |_, _| true, |_| Status::Valid).unwrap();
        String::from_utf16(store.identities()[index].alias().unwrap()).unwrap()
    };
    assert_eq!(chosen(Algorithm::SunX509), "a");
    assert_eq!(chosen(Algorithm::NewSunX509), "b");
    drop(store); assert_eq!(memory.used_bytes(), 0);
}

fn certificate(extensions: Vec<X509Extension>, before: i64, after: i64) -> X509 {
    let group = EcGroup::from_curve_name(Nid::X9_62_PRIME256V1).unwrap();
    let key = PKey::from_ec_key(EcKey::generate(&group).unwrap()).unwrap();
    let mut name = X509NameBuilder::new().unwrap(); name.append_entry_by_text("CN", "client").unwrap(); let name = name.build();
    let mut certificate = X509::builder().unwrap(); certificate.set_version(2).unwrap();
    certificate.set_serial_number(&BigNum::from_u32(1).unwrap().to_asn1_integer().unwrap()).unwrap();
    certificate.set_subject_name(&name).unwrap(); certificate.set_issuer_name(&name).unwrap(); certificate.set_pubkey(&key).unwrap();
    certificate.set_not_before(&Asn1Time::from_unix(before).unwrap()).unwrap(); certificate.set_not_after(&Asn1Time::from_unix(after).unwrap()).unwrap();
    for extension in extensions { certificate.append_extension(extension).unwrap(); }
    certificate.sign(&key, MessageDigest::sha256()).unwrap(); certificate.build()
}

#[test]
fn client_status_checks_usage_before_dates_and_accepts_absent_usage_extensions() {
    let now = Asn1Time::from_unix(1_700_000_000).unwrap();
    let valid = || (1_600_000_000, 1_800_000_000);
    for extensions in [vec![], vec![ExtendedKeyUsage::new().client_auth().build().unwrap()], vec![KeyUsage::new().digital_signature().build().unwrap()]] {
        let (before, after) = valid(); assert_eq!(client_status(&certificate(extensions, before, after), &now), Status::Valid);
    }
    for extensions in [vec![ExtendedKeyUsage::new().server_auth().build().unwrap()], vec![KeyUsage::new().key_encipherment().build().unwrap()], vec![ExtendedKeyUsage::new().client_auth().build().unwrap(), ExtendedKeyUsage::new().client_auth().build().unwrap()]] {
        assert_eq!(client_status(&certificate(extensions, 1_500_000_000, 1_600_000_000), &now), Status::ExtensionMismatch);
    }
    for (before, after) in [(1_500_000_000, 1_600_000_000), (1_800_000_000, 1_900_000_000)] {
        assert_eq!(client_status(&certificate(vec![], before, after), &now), Status::Expired);
    }
    assert_eq!(client_status(&certificate(vec![], 1_700_000_000, 1_700_000_000), &now), Status::Valid);
}

#[test]
fn builtin_key_manager_aliases_and_signature_key_order_follow_sunjsse() {
    assert_eq!(Algorithm::parse(None).unwrap(), Algorithm::SunX509);
    assert_eq!(Algorithm::parse(Some("sunx509")).unwrap(), Algorithm::SunX509);
    for name in ["NewSunX509", "newSUNx509", "PKIX", "pkix"] { assert_eq!(Algorithm::parse(Some(name)).unwrap(), Algorithm::NewSunX509); }
    for name in ["", "custom.Manager", "SunPKIX"] { assert!(Algorithm::parse(Some(name)).is_err()); }
    assert_eq!(requested_types([0x080a, 0x0804, 0x0403, 0x0401, 0x0807, 0x0808, 0x0402, 0xffff]), vec![KeyType::RsaPss, KeyType::Rsa, KeyType::Ec, KeyType::EdDsa, KeyType::Dsa]);
}

fn client_status(certificate: &X509Ref, now: &Asn1TimeRef) -> Status {
    super::client_status(certificate, now, &MemoryBudget::new(1 << 20)).unwrap()
}

mod java_usage;
