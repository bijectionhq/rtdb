use super::*;

const SERVER: &[u8] = &[0x2b, 6, 1, 5, 5, 7, 3, 1];
const CLIENT: &[u8] = &[0x2b, 6, 1, 5, 5, 7, 3, 2];
const ANY: &[u8] = &[0x55, 0x1d, 0x25, 0];
const MICROSOFT_SGC: &[u8] = &[0x2b, 6, 1, 4, 1, 0x82, 0x37, 0x0a, 3, 3];
const NETSCAPE_SGC: &[u8] = &[0x60, 0x86, 0x48, 1, 0x86, 0xf8, 0x42, 4, 1];

fn extension(critical: bool, value: &[u8]) -> Vec<u8> {
    encoded(0x30, &[encoded(6, &[0x55, 0x1d, 0x25]), encoded(1, &[u8::from(critical)]), encoded(4, value)].concat())
}
fn certificate(extensions: &[Vec<u8>]) -> Vec<u8> {
    let mut tbs = vec![encoded(0xa0, &encoded(2, &[2])), encoded(2, &[1])];
    tbs.extend((0..5).map(|_| encoded(0x30, &[])));
    tbs.push(encoded(0xa3, &encoded(0x30, &extensions.concat())));
    encoded(0x30, &[encoded(0x30, &tbs.concat()), encoded(0x30, &[]), encoded(3, &[0])].concat())
}
fn usages(oids: &[&[u8]]) -> Vec<u8> { encoded(0x30, &oids.iter().flat_map(|oid| encoded(6, oid)).collect::<Vec<_>>()) }
fn decode(extensions: &[Vec<u8>]) -> CertificateExtendedKeyUsage {
    CertificateExtendedKeyUsage::decode(&certificate(extensions), &MemoryBudget::new(16 << 20)).unwrap()
}

#[test]
fn server_usage_accepts_absence_and_each_pinned_server_purpose() {
    assert!(decode(&[]).permits_server_authentication().unwrap());
    for oid in [SERVER, ANY, MICROSOFT_SGC, NETSCAPE_SGC] {
        for critical in [false, true] {
            assert!(decode(&[extension(critical, &usages(&[oid]))]).permits_server_authentication().unwrap());
            assert!(decode(&[extension(critical, &usages(&[CLIENT, oid, CLIENT]))]).permits_server_authentication().unwrap());
        }
    }
    for oids in [vec![], vec![CLIENT], vec![CLIENT, CLIENT], vec![&[0x2a, 3, 4][..]]] {
        assert!(!decode(&[extension(true, &usages(&oids))]).permits_server_authentication().unwrap());
    }
}

#[test]
fn malformed_noncritical_eku_is_queryable_as_an_error_without_discarding_the_certificate() {
    for value in [vec![], encoded(5, &[]), encoded(0x31, &[]), encoded(4, &usages(&[SERVER])),
        [usages(&[SERVER]), vec![0]].concat(), encoded(0x30, &encoded(5, &[])), encoded(0x30, &encoded(6, &[]))] {
        let usage = decode(&[extension(false, &value)]);
        assert_eq!(usage.permits_server_authentication().unwrap_err().code(), "rtdb.tls/invalid-keystore");
        assert_eq!(CertificateExtendedKeyUsage::decode(&certificate(&[extension(true, &value)]), &MemoryBudget::new(1 << 20)).unwrap_err().code(), "rtdb.tls/invalid-keystore");
    }
}

#[test]
fn parsed_eku_wins_over_malformed_copies_and_two_parsed_copies_fail() {
    let bad = encoded(5, &[]);
    for (good, expected) in [(usages(&[]), false), (usages(&[CLIENT]), false), (usages(&[SERVER]), true)] {
        for extensions in [vec![extension(false, &bad), extension(false, &good)], vec![extension(false, &good), extension(false, &bad)]] {
            assert_eq!(decode(&extensions).permits_server_authentication().unwrap(), expected);
        }
        for critical in [false, true] {
            assert!(CertificateExtendedKeyUsage::decode(&certificate(&[extension(false, &good), extension(critical, &good)]), &MemoryBudget::new(1 << 20)).is_err());
            assert!(CertificateExtendedKeyUsage::decode(&certificate(&[extension(false, &good), extension(true, &bad)]), &MemoryBudget::new(1 << 20)).is_err());
        }
    }
    assert!(decode(&[extension(false, &bad), extension(false, &bad)]).permits_server_authentication().is_err());
}

#[test]
fn accepted_server_oid_does_not_hide_malformed_later_purposes() {
    for oid in [vec![], vec![0x80, 0], vec![0x2a, 0x80, 0], vec![0x2a, 0x81], vec![1; 4097]] {
        let value = encoded(0x30, &[encoded(6, SERVER), encoded(6, &oid)].concat());
        assert!(decode(&[extension(false, &value)]).permits_server_authentication().is_err());
        assert!(CertificateExtendedKeyUsage::decode(&certificate(&[extension(true, &value)]), &MemoryBudget::new(16 << 20)).is_err());
    }
    let mut large = vec![0xff; 4096]; large[4095] = 0x7f;
    assert!(decode(&[extension(true, &usages(&[&large, SERVER]))]).permits_server_authentication().unwrap());
}

#[test]
fn eku_accepts_ber_lengths_and_constructed_indefinite_sequences() {
    let oid = [vec![6, 0x81, SERVER.len() as u8], SERVER.to_vec()].concat();
    for value in [
        [vec![0x30, 0x81, oid.len() as u8], oid.clone()].concat(),
        [vec![0x30, 0x80], encoded(6, SERVER), vec![0, 0]].concat(),
    ] {
        assert!(decode(&[extension(true, &value)]).permits_server_authentication().unwrap());
    }
    let padding = [vec![0x30, 0x80], oid, vec![0, 0]].concat();
    assert!(decode(&[extension(false, &padding)]).permits_server_authentication().is_err());
    assert!(CertificateExtendedKeyUsage::decode(&certificate(&[extension(true, &padding)]), &MemoryBudget::new(1 << 20)).is_err());
}

#[test]
fn eku_admission_covers_absent_unparseable_and_parsed_states_until_drop() {
    let memory = MemoryBudget::new(1 << 20);
    let bytes = certificate(&[extension(true, &usages(&[SERVER]))]);
    let parsed = CertificateExtendedKeyUsage::decode(&bytes, &memory).unwrap();
    assert!(parsed.permits_server_authentication().unwrap());
    let baseline = memory.used_bytes(); assert!(baseline > bytes.len());
    for extensions in [vec![], vec![extension(false, &[5, 0])]] {
        let value = CertificateExtendedKeyUsage::decode(&certificate(&extensions), &memory).unwrap();
        assert!(memory.used_bytes() > baseline); drop(value); assert_eq!(memory.used_bytes(), baseline);
    }
    assert!(CertificateExtendedKeyUsage::decode(&certificate(&[extension(true, &[5, 0])]), &memory).is_err());
    assert_eq!(memory.used_bytes(), baseline);
    drop(parsed); assert_eq!(memory.used_bytes(), 0);
    assert_eq!(CertificateExtendedKeyUsage::decode(&[0xff], &MemoryBudget::new(0)).unwrap_err().code(), "rtdb/memory-budget-exceeded");
}

#[test]
fn client_usage_accepts_absence_any_and_client_purposes_without_server_sgc_permissions() {
    assert!(decode(&[]).permits_client_authentication().unwrap());
    for (oid, expected) in [(CLIENT, true), (ANY, true), (SERVER, false), (MICROSOFT_SGC, false), (NETSCAPE_SGC, false), (&[0x2a, 3, 4][..], false)] {
        for critical in [false, true] {
            assert_eq!(decode(&[extension(critical, &usages(&[oid]))]).permits_client_authentication().unwrap(), expected);
            assert!(decode(&[extension(critical, &usages(&[oid, CLIENT, oid]))]).permits_client_authentication().unwrap());
        }
    }
    assert!(!decode(&[extension(true, &usages(&[]))]).permits_client_authentication().unwrap());
}

#[test]
fn client_usage_preserves_unparseable_and_parsed_duplicate_decisions_independently_of_server_usage() {
    let bad = encoded(5, &[]);
    assert_eq!(decode(&[extension(false, &bad)]).permits_client_authentication().unwrap_err().code(), "rtdb.tls/invalid-keystore");
    for (good, client, server) in [(usages(&[]), false, false), (usages(&[CLIENT]), true, false), (usages(&[SERVER]), false, true), (usages(&[ANY]), true, true)] {
        for extensions in [vec![extension(false, &bad), extension(false, &good)], vec![extension(false, &good), extension(false, &bad)]] {
            let usage = decode(&extensions);
            assert_eq!(usage.permits_client_authentication().unwrap(), client);
            assert_eq!(usage.permits_server_authentication().unwrap(), server);
        }
        for extensions in [vec![extension(false, &good), extension(false, &good)], vec![extension(false, &good), extension(true, &bad)]] {
            assert!(CertificateExtendedKeyUsage::decode(&certificate(&extensions), &MemoryBudget::new(1 << 20)).is_err());
        }
    }
}

#[test]
fn accepted_client_oid_requires_every_later_purpose_to_parse() {
    for oid in [vec![], vec![0x80, 0], vec![0x2a, 0x80, 0], vec![0x2a, 0x81], vec![1; 4097]] {
        let value = encoded(0x30, &[encoded(6, CLIENT), encoded(6, &oid)].concat());
        assert!(decode(&[extension(false, &value)]).permits_client_authentication().is_err());
        assert!(CertificateExtendedKeyUsage::decode(&certificate(&[extension(true, &value)]), &MemoryBudget::new(16 << 20)).is_err());
    }
    let mut large = vec![0xff; 4096]; large[4095] = 0x7f;
    assert!(decode(&[extension(true, &usages(&[CLIENT, &large]))]).permits_client_authentication().unwrap());
}

#[test]
fn client_usage_accepts_java_ber_framing_and_releases_its_admission() {
    for value in [
        [vec![0x30, 0x81, 10], encoded(6, CLIENT)].concat(),
        [vec![0x30, 0x80], encoded(6, CLIENT), vec![0, 0]].concat(),
    ] {
        let memory = MemoryBudget::new(1 << 20);
        let usage = CertificateExtendedKeyUsage::decode(&certificate(&[extension(true, &value)]), &memory).unwrap();
        assert!(usage.permits_client_authentication().unwrap());
        assert!(!usage.permits_server_authentication().unwrap());
        assert!(memory.used_bytes() > 0);
        drop(usage);
        assert_eq!(memory.used_bytes(), 0);
    }
}
