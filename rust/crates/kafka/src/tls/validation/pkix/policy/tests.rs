use super::*;
use openssl::{asn1::{Asn1Object, Asn1OctetString, Asn1Time}, bn::BigNum, hash::MessageDigest, pkey::{PKey, Private}, rsa::Rsa, x509::{X509, X509Extension, X509NameBuilder, extension::{BasicConstraints, KeyUsage}}};
use rtdb_columnar::MemoryBudget;

const P: &[u8] = &[0x2a, 3, 4];
const Q: &[u8] = &[0x2a, 3, 5];
const R: &[u8] = &[0x2a, 3, 6];

struct Fixture { key: PKey<Private>, memory: Arc<MemoryBudget>, charge: Arc<MemoryReservation> }
impl Fixture {
    fn new() -> Self {
        let memory = MemoryBudget::new(32 << 20);
        Self { key: PKey::from_rsa(Rsa::generate(1024).unwrap()).unwrap(), charge: memory.reserve(1024).unwrap(), memory }
    }
    fn certificate(&self, subject: &str, issuer: &str, ca: bool, extensions: Vec<X509Extension>) -> X509 {
        let name = |value: &str| { let mut name = X509NameBuilder::new().unwrap(); name.append_entry_by_text("CN", value).unwrap(); name.build() };
        let mut certificate = X509::builder().unwrap(); certificate.set_version(2).unwrap();
        certificate.set_serial_number(&BigNum::from_u32(1).unwrap().to_asn1_integer().unwrap()).unwrap();
        certificate.set_subject_name(&name(subject)).unwrap(); certificate.set_issuer_name(&name(issuer)).unwrap();
        certificate.set_pubkey(&self.key).unwrap();
        certificate.set_not_before(&Asn1Time::from_unix(1_600_000_000).unwrap()).unwrap();
        certificate.set_not_after(&Asn1Time::from_unix(1_800_000_000).unwrap()).unwrap();
        if ca {
            certificate.append_extension(BasicConstraints::new().critical().ca().build().unwrap()).unwrap();
            certificate.append_extension(KeyUsage::new().critical().key_cert_sign().build().unwrap()).unwrap();
        } else { certificate.append_extension(KeyUsage::new().digital_signature().build().unwrap()).unwrap(); }
        for extension in extensions { certificate.append_extension(extension).unwrap(); }
        certificate.sign(&self.key, MessageDigest::sha256()).unwrap(); certificate.build()
    }
    fn root(&self) -> X509 { self.certificate("root", "root", true, vec![]) }
    fn check(&self, chain: &[&X509Ref]) -> Result<()> {
        let result = verify(chain, &self.charge);
        assert_eq!(self.memory.used_bytes(), 1024);
        result
    }
}

fn der(tag: u8, bytes: &[u8]) -> Vec<u8> {
    let mut encoded = vec![tag];
    if bytes.len() < 128 { encoded.push(bytes.len() as u8); }
    else if bytes.len() < 256 { encoded.extend_from_slice(&[0x81, bytes.len() as u8]); }
    else { encoded.extend_from_slice(&[0x82, (bytes.len() >> 8) as u8, bytes.len() as u8]); }
    encoded.extend_from_slice(bytes); encoded
}
fn extension(oid: &str, critical: bool, bytes: &[u8]) -> X509Extension {
    X509Extension::new_from_der(&Asn1Object::from_str(oid).unwrap(), critical, &Asn1OctetString::new_from_bytes(bytes).unwrap()).unwrap()
}
fn policy(oid: &[u8], qualified: bool) -> Vec<u8> {
    let mut fields = der(6, oid);
    if qualified {
        let mut qualifier = der(6, &[0x2b, 6, 1, 5, 5, 7, 2, 1]);
        qualifier.extend(der(0x16, b"https://example.test/cps"));
        fields.extend(der(0x30, &der(0x30, &qualifier)));
    }
    der(0x30, &fields)
}
fn certificate_policies(critical: bool, policies: &[(&[u8], bool)]) -> X509Extension {
    let entries: Vec<u8> = policies.iter().flat_map(|(oid, qualified)| policy(oid, *qualified)).collect();
    extension("2.5.29.32", critical, &der(0x30, &entries))
}
fn policy_mapping(mappings: &[(&[u8], &[u8])]) -> X509Extension {
    let entries: Vec<u8> = mappings.iter().flat_map(|(issuer, subject)| {
        let mut fields = der(6, issuer); fields.extend(der(6, subject)); der(0x30, &fields)
    }).collect();
    extension("2.5.29.33", true, &der(0x30, &entries))
}
fn inhibit_mapping(count: u8) -> X509Extension { extension("2.5.29.36", true, &der(0x30, &der(0x81, &[count]))) }
fn inhibit_any_policy(count: u8) -> X509Extension { extension("2.5.29.54", true, &der(2, &[count])) }
fn require_explicit(count: u8) -> X509Extension { extension("2.5.29.36", true, &der(0x30, &der(0x80, &[count]))) }

#[test]
fn critical_ordinary_policy_qualifiers_are_rejected_on_a_direct_root_signed_leaf() {
    let fixture = Fixture::new(); let root = fixture.root();
    for (critical, oid, qualified, accepted) in [(true, P, true, false), (false, P, true, true), (true, P, false, true), (true, ANY_POLICY, true, true)] {
        let leaf = fixture.certificate("server", "root", false, vec![certificate_policies(critical, &[(oid, qualified)])]);
        assert_eq!(fixture.check(&[&leaf, &root]).is_ok(), accepted);
    }
}

#[test]
fn anchor_policies_and_exact_direct_trust_do_not_reject_qualifiers() {
    let fixture = Fixture::new();
    let root = fixture.certificate("root", "root", true, vec![certificate_policies(true, &[(P, true)]), inhibit_any_policy(0)]);
    let leaf = fixture.certificate("server", "root", false, vec![certificate_policies(true, &[(ANY_POLICY, true)])]);
    assert!(fixture.check(&[&leaf, &root]).is_ok());
    let leaf = fixture.certificate("direct trust", "root", false, vec![certificate_policies(true, &[(P, true)])]);
    assert!(fixture.check(&[&leaf]).is_ok());
}

#[test]
fn missing_or_empty_issuer_policies_null_the_tree_before_a_qualified_leaf() {
    let fixture = Fixture::new(); let root = fixture.root();
    let leaf = fixture.certificate("server", "issuer", false, vec![certificate_policies(true, &[(P, true)])]);
    for extensions in [vec![], vec![certificate_policies(true, &[])]] {
        let issuer = fixture.certificate("issuer", "root", true, extensions);
        assert!(fixture.check(&[&leaf, &issuer, &root]).is_ok());
    }
}

#[test]
fn qualifier_rejection_precedes_current_certificate_policy_matching() {
    let fixture = Fixture::new(); let root = fixture.root();
    let upper = fixture.certificate("upper", "root", true, vec![certificate_policies(false, &[(P, false)])]);
    let middle = fixture.certificate("middle", "upper", true, vec![certificate_policies(false, &[(Q, false)])]);
    let leaf = fixture.certificate("server", "middle", false, vec![certificate_policies(true, &[(P, true)])]);
    assert!(fixture.check(&[&leaf, &middle, &upper, &root]).is_ok());
    let leaf = fixture.certificate("server", "upper", false, vec![certificate_policies(true, &[(Q, true)])]);
    assert!(fixture.check(&[&leaf, &upper, &root]).is_err());
}

#[test]
fn mappings_replace_original_expectations_and_accumulate_subject_policies() {
    let fixture = Fixture::new(); let root = fixture.root();
    let upper = fixture.certificate("upper", "root", true, vec![certificate_policies(false, &[(P, false)]), policy_mapping(&[(P, Q), (P, R)])]);
    let leaf = fixture.certificate("server", "middle", false, vec![certificate_policies(true, &[(Q, true)])]);
    for (oid, accepted) in [(P, true), (Q, false), (R, false)] {
        let middle = fixture.certificate("middle", "upper", true, vec![certificate_policies(false, &[(oid, false)])]);
        assert_eq!(fixture.check(&[&leaf, &middle, &upper, &root]).is_ok(), accepted);
    }
    let mut frontier = Frontier::from([(ANY_POLICY.into(), Expected::new(ANY_POLICY.into()))]);
    apply_mappings(&mut frontier, &[(P.into(), Q.into()), (P.into(), R.into())], 1).unwrap();
    assert_eq!(frontier[P].policies, BTreeSet::from([Q.into(), R.into()]));
}

#[test]
fn inhibited_mapping_deletes_its_policy_but_current_qualifiers_still_reject() {
    let fixture = Fixture::new(); let root = fixture.root();
    let upper = fixture.certificate("upper", "root", true, vec![certificate_policies(false, &[(P, false)]), inhibit_mapping(0)]);
    let leaf = fixture.certificate("server", "middle", false, vec![certificate_policies(true, &[(Q, true)])]);
    for qualified in [false, true] {
        let middle = fixture.certificate("middle", "upper", true, vec![certificate_policies(true, &[(P, qualified)]), policy_mapping(&[(P, Q)])]);
        assert_eq!(fixture.check(&[&leaf, &middle, &upper, &root]).is_ok(), !qualified);
    }
}

#[test]
fn inhibited_any_policy_survives_only_on_nonfinal_self_issued_certificates() {
    let fixture = Fixture::new(); let root = fixture.root();
    let upper = fixture.certificate("Straße", "root", true, vec![certificate_policies(false, &[(ANY_POLICY, false)]), inhibit_any_policy(0)]);
    for (subject, accepted) in [("middle", true), ("STRASSE", false)] {
        let middle = fixture.certificate(subject, "Straße", true, vec![certificate_policies(true, &[(ANY_POLICY, true)])]);
        let leaf = fixture.certificate("server", subject, false, vec![certificate_policies(true, &[(P, true)])]);
        assert_eq!(fixture.check(&[&leaf, &middle, &upper, &root]).is_ok(), accepted);
    }
    let leaf = fixture.certificate("STRASSE", "Straße", false, vec![certificate_policies(true, &[(ANY_POLICY, true)])]);
    assert!(fixture.check(&[&leaf, &upper, &root]).is_ok());
}

#[test]
fn self_issued_certificates_do_not_decrement_mapping_counter() {
    let fixture = Fixture::new(); let root = fixture.root();
    let upper = fixture.certificate("upper", "root", true, vec![certificate_policies(false, &[(P, false)]), inhibit_mapping(1)]);
    let leaf = fixture.certificate("server", "mapper", false, vec![certificate_policies(true, &[(Q, true)])]);
    for (subject, accepted) in [("bridge", true), ("UPPER", false)] {
        let bridge = fixture.certificate(subject, "upper", true, vec![certificate_policies(false, &[(P, false)])]);
        let mapper = fixture.certificate("mapper", subject, true, vec![certificate_policies(false, &[(P, false)]), policy_mapping(&[(P, Q)])]);
        assert_eq!(fixture.check(&[&leaf, &mapper, &bridge, &upper, &root]).is_ok(), accepted);
    }
}

#[test]
fn any_policy_qualifiers_do_not_exempt_ordinary_policies_in_the_same_extension() {
    let fixture = Fixture::new(); let root = fixture.root();
    for entries in [[(ANY_POLICY, true), (P, true)], [(P, true), (ANY_POLICY, true)]] {
        let leaf = fixture.certificate("server", "root", false, vec![certificate_policies(true, &entries)]);
        assert!(fixture.check(&[&leaf, &root]).is_err());
    }
}

#[test]
fn mappings_are_skipped_on_the_final_certificate_and_after_a_null_tree() {
    let fixture = Fixture::new(); let root = fixture.root();
    let leaf = fixture.certificate("server", "root", false, vec![certificate_policies(false, &[(P, false)]), policy_mapping(&[(P, ANY_POLICY)])]);
    assert!(fixture.check(&[&leaf, &root]).is_ok());
    let leaf = fixture.certificate("server", "issuer", false, vec![certificate_policies(true, &[(P, true)])]);
    let missing = fixture.certificate("missing", "root", true, vec![]);
    let issuer = fixture.certificate("issuer", "missing", true, vec![certificate_policies(false, &[(P, false)]), policy_mapping(&[(P, ANY_POLICY)])]);
    assert!(fixture.check(&[&leaf, &issuer, &missing, &root]).is_ok());
    let issuer = fixture.certificate("issuer", "root", true, vec![certificate_policies(false, &[(P, false)]), policy_mapping(&[(ANY_POLICY, P)])]);
    assert!(fixture.check(&[&leaf, &issuer, &root]).is_err());
}

#[test]
fn any_policy_expansion_preserves_all_mapped_expectations_and_final_inhibition() {
    let mut previous = Frontier::from([(P.into(), Expected::new(P.into()))]);
    apply_mappings(&mut previous, &[(P.into(), Q.into()), (P.into(), R.into())], 1).unwrap();
    let next = advance(&previous, &[Policy { oid: ANY_POLICY.into(), qualified: true }], true, true).unwrap();
    assert_eq!(next.keys().map(|oid| oid.as_ref()).collect::<Vec<_>>(), vec![Q, R]);
    assert!(advance(&previous, &[Policy { oid: ANY_POLICY.into(), qualified: true }], true, false).unwrap().is_empty());
}

#[test]
fn malformed_noncritical_policies_are_absent_and_critical_ones_fail() {
    let fixture = Fixture::new(); let root = fixture.root();
    let leaf = fixture.certificate("server", "issuer", false, vec![certificate_policies(true, &[(P, true)])]);
    let mut fields = der(6, P); fields.extend(der(0x30, &[]));
    for critical in [false, true] {
        let issuer = fixture.certificate("issuer", "root", true, vec![extension("2.5.29.32", critical, &der(0x30, &der(0x30, &fields)))]);
        assert_eq!(fixture.check(&[&leaf, &issuer, &root]).is_ok(), !critical);
    }
}

#[test]
fn policy_parser_accepts_java_ber_lengths_and_opaque_qualifier_values() {
    let fields = der(6, P);
    let mut indefinite = vec![0x30, 0x80, 0x30, 0x80]; indefinite.extend(fields); indefinite.extend([0, 0, 0, 0]);
    assert_eq!(policies(&indefinite, true).unwrap()[0].oid.as_ref(), P);
    let mut qualifier = der(6, &[0x2a, 3, 7]); qualifier.extend([0xff]);
    let mut fields = der(6, P); fields.extend(der(0x30, &der(0x30, &qualifier))); fields.extend([0xff]);
    assert!(policies(&der(0x30, &der(0x30, &fields)), true).unwrap()[0].qualified);
    assert_eq!(integer(&[0, 0, 1]).unwrap(), 1);
    assert_eq!(integer(&[0xff, 0xff, 0xff]).unwrap(), -1);
}

#[test]
fn ber_normalization_padding_makes_noncritical_policies_absent() {
    let fixture = Fixture::new(); let root = fixture.root();
    let leaf = fixture.certificate("server", "issuer", false, vec![certificate_policies(true, &[(P, true)])]);
    let bytes = [0x30, 0x80, 0x30, 0x81, 5, 6, 3, 0x2a, 3, 4, 0, 0];
    assert!(policies(&bytes, false).is_err());
    for critical in [false, true] {
        let issuer = fixture.certificate("issuer", "root", true, vec![extension("2.5.29.32", critical, &bytes)]);
        assert_eq!(fixture.check(&[&leaf, &issuer, &root]).is_ok(), !critical);
    }
}

#[test]
fn qualifier_info_accepts_absent_values_but_rejects_malformed_identifiers() {
    let fixture = Fixture::new(); let root = fixture.root();
    let leaf = fixture.certificate("server", "issuer", false, vec![certificate_policies(true, &[(P, true)])]);
    for (qualifier, accepted) in [(der(6, P), false), (der(6, &[]), true), (der(4, P), true), (vec![], true)] {
        let mut fields = der(6, P); fields.extend(der(0x30, &der(0x30, &qualifier)));
        let issuer = fixture.certificate("issuer", "root", true, vec![extension("2.5.29.32", false, &der(0x30, &der(0x30, &fields)))]);
        assert_eq!(fixture.check(&[&leaf, &issuer, &root]).is_ok(), accepted);
    }
}

#[test]
fn qualifier_normalization_preserves_opaque_padding_and_noncritical_live_tree() {
    let fixture = Fixture::new(); let root = fixture.root();
    let leaf = fixture.certificate("server", "issuer", false, vec![certificate_policies(true, &[(P, true)])]);
    let qualifier = [0x30, 0x80, 6, 0x81, 3, 0x2a, 3, 7, 0, 0];
    let mut fields = der(6, P); fields.extend(der(0x30, &qualifier));
    let bytes = der(0x30, &der(0x30, &fields));
    assert!(policies(&bytes, false).unwrap()[0].qualified);
    let issuer = fixture.certificate("issuer", "root", true, vec![extension("2.5.29.32", false, &bytes)]);
    assert!(fixture.check(&[&leaf, &issuer, &root]).is_err());
}

#[test]
fn policy_ber_normalization_has_no_depth_cutoff_and_keeps_definite_bodies_opaque() {
    let mut bytes = Vec::new();
    for _ in 0..1024 { bytes.extend([0x30, 0x80]); }
    bytes.extend([4, 1, b'x']);
    for _ in 0..1024 { bytes.extend([0, 0]); }
    let mut expected = vec![4, 1, b'x'];
    for _ in 0..1024 { expected = der(0x30, &expected); }
    assert_eq!(convert_indefinite(&bytes).unwrap(), (expected, bytes.len()));
    bytes.truncate(bytes.len() - 1);
    assert!(convert_indefinite(&bytes).is_err());
    let opaque = [0x30, 0x80, 0x30, 7, 0x30, 0x80, 4, 1, b'x', 0, 0, 0, 0];
    assert_eq!(convert_indefinite(&opaque).unwrap().0, [0x30, 9, 0x30, 7, 0x30, 0x80, 4, 1, b'x', 0, 0]);
}

#[test]
fn policy_state_reservation_failure_preserves_the_budget_error() {
    let fixture = Fixture::new(); let root = fixture.root();
    let leaf = fixture.certificate("server", "root", false, vec![certificate_policies(true, &[(P, true)])]);
    let memory = MemoryBudget::new(1024); let charge = memory.reserve(1024).unwrap();
    let error = verify(&[&leaf, &root], &charge).unwrap_err();
    assert_eq!(error.code, "rtdb/memory-budget-exceeded");
    assert_eq!(memory.used_bytes(), 1024);
    drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn final_explicit_policy_requires_a_live_tree_only_for_zero_skip_certs() {
    let fixture = Fixture::new(); let root = fixture.root();
    for (count, accepted) in [(0, false), (1, true), (0xff, true), (0xfe, true)] {
        let leaf = fixture.certificate("server", "root", false, vec![require_explicit(count)]);
        assert_eq!(fixture.check(&[&leaf, &root]).is_ok(), accepted);
    }
    for policy in [P, ANY_POLICY] {
        let leaf = fixture.certificate("server", "root", false, vec![require_explicit(0), certificate_policies(true, &[(policy, false)])]);
        assert!(fixture.check(&[&leaf, &root]).is_ok());
    }
}

#[test]
fn issuer_explicit_policy_counts_remaining_nonselfissued_certificates() {
    let fixture = Fixture::new(); let root = fixture.root();
    for (count, accepted) in [(0, false), (1, false), (2, true), (0xff, true), (0xfe, true)] {
        let issuer = fixture.certificate("issuer", "root", true, vec![require_explicit(count)]);
        let leaf = fixture.certificate("server", "issuer", false, vec![certificate_policies(true, &[(P, true)])]);
        assert_eq!(fixture.check(&[&leaf, &issuer, &root]).is_ok(), accepted);
    }
    let issuer = fixture.certificate("Straße", "root", true, vec![require_explicit(1)]);
    let leaf = fixture.certificate("STRASSE", "Straße", false, vec![]);
    assert!(fixture.check(&[&leaf, &issuer, &root]).is_ok());
    let leaf = fixture.certificate("STRASSE", "Straße", false, vec![require_explicit(0)]);
    assert!(fixture.check(&[&leaf, &issuer, &root]).is_err());
}

#[test]
fn explicit_policy_checks_precede_nonfinal_constraint_merging() {
    let fixture = Fixture::new(); let root = fixture.root();
    let upper = fixture.certificate("upper", "root", true, vec![require_explicit(0)]);
    let middle = fixture.certificate("middle", "upper", true, vec![require_explicit(0xfe)]);
    let leaf = fixture.certificate("server", "middle", false, vec![]);
    assert!(fixture.check(&[&leaf, &middle, &upper, &root]).is_err());
}

#[test]
fn explicit_policy_counter_skips_selfissued_intermediates() {
    let fixture = Fixture::new(); let root = fixture.root();
    let upper = fixture.certificate("upper", "root", true, vec![require_explicit(2)]);
    for (subject, accepted) in [("bridge", false), ("UPPER", true)] {
        let bridge = fixture.certificate(subject, "upper", true, vec![]);
        let leaf = fixture.certificate("server", subject, false, vec![]);
        assert_eq!(fixture.check(&[&leaf, &bridge, &upper, &root]).is_ok(), accepted);
    }
}

#[test]
fn explicit_policy_survives_matching_and_mapped_policies_but_rejects_null_trees() {
    let fixture = Fixture::new(); let root = fixture.root();
    for mapped in [false, true] {
        let mut extensions = vec![require_explicit(0), certificate_policies(false, &[(P, false)])];
        if mapped { extensions.push(policy_mapping(&[(P, Q)])); }
        let issuer = fixture.certificate("issuer", "root", true, extensions);
        for (oid, accepted) in [(P, !mapped), (Q, mapped), (R, false), (ANY_POLICY, true)] {
            let leaf = fixture.certificate("server", "issuer", false, vec![certificate_policies(true, &[(oid, false)])]);
            assert_eq!(fixture.check(&[&leaf, &issuer, &root]).is_ok(), accepted);
        }
    }
}

#[test]
fn explicit_policy_rejects_inhibited_any_policy_on_a_selfissued_final_certificate() {
    let fixture = Fixture::new(); let root = fixture.root();
    let issuer = fixture.certificate("issuer", "root", true, vec![require_explicit(0), inhibit_any_policy(0), certificate_policies(false, &[(P, false)])]);
    let leaf = fixture.certificate("ISSUER", "issuer", false, vec![certificate_policies(true, &[(ANY_POLICY, true)])]);
    assert!(fixture.check(&[&leaf, &issuer, &root]).is_err());
    let bridge = fixture.certificate("ISSUER", "issuer", true, vec![certificate_policies(true, &[(ANY_POLICY, true)])]);
    let leaf = fixture.certificate("server", "ISSUER", false, vec![certificate_policies(true, &[(P, false)])]);
    assert!(fixture.check(&[&leaf, &bridge, &issuer, &root]).is_ok());
}

#[test]
fn malformed_critical_policy_extensions_fail_even_after_tree_null_and_on_final_leaf() {
    let fixture = Fixture::new(); let root = fixture.root();
    let issuer = fixture.certificate("issuer", "root", true, vec![]);
    for oid in ["2.5.29.32", "2.5.29.33", "2.5.29.36", "2.5.29.54"] {
        for critical in [false, true] {
            let leaf = fixture.certificate("server", "issuer", false, vec![extension(oid, critical, &[5, 0])]);
            assert_eq!(fixture.check(&[&leaf, &issuer, &root]).is_ok(), !critical);
        }
    }
}

#[test]
fn malformed_noncritical_constraints_are_absent_as_a_whole() {
    let fixture = Fixture::new(); let root = fixture.root();
    let mut fields = der(0x80, &[0]); fields.extend(der(0x82, &[0]));
    for critical in [false, true] {
        let leaf = fixture.certificate("server", "root", false, vec![extension("2.5.29.36", critical, &der(0x30, &fields))]);
        assert_eq!(fixture.check(&[&leaf, &root]).is_ok(), !critical);
    }
    let issuer = fixture.certificate("issuer", "root", true, vec![certificate_policies(false, &[(P, false)]), extension("2.5.29.54", false, &der(2, &[0]))]);
    let leaf = fixture.certificate("server", "issuer", false, vec![require_explicit(0), certificate_policies(false, &[(ANY_POLICY, false)])]);
    assert!(fixture.check(&[&leaf, &issuer, &root]).is_ok());
}

#[test]
fn unparseable_noncritical_duplicates_preserve_a_single_successful_policy_extension() {
    let fixture = Fixture::new(); let root = fixture.root();
    let leaf = fixture.certificate("server", "issuer", false, vec![require_explicit(0), certificate_policies(false, &[(P, false)])]);
    for invalid_first in [false, true] {
        let mut extensions = vec![certificate_policies(false, &[(P, false)]), extension("2.5.29.32", false, &[5, 0])];
        if invalid_first { extensions.reverse(); }
        let issuer = fixture.certificate("issuer", "root", true, extensions);
        assert!(fixture.check(&[&leaf, &issuer, &root]).is_ok());
    }
    let issuer = fixture.certificate("issuer", "root", true, vec![certificate_policies(false, &[(P, false)]), certificate_policies(false, &[(P, false)])]);
    assert!(fixture.check(&[&leaf, &issuer, &root]).is_err());
    let issuer = fixture.certificate("issuer", "root", true, vec![extension("2.5.29.32", false, &[5, 0]), extension("2.5.29.32", false, &[5, 0])]);
    let leaf = fixture.certificate("server", "issuer", false, vec![certificate_policies(true, &[(P, true)])]);
    assert!(fixture.check(&[&leaf, &issuer, &root]).is_ok());
}
