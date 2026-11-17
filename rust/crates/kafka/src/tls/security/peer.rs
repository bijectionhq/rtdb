use openssl::{nid::Nid, x509::X509Ref};

pub(crate) fn permits_client_certificate(certificate: &X509Ref, schemes: impl IntoIterator<Item = u16>, tls13: bool) -> bool {
    let algorithm = certificate.signature_algorithm().object().nid();
    let algorithm = if algorithm == Nid::DSAWITHSHA1_2 { Nid::DSAWITHSHA1 } else { algorithm };
    schemes.into_iter().filter_map(|scheme| signature(scheme, tls13)).any(|supported| algorithm == supported)
}

// OpenJDK 25.0.1 SSLAlgorithmConstraints.SupportedSignatureAlgorithmConstraints compares JCA algorithm names, including one RSASSA-PSS name for every PSS scheme.
fn signature(scheme: u16, tls13: bool) -> Option<Nid> {
    if tls13 && matches!(scheme, 0x0301..=0x0303 | 0x0202 | 0x0402) { return None; }
    Some(match scheme {
        0x0201 => Nid::SHA1WITHRSAENCRYPTION, 0x0301 => Nid::SHA224WITHRSAENCRYPTION,
        0x0401 => Nid::SHA256WITHRSAENCRYPTION, 0x0501 => Nid::SHA384WITHRSAENCRYPTION, 0x0601 => Nid::SHA512WITHRSAENCRYPTION,
        0x0202 => Nid::DSAWITHSHA1, 0x0302 => Nid::DSA_WITH_SHA224, 0x0402 => Nid::DSA_WITH_SHA256,
        0x0203 => Nid::ECDSA_WITH_SHA1, 0x0303 => Nid::ECDSA_WITH_SHA224,
        0x0403 => Nid::ECDSA_WITH_SHA256, 0x0503 => Nid::ECDSA_WITH_SHA384, 0x0603 => Nid::ECDSA_WITH_SHA512,
        0x0804..=0x0806 | 0x0809..=0x080b => Nid::RSASSAPSS,
        0x0807 => Nid::from_raw(openssl_sys::NID_ED25519), 0x0808 => Nid::from_raw(openssl_sys::NID_ED448),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use openssl::hash::MessageDigest;

    #[test]
    fn peer_certificate_signatures_retain_java_pss_names_and_protocol_scopes() {
        let certificate = super::super::tests::certificate(1024, MessageDigest::sha256(), 1_700_000_000, None);
        assert!(permits_client_certificate(&certificate, [0x0401], true));
        assert!(!permits_client_certificate(&certificate, [0x0804], true));
        assert!(!permits_client_certificate(&certificate, [], false));
        for scheme in [0x0804, 0x0805, 0x0806, 0x0809, 0x080a, 0x080b] { assert_eq!(signature(scheme, true), Some(Nid::RSASSAPSS)); }
        for scheme in [0x0301, 0x0302, 0x0303, 0x0202, 0x0402] {
            assert!(signature(scheme, false).is_some());
            assert!(signature(scheme, true).is_none());
        }
        assert_eq!(signature(0x0201, true), Some(Nid::SHA1WITHRSAENCRYPTION));
        assert_eq!(signature(0x0101, false), None);
    }
}
