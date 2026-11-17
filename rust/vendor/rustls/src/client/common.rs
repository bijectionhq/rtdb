use alloc::boxed::Box;
use alloc::vec::Vec;

use super::{ClientCertificateRequest, ResolvesClientCert};
use crate::log::{debug, trace};
use crate::msgs::enums::ExtensionType;
use crate::msgs::handshake::{CertificateChain, DistinguishedName, ProtocolName, ServerExtensions};
use crate::sync::Arc;
use crate::{CipherSuite, compress, sign};

#[derive(Debug)]
pub(super) struct ServerCertDetails<'a> {
    pub(super) cert_chain: CertificateChain<'a>,
    pub(super) ocsp_response: Vec<u8>,
}

impl<'a> ServerCertDetails<'a> {
    pub(super) fn new(cert_chain: CertificateChain<'a>, ocsp_response: Vec<u8>) -> Self {
        Self {
            cert_chain,
            ocsp_response,
        }
    }

    pub(super) fn into_owned(self) -> ServerCertDetails<'static> {
        let Self {
            cert_chain,
            ocsp_response,
        } = self;
        ServerCertDetails {
            cert_chain: cert_chain.into_owned(),
            ocsp_response,
        }
    }
}

pub(super) struct ClientHelloDetails {
    pub(super) alpn_protocols: Vec<ProtocolName>,
    pub(super) sent_extensions: Vec<ExtensionType>,
    pub(super) extension_order_seed: u16,
    pub(super) offered_cert_compression: bool,
    pub(super) offered_cipher_suites: Vec<CipherSuite>,
}

impl ClientHelloDetails {
    pub(super) fn new(alpn_protocols: Vec<ProtocolName>, extension_order_seed: u16) -> Self {
        Self {
            alpn_protocols,
            sent_extensions: Vec::new(),
            extension_order_seed,
            offered_cert_compression: false,
            offered_cipher_suites: Vec::new(),
        }
    }

    pub(super) fn server_sent_unsolicited_extensions(
        &self,
        received_exts: &ServerExtensions<'_>,
        allowed_unsolicited: &[ExtensionType],
    ) -> bool {
        let mut extensions = received_exts.collect_used();
        extensions.extend(
            received_exts
                .unknown_extensions
                .iter()
                .map(|ext| ExtensionType::from(*ext)),
        );
        for ext_type in extensions {
            if !self.sent_extensions.contains(&ext_type) && !allowed_unsolicited.contains(&ext_type)
            {
                trace!("Unsolicited extension {ext_type:?}");
                return true;
            }
        }

        false
    }
}

pub(super) enum ClientAuthDetails {
    /// Send an empty `Certificate` and no `CertificateVerify`.
    Empty { auth_context_tls13: Option<Vec<u8>> },
    /// Send a non-empty `Certificate` and a `CertificateVerify`.
    Verify {
        certkey: Arc<sign::CertifiedKey>,
        signer: Box<dyn sign::Signer>,
        auth_context_tls13: Option<Vec<u8>>,
        compressor: Option<&'static dyn compress::CertCompressor>,
    },
}

impl ClientAuthDetails {
    pub(super) fn resolve(
        resolver: &dyn ResolvesClientCert,
        canames: Option<&[DistinguishedName]>,
        request: &ClientCertificateRequest<'_>,
        auth_context_tls13: Option<Vec<u8>>,
        compressor: Option<&'static dyn compress::CertCompressor>,
    ) -> Self {
        let acceptable_issuers = canames
            .unwrap_or_default()
            .iter()
            .map(|p| p.as_ref())
            .collect::<Vec<&[u8]>>();

        if let Some(certkey) = resolver.resolve_with_request(&acceptable_issuers, request) {
            if let Some(signer) = certkey.key.choose_scheme(request.signature_schemes) {
                debug!("Attempting client auth");
                return Self::Verify {
                    certkey,
                    signer,
                    auth_context_tls13,
                    compressor,
                };
            }
        }

        debug!("Client auth requested but no cert/sigscheme available");
        Self::Empty { auth_context_tls13 }
    }
}

#[cfg(test)]
mod request_tests {
    use super::*;
    use crate::SignatureScheme;

    #[derive(Debug)]
    struct LegacyResolver;

    impl ResolvesClientCert for LegacyResolver {
        fn resolve(&self, issuers: &[&[u8]], schemes: &[SignatureScheme]) -> Option<Arc<sign::CertifiedKey>> {
            assert_eq!(issuers, &[&[0x30, 0][..]]);
            assert_eq!(schemes, &[SignatureScheme::RSA_PSS_SHA256]);
            None
        }

        fn has_certs(&self) -> bool { true }
    }

    #[derive(Debug)]
    struct RequestResolver;

    impl ResolvesClientCert for RequestResolver {
        fn resolve(&self, _: &[&[u8]], _: &[SignatureScheme]) -> Option<Arc<sign::CertifiedKey>> {
            panic!("complete request must reach the resolver")
        }

        fn resolve_with_request(&self, issuers: &[&[u8]], request: &ClientCertificateRequest<'_>) -> Option<Arc<sign::CertifiedKey>> {
            assert_eq!(issuers, &[&[0x30, 0][..]]);
            assert_eq!(request.signature_schemes, &[SignatureScheme::RSA_PSS_SHA256]);
            assert_eq!(request.certificate_signature_schemes, &[SignatureScheme::RSA_PKCS1_SHA256, SignatureScheme::RSA_PSS_SHA256]);
            assert_eq!(request.certificate_types, Some(&[1, 64][..]));
            None
        }

        fn has_certs(&self) -> bool { true }
    }

    #[test]
    fn complete_request_preserves_legacy_resolvers_and_exposes_certificate_preferences() {
        let request = ClientCertificateRequest {
            signature_schemes: &[SignatureScheme::RSA_PSS_SHA256],
            certificate_signature_schemes: &[SignatureScheme::RSA_PKCS1_SHA256, SignatureScheme::RSA_PSS_SHA256],
            certificate_types: Some(&[1, 64]),
        };
        let issuers = [DistinguishedName::from(alloc::vec![0x30, 0])];
        for resolver in [&LegacyResolver as &dyn ResolvesClientCert, &RequestResolver] {
            assert!(matches!(ClientAuthDetails::resolve(resolver, Some(&issuers), &request, None, None), ClientAuthDetails::Empty { auth_context_tls13: None }));
        }
    }
}
