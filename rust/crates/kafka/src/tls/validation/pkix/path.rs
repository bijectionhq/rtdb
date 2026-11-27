use super::*;
use crate::tls::{TlsConfigError, TlsConfigResult};
use openssl::{nid::Nid, x509::{X509NameRef, store::X509StoreBuilder}};
use rtdb_tls::CertificateIdentifiers;

mod build;

struct Search<'a> {
    peer: Vec<&'a X509Ref>,
    anchors: Stack<X509>,
    source: &'a X509StoreContextRef,
    now: &'a Asn1TimeRef,
    charge: &'a Arc<MemoryReservation>,
}

pub(super) fn verify(context: &X509StoreContextRef, now: &Asn1TimeRef, charge: &Arc<MemoryReservation>) -> Result<Vec<X509>> {
    let leaf = unsafe { X509_STORE_CTX_get0_cert(context.as_ptr()) };
    if leaf.is_null() { return Err(invalid("TLS peer supplied no certificate")); }
    let leaf = unsafe { X509Ref::from_ptr(leaf) };
    let mut peer = vec![leaf];
    let untrusted = unsafe { X509_STORE_CTX_get0_untrusted(context.as_ptr()) };
    if !untrusted.is_null() {
        for (index, certificate) in unsafe { StackRef::<X509>::from_ptr(untrusted) }.iter().enumerate() {
            if index != 0 || certificate.as_ptr() != leaf.as_ptr() { peer.push(certificate); }
        }
    }
    for certificate in &peer {
        for name in [certificate.subject_name(), certificate.issuer_name()] {
            let found = unsafe { X509_STORE_CTX_get1_certs(context.as_ptr(), name.as_ptr()) };
            if !found.is_null() { drop(unsafe { Stack::<X509>::from_ptr(found) }); }
        }
    }
    let store = unsafe { X509_STORE_CTX_get0_store(context.as_ptr()) };
    if store.is_null() { return Err(invalid("TLS PKIX trust store is absent")); }
    let anchors = unsafe { X509_STORE_get1_all_certs(store) };
    if anchors.is_null() { return Err(invalid(openssl::error::ErrorStack::get())); }
    let anchors = unsafe { Stack::<X509>::from_ptr(anchors) };
    let mut search = Search { peer, anchors, source: context, now, charge };
    search.verify()
}

impl Search<'_> {
    fn verify(&mut self) -> Result<Vec<X509>> {
        let selected = self.select()?;
        if selected.len() > 1 {
            super::super::super::security::verify_pkix_anchor(selected.last().expect("selected anchor"), self.charge.budget())?;
            super::super::pkix_server_usage(&selected[0], self.charge.budget())?;
            let references = selected.iter().map(X509::as_ref).collect::<Vec<_>>();
            super::super::super::security::verify_server_distrust(&references)?;
        }
        Ok(selected)
    }

    // OpenJDK 25.0.1 PKIXValidator.engineValidate chooses ordered validation before its path builder.
    fn select(&mut self) -> Result<Vec<X509>> {
        let leaf = self.peer[0].to_der().map_err(invalid)?;
        for anchor in &self.anchors {
            if anchor.to_der().map_err(invalid)? == leaf { return Ok(vec![self.peer[0].to_owned()]); }
        }
        for index in 1..self.peer.len() {
            if !self.same_name(self.peer[index].subject_name(), self.peer[index - 1].issuer_name())? { return self.build(); }
            let mut trusted = false;
            for anchor in &self.anchors {
                if self.same_name(anchor.subject_name(), self.peer[index].subject_name())? && super::super::super::public_key::same(anchor, self.peer[index], self.charge.budget())? {
                    trusted = true; break;
                }
            }
            if trusted { return self.ordered(index); }
        }
        let issuer = self.peer.last().expect("nonempty peer chain").issuer_name();
        for anchor in &self.anchors {
            if self.same_name(anchor.subject_name(), issuer)? { return self.ordered(self.peer.len()); }
        }
        self.build()
    }

    fn ordered(&mut self, length: usize) -> Result<Vec<X509>> { build::ordered(self, length) }
    fn build(&mut self) -> Result<Vec<X509>> { build::verify(self) }

    fn same_name(&self, left: &X509NameRef, right: &X509NameRef) -> TlsConfigResult<bool> {
        super::super::super::names::same_typed(left, right, self.charge.budget())
    }

    fn validate(&self, chain: &[&X509Ref]) -> TlsConfigResult<()> {
        for pair in chain.windows(2) {
            if pair[0].issuer_name().entries().next().is_none() || !self.same_name(pair[0].issuer_name(), pair[1].subject_name())? {
                return Err(TlsConfigError::invalid("TLS selected path has a mismatched issuer name"));
            }
        }
        for certificate in chain { identifiers(certificate, self.charge)?; }
        let metadata = super::super::super::security::PathMetadata::decode_typed(chain, self.charge.budget())?;
        metadata.verify_ssl_algorithms().map_err(TlsConfigError::from_invalid_anomaly)?;
        super::policy::verify_typed(chain, self.charge)?;
        for certificate in &chain[..chain.len() - 1] {
            super::super::critical_extensions(certificate, &[Nid::BASIC_CONSTRAINTS, Nid::NAME_CONSTRAINTS, Nid::CERTIFICATE_POLICIES,
                Nid::POLICY_MAPPINGS, Nid::POLICY_CONSTRAINTS, Nid::INHIBIT_ANY_POLICY, Nid::SUBJECT_ALT_NAME, Nid::KEY_USAGE, Nid::EXT_KEY_USAGE])
                .map_err(TlsConfigError::from_invalid_anomaly)?;
            if certificate.not_before().compare(self.now).map_err(TlsConfigError::invalid)?.is_gt() || certificate.not_after().compare(self.now).map_err(TlsConfigError::invalid)?.is_lt() {
                return Err(TlsConfigError::invalid("TLS certificate is outside its validity interval"));
            }
        }
        let _scratch = reserve_typed(chain.iter().copied(), self.charge)?;
        let store = X509StoreBuilder::new().map_err(TlsConfigError::invalid)?.build();
        let untrusted = Stack::new().map_err(TlsConfigError::invalid)?;
        let mut originals = Stack::new().map_err(TlsConfigError::invalid)?;
        for certificate in chain {
            originals.push((*certificate).to_owned()).map_err(TlsConfigError::invalid)?;
        }
        let mut context = X509StoreContext::new().map_err(TlsConfigError::invalid)?;
        context.init(&store, chain[0], &untrusted, |context| {
            let result = (|| -> TlsConfigResult<()> {
                configure_parameters(context, self.now).map_err(TlsConfigError::from_invalid_anomaly)?;
                let ssl_index = unsafe { openssl_sys::SSL_get_ex_data_X509_STORE_CTX_idx() };
                if ssl_index < 0 { return Err(TlsConfigError::invalid(openssl::error::ErrorStack::get())); }
                let ssl = unsafe { openssl_sys::X509_STORE_CTX_get_ex_data(self.source.as_ptr(), ssl_index) };
                if !ssl.is_null() && unsafe { X509_STORE_CTX_set_ex_data(context.as_ptr(), ssl_index, ssl) } != 1 { return Err(TlsConfigError::invalid(openssl::error::ErrorStack::get())); }
                unsafe { X509_STORE_CTX_set_verify_cb(context.as_ptr(), X509_STORE_CTX_get_verify_cb(self.source.as_ptr())); }
                if !verify_java_with_dates(context, self.now, &originals, &metadata).map_err(TlsConfigError::from_invalid_anomaly)? { return Err(TlsConfigError::invalid(context.error())); }
                let selected = context.chain().ok_or_else(|| TlsConfigError::invalid("TLS PKIX validation produced no chain"))?;
                if selected.len() != chain.len() || selected.iter().zip(chain.iter()).any(|(selected, expected)| selected.as_ptr() != expected.as_ptr()) {
                    return Err(TlsConfigError::invalid("TLS PKIX validation changed the selected certificate path"));
                }
                Ok(())
            })();
            Ok(result)
        }).map_err(TlsConfigError::invalid)?
    }
}

fn identifiers(certificate: &X509Ref, charge: &MemoryReservation) -> TlsConfigResult<CertificateIdentifiers> {
    let length = unsafe { openssl_sys::i2d_X509(certificate.as_ptr(), ptr::null_mut()) };
    let bound = usize::try_from(length).ok().and_then(|length| length.checked_add(64)).ok_or_else(|| TlsConfigError::invalid("TLS certificate encoding exceeds allocation range"))?;
    let _scratch = charge.budget().reserve(bound).map_err(TlsConfigError::Other)?;
    CertificateIdentifiers::decode(&certificate.to_der().map_err(TlsConfigError::invalid)?, charge.budget())
        .map_err(TlsConfigError::from_tls)
}

fn ordered_anchor(child: &X509Ref, anchor: &X509Ref, charge: &MemoryReservation) -> TlsConfigResult<bool> {
    let child = identifiers(child, charge)?;
    if child.authority_key_identifier().is_none() && child.authority_serial_number().is_none() { return Ok(true); }
    let candidate = identifiers(anchor, charge)?;
    if let Some(key) = child.authority_key_identifier() {
        if candidate.subject_key_identifier().is_some_and(|subject| subject != key) { return Ok(false); }
    }
    Ok(anchor.version() < 2 || child.authority_serial_number().is_none_or(|serial| serial == candidate.serial_number()))
}

unsafe extern "C" {
    fn X509_STORE_CTX_get0_store(context: *const openssl_sys::X509_STORE_CTX) -> *mut openssl_sys::X509_STORE;
    fn X509_STORE_get1_all_certs(store: *mut openssl_sys::X509_STORE) -> *mut openssl_sys::stack_st_X509;
}
