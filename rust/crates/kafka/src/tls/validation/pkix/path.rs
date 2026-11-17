use super::*;
use openssl::{nid::Nid, x509::{X509NameRef, store::X509StoreBuilder}};
use rtdb_tls::CertificateIdentifiers;

#[derive(Clone, Copy)]
enum PathMode { Ordered, Build }

struct Search<'a> {
    peer: Vec<&'a X509Ref>,
    anchors: Stack<X509>,
    source: &'a X509StoreContextRef,
    now: &'a Asn1TimeRef,
    charge: &'a Arc<MemoryReservation>,
    work: Vec<Arc<MemoryReservation>>,
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
    let mut search = Search { peer, anchors, source: context, now, charge, work: Vec::new() };
    search.verify()
}

impl Search<'_> {
    // OpenJDK 25.0.1 PKIXValidator.engineValidate chooses ordered validation before its path builder.
    fn verify(&mut self) -> Result<Vec<X509>> {
        let leaf = self.peer[0].to_der().map_err(invalid)?;
        for anchor in &self.anchors {
            if anchor.to_der().map_err(invalid)? == leaf { return Ok(vec![self.peer[0].to_owned()]); }
        }
        for index in 1..self.peer.len() {
            if !self.same_name(self.peer[index].subject_name(), self.peer[index - 1].issuer_name())? { return self.build(); }
            let mut trusted = false;
            for anchor in &self.anchors {
                if self.same_name(anchor.subject_name(), self.peer[index].subject_name())? && super::super::super::public_key::same(anchor, self.peer[index])? {
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

    fn ordered(&mut self, length: usize) -> Result<Vec<X509>> {
        let path = (0..length).collect::<Vec<_>>();
        self.finish(&path, PathMode::Ordered)?.ok_or_else(|| invalid("TLS PKIX ordered path validation failed"))
    }

    fn build(&mut self) -> Result<Vec<X509>> {
        let mut path = vec![0];
        let mut frames = vec![(1usize, 0usize)];
        while !frames.is_empty() {
            let (start, intermediates) = *frames.last().expect("nonempty search");
            if start == 1 {
                self.work.push(self.charge.budget().reserve(256)?);
                if let Some(chain) = self.finish(&path, PathMode::Build)? { return Ok(chain); }
            }
            let last = self.peer[*path.last().expect("nonempty path")];
            let mut next = None;
            let end = if intermediates < 5 { self.peer.len() } else { start };
            for index in start..end {
                let candidate = self.peer[index];
                if candidate.version() < 2 { continue; }
                if !self.same_name(last.issuer_name(), candidate.subject_name())? { continue; }
                let encoded = candidate.to_der().map_err(invalid)?;
                let mut repeated = false;
                for previous in path.iter().copied() {
                    if self.peer[previous].to_der().map_err(invalid)? == encoded { repeated = true; break; }
                }
                if repeated { continue; }
                let count = intermediates + usize::from(!self.same_name(candidate.subject_name(), candidate.issuer_name())?);
                if count <= 5 { next = Some((index, count)); break; }
            }
            if let Some((index, count)) = next {
                frames.last_mut().expect("nonempty search").0 = index + 1;
                path.push(index);
                frames.push((1, count));
            } else { frames.pop(); path.pop(); }
        }
        Err(invalid("TLS PKIX path building found no permitted path"))
    }

    fn finish(&mut self, path: &[usize], mode: PathMode) -> Result<Option<Vec<X509>>> {
        let last = self.peer[*path.last().expect("nonempty path")];
        for index in 0..self.anchors.len() {
            let anchor: &X509Ref = &self.anchors[index];
            if !self.same_name(last.issuer_name(), anchor.subject_name())? { continue; }
            match mode {
                PathMode::Ordered if !ordered_anchor(last, anchor, self.charge)? => continue,
                PathMode::Build if anchor.version() < 2 && (anchor.not_before().compare(last.not_before()).map_err(invalid)?.is_gt()
                    || anchor.not_after().compare(last.not_after()).map_err(invalid)?.is_lt()) => continue,
                _ => {},
            }
            self.work.push(self.charge.budget().reserve(256)?);
            let chain = path.iter().map(|index| self.peer[*index]).chain(std::iter::once(anchor)).collect::<Vec<_>>();
            match self.validate(&chain) {
                Ok(()) => return Ok(Some(chain.iter().map(|certificate| (*certificate).to_owned()).collect())),
                Err(error) if error.code == "xtdb.kafka/invalid-tls-config" => {},
                Err(error) => return Err(error),
            }
        }
        Ok(None)
    }

    fn same_name(&self, left: &X509NameRef, right: &X509NameRef) -> Result<bool> {
        super::super::super::names::same(left, right, self.charge.budget())
    }

    fn validate(&self, chain: &[&X509Ref]) -> Result<()> {
        for pair in chain.windows(2) {
            if pair[0].issuer_name().entries().next().is_none() || !self.same_name(pair[0].issuer_name(), pair[1].subject_name())? {
                return Err(invalid("TLS selected path has a mismatched issuer name"));
            }
        }
        for certificate in chain { identifiers(certificate, self.charge)?; }
        super::super::super::security::verify_pkix_server_chain(chain)?;
        super::policy::verify(chain, self.charge)?;
        for certificate in &chain[..chain.len() - 1] {
            super::super::critical_extensions(certificate, &[Nid::BASIC_CONSTRAINTS, Nid::NAME_CONSTRAINTS, Nid::CERTIFICATE_POLICIES,
                Nid::POLICY_MAPPINGS, Nid::POLICY_CONSTRAINTS, Nid::INHIBIT_ANY_POLICY, Nid::SUBJECT_ALT_NAME, Nid::KEY_USAGE, Nid::EXT_KEY_USAGE])?;
            if certificate.not_before().compare(self.now).map_err(invalid)?.is_gt() || certificate.not_after().compare(self.now).map_err(invalid)?.is_lt() {
                return Err(invalid("TLS certificate is outside its validity interval"));
            }
        }
        super::super::server_usage(chain[0])?;
        let _scratch = reserve(chain.iter().copied(), self.charge)?;
        let metadata_bound = chain[..chain.len() - 1].iter().try_fold(0usize, |bound, certificate| {
            let length = unsafe { openssl_sys::i2d_X509(certificate.as_ptr(), ptr::null_mut()) };
            usize::try_from(length).ok().and_then(|length| length.checked_mul(64)).and_then(|length| length.checked_add(65_536))
                .and_then(|length| bound.checked_add(length)).ok_or_else(|| invalid("TLS selected-chain metadata exceeds allocation range"))
        })?;
        let _metadata = self.charge.budget().reserve(metadata_bound)?;
        let store = X509StoreBuilder::new().map_err(invalid)?.build();
        let untrusted = Stack::new().map_err(invalid)?;
        let mut originals = Stack::new().map_err(invalid)?;
        let mut self_issued = Vec::with_capacity(chain.len());
        for certificate in chain {
            originals.push((*certificate).to_owned()).map_err(invalid)?;
            self_issued.push(u8::from(self.same_name(certificate.subject_name(), certificate.issuer_name())?));
        }
        let mut context = X509StoreContext::new().map_err(invalid)?;
        context.init(&store, chain[0], &untrusted, |context| {
            let result = (|| -> Result<()> {
                configure_parameters(context, self.now)?;
                let ssl_index = unsafe { openssl_sys::SSL_get_ex_data_X509_STORE_CTX_idx() };
                if ssl_index < 0 { return Err(invalid(openssl::error::ErrorStack::get())); }
                let ssl = unsafe { openssl_sys::X509_STORE_CTX_get_ex_data(self.source.as_ptr(), ssl_index) };
                if !ssl.is_null() && unsafe { X509_STORE_CTX_set_ex_data(context.as_ptr(), ssl_index, ssl) } != 1 { return Err(invalid(openssl::error::ErrorStack::get())); }
                unsafe { X509_STORE_CTX_set_verify_cb(context.as_ptr(), X509_STORE_CTX_get_verify_cb(self.source.as_ptr())); }
                if !verify_with_dates(context, self.now, &originals, &self_issued)? { return Err(invalid(context.error())); }
                let selected = context.chain().ok_or_else(|| invalid("TLS PKIX validation produced no chain"))?;
                if selected.len() != chain.len() || selected.iter().zip(chain.iter()).any(|(selected, expected)| selected.as_ptr() != expected.as_ptr()) {
                    return Err(invalid("TLS PKIX validation changed the selected certificate path"));
                }
                Ok(())
            })();
            Ok(result)
        }).map_err(invalid)?
    }
}

fn identifiers(certificate: &X509Ref, charge: &MemoryReservation) -> Result<CertificateIdentifiers> {
    let length = unsafe { openssl_sys::i2d_X509(certificate.as_ptr(), ptr::null_mut()) };
    let bound = usize::try_from(length).ok().and_then(|length| length.checked_add(64)).ok_or_else(|| invalid("TLS certificate encoding exceeds allocation range"))?;
    let _scratch = charge.budget().reserve(bound)?;
    CertificateIdentifiers::decode(&certificate.to_der().map_err(invalid)?, charge.budget())
        .map_err(|error| if error.code == "rtdb.tls/invalid-keystore" { invalid(error) } else { error })
}

fn ordered_anchor(child: &X509Ref, anchor: &X509Ref, charge: &MemoryReservation) -> Result<bool> {
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
