use super::*;
use openssl::{nid::Nid, x509::{X509Extension, extension::{BasicConstraints, KeyUsage}, store::{X509Store, X509StoreBuilder}}};

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
            if !super::super::same_name(self.peer[index].subject_name(), self.peer[index - 1].issuer_name()) { return self.build(); }
            let mut trusted = false;
            for anchor in &self.anchors {
                if super::super::same_name(anchor.subject_name(), self.peer[index].subject_name()) && super::super::super::public_key::same(anchor, self.peer[index])? {
                    trusted = true; break;
                }
            }
            if trusted { return self.ordered(index); }
        }
        let issuer = self.peer.last().expect("nonempty peer chain").issuer_name();
        if self.anchors.iter().any(|anchor| super::super::same_name(anchor.subject_name(), issuer)) { self.ordered(self.peer.len()) } else { self.build() }
    }

    fn ordered(&mut self, length: usize) -> Result<Vec<X509>> {
        let path = (0..length).collect::<Vec<_>>();
        self.finish(&path)?.ok_or_else(|| invalid("TLS PKIX ordered path validation failed"))
    }

    fn build(&mut self) -> Result<Vec<X509>> {
        let mut path = vec![0];
        let mut frames = vec![(1usize, 0usize)];
        while !frames.is_empty() {
            let (start, intermediates) = *frames.last().expect("nonempty search");
            if start == 1 {
                self.work.push(self.charge.budget().reserve(256)?);
                if let Some(chain) = self.finish(&path)? { return Ok(chain); }
            }
            let last = self.peer[*path.last().expect("nonempty path")];
            let mut next = None;
            let end = if intermediates < 5 { self.peer.len() } else { start };
            for index in start..end {
                let candidate = self.peer[index];
                if !super::super::same_name(last.issuer_name(), candidate.subject_name()) { continue; }
                let encoded = candidate.to_der().map_err(invalid)?;
                let mut repeated = false;
                for previous in path.iter().copied() {
                    if self.peer[previous].to_der().map_err(invalid)? == encoded { repeated = true; break; }
                }
                if repeated { continue; }
                let count = intermediates + usize::from(!super::super::same_name(candidate.subject_name(), candidate.issuer_name()));
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

    fn finish(&mut self, path: &[usize]) -> Result<Option<Vec<X509>>> {
        let last = self.peer[*path.last().expect("nonempty path")];
        for index in 0..self.anchors.len() {
            let anchor: &X509Ref = &self.anchors[index];
            if !super::super::same_name(last.issuer_name(), anchor.subject_name()) { continue; }
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

    fn validate(&self, chain: &[&X509Ref]) -> Result<()> {
        super::super::super::security::verify_pkix_server_chain(chain)?;
        super::policy::verify(chain, self.charge)?;
        for certificate in &chain[..chain.len() - 1] {
            if certificate.not_before().compare(self.now).map_err(invalid)?.is_gt() || certificate.not_after().compare(self.now).map_err(invalid)?.is_lt() {
                return Err(invalid("TLS certificate is outside its validity interval"));
            }
        }
        super::super::server_usage(chain[0])?;
        let _scratch = reserve(chain.iter().copied(), self.charge)?;
        let anchor = surrogate(chain[chain.len() - 1])?;
        let store = single_anchor(&anchor)?;
        let mut untrusted = Stack::new().map_err(invalid)?;
        for certificate in &chain[1..chain.len() - 1] { untrusted.push((*certificate).to_owned()).map_err(invalid)?; }
        let mut links = chain[..chain.len() - 1].windows(2).map(|pair| (pair[0].as_ptr(), pair[1].as_ptr())).collect::<Vec<_>>();
        links.push((chain[chain.len() - 2].as_ptr(), anchor.as_ptr()));
        let mut context = X509StoreContext::new().map_err(invalid)?;
        context.init(&store, chain[0], &untrusted, |context| {
            let result = (|| -> Result<()> {
                configure_parameters(context, self.now)?;
                let ssl_index = unsafe { openssl_sys::SSL_get_ex_data_X509_STORE_CTX_idx() };
                if ssl_index < 0 { return Err(invalid(openssl::error::ErrorStack::get())); }
                let ssl = unsafe { openssl_sys::X509_STORE_CTX_get_ex_data(self.source.as_ptr(), ssl_index) };
                if !ssl.is_null() && unsafe { X509_STORE_CTX_set_ex_data(context.as_ptr(), ssl_index, ssl) } != 1 { return Err(invalid(openssl::error::ErrorStack::get())); }
                unsafe { X509_STORE_CTX_set_verify_cb(context.as_ptr(), X509_STORE_CTX_get_verify_cb(self.source.as_ptr())); }
                if !verify_with_dates(context, self.now, &links)? { return Err(invalid(context.error())); }
                let selected = context.chain().ok_or_else(|| invalid("TLS PKIX validation produced no chain"))?;
                if selected.len() != chain.len() || selected.iter().take(chain.len() - 1).zip(chain.iter()).any(|(selected, expected)| selected.as_ptr() != expected.as_ptr()) {
                    return Err(invalid("TLS PKIX validation changed the selected certificate path"));
                }
                Ok(())
            })();
            Ok(result)
        }).map_err(invalid)?
    }
}

// This trusted DN/key assertion preserves anchor identity fields; its retained signature bytes do not authenticate the modified TBS and must never be verified or sent to a peer.
fn surrogate(original: &X509Ref) -> Result<X509> {
    let certificate = X509::from_der(&original.to_der().map_err(invalid)?).map_err(invalid)?;
    for index in (0..unsafe { openssl_sys::X509_get_ext_count(certificate.as_ptr()) }).rev() {
        let extension = unsafe { openssl_sys::X509_delete_ext(certificate.as_ptr(), index) };
        if extension.is_null() { return Err(invalid(openssl::error::ErrorStack::get())); }
        let extension = unsafe { X509Extension::from_ptr(extension) };
        let object = unsafe { openssl_sys::X509_EXTENSION_get_object(extension.as_ptr()) };
        if unsafe { openssl_sys::OBJ_obj2nid(object) } == Nid::SUBJECT_KEY_IDENTIFIER.as_raw() {
            if unsafe { openssl_sys::X509_EXTENSION_set_critical(extension.as_ptr(), 0) } != 1 || unsafe { openssl_sys::X509_add_ext(certificate.as_ptr(), extension.as_ptr(), -1) } != 1 {
                return Err(invalid(openssl::error::ErrorStack::get()));
            }
        }
    }
    for extension in [BasicConstraints::new().critical().ca().build().map_err(invalid)?, KeyUsage::new().key_cert_sign().build().map_err(invalid)?] {
        if unsafe { openssl_sys::X509_add_ext(certificate.as_ptr(), extension.as_ptr(), -1) } != 1 { return Err(invalid(openssl::error::ErrorStack::get())); }
    }
    if unsafe { openssl_sys::X509_set_version(certificate.as_ptr(), 2) } != 1 { return Err(invalid(openssl::error::ErrorStack::get())); }
    if unsafe { i2d_re_X509_tbs(certificate.as_ptr(), ptr::null_mut()) } <= 0 { return Err(invalid(openssl::error::ErrorStack::get())); }
    Ok(certificate)
}

fn single_anchor(anchor: &X509Ref) -> Result<X509Store> {
    let mut store = X509StoreBuilder::new().map_err(invalid)?;
    store.add_cert(anchor.to_owned()).map_err(invalid)?;
    unsafe { X509_STORE_set_check_issued(store.as_ptr(), Some(check_issued)); }
    Ok(store.build())
}

unsafe extern "C" fn check_issued(context: *mut openssl_sys::X509_STORE_CTX, certificate: *mut openssl_sys::X509, issuer: *mut openssl_sys::X509) -> i32 {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let Ok(index) = date_index() else { return 0; };
        let state = unsafe { openssl_sys::X509_STORE_CTX_get_ex_data(context, index) }.cast::<DateCheck<'_>>();
        if state.is_null() { return 0; }
        let state = unsafe { &*state };
        i32::from(state.links.contains(&(certificate, issuer)) && unsafe { openssl_sys::X509_check_issued(issuer, certificate) } == openssl_sys::X509_V_OK)
    })).unwrap_or(0)
}

unsafe extern "C" {
    fn X509_STORE_CTX_get0_store(context: *const openssl_sys::X509_STORE_CTX) -> *mut openssl_sys::X509_STORE;
    fn X509_STORE_get1_all_certs(store: *mut openssl_sys::X509_STORE) -> *mut openssl_sys::stack_st_X509;
    fn X509_STORE_set_check_issued(store: *mut openssl_sys::X509_STORE, callback: Option<unsafe extern "C" fn(*mut openssl_sys::X509_STORE_CTX, *mut openssl_sys::X509, *mut openssl_sys::X509) -> i32>);
    fn i2d_re_X509_tbs(certificate: *mut openssl_sys::X509, encoded: *mut *mut u8) -> i32;
}
