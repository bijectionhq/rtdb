use std::{ffi::c_void, sync::{Arc, OnceLock}};
use foreign_types::ForeignTypeRef;
use openssl::{error::ErrorStack, ex_data::Index, ssl::{Ssl, SslContextBuilder, SslRef}};
use rtdb_columnar::MemoryReservation;
use rtdb_types::Result;
use super::{invalid, keys::{self, KeyType}};

pub(super) struct Request {
    signatures: Vec<u16>,
    certificate_signatures: Option<Vec<u16>>,
    certificate_types: Option<Vec<u8>>,
    _charge: Arc<MemoryReservation>,
}
enum Captured { Rejected, Ready(Request) }

// The installed message callback borrows the reservation until the context and its SSL objects are destroyed.
pub(super) unsafe fn configure(context: &mut SslContextBuilder, charge: &Arc<MemoryReservation>) -> Result<()> {
    index()?;
    unsafe {
        SSL_CTX_set_msg_callback(context.as_ptr(), Some(capture));
        openssl_sys::SSL_CTX_ctrl(context.as_ptr(), 16, 0, Arc::as_ptr(charge).cast_mut().cast());
    }
    Ok(())
}

impl Request {
    pub(super) fn of(ssl: &SslRef) -> Result<&Self> {
        match ssl.ex_data(index()?) {
            Some(Captured::Ready(request)) => Ok(request),
            _ => Err(invalid("TLS client certificate request was not admitted")),
        }
    }

    pub(super) fn permits(&self, certificate: &openssl::x509::X509Ref) -> bool {
        super::security::permits_client_certificate(certificate, self.certificate_signatures.as_deref().unwrap_or(&self.signatures).iter().copied(), self.certificate_types.is_none())
    }

    pub(super) fn requested_types(&self, ssl: &SslRef) -> Vec<KeyType> {
        let certificates = self.certificate_signatures.as_deref().unwrap_or(&self.signatures);
        let count = unsafe { SSL_get_shared_sigalgs(ssl.as_ptr(), 0, std::ptr::null_mut(), std::ptr::null_mut(), std::ptr::null_mut(), std::ptr::null_mut(), std::ptr::null_mut()) };
        let signatures = (0..count).map(|index| {
            let (mut signature, mut hash) = (0, 0);
            unsafe { SSL_get_shared_sigalgs(ssl.as_ptr(), index, std::ptr::null_mut(), std::ptr::null_mut(), std::ptr::null_mut(), &mut signature, &mut hash); }
            u16::from_be_bytes([hash, signature])
        });
        selection_types(certificates.iter().copied(), signatures, self.certificate_types.as_deref())
    }
}

pub(crate) fn selection_types(certificates: impl IntoIterator<Item = u16>, signatures: impl IntoIterator<Item = u16>, certificate_types: Option<&[u8]>) -> Vec<KeyType> {
    let tls13 = certificate_types.is_none();
    let signing_types = keys::requested_types(signatures);
    let mut types = keys::requested_types(certificates.into_iter().filter(|scheme| !tls13 || !matches!(*scheme, 0x0301..=0x0303 | 0x0202 | 0x0402)));
    types.retain(|kind| signing_types.contains(kind) && certificate_types.is_none_or(|offered| {
        offered.contains(&match kind { KeyType::Rsa | KeyType::RsaPss => 1, KeyType::Dsa => 2, KeyType::Ec | KeyType::EdDsa => 64, KeyType::Other => 0 })
    }));
    types
}

fn index() -> Result<Index<Ssl, Captured>> {
    static INDEX: OnceLock<std::result::Result<Index<Ssl, Captured>, ErrorStack>> = OnceLock::new();
    INDEX.get_or_init(Ssl::new_ex_index).as_ref().copied().map_err(invalid)
}

unsafe extern "C" fn capture(write: i32, version: i32, content: i32, buffer: *const c_void, length: usize, ssl: *mut openssl_sys::SSL, opaque: *mut c_void) {
    if write != 0 || content != 22 || length == 0 || buffer.is_null() || ssl.is_null() { return; }
    let bytes = unsafe { std::slice::from_raw_parts(buffer.cast::<u8>(), length) };
    if bytes[0] != 13 { return; }
    let Ok(index) = index() else { return; };
    let captured = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let charge = unsafe { &*opaque.cast::<MemoryReservation>() };
        let bound = bytes.len().checked_mul(4).and_then(|size| size.checked_add(65_536)).ok_or_else(|| invalid("TLS certificate request exceeds allocation range"))?;
        let charge = charge.budget().reserve(bound)?;
        Request::parse(bytes, version, charge)
    }));
    let state = match captured { Ok(Ok(request)) => Captured::Ready(request), _ => Captured::Rejected };
    unsafe { SslRef::from_ptr_mut(ssl) }.set_ex_data(index, state);
}

impl Request {
    // RFC 5246 section 7.4.4 and RFC 8446 section 4.3.2.
    fn parse(bytes: &[u8], version: i32, charge: Arc<MemoryReservation>) -> Result<Self> {
        let mut input = Cursor(bytes);
        if input.take(1)? != [13] { return Err(invalid("TLS handshake is not a certificate request")); }
        let length = input.take(3)?;
        let length = usize::from(length[0]) << 16 | usize::from(length[1]) << 8 | usize::from(length[2]);
        if input.0.len() != length { return Err(invalid("TLS certificate request length is invalid")); }
        let (signatures, certificate_signatures, certificate_types) = match version {
            0x0303 => {
                let types = input.vector8()?.to_vec();
                let signatures = schemes(input.vector16()?)?;
                input.vector16()?;
                (signatures, None, Some(types))
            }
            0x0304 => {
                input.vector8()?;
                let mut extensions = Cursor(input.vector16()?);
                let (mut signatures, mut certificates) = (None, None);
                while !extensions.0.is_empty() {
                    let kind = extensions.take(2)?;
                    let payload = extensions.vector16()?;
                    let destination = match u16::from_be_bytes([kind[0], kind[1]]) { 13 => &mut signatures, 50 => &mut certificates, _ => continue };
                    if destination.is_some() { return Err(invalid("TLS certificate request repeats a signature extension")); }
                    let mut payload = Cursor(payload);
                    *destination = Some(schemes(payload.vector16()?)?);
                    payload.finish()?;
                }
                (signatures.ok_or_else(|| invalid("TLS certificate request omits signature algorithms"))?, certificates, None)
            }
            _ => return Err(invalid("TLS certificate request has an unsupported protocol")),
        };
        input.finish()?;
        Ok(Self { signatures, certificate_signatures, certificate_types, _charge: charge })
    }
}

struct Cursor<'a>(&'a [u8]);
impl<'a> Cursor<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let (head, tail) = self.0.split_at_checked(length).ok_or_else(|| invalid("TLS certificate request is truncated"))?;
        self.0 = tail;
        Ok(head)
    }
    fn vector8(&mut self) -> Result<&'a [u8]> { let length = usize::from(self.take(1)?[0]); self.take(length) }
    fn vector16(&mut self) -> Result<&'a [u8]> { let length = self.take(2)?; let length = usize::from(u16::from_be_bytes([length[0], length[1]])); self.take(length) }
    fn finish(&self) -> Result<()> { if self.0.is_empty() { Ok(()) } else { Err(invalid("TLS certificate request has trailing bytes")) } }
}
fn schemes(bytes: &[u8]) -> Result<Vec<u16>> {
    if bytes.is_empty() || bytes.len() % 2 != 0 { return Err(invalid("TLS certificate request has an invalid signature list")); }
    Ok(bytes.chunks_exact(2).map(|pair| u16::from_be_bytes([pair[0], pair[1]])).collect())
}

unsafe extern "C" {
    fn SSL_CTX_set_msg_callback(context: *mut openssl_sys::SSL_CTX, callback: Option<unsafe extern "C" fn(i32, i32, i32, *const c_void, usize, *mut openssl_sys::SSL, *mut c_void)>);
    fn SSL_get_shared_sigalgs(ssl: *mut openssl_sys::SSL, index: i32, signature: *mut i32, hash: *mut i32, signature_hash: *mut i32, raw_signature: *mut u8, raw_hash: *mut u8) -> i32;
}

#[cfg(test)]
mod tests;
