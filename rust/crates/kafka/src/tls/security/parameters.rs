use std::{ffi::c_void, ptr};
use foreign_types::ForeignTypeRef;
use openssl::{nid::Nid, x509::X509Ref};
use rtdb_types::Result;
use super::super::invalid;

pub(super) fn of(certificate: &X509Ref) -> Result<Option<&[u8]>> {
    let mut kind = -1;
    let mut value: *const c_void = ptr::null();
    unsafe { openssl_sys::X509_ALGOR_get0(ptr::null_mut(), &mut kind, &mut value, certificate.signature_algorithm().as_ptr()); }
    if kind == -1 || kind == openssl_sys::V_ASN1_NULL { return Ok(None); }
    if kind != openssl_sys::V_ASN1_SEQUENCE || value.is_null() { return Err(invalid("TLS signature parameters are not a sequence")); }
    let value = value.cast::<openssl_sys::ASN1_STRING>();
    let length = usize::try_from(unsafe { openssl_sys::ASN1_STRING_length(value) }).map_err(|_| invalid("TLS signature parameter length is invalid"))?;
    let bytes = unsafe { openssl_sys::ASN1_STRING_get0_data(value) };
    if length == 0 { return Ok(Some(&[])); }
    if bytes.is_null() { return Err(invalid("TLS signature parameters are missing")); }
    Ok(Some(unsafe { std::slice::from_raw_parts(bytes, length) }))
}

struct Der<'a>(&'a [u8]);
impl<'a> Der<'a> {
    fn next(&mut self) -> Result<(u8, &'a [u8])> {
        let (&tag, rest) = self.0.split_first().ok_or_else(malformed)?;
        let (&first, mut rest) = rest.split_first().ok_or_else(malformed)?;
        let length = if first < 0x80 { usize::from(first) } else {
            let count = usize::from(first & 0x7f);
            if count == 0 || count > std::mem::size_of::<usize>() || rest.len() < count || rest[0] == 0 { return Err(malformed()); }
            let value = rest[..count].iter().try_fold(0usize, |value, byte| value.checked_mul(256).and_then(|value| value.checked_add(usize::from(*byte))).ok_or_else(malformed))?;
            if value < 128 { return Err(malformed()); }
            rest = &rest[count..]; value
        };
        let content = rest.get(..length).ok_or_else(malformed)?;
        self.0 = &rest[length..];
        Ok((tag, content))
    }
    fn expect(&mut self, expected: u8) -> Result<&'a [u8]> {
        let (tag, value) = self.next()?;
        if tag != expected { return Err(malformed()); }
        Ok(value)
    }
    fn end(self) -> Result<()> { if self.0.is_empty() { Ok(()) } else { Err(malformed()) } }
}

fn algorithm(value: &[u8]) -> Result<(&[u8], Option<&[u8]>)> {
    let mut outer = Der(value);
    let mut fields = Der(outer.expect(0x30)?); outer.end()?;
    let oid = fields.expect(0x06)?;
    let parameters = if fields.0.is_empty() { None } else {
        let original = fields.0; fields.next()?; Some(&original[..original.len() - fields.0.len()])
    };
    fields.end()?;
    Ok((oid, parameters))
}

fn hash(value: &[u8]) -> Result<Nid> {
    Ok(match algorithm(value)?.0 {
        [0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 2, 2] => Nid::MD2,
        [0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 2, 5] => Nid::MD5,
        [0x2b, 0x0e, 3, 2, 0x1a] => Nid::SHA1,
        _ => Nid::UNDEF,
    })
}

// RFC 8017 Appendix A.2.3, RSASSA-PSS-params defaults and explicit digest identifiers.
pub(super) fn pss(value: &[u8]) -> Result<(Nid, Nid)> {
    let mut outer = Der(value);
    let mut fields = Der(outer.expect(0x30)?); outer.end()?;
    let (mut digest, mut mask_digest) = (Nid::SHA1, Nid::SHA1);
    let mut previous = None;
    while !fields.0.is_empty() {
        let (tag, value) = fields.next()?;
        if !(0xa0..=0xa3).contains(&tag) || previous.is_some_and(|previous| previous >= tag) { return Err(malformed()); }
        previous = Some(tag);
        match tag {
            0xa0 => digest = hash(value)?,
            0xa1 => {
                let (oid, parameters) = algorithm(value)?;
                if oid != [0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 1, 8] { return Err(malformed()); }
                mask_digest = hash(parameters.ok_or_else(malformed)?)?;
            }
            _ => {
                let mut integer = Der(value);
                let bytes = integer.expect(2)?; integer.end()?;
                if bytes.is_empty() || bytes[0] & 0x80 != 0 { return Err(malformed()); }
            }
        }
    }
    Ok((digest, mask_digest))
}

fn malformed() -> rtdb_types::Anomaly { invalid("TLS RSA-PSS parameters are malformed") }
