use super::*;
use admission::{expect, next, malformed_parameter};

pub(super) const OID: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 5, 13];
const PBKDF2: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 5, 12];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Name {
    HmacSha1Aes128, HmacSha224Aes128, HmacSha256Aes128, HmacSha384Aes128,
    HmacSha512Aes128, HmacSha512224Aes128, HmacSha512256Aes128,
    HmacSha1Aes256, HmacSha224Aes256, HmacSha256Aes256, HmacSha384Aes256,
    HmacSha512Aes256, HmacSha512224Aes256, HmacSha512256Aes256,
}

#[derive(Clone, Copy)]
enum Prf { Sha1, Sha224, Sha256, Sha384, Sha512, Sha512224, Sha512256 }

#[derive(Clone, Copy)]
enum Aes { Bits128, Bits256 }

impl Name {
    pub(super) fn reencoding_oid(&self) -> Result<&[u8]> {
        match self {
            Self::HmacSha1Aes128 | Self::HmacSha224Aes128 | Self::HmacSha256Aes128 | Self::HmacSha384Aes128
                | Self::HmacSha512Aes128 | Self::HmacSha512224Aes128 | Self::HmacSha512256Aes128
                | Self::HmacSha1Aes256 | Self::HmacSha224Aes256 | Self::HmacSha256Aes256 | Self::HmacSha384Aes256
                | Self::HmacSha512Aes256 | Self::HmacSha512224Aes256 | Self::HmacSha512256Aes256 => Err(malformed()),
        }
    }

    #[cfg(test)]
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::HmacSha1Aes128 => "PBEWithHmacSHA1AndAES_128",
            Self::HmacSha224Aes128 => "PBEWithHmacSHA224AndAES_128",
            Self::HmacSha256Aes128 => "PBEWithHmacSHA256AndAES_128",
            Self::HmacSha384Aes128 => "PBEWithHmacSHA384AndAES_128",
            Self::HmacSha512Aes128 => "PBEWithHmacSHA512AndAES_128",
            Self::HmacSha512224Aes128 => "PBEWithHmacSHA512/224AndAES_128",
            Self::HmacSha512256Aes128 => "PBEWithHmacSHA512/256AndAES_128",
            Self::HmacSha1Aes256 => "PBEWithHmacSHA1AndAES_256",
            Self::HmacSha224Aes256 => "PBEWithHmacSHA224AndAES_256",
            Self::HmacSha256Aes256 => "PBEWithHmacSHA256AndAES_256",
            Self::HmacSha384Aes256 => "PBEWithHmacSHA384AndAES_256",
            Self::HmacSha512Aes256 => "PBEWithHmacSHA512AndAES_256",
            Self::HmacSha512256Aes256 => "PBEWithHmacSHA512/256AndAES_256",
            Self::HmacSha512224Aes256 => "PBEWithHmacSHA512/224AndAES_256",
        }
    }
}

pub(super) fn decode(bytes: &[u8]) -> DecodeResult<Name> {
    let mut input = Der(bytes);
    let outer = expect(&mut input, 0x30)?;
    admission::end(input)?;
    let mut fields = Der(&outer);
    let first = next(&mut fields)?;
    if first.tag == 6 {
        let actual = next(&mut fields)?;
        let mut fields = Der(&actual.content);
        let kdf = next(&mut fields)?;
        named(actual.tag, kdf, &mut fields)
    } else { named(0x30, first, &mut fields) }
}

fn named(tag: u8, kdf: secret_der::Value<'_>, fields: &mut Der<'_>) -> DecodeResult<Name> {
    let prf = parse_kdf(kdf)?;
    if tag != 0x30 { return Err(malformed_parameter()); }
    let aes = parse_encryption(next(fields)?)?;
    Ok(match (prf, aes) {
        (Prf::Sha1, Aes::Bits128) => Name::HmacSha1Aes128,
        (Prf::Sha224, Aes::Bits128) => Name::HmacSha224Aes128,
        (Prf::Sha256, Aes::Bits128) => Name::HmacSha256Aes128,
        (Prf::Sha384, Aes::Bits128) => Name::HmacSha384Aes128,
        (Prf::Sha512, Aes::Bits128) => Name::HmacSha512Aes128,
        (Prf::Sha512224, Aes::Bits128) => Name::HmacSha512224Aes128,
        (Prf::Sha512256, Aes::Bits128) => Name::HmacSha512256Aes128,
        (Prf::Sha1, Aes::Bits256) => Name::HmacSha1Aes256,
        (Prf::Sha224, Aes::Bits256) => Name::HmacSha224Aes256,
        (Prf::Sha256, Aes::Bits256) => Name::HmacSha256Aes256,
        (Prf::Sha384, Aes::Bits256) => Name::HmacSha384Aes256,
        (Prf::Sha512, Aes::Bits256) => Name::HmacSha512Aes256,
        (Prf::Sha512224, Aes::Bits256) => Name::HmacSha512224Aes256,
        (Prf::Sha512256, Aes::Bits256) => Name::HmacSha512256Aes256,
    })
}

fn parse_kdf(kdf: secret_der::Value<'_>) -> DecodeResult<Prf> {
    let mut fields = Der(&kdf.content);
    let oid = expect(&mut fields, 6)?;
    admission::valid_oid(&oid)?;
    if oid.as_ref() != PBKDF2 || kdf.tag != 0x30 { return Err(malformed_parameter()); }
    let parameters = expect(&mut fields, 0x30)?;
    let mut parameters = Der(&parameters);
    expect(&mut parameters, 4)?;
    admission::int(&mut parameters)?;
    if parameters.0.first() == Some(&2) { admission::int(&mut parameters)?; }
    if parameters.0.first() != Some(&0x30) { return Ok(Prf::Sha1); }
    let prf = expect(&mut parameters, 0x30)?;
    let mut prf = Der(&prf);
    let oid = expect(&mut prf, 6)?;
    admission::valid_oid(&oid)?;
    let algorithm = match oid.as_ref() {
        [0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 2, 7] => Prf::Sha1,
        [0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 2, 8] => Prf::Sha224,
        [0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 2, 9] => Prf::Sha256,
        [0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 2, 10] => Prf::Sha384,
        [0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 2, 11] => Prf::Sha512,
        [0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 2, 12] => Prf::Sha512224,
        [0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 2, 13] => Prf::Sha512256,
        _ => return Err(malformed_parameter()),
    };
    if prf.0.first() == Some(&5) { next(&mut prf)?; }
    admission::end(prf)?;
    Ok(algorithm)
}

fn parse_encryption(encryption: secret_der::Value<'_>) -> DecodeResult<Aes> {
    let mut fields = Der(&encryption.content);
    let oid = expect(&mut fields, 6)?;
    admission::valid_oid(&oid)?;
    let aes = match oid.as_ref() {
        [0x60, 0x86, 0x48, 1, 0x65, 3, 4, 1, 2] => Aes::Bits128,
        [0x60, 0x86, 0x48, 1, 0x65, 3, 4, 1, 42] => Aes::Bits256,
        _ => return Err(malformed_parameter()),
    };
    expect(&mut fields, 4)?;
    Ok(aes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::java_der::encoded as der;

    #[test]
    fn initialized_names_match_all_fourteen_java_spellings_and_have_no_reverse_oid() {
        for (suffix, digest) in [(7, "SHA1"), (8, "SHA224"), (9, "SHA256"), (10, "SHA384"), (11, "SHA512"), (12, "SHA512/224"), (13, "SHA512/256")] {
            for (cipher, bits) in [(2, 128), (42, 256)] {
                let prf = der(0x30, &der(6, &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 2, suffix]));
                let pbkdf2 = der(0x30, &[der(4, &[]), der(2, &[0xff]), prf].concat());
                let kdf = der(0x30, &[der(6, PBKDF2), pbkdf2].concat());
                let encryption = der(0x30, &[der(6, &[0x60, 0x86, 0x48, 1, 0x65, 3, 4, 1, cipher]), der(4, &[])].concat());
                let name = decode(&der(0x30, &[kdf, encryption].concat())).unwrap();
                assert_eq!(name.as_str(), format!("PBEWithHmac{digest}AndAES_{bits}"));
                assert!(name.reencoding_oid().is_err());
            }
        }
    }
}
