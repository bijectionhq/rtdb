use sha1::{Digest, Sha1};
use subtle::ConstantTimeEq;
use zeroize::Zeroizing;
use rtdb_types::Result;
use crate::{invalid, java_der::Der, secret_der::{self, Algorithm, Value, next}};

fn octets(value: Value<'_>) -> Result<Zeroizing<Vec<u8>>> { secret_der::octets(value, malformed) }
fn algorithm(input: &mut Der<'_>) -> Result<Algorithm> { secret_der::algorithm(input, malformed) }

pub(super) fn decrypt(protected: &[u8], password: &[u8]) -> Result<Zeroizing<Vec<u8>>> {
    if protected.len() > i32::MAX as usize { return Err(malformed()); }
    let mut input = Der(protected);
    let outer = next(&mut input)?;
    input.end()?;
    let mut fields = Der(&outer.content);
    let algorithm = algorithm(&mut fields)?;
    let encrypted = octets(next(&mut fields)?)?;
    fields.end()?;
    if algorithm.oid.as_slice() != [0x2b, 6, 1, 4, 1, 0x2a, 2, 0x11, 1, 1] { return Err(invalid("Unsupported JKS private key protection algorithm")); }
    if encrypted.len() < 40 { return Err(malformed()); }
    let (salt, rest) = encrypted.split_at(20);
    let (encrypted, expected) = rest.split_at(rest.len() - 20);
    let mut previous = Zeroizing::new(<[u8; 20]>::try_from(salt).map_err(invalid)?);
    let mut plaintext = Zeroizing::new(Vec::with_capacity(encrypted.len()));
    for block in encrypted.chunks(20) {
        let mut hash = Sha1::new(); hash.update(password); hash.update(previous.as_slice());
        *previous = hash.finalize().into();
        plaintext.extend(block.iter().zip(previous.iter()).map(|(byte, mask)| byte ^ mask));
    }
    let mut verifier = Sha1::new(); verifier.update(password); verifier.update(plaintext.as_slice());
    if !bool::from(verifier.finalize().as_slice().ct_eq(expected)) { return Err(invalid("JKS private key password is incorrect")); }
    Ok(plaintext)
}

fn malformed() -> rtdb_types::Anomaly { invalid("Malformed SunX509 JKS private key") }
