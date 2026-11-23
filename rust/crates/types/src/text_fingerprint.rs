use sha2::{Digest, Sha256};

pub fn text_pair_fingerprint<'a, E>(
    pairs: impl IntoIterator<Item = Result<(&'a str, &'a str), E>>,
) -> Result<[u8; 32], E> {
    let mut digest = Sha256::new();
    digest.update(b"rtdb.search.corpus.v1\0");
    for pair in pairs {
        let (id, body) = pair?;
        for text in [id, body] {
            digest.update((text.len() as u64).to_be_bytes());
            digest.update(text.as_bytes());
        }
    }
    Ok(digest.finalize().into())
}
