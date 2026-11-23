//! Native trie data uses fixed, independently authenticated frames.
//! An authenticated header binds the length and digest of the entire plaintext.
//! Each frame binds that identity, the logical object key and its frame index.

use std::ops::Range;

use crate::{Error, ObjectKey, Result};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use ring::digest::{SHA256, digest};

use super::{Cipher, SEALED_OVERHEAD};

pub const FRAME_BYTES: usize = 64 * 1024;
const FRAME_STRIDE: usize = FRAME_BYTES + SEALED_OVERHEAD;
const HEADER_BYTES: usize = 8 + 32;
pub const SEALED_HEADER_BYTES: usize = HEADER_BYTES + SEALED_OVERHEAD;

pub fn framed(key: &ObjectKey) -> bool {
    let key = key.as_str();
    (key.starts_with("tables/") && key.contains("/data/") && key.ends_with(".arrow"))
        || rtdb_format::entity_history::is_ranged_artefact(key)
        || rtdb_format::search_manifest::is_ranged_run(key)
}

fn invalid(key: &ObjectKey) -> Error {
    Error::ConflictingObject(key.to_string())
}

pub fn sealed_size(key: &ObjectKey, length: usize) -> Result<usize> {
    let overhead = if framed(key) {
        length
            .div_ceil(FRAME_BYTES)
            .checked_mul(SEALED_OVERHEAD)
            .and_then(|frames| frames.checked_add(SEALED_HEADER_BYTES))
    } else {
        Some(SEALED_OVERHEAD)
    };
    overhead
        .and_then(|overhead| length.checked_add(overhead))
        .ok_or_else(|| Error::Task("Encrypted object length overflow".into()))
}

pub fn plaintext_size(key: &ObjectKey, length: u64) -> Result<u64> {
    if !framed(key) {
        return Ok(length.saturating_sub(SEALED_OVERHEAD as u64));
    }
    let frames = length
        .checked_sub(SEALED_HEADER_BYTES as u64)
        .ok_or_else(|| invalid(key))?;
    let tail = frames % FRAME_STRIDE as u64;
    let tail = if tail == 0 {
        0
    } else {
        tail.checked_sub(SEALED_OVERHEAD as u64)
            .filter(|length| *length > 0)
            .ok_or_else(|| invalid(key))?
    };
    Ok(frames / FRAME_STRIDE as u64 * FRAME_BYTES as u64 + tail)
}

#[derive(Clone, Copy)]
pub struct FramedObject {
    length: usize,
    identity: [u8; 32],
}

impl FramedObject {
    fn new(plaintext: &[u8]) -> Self {
        let mut identity = [0; 32];
        identity.copy_from_slice(digest(&SHA256, plaintext).as_ref());
        Self {
            length: plaintext.len(),
            identity,
        }
    }

    pub fn length(&self) -> usize {
        self.length
    }
    fn frame_count(&self) -> usize {
        self.length.div_ceil(FRAME_BYTES)
    }

    fn header_associated(key: &ObjectKey) -> String {
        format!("\0rtdb-frame-header/v1/{}", key.as_str())
    }

    fn frame_associated(&self, key: &ObjectKey, index: usize) -> String {
        format!(
            "\0rtdb-frame/v1/{}/{index}/{}/{}",
            self.length,
            URL_SAFE_NO_PAD.encode(self.identity),
            key.as_str()
        )
    }

    pub fn seal_header(&self, cipher: &Cipher, key: &ObjectKey) -> Result<Bytes> {
        let mut header = [0; HEADER_BYTES];
        header[..8].copy_from_slice(&(self.length as u64).to_be_bytes());
        header[8..].copy_from_slice(&self.identity);
        cipher.seal_deterministic(&Self::header_associated(key), &header)
    }

    pub fn open_header(cipher: &Cipher, key: &ObjectKey, sealed: &[u8]) -> Result<Self> {
        if sealed.len() != SEALED_HEADER_BYTES {
            return Err(invalid(key));
        }
        let header = cipher.open(&Self::header_associated(key), sealed)?;
        if header.len() != HEADER_BYTES {
            return Err(invalid(key));
        }
        let length = u64::from_be_bytes(header[..8].try_into().map_err(|_| invalid(key))?);
        let length = usize::try_from(length).map_err(|_| invalid(key))?;
        sealed_size(key, length)?;
        Ok(Self {
            length,
            identity: header[8..].try_into().map_err(|_| invalid(key))?,
        })
    }

    pub fn covering_frames(&self, key: &ObjectKey, range: &Range<u64>) -> Result<Range<usize>> {
        if range.start > range.end || range.end > self.length as u64 {
            return Err(Error::InvalidRange {
                key: key.to_string(),
                start: range.start,
                end: range.end,
                length: self.length as u64,
            });
        }
        if range.is_empty() {
            return Ok(0..0);
        }
        Ok(range.start as usize / FRAME_BYTES..(range.end as usize).div_ceil(FRAME_BYTES))
    }

    fn boundary(&self, index: usize) -> usize {
        SEALED_HEADER_BYTES
            + index * SEALED_OVERHEAD
            + self.length.min(index.saturating_mul(FRAME_BYTES))
    }

    pub fn physical_range(&self, frames: &Range<usize>) -> Range<u64> {
        self.boundary(frames.start) as u64..self.boundary(frames.end) as u64
    }

    pub fn selected_frames(&self, key: &ObjectKey, range: &Range<u64>) -> Result<Range<usize>> {
        let start = range
            .start
            .checked_sub(SEALED_HEADER_BYTES as u64)
            .ok_or_else(|| invalid(key))?;
        let end = range
            .end
            .checked_sub(SEALED_HEADER_BYTES as u64)
            .ok_or_else(|| invalid(key))?;
        let first = usize::try_from(start / FRAME_STRIDE as u64).map_err(|_| invalid(key))?;
        let after = usize::try_from(end.div_ceil(FRAME_STRIDE as u64)).map_err(|_| invalid(key))?;
        let frames = first..after;
        if first >= after || after > self.frame_count() || self.physical_range(&frames) != *range {
            return Err(invalid(key));
        }
        Ok(frames)
    }

    pub fn seal_frames(
        &self,
        cipher: &Cipher,
        key: &ObjectKey,
        first: usize,
        plaintext: &[u8],
        out: &mut Vec<u8>,
    ) -> Result<()> {
        for (offset, frame) in plaintext.chunks(FRAME_BYTES).enumerate() {
            out.extend_from_slice(
                &cipher.seal_deterministic(&self.frame_associated(key, first + offset), frame)?,
            );
        }
        Ok(())
    }

    pub fn open_frames(
        &self,
        cipher: &Cipher,
        key: &ObjectKey,
        frames: Range<usize>,
        sealed: &[u8],
    ) -> Result<Bytes> {
        if frames.start > frames.end || frames.end > self.frame_count() {
            return Err(invalid(key));
        }
        let expected = self.physical_range(&frames);
        if sealed.len() as u64 != expected.end - expected.start {
            return Err(invalid(key));
        }
        let mut plaintext = Vec::with_capacity(sealed.len() - frames.len() * SEALED_OVERHEAD);
        let mut offset = 0;
        for index in frames {
            let length = self.boundary(index + 1) - self.boundary(index);
            let frame = cipher.open(
                &self.frame_associated(key, index),
                &sealed[offset..offset + length],
            )?;
            plaintext.extend_from_slice(&frame);
            offset += length;
        }
        Ok(plaintext.into())
    }
}

impl Cipher {
    pub fn seal_object(&self, key: &ObjectKey, plaintext: &[u8]) -> Result<Bytes> {
        if !framed(key) {
            return self.seal_deterministic(key.as_str(), plaintext);
        }
        let header = FramedObject::new(plaintext);
        let mut sealed = Vec::with_capacity(sealed_size(key, plaintext.len())?);
        sealed.extend_from_slice(&header.seal_header(self, key)?);
        header.seal_frames(self, key, 0, plaintext, &mut sealed)?;
        Ok(sealed.into())
    }

    pub fn open_object(&self, key: &ObjectKey, sealed: &[u8]) -> Result<Bytes> {
        if !framed(key) {
            return self.open(key.as_str(), sealed);
        }
        let head = sealed
            .get(..SEALED_HEADER_BYTES)
            .ok_or_else(|| invalid(key))?;
        let header = FramedObject::open_header(self, key, head)?;
        self.open_framed_object(key, sealed, header)
    }

    pub fn open_retained_object(&self, key: &ObjectKey, sealed: &[u8]) -> Result<Bytes> {
        if framed(key)
            && let Some(header) = self.retained_frame_header(key, sealed)
        {
            return self.open_framed_object(key, sealed, header);
        }
        self.open(key.as_str(), sealed)
    }

    pub fn retained_plaintext_size(
        &self,
        key: &ObjectKey,
        stored: usize,
        header: &[u8],
    ) -> Result<usize> {
        if framed(key)
            && let Some(layout) = self.retained_frame_header(key, header)
        {
            if sealed_size(key, layout.length())? != stored {
                return Err(invalid(key));
            }
            return Ok(layout.length());
        }
        stored
            .checked_sub(SEALED_OVERHEAD)
            .ok_or_else(|| invalid(key))
    }

    fn retained_frame_header(&self, key: &ObjectKey, sealed: &[u8]) -> Option<FramedObject> {
        FramedObject::open_header(self, key, sealed.get(..SEALED_HEADER_BYTES)?).ok()
    }

    fn open_framed_object(
        &self,
        key: &ObjectKey,
        sealed: &[u8],
        header: FramedObject,
    ) -> Result<Bytes> {
        let plaintext = header.open_frames(
            self,
            key,
            0..header.frame_count(),
            &sealed[SEALED_HEADER_BYTES..],
        )?;
        if FramedObject::new(&plaintext).identity != header.identity {
            return Err(invalid(key));
        }
        Ok(plaintext)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn shared_history_and_allocated_search_runs_admit_framed_ranges() {
        use super::framed;
        use crate::ObjectKey;

        let key = |name: &str| ObjectKey::new(format!("tables/public$docs/{name}")).unwrap();
        assert!(framed(&key(
            "artefacts/l00-rc-b00/entity-history/headers-2-v2.bin"
        )));
        assert!(framed(&key(
            "artefacts/l00-rc-b00/search/docs_body~a1f16w-v1.bin"
        )));
        let digest = "a".repeat(64);
        assert!(framed(&key(&format!("search-runs/{digest}.f16.arrow"))));
        assert!(!framed(&key(&format!("search-runs/{digest}.arrow"))));
        assert!(!framed(&key(&format!("search-pending/{digest}.f16.arrow"))));
    }

    use super::*;

    #[test]
    fn retained_envelopes_require_the_authentic_logical_key_and_complete_body() {
        let cipher = Cipher::new(&[7; 32]).unwrap();
        let name = ObjectKey::new("tables/orders/data/l00-rc-b00.arrow").unwrap();
        let other = ObjectKey::new("tables/orders/data/l00-rc-b01.arrow").unwrap();
        let data = vec![3; FRAME_BYTES + 1];
        let legacy = cipher.seal_deterministic(name.as_str(), &data).unwrap();
        assert!(cipher.open_retained_object(&other, &legacy).is_err());
        assert_eq!(
            cipher
                .retained_plaintext_size(&name, legacy.len(), &legacy[..SEALED_HEADER_BYTES])
                .unwrap(),
            data.len()
        );
        assert_eq!(
            cipher
                .open_retained_object(&name, &legacy)
                .unwrap()
                .as_ref(),
            data.as_slice()
        );
        let framed = cipher.seal_object(&name, &data).unwrap();
        assert_eq!(
            cipher
                .retained_plaintext_size(&name, framed.len(), &framed[..SEALED_HEADER_BYTES])
                .unwrap(),
            data.len()
        );
        assert!(
            cipher
                .retained_plaintext_size(&name, framed.len() - 1, &framed[..SEALED_HEADER_BYTES])
                .is_err()
        );
        let mut corrupt = framed.to_vec();
        corrupt[SEALED_HEADER_BYTES] ^= 1;
        assert!(cipher.open_retained_object(&name, &corrupt).is_err());
    }

    #[test]
    fn a_frame_cannot_move_between_files_contents_or_positions() {
        let cipher = Cipher::new(&[7; 32]).unwrap();
        let key = ObjectKey::new("tables/t/data/l00-rc-b00.arrow").unwrap();
        let other = ObjectKey::new("tables/t/data/l00-rc-b01.arrow").unwrap();
        let bytes = vec![3; FRAME_BYTES * 2 + 19];
        let sealed = cipher.seal_object(&key, &bytes).unwrap();
        assert_eq!(
            plaintext_size(&key, sealed.len() as u64).unwrap(),
            bytes.len() as u64
        );
        assert_eq!(
            cipher.open_object(&key, &sealed).unwrap().as_ref(),
            bytes.as_slice()
        );
        assert!(cipher.open_object(&other, &sealed).is_err());

        let layout =
            FramedObject::open_header(&cipher, &key, &sealed[..SEALED_HEADER_BYTES]).unwrap();
        let window = FRAME_BYTES as u64 - 7..FRAME_BYTES as u64 + 10;
        assert_eq!(layout.covering_frames(&key, &window).unwrap(), 0..2);
        let first = layout.physical_range(&(0..1));
        let second = layout.physical_range(&(1..2));
        let first = first.start as usize..first.end as usize;
        let second = second.start as usize..second.end as usize;

        let mut changed = bytes.clone();
        changed[0] ^= 1;
        let changed = cipher.seal_object(&key, &changed).unwrap();
        let mut spliced = sealed.to_vec();
        spliced[first.clone()].copy_from_slice(&changed[first.clone()]);
        assert!(cipher.open_object(&key, &spliced).is_err());

        let mut reordered = sealed.to_vec();
        reordered[first].copy_from_slice(&sealed[second]);
        assert!(cipher.open_object(&key, &reordered).is_err());
    }
}
