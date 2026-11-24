use std::io::{self, Read, Seek, SeekFrom};
use std::ops::Range;

use super::COPY_INPUT_SCRATCH_BYTES;

// Fit both allocations inside the existing COPY input charge so retention cannot refuse a COPY.
const RETAINED_BYTES: usize = COPY_INPUT_SCRATCH_BYTES / 2;
const RETAINED_SPANS: usize = 64;

struct HeaderSpan {
    file_offset: u64,
    bytes: Range<usize>,
}

#[derive(Default)]
pub(super) struct StreamHeaders {
    bytes: Vec<u8>,
    spans: Vec<HeaderSpan>,
}

impl StreamHeaders {
    pub(super) fn retain(&mut self, file_offset: u64, prefix: &[u8], metadata: &[u8]) {
        let start = self.bytes.len();
        if self.spans.len() == RETAINED_SPANS || prefix.len() + metadata.len() > RETAINED_BYTES - start {
            return;
        }
        if self.spans.is_empty() && (self.spans.try_reserve_exact(RETAINED_SPANS).is_err()
            || self.bytes.try_reserve_exact(RETAINED_BYTES).is_err()) {
            return;
        }
        self.bytes.extend_from_slice(prefix);
        self.bytes.extend_from_slice(metadata);
        self.spans.push(HeaderSpan { file_offset, bytes: start..self.bytes.len() });
    }

    pub(super) fn replay<R: Read + Seek>(self, file: R) -> StreamReplay<R> {
        StreamReplay { file, headers: self, next: 0, position: 0, file_position: 0 }
    }
}

pub(super) struct StreamReplay<R> {
    file: R,
    headers: StreamHeaders,
    next: usize,
    position: u64,
    file_position: u64,
}

impl<R: Seek> Seek for StreamReplay<R> {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        if matches!(position, SeekFrom::Current(0)) { return Ok(self.position); }
        self.position = match position {
            SeekFrom::Start(offset) => offset,
            SeekFrom::Current(offset) => self.position.checked_add_signed(offset)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Invalid COPY spool offset"))?,
            SeekFrom::End(offset) => {
                self.file_position = self.file.seek(SeekFrom::End(offset))?;
                self.file_position
            }
        };
        self.next = self.headers.spans.partition_point(|span| {
            span.file_offset + span.bytes.len() as u64 <= self.position
        });
        Ok(self.position)
    }
}

impl<R: Read + Seek> Read for StreamReplay<R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() { return Ok(0); }
        let mut read_limit = out.len();
        if let Some(span) = self.headers.spans.get(self.next) {
            if self.position >= span.file_offset {
                let start = span.bytes.start + (self.position - span.file_offset) as usize;
                let count = out.len().min(span.bytes.end - start);
                out[..count].copy_from_slice(&self.headers.bytes[start..start + count]);
                self.position += count as u64;
                if start + count == span.bytes.end { self.next += 1; }
                return Ok(count);
            }
            read_limit = (out.len() as u64).min(span.file_offset - self.position) as usize;
        }
        if self.file_position != self.position {
            self.file_position = self.file.seek(SeekFrom::Start(self.position))?;
        }
        let count = self.file.read(&mut out[..read_limit])?;
        self.file_position += count as u64;
        self.position += count as u64;
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::{RETAINED_BYTES, StreamHeaders};
    use std::io::{Cursor, Read};

    #[test]
    fn replay_reads_cached_headers_and_uses_the_spool_for_bodies_and_large_headers() {
        let large = vec![42; RETAINED_BYTES];
        let mut spool = b"....body".to_vec();
        spool.extend_from_slice(b"size");
        spool.extend_from_slice(&large);
        let end = spool.len();
        spool.extend_from_slice(b"....");

        let mut headers = StreamHeaders::default();
        headers.retain(0, b"he", b"ad");
        headers.retain(8, b"size", &large);
        headers.retain(end as u64, b"stop", &[]);

        let mut replay = headers.replay(Cursor::new(spool));
        let mut first_byte = [0];
        replay.read_exact(&mut first_byte).unwrap();
        let mut bytes = first_byte.to_vec();
        replay.read_to_end(&mut bytes).unwrap();
        assert_eq!(&bytes[..12], b"headbodysize");
        assert_eq!(&bytes[12..end], large);
        assert_eq!(&bytes[end..], b"stop");
    }
}
