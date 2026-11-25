//! Bounded compaction of canonical append frames.

use super::{Kind, Location, Record, Result, corrupt, io_error, read_exact_at};
use crate::protocol::FrameParts;
use bytes::Bytes;
use prost::{Message, encoding};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

const COPY_BYTES: usize = 64 * 1024;

pub(super) struct CompactionFrames<'a> {
    file: BufWriter<File>,
    path: &'a Path,
    parts: FrameParts,
    scratch: Vec<u8>,
    written: u64,
}

impl<'a> CompactionFrames<'a> {
    pub(super) fn new(file: File, path: &'a Path) -> Self {
        Self {
            file: BufWriter::with_capacity(COPY_BYTES, file),
            path,
            parts: FrameParts::default(),
            scratch: Vec::new(),
            written: 0,
        }
    }

    pub(super) fn position(&self) -> u64 {
        self.written
    }

    pub(super) fn push_kind(&mut self, stream: u64, kind: Kind) -> Result<()> {
        self.push(&Record {
            stream,
            kind: Some(kind),
            ..Record::default()
        })
    }

    pub(super) fn push(&mut self, record: &Record) -> Result<()> {
        self.parts.push_durable(record);
        self.parts
            .write_to(&mut self.file)
            .map_err(|error| io_error(self.path, error))?;
        self.written += self.parts.len() as u64;
        self.parts.clear();
        Ok(())
    }

    pub(super) fn finish(self) -> Result<File> {
        self.file
            .into_inner()
            .map_err(|error| io_error(self.path, error.into_error()))
    }

    /// A canonical frame already contains the exact output bytes. Validate its
    /// full controls and descriptor before using that layout to stream its body.
    /// An unfamiliar layout takes the original located Prost decoder.
    pub(super) fn copy_canonical(
        &mut self,
        source: &File,
        source_path: &Path,
        location: Location,
        mut record: Record,
    ) -> Result<bool> {
        let Some(Kind::Append(mut entry)) = record.kind.take() else {
            unreachable!("compaction copies append frames")
        };
        debug_assert!(entry.payload.is_empty());
        let (lsn, payload_len) = (entry.lsn, location.payload as usize);
        let entry_len = entry.encoded_len()
            + if payload_len == 0 {
                0
            } else {
                encoding::key_len(4)
                    + encoding::encoded_len_varint(payload_len as u64)
                    + payload_len
            };
        let mut prefix = Vec::new();
        delimited(6, entry_len, &mut prefix);
        if entry.lsn != 0 {
            encoding::sint64::encode(1, &entry.lsn, &mut prefix);
        }
        if entry.epoch != 0 {
            encoding::uint64::encode(2, &entry.epoch, &mut prefix);
        }
        if entry.timestamp != 0 {
            encoding::sint64::encode(3, &entry.timestamp, &mut prefix);
        }
        if payload_len != 0 {
            delimited(4, payload_len, &mut prefix);
        }
        entry.lsn = 0;
        entry.epoch = 0;
        entry.timestamp = 0;
        let mut suffix = Vec::new();
        entry.encode_raw(&mut suffix);
        record.encode_raw(&mut suffix);

        let length = u64::from(location.length);
        let body_len = prefix.len() as u64 + payload_len as u64 + suffix.len() as u64;
        let Some(body_at) = location.offset.checked_add(8) else {
            return Ok(false);
        };
        let Some(end) = body_at.checked_add(body_len) else {
            return Ok(false);
        };
        if length != 8 + body_len {
            return Ok(false);
        }

        let read = |bytes: &mut [u8], offset| {
            read_exact_at(source, bytes, offset).map_err(|error| io_error(source_path, error))
        };
        let mut header = [0; 8];
        read(&mut header, location.offset)?;
        if header[..4] != (body_len as u32).to_be_bytes() {
            return Ok(false);
        }
        if self.scratch.is_empty() {
            self.scratch.resize(COPY_BYTES, 0);
        }
        let payload_at = body_at + prefix.len() as u64;
        for (at, expected) in [(body_at, &prefix), (end - suffix.len() as u64, &suffix)] {
            let mut at = at;
            for part in expected.chunks(COPY_BYTES) {
                let observed = &mut self.scratch[..part.len()];
                read(observed, at)?;
                if &*observed != part {
                    return Ok(false);
                }
                at += part.len() as u64;
            }
        }

        let mut crc = crc32fast::Hasher::new();
        crc.update(&prefix);
        self.file
            .write_all(&header)
            .and_then(|_| self.file.write_all(&prefix))
            .map_err(|error| io_error(self.path, error))?;
        let mut copied = 0;
        while copied != payload_len {
            let n = (payload_len - copied).min(COPY_BYTES);
            let bytes = &mut self.scratch[..n];
            read(bytes, payload_at + copied as u64)?;
            crc.update(bytes);
            self.file
                .write_all(bytes)
                .map_err(|error| io_error(self.path, error))?;
            copied += n;
        }
        crc.update(&suffix);
        if header[4..] != crc.finalize().to_be_bytes() {
            return Err(corrupt(
                source_path,
                format!("no entry {lsn} at {}", location.offset),
            ));
        }
        self.file
            .write_all(&suffix)
            .map_err(|error| io_error(self.path, error))?;
        self.written += length;
        Ok(true)
    }
}

fn delimited(tag: u32, len: usize, output: &mut Vec<u8>) {
    encoding::encode_key(tag, encoding::WireType::LengthDelimited, output);
    encoding::encode_varint(len as u64, output);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_append_copies_the_original_framing_across_copy_chunks() {
        let payload = Bytes::from(vec![0x5a; 2 * COPY_BYTES + 11]);
        let record = Record {
            stream: 135,
            kind: Some(Kind::Append(crate::protocol::Entry {
                lsn: -3,
                epoch: 394,
                timestamp: -7,
                payload: payload.clone(),
                guarded: true,
                guarded_before: Some(200),
                ..crate::protocol::Entry::default()
            })),
            original_append: Some(true),
            ..Record::default()
        };
        let mut frames = FrameParts::default();
        frames.push_durable(&record);
        let mut expected = Vec::new();
        frames.write_to(&mut expected).unwrap();
        let mut source = tempfile::tempfile().unwrap();
        source.write_all(b"preceding record").unwrap();
        source.write_all(&expected).unwrap();
        let location = Location {
            segment: 1,
            offset: b"preceding record".len() as u64,
            length: expected.len() as u32,
            payload: payload.len() as u32,
        };
        let mut controls = record;
        let Some(Kind::Append(entry)) = &mut controls.kind else {
            unreachable!()
        };
        entry.payload = Bytes::new();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("compacting");
        let mut output = CompactionFrames::new(File::create(&path).unwrap(), &path);
        assert!(
            output
                .copy_canonical(&source, &path, location, controls)
                .unwrap()
        );
        assert_eq!(output.position(), expected.len() as u64);
        drop(output.finish().unwrap());
        assert_eq!(std::fs::read(&path).unwrap(), expected);
    }
}
