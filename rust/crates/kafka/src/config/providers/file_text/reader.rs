use std::{fs::File, io::{ErrorKind, Read}, mem::size_of};

use rtdb_types::Result;

use super::super::invalid;

const WINDOW: usize = 8192;
pub(super) const SCRATCH_BYTES: usize = 4096 + WINDOW * (1 + 2 * size_of::<u16>());

struct Decoder {
    file: File,
    bytes: [u8; WINDOW],
    start: usize,
    end: usize,
    file_offset: u64,
}

impl Decoder {
    fn available(&self) -> bool {
        self.file.metadata().is_ok_and(|metadata| metadata.len() > self.file_offset)
    }

    fn ready(&self) -> bool { self.start < self.end || self.available() }

    fn refill(&mut self) -> Result<bool> {
        self.bytes.copy_within(self.start..self.end, 0);
        self.end -= self.start;
        self.start = 0;
        let length = loop {
            match self.file.read(&mut self.bytes[self.end..]) {
                Ok(length) => break length,
                Err(error) if error.kind() == ErrorKind::Interrupted => continue,
                Err(_) => return Err(invalid("Kafka ConfigProvider file could not be read")),
            }
        };
        self.end += length;
        self.file_offset = self.file_offset.saturating_add(length as u64);
        Ok(length != 0)
    }

    fn read(&mut self, output: &mut [u16; WINDOW]) -> Result<usize> {
        let mut written = 0;
        let mut eof = false;
        loop {
            let bytes = &self.bytes[self.start..self.end];
            match decode(bytes, output.len() - written)? {
                Step::Scalar(scalar, width) => {
                    self.start += width;
                    written += scalar.encode_utf16(&mut output[written..]).len();
                }
                Step::Full => return Ok(written),
                Step::Refill if eof => {
                    if bytes.is_empty() { return Ok(written); }
                    return Err(malformed());
                }
                Step::Refill => {
                    if written == output.len() || (written != 0 && !self.available()) {
                        return Ok(written);
                    }
                    eof = !self.refill()?;
                }
            }
        }
    }
}

enum Step { Scalar(char, usize), Refill, Full }

fn malformed() -> rtdb_types::Anomaly { invalid("Kafka ConfigProvider file is not valid UTF-8") }

fn decode(input: &[u8], remaining_units: usize) -> Result<Step> {
    let Some(&first) = input.first() else { return Ok(Step::Refill); };
    let width = match first {
        0x00..=0x7f => 1,
        0xc2..=0xdf => 2,
        0xe0..=0xef => 3,
        0xf0..=0xf7 => 4,
        _ => return Err(malformed()),
    };
    let units = if width == 4 { 2 } else { 1 };
    if input.len() < width || remaining_units < units {
        if width == 3 && input.get(1).is_some_and(|second| {
            !(0x80..=0xbf).contains(second) || first == 0xe0 && *second < 0xa0
        }) {
            return Err(malformed());
        }
        if width == 4 && (first > 0xf4 || input.get(1).is_some_and(|second| {
            !(0x80..=0xbf).contains(second) || first == 0xf0 && *second < 0x90 || first == 0xf4 && *second > 0x8f
        }) || input.get(2).is_some_and(|third| !(0x80..=0xbf).contains(third))) {
            return Err(malformed());
        }
        return Ok(if input.len() < width { Step::Refill } else { Step::Full });
    }
    let scalar = std::str::from_utf8(&input[..width]).map_err(|_| malformed())?
        .chars().next().expect("decoder supplied a nonempty UTF-8 scalar");
    Ok(Step::Scalar(scalar, width))
}

pub(super) struct Reader {
    decoder: Decoder,
    buffered: [u16; WINDOW],
    buffered_at: usize,
    buffered_len: usize,
    current: [u16; WINDOW],
    current_at: usize,
    current_len: usize,
}

impl Reader {
    pub(super) fn new(file: File) -> Self {
        Self { decoder: Decoder { file, bytes: [0; WINDOW], start: 0, end: 0, file_offset: 0 },
            buffered: [0; WINDOW], buffered_at: 0, buffered_len: 0,
            current: [0; WINDOW], current_at: 0, current_len: 0 }
    }

    fn copy_buffered(&mut self) {
        let count = (self.buffered_len - self.buffered_at).min(WINDOW - self.current_len);
        self.current[self.current_len..self.current_len + count]
            .copy_from_slice(&self.buffered[self.buffered_at..self.buffered_at + count]);
        self.buffered_at += count;
        self.current_len += count;
    }

    fn refill(&mut self) -> Result<()> {
        self.current_at = 0;
        self.current_len = 0;
        if self.buffered_at < self.buffered_len {
            self.copy_buffered();
        } else {
            self.current_len = self.decoder.read(&mut self.current)?;
        }
        while self.current_len > 0 && self.current_len < WINDOW && self.decoder.ready() {
            if self.buffered_at == self.buffered_len {
                self.buffered_at = 0;
                self.buffered_len = self.decoder.read(&mut self.buffered)?;
                if self.buffered_len == 0 { break; }
            }
            self.copy_buffered();
        }
        Ok(())
    }

    pub(super) fn next_unit(&mut self) -> Result<Option<u16>> {
        if self.current_at == self.current_len { self.refill()?; }
        if self.current_at == self.current_len { return Ok(None); }
        let unit = self.current[self.current_at];
        self.current_at += 1;
        Ok(Some(unit))
    }

}
