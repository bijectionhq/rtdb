use std::{collections::BTreeSet, ops::Range};

use rtdb_crypt::Plaintext;
use rtdb_format::journal::{ObjectId, ObjectKind};
use rtdb_index::branches::BranchCut;
use rtdb_tx::transact::TransactState;
use rtdb_types::Result;
use uuid::Uuid;

use super::{backup_too_large, invalid_backup};

const MAX_FILES: usize = 4_096;
const FILE_ID_BYTES: usize = 16 + 8 + 8 + 32;
const INDEX_HEADER: &[u8] = b"RTDBBFI\x01";
pub(super) const MAX_INDEX_BYTES: usize = INDEX_HEADER.len() + 4 + 2 * FILE_ID_BYTES * MAX_FILES + 4096;

pub(super) struct ArchivedFiles {
    plaintext: Plaintext,
    entries: Vec<(ObjectId, Range<usize>)>,
}

impl ArchivedFiles {
    pub(super) fn decode(plaintext: Plaintext, cut: BranchCut) -> Result<(TransactState, Self)> {
        let mut input: &[u8] = &plaintext.bytes;
        let checkpoint_len = read_len(&mut input)?;
        let count = read_count(&mut input)?;
        let transactions = TransactState::decode_binary_checkpoint(take(&mut input, checkpoint_len)?, cut)?;
        let source_database = Uuid::from_bytes(cut.basis.incarnation());
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let source = read_id(&mut input)?;
            if source.database != source_database || entries.last().is_some_and(|(prior, _)| *prior >= source) {
                return Err(invalid_backup());
            }
            let length = read_len(&mut input)?;
            let start = plaintext.bytes.len() - input.len();
            take(&mut input, length)?;
            entries.push((source, start..start + length));
        }
        if !input.is_empty() {
            return Err(invalid_backup());
        }
        Ok((transactions, Self { plaintext, entries }))
    }

    pub(super) fn entries(&self) -> impl ExactSizeIterator<Item = (ObjectId, &[u8])> + '_ {
        self.entries.iter().map(|(id, range)| (*id, &self.plaintext.bytes[range.clone()]))
    }
}

pub(super) fn archive_prefix(checkpoint: &[u8], count: usize) -> Result<Vec<u8>> {
    let mut output = Vec::with_capacity(8 + checkpoint.len());
    write_len(&mut output, checkpoint.len())?;
    write_count(&mut output, count)?;
    output.extend_from_slice(checkpoint);
    Ok(output)
}

pub(super) fn append_archive_file(output: &mut Vec<u8>, id: ObjectId, data: &[u8]) -> Result<()> {
    write_id(output, id);
    write_len(output, data.len())?;
    output.extend_from_slice(data);
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BackupFileRelocation {
    pub source_file: ObjectId,
    pub destination_file: ObjectId,
}

pub(super) fn encode_relocations(relocations: &[BackupFileRelocation]) -> Result<Vec<u8>> {
    let mut output = INDEX_HEADER.to_vec();
    write_count(&mut output, relocations.len())?;
    for relocation in relocations {
        write_id(&mut output, relocation.source_file);
        write_id(&mut output, relocation.destination_file);
    }
    Ok(output)
}

pub(super) fn decode_relocations(
    input: &[u8], source_database: Uuid, index_id: ObjectId,
) -> Result<Vec<BackupFileRelocation>> {
    let mut input = input.strip_prefix(INDEX_HEADER).ok_or_else(invalid_backup)?;
    let count = read_count(&mut input)?;
    let mut relocations: Vec<BackupFileRelocation> = Vec::with_capacity(count);
    let mut destinations = BTreeSet::new();
    for _ in 0..count {
        let source_file = read_id(&mut input)?;
        let destination_file = read_id(&mut input)?;
        if source_file.database != source_database
            || destination_file.database != index_id.database
            || destination_file.epoch != index_id.epoch
            || destination_file.seq >= index_id.seq
            || relocations.last().is_some_and(|prior| prior.source_file >= source_file)
            || !destinations.insert(destination_file)
        {
            return Err(invalid_backup());
        }
        relocations.push(BackupFileRelocation { source_file, destination_file });
    }
    if !input.is_empty() {
        return Err(invalid_backup());
    }
    Ok(relocations)
}

fn write_id(output: &mut Vec<u8>, id: ObjectId) {
    output.extend_from_slice(id.database.as_bytes());
    output.extend_from_slice(&id.epoch.to_be_bytes());
    output.extend_from_slice(&id.seq.to_be_bytes());
    output.extend_from_slice(&id.digest);
}

fn read_id(input: &mut &[u8]) -> Result<ObjectId> {
    let (id, rest) = input.split_first_chunk::<FILE_ID_BYTES>().ok_or_else(invalid_backup)?;
    *input = rest;
    Ok(ObjectId {
        database: Uuid::from_bytes(id[..16].try_into().unwrap()),
        kind: ObjectKind::File,
        epoch: u64::from_be_bytes(id[16..24].try_into().unwrap()),
        seq: u64::from_be_bytes(id[24..32].try_into().unwrap()),
        digest: id[32..].try_into().unwrap(),
    })
}

fn write_len(output: &mut Vec<u8>, length: usize) -> Result<()> {
    let length = u32::try_from(length).map_err(|_| backup_too_large())?;
    output.extend_from_slice(&length.to_be_bytes());
    Ok(())
}

fn read_len(input: &mut &[u8]) -> Result<usize> {
    let (length, rest) = input.split_first_chunk::<4>().ok_or_else(invalid_backup)?;
    *input = rest;
    Ok(u32::from_be_bytes(*length) as usize)
}

fn write_count(output: &mut Vec<u8>, count: usize) -> Result<()> {
    if !(1..=MAX_FILES).contains(&count) {
        return Err(backup_too_large());
    }
    write_len(output, count)
}

fn read_count(input: &mut &[u8]) -> Result<usize> {
    let count = read_len(input)?;
    if !(1..=MAX_FILES).contains(&count) {
        return Err(invalid_backup());
    }
    Ok(count)
}

fn take<'a>(input: &mut &'a [u8], length: usize) -> Result<&'a [u8]> {
    let (value, rest) = input.split_at_checked(length).ok_or_else(invalid_backup)?;
    *input = rest;
    Ok(value)
}
