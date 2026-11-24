use std::fs::File;
use std::ops::Range;
use std::os::unix::fs::MetadataExt;
use std::sync::Arc;

use bytes::Bytes;
use memmap2::{Mmap, MmapOptions};
use rtdb_columnar::MemoryReservation;

use super::{COPY_INPUT_SCRATCH_BYTES, COPY_RETAINED_STREAM_BYTES};

// The stream header cache can coexist with this mapping in the input scratch charge.
const WHOLE_SPOOL_BYTES: usize = COPY_INPUT_SCRATCH_BYTES / 4;
const MAX_FRAME_BYTES: usize = COPY_RETAINED_STREAM_BYTES;

struct MappedBytes {
    mapping: Mmap,
    _charge: Arc<MemoryReservation>,
    _frame_slot: Option<Arc<()>>,
}

impl AsRef<[u8]> for MappedBytes {
    fn as_ref(&self) -> &[u8] {
        &self.mapping
    }
}

/// The file must remain immutable after this call, including while any returned view is live.
pub(super) unsafe fn retained_ranges(
    file: &File,
    length: u64,
    input: &Arc<MemoryReservation>,
    decoder: &Arc<MemoryReservation>,
) -> Option<impl FnMut(Range<u64>) -> Option<Bytes> + use<>> {
    let metadata = file.metadata().ok()?;
    if metadata.nlink() != 0 || metadata.len() != length || length == 0 {
        return None;
    }
    let file = file.try_clone().ok()?;
    let whole = if length <= WHOLE_SPOOL_BYTES as u64 {
        map(&file, 0, length as usize, Arc::clone(input), None)
    } else {
        None
    };
    let decoder = Arc::clone(decoder);
    let slot = Arc::new(());
    Some(move |range: Range<u64>| {
        let size = usize::try_from(range.end.checked_sub(range.start)?).ok()?;
        if range.end > length
            || size == 0
            || size > MAX_FRAME_BYTES
            || range.start % std::mem::align_of::<u128>() as u64 != 0
        {
            return None;
        }
        if let Some(whole) = &whole {
            return Some(whole.slice(range.start as usize..range.end as usize));
        }
        // The decoder charge covers one record frame. An outstanding view holds that slot.
        if Arc::strong_count(&slot) != 1 {
            return None;
        }
        map(
            &file,
            range.start,
            size,
            Arc::clone(&decoder),
            Some(Arc::clone(&slot)),
        )
    })
}

fn map(
    file: &File,
    offset: u64,
    length: usize,
    charge: Arc<MemoryReservation>,
    frame_slot: Option<Arc<()>>,
) -> Option<Bytes> {
    // SAFETY: COPY has consumed the writer, the spool is unlinked, and its checked length is final.
    let mapping = unsafe { MmapOptions::new().offset(offset).len(length).map(file) }.ok()?;
    Some(Bytes::from_owner(MappedBytes {
        mapping,
        _charge: charge,
        _frame_slot: frame_slot,
    }))
}
