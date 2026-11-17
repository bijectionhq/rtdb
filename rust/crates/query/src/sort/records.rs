use std::sync::Arc;

use rtdb_columnar::{MemoryBudget,MemoryReservation};
use rtdb_types::{Result,Value};

use crate::{QueryContext,cursor::OwnedRow,invalid,pages::{OwnedPage,PageBuilder},spill::{SpillFile,SpillReader,SpillWriter}};

const MAX_RECORDS: usize = i32::MAX as usize;

pub(crate) struct RecordBuffer { context: Arc<QueryContext>,state: BufferState }
enum BufferState {
    Memory { rows: PageBuilder,bytes: usize },
    Disk { writer: SpillWriter,offsets: RecordOffsets },
    Failed,
}
impl RecordBuffer {
    pub fn new(context: Arc<QueryContext>) -> Result<Self> {
        context.check_cancelled()?;
        let rows = PageBuilder::new(0,&context.memory)?;
        Ok(Self { context,state: BufferState::Memory { rows,bytes: 0 } })
    }
    pub fn len(&self) -> usize {
        match &self.state { BufferState::Memory { rows,.. } => rows.len(),BufferState::Disk { offsets,.. } => offsets.values.len(),BufferState::Failed => 0 }
    }
    pub fn push(&mut self,row: OwnedRow) -> Result<()> {
        let state = std::mem::replace(&mut self.state,BufferState::Failed);
        self.state = self.append(state,row)?;
        Ok(())
    }
    fn append(&self,state: BufferState,row: OwnedRow) -> Result<BufferState> {
        self.context.check_cancelled()?;
        match state {
            BufferState::Memory { mut rows,bytes } => {
                checked_count(rows.len(),1)?;
                let bytes = bytes.checked_add(row.bytes()).ok_or_else(||invalid("sort-size","Sort record bytes overflow"))?;
                if bytes >= self.context.options.sort_memory_bytes {
                    let mut writer = self.context.spill.create()?;
                    let mut offsets = RecordOffsets::new(&self.context.memory)?;
                    let mut rows = rows.finish();
                    while let Some(row) = rows.next() { append_disk(&mut writer,&mut offsets,row,&self.context)?; }
                    append_disk(&mut writer,&mut offsets,row,&self.context)?;
                    Ok(BufferState::Disk { writer,offsets })
                } else {
                    if rows.len() == 0 || rows.len().is_power_of_two() {
                        let capacity = next_capacity(rows.len())?;
                        let mut grown = PageBuilder::new(capacity,&self.context.memory)?;
                        let mut previous = rows.finish();
                        while let Some(row) = previous.next() { self.context.check_cancelled()?;grown.push(row)?; }
                        rows = grown;
                    }
                    rows.push(row)?;
                    Ok(BufferState::Memory { rows,bytes })
                }
            }
            BufferState::Disk { mut writer,mut offsets } => {
                append_disk(&mut writer,&mut offsets,row,&self.context)?;
                Ok(BufferState::Disk { writer,offsets })
            }
            BufferState::Failed => Err(failed_buffer()),
        }
    }
    pub fn finish(self) -> Result<RecordStore> {
        self.context.check_cancelled()?;
        let backing = match self.state {
            BufferState::Memory { rows,.. } => RecordBacking::Memory(rows.finish()),
            BufferState::Disk { writer,offsets } => RecordBacking::Disk { file: writer.finish()?,offsets },
            BufferState::Failed => return Err(failed_buffer()),
        };
        Ok(RecordStore { context: self.context,backing })
    }
}

struct RecordOffsets { values: Vec<u64>,_reservation: Arc<MemoryReservation> }
impl RecordOffsets {
    fn new(memory: &Arc<MemoryBudget>) -> Result<Self> { Ok(Self { values: Vec::new(),_reservation: memory.reserve(256)? }) }
    fn reserve_next(&mut self,memory: &Arc<MemoryBudget>) -> Result<()> {
        checked_count(self.values.len(),1)?;
        if self.values.len() < self.values.capacity() { return Ok(()); }
        let capacity = next_capacity(self.values.len())?;
        let bytes = capacity.checked_mul(std::mem::size_of::<u64>()).and_then(|bytes|bytes.checked_add(256)).ok_or_else(||invalid("sort-size","Sort offset index capacity overflow"))?;
        let reservation = memory.reserve(bytes)?;
        self.values.reserve_exact(capacity - self.values.len());
        self._reservation = reservation;
        Ok(())
    }
}
fn append_disk(writer: &mut SpillWriter,offsets: &mut RecordOffsets,row: OwnedRow,context: &QueryContext) -> Result<()> {
    context.check_cancelled()?;
    offsets.reserve_next(&context.memory)?;
    let offset = writer.position()?;
    writer.write(&row.values,&context.memory)?;
    offsets.values.push(offset);
    Ok(())
}

pub(crate) struct RecordStore { context: Arc<QueryContext>,backing: RecordBacking }
enum RecordBacking { Memory(OwnedPage),Disk { file: Arc<SpillFile>,offsets: RecordOffsets } }
impl RecordStore {
    pub fn len(&self) -> usize {
        match &self.backing { RecordBacking::Memory(rows) => rows.len(),RecordBacking::Disk { offsets,.. } => offsets.values.len() }
    }
    pub fn access(&self) -> Result<RecordAccess<'_>> {
        self.context.check_cancelled()?;
        let backing = match &self.backing {
            RecordBacking::Memory(rows) => AccessBacking::Memory(rows.rows()),
            RecordBacking::Disk { file,offsets } => AccessBacking::Disk { reader: file.reader(Arc::clone(&self.context.memory))?,offsets: &offsets.values },
        };
        Ok(RecordAccess { context: &self.context,backing })
    }
    pub fn select(&self,indices: &[usize],context: Arc<QueryContext>) -> Result<Self> {
        checked_count(indices.len(),0)?;
        let mut selected = RecordBuffer::new(context)?;
        let mut access = self.access()?;
        for index in indices {
            selected.context.check_cancelled()?;
            let row = access.get(*index)?;
            let row = OwnedRow::copy_parts(&[row.values()],0,0,&selected.context.memory)?;
            selected.push(row)?;
        }
        selected.finish()
    }
    pub fn into_cursor(self) -> Result<RecordCursor> {
        self.context.check_cancelled()?;
        let backing = match self.backing {
            RecordBacking::Memory(rows) => CursorBacking::Memory(rows),
            RecordBacking::Disk { file,offsets } => {
                drop(offsets);
                CursorBacking::Disk(file.reader(Arc::clone(&self.context.memory))?)
            }
        };
        Ok(RecordCursor { context: self.context,backing: Some(backing) })
    }
}

pub(crate) struct RecordAccess<'store> { context: &'store QueryContext,backing: AccessBacking<'store> }
enum AccessBacking<'store> { Memory(&'store [OwnedRow]),Disk { reader: SpillReader,offsets: &'store [u64] } }
impl<'store> RecordAccess<'store> {
    pub fn get(&mut self,index: usize) -> Result<RecordRow<'store>> {
        self.context.check_cancelled()?;
        match &mut self.backing {
            AccessBacking::Memory(rows) => Ok(RecordRow::Borrowed((*rows).get(index).ok_or_else(record_index)?)),
            AccessBacking::Disk { reader,offsets } => {
                reader.seek(*offsets.get(index).ok_or_else(record_index)?)?;
                Ok(RecordRow::Owned(reader.next()?.ok_or_else(||invalid("invalid-spill","Sort record offset has no row"))?))
            }
        }
    }
}
pub(crate) enum RecordRow<'store> { Borrowed(&'store OwnedRow),Owned(OwnedRow) }
impl RecordRow<'_> {
    pub fn values(&self) -> &[Value] { match self { Self::Borrowed(row) => &row.values,Self::Owned(row) => &row.values } }
}

pub(crate) struct RecordCursor { context: Arc<QueryContext>,backing: Option<CursorBacking> }
enum CursorBacking { Memory(OwnedPage),Disk(SpillReader) }
impl RecordCursor {
    pub fn next(&mut self) -> Result<Option<OwnedRow>> {
        let Some(mut backing) = self.backing.take() else { return Ok(None); };
        self.context.check_cancelled()?;
        let row = match &mut backing { CursorBacking::Memory(rows) => rows.next(),CursorBacking::Disk(reader) => reader.next()? };
        if row.is_some() { self.backing = Some(backing); }
        Ok(row)
    }
}

fn checked_count(current: usize,additional: usize) -> Result<usize> {
    current.checked_add(additional).filter(|count|*count <= MAX_RECORDS).ok_or_else(||invalid("sort-size","Sort record count exceeds the signed array limit"))
}
fn next_capacity(current: usize) -> Result<usize> {
    checked_count(current,1)?;
    Ok(current.saturating_mul(2).max(1).min(MAX_RECORDS))
}
fn record_index() -> rtdb_types::Anomaly { invalid("sort-index","Sort record index is outside its backing") }
fn failed_buffer() -> rtdb_types::Anomaly { invalid("sort-state","Sort record buffer already failed") }

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use rtdb_storage::{MemoryStore,cache::BufferPool};
    use rtdb_types::Timestamp;
    use tokio_util::sync::CancellationToken;
    use crate::{CatalogSnapshot,DatabaseSnapshot,QueryOptions};

    fn context(threshold: usize,spill_limit: u64) -> Arc<QueryContext> {
        let database = DatabaseSnapshot { partitions: vec![],storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()),1 << 20)) };
        let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),database)]),false).unwrap();
        QueryContext::new(catalog,MemoryBudget::new(1 << 25),Timestamp(0),"UTC",QueryOptions { sort_memory_bytes: threshold,spill_limit_bytes: spill_limit,..Default::default() },&CancellationToken::new()).unwrap()
    }
    fn row(value: i64,context: &QueryContext) -> OwnedRow { OwnedRow::new(vec![Value::I64(value)],&context.memory).unwrap() }
    fn store(values: &[i64],context: &Arc<QueryContext>) -> RecordStore {
        let mut buffer = RecordBuffer::new(Arc::clone(context)).unwrap();
        for value in values { buffer.push(row(*value,context)).unwrap(); }
        assert_eq!(buffer.len(),values.len());
        buffer.finish().unwrap()
    }

    #[test]
    fn physical_spill_preserves_unsorted_records_and_independent_indexed_reads() {
        for threshold in [1,usize::MAX] {
            let context = context(threshold,1 << 20);let baseline = context.memory.used_bytes();
            let store = store(&[3,1,2],&context);assert_eq!(store.len(),3);
            assert_eq!(matches!(&store.backing,RecordBacking::Disk { .. }),threshold == 1);
            let mut access = store.access().unwrap();
            let third = access.get(2).unwrap();let first = access.get(0).unwrap();
            assert_eq!(third.values(),[Value::I64(2)]);assert_eq!(first.values(),[Value::I64(3)]);
            assert_eq!(access.get(1).unwrap().values(),[Value::I64(1)]);
            assert_eq!(access.get(3).err().unwrap().code,"rtdb.query/sort-index");
            drop(first);drop(third);drop(access);drop(store);
            assert_eq!(context.memory.used_bytes(),baseline);assert_eq!(context.spill_bytes(),0);
        }
    }

    #[test]
    fn threshold_equality_spills_after_preserving_earlier_rows() {
        let probe = context(usize::MAX,1 << 20);let bytes = row(1,&probe).bytes();
        let context = context(bytes * 2,1 << 20);let baseline = context.memory.used_bytes();
        let mut buffer = RecordBuffer::new(Arc::clone(&context)).unwrap();
        buffer.push(row(9,&context)).unwrap();assert!(matches!(&buffer.state,BufferState::Memory { .. }));
        buffer.push(row(4,&context)).unwrap();assert!(matches!(&buffer.state,BufferState::Disk { .. }));
        let mut cursor = buffer.finish().unwrap().into_cursor().unwrap();
        assert_eq!(cursor.next().unwrap().unwrap().values,[Value::I64(9)]);
        assert_eq!(cursor.next().unwrap().unwrap().values,[Value::I64(4)]);
        assert!(cursor.next().unwrap().is_none());drop(cursor);
        assert_eq!(context.memory.used_bytes(),baseline);assert_eq!(context.spill_bytes(),0);
    }

    #[test]
    fn selection_retains_requested_order_and_duplicates_across_backings() {
        for source_threshold in [1,usize::MAX] { for target_threshold in [1,usize::MAX] {
            let source_context = context(source_threshold,1 << 20);let target_context = context(target_threshold,1 << 20);
            let source_baseline = source_context.memory.used_bytes();let target_baseline = target_context.memory.used_bytes();
            let source = store(&[8,4,6],&source_context);
            let indices = [2,0,2,1];
            let selected = source.select(&indices,Arc::clone(&target_context)).unwrap();
            assert_eq!(selected.len(),4);drop(source);
            assert_eq!(source_context.memory.used_bytes(),source_baseline);assert_eq!(source_context.spill_bytes(),0);
            let mut cursor = selected.into_cursor().unwrap();
            for value in [6,8,6,4] { assert_eq!(cursor.next().unwrap().unwrap().values,[Value::I64(value)]); }
            assert!(cursor.next().unwrap().is_none());drop(cursor);
            assert_eq!(target_context.memory.used_bytes(),target_baseline);assert_eq!(target_context.spill_bytes(),0);
        } }
    }

    #[test]
    fn sequential_cursor_releases_disk_index_and_returned_row_keeps_its_charge() {
        for threshold in [1,usize::MAX] {
            let context = context(threshold,1 << 20);let baseline = context.memory.used_bytes();
            let store = store(&[7,8],&context);let before_cursor = context.memory.used_bytes();
            let mut cursor = store.into_cursor().unwrap();
            if threshold == 1 { assert!(context.memory.used_bytes() < before_cursor);assert!(context.spill_bytes() > 0); }
            let row = cursor.next().unwrap().unwrap();drop(cursor);
            assert_eq!(row.values,[Value::I64(7)]);assert!(context.memory.used_bytes() > baseline);assert_eq!(context.spill_bytes(),0);
            drop(row);assert_eq!(context.memory.used_bytes(),baseline);
        }
    }

    #[test]
    fn failed_spill_append_is_terminal_and_releases_unpublished_records() {
        let context = context(1,1);let baseline = context.memory.used_bytes();
        let mut buffer = RecordBuffer::new(Arc::clone(&context)).unwrap();
        assert_eq!(buffer.push(row(1,&context)).unwrap_err().code,"rtdb.query/spill-limit");
        assert_eq!(buffer.len(),0);assert_eq!(context.memory.used_bytes(),baseline);assert_eq!(context.spill_bytes(),0);
        assert_eq!(buffer.push(row(2,&context)).unwrap_err().code,"rtdb.query/sort-state");
        assert_eq!(buffer.finish().err().unwrap().code,"rtdb.query/sort-state");
        assert_eq!(context.memory.used_bytes(),baseline);
    }

    #[test]
    fn container_growth_admission_failure_releases_memory_and_disk_backing() {
        for threshold in [1,usize::MAX] {
            let context = context(threshold,1 << 20);let baseline = context.memory.used_bytes();
            let mut buffer = RecordBuffer::new(Arc::clone(&context)).unwrap();buffer.push(row(1,&context)).unwrap();
            let incoming = row(2,&context);
            let filler = context.memory.reserve(context.memory.limit_bytes() - context.memory.used_bytes() - 1).unwrap();
            assert_eq!(buffer.push(incoming).unwrap_err().code,"rtdb/memory-budget-exceeded");
            assert_eq!(buffer.len(),0);assert_eq!(context.spill_bytes(),0);
            assert_eq!(context.memory.used_bytes(),baseline + filler.bytes());
            drop(filler);drop(buffer);assert_eq!(context.memory.used_bytes(),baseline);
        }
    }

    #[test]
    fn spill_round_trip_preserves_nonfinite_and_signed_zero_record_bits() {
        let context = context(1,1 << 20);let baseline = context.memory.used_bytes();
        let bits = [f64::NEG_INFINITY.to_bits(),(-0.0f64).to_bits(),0x7ff8_0000_0000_0042,f64::INFINITY.to_bits()];
        let mut buffer = RecordBuffer::new(Arc::clone(&context)).unwrap();
        for value in bits { buffer.push(OwnedRow::new(vec![Value::F64(f64::from_bits(value))],&context.memory).unwrap()).unwrap(); }
        let store = buffer.finish().unwrap();let mut access = store.access().unwrap();
        for (index,expected) in bits.into_iter().enumerate() {
            let value = access.get(index).unwrap();
            let [Value::F64(value)] = value.values() else { panic!("Expected double record"); };
            assert_eq!(value.to_bits(),expected);
        }
        drop(access);drop(store);assert_eq!(context.memory.used_bytes(),baseline);assert_eq!(context.spill_bytes(),0);
    }

    #[test]
    fn failed_selection_releases_only_its_partial_target() {
        let context = context(1,1 << 20);let baseline = context.memory.used_bytes();
        let source = store(&[5,6],&context);let retained = context.memory.used_bytes();let spill = context.spill_bytes();
        assert_eq!(source.select(&[0,3],Arc::clone(&context)).err().unwrap().code,"rtdb.query/sort-index");
        assert_eq!(context.memory.used_bytes(),retained);assert_eq!(context.spill_bytes(),spill);
        assert_eq!(source.access().unwrap().get(1).unwrap().values(),[Value::I64(6)]);
        drop(source);assert_eq!(context.memory.used_bytes(),baseline);assert_eq!(context.spill_bytes(),0);
    }

    #[test]
    fn cancellation_drops_buffer_and_cursor_backings_on_the_next_call() {
        for threshold in [1,usize::MAX] {
            let context = context(threshold,1 << 20);let baseline = context.memory.used_bytes();
            let mut buffer = RecordBuffer::new(Arc::clone(&context)).unwrap();buffer.push(row(1,&context)).unwrap();
            context.cancellation.cancel();
            assert_eq!(buffer.push(row(2,&context)).unwrap_err().code,"xtdb/query-cancelled");
            assert_eq!(context.memory.used_bytes(),baseline);assert_eq!(context.spill_bytes(),0);
            let context = self::context(threshold,1 << 20);let baseline = context.memory.used_bytes();
            let mut cursor = store(&[3,4],&context).into_cursor().unwrap();context.cancellation.cancel();
            assert_eq!(cursor.next().err().unwrap().code,"xtdb/query-cancelled");
            assert_eq!(context.memory.used_bytes(),baseline);assert_eq!(context.spill_bytes(),0);
            assert!(cursor.next().unwrap().is_none());
        }
    }

    #[test]
    fn empty_store_and_empty_selection_do_not_open_spill_files() {
        let context = context(1,0);let baseline = context.memory.used_bytes();
        let empty = store(&[],&context);assert_eq!(empty.len(),0);assert_eq!(context.spill_bytes(),0);
        let selected = empty.select(&[],Arc::clone(&context)).unwrap();
        let mut cursor = selected.into_cursor().unwrap();assert!(cursor.next().unwrap().is_none());drop(cursor);drop(empty);
        assert_eq!(context.memory.used_bytes(),baseline);
    }

    #[test]
    fn container_growth_is_capped_before_signed_array_limit() {
        assert_eq!(next_capacity(0).unwrap(),1);
        assert_eq!(next_capacity(1 << 30).unwrap(),MAX_RECORDS);
        assert!(next_capacity(MAX_RECORDS).is_err());
        assert_eq!(checked_count(MAX_RECORDS,0).unwrap(),MAX_RECORDS);
        assert!(checked_count(MAX_RECORDS,1).is_err());
        assert!(checked_count(usize::MAX,1).is_err());
    }
}
