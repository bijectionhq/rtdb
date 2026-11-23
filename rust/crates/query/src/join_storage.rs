use std::sync::Arc;
use rtdb_columnar::{MemoryBudget,MemoryReservation};
use rtdb_types::{Result,Value};
use crate::{QueryContext,cursor::OwnedRow,invalid,pages::{OwnedPage,PageBuilder},spill::{SpillFile,SpillReader,SpillWriter}};

pub(crate) fn hash_values<'a>(values: impl IntoIterator<Item=&'a Value>) -> Result<u32> {
    values.into_iter().try_fold(0u32,|hash,value| Ok(combine_cell_hash(hash,rtdb_columnar::hash_value(value,0)?)))
}
/// Folds one cell's `hash_value` into a row hash, as `hash_values` does for each of a row's cells in turn.
pub(crate) fn combine_cell_hash(hash: u32,cell: i32) -> u32 {
    let cell = (cell as u32).wrapping_mul(0xcc9e_2d51).rotate_left(15).wrapping_mul(0x1b87_3593);
    (hash ^ cell).rotate_left(13).wrapping_mul(5).wrapping_add(0xe654_6b64)
}

pub(crate) enum BuildInput { Memory(OwnedPage),Spilled(BlockSpool) }
pub(crate) struct BuildAccumulator {
    page: PageBuilder,writer: Option<SpillWriter>,blocks: usize,rows: u64,threshold: i64,context: Arc<QueryContext>,
}
impl BuildAccumulator {
    pub fn new(threshold: i64,context: Arc<QueryContext>) -> Result<Self> {
        Ok(Self { page: PageBuilder::new(0,&context.memory)?,writer: None,blocks: 0,rows: 0,threshold,context })
    }
    pub fn append(&mut self,mut page: OwnedPage) -> Result<()> {
        self.context.check_cancelled()?;
        while let Some(row) = page.next() { self.page.push_growing(row,&self.context.memory)?; }
        let count = i64::try_from(self.page.len()).map_err(|_|size_error())?;
        if count > self.threshold {
            if self.writer.is_none() { self.writer = Some(self.context.spill.create()?); }
            self.flush()?;
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<()> {
        if self.page.len() == 0 { return Ok(()); }
        let page = std::mem::replace(&mut self.page,PageBuilder::new(0,&self.context.memory)?).finish();
        let count = i64::try_from(page.len()).map_err(|_|size_error())?;
        let writer = self.writer.as_mut().ok_or_else(||invalid("join-state","Join spill writer is absent"))?;
        writer.write(&[Value::I64(count)],&self.context.memory)?;
        for row in page.rows() {
            self.context.check_cancelled()?;
            writer.write(&row.values,&self.context.memory)?;
        }
        self.rows = self.rows.checked_add(count as u64).filter(|rows|*rows <= i64::MAX as u64).ok_or_else(size_error)?;
        self.blocks = self.blocks.checked_add(1).filter(|blocks|*blocks <= i32::MAX as usize).ok_or_else(size_error)?;
        Ok(())
    }
    pub fn finish(mut self) -> Result<BuildInput> {
        if self.writer.is_none() { return Ok(BuildInput::Memory(self.page.finish())); }
        self.flush()?;
        let writer = self.writer.take().ok_or_else(||invalid("join-state","Join spill writer is absent"))?;
        Ok(BuildInput::Spilled(BlockSpool { file: writer.finish()?,rows: self.rows,blocks: self.blocks }))
    }
    pub fn finish_spilled(mut self) -> Result<BlockSpool> {
        if self.writer.is_none() { self.writer = Some(self.context.spill.create()?); }
        match self.finish()? { BuildInput::Spilled(spool) => Ok(spool),BuildInput::Memory(_) => unreachable!() }
    }
}

pub(crate) struct BlockSpool { file: Arc<SpillFile>,pub rows: u64,pub blocks: usize }
struct BlockReader { reader: SpillReader,context: Arc<QueryContext> }
impl BlockSpool {
    fn reader(&self,context: &Arc<QueryContext>) -> Result<BlockReader> {
        Ok(BlockReader { reader: self.file.reader(Arc::clone(&context.memory))?,context: Arc::clone(context) })
    }
}
impl BlockReader {
    fn next(&mut self) -> Result<Option<OwnedPage>> {
        self.context.check_cancelled()?;
        let Some(header) = self.reader.next()? else { return Ok(None); };
        let [Value::I64(count)] = header.values.as_slice() else { return Err(invalid("join-spill","Invalid join block header")); };
        let count = usize::try_from(*count).map_err(|_|invalid("join-spill","Invalid join block row count"))?;
        drop(header);
        let mut page = PageBuilder::new(count,&self.context.memory)?;
        for _ in 0..count {
            self.context.check_cancelled()?;
            page.push(self.reader.next()?.ok_or_else(||invalid("join-spill","Join block ended before its row count"))?)?;
        }
        Ok(Some(page.finish()))
    }
}

pub(crate) struct HashedPage { pub page: OwnedPage,hashes: Vec<u32>,_reservation: Arc<MemoryReservation> }
impl HashedPage {
    pub fn new(page: OwnedPage,hash: impl Fn(&OwnedRow)->Result<u32>,memory: &Arc<MemoryBudget>) -> Result<Self> {
        let reservation = memory.reserve(page.len().checked_mul(std::mem::size_of::<u32>()).and_then(|bytes|bytes.checked_add(256)).ok_or_else(size_error)?)?;
        let _scratch = memory.reserve(4096)?;
        let mut hashes = Vec::with_capacity(page.len());
        for row in page.rows() { hashes.push(hash(row)?); }
        Ok(Self { page,hashes,_reservation: reservation })
    }
    pub fn hash(&self,index: usize) -> Result<u32> { self.hashes.get(index).copied().ok_or_else(||invalid("join-hash","Join hash cell is absent")) }
}

#[derive(Clone,Copy)]
struct Span { offset: u64,rows: usize }
pub(crate) struct PartitionSpool { file: Arc<SpillFile>,parts: Vec<Vec<Span>>,_reservation: Arc<MemoryReservation> }
impl PartitionSpool {
    pub fn build(blocks: BlockSpool,minimum_parts: usize,hash: impl Fn(&OwnedRow)->Result<u32>,context: &Arc<QueryContext>) -> Result<Self> {
        let part_count = partition_count(blocks.blocks,minimum_parts)?;
        let rows = usize::try_from(blocks.rows).map_err(|_|size_error())?;
        let bytes = part_count.checked_mul(std::mem::size_of::<Vec<Span>>()).and_then(|bytes|rows.checked_mul(std::mem::size_of::<Span>() * 8).and_then(|spans|bytes.checked_add(spans))).and_then(|bytes|bytes.checked_add(4096)).ok_or_else(size_error)?;
        let reservation = context.memory.reserve(bytes)?;
        let mut parts = vec![Vec::new();part_count];
        let mut writer = context.spill.create()?;
        let mut reader = blocks.reader(context)?;
        while let Some(page) = reader.next()? {
            context.check_cancelled()?;
            let page = HashedPage::new(page,&hash,&context.memory)?;
            let selection_bytes = part_count.checked_mul(std::mem::size_of::<Vec<usize>>()).and_then(|bytes|page.page.len().checked_mul(std::mem::size_of::<usize>() * 8).and_then(|indices|bytes.checked_add(indices))).and_then(|bytes|bytes.checked_add(4096)).ok_or_else(size_error)?;
            let _selection = context.memory.reserve(selection_bytes)?;
            let mut selections = vec![Vec::new();part_count];
            for (index,hash) in page.hashes.iter().enumerate() { selections[*hash as usize & (part_count - 1)].push(index); }
            for (part,indices) in selections.iter().enumerate() {
                if indices.is_empty() { continue; }
                let offset = writer.position()?;
                for index in indices {
                    context.check_cancelled()?;
                    let row = &page.page.rows()[*index];
                    let record = OwnedRow::copy_parts(&[&[Value::I32(page.hash(*index)? as i32)],&row.values],0,0,&context.memory)?;
                    writer.write(&record.values,&context.memory)?;
                }
                parts[part].push(Span { offset,rows: indices.len() });
            }
        }
        Ok(Self { file: writer.finish()?,parts,_reservation: reservation })
    }
    pub fn part_count(&self) -> usize { self.parts.len() }
    pub fn load(&self,part: usize,context: &Arc<QueryContext>) -> Result<HashedPage> {
        context.check_cancelled()?;
        let spans = self.parts.get(part).ok_or_else(||invalid("join-partition","Join partition is absent"))?;
        let count = spans.iter().try_fold(0usize,|rows,span|rows.checked_add(span.rows).ok_or_else(size_error))?;
        let reservation = context.memory.reserve(count.checked_mul(std::mem::size_of::<u32>()).and_then(|bytes|bytes.checked_add(256)).ok_or_else(size_error)?)?;
        let mut hashes = Vec::with_capacity(count);
        let mut page = PageBuilder::new(count,&context.memory)?;
        let mut reader = self.file.reader(Arc::clone(&context.memory))?;
        for span in spans {
            reader.seek(span.offset)?;
            for _ in 0..span.rows {
                context.check_cancelled()?;
                let mut row = reader.next()?.ok_or_else(||invalid("join-spill","Join partition ended before its row count"))?;
                let Some(Value::I32(hash)) = row.values.first() else { return Err(invalid("join-spill","Join partition row lacks its hash")); };
                hashes.push(*hash as u32);row.values.remove(0);page.push(row)?;
            }
        }
        Ok(HashedPage { page: page.finish(),hashes,_reservation: reservation })
    }
}

pub(crate) fn partition_count(blocks: usize,minimum: usize) -> Result<usize> {
    if minimum == 0 || minimum > i32::MAX as usize || !minimum.is_power_of_two() { return Err(invalid("join-partitions","Minimum join partitions must be a positive signed power of two")); }
    let blocks = i32::try_from(blocks).map_err(|_|size_error())?;
    let count = if blocks == 0 { 0 } else { (1u32 << (31 - blocks.leading_zeros())).wrapping_shl(1) as i32 };
    Ok(count.max(minimum as i32) as usize)
}

#[derive(Clone,Copy)]
enum Slot { Empty,Occupied { hash: u32,row: usize } }
pub(crate) struct HashIndex { slots: Vec<Slot>,mask: usize,_reservation: Arc<MemoryReservation> }
impl HashIndex {
    pub(crate) fn new(hashes: &[u32],memory: &Arc<MemoryBudget>) -> Result<Self> {
        if hashes.len() > i32::MAX as usize { return Err(size_error()); }
        let expected = (hashes.len() as f64 / 0.6) as u64;
        let capacity = 1usize.checked_shl(u64::BITS - expected.leading_zeros()).ok_or_else(size_error)?;
        let reservation = memory.reserve(capacity.checked_mul(std::mem::size_of::<Slot>()).and_then(|bytes|bytes.checked_add(256)).ok_or_else(size_error)?)?;
        let _temporary = memory.reserve(capacity.checked_mul(std::mem::size_of::<usize>()).ok_or_else(size_error)?)?;
        let mut slots = vec![Slot::Empty;capacity];let mask = capacity - 1;
        let mut skip = (0..capacity).collect::<Vec<_>>();
        for (row,hash) in hashes.iter().enumerate() {
            let masked = *hash as usize & mask;
            let mut index = skip[masked];
            while matches!(slots[index],Slot::Occupied { .. }) {
                let next = (index + 1) & mask;
                skip[index] = skip[next];
                index = skip[next];
            }
            skip[masked] = skip[(index + 1) & mask];
            slots[index] = Slot::Occupied { hash: *hash,row };
        }
        Ok(Self { slots,mask,_reservation: reservation })
    }
    pub fn candidates(&self,hash: u32) -> Candidates<'_> { Candidates { index: self,next: hash as usize & self.mask,hash,done: false } }
}
pub(crate) struct Candidates<'a> { index: &'a HashIndex,next: usize,hash: u32,done: bool }
impl Iterator for Candidates<'_> {
    type Item = usize;
    fn next(&mut self) -> Option<usize> {
        if self.done { return None; }
        loop {
            match self.index.slots[self.next] {
                Slot::Empty => { self.done = true;return None; }
                Slot::Occupied { hash,row } => {
                    self.next = (self.next + 1) & self.index.mask;
                    if hash == self.hash { return Some(row); }
                }
            }
        }
    }
}

pub(crate) struct BuildTable { pub rows: HashedPage,pub index: HashIndex,pub unmatched: Option<Vec<u8>>,_reservation: Arc<MemoryReservation> }
impl BuildTable {
    pub fn new(rows: HashedPage,track_unmatched: bool,memory: &Arc<MemoryBudget>) -> Result<Self> {
        let index = HashIndex::new(&rows.hashes,memory)?;
        let reservation = memory.reserve(if track_unmatched { rows.page.len() } else { 0 })?;
        let unmatched = track_unmatched.then(||vec![1;rows.page.len()]);
        Ok(Self { rows,index,unmatched,_reservation: reservation })
    }
    pub fn candidates(&self,hash: u32) -> Candidates<'_> { self.index.candidates(hash) }
    #[cfg(test)]
    pub fn add_match(&mut self,row: usize) -> Result<()> {
        if let Some(unmatched) = &mut self.unmatched { *unmatched.get_mut(row).ok_or_else(||invalid("join-row","Matched build row is absent"))? = 0; }
        Ok(())
    }
    pub fn unmatched(&self) -> impl Iterator<Item=usize> + '_ {
        self.unmatched.iter().flat_map(|rows|rows.iter().enumerate().filter_map(|(index,present)|(*present != 0).then_some(index)))
    }
}

fn size_error() -> rtdb_types::Anomaly { invalid("join-size","Join rows, partitions or index exceed supported allocation bounds") }

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use rtdb_storage::{MemoryStore,cache::BufferPool};
    use rtdb_types::Timestamp;
    use tokio_util::sync::CancellationToken;
    use crate::{CatalogSnapshot,DatabaseSnapshot,QueryOptions};
    fn context() -> Arc<QueryContext> {
        let database = DatabaseSnapshot { partitions: vec![],storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()),1 << 20)) };
        let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),database)]),false).unwrap();
        QueryContext::new(catalog,MemoryBudget::new(1 << 24),Timestamp(0),"UTC",QueryOptions::default(),&CancellationToken::new()).unwrap()
    }
    fn page(values: &[i64],context: &Arc<QueryContext>) -> OwnedPage {
        let mut page = PageBuilder::new(values.len(),&context.memory).unwrap();
        for value in values { page.push(OwnedRow::new(vec![Value::I64(*value)],&context.memory).unwrap()).unwrap(); }
        page.finish()
    }
    fn row_hash(row: &OwnedRow) -> Result<u32> { match row.values.first() { Some(Value::I64(value)) => Ok(*value as u32),_ => Err(invalid("test-hash","Missing test hash")) } }
    fn values(page: &OwnedPage) -> Vec<i64> { page.rows().iter().map(|row|match row.values[0] { Value::I64(value) => value,_ => panic!("Expected integer") }).collect() }

    #[test]
    fn build_accumulation_spills_after_whole_page_crosses_threshold() {
        let context = context();let mut accumulator = BuildAccumulator::new(3,Arc::clone(&context)).unwrap();
        accumulator.append(page(&[0,1,2],&context)).unwrap();assert_eq!(context.spill_bytes(),0);
        accumulator.append(page(&[3,4],&context)).unwrap();assert!(context.spill_bytes() > 0);
        accumulator.append(page(&[5,6,7,8],&context)).unwrap();
        let BuildInput::Spilled(spool) = accumulator.finish().unwrap() else { panic!("Expected spill") };
        assert_eq!(spool.rows,9);assert_eq!(spool.blocks,2);
        let mut reader = spool.reader(&context).unwrap();
        assert_eq!(values(&reader.next().unwrap().unwrap()),[0,1,2,3,4]);
        assert_eq!(values(&reader.next().unwrap().unwrap()),[5,6,7,8]);assert!(reader.next().unwrap().is_none());
        drop(reader);drop(spool);assert_eq!(context.memory.used_bytes(),0);assert_eq!(context.spill_bytes(),0);
    }

    #[test]
    fn threshold_equal_count_stays_in_memory_and_negative_empty_page_opens_empty_spill() {
        let context = context();let mut accumulator = BuildAccumulator::new(2,Arc::clone(&context)).unwrap();
        accumulator.append(page(&[1,2],&context)).unwrap();
        let BuildInput::Memory(page) = accumulator.finish().unwrap() else { panic!("Expected memory") };
        assert_eq!(values(&page),[1,2]);drop(page);assert_eq!(context.memory.used_bytes(),0);
        let BuildInput::Memory(absent) = BuildAccumulator::new(-1,Arc::clone(&context)).unwrap().finish().unwrap() else { panic!("Absent input must not open a spill") };
        assert_eq!(absent.len(),0);drop(absent);assert_eq!(context.spill_bytes(),0);
        let mut accumulator = BuildAccumulator::new(-1,Arc::clone(&context)).unwrap();accumulator.append(super::tests::page(&[],&context)).unwrap();
        let BuildInput::Spilled(spool) = accumulator.finish().unwrap() else { panic!("Expected empty spill") };
        assert_eq!(spool.rows,0);assert_eq!(spool.blocks,0);
        let partitions = PartitionSpool::build(spool,1,row_hash,&context).unwrap();assert_eq!(partitions.part_count(),1);
        assert_eq!(partitions.load(0,&context).unwrap().page.len(),0);
        drop(partitions);assert_eq!(context.memory.used_bytes(),0);assert_eq!(context.spill_bytes(),0);
    }

    #[test]
    fn shuffle_retains_stable_block_order_and_exact_power_of_two_partitions() {
        let context = context();let mut accumulator = BuildAccumulator::new(3,Arc::clone(&context)).unwrap();
        for values in [&[0,1,2][..],&[3,4][..],&[5,6,7,8][..]] { accumulator.append(page(values,&context)).unwrap(); }
        let BuildInput::Spilled(spool) = accumulator.finish().unwrap() else { panic!("Expected spill") };
        let partitions = PartitionSpool::build(spool,1,row_hash,&context).unwrap();assert_eq!(partitions.part_count(),4);
        for part in 0..4 {
            let page = partitions.load(part,&context).unwrap();
            assert_eq!(values(&page.page),(0i64..9).filter(|value|(*value as usize & 3) == part).collect::<Vec<_>>());
            for (index,row) in page.page.rows().iter().enumerate() { assert_eq!(page.hash(index).unwrap(),row_hash(row).unwrap()); }
        }
        let page = partitions.load(0,&context).unwrap();drop(partitions);assert_eq!(context.spill_bytes(),0);
        assert_eq!(values(&page.page),[0,4,8]);drop(page);assert_eq!(context.memory.used_bytes(),0);
    }

    #[test]
    fn partition_count_uses_reference_signed_shift_and_probe_minimum() {
        for (blocks,minimum,expected) in [(0,1,1),(1,1,2),(2,1,4),(3,1,4),(4,1,8),(1,8,8),(0,8,8),(1usize << 30,1,1)] {
            assert_eq!(partition_count(blocks,minimum).unwrap(),expected);
        }
        assert!(partition_count(1,3).is_err());assert!(partition_count(1,0).is_err());
    }

    #[test]
    fn hash_index_candidates_keep_insertion_order_through_wrapped_collisions() {
        let memory = MemoryBudget::new(1 << 20);
        let index = HashIndex::new(&[u32::MAX,7,u32::MAX,15,u32::MAX],&memory).unwrap();
        assert_eq!(index.candidates(u32::MAX).collect::<Vec<_>>(),[0,2,4]);
        assert_eq!(index.candidates(7).collect::<Vec<_>>(),[1]);assert_eq!(index.candidates(15).collect::<Vec<_>>(),[3]);
        assert_eq!(index.candidates(23).count(),0);drop(index);assert_eq!(memory.used_bytes(),0);
        let memory = MemoryBudget::new(1);
        assert!(HashIndex::new(&[1],&memory).is_err());assert_eq!(memory.used_bytes(),0);
    }

    #[test]
    fn build_table_marks_only_selected_rows_and_retains_its_page_charge() {
        let context = context();let rows = HashedPage::new(page(&[1,1,2],&context),row_hash,&context.memory).unwrap();
        let mut table = BuildTable::new(rows,true,&context.memory).unwrap();assert_eq!(table.candidates(1).collect::<Vec<_>>(),[0,1]);
        table.add_match(0).unwrap();assert_eq!(table.unmatched().collect::<Vec<_>>(),[1,2]);
        drop(table);assert_eq!(context.memory.used_bytes(),0);
    }
}
