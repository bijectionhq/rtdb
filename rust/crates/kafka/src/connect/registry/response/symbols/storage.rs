use super::*;

pub(super) struct Table {
    pub primary: Vec<Option<Arc<SymbolName>>>, pub buckets: Vec<Option<Arc<Bucket>>>,
    pub count: usize, pub threshold: usize, pub longest: i32, charge: Arc<MemoryReservation>,
}
impl Table {
    pub fn new(size: usize, context: &mut ConversionContext) -> Result<Self> {
        let primary_layout = Layout::array::<Option<Arc<SymbolName>>>(size).map_err(|_| overflow())?;
        let buckets_layout = Layout::array::<Option<Arc<Bucket>>>(size >> 1).map_err(|_| overflow())?;
        let owner = shared_bound::<Self>()?;
        let bytes = primary_layout.size().checked_add(buckets_layout.size()).and_then(|bytes| bytes.checked_add(owner)).ok_or_else(overflow)?;
        let charge = context.reserve(bytes)?;
        let mut primary = Vec::new();
        primary.try_reserve_exact(size).map_err(|_| allocation_failed())?;
        primary.resize_with(size, || None);
        let mut buckets = Vec::new();
        buckets.try_reserve_exact(size >> 1).map_err(|_| allocation_failed())?;
        buckets.resize_with(size >> 1, || None);
        Ok(Self { primary, buckets, count: 0, threshold: size - (size >> 2), longest: 0, charge })
    }
    pub fn copy(&self, context: &mut ConversionContext) -> Result<Self> {
        let mut result = Self::new(self.primary.len(), context)?;
        for (target, source) in result.primary.iter_mut().zip(&self.primary) { *target = source.clone(); }
        for (target, source) in result.buckets.iter_mut().zip(&self.buckets) { *target = source.clone(); }
        result.count = self.count;
        result.threshold = self.threshold;
        result.longest = self.longest;
        Ok(result)
    }
    pub fn retain(&self, context: &mut ConversionContext) -> rtdb_types::Result<()> {
        context.retain(Arc::clone(&self.charge))?;
        for name in self.primary.iter().flatten() { name.retain(context)?; }
        for head in &self.buckets {
            let mut bucket = head.as_deref();
            while let Some(entry) = bucket {
                context.retain(Arc::clone(&entry.charge))?;
                entry.name.retain(context)?;
                bucket = entry.next.as_deref();
            }
        }
        Ok(())
    }
}

pub(super) struct Bucket {
    pub name: Arc<SymbolName>, pub next: Option<Arc<Bucket>>, pub length: usize, charge: Arc<MemoryReservation>,
}
impl Bucket {
    pub fn new(name: Arc<SymbolName>, next: Option<Arc<Self>>, context: &mut ConversionContext) -> Result<Arc<Self>> {
        let charge = context.reserve(shared_bound::<Self>()?)?;
        let length = next.as_ref().map_or(0, |next| next.length).checked_add(1).ok_or_else(overflow)?;
        Ok(Arc::new(Self { name, next, length, charge }))
    }
}

pub(super) struct OverflowBits { words: Vec<u64>, _charge: Arc<MemoryReservation> }
impl OverflowBits {
    pub fn new(context: &mut ConversionContext) -> Result<Self> { Self::allocate(1, context) }
    fn allocate(size: usize, context: &mut ConversionContext) -> Result<Self> {
        let bytes = size.checked_mul(size_of::<u64>()).and_then(|bytes| bytes.checked_add(size_of::<Self>())).and_then(|bytes| bytes.checked_add(256)).ok_or_else(overflow)?;
        Layout::array::<u64>(size).map_err(|_| overflow())?;
        let charge = context.reserve(bytes)?;
        let mut words = Vec::new();
        words.try_reserve_exact(size).map_err(|_| allocation_failed())?;
        words.resize(size, 0);
        Ok(Self { words, _charge: charge })
    }
    pub fn get(&self, index: usize) -> bool { self.words.get(index >> 6).is_some_and(|word| word & (1u64 << (index & 63)) != 0) }
    pub fn set(&mut self, index: usize, context: &mut ConversionContext) -> Result<()> {
        let required = (index >> 6).checked_add(1).ok_or_else(overflow)?;
        if required > self.words.len() {
            let size = self.words.len().checked_mul(2).ok_or_else(overflow)?.max(required);
            let mut replacement = Self::allocate(size, context)?;
            replacement.words[..self.words.len()].copy_from_slice(&self.words);
            *self = replacement;
        }
        self.words[index >> 6] |= 1u64 << (index & 63);
        Ok(())
    }
}
