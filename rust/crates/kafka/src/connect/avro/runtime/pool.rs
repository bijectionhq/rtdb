use std::{alloc::Layout, mem::replace, sync::{Mutex, atomic::AtomicUsize}, thread::ThreadId};
use crate::connect::registry::reader_symbols::Profile;
use super::*;
use super::super::{TokenBuffer, TokenRecycler};

pub(in crate::connect) struct ThreadTokenPool {
    profile: Profile,
    entries: Mutex<Entries>,
    charge: Arc<MemoryReservation>,
}
struct Entries { values: Vec<Entry>, charge: Arc<MemoryReservation> }
struct Entry { thread: ThreadId, slot: Arc<Slot> }
struct Slot { recycler: Mutex<TokenRecycler>, charge: Arc<MemoryReservation> }

pub(in crate::connect) struct ThreadInputBuffers {
    pool: Arc<ThreadTokenPool>,
    capture: Capture,
}
enum Capture { Idle, Active(Arc<Slot>) }

impl ThreadTokenPool {
    pub(in crate::connect) fn new(profile: Profile, context: &mut ConversionContext) -> Result<Arc<Self>> {
        let charge = context.reserve(shared_bound::<Self>()?)?;
        let backing = context.reserve(backing_bound(4)?)?;
        let mut values = Vec::new();
        values.try_reserve_exact(4).map_err(allocation)?;
        Ok(Arc::new(Self { profile, entries: Mutex::new(Entries { values, charge: backing }), charge }))
    }

    pub(in crate::connect) fn buffers(self: &Arc<Self>) -> ThreadInputBuffers {
        ThreadInputBuffers { pool: Arc::clone(self), capture: Capture::Idle }
    }

    fn acquire(&self, thread: ThreadId, context: &mut ConversionContext) -> Result<Arc<Slot>> {
        context.retain(Arc::clone(&self.charge))?;
        let mut entries = self.entries.lock().map_err(|_| poisoned())?;
        context.retain(Arc::clone(&entries.charge))?;
        if let Some(entry) = entries.values.iter().find(|entry| entry.thread == thread) {
            context.retain(Arc::clone(&entry.slot.charge))?;
            return Ok(Arc::clone(&entry.slot));
        }
        let charge = context.reserve(shared_bound::<Slot>()?)?;
        let slot = Arc::new(Slot { recycler: Mutex::new(TokenRecycler::empty(self.profile)), charge });
        if entries.values.len() == entries.values.capacity() {
            let capacity = entries.values.capacity().checked_mul(2).ok_or_else(overflow)?;
            let charge = context.reserve(backing_bound(capacity)?)?;
            let additional = capacity - entries.values.len();
            entries.values.try_reserve_exact(additional).map_err(allocation)?;
            entries.charge = charge;
        }
        entries.values.push(Entry { thread, slot: Arc::clone(&slot) });
        Ok(slot)
    }
}

impl ThreadInputBuffers {
    fn active(&self) -> Result<&Slot> {
        match &self.capture { Capture::Active(slot) => Ok(slot), Capture::Idle => Err(state_error()) }
    }
}

impl InputBuffers for ThreadInputBuffers {
    fn profile(&self) -> Profile { self.pool.profile }

    fn acquire(&mut self, context: &mut ConversionContext) -> Result<()> {
        if !matches!(self.capture, Capture::Idle) { return Err(state_error()); }
        let slot = self.pool.acquire(std::thread::current().id(), context)?;
        self.capture = Capture::Active(slot);
        Ok(())
    }

    fn take(&mut self, minimum: usize, context: &mut ConversionContext) -> Result<TokenBuffer> {
        self.active()?.recycler.lock().map_err(|_| poisoned())?.take(minimum, context)
    }

    fn give(&mut self, buffer: TokenBuffer, context: &mut ConversionContext) -> Result<()> {
        self.active()?.recycler.lock().map_err(|_| poisoned())?.give(buffer, context)
    }

    fn close(&mut self, _: &mut ConversionContext) -> Result<()> {
        match replace(&mut self.capture, Capture::Idle) {
            Capture::Active(_) => Ok(()),
            Capture::Idle => Err(state_error()),
        }
    }

    fn abandon(&mut self) { self.capture = Capture::Idle; }
}

fn backing_bound(capacity: usize) -> Result<usize> {
    capacity.checked_mul(size_of::<Entry>()).and_then(|bytes| bytes.checked_mul(16))
        .and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)
}
fn shared_bound<T>() -> Result<usize> {
    let layout = Layout::new::<[AtomicUsize; 2]>().extend(Layout::new::<T>())
        .map_err(|_| overflow())?.0.pad_to_align();
    layout.size().checked_mul(16).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)
}
fn allocation(cause: impl std::fmt::Display) -> rtdb_types::Anomaly { error("allocation-failed", cause) }
fn state_error() -> rtdb_types::Anomaly { error("avro-recycler-state", "Avro parser recycler capture is not in the required state") }
fn poisoned() -> rtdb_types::Anomaly { error("avro-recycler-lock", "Avro recycler state lock was poisoned") }

#[cfg(test)]
mod tests;
