mod buffer;
mod scalar;
mod copy;
#[cfg(test)]
mod tests;

use super::*;
use crate::MemoryBudget;
use buffer::{MutableBits, MutableBytes};
use native_scalar::{NativeScalarData, NativeScalarStorage, ScalarOffsets, ScalarStorageRef, StoredBits, StoredBytes};
pub(crate) use scalar::{ScalarReader, ScalarWriter};
pub(crate) use copy::{ScalarCopier, ScalarSource};

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ReferenceFailure { Unsupported(&'static str), InvalidCopySource, CheckFailed, RequireFailed, Bounds }
#[derive(Debug)]
pub(crate) enum WriteError { Reference(ReferenceFailure), Native(Anomaly) }
pub(crate) type WriteResult<T> = std::result::Result<T, WriteError>;
impl From<Anomaly> for WriteError { fn from(error: Anomaly) -> Self { Self::Native(error) } }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PublicationState { Ready, NativeAborted }
#[derive(Default)]
struct Mutation { changed: bool }
impl Mutation { fn mark(&mut self) { self.changed = true; } }

fn unsupported_write(method: &'static str) -> WriteError { WriteError::Reference(ReferenceFailure::Unsupported(method)) }
fn bounds() -> WriteError { WriteError::Reference(ReferenceFailure::Bounds) }
fn native_size() -> WriteError { WriteError::Native(allocation()) }
fn checked_count(count: usize) -> WriteResult<usize> { vector_length(count)?; Ok(count) }
fn position(index: i32) -> WriteResult<usize> { usize::try_from(index).map_err(|_| native_size()) }
fn aborted() -> Anomaly { Anomaly::fault("rtdb/aborted-vector-writer", "Native vector admission failed after mutation; clear is required before writing or publication") }
