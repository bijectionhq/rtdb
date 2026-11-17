mod buffer;
mod owner;
mod copy;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod count_tests;

use super::*;
use crate::MemoryBudget;
use buffer::{MutableBits, MutableBytes};
use native_scalar::{NativeScalarData, NativeScalarStorage, ScalarOffsets, ScalarStorageRef, StoredBits, StoredBytes};
pub(crate) use owner::{ScalarReader, ScalarWriter, VectorWriter};
pub(crate) use copy::{ScalarCopier, ScalarSource};

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ReferenceFailure { Unsupported(&'static str), InvalidCopySource, CheckFailed, RequireFailed, Bounds, NegativeArraySize(i32), CannotPromoteUnionLeg, RunEndReceiverClass, CannotSetUnionNullable }
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
fn array_count(count: usize) -> WriteResult<i32> { vector_length(count)?; Ok(count as i32) }
fn unsafe_cursor() -> WriteError { WriteError::Native(Anomaly::incorrect("rtdb/unsafe-arrow-write-cursor", "Unchecked Arrow append cursor exceeds retained storage")) }
fn aborted() -> Anomaly { Anomaly::fault("rtdb/aborted-vector-writer", "Native vector admission failed after mutation; clear is required before writing or publication") }
