//! Scan fragments: a scan of one table split by trie prefix, so that another node can read a slice of the table at the
//! query's basis and send back the rows it resolves, in place of this node reading those tries itself.
use std::{io::{self, Cursor, Write}, mem::size_of, ops::Range};
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_buffer::Buffer;
use arrow_ipc::{MetadataVersion, writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions, StreamWriter, write_message}};
use arrow_schema::{ArrowError, DataType, Field as ArrowField, Schema as ArrowSchema};
use bytes::Bytes;
use futures::future::BoxFuture;
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_format::ipc::{ArrowStream, OwnedStream, RecordSpans};
use rtdb_plan::{Scan, TemporalFilter};
use rtdb_types::{Anomaly, AnomalyCategory, Iid, Interval, LogicalType, Result, TableRef, TemporalBounds, Timestamp, TransactionKey, Value};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::columnar::{Column, ColumnBatch, compact_arrow_rows, fragment_column, nested_layout};
use crate::context::{Execution, OuterRows, QueryContext, cancelled};
use crate::invalid;

mod literals;
mod slice;

/// The deepest split: 4^4 = 256 fragments.
const MAXIMUM_DEPTH: u8 = 4;

/// A run of base-four trie buckets from the root, naming the IIDs that share it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "Vec<u8>", into = "Vec<u8>")]
pub struct TriePrefix(Vec<u8>);

impl TriePrefix {
    pub fn new(buckets: Vec<u8>) -> Result<Self> {
        if buckets.len() > usize::from(MAXIMUM_DEPTH) || buckets.iter().any(|bucket| *bucket > 3) {
            return Err(invalid("trie-prefix", format!("A fragment prefix is at most {MAXIMUM_DEPTH} buckets of 0 to 3")));
        }
        Ok(Self(buckets))
    }
    pub fn buckets(&self) -> &[u8] { &self.0 }
    /// The lowest and highest IIDs under the prefix.
    pub fn range(&self) -> (Iid, Iid) {
        let mut lower = [0u8; 16];
        let mut upper = [u8::MAX; 16];
        for (depth, bucket) in self.0.iter().enumerate() {
            let shift = 6 - 2 * (depth % 4);
            lower[depth / 4] |= bucket << shift;
            upper[depth / 4] = (upper[depth / 4] & !(3 << shift)) | (bucket << shift);
        }
        (Iid(lower), Iid(upper))
    }
}

impl TryFrom<Vec<u8>> for TriePrefix {
    type Error = Anomaly;
    fn try_from(buckets: Vec<u8>) -> Result<Self> { Self::new(buckets) }
}
impl From<TriePrefix> for Vec<u8> { fn from(prefix: TriePrefix) -> Self { prefix.0 } }

impl std::fmt::Display for TriePrefix {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for bucket in &self.0 { write!(formatter, "{bucket}")?; }
        Ok(())
    }
}

/// How finely a scan splits: into every prefix of this many buckets, which together cover each IID exactly once.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FragmentDepth(u8);

impl FragmentDepth {
    pub fn new(depth: u8) -> Result<Self> {
        if depth == 0 || depth > MAXIMUM_DEPTH { return Err(invalid("fragment-depth", format!("A scan splits by 1 to {MAXIMUM_DEPTH} trie buckets"))); }
        Ok(Self(depth))
    }
    pub fn prefixes(self) -> Vec<TriePrefix> {
        (0..4usize.pow(u32::from(self.0))).map(|index| {
            TriePrefix((0..self.0).rev().map(|position| ((index >> (2 * position)) & 3) as u8).collect())
        }).collect()
    }
}

/// The physical storage and replica journal used by the captured view.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FragmentResidence {
    pub storage: Arc<str>,
    pub replica_epoch: i32,
}

/// One slice of a scan, as a node other than the coordinator evaluates it: the table's rows under `prefix`, resolved within
/// `bounds` over the snapshot whose latest transaction is `basis`, projected to the scan's columns and filtered by those of its predicates any node evaluates alike.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScanFragment {
    pub scan: Scan,
    pub prefix: TriePrefix,
    pub basis: TransactionKey,
    pub residence: Option<FragmentResidence>,
    pub bounds: TemporalBounds,
    /// The coordinator's time zone, under which the predicates' casts read.
    pub timezone: String,
}

/// Whether `predicate` evaluates alike on any node: it names no parameter or outer row, and calls nothing that reads the clock, the session or chance.
fn portable(predicate: &rtdb_plan::Expr) -> bool {
    const CONTEXTUAL: [&str; 9] = ["random", "now", "current_", "local", "snapshot_", "clock_", "statement_", "transaction_", "gen_random"];
    let mut portable = true;
    predicate.visit(&mut |expr| match expr {
        rtdb_plan::Expr::Parameter { .. } | rtdb_plan::Expr::Correlated { .. } => portable = false,
        rtdb_plan::Expr::Call { name, .. } if CONTEXTUAL.iter().any(|prefix| name.to_ascii_lowercase().starts_with(prefix)) => portable = false,
        _ => {}
    });
    portable
}

impl ScanFragment {
    pub fn database(&self) -> &str { &self.scan.table.name.database }
    pub fn table(&self) -> &TableRef { &self.scan.table.name.table }

    /// Binds each predicate slot to a matching column in one uploaded IPC row.
    /// Predicate insertion is atomic and the returned bindings retain the Arrow data.
    pub fn bind_wire_predicates(
        &mut self, predicates: Vec<(usize, rtdb_plan::Expr)>, bytes: FragmentBytes, memory: &Arc<MemoryBudget>,
    ) -> Result<crate::BoundParameters> {
        use rtdb_plan::Expr;
        let malformed = || invalid("fragment-argument", "Invalid delegated predicate argument");
        let stream = rtdb_format::ipc::OwnedStream::read_buffer(bytes.into_ipc_buffer(), memory)?;
        let [page] = stream.pages() else { return Err(malformed()); };
        let batch = page.batch();
        if batch.num_rows() != 1 || self.scan.predicates.iter().any(|predicate| !portable(predicate)) { return Err(malformed()); }
        let _schema = memory.reserve(rtdb_format::ipc::schema_translation_bound(batch.schema_ref())?)?;
        let types = batch.schema_ref().fields().iter().map(|field| rtdb_columnar::from_field(field)).collect::<Result<Vec<_>>>()?;
        let mut unused = vec![true; batch.num_columns()];
        for (_, predicate) in &predicates {
            let mut invalid_parameter = false;
            let mut portable_shape = predicate.clone();
            portable_shape.rewrite(&mut |expr| {
                if let Expr::Parameter { index, data_type } = expr {
                    if types.get(*index) == Some(&*data_type) && std::mem::take(&mut unused[*index]) {
                        *expr = Expr::Literal(Value::Null);
                    } else {
                        invalid_parameter = true;
                    }
                }
            });
            if invalid_parameter || !portable(&portable_shape) { return Err(malformed()); }
        }
        if unused.into_iter().any(|slot| slot) { return Err(malformed()); }
        let mut previous = None;
        let original_count = self.scan.predicates.len();
        for (inserted, (position, _)) in predicates.iter().enumerate() {
            if *position > original_count + inserted || previous.is_some_and(|previous| previous >= *position) {
                return Err(malformed());
            }
            previous = Some(*position);
        }
        let arguments = crate::BoundParameters::from_arrow(page.clone(), 0, types, memory)?;
        for (position, predicate) in predicates {
            self.scan.predicates.insert(position, predicate);
        }
        Ok(arguments)
    }

    /// The fragment of `scan` under `prefix`: its predicates and temporal filters stay with the coordinator, which filters
    /// the rows that come back, so the fragment carries only the projection and the bounds already evaluated.
    pub(crate) fn of(scan: &Scan, prefix: TriePrefix, basis: TransactionKey, residence: Option<FragmentResidence>, mut bounds: TemporalBounds, timezone: &str) -> Self {
        // The reader filters by the predicates it would evaluate as the coordinator does; the coordinator filters its rows by all of them again.
        let predicates = scan.predicates.iter().filter(|predicate| portable(predicate)).cloned().collect();
        let scan = Scan { predicates, iids: None, valid_time: TemporalFilter::Default, system_time: TemporalFilter::Default, ..scan.clone() };
        // The wire bounds are limited by the coordinator's admitted transaction head.
        let upper = Timestamp(basis.system_time.0.saturating_add(1));
        if bounds.system_time.upper() > upper {
            bounds.system_time = Interval::new(bounds.system_time.lower().min(basis.system_time), upper).expect("a nonempty interval up to the basis");
        }
        Self { scan, prefix, basis, residence, bounds, timezone: timezone.to_owned() }
    }
}

fn numeric_literal(expr: &rtdb_plan::Expr) -> Option<Value> {
    use rtdb_plan::Expr;
    let scalar = |value: &Value| matches!(value, Value::Null | Value::I8(_) | Value::I16(_) | Value::I32(_) | Value::I64(_)
        | Value::F16(_) | Value::F32(_) | Value::F64(_) | Value::Decimal(_));
    match expr {
        Expr::Literal(value @ (Value::List(values) | Value::FixedSizeList(values))) =>
            (!values.is_empty() && values.array().is_some()).then(|| value.clone()),
        Expr::List(items) if !items.is_empty() && items.iter().all(|expr| matches!(expr, Expr::Literal(value) if scalar(value))) => {
            let mut values = rtdb_types::ListBuilder::with_capacity(items.len());
            for item in items {
                let Expr::Literal(value) = item else { unreachable!("checked literal list") };
                values.push(value.clone());
            }
            Some(Value::List(values.finish()))
        }
        _ => None,
    }
}

/// Decides whether a placed database's scans are split, and runs each fragment on the node it belongs to.
#[async_trait::async_trait]
pub trait ScanFragmenter: Send + Sync {
    /// The depth a scan of `table` splits at, given the bytes of persisted tries the whole database holds; `None` scans it whole.
    fn depth(&self, database: &str, table: &TableRef, persisted_bytes: u64) -> Option<FragmentDepth>;
    /// The fragment's rows as encoded by `encode_fragment` on the node it belongs to, or `None` when that node is this one.
    /// An error, like `None`, has the coordinator scan the fragment itself.
    async fn dispatch(&self, fragment: &ScanFragment) -> Result<Option<Vec<u8>>>;

    async fn dispatch_reply(&self, fragment: &ScanFragment, memory: &Arc<MemoryBudget>) -> Result<Option<FragmentReply>> {
        let Some(bytes) = self.dispatch(fragment).await? else { return Ok(None) };
        let memory = Arc::clone(memory);
        Ok(Some(Box::pin(async move { FragmentBytes::from_vec(bytes, &memory) })))
    }
}

pub type FragmentReply = BoxFuture<'static, Result<FragmentBytes>>;

/// Evaluates a delegated scan at the coordinator's exact captured transaction head.
pub async fn evaluate_fragment(context: Arc<QueryContext>, fragment: &ScanFragment) -> Result<Vec<u8>> {
    Ok(evaluate_fragment_retained(context, fragment).await?.into_bytes())
}

/// Evaluates a fragment and keeps the wire allocation charged while the caller sends it.
pub async fn evaluate_fragment_retained(context: Arc<QueryContext>, fragment: &ScanFragment) -> Result<FragmentBytes> {
    evaluate_fragment_with_arguments(context, fragment, None).await
}

/// Evaluates a delegated scan with the bindings paired with its predicates.
/// Parameter-free fragments need no bindings.
pub async fn evaluate_fragment_with_arguments(
    context: Arc<QueryContext>, fragment: &ScanFragment, arguments: Option<crate::BoundParameters>,
) -> Result<FragmentBytes> {
    let cancellation = context.cancellation.clone();
    let owner = context.owner_cancellation.clone();
    let work = evaluate_retained_fragment(Arc::clone(&context), fragment, arguments);
    let work = owner.run_until_cancelled(cancellation.run_until_cancelled(work));
    context.reading(async { work.await.flatten().ok_or_else(cancelled)? }).await
}

async fn evaluate_retained_fragment(
    context: Arc<QueryContext>, fragment: &ScanFragment, arguments: Option<crate::BoundParameters>,
) -> Result<FragmentBytes> {
    let database = context.catalog.database(fragment.database())?.clone();
    let [snapshot] = database.partitions.as_slice() else { return Err(invalid("fragment-partitions", "A fragment reads a database of one partition")) };
    if snapshot.tx_basis() != Some(fragment.basis) || snapshot.has_overlays() {
        return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.query/fragment-basis", format!("This node cannot serve the fragment's basis {:?}", fragment.basis)));
    }
    let memory = Arc::clone(&context.memory);
    let mut arguments = match arguments {
        Some(arguments) => arguments,
        None => crate::BoundParameters::new(Vec::new(), Vec::new(), &memory)?,
    };
    arguments.use_execution_budget(&memory);
    let execution = Execution { parameters: Arc::new(arguments), outer: OuterRows::empty(&memory)?, context, bindings: None, analysis: None };
    let scan = crate::scan::PartitionScan::fragment(snapshot.clone(), Arc::clone(&database.storage), fragment, execution.clone());
    let mut batches: crate::columnar::BoxBatchCursor = Box::new(FragmentRows(scan));
    if let Some(predicate) = fragment.scan.predicates.iter().cloned().reduce(|left, right| rtdb_plan::Expr::binary(rtdb_plan::BinaryOp::And, left, right)) {
        batches = Box::new(crate::columnar::BatchFilter::new(batches, rtdb_plan::Schema::new(fragment.scan.columns.clone())?, predicate, execution));
    }
    let mut encoded = FragmentBytes::new(&memory)?;
    while let Some(batch) = batches.next_batch().await? { encode_batch(&batch, &memory, &mut encoded)?; }
    Ok(encoded)
}

struct FragmentRows(crate::scan::PartitionScan);

#[async_trait::async_trait]
impl crate::columnar::BatchCursor for FragmentRows {
    async fn next_batch(&mut self) -> Result<Option<crate::columnar::ColumnBatch>> { self.0.next_batch().await }
}

const COLUMN_KEY: &str = "rtdb.fragment.column";
const NAME_KEY: &str = "rtdb.fragment.name";
const LOGICAL_KEY: &str = "rtdb.fragment.logical";
const IPC_ALIGNMENT: usize = 64;
const WIRE_FIELD_CACHE_COLUMNS: usize = 32;
const WIRE_FIELD_CACHE_BYTES: usize = 64 << 10;

struct WireFields {
    by_position: [Option<WireField>; WIRE_FIELD_CACHE_COLUMNS],
}

struct WireField {
    physical: Arc<ArrowField>,
    logical: Option<LogicalType>,
    wire: Arc<ArrowField>,
    charge: Arc<MemoryReservation>,
}

impl WireFields {
    fn new() -> Self {
        Self { by_position: std::array::from_fn(|_| None) }
    }

    fn clear(&mut self) -> bool {
        let populated = self.by_position.iter().any(Option::is_some);
        *self = Self::new();
        populated
    }

    fn field(&mut self, position: usize, physical: Arc<ArrowField>, logical: Option<LogicalType>,
        memory: &Arc<MemoryBudget>,
    ) -> Result<Arc<ArrowField>> {
        let Some(slot) = self.by_position.get_mut(position) else {
            return wire_field(position, &physical, logical.as_ref());
        };
        if let Some(field) = slot
            && (Arc::ptr_eq(&physical, &field.physical)
                || physical == field.physical && same_dictionary_fields(&physical, &field.physical))
            && match (&logical, &field.logical) {
                (Some(left), Some(right)) => same_wire_logical(left, right),
                (None, None) => true,
                _ => false,
            }
        {
            return Ok(Arc::clone(&field.wire));
        }
        *slot = None;
        let used = self.by_position.iter().flatten().map(|field| field.charge.bytes()).sum::<usize>();
        let logical_bytes = logical.as_ref().map_or(Ok(0), LogicalType::retained_bytes);
        let bound = physical.size().checked_mul(4)
            .and_then(|bytes| bytes.checked_add(logical_bytes.ok()?.checked_mul(8)?))
            .and_then(|bytes| bytes.checked_add(1024));
        let charge = bound.filter(|&bytes| bytes <= WIRE_FIELD_CACHE_BYTES - used)
            .and_then(|bytes| memory.reserve(bytes).ok());
        let wire = wire_field(position, &physical, logical.as_ref())?;
        if let Some(charge) = charge {
            self.by_position[position] = Some(WireField { physical, logical, wire: Arc::clone(&wire), charge });
        }
        Ok(wire)
    }
}

// Arrow field equality omits IPC dictionary properties, including those on children.
#[allow(deprecated)]
fn same_dictionary_fields(left: &ArrowField, right: &ArrowField) -> bool {
    fn same_type(left: &DataType, right: &DataType) -> bool {
        match (left, right) {
            (DataType::Dictionary(lk, lv), DataType::Dictionary(rk, rv)) => same_type(lk, rk) && same_type(lv, rv),
            (DataType::Struct(left), DataType::Struct(right)) => left.iter().zip(right.iter()).all(|(l, r)| same_dictionary_fields(l, r)),
            (DataType::Union(left, _), DataType::Union(right, _)) => left.iter().zip(right.iter())
                .all(|((_, l), (_, r))| same_dictionary_fields(l, r)),
            (DataType::RunEndEncoded(le, lv), DataType::RunEndEncoded(re, rv)) => {
                same_dictionary_fields(le, re) && same_dictionary_fields(lv, rv)
            }
            (DataType::List(l), DataType::List(r))
            | (DataType::LargeList(l), DataType::LargeList(r))
            | (DataType::ListView(l), DataType::ListView(r))
            | (DataType::LargeListView(l), DataType::LargeListView(r))
            | (DataType::FixedSizeList(l, _), DataType::FixedSizeList(r, _))
            | (DataType::Map(l, _), DataType::Map(r, _)) => same_dictionary_fields(l, r),
            _ => true,
        }
    }
    left.dict_id() == right.dict_id() && left.dict_is_ordered() == right.dict_is_ordered()
        && same_type(left.data_type(), right.data_type())
}

// Logical equality ignores the order that structs and unions write to the wire.
fn same_wire_logical(left: &LogicalType, right: &LogicalType) -> bool {
    use LogicalType::*;
    match (left, right) {
        (Struct(left), Struct(right)) => left.len() == right.len() && left.iter().zip(right.iter())
            .all(|((ln, lt), (rn, rt))| ln == rn && same_wire_logical(lt, rt)),
        (Union(left), Union(right)) => left.len() == right.len() && left.iter().zip(right.iter())
            .all(|(l, r)| same_wire_logical(l, r)),
        (List { kind: lk, element: l }, List { kind: rk, element: r }) => lk == rk && same_wire_logical(l, r),
        (RunEndEncoded { width: lw, value: l }, RunEndEncoded { width: rw, value: r }) => lw == rw && same_wire_logical(l, r),
        (Maybe(l), Maybe(r)) => same_wire_logical(l, r),
        _ => left == right,
    }
}

fn wire_field(position: usize, field: &ArrowField, logical: Option<&LogicalType>) -> Result<Arc<ArrowField>> {
    let mut metadata = field.metadata().clone();
    metadata.insert(COLUMN_KEY.into(), "arrow".into());
    metadata.insert(NAME_KEY.into(), field.name().clone());
    if let Some(logical) = logical {
        metadata.insert(LOGICAL_KEY.into(), serde_json::to_string(logical).map_err(|error| invalid("fragment-encode", error))?);
    }
    Ok(Arc::new(field.clone().with_name(format!("c{position}")).with_metadata(metadata)))
}

/// Fragment IPC and the reservation for the bytes it retains.
#[derive(Debug)]
pub struct FragmentBytes { bytes: Vec<u8>, reservation: Arc<MemoryReservation> }

impl FragmentBytes {
    pub(crate) fn new(memory: &Arc<MemoryBudget>) -> Result<Self> {
        Ok(Self { bytes: Vec::new(), reservation: memory.reserve(0)? })
    }

    pub fn from_vec(bytes: Vec<u8>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let reservation = memory.reserve(bytes.capacity())?;
        Ok(Self { bytes, reservation })
    }

    pub async fn read_from(reader: &mut (impl AsyncRead + Unpin), length: usize, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let mut reservation = memory.reserve(length)?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(length).map_err(|error| invalid("fragment-decode", error))?;
        Arc::get_mut(&mut reservation).expect("The fragment reader owns its reservation").grow_to(bytes.capacity())?;
        bytes.resize(length, 0);
        reader.read_exact(&mut bytes).await.map_err(|error| invalid("fragment-decode", error))?;
        Ok(Self { bytes, reservation })
    }

    pub fn as_slice(&self) -> &[u8] { &self.bytes }
    pub fn into_bytes(self) -> Vec<u8> { self.bytes }
    pub(crate) fn into_ipc_buffer(self) -> Buffer { Buffer::from(Bytes::from_owner(self)) }

    fn reserve_append(&mut self, bytes: usize) -> io::Result<()> {
        let required = self.bytes.len().checked_add(bytes).ok_or_else(|| io::Error::other("Fragment byte length overflow"))?;
        if required > self.bytes.capacity() {
            let lease = Arc::get_mut(&mut self.reservation).expect("The fragment writer owns its reservation");
            let capacity = required.max(self.bytes.capacity().saturating_mul(2));
            let admitted = match lease.grow_to(capacity) {
                Ok(()) => capacity,
                Err(_) => {
                    lease.grow_to(required).map_err(io::Error::other)?;
                    required
                }
            };
            self.bytes.try_reserve_exact(admitted - self.bytes.len()).map_err(io::Error::other)?;
        }
        Ok(())
    }
}

impl AsRef<[u8]> for FragmentBytes {
    fn as_ref(&self) -> &[u8] { self.as_slice() }
}

impl Write for FragmentBytes {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.reserve_append(bytes.len())?;
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> { Ok(()) }
}

/// Appends length-prefixed Arrow IPC streams in scan order, splitting where selected vectors require different schemas.
pub(crate) fn encode_batch(batch: &ColumnBatch, memory: &Arc<MemoryBudget>, out: &mut FragmentBytes) -> Result<()> {
    let packed;
    let batch = if batch.columns.iter().any(|column| matches!(column, Column::Values(_))) {
        packed = batch.pack_evaluated(memory)?;
        &packed
    } else {
        batch
    };
    if !batch.columns.iter().any(|column| matches!(column, Column::Selected(_))) {
        return append_frame(batch, 0..batch.len, memory, out, None);
    }
    let _reservation = memory.reserve(batch.len)?;
    let mut boundaries = vec![false; batch.len];
    for column in &batch.columns {
        if let Column::Selected(selected) = column { selected.fragment_boundaries(&mut boundaries); }
    }
    let mut ends = boundaries.into_iter().enumerate().filter_map(|(index, boundary)| boundary.then_some(index)).peekable();
    if ends.peek().is_none() {
        return append_frame(batch, 0..batch.len, memory, out, None);
    }
    let mut fields = WireFields::new();
    let mut start = 0;
    for end in ends.chain(std::iter::once(batch.len)) {
        append_frame(batch, start..end, memory, out, Some(&mut fields))?;
        start = end;
    }
    Ok(())
}

fn append_frame(batch: &ColumnBatch, rows: Range<usize>, memory: &Arc<MemoryBudget>, out: &mut FragmentBytes,
    mut fields: Option<&mut WireFields>,
) -> Result<()> {
    let prefix = out.bytes.len();
    let mut result = encode_frame(batch, rows.clone(), memory, out, fields.as_deref_mut());
    if let Err(error) = &result {
        out.bytes.truncate(prefix);
        // Cache admission is optional, including when its charge would force a frame split.
        if fields.as_deref_mut().is_some_and(WireFields::clear) && error.code == "rtdb/memory-budget-exceeded" {
            result = encode_frame(batch, rows.clone(), memory, out, None);
            if result.is_err() { out.bytes.truncate(prefix); }
        }
    }
    if rows.len() > 1 && result.as_ref().is_err_and(|error| {
        matches!(error.code.as_ref(), "rtdb/run-end-selection-width" | "rtdb/memory-budget-exceeded")
    }) {
        let middle = rows.start + rows.len() / 2;
        let mut split_fields;
        let fields = match fields {
            Some(fields) => fields,
            None => {
                split_fields = WireFields::new();
                &mut split_fields
            }
        };
        append_frame(batch, rows.start..middle, memory, out, Some(fields))?;
        return append_frame(batch, middle..rows.end, memory, out, Some(fields));
    }
    result
}

fn fragment_io(error: io::Error) -> Anomaly {
    error.get_ref().and_then(|cause| cause.downcast_ref::<Anomaly>()).cloned()
        .unwrap_or_else(|| invalid("fragment-encode", error))
}

fn fragment_arrow_error(error: ArrowError) -> Anomaly {
    match error {
        ArrowError::IoError(_, error) => fragment_io(error),
        error => invalid("fragment-encode", error),
    }
}

fn frame_slice(array: &ArrayRef, field: &ArrowField, rows: Range<usize>, memory: &Arc<MemoryBudget>,
    reservations: &mut Vec<Arc<MemoryReservation>>) -> Option<(ArrayRef, Option<usize>)> {
    if rows.len() == array.len() {
        return Some((Arc::clone(array), None));
    }
    if !nested_layout(array.data_type()) {
        return Some((array.slice(rows.start, rows.len()), None));
    }
    let (array, reservation, body) = slice::prepare(array.as_ref(), field, rows, memory)?;
    reservations.push(reservation);
    Some((array, Some(body)))
}

fn encode_frame(batch: &ColumnBatch, rows: Range<usize>, memory: &Arc<MemoryBudget>, out: &mut FragmentBytes,
    mut wire_fields: Option<&mut WireFields>,
) -> Result<()> {
    let mut reservations = Vec::new();
    let mut fields = Vec::with_capacity(batch.columns.len());
    let mut arrays = Vec::with_capacity(batch.columns.len());
    let mut slice_bodies = Vec::with_capacity(batch.columns.len());
    for (position, column) in batch.columns.iter().enumerate() {
        let (array, field, logical, body) = match column {
            Column::Arrow { array, field, logical } if array.len() == batch.len => {
                let (array, body) = if let Some(slice) = frame_slice(array, field, rows.clone(), memory, &mut reservations) {
                    slice
                } else {
                    let (compact, reservation) = compact_arrow_rows(array.as_ref(), field, rows.clone(), memory)?;
                    reservations.push(reservation);
                    (compact, None)
                };
                (array, Arc::clone(field), logical.clone(), body)
            }
            Column::Selected(selected) => {
                if let Some(((source, field, logical), source_rows)) = selected.fragment_source(rows.clone())
                    && let Some((array, body)) = frame_slice(&source, &field, source_rows, memory, &mut reservations) {
                    (array, field, logical, body)
                } else {
                    let ((array, field, logical), reservation) = selected.fragment_vector(rows.clone(), memory)?;
                    reservations.push(reservation);
                    (array, field, logical, None)
                }
            }
            _ => return Err(invalid("fragment-encode", "Fragment column lacks its packed Arrow vector")),
        };
        let field = match wire_fields.as_deref_mut() {
            Some(cache) => cache.field(position, field, logical, memory)?,
            None => wire_field(position, &field, logical.as_ref())?,
        };
        fields.push(field);
        arrays.push(array);
        slice_bodies.push(body);
    }
    let schema = Arc::new(ArrowSchema::new(fields));
    let record = RecordBatch::try_new_with_options(Arc::clone(&schema), arrays, &RecordBatchOptions::new().with_row_count(Some(rows.len()))).map_err(|error| invalid("fragment-encode", error))?;
    let options = IpcWriteOptions::try_new(IPC_ALIGNMENT, false, MetadataVersion::V5).map_err(fragment_arrow_error)?;
    match try_write_spanned_frame(&record, &options, memory, out) {
        Ok(true) => return Ok(()),
        Ok(false) => (),
        Err(error) if error.code == "rtdb/memory-budget-exceeded" => (),
        Err(error) => return Err(error),
    }

    let schema_bound = rtdb_format::ipc::schema_translation_bound(&schema)?;
    let scratch = record.columns().iter().zip(slice_bodies).try_fold(schema_bound, |bound, (array, body)| {
        let bytes = match body {
            Some(bytes) => bytes,
            None => array.to_data().get_slice_memory_size().map_err(|error| invalid("fragment-encode", error))?,
        };
        bytes.checked_mul(4).and_then(|bytes| bound.checked_add(bytes))
            .ok_or_else(|| invalid("fragment-encode", "IPC allocation bound overflow"))
    })?;
    let _encoding = memory.reserve(scratch)?;
    let prefix = out.bytes.len();
    out.write_all(&[0; 8]).map_err(fragment_io)?;
    {
        let mut writer = StreamWriter::try_new_with_options(&mut *out, &schema, options).map_err(fragment_arrow_error)?;
        writer.write(&record).map_err(fragment_arrow_error)?;
        writer.finish().map_err(fragment_arrow_error)?;
    }
    let start = prefix + 8;
    let bytes = u64::try_from(out.bytes.len() - start).map_err(|error| invalid("fragment-encode", error))?;
    out.bytes[prefix..start].copy_from_slice(&bytes.to_be_bytes());
    Ok(())
}

fn try_write_spanned_frame(record: &RecordBatch, options: &IpcWriteOptions, memory: &Arc<MemoryBudget>,
    out: &mut FragmentBytes,
) -> Result<bool> {
    let Some(spans) = RecordSpans::exact(record, IPC_ALIGNMENT, memory)? else { return Ok(false); };
    const EOS: [u8; 8] = [0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0];
    let (metadata, body) = spans.framed_metadata().map_err(|error| invalid("fragment-encode", error))?;
    let mut schema = Vec::new();
    let message = IpcDataGenerator::default().schema_to_bytes_with_dictionary_tracker(
        record.schema_ref(), &mut DictionaryTracker::new(false), options,
    );
    write_message(&mut schema, message, options).map_err(fragment_arrow_error)?;

    let overflow = || invalid("fragment-encode", "Fragment byte length overflow");
    let length = schema.len().checked_add(metadata.len())
        .and_then(|length| length.checked_add(body))
        .and_then(|length| length.checked_add(EOS.len())).ok_or_else(overflow)?;
    let prefix = u64::try_from(length).map_err(|error| invalid("fragment-encode", error))?.to_be_bytes();
    // Pre-admit the frame so a budget refusal leaves the reply intact for the generic writer.
    out.reserve_append(length.checked_add(prefix.len()).ok_or_else(overflow)?).map_err(fragment_io)?;
    out.write_all(&prefix).map_err(fragment_io)?;
    out.write_all(&schema).map_err(fragment_io)?;
    out.write_all(&metadata).map_err(fragment_io)?;
    spans.write_body(out).map_err(fragment_io)?;
    out.write_all(&EOS).map_err(fragment_io)?;
    Ok(true)
}

/// The batches `encode_batch` wrote into `bytes`, each charged to `memory` for what it retains.
pub(crate) fn decode_batches(bytes: Buffer, memory: &Arc<MemoryBudget>) -> Result<Vec<ColumnBatch>> {
    match decode_fragment_frames(bytes.clone(), memory, true) {
        Err(error) if error.code == "rtdb/memory-budget-exceeded" => {
            // A retained prefix can tie up more of the budget than an entirely copied reply.
            decode_fragment_frames(bytes, memory, false)
        }
        result => result,
    }
}

fn decode_fragment_frames(mut rest: Buffer, memory: &Arc<MemoryBudget>, retain: bool) -> Result<Vec<ColumnBatch>> {
    let malformed = |message: &str| invalid("fragment-decode", message);
    let mut batches = Vec::new();
    while !rest.is_empty() {
        let (length, tail) = rest.as_slice().split_first_chunk::<8>()
            .ok_or_else(|| malformed("A fragment frame is truncated"))?;
        let length = usize::try_from(u64::from_be_bytes(*length)).map_err(|error| invalid("fragment-decode", error))?;
        if tail.len() < length { return Err(malformed("A fragment frame is truncated")); }
        let frame = rest.slice_with_length(8, length);
        rest.advance(8 + length);
        if retain {
            let prefix = batches.len();
            match decode_retained_fragment(frame.clone(), memory, &mut batches) {
                Ok(()) => continue,
                Err(error) if error.code == "rtdb/memory-budget-exceeded" => batches.truncate(prefix),
                Err(error) => return Err(error),
            }
        }
        decode_copied_fragment(&frame, memory, &mut batches)?;
    }
    Ok(batches)
}

fn decode_retained_fragment(frame: Buffer, memory: &Arc<MemoryBudget>, batches: &mut Vec<ColumnBatch>) -> Result<()> {
    let stream = OwnedStream::read_buffer(frame, memory).map_err(fragment_decode_error)?;
    for page in stream.into_pages() {
        batches.push(decode_fragment_batch(page, memory)?);
    }
    Ok(())
}

fn fragment_decode_error(error: Anomaly) -> Anomaly {
    if error.code == "rtdb.format/invalid-arrow" { invalid("fragment-decode", error) } else { error }
}

fn decode_copied_fragment(frame: &[u8], memory: &Arc<MemoryBudget>, batches: &mut Vec<ColumnBatch>) -> Result<()> {
    if let Some(page) = copy_standard_fragment(frame, memory)? {
        batches.push(decode_fragment_batch(page, memory)?);
    } else {
        let stream = OwnedStream::read(frame, memory).map_err(fragment_decode_error)?;
        for page in stream.into_pages() {
            batches.push(decode_fragment_batch(page, memory)?);
        }
    }
    Ok(())
}

fn standard_message(bytes: &[u8]) -> Option<(arrow_ipc::Message<'_>, &[u8], &[u8])> {
    let (continuation, bytes) = bytes.split_first_chunk::<4>()?;
    if *continuation != [255; 4] { return None; }
    let (length, bytes) = bytes.split_first_chunk::<4>()?;
    let (header, bytes) = bytes.split_at_checked(usize::try_from(i32::from_le_bytes(*length)).ok()?)?;
    let message = arrow_ipc::root_as_message(header).ok()?;
    if message.version() != arrow_ipc::MetadataVersion::V5 { return None; }
    let (body, tail) = bytes.split_at_checked(usize::try_from(message.bodyLength()).ok()?)?;
    Some((message, body, tail))
}

fn copied_metadata_bound<'a>(entries: impl Iterator<Item = arrow_ipc::KeyValue<'a>>) -> Option<usize> {
    entries.into_iter().try_fold(0usize, |bytes, entry| {
        let lengths = entry.key().map_or(0, str::len).checked_add(entry.value().map_or(0, str::len))?;
        bytes.checked_add(512)?.checked_add(lengths.checked_mul(16)?)
    })
}

fn copied_schema_bound(schema: arrow_ipc::Schema<'_>, header: usize) -> Option<usize> {
    fn field_bound(field: arrow_ipc::Field<'_>) -> Option<usize> {
        if field.dictionary().is_some() { return None; }
        let name = field.name().map_or(0, str::len);
        let timezone = field.type_as_timestamp().and_then(|ty| ty.timezone()).map_or(0, str::len);
        let union_ids = field.type_as_union().and_then(|ty| ty.typeIds()).map_or(0, |ids| ids.len());
        let metadata = copied_metadata_bound(field.custom_metadata().into_iter().flatten())?;
        let base = 2048usize.checked_add(name.checked_add(timezone)?.checked_add(union_ids)?.checked_mul(16)?)?
            .checked_add(metadata)?;
        field.children().into_iter().flatten().try_fold(base, |bytes, child| bytes.checked_add(field_bound(child)?))
    }
    let metadata = copied_metadata_bound(schema.custom_metadata().into_iter().flatten())?;
    let base = header.checked_mul(16)?.checked_add(8192)?.checked_add(metadata)?;
    schema.fields().into_iter().flatten().try_fold(base, |bytes, field| bytes.checked_add(field_bound(field)?))
}

fn copy_standard_fragment(frame: &[u8], memory: &Arc<MemoryBudget>) -> Result<Option<OwnedBatch>> {
    let Some((schema, [], rest)) = standard_message(frame) else { return Ok(None); };
    let Some(schema) = schema.header_as_schema() else { return Ok(None); };
    let Some((message, body, [255, 255, 255, 255, 0, 0, 0, 0])) = standard_message(rest) else { return Ok(None); };
    let Some(batch) = message.header_as_record_batch() else { return Ok(None); };
    let Some((nodes, buffers)) = batch.nodes().zip(batch.buffers()) else { return Ok(None); };
    if batch.compression().is_some() || nodes.iter().any(|node| node.null_count() < 0)
        || buffers.iter().any(|buffer| buffer.offset() % IPC_ALIGNMENT as i64 != 0)
    {
        return Ok(None);
    }
    rtdb_format::ipc::validate_complete_stream(frame).map_err(fragment_decode_error)?;

    // These offsets let the copy fund one aligned body instead of potential per-buffer alignment copies.
    let schema_end = frame.len() - rest.len();
    let Some(schema_bound) = copied_schema_bound(schema, schema_end) else { return Ok(None); };
    let overflow = || invalid("fragment-decode", "IPC copy allocation bound overflow");
    let descriptors = nodes.iter().try_fold(8192usize, |bytes, node| {
        let len = usize::try_from(node.length()).ok()?;
        bytes.checked_add(4096)?.checked_add(len.div_ceil(8).checked_mul(4)?)
    }).ok_or_else(overflow)?;
    let bound = buffers.len().checked_mul(512).and_then(|bytes| bytes.checked_add(descriptors))
        .and_then(|bytes| bytes.checked_add(body.len())).and_then(|bytes| bytes.checked_add(IPC_ALIGNMENT))
        .and_then(|bytes| bytes.checked_add(schema_bound)).ok_or_else(overflow)?;
    let mut charge = memory.reserve(bound)?;
    let grant = Arc::get_mut(&mut charge).expect("The fragment copy owns its grant");
    let reader = ArrowStream::from_reader(Cursor::new(&frame[..schema_end]), None).map_err(fragment_decode_error)?;
    let schema = reader.schema();
    drop(reader);
    let retained_schema = schema.fields().iter().try_fold(256usize, |bytes, field| {
        bytes.checked_add(field.size())?.checked_add(64)
    }).and_then(|bytes| {
        let metadata = schema.metadata();
        metadata.iter().try_fold(bytes.checked_add(metadata.capacity().checked_mul(64)?)?, |bytes, (key, value)| {
            bytes.checked_add(key.capacity())?.checked_add(value.capacity())
        })
    }).ok_or_else(overflow)?;
    let schema_charge = grant.split_off(retained_schema)?;
    let body = Buffer::from_slice_ref(body);
    let record = arrow_ipc::reader::read_record_batch(&body, batch, schema, &Default::default(), None, &message.version())
        .map_err(|error| invalid("fragment-decode", error))?;
    OwnedBatch::with_independent_buffers(record, grant, schema_charge, |_| false).map(Some).map_err(fragment_decode_error)
}

fn decode_fragment_batch(page: OwnedBatch, memory: &Arc<MemoryBudget>) -> Result<ColumnBatch> {
    let malformed = |message: &str| invalid("fragment-decode", message);
    let overflow = || malformed("Fragment column metadata exceeds addressable memory");
    let record = page.batch();
    let base = record.num_columns().checked_mul(size_of::<Column>())
        .and_then(|bytes| bytes.checked_add(256)).ok_or_else(overflow)?;
    let mut reservation = memory.reserve_with_backing(base, std::iter::once(Arc::clone(page.reservation())))?;
    let mut columns = Vec::with_capacity(record.num_columns());
    for (field, array) in record.schema_ref().fields().iter().zip(record.columns()) {
        if field.metadata().get(COLUMN_KEY).map(String::as_str) != Some("arrow") {
            return Err(malformed("A fragment column has no Arrow encoding"));
        }
        if !field.metadata().contains_key(NAME_KEY) { return Err(malformed("A fragment column has no name")); }
        let logical_bytes = field.metadata().get(LOGICAL_KEY).map_or(0, String::len);
        let bound = field.size().checked_mul(2)
            .and_then(|bytes| bytes.checked_add(logical_bytes.checked_mul(32)?))
            .and_then(|bytes| bytes.checked_add(512)).ok_or_else(overflow)?;
        let nested = nested_layout(array.data_type());
        let mut owner = if nested {
            memory.reserve_with_backing(bound, std::iter::once(Arc::clone(page.reservation())))?
        } else {
            memory.reserve(bound)?
        };
        let mut metadata = field.metadata().clone();
        metadata.remove(COLUMN_KEY);
        let name = metadata.remove(NAME_KEY).expect("The fragment column has a name");
        let logical = metadata.remove(LOGICAL_KEY).map(|logical| serde_json::from_str::<LogicalType>(&logical))
            .transpose().map_err(|error| invalid("fragment-decode", error))?;
        let field = Arc::new(field.as_ref().clone().with_name(name).with_metadata(metadata));
        let backing = if nested { size_of::<Arc<MemoryReservation>>() } else { 0 };
        let bytes = logical.as_ref().map_or(Ok(0), LogicalType::retained_bytes)?.checked_add(field.size())
            .and_then(|bytes| bytes.checked_add(512 + backing)).ok_or_else(overflow)?;
        let lease = Arc::get_mut(&mut owner).expect("A fragment column owns its metadata");
        lease.shrink_to(bytes)?;
        if !nested {
            Arc::get_mut(&mut reservation).expect("A fragment batch owns its metadata").absorb(lease)?;
        }
        columns.push(fragment_column(Arc::clone(array), field, logical, &owner));
    }
    Ok(ColumnBatch::sharing(columns, record.num_rows(), reservation))
}

#[cfg(test)]
mod tests {
    use rtdb_types::Value;
    use super::*;

    #[test]
    fn the_prefixes_of_a_depth_cover_every_iid_once_in_order() {
        let prefixes = FragmentDepth::new(2).unwrap().prefixes();
        assert_eq!(prefixes.len(), 16);
        assert_eq!(prefixes.first().unwrap().range().0, Iid([0; 16]));
        assert_eq!(prefixes.last().unwrap().range().1, Iid([u8::MAX; 16]));
        for pair in prefixes.windows(2) {
            let next = u128::from_be_bytes(pair[0].range().1.0).checked_add(1).unwrap();
            assert_eq!(next.to_be_bytes(), pair[1].range().0.0, "{} and {} leave no gap and do not overlap", pair[0], pair[1]);
        }
        assert!(FragmentDepth::new(0).is_err() && FragmentDepth::new(5).is_err() && TriePrefix::new(vec![4]).is_err());
    }

    fn fragment(predicates: Vec<rtdb_plan::Expr>) -> ScanFragment {
        use rtdb_plan::{QualifiedTable, TableBinding};
        let scan = Scan {
            table: TableBinding { name: QualifiedTable { database: "db".into(), table: TableRef::new("public", "docs") },
                oid: None, columns: Default::default() },
            columns: Vec::new(), valid_time: TemporalFilter::All, system_time: TemporalFilter::All,
            clamp_valid_time: false, iids: None, predicates, top_k: None,
            ordered_top_k: None, matches: Vec::new(),
        };
        ScanFragment::of(
            &scan, TriePrefix::new(vec![]).unwrap(), TransactionKey { tx_id: rtdb_types::TxId(1), system_time: Timestamp(1) },
            None, TemporalBounds { valid_time: Interval::ALL, system_time: Interval::ALL }, "UTC",
        )
    }

    #[test]
    fn fragment_argument_page_carries_typed_literals_outside_the_header() {
        use arrow_array::{Array, Float64Array, ListArray, UnionArray};
        use rtdb_plan::{BinaryOp, Expr};

        let nan = f64::from_bits(0xfff8_0000_0000_1234);
        let vector = Value::list(vec![Value::F32(-0.0), Value::Null, Value::F64(nan)]);
        let text = Expr::Literal(Value::Utf8("2026-01-01".into()));
        let timestamp = Expr::Literal(Value::Timestamp { value: 1, unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()) });
        let original = fragment(vec![
            Expr::binary(BinaryOp::Eq, timestamp, text.clone()),
            Expr::Literal(Value::Bool(false)),
            Expr::Literal(Value::Struct(rtdb_types::StructFields::from([
                ("embedding".into(), vector),
                ("omitted".into(), Value::Undefined),
                ("empty".into(), Value::list(Vec::new())),
                ("fixed_empty".into(), Value::FixedSizeList(Vec::new().into())),
            ]))),
            Expr::DurationLiteral((i64::MAX, 999).try_into().unwrap()),
            Expr::regex_replace(text, rtdb_plan::RegexReplacement::default()),
        ]);
        let memory = MemoryBudget::new(4 << 20);
        let (mut wire, bytes) = original.for_wire(&memory).unwrap();
        for predicate in &wire.scan.predicates {
            predicate.visit(&mut |expr| assert!(!matches!(expr, Expr::Literal(_) | Expr::DurationLiteral(_) | Expr::RegexReplace { .. })));
        }
        let header = serde_json::to_string(&wire).unwrap();
        assert!(!header.contains("2026-01-01"));
        let read = rtdb_format::ipc::OwnedStream::read(bytes.as_slice(), &memory).unwrap();
        let page = read.pages()[0].batch();
        assert_eq!(page.column(1).data_type(), &arrow_schema::DataType::Utf8);
        assert_eq!(page.column(2).data_type(), &arrow_schema::DataType::Boolean);
        let recipe = page.column(page.num_columns() - 1).as_any().downcast_ref::<arrow_array::StructArray>().unwrap();
        assert_eq!(recipe.column_by_name("pattern").unwrap().data_type(), &arrow_schema::DataType::Null);
        let record = page.column(3).as_any().downcast_ref::<arrow_array::StructArray>().unwrap();
        let vector = record.column(0).as_any().downcast_ref::<ListArray>().unwrap();
        let components = vector.values().as_any().downcast_ref::<UnionArray>().unwrap();
        let position = components.value_offset(2);
        let wide = components.child(components.type_id(2)).as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(wide.value(position).to_bits(), nan.to_bits());
        let charge = wire.bind_wire_literals(bytes.as_slice(), &memory).unwrap();
        assert_eq!(serde_json::to_vec(&wire).unwrap(), serde_json::to_vec(&original).unwrap());
        let Expr::Literal(value) = &wire.scan.predicates[2] else { panic!("record literal") };
        assert!(value.field("embedding").unwrap().list_view().unwrap().is_float());
        drop((wire, bytes, charge, read));
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn wide_scalar_literals_keep_their_original_bits_and_type_in_typed_columns() {
        use arrow_array::{Array, BinaryArray, Int64Array, StringArray, StructArray, UInt16Array};
        use rtdb_types::{DecimalValue, TimeUnit};

        let original = fragment(vec![
            rtdb_plan::Expr::Literal(Value::Time { value: i64::MIN, unit: TimeUnit::Millisecond, bit_width: 32 }),
            rtdb_plan::Expr::Literal(Value::Decimal(DecimalValue {
                unscaled: vec![0x80, 0x00, 0xff], precision: 999, scale: -200, bit_width: 24,
            })),
            rtdb_plan::Expr::Literal(Value::Uri("bad uri".into())),
        ]);
        let memory = MemoryBudget::new(4 << 20);
        let (mut wire, bytes) = original.for_wire(&memory).unwrap();
        let read = rtdb_format::ipc::OwnedStream::read(bytes.as_slice(), &memory).unwrap();
        let page = read.pages()[0].batch();
        let time = page.column(0).as_any().downcast_ref::<StructArray>().unwrap();
        let value = time.column_by_name("value").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(value.value(0), i64::MIN);
        let decimal = page.column(1).as_any().downcast_ref::<StructArray>().unwrap();
        let width = decimal.column_by_name("bit_width").unwrap().as_any().downcast_ref::<UInt16Array>().unwrap();
        let coefficient = decimal.column_by_name("unscaled").unwrap().as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(width.value(0), 24);
        assert_eq!(coefficient.value(0), &[0x80, 0x00, 0xff]);
        let uri = page.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(uri.value(0), "bad uri");
        let _reservation = wire.bind_wire_literals(bytes.as_slice(), &memory).unwrap();
        assert_eq!(wire, original);

        let (mut wire, bytes) = original.for_wire_v1(&memory).unwrap();
        let read = rtdb_format::ipc::OwnedStream::read(bytes.as_slice(), &memory).unwrap();
        assert!(read.pages()[0].batch().columns().iter().all(|column| column.as_any().is::<BinaryArray>()));
        let _reservation = wire.bind_wire_literals(bytes.as_slice(), &memory).unwrap();
        assert_eq!(wire, original);
    }

    #[test]
    fn regex_literal_arguments_use_arrow_and_raw_forms_preserve_their_syntax() {
        use arrow_array::{BinaryArray, Int64Array, StringArray, StructArray};
        use rtdb_plan::{Expr, RegexFormBuilder, RegexReplacement};

        let source = Expr::Literal(Value::Utf8("txt".into()));
        let mut builder = RegexFormBuilder::new_untracked();
        builder.begin_literal().unwrap();
        builder.value_text("p.*").unwrap();
        builder.end().unwrap();
        let pattern = builder.finish().unwrap();
        let mut builder = RegexFormBuilder::new_untracked();
        builder.begin_literal().unwrap();
        builder.value_leaf(&Value::I64(i64::MAX)).unwrap();
        builder.end().unwrap();
        let start = builder.finish().unwrap();
        let mut builder = RegexFormBuilder::new_untracked();
        builder.symbol("raw-pattern").unwrap();
        let symbol = builder.finish().unwrap();
        let original = fragment(vec![
            Expr::regex_replace(source.clone(), RegexReplacement { pattern, start, ..Default::default() }),
            Expr::regex_replace(source, RegexReplacement { pattern: symbol, ..Default::default() }),
        ]);
        let memory = MemoryBudget::new(4 << 20);
        let (mut wire, bytes) = original.for_wire(&memory).unwrap();
        let read = rtdb_format::ipc::OwnedStream::read(bytes.as_slice(), &memory).unwrap();
        let page = read.pages()[0].batch();
        let recipe = page.column(1).as_any().downcast_ref::<StructArray>().unwrap();
        let pattern = recipe.column_by_name("pattern").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
        let start = recipe.column_by_name("start").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(pattern.value(0), "p.*");
        assert_eq!(start.value(0), i64::MAX);
        assert!(page.column(3).as_any().is::<BinaryArray>());
        let _reservation = wire.bind_wire_literals(bytes.as_slice(), &memory).unwrap();
        assert_eq!(wire, original);

        let (mut wire, bytes) = original.for_wire_v1(&memory).unwrap();
        let _reservation = wire.bind_wire_literals(bytes.as_slice(), &memory).unwrap();
        assert_eq!(wire, original);
    }

    #[test]
    fn a_legacy_numeric_predicate_page_still_binds_at_its_original_position() {
        use rtdb_plan::{BinaryOp, Expr};
        let vector = Value::list(vec![Value::F32(0.5), Value::Null, Value::F64(-1.0)]);
        let memory = MemoryBudget::new(4 << 20);
        let logical = vector.logical_type();
        let field = rtdb_columnar::to_field("?0", &logical).unwrap();
        let column = rtdb_columnar::copy_array(&logical, std::slice::from_ref(&vector)).unwrap();
        let batch = RecordBatch::try_new(Arc::new(ArrowSchema::new(vec![field])), vec![column]).unwrap();
        let bytes = rtdb_format::ipc::write_stream(batch.schema_ref(), &[batch.clone()]).unwrap();
        let mut wire = fragment(vec![Expr::Literal(Value::Bool(true)), Expr::Literal(Value::Bool(false))]);
        let predicate = Expr::binary(BinaryOp::Eq, Expr::Parameter { index: 0, data_type: logical }, Expr::Literal(vector.clone()));
        let args = wire.bind_wire_predicates(vec![(1, predicate.clone())], FragmentBytes::from_vec(bytes, &memory).unwrap(), &memory).unwrap();
        assert_eq!(wire.scan.predicates[1], predicate);
        assert!(args.arrow_input(0).is_some());
        assert_eq!(args.get(0).unwrap(), Some(&vector));
    }

    #[test]
    fn a_fragment_packs_mixed_float_widths_and_nulls_in_one_fixed_list_frame() {
        use arrow_array::{Array, FixedSizeListArray, Float32Array, Float64Array, UnionArray};
        let memory = MemoryBudget::new(4 << 20);
        let nan = f64::from_bits(0x7ff8_0000_0000_4321);
        let mut unpacked = rtdb_types::ListValue::from(vec![Value::Bool(false), Value::Bool(true)]);
        unpacked.values_mut().unwrap().splice(.., [Value::F32(11.0), Value::F64(-0.0)]);
        let values = vec![
            Value::FixedSizeList(rtdb_types::ListValue::from_f32(Float32Array::from(vec![Some(-0.0), None]))),
            Value::Null,
            Value::FixedSizeList(rtdb_types::ListValue::from_f64(Float64Array::from(vec![Some(nan), Some(7.5)]))),
            Value::FixedSizeList(unpacked),
        ];
        let batch = ColumnBatch::settled(vec![Column::Values(values.clone())], 4, memory.reserve(4096).unwrap()).unwrap();
        let mut bytes = FragmentBytes::new(&memory).unwrap();
        encode_batch(&batch, &memory, &mut bytes).unwrap();
        let batches = decode_batches(bytes.into_ipc_buffer(), &memory).unwrap();
        let [frame] = batches.as_slice() else { panic!("one fixed-width IPC frame") };
        let Column::Selected(selection) = &frame.columns[0] else { panic!("decoded Arrow-backed selection") };
        let mut sources = selection.source_layouts();
        let (array, _, _) = sources.next().unwrap();
        assert!(sources.next().is_none());
        let array = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
        assert!(array.is_null(1));
        assert!(array.values().as_any().is::<UnionArray>());
        for (row, expected) in values.iter().enumerate() {
            assert_eq!(serde_json::to_vec(&frame.columns[0].value(row).unwrap()).unwrap(), serde_json::to_vec(expected).unwrap());
        }
    }

    #[test]
    fn fragment_bytes_admit_the_accumulated_response() {
        let memory = MemoryBudget::new(64);
        let mut out = FragmentBytes::new(&memory).unwrap();
        out.write_all(&[1; 32]).unwrap();
        out.write_all(&[2; 16]).unwrap();

        assert!(out.write_all(&[3; 17]).is_err());
        assert_eq!(out.bytes.len(), 48);
        assert!(memory.used_bytes() >= out.bytes.capacity());
        assert_eq!(&out.as_slice()[32..], &[2; 16]);
        drop(out);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn a_sliced_fragment_fits_without_staging_and_matches_the_arrow_writer() {
        use arrow_array::StringArray;
        use rtdb_types::ScalarType;

        let source_memory = MemoryBudget::new(1 << 20);
        let memory = MemoryBudget::new(256 << 10);
        let value = "x".repeat(128 << 10);
        let array: ArrayRef = Arc::new(StringArray::from(vec![Some("before"), Some(value.as_str()), None, Some("after")]));
        let field = Arc::new(ArrowField::new("value", DataType::Utf8, true)
            .with_metadata([("unit".into(), "text".into())].into()));
        let logical = LogicalType::Scalar(ScalarType::Utf8);
        let batch = ColumnBatch::settled(vec![Column::Arrow {
            array: Arc::clone(&array), field: Arc::clone(&field), logical: Some(logical.clone()),
        }], 4, source_memory.reserve(256).unwrap()).unwrap();

        let schema = Arc::new(ArrowSchema::new(vec![wire_field(0, &field, Some(&logical)).unwrap()]));
        let record = RecordBatch::try_new(Arc::clone(&schema), vec![array.slice(1, 2)]).unwrap();
        let options = IpcWriteOptions::try_new(IPC_ALIGNMENT, false, MetadataVersion::V5).unwrap();
        let mut writer = StreamWriter::try_new_with_options(Vec::new(), &schema, options).unwrap();
        writer.write(&record).unwrap();
        let expected = writer.into_inner().unwrap();

        let mut actual = FragmentBytes::new(&memory).unwrap();
        append_frame(&batch, 1..3, &memory, &mut actual, None).unwrap();
        assert_eq!(&actual.as_slice()[..8], &(expected.len() as u64).to_be_bytes());
        assert_eq!(&actual.as_slice()[8..], expected.as_slice());
        assert_eq!(memory.used_bytes(), actual.bytes.capacity());
        drop(actual);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn decoded_fragment_values_keep_the_charged_reply_allocation() {
        use arrow_array::StringArray;

        let reply_memory = MemoryBudget::new(1 << 20);
        let memory = MemoryBudget::new(1 << 20);
        let column = Column::Arrow {
            array: Arc::new(StringArray::from(vec!["retained"])),
            field: Arc::new(ArrowField::new("text", arrow_schema::DataType::Utf8, false)),
            logical: None,
        };
        let batch = ColumnBatch::settled(vec![column], 1, memory.reserve(256).unwrap()).unwrap();
        let mut reply = FragmentBytes::new(&reply_memory).unwrap();
        encode_batch(&batch, &memory, &mut reply).unwrap();
        drop(batch);
        let reply_addresses = reply.as_slice().as_ptr_range();
        let reply_charge = reply_memory.used_bytes();

        let decoded = decode_batches(reply.into_ipc_buffer(), &memory).unwrap();
        let Column::Arrow { array, .. } = &decoded[0].columns[0] else { panic!("Expected an Arrow column") };
        let array = Arc::clone(array);
        drop(decoded);

        let text = array.as_any().downcast_ref::<StringArray>().unwrap().value(0);
        assert_eq!(text, "retained");
        assert!(reply_addresses.contains(&text.as_ptr()));
        assert_eq!(reply_memory.used_bytes(), reply_charge);
        drop(array);
        assert_eq!(reply_memory.used_bytes(), 0);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn a_nested_fragment_span_keeps_its_decoded_backing_after_the_page_drops() {
        use arrow_array::{ListArray, StringArray};
        use arrow_buffer::{NullBuffer, OffsetBuffer};

        let reply_memory = MemoryBudget::new(8 << 20);
        let memory = MemoryBudget::new(8 << 20);
        let rows = 16_384;
        let text = "x".repeat(128 << 10);
        let item = Arc::new(ArrowField::new("item", DataType::Utf8, false));
        let offsets = std::iter::once(0).chain(std::iter::repeat_n(1, rows)).collect::<Vec<_>>();
        let validity = (0..rows).map(|row| row != 1).collect::<Vec<_>>();
        let array: ArrayRef = Arc::new(ListArray::new(item, OffsetBuffer::new(offsets.into()),
            Arc::new(StringArray::from(vec![text.as_str()])), Some(NullBuffer::from(validity))));
        let field = Arc::new(ArrowField::new("items", array.data_type().clone(), true));
        let input = ColumnBatch::settled(vec![Column::Arrow { array, field, logical: None }], rows, memory.reserve(256).unwrap()).unwrap();
        let mut reply = FragmentBytes::new(&reply_memory).unwrap();
        encode_batch(&input, &memory, &mut reply).unwrap();
        drop(input);
        let addresses = reply.as_slice().as_ptr_range();
        let bytes = reply.into_ipc_buffer();
        let stream = OwnedStream::read_buffer(bytes.slice(8), &memory).unwrap();
        let admitted = memory.used_bytes();
        let batch = decode_fragment_batch(stream.pages()[0].clone(), &memory).unwrap();
        assert!(memory.used_bytes() - admitted < 8 << 10);
        let Column::Selected(selected) = &batch.columns[0] else { panic!("Expected a nested selection") };
        assert_eq!(selected.fragment_source(0..rows).unwrap().1, 0..rows);
        let selection = Column::Selected(selected.clone());
        drop((bytes, stream, batch));

        let Some(Some((array, _, _, row))) = selection.arrow_cell(0) else { panic!("Expected a list") };
        let list = array.as_any().downcast_ref::<ListArray>().unwrap();
        let child = list.values().as_any().downcast_ref::<StringArray>().unwrap();
        let value = child.value(list.value_offsets()[row] as usize);
        assert_eq!(value, text);
        assert!(addresses.contains(&value.as_ptr()));
        assert_eq!(selection.value(1).unwrap(), Value::Null);
        assert_eq!(selection.value(2).unwrap(), Value::list(Vec::new()));
        assert!(reply_memory.used_bytes() > 0);
        drop(selection);
        assert_eq!(reply_memory.used_bytes(), 0);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn a_copying_frame_keeps_the_admitted_reply_prefix() {
        use arrow_array::StringArray;
        use arrow_schema::DataType;

        let source = MemoryBudget::new(16 << 20);
        let memory = MemoryBudget::new(512 << 10);
        let large = "x".repeat(128 << 10);
        let mut reply = FragmentBytes::new(&source).unwrap();
        let append = |reply: &mut FragmentBytes, text: &str| {
            let column = Column::Arrow {
                array: Arc::new(StringArray::from(vec![text])),
                field: Arc::new(ArrowField::new("text", DataType::Utf8, false)),
                logical: None,
            };
            let batch = ColumnBatch::settled(vec![column], 1, source.reserve(256).unwrap()).unwrap();
            encode_frame(&batch, 0..1, &source, reply, None).unwrap();
        };
        append(&mut reply, "prefix");
        let second = reply.as_slice().len();
        append(&mut reply, &large);
        let addresses = reply.as_slice().as_ptr_range();
        let bytes = reply.into_ipc_buffer();
        let second = bytes.slice(second + 8);
        let error = rtdb_format::ipc::OwnedStream::read_buffer(second, &memory).err().unwrap();
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");

        let detached_memory = MemoryBudget::new(512 << 10);
        let detached = decode_fragment_frames(bytes.clone(), &detached_memory, false).unwrap();
        let refused = MemoryBudget::new(large.len() - 1);
        let error = decode_fragment_frames(bytes.clone(), &refused, false).err().unwrap();
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        assert_eq!(refused.used_bytes(), 0);

        let decoded = decode_batches(bytes, &memory).unwrap();
        let [first, second] = decoded.as_slice() else { panic!("Expected two batches") };
        for (batch, expected, retained) in [(first, "prefix", true), (second, large.as_str(), false)] {
            let Column::Arrow { array, .. } = &batch.columns[0] else { panic!("Expected an Arrow column") };
            let text = array.as_any().downcast_ref::<StringArray>().unwrap().value(0);
            assert_eq!(text, expected);
            assert_eq!(addresses.contains(&text.as_ptr()), retained);
        }
        drop(decoded);
        assert_eq!(source.used_bytes(), 0);
        let Column::Arrow { array, .. } = &detached[1].columns[0] else { panic!("Expected detached Arrow") };
        let array = Arc::clone(array);
        drop(detached);
        assert_eq!(array.as_any().downcast_ref::<StringArray>().unwrap().value(0), large);
        assert!(detached_memory.used_bytes() >= large.len());
        drop(array);
        assert_eq!(detached_memory.used_bytes(), 0);
    }

    #[test]
    fn fragment_decoder_refuses_truncated_frames_and_invalid_wire_layouts() {
        use arrow_array::Int64Array;
        use arrow_ipc::writer::StreamWriter;

        let memory = MemoryBudget::new(1 << 20);
        let stream = |metadata| {
            let field = ArrowField::new("c0", arrow_schema::DataType::Int64, false).with_metadata(metadata);
            let schema = Arc::new(ArrowSchema::new(vec![field]));
            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Int64Array::from(vec![42]))]).unwrap();
            let mut writer = StreamWriter::try_new(Vec::new(), &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.into_inner().unwrap()
        };
        let decode = |frame: &[u8], length: usize| {
            let mut bytes = (length as u64).to_be_bytes().to_vec();
            bytes.extend_from_slice(frame);
            decode_batches(FragmentBytes::from_vec(bytes, &memory).unwrap().into_ipc_buffer(), &memory)
        };
        let current = stream([(COLUMN_KEY.into(), "arrow".into()), (NAME_KEY.into(), "n".into())].into());
        let decoded = decode(&current, current.len()).unwrap();
        assert_eq!(decoded[0].columns[0].value(0).unwrap(), Value::I64(42));
        drop(decoded);
        let missing_encoding = stream(Default::default());
        let missing_name = stream([(COLUMN_KEY.into(), "arrow".into())].into());
        let old = stream([(COLUMN_KEY.into(), "values".into())].into());
        let mut trailing = current.clone();
        trailing.push(0);
        for (frame, length) in [
            (current.as_slice(), current.len() + 1),
            (&current[..current.len() - 8], current.len() - 8),
            (trailing.as_slice(), trailing.len()),
            (old.as_slice(), old.len()),
            (missing_encoding.as_slice(), missing_encoding.len()),
            (missing_name.as_slice(), missing_name.len()),
        ] {
            assert_eq!(decode(frame, length).err().unwrap().code, "rtdb.query/fragment-decode");
            assert_eq!(memory.used_bytes(), 0);
        }
    }

    #[test]
    fn fragment_splits_after_a_partial_write_fails_admission() {
        let source_memory = MemoryBudget::new(32 << 20);
        let memory = MemoryBudget::new(9 << 20);
        let data = vec![9u8; 1 << 20];
        let array = arrow_array::BinaryArray::from_iter_values([data.as_slice(), data.as_slice()]);
        let column = Column::Arrow {
            array: Arc::new(array),
            field: Arc::new(ArrowField::new("data", arrow_schema::DataType::Binary, false)),
            logical: None,
        };
        let flags = Column::Arrow {
            array: Arc::new(arrow_array::BooleanArray::from(vec![true; 8]).slice(0, 2)),
            field: Arc::new(ArrowField::new("flag", DataType::Boolean, false)),
            logical: None,
        };
        let batch = ColumnBatch::settled(vec![column, flags], 2, source_memory.reserve(256).unwrap()).unwrap();
        let mut out = FragmentBytes::new(&memory).unwrap();

        encode_batch(&batch, &memory, &mut out).unwrap();

        let frames = decode_batches(out.into_ipc_buffer(), &source_memory).unwrap();
        assert_eq!(frames.iter().map(|frame| frame.len).collect::<Vec<_>>(), [1, 1]);
        for frame in frames {
            assert_eq!(frame.columns[0].value(0).unwrap(), Value::Binary(data.clone()));
            assert_eq!(frame.columns[1].value(0).unwrap(), Value::Bool(true));
        }
    }

    #[test]
    fn a_wire_field_cache_matches_position_and_ordered_logical_metadata() {
        use rtdb_types::{ScalarType, StructFields};

        let memory = MemoryBudget::new(1 << 20);
        let physical = Arc::new(ArrowField::new("value", DataType::Null, true));
        let left = LogicalType::Struct(StructFields::from([
            ("a".into(), LogicalType::Scalar(ScalarType::I64)), ("b".into(), LogicalType::Null),
        ]));
        let right = LogicalType::Struct(StructFields::from([
            ("b".into(), LogicalType::Null), ("a".into(), LogicalType::Scalar(ScalarType::I64)),
        ]));
        assert_eq!(left, right);
        let mut fields = WireFields::new();
        let first = fields.field(0, Arc::clone(&physical), Some(left.clone()), &memory).unwrap();
        let equivalent = fields.field(0, Arc::new(physical.as_ref().clone()), Some(left.clone()), &memory).unwrap();
        assert!(Arc::ptr_eq(&first, &equivalent));
        let other_position = fields.field(1, Arc::clone(&physical), Some(left), &memory).unwrap();
        assert_eq!(other_position.name(), "c1");
        let changed = fields.field(0, Arc::clone(&physical), Some(right), &memory).unwrap();
        assert_ne!(first.metadata()[LOGICAL_KEY], changed.metadata()[LOGICAL_KEY]);
        let nullable = fields.field(0, Arc::new(physical.as_ref().clone().with_nullable(false)), None, &memory).unwrap();
        assert!(!nullable.is_nullable());
        assert!(!nullable.metadata().contains_key(LOGICAL_KEY));

        let dictionary = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let unordered = Arc::new(ArrowField::new("dictionary", dictionary, true));
        let ordered = Arc::new(unordered.as_ref().clone().with_dict_is_ordered(true));
        let before = fields.field(0, unordered, None, &memory).unwrap();
        let after = fields.field(0, ordered, None, &memory).unwrap();
        assert_eq!(before.dict_is_ordered(), Some(false));
        assert_eq!(after.dict_is_ordered(), Some(true));

        let uncached = Arc::new(ArrowField::new("x".repeat(WIRE_FIELD_CACHE_BYTES), DataType::Null, true));
        fields.field(2, uncached, None, &memory).unwrap();
        assert!(fields.by_position[2].is_none());
        assert!(memory.used_bytes() <= WIRE_FIELD_CACHE_BYTES);
        drop(fields);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn wire_descriptor_charges_can_be_released_before_splitting_a_frame() {
        use arrow_array::Int64Array;

        let reply_memory = MemoryBudget::new(1 << 20);
        let probe = MemoryBudget::new(1 << 20);
        let rows = 4096;
        let column = Column::Arrow {
            array: Arc::new(Int64Array::from(vec![7; rows])),
            field: Arc::new(ArrowField::new("value", DataType::Int64, false)),
            logical: Some(LogicalType::Scalar(rtdb_types::ScalarType::I64)),
        };
        let batch = ColumnBatch::settled(vec![column], rows, reply_memory.reserve(256).unwrap()).unwrap();
        let mut expected = FragmentBytes::new(&reply_memory).unwrap();
        encode_frame(&batch, 0..rows, &probe, &mut expected, None).unwrap();

        let memory = MemoryBudget::new(probe.peak_bytes());
        let mut actual = FragmentBytes::new(&reply_memory).unwrap();
        let mut fields = WireFields::new();
        append_frame(&batch, 0..rows, &memory, &mut actual, Some(&mut fields)).unwrap();
        assert_eq!(actual.as_slice(), expected.as_slice());
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn adjacent_frames_share_wire_descriptors_and_keep_their_ipc_bytes() {
        let memory = MemoryBudget::new(1 << 20);
        let column = Column::Arrow {
            array: Arc::new(arrow_array::Int64Array::from(vec![1, 2])),
            field: Arc::new(ArrowField::new("value", DataType::Int64, false)),
            logical: Some(LogicalType::Scalar(rtdb_types::ScalarType::I64)),
        };
        let batch = ColumnBatch::settled(vec![column], 2, memory.reserve(256).unwrap()).unwrap();
        let mut cached = FragmentBytes::new(&memory).unwrap();
        let mut uncached = FragmentBytes::new(&memory).unwrap();
        let mut fields = WireFields::new();
        append_frame(&batch, 0..1, &memory, &mut cached, Some(&mut fields)).unwrap();
        let wire = Arc::clone(&fields.by_position[0].as_ref().unwrap().wire);
        append_frame(&batch, 1..2, &memory, &mut cached, Some(&mut fields)).unwrap();
        assert!(Arc::ptr_eq(&wire, &fields.by_position[0].as_ref().unwrap().wire));
        for rows in [0..1, 1..2] {
            encode_frame(&batch, rows, &memory, &mut uncached, None).unwrap();
        }
        assert_eq!(cached.as_slice(), uncached.as_slice());
    }

    #[test]
    fn sibling_layout_splits_encode_contiguous_nested_columns_under_visible_admission() {
        use arrow_array::{Array, Int64Array, ListArray, StringArray};
        use arrow_buffer::{NullBuffer, OffsetBuffer};
        use arrow_schema::DataType;
        use rtdb_types::{ListKind, ScalarType};
        use crate::columnar::SelectedColumn;

        let source_memory = MemoryBudget::new(16 << 20);
        let memory = MemoryBudget::new(256 << 10);
        let logical = LogicalType::List { kind: ListKind::List, element: Box::new(LogicalType::Scalar(ScalarType::Utf8)) };
        let field = rtdb_columnar::to_field("items", &logical).unwrap()
            .with_metadata([("unit".into(), "item".into())].into());
        let DataType::List(item) = field.data_type() else { unreachable!() };
        let unpublished = "x".repeat(1 << 20);
        let child = StringArray::from_iter_values(std::iter::once(unpublished.as_str())
            .chain(std::iter::repeat_n("item", 1024)).chain(std::iter::once(unpublished.as_str())));
        let source: ArrayRef = Arc::new(ListArray::new(Arc::clone(item), OffsetBuffer::new(vec![0, 1, 513, 513, 513, 1025, 1026].into()),
            Arc::new(child), Some(NullBuffer::from(vec![true, true, false, true, true, true]))));
        let field = Arc::new(field);
        let grant = source_memory.reserve(source.get_array_memory_size()).unwrap();
        let selected = SelectedColumn::new(vec![Some((Arc::clone(&source), Arc::clone(&field), Some(logical.clone()), Some(grant)))],
            Arc::new((1..5).map(|row| (0, row)).collect()));
        let layout = |array: ArrayRef| Some((Arc::clone(&array), Arc::new(ArrowField::new("split", array.data_type().clone(), false)), None, None));
        let split = SelectedColumn::new(vec![layout(Arc::new(Int64Array::from(vec![10, 20]))), layout(Arc::new(StringArray::from(vec!["third", "fourth"])))],
            Arc::new(vec![(0, 0), (0, 1), (1, 0), (1, 1)]));
        let direct = source.slice(1, 4);
        let batch = ColumnBatch::settled(vec![
            Column::Arrow { array: Arc::clone(&direct), field: Arc::clone(&field), logical: Some(logical.clone()) },
            Column::Selected(selected.clone()), Column::Selected(split.clone()),
        ], 4, source_memory.reserve(256).unwrap()).unwrap();
        let mut actual = FragmentBytes::new(&memory).unwrap();
        encode_batch(&batch, &memory, &mut actual).unwrap();
        assert!(actual.as_slice().len() < 32 << 10);

        let mut reference = FragmentBytes::new(&source_memory).unwrap();
        for rows in [0..2, 2..4] {
            let (array, direct_grant) = compact_arrow_rows(direct.as_ref(), &field, rows.clone(), &source_memory).unwrap();
            let ((selected_array, selected_field, selected_logical), _selected_grant) = selected.fragment_vector(rows.clone(), &source_memory).unwrap();
            let ((split_array, split_field, split_logical), _split_grant) = split.fragment_vector(rows.clone(), &source_memory).unwrap();
            let packed = ColumnBatch::settled(vec![
                Column::Arrow { array, field: Arc::clone(&field), logical: Some(logical.clone()) },
                Column::Arrow { array: selected_array, field: selected_field, logical: selected_logical },
                Column::Arrow { array: split_array, field: split_field, logical: split_logical },
            ], rows.len(), direct_grant).unwrap();
            encode_batch(&packed, &source_memory, &mut reference).unwrap();
        }
        let actual = decode_batches(actual.into_ipc_buffer(), &source_memory).unwrap();
        let reference = decode_batches(reference.into_ipc_buffer(), &source_memory).unwrap();
        assert_eq!(actual.iter().map(|frame| frame.len).collect::<Vec<_>>(), [2, 2]);
        assert_eq!(reference.len(), actual.len());
        for (actual, expected) in actual.iter().zip(&reference) {
            for (column, expected) in actual.columns.iter().zip(&expected.columns) {
                for row in 0..actual.len { assert_eq!(column.value(row).unwrap(), expected.value(row).unwrap()); }
            }
            for column in &actual.columns[..2] {
                let Column::Selected(column) = column else { panic!("expected a decoded nested field") };
                let (_, wire_field, wire_logical) = column.source_layouts().next().unwrap();
                assert_eq!(wire_field, field.as_ref());
                assert_eq!(wire_logical, Some(&logical));
            }
        }
    }

    #[test]
    fn incompatible_nested_slice_gathers_without_serializing_a_null_lists_hidden_children() {
        use arrow_array::{ListArray, StringArray};
        use arrow_buffer::{NullBuffer, OffsetBuffer};
        use arrow_schema::DataType;

        let memory = MemoryBudget::new(1 << 20);
        let item = Arc::new(ArrowField::new("item", DataType::Utf8, true));
        let array: ArrayRef = Arc::new(ListArray::new(item, OffsetBuffer::new(vec![0, 1, 2, 3].into()),
            Arc::new(StringArray::from(vec!["first", "hidden-payload-under-null", "last"])),
            Some(NullBuffer::from(vec![true, false, true]))));
        let field = Arc::new(ArrowField::new("nested", array.data_type().clone(), true));
        let batch = ColumnBatch::settled(vec![Column::Arrow { array, field, logical: None }], 3, memory.reserve(1024).unwrap()).unwrap();
        let mut out = FragmentBytes::new(&memory).unwrap();
        append_frame(&batch, 1..3, &memory, &mut out, None).unwrap();
        assert!(!out.as_slice().windows(b"hidden-payload-under-null".len()).any(|bytes| bytes == b"hidden-payload-under-null"));

        let frames = decode_batches(out.into_ipc_buffer(), &memory).unwrap();
        let [frame] = frames.as_slice() else { panic!("expected one frame") };
        assert_eq!(frame.len, 2);
        for row in 0..2 {
            assert_eq!(frame.columns[0].value(row).unwrap(), batch.columns[0].value(row + 1).unwrap());
        }
    }

    #[test]
    fn nested_slice_admission_preserves_the_existing_reply_prefix_on_refusal() {
        use arrow_array::{ListArray, StringArray};
        use arrow_buffer::OffsetBuffer;
        use arrow_schema::DataType;

        let memory = MemoryBudget::new(1 << 20);
        let reply_memory = MemoryBudget::new(256);
        let item = Arc::new(ArrowField::new("item", DataType::Utf8, false));
        let array: ArrayRef = Arc::new(ListArray::new(item, OffsetBuffer::new(vec![0, 1, 2].into()),
            Arc::new(StringArray::from(vec!["first", "last"])), None));
        let field = Arc::new(ArrowField::new("items", array.data_type().clone(), false));
        let batch = ColumnBatch::settled(vec![Column::Arrow { array, field, logical: None }], 2, memory.reserve(1024).unwrap()).unwrap();
        let source_charge = memory.used_bytes();
        let mut out = FragmentBytes::new(&reply_memory).unwrap();
        out.write_all(&[0x42; 16]).unwrap();

        let error = append_frame(&batch, 1..2, &memory, &mut out, None).unwrap_err();
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        assert_eq!(out.as_slice(), &[0x42; 16]);
        assert_eq!(memory.used_bytes(), source_charge);
    }

    #[test]
    fn a_batch_of_arrow_and_value_columns_decodes_to_the_same_cells() {
        let memory = MemoryBudget::new(64 << 20);
        let ints: arrow_array::ArrayRef = Arc::new(arrow_array::Int64Array::from(vec![Some(1), None, Some(3)]));
        let nested = vec![
            Value::Struct(rtdb_types::StructFields::from([("a".to_owned(), Value::I64(1)), ("b".to_owned(), Value::Utf8("x".into()))])),
            Value::list(vec![Value::F64(f64::NAN), Value::Null]),
            Value::Timestamp { value: 7, unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()) },
        ];
        let columns = vec![
            Column::Arrow { array: ints, field: Arc::new(ArrowField::new("total", arrow_schema::DataType::Int64, true)), logical: None },
            Column::Values(nested.clone()),
            Column::Values(vec![Value::Null; 3]),
        ];
        let batch = ColumnBatch::settled(columns, 3, memory.reserve(256).unwrap()).unwrap();
        let mut bytes = FragmentBytes::new(&memory).unwrap();
        encode_batch(&batch, &memory, &mut bytes).unwrap();
        encode_batch(&ColumnBatch::settled(Vec::new(), 2, memory.reserve(256).unwrap()).unwrap(), &memory, &mut bytes).unwrap();
        let decoded = decode_batches(bytes.into_ipc_buffer(), &memory).unwrap();
        let (empty_width, payload) = decoded.split_last().unwrap();
        assert_eq!((empty_width.len, empty_width.columns.len()), (2, 0), "a batch of no columns keeps its row count");
        let cells = |batch: &ColumnBatch| -> Vec<Vec<String>> { (0..batch.len).map(|row| batch.columns.iter().map(|column| format!("{:?}", column.value(row).unwrap())).collect()).collect() };
        assert_eq!(payload.iter().flat_map(cells).collect::<Vec<_>>(), cells(&batch));
        let Column::Arrow { field, .. } = &decoded[0].columns[0] else { panic!("an Arrow column stays one") };
        assert_eq!(field.name(), "total");
    }
}
