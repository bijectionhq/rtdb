//! Scan fragments: a scan of one table split by trie prefix, so that another node can read a slice of the table at the
//! query's basis and send back the rows it resolves, in place of this node reading those tries itself.
use std::{io::{self, Cursor, Write}, ops::Range};
use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_buffer::Buffer;
use arrow_schema::{ArrowError, Field as ArrowField, Schema as ArrowSchema};
use bytes::Bytes;
use futures::future::BoxFuture;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_plan::{Scan, TemporalFilter};
use rtdb_types::{Anomaly, AnomalyCategory, Iid, Interval, LogicalType, Result, TableRef, TemporalBounds, Timestamp, TransactionKey, Value};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::columnar::{Column, ColumnBatch, compact_arrow_rows, fragment_column, nested_layout};
use crate::context::{Execution, OuterRows, QueryContext, cancelled};
use crate::invalid;

mod literals;

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
        let stream = rtdb_format::ipc::OwnedStream::read_buffer(Buffer::from(Bytes::from_owner(bytes)), memory)?;
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
}

impl AsRef<[u8]> for FragmentBytes {
    fn as_ref(&self) -> &[u8] { self.as_slice() }
}

impl Write for FragmentBytes {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let required = self.bytes.len().checked_add(bytes.len()).ok_or_else(|| io::Error::other("Fragment byte length overflow"))?;
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
        return append_frame(batch, 0..batch.len, memory, out);
    }
    let _reservation = memory.reserve(batch.len)?;
    let mut boundaries = vec![false; batch.len];
    for column in &batch.columns {
        if let Column::Selected(selected) = column { selected.fragment_boundaries(&mut boundaries); }
    }
    let ends = boundaries.into_iter().enumerate().filter_map(|(index, boundary)| boundary.then_some(index))
        .chain(std::iter::once(batch.len));
    let mut start = 0;
    for end in ends {
        append_frame(batch, start..end, memory, out)?;
        start = end;
    }
    Ok(())
}

fn append_frame(batch: &ColumnBatch, rows: Range<usize>, memory: &Arc<MemoryBudget>, out: &mut FragmentBytes) -> Result<()> {
    let prefix = out.bytes.len();
    let result = encode_frame(batch, rows.clone(), memory, out);
    if result.is_err() { out.bytes.truncate(prefix); }
    if rows.len() > 1 && result.as_ref().is_err_and(|error| {
        matches!(error.code.as_ref(), "rtdb/run-end-selection-width" | "rtdb/memory-budget-exceeded")
    }) {
        let middle = rows.start + rows.len() / 2;
        append_frame(batch, rows.start..middle, memory, out)?;
        return append_frame(batch, middle..rows.end, memory, out);
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

fn encode_frame(batch: &ColumnBatch, rows: Range<usize>, memory: &Arc<MemoryBudget>, out: &mut FragmentBytes) -> Result<()> {
    let mut fields = Vec::with_capacity(batch.columns.len());
    let mut arrays = Vec::with_capacity(batch.columns.len());
    let mut reservations = Vec::new();
    for (position, column) in batch.columns.iter().enumerate() {
        let wire_name = format!("c{position}");
        let (array, field, logical) = match column {
            Column::Arrow { array, field, logical } if array.len() == batch.len => {
                let array = if rows.len() == batch.len {
                    Arc::clone(array)
                } else if nested_layout(array.data_type()) {
                    let (compact, reservation) = compact_arrow_rows(array.as_ref(), field, rows.clone(), memory)?;
                    reservations.push(reservation);
                    compact
                } else {
                    array.slice(rows.start, rows.len())
                };
                (array, Arc::clone(field), logical.clone())
            }
            Column::Selected(selected) => {
                let (vector, reservation) = selected.fragment_vector(rows.clone(), memory)?;
                reservations.push(reservation);
                vector
            }
            _ => return Err(invalid("fragment-encode", "Fragment column lacks its packed Arrow vector")),
        };
        let mut metadata = field.metadata().clone();
        metadata.insert(COLUMN_KEY.into(), "arrow".into());
        metadata.insert(NAME_KEY.into(), field.name().clone());
        if let Some(logical) = logical {
            metadata.insert(LOGICAL_KEY.into(), serde_json::to_string(&logical).map_err(|error| invalid("fragment-encode", error))?);
        }
        fields.push(field.as_ref().clone().with_name(wire_name).with_metadata(metadata));
        arrays.push(array);
    }
    let schema = Arc::new(ArrowSchema::new(fields));
    let record = RecordBatch::try_new_with_options(Arc::clone(&schema), arrays, &RecordBatchOptions::new().with_row_count(Some(rows.len()))).map_err(|error| invalid("fragment-encode", error))?;
    let schema_bound = rtdb_format::ipc::schema_translation_bound(&schema)?;
    let scratch = record.columns().iter().try_fold(schema_bound, |bound, array| {
        let bytes = array.to_data().get_slice_memory_size().map_err(|error| invalid("fragment-encode", error))?;
        bytes.checked_mul(4).and_then(|bytes| bound.checked_add(bytes))
            .ok_or_else(|| invalid("fragment-encode", "IPC allocation bound overflow"))
    })?;
    let _encoding = memory.reserve(scratch)?;
    let prefix = out.bytes.len();
    out.write_all(&[0; 8]).map_err(fragment_io)?;
    {
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut *out, &schema).map_err(fragment_arrow_error)?;
        writer.write(&record).map_err(fragment_arrow_error)?;
        writer.finish().map_err(fragment_arrow_error)?;
    }
    let start = prefix + 8;
    let bytes = u64::try_from(out.bytes.len() - start).map_err(|error| invalid("fragment-encode", error))?;
    out.bytes[prefix..start].copy_from_slice(&bytes.to_be_bytes());
    Ok(())
}

/// The batches `encode_batch` wrote into `bytes`, each charged to `memory` for what it retains.
pub(crate) fn decode_batches(bytes: &[u8], memory: &Arc<MemoryBudget>) -> Result<Vec<ColumnBatch>> {
    let malformed = |message: &str| invalid("fragment-decode", message);
    let mut batches = Vec::new();
    let mut rest = bytes;
    while !rest.is_empty() {
        let (length, tail) = rest.split_first_chunk::<8>().ok_or_else(|| malformed("A fragment frame is truncated"))?;
        let length = usize::try_from(u64::from_be_bytes(*length)).map_err(|error| invalid("fragment-decode", error))?;
        if tail.len() < length { return Err(malformed("A fragment frame is truncated")); }
        let (frame, tail) = tail.split_at(length);
        rest = tail;
        let reader = arrow_ipc::reader::StreamReader::try_new(Cursor::new(frame), None).map_err(|error| invalid("fragment-decode", error))?;
        for record in reader {
            let record = record.map_err(|error| invalid("fragment-decode", error))?;
            let mut reservation = memory.reserve(256)?;
            let metadata_charge = Arc::get_mut(&mut reservation).expect("A fragment batch owns its reservation");
            let mut columns = Vec::with_capacity(record.num_columns());
            for (field, array) in record.schema().fields().iter().zip(record.columns()) {
                let mut metadata = field.metadata().clone();
                let kind = metadata.remove(COLUMN_KEY);
                columns.push(match kind.as_deref() {
                    Some("arrow") => {
                        let name = metadata.remove(NAME_KEY).ok_or_else(|| malformed("A fragment column has no name"))?;
                        let logical = metadata.remove(LOGICAL_KEY).map(|logical| serde_json::from_str::<LogicalType>(&logical)).transpose().map_err(|error| invalid("fragment-decode", error))?;
                        let field: Arc<ArrowField> = Arc::new(field.as_ref().clone().with_name(name).with_metadata(metadata));
                        fragment_column(Arc::clone(array), field, logical, memory, metadata_charge)?
                    }
                    Some("values") => fragment_column(Arc::clone(array), Arc::clone(field), None, memory, metadata_charge)?,
                    _ => return Err(malformed("A fragment column has no encoding")),
                });
            }
            batches.push(ColumnBatch::settled(columns, record.num_rows(), reservation)?);
        }
    }
    Ok(batches)
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
        let batches = decode_batches(bytes.as_slice(), &memory).unwrap();
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
        let batch = ColumnBatch::settled(vec![column], 2, source_memory.reserve(256).unwrap()).unwrap();
        let mut out = FragmentBytes::new(&memory).unwrap();

        encode_batch(&batch, &memory, &mut out).unwrap();

        let frames = decode_batches(out.as_slice(), &source_memory).unwrap();
        assert_eq!(frames.iter().map(|frame| frame.len).collect::<Vec<_>>(), [1, 1]);
        for frame in frames {
            assert_eq!(frame.columns[0].value(0).unwrap(), Value::Binary(data.clone()));
        }
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
        let decoded = decode_batches(&bytes.into_bytes(), &memory).unwrap();
        let (empty_width, payload) = decoded.split_last().unwrap();
        assert_eq!((empty_width.len, empty_width.columns.len()), (2, 0), "a batch of no columns keeps its row count");
        let cells = |batch: &ColumnBatch| -> Vec<Vec<String>> { (0..batch.len).map(|row| batch.columns.iter().map(|column| format!("{:?}", column.value(row).unwrap())).collect()).collect() };
        assert_eq!(payload.iter().flat_map(cells).collect::<Vec<_>>(), cells(&batch));
        let Column::Arrow { field, .. } = &decoded[0].columns[0] else { panic!("an Arrow column stays one") };
        assert_eq!(field.name(), "total");
    }
}
