//! Scan fragments: a scan of one table split by trie prefix, so that another node can read a slice of the table at the
//! query's basis and send back the rows it resolves, in place of this node reading those tries itself.
use std::{io::{self, Cursor, Write}, ops::Range};
use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_schema::{ArrowError, Field as ArrowField, Schema as ArrowSchema};
use futures::future::BoxFuture;
use rtdb_columnar::{MemoryBudget, MemoryReservation, read_value};
use rtdb_plan::{Scan, TemporalFilter};
use rtdb_storage::IoRegion;
use rtdb_types::{Anomaly, AnomalyCategory, Iid, Interval, LogicalType, Result, TableRef, TemporalBounds, Timestamp, TransactionKey};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::columnar::{Column, ColumnBatch, compact_arrow_rows, fragment_column, nested_layout};
use crate::context::{Execution, OuterRows, QueryContext, cancelled};
use crate::invalid;

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
    let cancellation = context.cancellation.clone();
    let owner = context.owner_cancellation.clone();
    let work = owner.run_until_cancelled(cancellation.run_until_cancelled(evaluate_retained_fragment(Arc::clone(&context), fragment)));
    IoRegion::retaining(context, work).await.flatten().ok_or_else(cancelled)?
}

async fn evaluate_retained_fragment(context: Arc<QueryContext>, fragment: &ScanFragment) -> Result<FragmentBytes> {
    let database = context.catalog.database(fragment.database())?.clone();
    let [snapshot] = database.partitions.as_slice() else { return Err(invalid("fragment-partitions", "A fragment reads a database of one partition")) };
    if snapshot.tx_basis() != Some(fragment.basis) || snapshot.has_overlays() {
        return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.query/fragment-basis", format!("This node cannot serve the fragment's basis {:?}", fragment.basis)));
    }
    let memory = Arc::clone(&context.memory);
    let execution = Execution { parameters: Arc::new(crate::parameters::BoundParameters::new(Vec::new(), Vec::new(), &memory)?), outer: OuterRows::empty(&memory)?, context, bindings: None, analysis: None };
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
                    Some("values") => Column::Values((0..record.num_rows()).map(|row| read_value(field, array.as_ref(), row)).collect::<Result<_>>()?),
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
            Value::List(vec![Value::F64(f64::NAN), Value::Null]),
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
