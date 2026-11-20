//! Scan fragments: a scan of one table split by trie prefix, so that another node can read a slice of the table at the
//! query's basis and send back the rows it resolves, in place of this node reading those tries itself.
use std::io::Cursor;
use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_schema::{Field as ArrowField, Schema as ArrowSchema};
use rtdb_columnar::{MemoryBudget, build_array, read_value, to_field};
use rtdb_plan::{Scan, TemporalFilter};
use rtdb_types::{Anomaly, AnomalyCategory, Iid, LogicalType, Result, TableRef, TemporalBounds, TransactionKey, Value};
use serde::{Deserialize, Serialize};

use crate::columnar::{Column, ColumnBatch};
use crate::context::{Execution, OuterRows, QueryContext};
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

/// One slice of a scan, as a node other than the coordinator evaluates it: the table's rows under `prefix`, resolved within
/// `bounds` over the snapshot whose latest transaction is `basis`, projected to the scan's columns and filtered by nothing.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScanFragment {
    pub scan: Scan,
    pub prefix: TriePrefix,
    pub basis: TransactionKey,
    pub bounds: TemporalBounds,
}

impl ScanFragment {
    pub fn database(&self) -> &str { &self.scan.table.name.database }
    pub fn table(&self) -> &TableRef { &self.scan.table.name.table }

    /// The fragment of `scan` under `prefix`: its predicates and temporal filters stay with the coordinator, which filters
    /// the rows that come back, so the fragment carries only the projection and the bounds already evaluated.
    pub(crate) fn of(scan: &Scan, prefix: TriePrefix, basis: TransactionKey, bounds: TemporalBounds) -> Self {
        let scan = Scan { predicates: Vec::new(), iids: None, valid_time: TemporalFilter::Default, system_time: TemporalFilter::Default, ..scan.clone() };
        Self { scan, prefix, basis, bounds }
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
}

/// Evaluates `fragment` over the context's snapshot of its database, which must be at the fragment's basis.
pub async fn evaluate_fragment(context: Arc<QueryContext>, fragment: &ScanFragment) -> Result<Vec<u8>> {
    let database = context.catalog.database(fragment.database())?.clone();
    let [snapshot] = database.partitions.as_slice() else { return Err(invalid("fragment-partitions", "A fragment reads a database of one partition")) };
    if snapshot.tx_basis() != Some(fragment.basis) {
        return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.query/fragment-basis", format!("This node's snapshot is at {:?}, not the fragment's basis {:?}", snapshot.tx_basis(), fragment.basis)));
    }
    let memory = Arc::clone(&context.memory);
    let execution = Execution { parameters: Arc::new(crate::parameters::BoundParameters::new(Vec::new(), Vec::new(), &memory)?), outer: OuterRows::empty(&memory)?, context, bindings: None, analysis: None };
    let mut scan = crate::scan::PartitionScan::fragment(snapshot.clone(), Arc::clone(&database.storage), fragment, execution);
    let mut encoded = Vec::new();
    while let Some(batch) = scan.next_batch().await? { encode_batch(&batch, &mut encoded)?; }
    Ok(encoded)
}

const COLUMN_KEY: &str = "rtdb.fragment.column";
const NAME_KEY: &str = "rtdb.fragment.name";
const LOGICAL_KEY: &str = "rtdb.fragment.logical";

/// Appends `batch` as one length-prefixed Arrow IPC stream: an Arrow column keeps its array and field, and a column of
/// values travels as the array the storage codec builds for them, read back as the same values.
pub(crate) fn encode_batch(batch: &ColumnBatch, out: &mut Vec<u8>) -> Result<()> {
    let mut fields = Vec::with_capacity(batch.columns.len());
    let mut arrays = Vec::with_capacity(batch.columns.len());
    for (position, column) in batch.columns.iter().enumerate() {
        let wire_name = format!("c{position}");
        match column {
            Column::Arrow { array, field, logical } if array.len() == batch.len => {
                let mut metadata = field.metadata().clone();
                metadata.insert(COLUMN_KEY.into(), "arrow".into());
                metadata.insert(NAME_KEY.into(), field.name().clone());
                if let Some(logical) = logical { metadata.insert(LOGICAL_KEY.into(), serde_json::to_string(logical).map_err(|error| invalid("fragment-encode", error))?); }
                fields.push(field.as_ref().clone().with_name(wire_name).with_metadata(metadata));
                arrays.push(Arc::clone(array));
            }
            column => {
                let values = (0..batch.len).map(|row| column.value(row)).collect::<Result<Vec<_>>>()?;
                let logical = LogicalType::join_all(values.iter().map(Value::logical_type).collect::<Vec<_>>().iter());
                let mut field = to_field(wire_name, &logical)?;
                let mut metadata = field.metadata().clone();
                metadata.insert(COLUMN_KEY.into(), "values".into());
                field = field.with_metadata(metadata);
                arrays.push(build_array(&logical, &values)?);
                fields.push(field);
            }
        }
    }
    let schema = Arc::new(ArrowSchema::new(fields));
    let record = RecordBatch::try_new_with_options(Arc::clone(&schema), arrays, &RecordBatchOptions::new().with_row_count(Some(batch.len))).map_err(|error| invalid("fragment-encode", error))?;
    let mut stream = Vec::new();
    {
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut stream, &schema).map_err(|error| invalid("fragment-encode", error))?;
        writer.write(&record).map_err(|error| invalid("fragment-encode", error))?;
        writer.finish().map_err(|error| invalid("fragment-encode", error))?;
    }
    out.extend_from_slice(&u64::try_from(stream.len()).map_err(|error| invalid("fragment-encode", error))?.to_be_bytes());
    out.extend_from_slice(&stream);
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
            let mut columns = Vec::with_capacity(record.num_columns());
            for (field, array) in record.schema().fields().iter().zip(record.columns()) {
                let mut metadata = field.metadata().clone();
                let kind = metadata.remove(COLUMN_KEY);
                columns.push(match kind.as_deref() {
                    Some("arrow") => {
                        let name = metadata.remove(NAME_KEY).ok_or_else(|| malformed("A fragment column has no name"))?;
                        let logical = metadata.remove(LOGICAL_KEY).map(|logical| serde_json::from_str::<LogicalType>(&logical)).transpose().map_err(|error| invalid("fragment-decode", error))?;
                        let field: Arc<ArrowField> = Arc::new(field.as_ref().clone().with_name(name).with_metadata(metadata));
                        Column::Arrow { array: Arc::clone(array), field, logical }
                    }
                    Some("values") => Column::Values((0..record.num_rows()).map(|row| read_value(field, array.as_ref(), row)).collect::<Result<_>>()?),
                    _ => return Err(malformed("A fragment column has no encoding")),
                });
            }
            batches.push(ColumnBatch::settled(columns, record.num_rows(), memory.reserve(256)?)?);
        }
    }
    Ok(batches)
}

#[cfg(test)]
mod tests {
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
        let mut bytes = Vec::new();
        encode_batch(&batch, &mut bytes).unwrap();
        encode_batch(&ColumnBatch::settled(Vec::new(), 2, memory.reserve(256).unwrap()).unwrap(), &mut bytes).unwrap();
        let decoded = decode_batches(&bytes, &memory).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!((decoded[1].len, decoded[1].columns.len()), (2, 0), "a batch of no columns keeps its row count");
        let cells = |batch: &ColumnBatch| -> Vec<Vec<String>> { (0..batch.len).map(|row| batch.columns.iter().map(|column| format!("{:?}", column.value(row).unwrap())).collect()).collect() };
        assert_eq!(cells(&decoded[0]), cells(&batch));
        let Column::Arrow { field, .. } = &decoded[0].columns[0] else { panic!("an Arrow column stays one") };
        assert_eq!(field.name(), "total");
    }
}
