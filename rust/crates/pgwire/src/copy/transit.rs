use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use rtdb_columnar::{BatchBuilder, MemoryBudget};
use rtdb_pg_codec::Format;
use rtdb_plan::CopyFormat;
use rtdb_tx::{ArrowDocuments, Documents, TxOp};
use rtdb_types::{ListKind, LogicalType, Result, StructFields, TableRef, Value};

use super::{io_error, protocol};

const PAGE_ROWS: usize = 128;

pub(super) enum TransitDocuments {
    Arrow(ArrowDocuments),
    Rows(Vec<StructFields<Value>>),
}

impl TransitDocuments {
    pub(super) fn row_count(&self) -> usize {
        match self {
            Self::Arrow(documents) => documents.row_count(),
            Self::Rows(rows) => rows.len(),
        }
    }

    pub(super) fn into_operation(self, table: TableRef, memory: &Arc<MemoryBudget>) -> Result<TxOp> {
        Ok(match self {
            Self::Arrow(documents) => TxOp::PutArrowDocs { table, documents },
            Self::Rows(rows) => TxOp::PutDocs {
                table, documents: Documents::new(rows, memory)?, valid_from: None, valid_to: None,
            },
        })
    }
}

pub(super) fn read(
    file: &mut File, format: CopyFormat, size: usize, owns_transaction: bool, memory: &Arc<MemoryBudget>,
) -> Result<TransitDocuments> {
    let mut pages = Some(DocumentPages::default());
    let mut conversion = Ok(());
    visit(file, format, size, |value| {
        if let Some(builder) = pages.as_mut() && conversion.is_ok() {
            match builder.append(value) {
                Ok(valid_id) => {
                    if !valid_id || (builder.rows == PAGE_ROWS && !builder.flush(owns_transaction, memory)) {
                        pages = None;
                    }
                }
                Err(error) => conversion = Err(error),
            }
        }
    })?;
    conversion?;
    if let Some(mut pages) = pages
        && pages.flush(owns_transaction, memory)
        && !pages.finished.is_empty()
        && let Ok(documents) = ArrowDocuments::coalesce_pages(pages.finished, memory)
    {
        return Ok(TransitDocuments::Arrow(documents));
    }

    // The row writer owns layouts whose union order or nested field presence can change across
    // pages or operations, and the original document used by a missing-ID diagnostic.
    let mut rows = Vec::new();
    let mut conversion = Ok(());
    visit(file, format, size, |value| {
        if conversion.is_ok() {
            conversion = value.and_then(|value| match value {
                Value::Struct(fields) => { rows.push(fields); Ok(()) }
                _ => Err(expected_document()),
            });
        }
    })?;
    conversion?;
    Ok(TransitDocuments::Rows(rows))
}

fn visit(file: &mut File, format: CopyFormat, size: usize, consume: impl FnMut(Result<Value>)) -> Result<()> {
    file.seek(SeekFrom::Start(0)).map_err(io_error)?;
    let reader = BufReader::new(file);
    let format = if format == CopyFormat::TransitJson { Format::Text } else { Format::Binary };
    rtdb_pg_codec::visit_transit_sequence(reader, size, format, io_error, consume)
}

fn expected_document() -> rtdb_types::Anomaly {
    protocol("COPY Transit values must be documents")
}

#[derive(Default)]
struct DocumentPages {
    rows: usize,
    columns: StructFields<Vec<(usize, Value)>>,
    types: StructFields<LogicalType>,
    finished: Vec<ArrowDocuments>,
}

impl DocumentPages {
    fn append(&mut self, value: Result<Value>) -> Result<bool> {
        let Value::Struct(fields) = value? else { return Err(expected_document()); };
        let mut valid_id = false;
        for (name, value) in fields {
            if name == "_id" {
                valid_id = matches!(&value, Value::Utf8(_) | Value::Keyword(_) | Value::I8(_) | Value::I16(_)
                    | Value::I32(_) | Value::I64(_) | Value::Uuid(_));
            }
            let column = self.columns.entry(name).or_default();
            if let Some((_, prior)) = column.last_mut().filter(|(row, _)| *row == self.rows) {
                *prior = value;
            } else {
                column.push((self.rows, value));
            }
        }
        self.rows += 1;
        Ok(valid_id)
    }

    fn flush(&mut self, owns_transaction: bool, memory: &Arc<MemoryBudget>) -> bool {
        if self.rows == 0 { return true; }
        match self.finish_page(owns_transaction, memory) {
            Ok(Some(page)) => {
                self.rows = 0;
                self.finished.push(page);
                true
            }
            Ok(None) | Err(_) => false,
        }
    }

    fn finish_page(&mut self, owns_transaction: bool, memory: &Arc<MemoryBudget>) -> Result<Option<ArrowDocuments>> {
        if !owns_transaction && !self.finished.is_empty()
            && (self.columns.len() != self.types.len() || self.columns.keys().any(|name| !self.types.contains_key(name)))
        {
            return Ok(None);
        }
        let mut output = BatchBuilder::new(4096, memory)?;
        let mut fields = Vec::with_capacity(self.columns.len());
        let mut arrays = Vec::with_capacity(self.columns.len());
        for (name, entries) in std::mem::take(&mut self.columns) {
            let mut cells = vec![Value::Null; self.rows];
            for (row, value) in entries { cells[row] = value; }
            let ty = cells.iter().fold(LogicalType::Nothing, |ty, cell| {
                let actual = cell.logical_type();
                if ty == actual { ty } else { ty.join(&actual) }
            });
            let shape = match &ty { LogicalType::Maybe(inner) => inner.as_ref(), _ => &ty };
            if !compatible(shape) || (!owns_transaction && ty.nullable()) { return Ok(None); }
            if *shape != LogicalType::Null {
                match self.types.get(&name) {
                    Some(existing) if existing != shape => return Ok(None),
                    Some(_) => {}
                    None => { self.types.insert(name.clone(), shape.clone()); }
                }
            }
            let bound = rtdb_columnar::estimate_builder_bytes(&ty, &cells)?;
            arrays.push(output.column(bound, || rtdb_columnar::copy_array(&ty, &cells))?);
            fields.push(rtdb_columnar::to_field(name, &ty)?);
        }
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(io_error)?;
        let batch = output.finish_with_metadata(batch, 4096)?;
        ArrowDocuments::from_owned_payload(&batch, memory)
    }
}

fn compatible(ty: &LogicalType) -> bool {
    match ty {
        LogicalType::Null | LogicalType::Scalar(_) => true,
        LogicalType::List { kind: ListKind::List, element } => match element.as_ref() {
            LogicalType::Maybe(inner) => matches!(inner.as_ref(), LogicalType::Scalar(_)),
            LogicalType::Null | LogicalType::Nothing | LogicalType::Scalar(_) => true,
            _ => false,
        },
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_format::transit::{self, TransitValue as T};
    use rtdb_tx::SourceTransaction;
    use std::io::Write;

    const FORMATS: [CopyFormat; 2] = [CopyFormat::TransitJson, CopyFormat::TransitMsgpack];

    fn encode(format: CopyFormat, value: &T) -> Vec<u8> {
        match format {
            CopyFormat::TransitJson => transit::encode_json(value).unwrap(),
            CopyFormat::TransitMsgpack => transit::encode_msgpack(value).unwrap(),
            _ => unreachable!(),
        }
    }

    fn read_bytes(bytes: &[u8], format: CopyFormat, owns_transaction: bool, memory: &Arc<MemoryBudget>) -> Result<TransitDocuments> {
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(bytes).unwrap();
        read(&mut file, format, bytes.len(), owns_transaction, memory)
    }

    #[test]
    fn transit_copy_combines_arrow_pages_with_exact_source_bytes() {
        let memory = MemoryBudget::new(1 << 28);
        let messages = (0..PAGE_ROWS + 2).map(|row| {
            let mut fields = vec![
                (T::Keyword("xt/id".into()), T::Integer(row as i64)),
                (T::String("vector".into()), T::Array(vec![T::Float(-0.0), T::Null, T::Float(2.5)])),
                (T::String("label".into()), if row % 2 == 0 { T::String("text".into()) } else { T::Null }),
            ];
            if row >= PAGE_ROWS {
                fields.push((T::String("new".into()), T::Bool(row % 2 == 0)));
            }
            T::Map(fields)
        }).collect::<Vec<_>>();
        for format in FORMATS {
            let bytes = messages.iter().flat_map(|message| encode(format, message)).collect::<Vec<_>>();
            let actual = read_bytes(&bytes, format, true, &memory).unwrap();
            assert_eq!(actual.row_count(), messages.len());
            let table = TableRef::new("public", "docs");
            let actual = actual.into_operation(table.clone(), &memory).unwrap();
            assert!(matches!(actual, TxOp::PutArrowDocs { .. }));
            let decoded = match format {
                CopyFormat::TransitJson => transit::decode_json_sequence(&bytes),
                _ => transit::decode_msgpack_sequence(&bytes),
            }.unwrap();
            let rows = decoded.into_iter().map(|wire| match rtdb_pg_codec::from_transit(wire).unwrap() {
                Value::Struct(fields) => fields,
                other => panic!("Expected a document, got {other:?}"),
            }).collect();
            let legacy = TransitDocuments::Rows(rows).into_operation(table, &memory).unwrap();
            let tx = |op| SourceTransaction::new(vec![op], Default::default(), &memory).unwrap();
            let (actual, legacy) = (tx(actual), tx(legacy));
            assert_eq!(actual.encode().unwrap().as_slice(), legacy.encode().unwrap().as_slice());
        }
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn transit_copy_reports_wire_errors_before_document_conversion_errors() {
        let memory = MemoryBudget::new(1 << 22);
        for (format, tail) in [(CopyFormat::TransitJson, b"\n\n}".as_slice()), (CopyFormat::TransitMsgpack, b"\xdd\0".as_slice())] {
            let mut bytes = encode(format, &T::Integer(1));
            let Err(document) = read_bytes(&bytes, format, false, &memory) else { panic!("Expected invalid document") };
            assert_eq!(document, expected_document());
            bytes.extend_from_slice(tail);
            let expected = match format {
                CopyFormat::TransitJson => transit::decode_json_sequence(&bytes),
                _ => transit::decode_msgpack_sequence(&bytes),
            }.unwrap_err();
            let Err(actual) = read_bytes(&bytes, format, false, &memory) else { panic!("Expected invalid wire value") };
            assert_eq!(actual, expected);
        }
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn transit_copy_keeps_original_missing_id_diagnostics() {
        let memory = MemoryBudget::new(1 << 22);
        let value = T::Map(vec![(T::String("field".into()), T::Null)]);
        let bytes = encode(CopyFormat::TransitJson, &value);
        let actual = read_bytes(&bytes, CopyFormat::TransitJson, true, &memory).unwrap();
        let TransitDocuments::Rows(rows) = actual else { panic!("Expected row diagnostic input") };
        assert_eq!(rows[0].keys().map(String::as_str).collect::<Vec<_>>(), ["field"]);
        assert_eq!(rows[0].get("field"), Some(&Value::Null));
    }

    #[test]
    fn transit_copy_keeps_nullable_rows_for_union_order_in_an_open_transaction() {
        let memory = MemoryBudget::new(1 << 22);
        let bytes = br#"{"_id":1,"v":null}{"_id":2,"v":7}"#;
        let auto = read_bytes(bytes, CopyFormat::TransitJson, true, &memory).unwrap();
        let explicit = read_bytes(bytes, CopyFormat::TransitJson, false, &memory).unwrap();
        assert!(matches!(auto, TransitDocuments::Arrow(_)));
        assert!(matches!(explicit, TransitDocuments::Rows(_)));

        let table = TableRef::new("public", "docs");
        let encode = |copy: TransitDocuments| {
            let earlier = TxOp::PutDocs {
                table: table.clone(),
                documents: Documents::new(vec![StructFields::from([
                    ("_id".into(), Value::I64(0)), ("v".into(), Value::Utf8("first".into())),
                ])], &memory).unwrap(),
                valid_from: None,
                valid_to: None,
            };
            let copy = copy.into_operation(table.clone(), &memory).unwrap();
            SourceTransaction::new(vec![earlier, copy], Default::default(), &memory).unwrap().encode().unwrap()
        };
        assert_ne!(encode(auto).as_slice(), encode(explicit).as_slice());
    }
}
