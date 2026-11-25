use std::{collections::HashMap, sync::Arc};

use arrow_array::{
    Array, DictionaryArray, RecordBatch, StringArray, StructArray,
    builder::{FixedSizeBinaryBuilder, Int32Builder, StringBuilder},
    types::Int32Type,
};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_types::{Anomaly, Result};
use sha2::{Digest, Sha256};

use crate::error::protocol;

use super::arrow_output_error;

const MAX_ENTRIES: usize = 4096;
const MAX_SPELLING_BYTES: usize = 4096;
const MAX_DICTIONARY_TEXT_BYTES: usize = 1 << 20;

pub(super) struct HistoryLabels {
    column: usize,
    fields: Fields,
    pub(super) schema: SchemaRef,
}

impl HistoryLabels {
    pub(super) fn for_schema(source: &Schema) -> Option<Self> {
        let (column, label) = source
            .column_with_name("label")
            .filter(|(_, label)| label.data_type() == &DataType::Utf8)?;
        let fields = Fields::from(vec![
            Field::new("id", DataType::FixedSizeBinary(16), false),
            Field::new("canonical", DataType::Utf8, false),
        ]);
        let dictionary = DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Struct(fields.clone())),
        );
        let mut output = source.fields().to_vec();
        output[column] = Arc::new(label.clone().with_data_type(dictionary));
        let schema = Arc::new(Schema::new(output).with_metadata(source.metadata().clone()));
        Some(Self {
            column,
            fields,
            schema,
        })
    }

    pub(super) fn encode(
        &self,
        batch: &RecordBatch,
        memory: &Arc<MemoryBudget>,
    ) -> Result<(RecordBatch, Arc<MemoryReservation>)> {
        let labels = batch
            .column(self.column)
            .as_any()
            .downcast_ref::<StringArray>()
            .filter(|labels| labels.null_count() == 0)
            .ok_or_else(|| protocol("History label dictionary requires non-null Utf8 spellings"))?;
        let distinct = labels.len().min(MAX_ENTRIES);
        let key_bytes = labels.len().checked_mul(4).ok_or_else(dictionary_bound)?;
        let _scratch = memory.reserve(4096 + distinct * 128)?;
        let mut keys = memory.reserve(
            4096usize
                .checked_add(key_bytes)
                .ok_or_else(dictionary_bound)?,
        )?;
        let mut indexes = Int32Builder::with_capacity(labels.len());
        let mut by_spelling = HashMap::with_capacity(distinct);
        let mut spellings = Vec::with_capacity(distinct);
        let mut text_bytes = 0;
        for spelling in labels.iter().flatten() {
            let index = if let Some(&index) = by_spelling.get(spelling) {
                index
            } else {
                text_bytes += spelling.len().min(MAX_SPELLING_BYTES + 1);
                if spellings.len() == MAX_ENTRIES
                    || spelling.len() > MAX_SPELLING_BYTES
                    || text_bytes > MAX_DICTIONARY_TEXT_BYTES
                {
                    return Err(dictionary_bound());
                }
                let index = spellings.len() as i32;
                by_spelling.insert(spelling, index);
                spellings.push(spelling);
                index
            };
            indexes.append_value(index);
        }
        let values_bytes = text_bytes + 32 * spellings.len() + 4096;
        let charge = Arc::get_mut(&mut keys).expect("dictionary construction owns its reservation");
        charge.grow_to(
            charge
                .bytes()
                .checked_add(values_bytes)
                .ok_or_else(dictionary_bound)?,
        )?;
        let mut ids = FixedSizeBinaryBuilder::with_capacity(spellings.len(), 16);
        let mut text = StringBuilder::with_capacity(spellings.len(), text_bytes);
        for spelling in spellings {
            ids.append_value(label_id(spelling))
                .map_err(arrow_output_error)?;
            text.append_value(spelling);
        }
        let values = StructArray::try_new(
            self.fields.clone(),
            vec![Arc::new(ids.finish()), Arc::new(text.finish())],
            None,
        )
        .map_err(arrow_output_error)?;
        let labels = DictionaryArray::<Int32Type>::try_new(indexes.finish(), Arc::new(values))
            .map_err(arrow_output_error)?;
        let mut columns = batch.columns().to_vec();
        columns[self.column] = Arc::new(labels);
        let batch =
            RecordBatch::try_new(Arc::clone(&self.schema), columns).map_err(arrow_output_error)?;
        Ok((batch, keys))
    }
}

fn dictionary_bound() -> Anomaly {
    Anomaly::incorrect(
        "rtdb.pgwire/history-label-bound",
        "History label dictionary exceeds its bound",
    )
}

fn label_id(spelling: &str) -> [u8; 16] {
    let mut hash = Sha256::new();
    hash.update(b"bijection.native-root-catalog.v1");
    hash.update(5u64.to_be_bytes());
    hash.update(b"label");
    hash.update((spelling.len() as u64).to_be_bytes());
    hash.update(spelling.as_bytes());
    let mut id = [0; 16];
    id.copy_from_slice(&hash.finalize()[..16]);
    id
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text_batch(spellings: &[&str]) -> RecordBatch {
        RecordBatch::try_from_iter([(
            "label",
            Arc::new(StringArray::from_iter_values(spellings.iter().copied())) as _,
        )])
        .unwrap()
    }

    #[test]
    fn replacement_pages_match_fresh_writer_frames() {
        use super::super::{CopyArrowEncoder, CopyChunks};

        let first = text_batch(&["[]", "top"]);
        let labels = HistoryLabels::for_schema(first.schema_ref()).unwrap();
        let memory = MemoryBudget::new(1 << 20);
        let mut encoder = CopyArrowEncoder::new(&labels.schema).unwrap();
        let header = encoder.chunks.take_header();
        for input in [first, text_batch(&["top", "[]"]), text_batch(&["top", "[]"])] {
            let (batch, _charge) = labels.encode(&input, &memory).unwrap();
            let mut previous = arrow_ipc::writer::StreamWriter::try_new(
                CopyChunks::new(), &labels.schema,
            ).unwrap();
            assert_eq!(previous.get_mut().take_header(), header);
            previous.write(&batch).unwrap();
            assert_eq!(encoder.write(&batch, true).unwrap(), previous.get_mut().take_batch());
        }
    }

    #[test]
    fn history_label_dictionary_refuses_oversized_symbols_and_budgets_allocations() {
        let batch = text_batch(&["[]", "top", "[]"]);
        let encoder = HistoryLabels::for_schema(batch.schema_ref()).unwrap();
        let denied = MemoryBudget::new(0);
        assert!(encoder.encode(&batch, &denied).is_err());
        assert_eq!(denied.used_bytes(), 0);

        let memory = MemoryBudget::new(4 << 20);
        let (output, charge) = encoder.encode(&batch, &memory).unwrap();
        assert_eq!(memory.used_bytes(), charge.bytes());
        drop(output);
        drop(charge);
        assert_eq!(memory.used_bytes(), 0);

        let long = "x".repeat(MAX_SPELLING_BYTES + 1);
        assert!(encoder.encode(&text_batch(&[&long]), &memory).is_err());
        let entries: Vec<_> = (0..=MAX_ENTRIES).map(|i| i.to_string()).collect();
        let distinct: Vec<_> = entries.iter().map(String::as_str).collect();
        assert!(
            encoder
                .encode(&text_batch(&distinct[..MAX_ENTRIES]), &memory)
                .is_ok()
        );
        assert!(encoder.encode(&text_batch(&distinct), &memory).is_err());

        let payload = "x".repeat(MAX_SPELLING_BYTES - 4);
        let entries: Vec<_> = (0..=MAX_DICTIONARY_TEXT_BYTES / MAX_SPELLING_BYTES)
            .map(|i| format!("{i:04}{payload}"))
            .collect();
        let distinct: Vec<_> = entries.iter().map(String::as_str).collect();
        let limit = MAX_DICTIONARY_TEXT_BYTES / MAX_SPELLING_BYTES;
        assert!(
            encoder
                .encode(&text_batch(&distinct[..limit]), &memory)
                .is_ok()
        );
        assert!(encoder.encode(&text_batch(&distinct), &memory).is_err());
        assert_eq!(memory.used_bytes(), 0);
    }
}
