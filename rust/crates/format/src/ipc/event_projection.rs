use std::{collections::HashMap, sync::Arc};

use arrow_array::{Array, ArrayRef, RecordBatch, StructArray, UnionArray};
use arrow_buffer::Buffer;
use arrow_ipc::{BodyCompression, BodyCompressionArgs, FieldNode, MetadataVersion, RecordBatchArgs};
use arrow_schema::{DataType, Schema, SchemaRef, UnionFields, UnionMode};

use rtdb_types::Result;

use super::invalid;
use crate::data::{COMMIT_POSITION_FIELD, ENTITY_KEY_FIELD, EVENT_ORIGIN_FIELD, EVENT_POSITION_FIELD};

pub(super) struct EventProjection {
    pub(super) schema: SchemaRef,
    fields: Vec<FieldSelection>,
}

enum FieldSelection {
    Keep,
    Skip,
    Children(Vec<FieldSelection>),
}

impl EventProjection {
    pub(super) fn new(source: &Schema, columns: &[String]) -> Option<Self> {
        use FieldSelection::{Children, Keep, Skip};
        let mut changed = false;
        let mut fields = Vec::new();
        let mut selections = Vec::new();
        for field in source.fields() {
            match field.name().as_str() {
                "op" => {
                    let DataType::Union(legs, UnionMode::Dense) = field.data_type() else { return None; };
                    let mut projected = Vec::new();
                    let mut select_legs = Vec::new();
                    for (id, leg) in legs.iter() {
                        if leg.name() != "put" {
                            projected.push((id, Arc::clone(leg)));
                            select_legs.push(Keep);
                            continue;
                        }
                        let DataType::Struct(children) = leg.data_type() else { return None; };
                        rtdb_columnar::from_field(leg).ok()?;
                        let mut select_children = Vec::new();
                        let mut retained = Vec::new();
                        for child in children {
                            if child.name() == "_id" || columns.contains(child.name()) {
                                retained.push(Arc::clone(child));
                                select_children.push(Keep);
                            } else {
                                changed = true;
                                select_children.push(Skip);
                            }
                        }
                        let leg = leg.as_ref().clone().with_data_type(DataType::Struct(retained.into()));
                        projected.push((id, Arc::new(leg)));
                        select_legs.push(Children(select_children));
                    }
                    let legs = UnionFields::try_new(projected.iter().map(|(id, _)| *id),
                        projected.iter().map(|(_, field)| Arc::clone(field))).ok()?;
                    fields.push(Arc::new(field.as_ref().clone().with_data_type(DataType::Union(legs, UnionMode::Dense))));
                    selections.push(Children(select_legs));
                }
                "_iid" | "_system_from" | "_valid_from" | "_valid_to" | "_recency" |
                    ENTITY_KEY_FIELD | COMMIT_POSITION_FIELD | EVENT_ORIGIN_FIELD | EVENT_POSITION_FIELD => {
                    fields.push(Arc::clone(field));
                    selections.push(Keep);
                }
                _ => {
                    selections.push(Skip);
                    changed = true;
                }
            }
        }
        changed.then(|| Self {
            schema: Arc::new(Schema::new_with_metadata(fields, source.metadata().clone())),
            fields: selections,
        })
    }

    pub(super) fn select_decoded(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let columns = self.schema.fields().iter().zip(batch.columns().iter().zip(&self.fields)
            .filter(|(_, selection)| !matches!(selection, FieldSelection::Skip))).map(|(field, (array, selection))| {
            let FieldSelection::Children(legs) = selection else { return Ok(Arc::clone(array)); };
            let source = array.as_any().downcast_ref::<UnionArray>().expect("operation union");
            let DataType::Union(fields, _) = field.data_type() else { unreachable!("projected operation union") };
            let children = fields.iter().zip(legs).map(|((id, field), selection)| {
                let child = source.child(id);
                let FieldSelection::Children(children) = selection else { return Ok(Arc::clone(child)); };
                let source = child.as_any().downcast_ref::<StructArray>().expect("put struct");
                let DataType::Struct(fields) = field.data_type() else { unreachable!("projected put struct") };
                let arrays = source.columns().iter().zip(children)
                    .filter(|(_, selection)| !matches!(selection, FieldSelection::Skip))
                    .map(|(array, _)| Arc::clone(array)).collect();
                StructArray::try_new_with_length(fields.clone(), arrays, source.nulls().cloned(), source.len())
                    .map(|array| Arc::new(array) as ArrayRef).map_err(invalid)
            }).collect::<Result<Vec<_>>>()?;
            UnionArray::try_new(fields.clone(), source.type_ids().clone(), source.offsets().cloned(), children)
                .map(|array| Arc::new(array) as ArrayRef).map_err(invalid)
        }).collect::<Result<Vec<_>>>()?;
        RecordBatch::try_new(Arc::clone(&self.schema), columns).map_err(invalid)
    }

    pub(super) fn decode(&self, source: &Schema, message: arrow_ipc::Message<'_>, body: &Buffer) -> Option<RecordBatch> {
        let batch = message.header_as_record_batch()?;
        let mut inventory = Inventory {
            nodes: batch.nodes()?.iter(),
            buffers: batch.buffers()?.iter(),
            variadic: batch.variadicBufferCounts().into_iter().flatten(),
            retained_nodes: Vec::new(),
            retained_buffers: Vec::new(),
            retained_variadic: Vec::new(),
            version: message.version(),
        };
        for (field, selection) in source.fields().iter().zip(&self.fields) {
            inventory.field(field.data_type(), selection)?;
        }
        if inventory.nodes.next().is_some() || inventory.buffers.next().is_some() || inventory.variadic.next().is_some() {
            return None;
        }
        let mut metadata = flatbuffers::FlatBufferBuilder::new();
        let nodes = metadata.create_vector(&inventory.retained_nodes);
        let buffers = metadata.create_vector(&inventory.retained_buffers);
        let variadic = metadata.create_vector(&inventory.retained_variadic);
        let compression = batch.compression().map(|compression| BodyCompression::create(&mut metadata,
            &BodyCompressionArgs { codec: compression.codec(), method: compression.method() }));
        let projected = arrow_ipc::RecordBatch::create(&mut metadata, &RecordBatchArgs {
            length: batch.length(), nodes: Some(nodes), buffers: Some(buffers), compression, variadicBufferCounts: Some(variadic),
        });
        metadata.finish(projected, None);

        // The projected inventory points into the original body, so Arrow only inflates the retained arrays.
        let projected = flatbuffers::root::<arrow_ipc::RecordBatch<'_>>(metadata.finished_data()).ok()?;
        arrow_ipc::reader::read_record_batch(body, projected, Arc::clone(&self.schema), &HashMap::new(),
            None, &message.version()).ok()
    }
}

struct Inventory<'a, V> {
    nodes: flatbuffers::VectorIter<'a, FieldNode>,
    buffers: flatbuffers::VectorIter<'a, arrow_ipc::Buffer>,
    variadic: V,
    retained_nodes: Vec<FieldNode>,
    retained_buffers: Vec<arrow_ipc::Buffer>,
    retained_variadic: Vec<i64>,
    version: MetadataVersion,
}

impl<V: Iterator<Item = i64>> Inventory<'_, V> {
    fn field(&mut self, ty: &DataType, selection: &FieldSelection) -> Option<()> {
        use FieldSelection::{Children, Keep, Skip};
        let ty = match ty { DataType::Dictionary(key, _) => key.as_ref(), other => other };
        let node = *self.nodes.next()?;
        let retain = !matches!(selection, Skip);
        if retain { self.retained_nodes.push(node); }
        let layout = arrow_data::layout(ty);
        let mut count = layout.buffers.len() + usize::from(layout.can_contain_null_mask
            || (self.version < MetadataVersion::V5 && matches!(ty, DataType::Union(..))));
        if layout.variadic {
            let variadic = self.variadic.next()?;
            count = count.checked_add(usize::try_from(variadic).ok()?)?;
            if retain { self.retained_variadic.push(variadic); }
        }
        for _ in 0..count {
            let buffer = *self.buffers.next()?;
            if retain { self.retained_buffers.push(buffer); }
        }

        let mut child = |index: usize, ty: &DataType| {
            let selection = match selection {
                Children(fields) => fields.get(index)?,
                Keep | Skip => selection,
            };
            self.field(ty, selection)
        };
        match ty {
            DataType::Struct(fields) => for (index, field) in fields.iter().enumerate() { child(index, field.data_type())?; },
            DataType::Union(fields, _) => for (index, (_, field)) in fields.iter().enumerate() { child(index, field.data_type())?; },
            DataType::List(field) | DataType::LargeList(field) | DataType::ListView(field) | DataType::LargeListView(field)
            | DataType::FixedSizeList(field, _) | DataType::Map(field, _) => child(0, field.data_type())?,
            DataType::RunEndEncoded(ends, values) => {
                child(0, ends.data_type())?;
                child(1, values.data_type())?;
            }
            _ => {}
        }
        Some(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use arrow_array::{cast::AsArray, types::TimestampMicrosecondType};
    use arrow_buffer::Buffer;
    use rtdb_types::{EventPosition, Iid, Interval, LogicalType, ScalarType, StructFields, Timestamp, TxId, Value};

    use crate::{data::{EventBatch, PutText, StoredEvent, StoredOperation, build_event_batch}, ipc::{ArrowFile, inspect_file, read_block_from}};

    #[test]
    fn search_arrow_projection_decodes_only_selected_children_and_borrows_the_ipc_body() {
        let position = EventPosition { transaction: TxId(21), ordinal: 3 };
        let rows = vec![StoredEvent { position: Some(position), iid: Iid([1; 16]), entity_key: None, origin: None, commit_position: 0,
            system_from: Timestamp(57), valid_time: Interval::ALL, recency: None,
            operation: StoredOperation::Put(StructFields::from([
                ("_id".into(), Value::I64(42)), ("body".into(), Value::Utf8("Indexed text".into())),
                ("embedding".into(), Value::list(vec![Value::F64(1.2); 2048])),
            ])),
        }];
        let double = LogicalType::Scalar(ScalarType::F64);
        let fields = StructFields::from([("_id".into(), LogicalType::Scalar(ScalarType::I64)),
            ("body".into(), LogicalType::Scalar(ScalarType::Utf8)), ("embedding".into(), LogicalType::list(double))]);
        let batch = build_event_batch(&rows, Some(fields), None).unwrap();
        let mut writer = arrow_ipc::writer::FileWriter::try_new(Vec::new(), batch.schema_ref()).unwrap();
        writer.write(&batch).unwrap();
        let buffer = Buffer::from(writer.into_inner().unwrap());
        let layout = inspect_file(&mut Cursor::new(buffer.as_slice())).unwrap();
        let mut file = ArrowFile::from_inspected_buffer(buffer.clone(), None, layout).unwrap();
        file.project_event_payload(&["body".into()]);

        let block = file.pages.block(0).unwrap();
        let encoded = read_block_from(file.buffer.as_ref(), file.reader.as_mut(), block).unwrap();
        let (header, body) = super::super::block_message(block, &encoded).unwrap();
        let message = arrow_ipc::root_as_message(&encoded[header]).unwrap();
        let page = file.event_projection.as_ref().unwrap().decode(&file.pages.source_schema, message, &encoded.slice(body))
            .expect("projection must decode before falling back to full-page reads");
        assert_eq!(page.schema(), file.schema());
        drop(file);
        let start = buffer.as_ptr() as usize;
        let timestamps = page.column_by_name("_system_from").unwrap().as_primitive::<TimestampMicrosecondType>();
        assert!((start..start + buffer.len()).contains(&(timestamps.values().as_ptr() as usize)));
        let event = EventBatch::new(page).unwrap();
        let row = event.borrow_event(0).unwrap();
        assert_eq!(event.header(0).unwrap().position, Some(position));
        assert!(matches!(row.put_text("body").unwrap(), PutText::Utf8("Indexed text")));
        assert!(matches!(row.put_text("embedding").unwrap(), PutText::Missing));
        assert!(matches!(row.put_identity().unwrap(), Some(rtdb_columnar::EntityIdRef::Integer(42))));
    }

    #[test]
    fn search_arrow_delta_keeps_the_length_delimited_replica_field() {
        use crate::log::proto::ResolvedTx;
        use prost::Message;

        let old_wire = [0x62, 3, 0x00, 0x7f, 0xff];
        let decoded = ResolvedTx::decode(old_wire.as_slice()).unwrap();
        assert_eq!(decoded.search_delta.as_deref(), Some(&old_wire[2..]));
        assert_eq!(decoded.encode_to_vec(), old_wire);
    }
}
