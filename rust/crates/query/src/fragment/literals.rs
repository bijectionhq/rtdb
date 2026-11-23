use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BinaryArray, FixedSizeListArray, ListArray, RecordBatch, StructArray, cast::AsArray};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Schema};
use rtdb_columnar::{BatchBuilder, MemoryBudget, MemoryReservation};
use rtdb_plan::{Expr, Scan, TemporalFilter};
use rtdb_types::{CalendarDate, CivilDate, DateUnit, ListValue, LogicalType, Result, ScalarType, Value};

use super::{FragmentBytes, ScanFragment, invalid, numeric_literal, portable};

const KIND: &str = "rtdb.fragment.literal";
const SLOT: &str = "\0rtdb.fragment.literal";
const REGEX: &str = "\0rtdb.fragment.regex";

impl ScanFragment {
    pub fn for_wire(&self, memory: &Arc<MemoryBudget>) -> Result<(Self, FragmentBytes)> {
        let mut fragment = self.clone();
        let mut builder = BatchBuilder::new(4096, memory)?;
        let mut fields = Vec::new();
        let mut columns = Vec::new();
        let mut add = |value: &Value, kind: Option<&str>| {
            let name = format!("?{}", columns.len());
            let (field, array) = encode_literal(&name, value, &mut builder)?;
            fields.push(match kind { Some(kind) => tagged(field, kind), None => field });
            columns.push(array);
            Ok::<_, rtdb_types::Anomaly>(columns.len() - 1)
        };
        rewrite_scan(&mut fragment.scan, |root| {
            root.rewrite(&mut |expr| {
                if matches!(expr, Expr::List(_)) && let Some(value) = numeric_literal(expr) {
                    *expr = Expr::Literal(value);
                }
            });
            let mut result = Ok(());
            root.rewrite(&mut |expr| {
                if result.is_ok() {
                    result = (|| {
                        match expr {
                            Expr::Literal(value) => {
                                let index = add(value, None)?;
                                *expr = placeholder(index, value.logical_type());
                            }
                            Expr::DurationLiteral(duration) => {
                                let (seconds, nanos) = (*duration).into();
                                let value = Value::Struct(rtdb_types::StructFields::from([
                                    ("seconds".into(), Value::I64(seconds)),
                                    ("nanos".into(), Value::U32(nanos)),
                                ]));
                                *expr = placeholder(add(&value, Some("duration"))?, expr.data_type());
                            }
                            Expr::RegexReplace { source, recipe, return_type } => {
                                let scratch = recipe.retained_bytes()?.checked_mul(4)
                                    .and_then(|bytes| bytes.checked_add(4096)).ok_or_else(malformed)?;
                                let _scratch = memory.reserve(scratch)?;
                                let encoded = serde_json::to_vec(recipe).map_err(encode_error)?;
                                let index = add(&Value::Binary(encoded), Some("regex"))?;
                                let source = std::mem::replace(source.as_mut(), Expr::Literal(Value::Null));
                                *expr = Expr::Call {
                                    name: REGEX.into(),
                                    args: vec![source, placeholder(index, LogicalType::Scalar(ScalarType::Binary))],
                                    return_type: return_type.clone(),
                                };
                            }
                            _ => {}
                        }
                        Ok(())
                    })();
                }
            });
            result
        })?;

        let mut bytes = FragmentBytes::new(memory)?;
        if !columns.is_empty() {
            let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(encode_error)?;
            let batch = builder.finish(batch)?;
            let scratch = rtdb_format::ipc::schema_translation_bound(batch.batch().schema_ref())?;
            let payload = rtdb_columnar::shared_allocation_size(batch.batch())?;
            let _scratch = memory.reserve(payload.checked_mul(4).and_then(|bytes| bytes.checked_add(scratch))
                .ok_or_else(|| encode_error("Argument encoding exceeds addressable memory"))?)?;
            rtdb_format::ipc::write_stream_to(batch.batch().schema_ref(), [batch.batch()], &mut bytes)?;
        }
        Ok((fragment, bytes))
    }

    pub fn bind_wire_literals(&mut self, bytes: &[u8], memory: &Arc<MemoryBudget>) -> Result<Arc<MemoryReservation>> {
        let stream = if bytes.is_empty() { None } else { Some(rtdb_format::ipc::OwnedStream::read(bytes, memory)?) };
        let batch = match stream.as_ref().map(|stream| stream.pages()) {
            None => None,
            Some([page]) if page.batch().num_rows() == 1 => Some(page.batch()),
            _ => return Err(malformed()),
        };
        let bound = match batch {
            Some(batch) => batch.schema_ref().fields().iter().zip(batch.columns()).try_fold(4096usize, |bytes, (field, column)| {
                bytes.checked_add(decode_bound(field, column.as_ref())?).ok_or_else(malformed)
            })?,
            None => 0,
        };
        let reservation = memory.reserve(bound)?;
        let mut unused = vec![true; batch.map_or(0, RecordBatch::num_columns)];
        rewrite_scan(&mut self.scan, |root| {
            let mut result = Ok(());
            root.rewrite(&mut |expr| {
                if result.is_ok() {
                    result = restore_expression(expr, batch, &mut unused);
                }
            });
            result
        })?;
        if unused.into_iter().any(|slot| slot) || self.scan.predicates.iter().any(|expr| !portable(expr)) {
            return Err(malformed());
        }
        Ok(reservation)
    }
}

fn rewrite_scan(scan: &mut Scan, mut rewrite: impl FnMut(&mut Expr) -> Result<()>) -> Result<()> {
    for expression in &mut scan.predicates { rewrite(expression)?; }
    for filter in [&mut scan.valid_time, &mut scan.system_time] {
        match filter {
            TemporalFilter::At(at) => rewrite(at)?,
            TemporalFilter::FromTo { from, to } | TemporalFilter::Between { from, to } => { rewrite(from)?; rewrite(to)?; }
            _ => {}
        }
    }
    for item in &mut scan.matches { rewrite(&mut item.query)?; }
    if let Some(top_k) = &mut scan.top_k {
        rewrite(&mut top_k.query)?;
        rewrite(&mut top_k.limit)?;
        if let Some(offset) = &mut top_k.offset { rewrite(offset)?; }
    }
    if let Some(top_k) = &mut scan.ordered_top_k {
        for order in &mut top_k.order { rewrite(&mut order.expr)?; }
        for residual in &mut top_k.residuals { rewrite(residual)?; }
    }
    Ok(())
}

fn tagged(field: Field, kind: &str) -> Field {
    let mut metadata = field.metadata().clone();
    metadata.insert(KIND.into(), kind.into());
    field.with_metadata(metadata)
}

fn encode_literal(name: &str, value: &Value, builder: &mut BatchBuilder) -> Result<(Field, ArrayRef)> {
    let size = encode_bound(value)?;
    let mut field = None;
    let array = builder.column(size, || {
        let (literal, array) = literal_column(name, value)?;
        field = Some(literal);
        Ok(array)
    })?;
    Ok((field.expect("literal column carries its field"), array))
}

fn children_bound<'a>(mut values: impl Iterator<Item = &'a Value>) -> Result<usize> {
    values.try_fold(4096usize, |bytes, value| bytes.checked_add(encode_bound(value)?).ok_or_else(malformed))
}

fn encode_bound(value: &Value) -> Result<usize> {
    match value {
        Value::List(list) | Value::FixedSizeList(list) => match list.array() {
            Some(_) => value.retained_bytes()?.checked_mul(2).and_then(|size| size.checked_add(4096)).ok_or_else(malformed),
            None => children_bound(list.scalar_values().expect("unpacked literal").iter()),
        },
        Value::Struct(fields) => {
            let bytes = children_bound(fields.values())?;
            fields.keys().try_fold(bytes, |bytes, name| bytes.checked_add(name.len().saturating_mul(4)).ok_or_else(malformed))
        }
        Value::Set(values) => children_bound(values.iter()),
        Value::Map { entries, .. } => children_bound(entries.iter().flat_map(|(key, value)| [key, value])),
        _ => value.retained_bytes()?.checked_mul(4).and_then(|size| size.checked_add(4096)).ok_or_else(malformed),
    }
}

fn literal_column(name: &str, value: &Value) -> Result<(Field, ArrayRef)> {
    if let Value::List(list) | Value::FixedSizeList(list) = value
        && let Some((field, child)) = list.packed_column()
    {
        let field = Arc::new(field.as_ref().clone().with_nullable(field.is_nullable() || child.is_nullable()));
        let length = i32::try_from(child.len()).map_err(encode_error)?;
        let array: ArrayRef = if matches!(value, Value::FixedSizeList(_)) {
            Arc::new(FixedSizeListArray::try_new_with_length(field, length, child, None, 1).map_err(encode_error)?)
        } else {
            let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, length]));
            Arc::new(ListArray::try_new(field, offsets, child, None).map_err(encode_error)?)
        };
        return Ok((tagged(Field::new(name, array.data_type().clone(), false), "packed"), array));
    }

    match value {
        // A literal collection can contain records with different fields and distinguish an absent field from an explicit null.
        Value::Struct(fields) => literal_struct(name, "record", fields.iter().map(|(name, value)| (name.clone(), value))),
        Value::List(items) | Value::FixedSizeList(items) => {
            let kind = if matches!(value, Value::List(_)) { "list" } else { "fixed-list" };
            let items = items.scalar_values().expect("unpacked literal");
            literal_struct(name, kind, items.iter().enumerate().map(|(index, value)| (index.to_string(), value)))
        }
        Value::Set(items) => literal_struct(name, "set", items.iter().enumerate().map(|(index, value)| (index.to_string(), value))),
        Value::Map { entries, sorted } => literal_struct(name, if *sorted { "sorted-map" } else { "map" },
            entries.iter().flat_map(|(key, value)| [key, value]).enumerate().map(|(index, value)| (index.to_string(), value))),
        Value::Undefined => Ok((tagged(Field::new(name, DataType::Null, true), "undefined"), Arc::new(arrow_array::NullArray::new(1)))),
        Value::Date(date) => {
            let kind = match date.unit() { DateUnit::Day => "calendar-day", DateUnit::Millisecond => "calendar-milli" };
            let array = arrow_array::Int64Array::from(vec![date.date().epoch_day()]);
            Ok((tagged(Field::new(name, DataType::Int64, false), kind), Arc::new(array)))
        }
        value => {
            let ty = value.logical_type();
            let typed = rtdb_columnar::to_field(name, &ty).and_then(|field| {
                rtdb_columnar::copy_array(&ty, std::slice::from_ref(value)).map(|array| (field, array))
            });
            if let Ok(typed) = typed { return Ok(typed); }
            // Row literals can represent primitives outside their physical Arrow type, such as an overflowing Time32.
            let raw = serde_json::to_vec(value).map_err(encode_error)?;
            let field = tagged(Field::new(name, DataType::Binary, false), "raw-scalar");
            Ok((field, Arc::new(BinaryArray::from_vec(vec![&raw]))))
        }
    }
}

fn literal_struct<'a>(
    name: &str, kind: &str, children: impl Iterator<Item = (String, &'a Value)>,
) -> Result<(Field, ArrayRef)> {
    let (fields, columns): (Vec<_>, Vec<_>) = children.map(|(name, value)| literal_column(&name, value))
        .collect::<Result<Vec<_>>>()?.into_iter().unzip();
    let array = StructArray::try_new_with_length(fields.into(), columns, None, 1).map_err(encode_error)?;
    Ok((tagged(Field::new(name, array.data_type().clone(), false), kind), Arc::new(array)))
}

fn decode_bound(field: &Field, array: &dyn Array) -> Result<usize> {
    let extra = match field.metadata().get(KIND).map(String::as_str) {
        Some("packed") => 0,
        Some("record" | "list" | "fixed-list" | "set" | "map" | "sorted-map") => {
            let array = array.as_struct_opt().ok_or_else(malformed)?;
            array.fields().iter().zip(array.columns()).try_fold(256usize, |bytes, (field, column)| {
                bytes.checked_add(decode_bound(field, column.as_ref())?).ok_or_else(malformed)
            })?
        }
        Some("raw-scalar" | "regex") => array.get_buffer_memory_size().checked_mul(32).ok_or_else(malformed)?,
        _ => rtdb_columnar::estimate_read_value_bytes(field, array, 0)?,
    };
    field.size().checked_mul(4).and_then(|size| size.checked_add(extra)).ok_or_else(malformed)
}

fn placeholder(index: usize, return_type: LogicalType) -> Expr {
    Expr::Call {
        name: SLOT.into(),
        args: vec![Expr::Parameter { index, data_type: LogicalType::Nothing }],
        return_type,
    }
}

fn restore_expression(expr: &mut Expr, batch: Option<&RecordBatch>, unused: &mut [bool]) -> Result<()> {
    let Expr::Call { name, args, return_type } = expr else {
        if matches!(expr, Expr::Literal(_) | Expr::DurationLiteral(_) | Expr::RegexReplace { .. }) {
            return Err(malformed());
        }
        return Ok(());
    };
    if name == REGEX {
        let [source, recipe] = args.as_mut_slice() else { return Err(malformed()); };
        let Expr::Literal(Value::Binary(recipe)) = recipe else { return Err(malformed()); };
        let recipe = serde_json::from_slice(recipe).map_err(|_| malformed())?;
        let source = Box::new(std::mem::replace(source, Expr::Literal(Value::Null)));
        *expr = Expr::RegexReplace { source, recipe, return_type: return_type.clone() };
        return Ok(());
    }
    if name != SLOT { return Ok(()); }
    let [Expr::Parameter { index, data_type: LogicalType::Nothing }] = args.as_slice() else { return Err(malformed()); };
    let batch = batch.ok_or_else(malformed)?;
    let slot = unused.get_mut(*index).ok_or_else(malformed)?;
    if !std::mem::take(slot) { return Err(malformed()); }
    let field = batch.schema_ref().field(*index);
    if field.name() != &format!("?{index}") { return Err(malformed()); }
    let array = batch.column(*index).as_ref();
    let decoded = if field.metadata().get(KIND).is_some_and(|kind| kind == "duration") {
        let value = rtdb_columnar::read_value(field, array, 0)?;
        let (Some(Value::I64(seconds)), Some(Value::U32(nanos))) = (value.field("seconds"), value.field("nanos")) else {
            return Err(malformed());
        };
        Expr::DurationLiteral((*seconds, *nanos).try_into().map_err(|_| malformed())?)
    } else {
        Expr::Literal(decode_literal(field, array)?)
    };
    if decoded.data_type() != *return_type { return Err(malformed()); }
    *expr = decoded;
    Ok(())
}

fn decode_literal(field: &Field, array: &dyn Array) -> Result<Value> {
    let kind = field.metadata().get(KIND).map(String::as_str);
    match kind {
        Some("packed") => {
            let cell = rtdb_columnar::ArrowCell { field, array, row: 0 };
            let (child, values, span) = cell.sequence_range().ok_or_else(malformed)?;
            let values = ListValue::from_numeric_arrow(Arc::new(child.clone()), values.slice(span.start, span.len()))
                .ok_or_else(malformed)?;
            match field.data_type() {
                DataType::List(_) => Ok(Value::List(values)),
                DataType::FixedSizeList(..) => Ok(Value::FixedSizeList(values)),
                _ => Err(malformed()),
            }
        }
        Some("record" | "list" | "fixed-list" | "set" | "map" | "sorted-map") => {
            let kind = kind.expect("structural literal");
            let array = array.as_struct_opt().ok_or_else(malformed)?;
            let cells = array.fields().iter().zip(array.columns());
            if kind == "record" {
                return cells.map(|(field, array)| Ok((field.name().clone(), decode_literal(field, array.as_ref())?)))
                    .collect::<Result<_>>().map(Value::Struct);
            }
            let values = cells.map(|(field, array)| decode_literal(field, array.as_ref())).collect::<Result<Vec<_>>>()?;
            Ok(match kind {
                "list" => Value::list(values),
                "fixed-list" => Value::FixedSizeList(values.into()),
                "set" => Value::Set(values),
                "map" | "sorted-map" => {
                    if values.len() % 2 != 0 { return Err(malformed()); }
                    let mut values = values.into_iter();
                    let entries = std::iter::from_fn(|| Some((values.next()?, values.next()?))).collect();
                    Value::Map { entries, sorted: kind == "sorted-map" }
                }
                _ => unreachable!("structural literal"),
            })
        },
        Some("calendar-day" | "calendar-milli") => {
            let Value::I64(day) = rtdb_columnar::read_value(field, array, 0)? else { return Err(malformed()); };
            let unit = if kind == Some("calendar-day") { DateUnit::Day } else { DateUnit::Millisecond };
            Ok(Value::Date(CalendarDate::new(CivilDate::from_epoch_day(day)?, unit)))
        }
        Some("raw-scalar") => {
            let array = array.as_binary_opt::<i32>().ok_or_else(malformed)?;
            serde_json::from_slice(array.value(0)).map_err(|_| malformed())
        }
        Some("undefined") if field.data_type() == &DataType::Null => Ok(Value::Undefined),
        _ => rtdb_columnar::read_value(field, array, 0),
    }
}

fn encode_error(error: impl std::fmt::Display) -> rtdb_types::Anomaly { invalid("fragment-argument", error) }
fn malformed() -> rtdb_types::Anomaly { encode_error("Invalid delegated literal argument") }
