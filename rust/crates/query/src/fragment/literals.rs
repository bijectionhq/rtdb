use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BinaryArray, FixedSizeListArray, Int32Array, Int64Array, ListArray,
    RecordBatch, StringArray, StructArray, UInt8Array, UInt16Array, UInt32Array, cast::AsArray};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Schema};
use rtdb_columnar::{BatchBuilder, MemoryBudget, MemoryReservation};
use rtdb_plan::{Expr, RegexForm, RegexFormBuilder, RegexFormKind, RegexLiteralKind, RegexReplacement, Scan, TemporalFilter};
use rtdb_types::{CalendarDate, CivilDate, DateUnit, DecimalValue, ListValue, LogicalType, Result, ScalarType, TimeUnit, Value};

use super::{FragmentBytes, ScanFragment, invalid, numeric_literal, portable};

const KIND: &str = "rtdb.fragment.literal";
const SLOT: &str = "\0rtdb.fragment.literal";
const REGEX: &str = "\0rtdb.fragment.regex";

impl ScanFragment {
    pub fn for_wire(&self, memory: &Arc<MemoryBudget>) -> Result<(Self, FragmentBytes)> {
        self.encode_wire_literals(memory, false)
    }

    pub fn for_wire_v1(&self, memory: &Arc<MemoryBudget>) -> Result<(Self, FragmentBytes)> {
        self.encode_wire_literals(memory, true)
    }

    fn encode_wire_literals(&self, memory: &Arc<MemoryBudget>, legacy: bool) -> Result<(Self, FragmentBytes)> {
        let mut fragment = self.clone();
        let mut builder = BatchBuilder::new(4096, memory)?;
        let mut fields = Vec::new();
        let mut columns = Vec::new();
        let mut add = |size, make: &mut dyn FnMut(&str) -> Result<(Field, ArrayRef)>| {
            let name = format!("?{}", columns.len());
            let array = builder.column(size, || {
                let (field, array) = make(&name)?;
                fields.push(field);
                Ok(array)
            })?;
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
                                let index = add(encode_bound(value)?, &mut |name| literal_column(name, value, legacy))?;
                                *expr = placeholder(index, value.logical_type());
                            }
                            Expr::DurationLiteral(duration) => {
                                let (seconds, nanos) = (*duration).into();
                                let index = add(4096, &mut |name| typed_struct(name, "duration", [
                                    ("seconds", Arc::new(Int64Array::from(vec![seconds]))),
                                    ("nanos", Arc::new(UInt32Array::from(vec![nanos]))),
                                ]))?;
                                *expr = placeholder(index, expr.data_type());
                            }
                            Expr::RegexReplace { source, recipe, return_type } => {
                                let scratch = recipe.retained_bytes()?.checked_mul(4)
                                    .and_then(|bytes| bytes.checked_add(4096)).ok_or_else(malformed)?;
                                let index = if !legacy && let Some(recipe) = regex_atoms(recipe) {
                                    add(scratch, &mut |name| typed_struct(name, "regex-fields", recipe.map(|(name, atom)| (name, atom.array()))))?
                                } else {
                                    let _scratch = memory.reserve(scratch)?;
                                    let encoded = serde_json::to_vec(recipe).map_err(encode_error)?;
                                    let bound = encoded.capacity().checked_mul(4).and_then(|size| size.checked_add(4096)).ok_or_else(malformed)?;
                                    add(bound, &mut |name| Ok(binary_literal(name, "regex", &encoded)))?
                                };
                                let source = std::mem::replace(source.as_mut(), Expr::Literal(Value::Null));
                                let recipe = if legacy {
                                    placeholder(index, LogicalType::Scalar(ScalarType::Binary))
                                } else {
                                    Expr::Parameter { index, data_type: LogicalType::Nothing }
                                };
                                *expr = Expr::Call {
                                    name: REGEX.into(),
                                    args: vec![source, recipe],
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

fn literal_column(name: &str, value: &Value, legacy: bool) -> Result<(Field, ArrayRef)> {
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
        Value::Struct(fields) => literal_struct(name, "record", fields.iter().map(|(name, value)| (name.clone(), value)), legacy),
        Value::List(items) | Value::FixedSizeList(items) => {
            let kind = if matches!(value, Value::List(_)) { "list" } else { "fixed-list" };
            let items = items.scalar_values().expect("unpacked literal");
            literal_struct(name, kind, items.iter().enumerate().map(|(index, value)| (index.to_string(), value)), legacy)
        }
        Value::Set(items) => literal_struct(name, "set", items.iter().enumerate().map(|(index, value)| (index.to_string(), value)), legacy),
        Value::Map { entries, sorted } => literal_struct(name, if *sorted { "sorted-map" } else { "map" },
            entries.iter().flat_map(|(key, value)| [key, value]).enumerate().map(|(index, value)| (index.to_string(), value)), legacy),
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
            match typed {
                Ok(typed) => Ok(typed),
                Err(_) if legacy => Ok(binary_literal(name, "raw-scalar", &serde_json::to_vec(value).map_err(encode_error)?)),
                Err(error) => wide_scalar(name, value).unwrap_or(Err(error)),
            }
        }
    }
}

fn literal_struct<'a>(
    name: &str, kind: &str, children: impl Iterator<Item = (String, &'a Value)>, legacy: bool,
) -> Result<(Field, ArrayRef)> {
    let (fields, columns): (Vec<_>, Vec<_>) = children.map(|(name, value)| literal_column(&name, value, legacy))
        .collect::<Result<Vec<_>>>()?.into_iter().unzip();
    let array = StructArray::try_new_with_length(fields.into(), columns, None, 1).map_err(encode_error)?;
    Ok((tagged(Field::new(name, array.data_type().clone(), false), kind), Arc::new(array)))
}

fn typed_struct<const N: usize>(name: &str, kind: &str, children: [(&str, ArrayRef); N]) -> Result<(Field, ArrayRef)> {
    let (fields, arrays): (Vec<_>, Vec<_>) = children.into_iter().map(|(name, array)| {
        (Field::new(name, array.data_type().clone(), array.is_nullable()), array)
    }).unzip();
    let array = StructArray::try_new_with_length(fields.into(), arrays, None, 1).map_err(encode_error)?;
    Ok((tagged(Field::new(name, array.data_type().clone(), false), kind), Arc::new(array)))
}

fn binary_literal(name: &str, kind: &str, bytes: &[u8]) -> (Field, ArrayRef) {
    (tagged(Field::new(name, DataType::Binary, false), kind), Arc::new(BinaryArray::from_vec(vec![bytes])))
}

fn wide_scalar(name: &str, value: &Value) -> Option<Result<(Field, ArrayRef)>> {
    // Admission belongs to the evaluator. The wire must carry the unadmitted value exactly.
    Some(match value {
        Value::Time { value, unit, bit_width } => {
            let unit = match unit { TimeUnit::Second => 0, TimeUnit::Millisecond => 1, TimeUnit::Microsecond => 2, TimeUnit::Nanosecond => 3 };
            typed_struct(name, "wide-time", [
                ("value", Arc::new(Int64Array::from(vec![*value]))),
                ("unit", Arc::new(UInt8Array::from(vec![unit]))),
                ("bit_width", Arc::new(UInt8Array::from(vec![*bit_width]))),
            ])
        }
        Value::Decimal(decimal) => typed_struct(name, "wide-decimal", [
            ("unscaled", Arc::new(BinaryArray::from_vec(vec![&decimal.unscaled]))),
            ("precision", Arc::new(Int32Array::from(vec![decimal.precision]))),
            ("scale", Arc::new(Int32Array::from(vec![decimal.scale]))),
            ("bit_width", Arc::new(UInt16Array::from(vec![decimal.bit_width]))),
        ]),
        Value::Uri(text) => Ok((tagged(Field::new(name, DataType::Utf8, false), "raw-uri"),
            Arc::new(StringArray::from(vec![text.as_str()])))),
        _ => return None,
    })
}

#[derive(Clone, Copy)]
enum RegexAtom<'a> { Null, Text(&'a str), Integer(i64), Boolean(bool) }

impl RegexAtom<'_> {
    fn array(self) -> ArrayRef {
        match self {
            Self::Null => Arc::new(arrow_array::NullArray::new(1)),
            Self::Text(text) => Arc::new(StringArray::from(vec![text])),
            Self::Integer(value) => Arc::new(Int64Array::from(vec![value])),
            Self::Boolean(value) => Arc::new(arrow_array::BooleanArray::from(vec![value])),
        }
    }
}

fn regex_atoms(recipe: &RegexReplacement) -> Option<[(&'static str, RegexAtom<'_>); 5]> {
    fn atom(form: &RegexForm) -> Option<RegexAtom<'_>> {
        let RegexFormKind::Literal(literal) = form.view().kind() else { return None; };
        match literal.kind() {
            RegexLiteralKind::Text(text) => Some(RegexAtom::Text(text)),
            RegexLiteralKind::Leaf(leaf) => match leaf.value().as_ref() {
                Value::Null => Some(RegexAtom::Null),
                Value::I64(value) => Some(RegexAtom::Integer(*value)),
                Value::Bool(value) => Some(RegexAtom::Boolean(*value)),
                _ => None,
            },
            _ => None,
        }
    }
    Some([
        ("pattern", atom(&recipe.pattern)?), ("replacement", atom(&recipe.replacement)?),
        ("start", atom(&recipe.start)?), ("occurrence", atom(&recipe.occurrence)?),
        ("flags", atom(&recipe.flags)?),
    ])
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
        Some(kind @ ("raw-scalar" | "regex" | "regex-fields")) => array.get_buffer_memory_size().checked_mul(32)
            .and_then(|size| size.checked_add(if kind == "regex-fields" { 16 << 10 } else { 0 }))
            .ok_or_else(malformed)?,
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
        let recipe = match recipe {
            Expr::Literal(Value::Binary(bytes)) => serde_json::from_slice(bytes).map_err(|_| malformed())?,
            _ => {
                let (field, array, slot) = literal_slot(std::slice::from_ref(recipe), batch, unused)?;
                let recipe = decode_regex(field, array)?;
                *slot = false;
                recipe
            }
        };
        let source = Box::new(std::mem::replace(source, Expr::Literal(Value::Null)));
        *expr = Expr::RegexReplace { source, recipe, return_type: return_type.clone() };
        return Ok(());
    }
    if name != SLOT { return Ok(()); }
    let (field, array, slot) = literal_slot(args, batch, unused)?;
    let decoded = match field.metadata().get(KIND).map(String::as_str) {
        Some("regex-fields") => return Err(malformed()),
        Some("duration") => {
            let array = non_null::<StructArray>(array)?;
            let seconds = struct_child::<Int64Array>(array, "seconds")?.value(0);
            let nanos = struct_child::<UInt32Array>(array, "nanos")?.value(0);
            Expr::DurationLiteral((seconds, nanos).try_into().map_err(|_| malformed())?)
        }
        _ => Expr::Literal(decode_literal(field, array)?),
    };
    if decoded.data_type() != *return_type { return Err(malformed()); }
    *slot = false;
    *expr = decoded;
    Ok(())
}

fn literal_slot<'page, 'slots>(args: &[Expr], batch: Option<&'page RecordBatch>, unused: &'slots mut [bool])
    -> Result<(&'page Field, &'page dyn Array, &'slots mut bool)> {
    let [Expr::Parameter { index, data_type: LogicalType::Nothing }] = args else { return Err(malformed()); };
    let batch = batch.ok_or_else(malformed)?;
    let slot = unused.get_mut(*index).filter(|slot| **slot).ok_or_else(malformed)?;
    let field = batch.schema_ref().field(*index);
    if field.name() != &format!("?{index}") { return Err(malformed()); }
    Ok((field, batch.column(*index).as_ref(), slot))
}

fn non_null<T: 'static>(array: &dyn Array) -> Result<&T> {
    if array.is_empty() || array.is_null(0) { return Err(malformed()); }
    array.as_any().downcast_ref().ok_or_else(malformed)
}

fn struct_child<'a, T: 'static>(array: &'a StructArray, name: &str) -> Result<&'a T> {
    non_null(array.column_by_name(name).ok_or_else(malformed)?.as_ref())
}

fn decode_regex(field: &Field, array: &dyn Array) -> Result<RegexReplacement> {
    match field.metadata().get(KIND).map(String::as_str) {
        Some("regex") => serde_json::from_slice(non_null::<BinaryArray>(array)?.value(0)).map_err(|_| malformed()),
        Some("regex-fields") => {
            let array = non_null::<StructArray>(array)?;
            let form = |name| {
                let child = array.column_by_name(name).ok_or_else(malformed)?.as_ref();
                if child.data_type() == &DataType::Null { return Ok(RegexForm::default()); }
                let mut builder = RegexFormBuilder::new_untracked();
                builder.begin_literal()?;
                match child.data_type() {
                    DataType::Utf8 => builder.value_text(non_null::<StringArray>(child)?.value(0))?,
                    DataType::Int64 => builder.value_leaf(&Value::I64(non_null::<Int64Array>(child)?.value(0)))?,
                    DataType::Boolean => builder.value_leaf(&Value::Bool(non_null::<arrow_array::BooleanArray>(child)?.value(0)))?,
                    _ => return Err(malformed()),
                }
                builder.end()?;
                builder.finish()
            };
            Ok(RegexReplacement { pattern: form("pattern")?, replacement: form("replacement")?,
                start: form("start")?, occurrence: form("occurrence")?, flags: form("flags")? })
        }
        _ => Err(malformed()),
    }
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
            serde_json::from_slice(non_null::<BinaryArray>(array)?.value(0)).map_err(|_| malformed())
        }
        Some("wide-time") => {
            let array = non_null::<StructArray>(array)?;
            let unit = match struct_child::<UInt8Array>(array, "unit")?.value(0) {
                0 => TimeUnit::Second, 1 => TimeUnit::Millisecond, 2 => TimeUnit::Microsecond, 3 => TimeUnit::Nanosecond,
                _ => return Err(malformed()),
            };
            Ok(Value::Time { value: struct_child::<Int64Array>(array, "value")?.value(0), unit,
                bit_width: struct_child::<UInt8Array>(array, "bit_width")?.value(0) })
        }
        Some("wide-decimal") => {
            let array = non_null::<StructArray>(array)?;
            Ok(Value::Decimal(DecimalValue {
                unscaled: struct_child::<BinaryArray>(array, "unscaled")?.value(0).to_vec(),
                precision: struct_child::<Int32Array>(array, "precision")?.value(0),
                scale: struct_child::<Int32Array>(array, "scale")?.value(0),
                bit_width: struct_child::<UInt16Array>(array, "bit_width")?.value(0),
            }))
        }
        Some("raw-uri") => Ok(Value::Uri(non_null::<StringArray>(array)?.value(0).to_owned())),
        Some("undefined") if field.data_type() == &DataType::Null => Ok(Value::Undefined),
        _ => rtdb_columnar::read_value(field, array, 0),
    }
}

fn encode_error(error: impl std::fmt::Display) -> rtdb_types::Anomaly { invalid("fragment-argument", error) }
fn malformed() -> rtdb_types::Anomaly { encode_error("Invalid delegated literal argument") }
