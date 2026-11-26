use rtdb_types::StructFields;
use std::{alloc::Layout, cell::RefCell, collections::{BTreeMap, BTreeSet}, mem::size_of, sync::Arc};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_plan::{AggregateFunction, BinaryOp, ColumnId, Expr, Field, JoinKind, Relation, RelationNode, Schema, UnaryOp,
    RegexFormView, RegexFormKind as F, RegexFormIter, RegexFormPairIter,
    RegexLiteralView, RegexLiteralKind as LiteralKind, RegexLiteralIter, RegexLiteralPairIter, RegexFieldIter};
use rtdb_types::{AllocationScope, CheckedIndex, ClojureKeyIndex, KeySemantics, Keyword, LogicalType, Result, ScalarType, Value, clojure_string_hasheq, clojure_symbol_hasheq};
use crate::invalid;

pub(crate) struct ExplainPlan { pub schema: Schema, rows: Arc<Vec<Vec<Value>>>, retained_bytes: usize, _reservation: Arc<MemoryReservation> }
impl ExplainPlan {
    pub fn new(relation: &Relation, memory: &Arc<MemoryBudget>) -> Result<Arc<Self>> {
        let base = crate::runtime_schema::allocation_bound(relation)?.checked_mul(8).and_then(|bytes| bytes.checked_add(depth_bound(relation, 0).ok()?)).ok_or_else(size_error)?;
        with_work(memory, base, |work| {
            let descriptor = describe(relation, work)?;
            let mut rows = WorkRows::new(work);
            descriptor.rows(0, &mut rows)?;
            let explanation = LogicalType::join_all(&rows.rows.iter().map(|row| row[2].logical_type()).collect::<Vec<_>>());
            let schema = Schema::new(vec![("depth", LogicalType::Scalar(ScalarType::Utf8)), ("op", LogicalType::Scalar(ScalarType::Keyword)), ("explain", explanation)]
                .into_iter().enumerate().map(|(index, (name, data_type))| Field { id: ColumnId(index as u32), name: name.into(), data_type, qualifier: None, qualified_only: false, unnamed: false }).collect())?;
            let retained_bytes = rows.rows.iter().try_fold(4096usize, |bytes, row| row.iter().try_fold(bytes, |bytes, value| bytes.checked_add(value.retained_bytes()?).ok_or_else(size_error)))?;
            rows.publish(schema, retained_bytes)
        })
    }
    pub fn copy_bound(&self) -> Result<usize> { self.retained_bytes.checked_mul(2).and_then(|n| n.checked_add(self.rows.len().checked_mul(512)?)).ok_or_else(size_error) }
    pub fn rows(&self) -> &[Vec<Value>] { &self.rows }
    pub fn recharged(&self, memory: &Arc<MemoryBudget>) -> Result<Arc<Self>> {
        let reservation = memory.reserve(self._reservation.bytes())?;
        Ok(Arc::new(Self { schema: self.schema.clone(), rows: Arc::clone(&self.rows), retained_bytes: self.retained_bytes, _reservation: reservation }))
    }

}
fn size_error() -> rtdb_types::Anomaly { invalid("explain-size", "Plan description exceeds addressable memory") }
fn region_error() -> rtdb_types::Anomaly { invalid("explain-region", "Plan description belongs to a different construction region") }
fn allocation_error() -> rtdb_types::Anomaly { rtdb_types::Anomaly::incorrect("rtdb/allocation-overflow", "Execution vector allocation bound exceeds usize") }
fn add(left: usize, right: usize) -> Result<usize> { left.checked_add(right).ok_or_else(size_error) }
fn array_bytes<T>(capacity: usize) -> Result<usize> { Ok(Layout::array::<T>(capacity).map_err(|_| size_error())?.size()) }
fn map_node_bytes<K, V>() -> Result<usize> { add(size_of::<(K, V)>().checked_mul(16).ok_or_else(size_error)?, 256) }

enum WorkState {
    Building { reservation: Arc<MemoryReservation>, base_bytes: usize, cumulative_buffers: usize },
    Published,
}
struct ExplainWork { memory: Arc<MemoryBudget>, state: RefCell<WorkState> }
fn with_work(memory: &Arc<MemoryBudget>, base: usize, body: impl for<'work> FnOnce(&'work ExplainWork) -> Result<Arc<ExplainPlan>>) -> Result<Arc<ExplainPlan>> {
    let reservation = memory.reserve(base)?;
    let work = ExplainWork { memory: Arc::clone(memory), state: RefCell::new(WorkState::Building { reservation, base_bytes: base, cumulative_buffers: 0 }) };
    body(&work)
}
impl ExplainWork {
    fn admit(&self, bytes: usize) -> Result<()> {
        let mut state = self.state.borrow_mut();
        let WorkState::Building { reservation, base_bytes, cumulative_buffers } = &mut *state else { return Err(region_error()); };
        let next = add(*cumulative_buffers, bytes)?;
        Arc::get_mut(reservation).ok_or_else(region_error)?.grow_to(add(*base_bytes, next)?)?;
        *cumulative_buffers = next;
        Ok(())
    }
    fn check(&self, other: &Self) -> Result<()> { if std::ptr::eq(self, other) { Ok(()) } else { Err(region_error()) } }
    fn pieces<'work>(&'work self, pieces: &[RenderPiece<'_, 'work>]) -> Result<WorkText<'work>> {
        self.check_pieces(pieces)?;
        let mut count = CountSink { bytes: 0 };
        write_pieces(pieces, &mut count)?;
        self.admit(count.bytes)?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(count.bytes).map_err(|_| allocation_error())?;
        let mut sink = FillSink { bytes: &mut bytes, limit: count.bytes };
        write_pieces(pieces, &mut sink)?;
        if bytes.len() != count.bytes { return Err(size_error()); }
        Ok(WorkText { text: String::from_utf8(bytes).map_err(|_| size_error())?, work: self })
    }
    fn copy_text<'work>(&'work self, source: &str) -> Result<WorkText<'work>> { self.pieces(&[RenderPiece::Text(source)]) }
    fn check_pieces(&self, pieces: &[RenderPiece<'_, '_>]) -> Result<()> {
        for piece in pieces { if let RenderPiece::Join(values, _) = piece { for value in *values { self.check(value.work)?; } } }
        Ok(())
    }
    fn text<'work>(&'work self, text: WorkText<'work>) -> Result<WorkValue<'work>> {
        self.check(text.work)?;
        let final_buffers = text.text.capacity();
        Ok(WorkValue { value: Value::Utf8(text.text), final_buffers, work: self })
    }
    fn keyword<'work>(&'work self, text: WorkText<'work>) -> Result<WorkValue<'work>> {
        self.check(text.work)?;
        let final_buffers = text.text.capacity();
        Ok(WorkValue { value: Value::Keyword(Keyword::from_name(text.text)), final_buffers, work: self })
    }
    fn null(&self) -> WorkValue<'_> { WorkValue { value: Value::Null, final_buffers: 0, work: self } }
    fn boolean(&self, value: bool) -> WorkValue<'_> { WorkValue { value: Value::Bool(value), final_buffers: 0, work: self } }
    fn list<'work>(&'work self, mut values: ScratchVec<'work, WorkValue<'work>>) -> Result<WorkValue<'work>> {
        self.check(values.work)?;
        let mut result = Vec::new();
        self.admit(array_bytes::<Value>(values.values.len())?)?;
        result.try_reserve_exact(values.values.len()).map_err(|_| allocation_error())?;
        let mut final_buffers = array_bytes::<Value>(result.capacity())?;
        for value in values.values.drain(..) {
            self.check(value.work)?;
            final_buffers = add(final_buffers, value.final_buffers)?;
            result.push(value.value);
        }
        Ok(WorkValue { value: Value::list(result), final_buffers, work: self })
    }
    fn list_array<'work, const N: usize>(&'work self, values: [WorkValue<'work>; N]) -> Result<WorkValue<'work>> {
        let mut pending = ScratchVec::new(self);
        for value in values { pending.push(value)?; }
        self.list(pending)
    }
    fn push_output<T>(&self, values: &mut Vec<T>, value: T) -> Result<()> {
        if values.len() == values.capacity() {
            let capacity = values.capacity().checked_mul(2).ok_or_else(size_error)?.max(8);
            self.admit(array_bytes::<T>(capacity)?)?;
            let mut replacement = Vec::new();
            replacement.try_reserve_exact(capacity).map_err(|_| allocation_error())?;
            replacement.append(values);
            *values = replacement;
        }
        values.push(value);
        Ok(())
    }
}
struct WorkText<'work> { text: String, work: &'work ExplainWork }
impl WorkText<'_> { fn as_str(&self) -> &str { &self.text } }
struct WorkValue<'work> { value: Value, final_buffers: usize, work: &'work ExplainWork }
struct WorkRows<'work> { rows: Vec<Vec<Value>>, final_buffers: usize, work: &'work ExplainWork }
impl<'work> WorkRows<'work> {
    fn new(work: &'work ExplainWork) -> Self { Self { rows: Vec::new(), final_buffers: 0, work } }
    fn push(&mut self, values: [WorkValue<'work>; 3]) -> Result<()> {
        self.work.admit(array_bytes::<Value>(3)?)?;
        let mut row = Vec::new();
        row.try_reserve_exact(3).map_err(|_| allocation_error())?;
        let mut row_buffers = array_bytes::<Value>(row.capacity())?;
        for value in values {
            self.work.check(value.work)?;
            row_buffers = add(row_buffers, value.final_buffers)?;
            row.push(value.value);
        }
        let old_outer = array_bytes::<Vec<Value>>(self.rows.capacity())?;
        self.work.push_output(&mut self.rows, row)?;
        self.final_buffers = add(self.final_buffers.checked_sub(old_outer).ok_or_else(size_error)?, add(array_bytes::<Vec<Value>>(self.rows.capacity())?, row_buffers)?)?;
        Ok(())
    }
    fn publish(self, schema: Schema, retained_bytes: usize) -> Result<Arc<ExplainPlan>> {
        {
            let mut state = self.work.state.borrow_mut();
            let WorkState::Building { reservation, base_bytes, cumulative_buffers } = &mut *state else { return Err(region_error()); };
            if self.final_buffers > *cumulative_buffers || self.rows.iter().any(|row| row.len() != 3) || schema.len() != 3 { return Err(size_error()); }
            let target = add(*base_bytes, self.final_buffers)?;
            if retained_bytes > target { return Err(invalid("explain-size", "Plan description exceeds its allocation bound")); }
            Arc::get_mut(reservation).ok_or_else(region_error)?.shrink_to(target)?;
        }
        let reservation = match self.work.state.replace(WorkState::Published) { WorkState::Building { reservation, .. } => reservation, WorkState::Published => unreachable!() };
        let plan = ExplainPlan { schema, rows: Arc::new(self.rows), retained_bytes, _reservation: reservation };
        Ok(Arc::new(plan))
    }
}

struct ScratchVec<'work, T> { values: Vec<T>, work: &'work ExplainWork, reservation: Option<Arc<MemoryReservation>> }
impl<'work, T> ScratchVec<'work, T> {
    fn new(work: &'work ExplainWork) -> Self { Self { values: Vec::new(), work, reservation: None } }
    fn push(&mut self, value: T) -> Result<()> {
        if self.values.len() == self.values.capacity() {
            let capacity = self.values.capacity().checked_mul(2).ok_or_else(size_error)?.max(8);
            let old = array_bytes::<T>(self.values.capacity())?;
            let new = array_bytes::<T>(capacity)?;
            let overlap = add(add(old, new)?, 256)?;
            match &mut self.reservation {
                Some(reservation) => Arc::get_mut(reservation).ok_or_else(region_error)?.grow_to(overlap)?,
                None => self.reservation = Some(self.work.memory.reserve(overlap)?),
            }
            let mut replacement = Vec::new();
            replacement.try_reserve_exact(capacity).map_err(|_| allocation_error())?;
            replacement.append(&mut self.values);
            self.values = replacement;
            Arc::get_mut(self.reservation.as_mut().ok_or_else(region_error)?).ok_or_else(region_error)?.shrink_to(add(new, 256)?)?;
        }
        self.values.push(value);
        Ok(())
    }
    fn pop(&mut self) -> Option<T> { self.values.pop() }
}
struct ScratchMap<'work, K, V> { values: BTreeMap<K, V>, work: &'work ExplainWork, reservation: Option<Arc<MemoryReservation>>, admitted: usize }
impl<'work, K: Ord, V> ScratchMap<'work, K, V> {
    fn new(work: &'work ExplainWork) -> Self { Self { values: BTreeMap::new(), work, reservation: None, admitted: 256 } }
    fn insert(&mut self, key: K, value: V) -> Result<()> {
        let next = add(self.admitted, map_node_bytes::<K, V>()?)?;
        match &mut self.reservation {
            Some(reservation) => Arc::get_mut(reservation).ok_or_else(region_error)?.grow_to(next)?,
            None => self.reservation = Some(self.work.memory.reserve(next)?),
        }
        self.admitted = next;
        self.values.insert(key, value);
        Ok(())
    }
}
/// Keeps fields in insertion order: each operator's explanation lists its keys in the reference's map-literal order.
struct WorkMap<'work> { fields: ScratchMap<'work, usize, (String, WorkValue<'work>)>, work: &'work ExplainWork }
impl<'work> WorkMap<'work> {
    fn new(work: &'work ExplainWork) -> Self { Self { fields: ScratchMap::new(work), work } }
    fn insert(&mut self, key: &str, value: WorkValue<'work>) -> Result<()> {
        self.work.check(value.work)?;
        if let Some((_, existing)) = self.fields.values.values_mut().find(|(name, _)| name == key) {
            *existing = value;
            return Ok(());
        }
        let key = self.work.copy_text(key)?;
        self.work.admit(map_node_bytes::<String, Value>()?)?;
        let position = self.fields.values.len();
        self.fields.insert(position, (key.text, value))
    }
    fn finish(mut self) -> Result<WorkValue<'work>> {
        let mut fields = StructFields::new();
        let mut final_buffers = 0;
        while let Some((_, (key, value))) = self.fields.values.pop_first() {
            self.work.check(value.work)?;
            final_buffers = add(final_buffers, add(map_node_bytes::<String, Value>()?, add(key.capacity(), value.final_buffers)?)?)?;
            fields.insert(key, value.value);
        }
        Ok(WorkValue { value: Value::Struct(fields), final_buffers, work: self.work })
    }
}

enum RenderPiece<'text, 'work> {
    Text(&'text str), Char(char), Bool(bool), I64(i64), U64(u64), HexU32(u32, usize), HexBytes(&'text [u8]), JsonText(&'text str),
    Join(&'text [WorkText<'work>], &'text str), RepeatStatic(&'static str, usize),
}
trait PieceSink { fn append(&mut self, bytes: &[u8]) -> Result<()>; }
struct CountSink { bytes: usize }
impl PieceSink for CountSink { fn append(&mut self, bytes: &[u8]) -> Result<()> { self.bytes = add(self.bytes, bytes.len())?; Ok(()) } }
struct FillSink<'bytes> { bytes: &'bytes mut Vec<u8>, limit: usize }
impl PieceSink for FillSink<'_> {
    fn append(&mut self, bytes: &[u8]) -> Result<()> {
        if add(self.bytes.len(), bytes.len())? > self.limit { return Err(size_error()); }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
}
fn unsigned(sink: &mut impl PieceSink, mut value: u64) -> Result<()> {
    let mut digits = [0u8; 20];
    let mut index = digits.len();
    loop { index -= 1; digits[index] = b'0' + (value % 10) as u8; value /= 10; if value == 0 { break; } }
    sink.append(&digits[index..])
}
fn hex(sink: &mut impl PieceSink, value: u32, width: usize) -> Result<()> {
    if width > 8 { return Err(size_error()); }
    let mut digits = [b'0'; 8];
    for (index, digit) in digits.iter_mut().enumerate() { *digit = b"0123456789abcdef"[((value >> ((7 - index) * 4)) & 15) as usize]; }
    sink.append(&digits[8 - width..])
}
fn json_text(sink: &mut impl PieceSink, value: &str) -> Result<()> {
    sink.append(b"\"")?;
    let mut start = 0;
    for (index, byte) in value.bytes().enumerate() {
        let escaped: Option<&[u8]> = match byte { b'"' => Some(b"\\\""), b'\\' => Some(b"\\\\"), b'\n' => Some(b"\\n"), b'\r' => Some(b"\\r"), b'\t' => Some(b"\\t"), 8 => Some(b"\\b"), 12 => Some(b"\\f"), _ => None };
        if let Some(escaped) = escaped {
            sink.append(&value.as_bytes()[start..index])?;
            sink.append(escaped)?;
            start = index + 1;
        } else if byte < 32 {
            sink.append(&value.as_bytes()[start..index])?;
            sink.append(b"\\u00")?;
            hex(sink, u32::from(byte), 2)?;
            start = index + 1;
        }
    }
    sink.append(&value.as_bytes()[start..])?;
    sink.append(b"\"")
}
fn write_pieces(pieces: &[RenderPiece<'_, '_>], sink: &mut impl PieceSink) -> Result<()> {
    for piece in pieces {
        match piece {
            RenderPiece::Text(value) => sink.append(value.as_bytes())?,
            RenderPiece::Char(value) => { let mut bytes = [0u8; 4]; sink.append(value.encode_utf8(&mut bytes).as_bytes())?; }
            RenderPiece::Bool(value) => sink.append(if *value { b"true" } else { b"false" })?,
            RenderPiece::I64(value) => { if *value < 0 { sink.append(b"-")?; } unsigned(sink, value.unsigned_abs())?; }
            RenderPiece::U64(value) => unsigned(sink, *value)?,
            RenderPiece::HexU32(value, width) => hex(sink, *value, *width)?,
            RenderPiece::HexBytes(bytes) => for byte in *bytes { hex(sink, u32::from(*byte), 2)?; },
            RenderPiece::JsonText(value) => json_text(sink, value)?,
            RenderPiece::Join(values, separator) => for (index, value) in values.iter().enumerate() { if index != 0 { sink.append(separator.as_bytes())?; } sink.append(value.as_str().as_bytes())?; },
            RenderPiece::RepeatStatic(value, count) => for _ in 0..*count { sink.append(value.as_bytes())?; },
        }
    }
    Ok(())
}
struct TextSink<'work> { bytes: Vec<u8>, work: &'work ExplainWork }
impl<'work> TextSink<'work> {
    fn new(work: &'work ExplainWork) -> Self { Self { bytes: Vec::new(), work } }
    fn append(&mut self, pieces: &[RenderPiece<'_, 'work>]) -> Result<()> {
        self.work.check_pieces(pieces)?;
        let mut count = CountSink { bytes: 0 };
        write_pieces(pieces, &mut count)?;
        let required = add(self.bytes.len(), count.bytes)?;
        if required > self.bytes.capacity() {
            let capacity = self.bytes.capacity().checked_mul(2).ok_or_else(size_error)?.max(8).max(required);
            self.work.admit(array_bytes::<u8>(capacity)?)?;
            let mut replacement = Vec::new();
            replacement.try_reserve_exact(capacity).map_err(|_| allocation_error())?;
            replacement.extend_from_slice(&self.bytes);
            self.bytes = replacement;
        }
        write_pieces(pieces, &mut FillSink { bytes: &mut self.bytes, limit: required })?;
        if self.bytes.len() != required { return Err(size_error()); }
        Ok(())
    }
    fn finish(self) -> Result<WorkText<'work>> { Ok(WorkText { text: String::from_utf8(self.bytes).map_err(|_| size_error())?, work: self.work }) }
}
fn depth_bound(relation: &Relation, depth: usize) -> Result<usize> {
    let own = depth.checked_mul(4).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(size_error)?;
    relation.children().iter().try_fold(own, |bytes, child| bytes.checked_add(depth_bound(child, depth.checked_add(1).ok_or_else(size_error)?)?).ok_or_else(size_error))
}

type Columns<'work> = ScratchMap<'work, ColumnId, WorkText<'work>>;
struct Descriptor<'work> { op: &'static str, explanation: WorkValue<'work>, children: ScratchVec<'work, Descriptor<'work>>, columns: Columns<'work> }
impl<'work> Descriptor<'work> {
    fn rows(self, depth: usize, rows: &mut WorkRows<'work>) -> Result<()> {
        let Self { op, explanation, mut children, columns } = self;
        drop(columns);
        let work = rows.work;
        rows.push([
            work.text(work.pieces(&[RenderPiece::RepeatStatic("  ", depth), RenderPiece::Text("->")])?)?,
            work.keyword(work.copy_text(op)?)?,
            explanation,
        ])?;
        for child in children.values.drain(..) { child.rows(add(depth, 1)?, rows)?; }
        Ok(())
    }
}
fn column<'work>(id: ColumnId, columns: &Columns<'work>, work: &'work ExplainWork) -> Result<WorkText<'work>> {
    match columns.values.get(&id) {
        Some(name) => work.copy_text(name.as_str()),
        None => work.pieces(&[RenderPiece::Text("_column_"), RenderPiece::U64(u64::from(id.0))]),
    }
}
fn map<'work, const N: usize>(work: &'work ExplainWork, entries: [(&str, WorkValue<'work>); N]) -> Result<WorkValue<'work>> {
    let mut fields = WorkMap::new(work);
    for (name, value) in entries { fields.insert(name, value)?; }
    fields.finish()
}
fn strings<'work>(work: &'work ExplainWork, mut values: ScratchVec<'work, WorkText<'work>>) -> Result<WorkValue<'work>> {
    work.check(values.work)?;
    let mut output = ScratchVec::new(work);
    for value in values.values.drain(..) { output.push(work.text(value)?)?; }
    work.list(output)
}
fn joined<'work>(work: &'work ExplainWork, open: &str, values: &ScratchVec<'work, WorkText<'work>>, separator: &str, close: &str) -> Result<WorkText<'work>> {
    work.check(values.work)?;
    work.pieces(&[RenderPiece::Text(open), RenderPiece::Join(&values.values, separator), RenderPiece::Text(close)])
}
fn orders<'work>(orders: &[rtdb_plan::SortExpr], columns: &Columns<'work>, work: &'work ExplainWork) -> Result<WorkText<'work>> {
    let mut values = ScratchVec::new(work);
    for order in orders {
        let expression = form(&order.expr, columns, work)?;
        values.push(work.pieces(&[
            RenderPiece::Text("["), RenderPiece::Text(expression.as_str()), RenderPiece::Text(" {:direction :"),
            RenderPiece::Text(if order.descending { "desc" } else { "asc" }), RenderPiece::Text(", :null-ordering :"),
            RenderPiece::Text(if order.nulls_first { "nulls-first" } else { "nulls-last" }), RenderPiece::Text("}]"),
        ])?)?;
    }
    joined(work, "[", &values, " ", "]")
}
fn arguments<'work>(arguments: &[(usize, Expr)], columns: &Columns<'work>, work: &'work ExplainWork) -> Result<WorkText<'work>> {
    let mut values = ScratchVec::new(work);
    for (index, expression) in arguments {
        let expression = form(expression, columns, work)?;
        values.push(work.pieces(&[RenderPiece::Text(expression.as_str()), RenderPiece::Text(" ?_"), RenderPiece::U64(u64::try_from(*index).map_err(|_| size_error())?)])?)?;
    }
    joined(work, "{", &values, ", ", "}")
}

enum ColumnName { Symbol, String }

impl KeySemantics<&str> for ColumnName {
    fn is_null(&self, _: &&str) -> bool { false }
    fn hash(&mut self, name: &&str) -> Result<i32> {
        Ok(match self { Self::Symbol => clojure_symbol_hasheq(None, name), Self::String => clojure_string_hasheq(name) })
    }
    fn equivalent(&mut self, left: &&str, right: &&str) -> Result<bool> { Ok(left == right) }
}

fn scan_names<'work>(fields: &[Field], work: &'work ExplainWork) -> Result<WorkValue<'work>> {
    let scope = AllocationScope::new(&work.memory)?;
    scope.reserve(fields.len().checked_mul(128).ok_or_else(size_error)?)?;
    let mut seen = BTreeSet::new();
    let symbols = fields.iter().map(|field| field.name.as_str()).filter(|name| seen.insert(*name)).collect();
    let CheckedIndex::Unique(symbols) = ClojureKeyIndex::checked_set(symbols, &mut ColumnName::Symbol, Some(&scope))? else {
        return Err(invalid("explain-columns", "Scan column names must be unique"));
    };
    let names = symbols.entries().map(|(_, name)| *name).collect();
    let CheckedIndex::Unique(names) = ClojureKeyIndex::checked_set(names, &mut ColumnName::String, Some(&scope))? else {
        return Err(invalid("explain-columns", "Scan column names must be unique"));
    };
    let mut result = ScratchVec::new(work);
    for (_, name) in names.entries() { result.push(work.copy_text(name)?)?; }
    strings(work, result)
}

fn describe<'work>(relation: &Relation, work: &'work ExplainWork) -> Result<Descriptor<'work>> {
    let mut children = ScratchVec::new(work);
    for child in relation.children() { children.push(describe(child, work)?)?; }
    let mut input_columns = Columns::new(work);
    for child in &children.values {
        for (id, name) in &child.columns.values { input_columns.insert(*id, work.copy_text(name.as_str())?)?; }
    }
    let expression = |expr| form(expr, &input_columns, work);
    let text = |text| work.text(text);
    let mut op = crate::analysis::operator(relation);
    let explanation = match relation.node() {
        RelationNode::Project { expressions, .. } | RelationNode::Extend { expressions, .. } => {
            let mut values = ScratchVec::new(work);
            for named in expressions {
                let expression = expression(&named.expr)?;
                values.push(work.pieces(&[RenderPiece::Text("{"), RenderPiece::Text(&named.field.name), RenderPiece::Text(" "), RenderPiece::Text(expression.as_str()), RenderPiece::Text("}")])?)?;
            }
            map(work, [("project", text(joined(work, "[", &values, " ", "]")?)?), ("append?", work.boolean(matches!(relation.node(), RelationNode::Extend { .. })))])?
        }
        RelationNode::Rename { prefix, expressions, .. } => {
            let mut fields = WorkMap::new(work);
            fields.insert("prefix", text(work.copy_text(prefix)?)?)?;
            if expressions.iter().any(|named| match &named.expr { Expr::Column { id, .. } => input_columns.values.get(id).is_some_and(|old| old.as_str().rsplit('/').next() != Some(named.field.name.as_str())), _ => true }) {
                let mut values = ScratchVec::new(work);
                for named in expressions {
                    let expression = expression(&named.expr)?;
                    values.push(work.pieces(&[RenderPiece::Text(&named.field.name), RenderPiece::Text(" "), RenderPiece::Text(expression.as_str())])?)?;
                }
                fields.insert("columns", text(joined(work, "{", &values, ", ", "}")?)?)?;
            } else {
                fields.insert("columns", work.null())?;
            }
            fields.finish()?
        }
        RelationNode::Scan(scan) => {
            let mut columns = Columns::new(work);
            for field in &scan.columns { columns.insert(field.id, work.copy_text(&field.name)?)?; }
            let table = work.pieces(&[RenderPiece::Text(&scan.table.name.database), RenderPiece::Text("."), RenderPiece::Text(&scan.table.name.table.schema), RenderPiece::Text("."), RenderPiece::Text(&scan.table.name.table.table)])?;
            let names = scan_names(&scan.columns, work)?;
            let mut predicates = ScratchVec::new(work);
            for predicate in &scan.predicates { predicates.push(form(predicate, &columns, work)?)?; }
            map(work, [("table", text(table)?), ("columns", names), ("predicates", strings(work, predicates)?)])?
        }
        RelationNode::Filter { predicate, .. } => map(work, [("predicate", text(expression(predicate)?)?)])?,
        RelationNode::SortLimit { order_by, offset, fetch, .. } => {
            let mut fields = WorkMap::new(work);
            if !order_by.is_empty() { fields.insert("order-specs", text(orders(order_by, &input_columns, work)?)?)?; }
            if let Some(offset) = offset { fields.insert("skip", text(expression(offset)?)?)?; }
            if let Some(fetch) = fetch { fields.insert("limit", text(expression(fetch)?)?)?; }
            fields.finish()?
        }
        RelationNode::PatchGaps { from, to, .. } => map(work, [
            ("valid-from", text(work_patch_endpoint(from, expression(from)?)?)?),
            ("valid-to", text(work_patch_endpoint(to, expression(to)?)?)?),
        ])?,
        RelationNode::Join { kind, condition, .. } => {
            op = match kind { JoinKind::Inner => "inner-join", JoinKind::Cross => "cross-join", JoinKind::Left | JoinKind::Right => "left-join", JoinKind::Full => "full-join", JoinKind::Semi => "semi-join", JoinKind::Anti => "anti-join", JoinKind::Single => "single-join" };
            let condition = match condition { Some(condition) => expression(condition)?, None => work.copy_text("")? };
            map(work, [("condition", text(work.pieces(&[RenderPiece::Text("["), RenderPiece::Text(condition.as_str()), RenderPiece::Text("]")])?)?)])?
        }
        RelationNode::Aggregate { groups, aggregates, .. } => {
            let mut group_values = ScratchVec::new(work);
            for group in groups { group_values.push(expression(&group.expr)?)?; }
            let groups = strings(work, group_values)?;
            let mut aggregate_values = ScratchVec::new(work);
            for aggregate in aggregates {
                aggregate_values.push(work.list_array([
                    text(work.copy_text(&aggregate.field.name)?)?,
                    text(aggregate_form(aggregate, &input_columns, work)?)?,
                ])?)?;
            }
            map(work, [("group-by", groups), ("aggregates", work.list(aggregate_values)?)])?
        }
        RelationNode::Window { partition_by, order_by, expressions, .. } => {
            let mut partitions = ScratchVec::new(work);
            for id in partition_by { partitions.push(column(*id, &input_columns, work)?)?; }
            let partition = strings(work, partitions)?;
            let order = orders(order_by, &input_columns, work)?;
            let mut windows = ScratchVec::new(work);
            for window in expressions {
                let function = match window.function {
                    rtdb_plan::WindowFunction::RowNumber => work.copy_text("[:nullary {:f row-number}]")?,
                    rtdb_plan::WindowFunction::Lead { column: id, offset } | rtdb_plan::WindowFunction::Lag { column: id, offset } => {
                        let name = if matches!(window.function, rtdb_plan::WindowFunction::Lead { .. }) { "lead" } else { "lag" };
                        let symbol = column(id, &input_columns, work)?;
                        work.pieces(&[
                            RenderPiece::Text("[:unary-with-offset {:f "), RenderPiece::Text(name),
                            RenderPiece::Text(", :from-column "), RenderPiece::Text(symbol.as_str()),
                            RenderPiece::Text(", :offset "), RenderPiece::U64(offset), RenderPiece::Text("}]"),
                        ])?
                    }
                };
                windows.push(work.list_array([text(work.copy_text(&window.field.name)?)?, text(function)?])?)?;
            }
            map(work, [("partition-by", partition), ("order-by", text(order)?), ("window-functions", work.list(windows)?)])?
        }
        RelationNode::List { expr, element, .. } => {
            let expression = expression(expr)?;
            let element = field_symbol(element, work)?;
            map(work, [("columns", text(work.pieces(&[RenderPiece::Text("{"), RenderPiece::Text(element.as_str()), RenderPiece::Text(" "), RenderPiece::Text(expression.as_str()), RenderPiece::Text("}")])?)?)])?
        }
        RelationNode::ColumnTable { .. } => work.null(),
        RelationNode::Ordinality { field, scope, .. } => {
            let namespace = field.qualifier.as_ref().and_then(|qualifier| qualifier.scope_symbol());
            let prefix = match namespace {
                Some(symbol) => work.pieces(&[RenderPiece::Text("[#:"), RenderPiece::Text(symbol), RenderPiece::Text("{")])?,
                None => work.copy_text("[{")?,
            };
            let row_number = match scope {
                rtdb_plan::OrdinalityScope::Page => " (local-row-number)}]",
                rtdb_plan::OrdinalityScope::Relation => " (row-number)}]",
            };
            map(work, [
                ("project", text(work.pieces(&[RenderPiece::Text(prefix.as_str()), RenderPiece::Text(&field.name), RenderPiece::Text(row_number)])?)?),
                ("append?", work.boolean(true)),
            ])?
        }
        RelationNode::Unnest { expr, element, ordinality, .. } => {
            let mut fields = WorkMap::new(work);
            fields.insert("from", text(expression(expr)?)?)?;
            fields.insert("to", text(field_symbol(element, work)?)?)?;
            if let Some(field) = ordinality { fields.insert("ordinality", text(field_symbol(field, work)?)?)?; }
            fields.finish()?
        }
        RelationNode::Mark { arguments, condition, comparison, .. } if arguments.is_empty() => {
            let comparison = expression(comparison)?;
            let condition = if let Some(condition) = condition {
                let condition = expression(condition)?;
                work.pieces(&[RenderPiece::Text("["), RenderPiece::Text(condition.as_str()), RenderPiece::Text("]")])?
            } else { work.copy_text("[]")? };
            map(work, [
                ("condition", text(condition)?),
                ("mark", text(work.pieces(&[RenderPiece::Text("(and "), RenderPiece::Text(comparison.as_str()), RenderPiece::Text(")")])?)?),
            ])?
        }
        RelationNode::Mark { arguments: values, comparison, output, .. } => {
            let arguments = arguments(values, &input_columns, work)?;
            let comparison = expression(comparison)?;
            map(work, [
                ("columns", text(arguments)?),
                ("mark-spec", text(work.pieces(&[RenderPiece::Text("{"), RenderPiece::Text(&output.name), RenderPiece::Text(" ["), RenderPiece::Text(comparison.as_str()), RenderPiece::Text("]}")])?)?),
            ])?
        }
        RelationNode::Apply { arguments: values, .. } => map(work, [("columns", text(arguments(values, &input_columns, work)?)?)])?,
        RelationNode::Set { operator, .. } => { op = match operator { rtdb_plan::SetOperator::Union => "union-all", rtdb_plan::SetOperator::Intersect => "intersect", rtdb_plan::SetOperator::Except => "difference" }; work.null() }
        RelationNode::Values { .. } | RelationNode::LiteralPages { .. } | RelationNode::ArrowLiteralPages { .. } | RelationNode::Sort { .. } | RelationNode::Limit { .. } | RelationNode::Distinct { .. }
        | RelationNode::Let { .. } | RelationNode::Reference { .. } => work.null(),
    };
    let mut columns = Columns::new(work);
    for field in relation.schema().fields() {
        let name = match relation.node() {
            RelationNode::Rename { prefix, .. } => work.pieces(&[RenderPiece::Text(prefix), RenderPiece::Text("/"), RenderPiece::Text(&field.name)])?,
            RelationNode::Scan(_) | RelationNode::Values { .. } | RelationNode::Project { .. } | RelationNode::Aggregate { .. } | RelationNode::Reference { .. } => work.copy_text(&field.name)?,
            _ => match input_columns.values.get(&field.id) {
                Some(name) => work.copy_text(name.as_str())?,
                None => field_symbol(field, work)?,
            },
        };
        columns.insert(field.id, name)?;
    }
    Ok(Descriptor { op, explanation, children, columns })
}
fn field_symbol<'work>(field: &Field, work: &'work ExplainWork) -> Result<WorkText<'work>> {
    match field.qualifier.as_ref().and_then(|qualifier| qualifier.scope_symbol()) {
        Some(symbol) => work.pieces(&[RenderPiece::Text(symbol), RenderPiece::Text("/"), RenderPiece::Text(&field.name)]),
        None => work.copy_text(&field.name),
    }
}
fn aggregate_form<'work>(aggregate: &rtdb_plan::AggregateExpr, columns: &Columns<'work>, work: &'work ExplainWork) -> Result<WorkText<'work>> {
    let name = aggregate.function.name();
    let argument = aggregate.argument.as_ref().map(|arg| form(arg, columns, work)).transpose()?;
    let name = work.pieces(&[RenderPiece::Text(name), RenderPiece::Text(if aggregate.distinct && aggregate.function.quantifier() != Some(true) { "_distinct" } else { "" })])?;
    let mut parts = ScratchVec::new(work);
    parts.push(name)?;
    match (&aggregate.function, argument) {
        (AggregateFunction::Percentile { fraction, descending, .. }, Some(argument)) => {
            let fraction = match fraction {
                rtdb_plan::PercentileFraction::Integer(value) => work.pieces(&[RenderPiece::I64(*value)])?,
                rtdb_plan::PercentileFraction::Literal(value) => {
                    let value = Value::F64(*value);
                    let _reservation = work.memory.reserve(rtdb_expr::text_allocation_bound(&value)?)?;
                    let Value::F64(value) = value else { unreachable!() };
                    let text = rtdb_types::java_f64_text(value);
                    work.copy_text(&text)?
                }
                rtdb_plan::PercentileFraction::Parameter { index } => work.pieces(&[RenderPiece::Text("?_"), RenderPiece::U64(u64::try_from(*index).map_err(|_| size_error())?)])?,
            };
            parts.push(fraction)?;
            parts.push(work.pieces(&[RenderPiece::Text("["), RenderPiece::Text(argument.as_str()), RenderPiece::Text(" {:direction :"), RenderPiece::Text(if *descending { "desc" } else { "asc" }), RenderPiece::Text("}]")])?)?;
        }
        (_, Some(argument)) => parts.push(argument)?,
        (_, None) => {}
    }
    let mut options = ScratchVec::new(work);
    if let Some(filter) = &aggregate.filter {
        let filter = form(filter, columns, work)?;
        options.push(work.pieces(&[RenderPiece::Text(":filter "), RenderPiece::Text(filter.as_str())])?)?;
    }
    if !aggregate.order_by.is_empty() {
        let orders = orders(&aggregate.order_by, columns, work)?;
        options.push(work.pieces(&[RenderPiece::Text(":order-specs "), RenderPiece::Text(orders.as_str())])?)?;
    }
    if !options.values.is_empty() { parts.push(joined(work, "{", &options, ", ", "}")?)?; }
    joined(work, "(", &parts, " ", ")")
}
fn work_patch_endpoint<'work>(expr: &Expr, rendered: WorkText<'work>) -> Result<WorkText<'work>> {
    let work = rendered.work;
    match rtdb_plan::PatchEndpoint::from_expr(expr)? {
        rtdb_plan::PatchEndpoint::Default => work.copy_text("nil"),
        rtdb_plan::PatchEndpoint::CurrentTime => work.pieces(&[RenderPiece::Text("[:now "), RenderPiece::Text(if matches!(expr, Expr::Call { .. }) { "(current-timestamp)" } else { rendered.as_str() }), RenderPiece::Text("]")]),
        rtdb_plan::PatchEndpoint::DateTime(_) => work.pieces(&[RenderPiece::Text("[:literal "), RenderPiece::Text(rendered.as_str()), RenderPiece::Text("]")]),
        rtdb_plan::PatchEndpoint::Parameter(_) => work.pieces(&[RenderPiece::Text("[:param "), RenderPiece::Text(rendered.as_str()), RenderPiece::Text("]")]),
    }
}
fn call<'work>(work: &'work ExplainWork, name: &str, args: &ScratchVec<'work, WorkText<'work>>) -> Result<WorkText<'work>> {
    if args.values.is_empty() { work.pieces(&[RenderPiece::Text("("), RenderPiece::Text(name), RenderPiece::Text(")")]) }
    else { work.pieces(&[RenderPiece::Text("("), RenderPiece::Text(name), RenderPiece::Text(" "), RenderPiece::Join(&args.values, " "), RenderPiece::Text(")")]) }
}
fn call_array<'work, const N: usize>(work: &'work ExplainWork, name: &str, args: [WorkText<'work>; N]) -> Result<WorkText<'work>> {
    let mut values = ScratchVec::new(work);
    for arg in args { values.push(arg)?; }
    call(work, name, &values)
}
fn form<'work>(expr: &Expr, columns: &Columns<'work>, work: &'work ExplainWork) -> Result<WorkText<'work>> {
    let child = |expr: &Expr| form(expr, columns, work);
    match expr {
        Expr::Literal(value) => literal(RegexLiteralView::new(value), work),
        Expr::DurationLiteral(value) => {
            work.admit(rtdb_types::DurationLiteral::MAX_TEXT_LEN)?;
            work.pieces(&[RenderPiece::Text("#time/duration "), RenderPiece::JsonText(&value.to_iso_string())])
        }
        Expr::Column { id, .. } | Expr::Correlated { id, .. } => column(*id, columns, work),
        Expr::Parameter { index, .. } => work.pieces(&[RenderPiece::Text("?_"), RenderPiece::U64(u64::try_from(*index).map_err(|_| size_error())?)]),
        Expr::Unary { op, expr } => call_array(work, match op { UnaryOp::Negate => "-", UnaryOp::Not => "not", UnaryOp::BitNot => "bit-not", UnaryOp::IsNull | UnaryOp::IsUnknown => "nil?", UnaryOp::IsNotNull => "some?", UnaryOp::IsTrue => "true?", UnaryOp::IsFalse => "false?" }, [child(expr)?]),
        Expr::Binary { op, left, right } => call_array(work, match op { BinaryOp::Add => "+", BinaryOp::Subtract => "-", BinaryOp::Multiply => "*", BinaryOp::Divide => "/", BinaryOp::Modulo => "mod", BinaryOp::Eq => "==", BinaryOp::NotEq => "<>", BinaryOp::Lt => "<", BinaryOp::LtEq => "<=", BinaryOp::Gt => ">", BinaryOp::GtEq => ">=", BinaryOp::IsDistinctFrom => "distinct-from", BinaryOp::IsNotDistinctFrom => "not-distinct-from", BinaryOp::And => "and", BinaryOp::Or => "or", BinaryOp::Concat => "concat", BinaryOp::BitAnd => "bit-and", BinaryOp::BitOr => "bit-or", BinaryOp::BitXor => "bit-xor", BinaryOp::ShiftLeft => "bit-shift-left", BinaryOp::ShiftRight => "bit-shift-right" }, [child(left)?, child(right)?]),
        Expr::Call { name, args, .. } => {
            let mut values = ScratchVec::new(work);
            for arg in args { values.push(child(arg)?)?; }
            call(work, name, &values)
        }
        Expr::RegexReplace { source, recipe, .. } => call_array(work, "regexp-replace", [
            child(source)?, regex_form(recipe.pattern.view(), work)?, regex_form(recipe.replacement.view(), work)?,
            regex_form(recipe.start.view(), work)?, regex_form(recipe.occurrence.view(), work)?, regex_form(recipe.flags.view(), work)?,
        ]),
        Expr::Cast { expr, target } => {
            let mut args = ScratchVec::new(work);
            args.push(child(expr)?)?;
            let requested = match target { rtdb_plan::CastTarget::Checked { requested, .. } => requested.to_edn()?, _ => target.result_type(&expr.data_type()).to_edn()? };
            args.push(work.copy_text(&requested)?)?;
            if let Some(options) = cast_options(target, work)? { args.push(options)?; }
            call(work, "cast", &args)
        }
        Expr::Coalesce(args) => {
            let mut values = ScratchVec::new(work);
            for arg in args { values.push(child(arg)?)?; }
            call(work, "coalesce", &values)
        }
        Expr::Between { operand, low, high, symmetric } => call_array(work, if *symmetric { "between-symmetric" } else { "between" }, [child(operand)?, child(low)?, child(high)?]),
        Expr::SimpleCase { operand, branches, otherwise } => {
            let mut args = ScratchVec::new(work);
            args.push(child(operand)?)?;
            for (candidate, value) in branches { args.push(child(candidate)?)?; args.push(child(value)?)?; }
            args.push(child(otherwise)?)?;
            call(work, "case", &args)
        }
        Expr::Case { branches, otherwise } => {
            let mut args = ScratchVec::new(work);
            for (test, value) in branches { args.push(child(test)?)?; args.push(child(value)?)?; }
            args.push(child(otherwise)?)?;
            call(work, "cond", &args)
        }
        Expr::Field { expr, name, .. } => call_array(work, ".", [child(expr)?, work.copy_text(name)?]),
        Expr::Subscript { expr, index, .. } => call_array(work, "nth", [child(expr)?, child(index)?]),
        Expr::List(values) => {
            let mut args = ScratchVec::new(work);
            for value in values { args.push(child(value)?)?; }
            joined(work, "[", &args, " ", "]")
        }
        Expr::Struct(fields) => {
            let mut args = ScratchVec::new(work);
            for (name, value) in fields {
                let value = child(value)?;
                args.push(work.pieces(&[RenderPiece::Text(":"), RenderPiece::Text(name), RenderPiece::Text(" "), RenderPiece::Text(value.as_str())])?)?;
            }
            joined(work, "{", &args, ", ", "}")
        }
        Expr::RowCopy(fields) => {
            let mut args = ScratchVec::new(work);
            for field in fields {
                let value = child(&field.expression())?;
                args.push(work.pieces(&[RenderPiece::Text(":"), RenderPiece::Text(&field.name), RenderPiece::Text(" "), RenderPiece::Text(value.as_str())])?)?;
            }
            joined(work, "{", &args, ", ", "}")
        }
        Expr::In { expr, values, negated } => {
            let expression = child(expr)?;
            let mut args = ScratchVec::new(work);
            for value in values { args.push(child(value)?)?; }
            let values = joined(work, "[", &args, " ", "]")?;
            let value = call_array(work, "in", [expression, values])?;
            if *negated { call_array(work, "not", [value]) } else { Ok(value) }
        }
        Expr::Like { expr, pattern, escape, case_insensitive, negated } => {
            let mut args = ScratchVec::new(work);
            args.push(child(expr)?)?;
            args.push(child(pattern)?)?;
            if let Some(escape) = escape {
                let mut bytes = [0u8; 4];
                args.push(work.pieces(&[RenderPiece::JsonText(escape.encode_utf8(&mut bytes))])?)?;
            }
            let value = call(work, if *case_insensitive { "ilike" } else { "like" }, &args)?;
            if *negated { call_array(work, "not", [value]) } else { Ok(value) }
        }
    }
}
fn cast_options<'work>(target: &rtdb_plan::CastTarget, work: &'work ExplainWork) -> Result<Option<WorkText<'work>>> {
    Ok(match target {
        rtdb_plan::CastTarget::Deferred(target) => match target {
            rtdb_plan::DeferredCast::Temporal { precision, .. } => if matches!(precision, rtdb_plan::CastOption::Null) { None } else {
                let precision = precision.to_edn();
                Some(work.pieces(&[RenderPiece::Text("{:precision "), RenderPiece::Text(&precision), RenderPiece::Text("}")])?)
            },
            rtdb_plan::DeferredCast::Interval { start, end, leading, fractional } => {
                let mut values = ScratchVec::new(work);
                for (name, value) in [("start-field", start), ("end-field", end), ("leading-precision", leading), ("fractional-precision", fractional)] {
                    if !matches!(value, rtdb_plan::CastOption::Null) {
                        let rendered = value.to_edn();
                        values.push(work.pieces(&[RenderPiece::Text(":"), RenderPiece::Text(name), RenderPiece::Text(" "), RenderPiece::Text(&rendered)])?)?;
                    }
                }
                Some(joined(work, "{", &values, ", ", "}")?)
            }
        },
        rtdb_plan::CastTarget::Temporal(target) => match target.precision {
            Some(precision) => Some(work.pieces(&[RenderPiece::Text("{:precision "), RenderPiece::I64(precision), RenderPiece::Text("}")])?),
            None => None,
        },
        rtdb_plan::CastTarget::Interval(target) => {
            let mut values = ScratchVec::new(work);
            values.push(work.pieces(&[RenderPiece::Text(":start-field \""), RenderPiece::Text(target.start.name()), RenderPiece::Text("\"")])?)?;
            if let Some(end) = target.end { values.push(work.pieces(&[RenderPiece::Text(":end-field \""), RenderPiece::Text(end.name()), RenderPiece::Text("\"")])?)?; }
            if let Some(leading) = target.leading_precision { values.push(work.pieces(&[RenderPiece::Text(":leading-precision "), RenderPiece::I64(leading)])?)?; }
            if let Some(fractional) = target.fractional_precision { values.push(work.pieces(&[RenderPiece::Text(":fractional-precision "), RenderPiece::I64(fractional)])?)?; }
            Some(joined(work, "{", &values, ", ", "}")?)
        }
        rtdb_plan::CastTarget::IntervalEmptyOptions => Some(work.copy_text("{}")?),
        rtdb_plan::CastTarget::Checked { target, .. } | rtdb_plan::CastTarget::List(target) => cast_options(target, work)?,
        _ => None,
    })
}

enum RenderFrame<'source> {
    Raw(RegexFormView<'source>), Literal(RegexLiteralView<'source>), Text(&'static str),
    RawSequence { values: RegexFormIter<'source>, first: bool, close: &'static str },
    RawPairs { entries: RegexFormPairIter<'source>, first: bool },
    Values { values: RegexLiteralIter<'source>, first: bool, close: &'static str },
    ValuePairs { entries: RegexLiteralPairIter<'source>, first: bool },
    Fields { fields: RegexFieldIter<'source>, first: bool },
}
fn regex_form<'work>(value: RegexFormView<'_>, work: &'work ExplainWork) -> Result<WorkText<'work>> { render(RenderFrame::Raw(value), work) }
fn literal<'work>(value: RegexLiteralView<'_>, work: &'work ExplainWork) -> Result<WorkText<'work>> { render(RenderFrame::Literal(value), work) }
fn render<'work>(first: RenderFrame<'_>, work: &'work ExplainWork) -> Result<WorkText<'work>> {
    let mut sink = TextSink::new(work);
    let mut frames = ScratchVec::new(work);
    frames.push(first)?;
    while let Some(frame) = frames.pop() {
        match frame {
            RenderFrame::Text(value) => sink.append(&[RenderPiece::Text(value)])?,
            RenderFrame::Raw(value) => match value.kind() {
                F::Literal(value) => frames.push(RenderFrame::Literal(value))?,
                F::Character(ch) => if u32::from(ch) <= 0xffff { sink.append(&[RenderPiece::Text("\\"), RenderPiece::Text("u"), RenderPiece::HexU32(u32::from(ch), 4)])?; }
                    else { sink.append(&[RenderPiece::Text("\\"), RenderPiece::Char(ch)])?; },
                F::Symbol(value) => sink.append(&[RenderPiece::Text(value)])?,
                F::BigInteger(value) => sink.append(&[RenderPiece::Text(value), RenderPiece::Text("N")])?,
                F::Decimal(value) => sink.append(&[RenderPiece::Text(value), RenderPiece::Text("M")])?,
                F::List(values) | F::Vector(values) | F::Set(values) => {
                    let (open, close) = match value.kind() { F::List(_) => ("(", ")"), F::Vector(_) => ("[", "]"), _ => ("#{", "}") };
                    sink.append(&[RenderPiece::Text(open)])?;
                    frames.push(RenderFrame::RawSequence { values: values.iter(), first: true, close })?;
                }
                F::Map(entries) => { sink.append(&[RenderPiece::Text("{")])?; frames.push(RenderFrame::RawPairs { entries: entries.iter(), first: true })?; }
                F::Tagged { tag, value } => { sink.append(&[RenderPiece::Text("#"), RenderPiece::Text(tag), RenderPiece::Text(" ")])?; frames.push(RenderFrame::Raw(value))?; }
                F::AnonymousFunction(value) => { sink.append(&[RenderPiece::Text("#")])?; frames.push(RenderFrame::Raw(value))?; }
            },
            RenderFrame::Literal(value) => match value.kind() {
                LiteralKind::Text(value) => sink.append(&[RenderPiece::JsonText(value)])?,
                LiteralKind::Keyword(value) => sink.append(&[RenderPiece::Text(":"), RenderPiece::Text(value)])?,
                LiteralKind::List(values) | LiteralKind::FixedSizeList(values) | LiteralKind::Set(values) => {
                    let (open, close) = if matches!(value.kind(), LiteralKind::Set(_)) { ("#{", "}") } else { ("[", "]") };
                    sink.append(&[RenderPiece::Text(open)])?;
                    frames.push(RenderFrame::Values { values: values.iter(), first: true, close })?;
                }
                LiteralKind::Map { entries, .. } => { sink.append(&[RenderPiece::Text("{")])?; frames.push(RenderFrame::ValuePairs { entries: entries.iter(), first: true })?; }
                LiteralKind::Struct(fields) => { sink.append(&[RenderPiece::Text("{")])?; frames.push(RenderFrame::Fields { fields: fields.iter(), first: true })?; }
                LiteralKind::Leaf(leaf) => match leaf.packed_float() {
                    Some(component) => render_float(component, &mut sink)?,
                    None => render_leaf(leaf.value().as_ref(), &mut sink)?,
                },
            },
            RenderFrame::RawSequence { mut values, first, close } => match values.next() {
                Some(value) => { if !first { sink.append(&[RenderPiece::Text(" ")])?; } frames.push(RenderFrame::RawSequence { values, first: false, close })?; frames.push(RenderFrame::Raw(value))?; }
                None => sink.append(&[RenderPiece::Text(close)])?,
            },
            RenderFrame::RawPairs { mut entries, first } => match entries.next() {
                Some((key, value)) => {
                    if !first { sink.append(&[RenderPiece::Text(", ")])?; }
                    frames.push(RenderFrame::RawPairs { entries, first: false })?;
                    frames.push(RenderFrame::Raw(value))?;
                    frames.push(RenderFrame::Text(" "))?;
                    frames.push(RenderFrame::Raw(key))?;
                }
                None => sink.append(&[RenderPiece::Text("}")])?,
            },
            RenderFrame::Values { mut values, first, close } => match values.next() {
                Some(value) => { if !first { sink.append(&[RenderPiece::Text(" ")])?; } frames.push(RenderFrame::Values { values, first: false, close })?; frames.push(RenderFrame::Literal(value))?; }
                None => sink.append(&[RenderPiece::Text(close)])?,
            },
            RenderFrame::ValuePairs { mut entries, first } => match entries.next() {
                Some((key, value)) => {
                    if !first { sink.append(&[RenderPiece::Text(", ")])?; }
                    frames.push(RenderFrame::ValuePairs { entries, first: false })?;
                    frames.push(RenderFrame::Literal(value))?;
                    frames.push(RenderFrame::Text(" "))?;
                    frames.push(RenderFrame::Literal(key))?;
                }
                None => sink.append(&[RenderPiece::Text("}")])?,
            },
            RenderFrame::Fields { mut fields, first } => match fields.next() {
                Some((name, value)) => {
                    if !first { sink.append(&[RenderPiece::Text(", ")])?; }
                    sink.append(&[RenderPiece::Text(":"), RenderPiece::Text(name), RenderPiece::Text(" ")])?;
                    frames.push(RenderFrame::Fields { fields, first: false })?;
                    frames.push(RenderFrame::Literal(value))?;
                }
                None => sink.append(&[RenderPiece::Text("}")])?,
            },
        }
    }
    sink.finish()
}
fn render_float(value: rtdb_types::FloatComponent, sink: &mut TextSink<'_>) -> Result<()> {
    use rtdb_types::FloatComponent;
    const FLOAT_SAMPLE: Value = Value::F64(0.0);
    if let FloatComponent::Null = value {
        return sink.append(&[RenderPiece::Text("nil")]);
    }
    let _reservation = sink.work.memory.reserve(rtdb_expr::text_allocation_bound(&FLOAT_SAMPLE)?)?;
    let text = match value {
        FloatComponent::F32(value) => rtdb_types::java_f32_text(value),
        FloatComponent::F64(value) => rtdb_types::java_f64_text(value),
        FloatComponent::Null => unreachable!("null renders without scratch"),
    };
    sink.append(&[RenderPiece::Text(&text)])
}

fn render_leaf(value: &Value, sink: &mut TextSink<'_>) -> Result<()> {
    match value {
        Value::Null | Value::Undefined => sink.append(&[RenderPiece::Text("nil")]),
        Value::Bool(value) => sink.append(&[RenderPiece::Bool(*value)]),
        Value::I8(value) => sink.append(&[RenderPiece::I64(i64::from(*value))]),
        Value::I16(value) => sink.append(&[RenderPiece::I64(i64::from(*value))]),
        Value::I32(value) | Value::Oid(value) | Value::RegClass(value) | Value::RegProc(value) => sink.append(&[RenderPiece::I64(i64::from(*value))]),
        Value::I64(value) => sink.append(&[RenderPiece::I64(*value)]),
        Value::U8(value) => sink.append(&[RenderPiece::U64(u64::from(*value))]),
        Value::U16(value) | Value::F16(value) => sink.append(&[RenderPiece::U64(u64::from(*value))]),
        Value::U32(value) => sink.append(&[RenderPiece::U64(u64::from(*value))]),
        Value::U64(value) => sink.append(&[RenderPiece::U64(*value)]),
        Value::Uuid(bytes) => sink.append(&[
            RenderPiece::Text("#uuid \""), RenderPiece::HexBytes(&bytes[..4]), RenderPiece::Text("-"), RenderPiece::HexBytes(&bytes[4..6]),
            RenderPiece::Text("-"), RenderPiece::HexBytes(&bytes[6..8]), RenderPiece::Text("-"), RenderPiece::HexBytes(&bytes[8..10]),
            RenderPiece::Text("-"), RenderPiece::HexBytes(&bytes[10..]), RenderPiece::Text("\""),
        ]),
        Value::Binary(bytes) | Value::FixedSizeBinary(bytes) | Value::Transit(bytes) => sink.append(&[RenderPiece::Text("#bytes \""), RenderPiece::HexBytes(bytes), RenderPiece::Text("\"")]),
        Value::F32(value) => render_float(rtdb_types::FloatComponent::F32(*value), sink),
        Value::F64(value) => render_float(rtdb_types::FloatComponent::F64(*value), sink),
        Value::Decimal(decimal) => {
            let _reservation = sink.work.memory.reserve(rtdb_expr::text_allocation_bound(value)?)?;
            let text = rtdb_expr::decimal_text(decimal);
            sink.append(&[RenderPiece::Text(&text), RenderPiece::Text("M")])
        }
        Value::Uri(text) => {
            let uri = rtdb_types::UriParts::parse(text)?;
            let _reservation = sink.work.memory.reserve(uri.ascii_allocation_bound()?)?;
            let text = uri.to_ascii_string();
            sink.append(&[RenderPiece::Text("#xt/uri "), RenderPiece::JsonText(&text)])
        }
        other @ (Value::Date(_) | Value::DateDay(_) | Value::DateMilli(_) | Value::Time { .. } | Value::Timestamp { .. } | Value::Duration { .. }
            | Value::IntervalYearMonth(_) | Value::IntervalDayTime { .. } | Value::IntervalMonthDayNano { .. } | Value::IntervalMonthDayMicro { .. } | Value::TimestampTzRange { .. }) => {
            let _reservation = sink.work.memory.reserve(rtdb_expr::text_allocation_bound(other)?)?;
            let Value::Utf8(text) = rtdb_expr::cast(other, &LogicalType::Scalar(ScalarType::Utf8), "UTC")? else { return Err(invalid("explain-literal", "Temporal literal did not render as text")); };
            let tag = match other { Value::Date(_) | Value::DateDay(_) | Value::DateMilli(_) => "time/date", Value::Time { .. } => "time/time", Value::Timestamp { timezone: None, .. } => "time/date-time", Value::Timestamp { .. } => "time/zoned-date-time", Value::Duration { .. } => "time/duration", Value::TimestampTzRange { .. } => "xt/tstz-range", _ => "xt/interval" };
            sink.append(&[RenderPiece::Text("#"), RenderPiece::Text(tag), RenderPiece::Text(" "), RenderPiece::JsonText(&text)])
        }
        Value::Utf8(_) | Value::Keyword(_) | Value::List(_) | Value::Set(_) | Value::Map { .. } | Value::FixedSizeList(_) | Value::Struct(_) => Err(region_error()),
    }
}

#[cfg(test)]
fn patch_endpoint(expr: &Expr, rendered: String) -> Result<String> {
    let memory = MemoryBudget::new(1 << 20);
    let reservation = memory.reserve(0)?;
    let work = ExplainWork { memory, state: RefCell::new(WorkState::Building { reservation, base_bytes: 0, cumulative_buffers: 0 }) };
    let rendered = work.copy_text(&rendered)?;
    let result = work_patch_endpoint(expr, rendered)?;
    Ok(result.as_str().to_owned())
}

#[cfg(test)]
mod patch_endpoint_tests {
    use super::*;
    #[test]
    fn patch_explanation_retains_conformed_endpoint_tags() {
        let clock = Expr::Call { name: "current_timestamp".into(),args: vec![],return_type: LogicalType::Nothing };
        assert_eq!(patch_endpoint(&clock,"(current_timestamp)".into()).unwrap(),"[:now (current-timestamp)]");
        assert_eq!(patch_endpoint(&Expr::Literal(Value::Null),"nil".into()).unwrap(),"nil");
        let marker = Expr::Literal(Value::Keyword(Keyword::from_name("now")));
        assert_eq!(patch_endpoint(&marker,":now".into()).unwrap(),"[:now :now]");
        let parameter = Expr::Parameter { index: 0,data_type: LogicalType::Nothing };
        assert_eq!(patch_endpoint(&parameter,"?_0".into()).unwrap(),"[:param ?_0]");
    }
}

#[cfg(test)]
mod rendering_tests {
    use super::*;
    use rtdb_plan::{RegexForm, RegexFormBuilder};
    use rtdb_types::{CalendarDate, CivilDate, DateUnit, DecimalValue, TimeUnit};

    fn work(memory: &Arc<MemoryBudget>) -> ExplainWork {
        ExplainWork { memory: Arc::clone(memory), state: RefCell::new(WorkState::Building { reservation: memory.reserve(0).unwrap(), base_bytes: 0, cumulative_buffers: 0 }) }
    }
    fn raw(emit: impl FnOnce(&mut RegexFormBuilder) -> Result<()>) -> RegexForm {
        let mut builder = RegexFormBuilder::new_untracked();
        emit(&mut builder).unwrap();
        builder.finish().unwrap()
    }
    fn output(value: &Value) -> String {
        let memory = MemoryBudget::new(1 << 20);
        let work = work(&memory);
        let text = literal(RegexLiteralView::new(value), &work).unwrap();
        text.as_str().into()
    }

    #[test]
    fn explain_reads_packed_float_leaves_at_their_original_width() {
        let vector = Value::FixedSizeList(vec![Value::F32(0.1), Value::Null, Value::F64(-0.0), Value::F64(f64::NAN)].into());
        let LiteralKind::FixedSizeList(cells) = RegexLiteralView::new(&vector).kind() else { panic!("typed list") };
        let LiteralKind::Leaf(leaf) = cells.get(0).unwrap().kind() else { panic!("typed float leaf") };
        assert!(matches!(leaf.packed_float(), Some(rtdb_types::FloatComponent::F32(value)) if value.to_bits() == 0.1f32.to_bits()));
        assert_eq!(output(&vector), "[0.1 nil -0.0 NaN]");
    }

    #[test]
    fn all_forty_one_physical_renderers_keep_exact_wire_spelling() {
        let cases = vec![
            (Value::Undefined, "nil"), (Value::Null, "nil"), (Value::Bool(true), "true"),
            (Value::I8(-8), "-8"), (Value::I16(-16), "-16"), (Value::I32(-32), "-32"), (Value::I64(i64::MIN), "-9223372036854775808"),
            (Value::U8(8), "8"), (Value::U16(16), "16"), (Value::U32(32), "32"), (Value::U64(u64::MAX), "18446744073709551615"),
            (Value::F16(0x8000), "32768"), (Value::F32(-0.0), "-0.0"), (Value::F64(f64::INFINITY), "Infinity"),
            (Value::Utf8("é😀\n\0".into()), r#""é😀\n\u0000""#),
            (Value::Binary(vec![0, 255]), r#"#bytes "00ff""#), (Value::FixedSizeBinary(vec![9]), r#"#bytes "09""#),
            (Value::Decimal(DecimalValue { unscaled: vec![123], precision: 3, scale: 2, bit_width: 16 }), "1.23M"),
            (Value::Date(CalendarDate::new(CivilDate::from_ymd(2000, 1, 2).unwrap(), DateUnit::Day)), r#"#time/date "2000-01-02""#),
            (Value::DateDay(0), r#"#time/date "1970-01-01""#), (Value::DateMilli(0), r#"#time/date "1970-01-01""#),
            (Value::Time { value: 0, unit: TimeUnit::Second, bit_width: 32 }, r#"#time/time "00:00""#),
            (Value::Timestamp { value: 0, unit: TimeUnit::Second, timezone: None }, r#"#time/date-time "1970-01-01T00:00""#),
            (Value::Duration { value: 0, unit: TimeUnit::Second }, r#"#time/duration "PT0S""#),
            (Value::IntervalYearMonth(0), r#"#xt/interval "P""#), (Value::IntervalDayTime { days: 0, milliseconds: 0 }, r#"#xt/interval "P""#),
            (Value::IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 0 }, r#"#xt/interval "P""#),
            (Value::IntervalMonthDayMicro { months: 0, days: 0, nanoseconds: 0 }, r#"#xt/interval "P""#),
            (Value::Keyword(Keyword::from_name("plain/name")), ":plain/name"),
            (Value::Uuid([0; 16]), r#"#uuid "00000000-0000-0000-0000-000000000000""#),
            (Value::Uri("https://example.test/é".into()), r#"#xt/uri "https://example.test/%C3%A9""#),
            (Value::Transit(vec![0, 255]), r#"#bytes "00ff""#), (Value::Oid(-1), "-1"), (Value::RegClass(2), "2"), (Value::RegProc(3), "3"),
            (Value::list(vec![Value::I64(1)]), "[1]"), (Value::Set(vec![Value::Null]), "#{nil}"),
            (Value::Map { entries: vec![(Value::Keyword(Keyword::from_name("k")), Value::Bool(true))], sorted: true }, "{:k true}"),
            (Value::FixedSizeList(vec![Value::Bool(false)].into()), "[false]"),
            (Value::TimestampTzRange { lower: 0, upper: 0 }, r#"#xt/tstz-range "[#time/zoned-date-time \"1970-01-01T00:00Z[UTC]\" #time/zoned-date-time \"1970-01-01T00:00Z[UTC]\"]""#),
            (Value::Struct(StructFields::from([("z".into(), Value::Bool(false)), ("a".into(), Value::I64(1))])), "{:z false, :a 1}"),
        ];
        assert_eq!(cases.len(), 41);
        for (value, expected) in cases { assert_eq!(output(&value), expected); }
        assert_eq!(output(&Value::Timestamp { value: 0, unit: TimeUnit::Second, timezone: Some("UTC".into()) }), r#"#time/zoned-date-time "1970-01-01T00:00Z[UTC]""#);
    }

    #[test]
    fn all_eleven_raw_renderers_keep_syntax_and_character_domains() {
        let cases = [
            (raw(|builder| builder.literal(&Value::Null)), "nil"),
            (raw(|builder| builder.character('\n')), "\\u000a"),
            (raw(|builder| builder.symbol("ns/name")), "ns/name"),
            (raw(|builder| builder.big_integer("00099")), "00099N"),
            (raw(|builder| builder.decimal("1.00")), "1.00M"),
            (raw(|builder| { builder.begin_list()?; builder.symbol("x")?; builder.end() }), "(x)"),
            (raw(|builder| { builder.begin_vector()?; builder.symbol("x")?; builder.end() }), "[x]"),
            (raw(|builder| { builder.begin_set()?; builder.symbol("x")?; builder.end() }), "#{x}"),
            (raw(|builder| { builder.begin_map()?; builder.symbol("k")?; builder.character('x')?; builder.end() }), "{k \\u0078}"),
            (raw(|builder| { builder.begin_tagged("reader/tag")?; builder.symbol("body")?; builder.end() }), "#reader/tag body"),
            (raw(|builder| { builder.begin_anonymous()?; builder.begin_list()?; builder.end()?; builder.end() }), "#()"),
        ];
        assert_eq!(cases.len(), 11);
        let memory = MemoryBudget::new(1 << 20);
        let work = work(&memory);
        for (value, expected) in cases { assert_eq!(regex_form(value.view(), &work).unwrap().as_str(), expected); }
        let supplementary = raw(|builder| builder.character('😀'));
        assert_eq!(regex_form(supplementary.view(), &work).unwrap().as_str(), "\\😀");
    }

    #[test]
    fn exact_and_one_byte_short_text_admission_cover_the_owned_result() {
        let memory = MemoryBudget::new(3);
        {
            let work = work(&memory);
            let text = work.copy_text("abc").unwrap();
            assert_eq!(text.as_str(), "abc");
            assert_eq!(memory.used_bytes(), 3);
            drop(text);
            assert_eq!(memory.used_bytes(), 3);
        }
        assert_eq!(memory.used_bytes(), 0);
        let memory = MemoryBudget::new(2);
        let work = work(&memory);
        assert_eq!(work.copy_text("abc").err().unwrap().code, "rtdb/memory-budget-exceeded");
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn sealed_piece_vocabulary_preserves_digits_json_escapes_and_borrowed_joins() {
        let memory = MemoryBudget::new(1 << 20);
        let work = work(&memory);
        let words = [work.copy_text("one").unwrap(), work.copy_text("two").unwrap()];
        let text = work.pieces(&[
            RenderPiece::Text("x"), RenderPiece::Char('😀'), RenderPiece::Bool(false),
            RenderPiece::I64(i64::MIN), RenderPiece::U64(u64::MAX), RenderPiece::HexU32(0xab, 4), RenderPiece::HexBytes(&[0, 255]),
            RenderPiece::JsonText("\"\\\n\r\t\u{8}\u{c}\u{1}é😀"), RenderPiece::Join(&words, "/"), RenderPiece::RepeatStatic("..", 3),
        ]).unwrap();
        assert_eq!(text.as_str(), concat!("x😀false-92233720368547758081844674407370955161500ab00ff", r#""\"\\\n\r\t\b\f\u0001é😀""#, "one/two......"));
    }

    #[test]
    fn failed_old_and_new_text_growth_preserves_the_original_prefix() {
        let memory = MemoryBudget::new(23);
        {
            let work = work(&memory);
            let mut sink = TextSink::new(&work);
            sink.append(&[RenderPiece::Text("12345678")]).unwrap();
            assert_eq!(sink.append(&[RenderPiece::Text("9")]).unwrap_err().code, "rtdb/memory-budget-exceeded");
            assert_eq!(sink.bytes.as_slice(), b"12345678");
            assert_eq!(memory.used_bytes(), 8);
            assert_eq!(memory.peak_bytes(), 8);
        }
        assert_eq!(memory.used_bytes(), 0);
        let memory = MemoryBudget::new(24);
        let work = work(&memory);
        let mut sink = TextSink::new(&work);
        sink.append(&[RenderPiece::Text("12345678")]).unwrap();
        sink.append(&[RenderPiece::Text("9")]).unwrap();
        assert_eq!(sink.finish().unwrap().as_str(), "123456789");
        assert_eq!(memory.used_bytes(), 24);
    }

    #[test]
    fn concrete_leaf_scratch_overlaps_the_admitted_destination() {
        for value in [Value::Uri("https://example.test/é".into()), Value::Decimal(DecimalValue { unscaled: vec![123], precision: 3, scale: 2, bit_width: 16 }), Value::Time { value: 0, unit: TimeUnit::Second, bit_width: 32 }] {
            let bound = match &value { Value::Uri(text) => rtdb_types::UriParts::parse(text).unwrap().ascii_allocation_bound().unwrap(), _ => rtdb_expr::text_allocation_bound(&value).unwrap() };
            let memory = MemoryBudget::new(1 << 20);
            {
                let work = work(&memory);
                let mut sink = TextSink::new(&work);
                render_leaf(&value, &mut sink).unwrap();
                let text = sink.finish().unwrap();
                assert!(!text.as_str().is_empty());
                assert!(memory.peak_bytes() >= bound + memory.used_bytes());
            }
            assert_eq!(memory.used_bytes(), 0);
        }
    }

    #[test]
    fn sealed_pieces_reuse_eager_semantic_results_without_repeating_operations() {
        let memory = MemoryBudget::new(1 << 20);
        let work = work(&memory);
        for index in 0..3 {
            let mut operations = 0;
            let value = match index { 0 => Value::Uri("https://example.test/é".into()), 1 => Value::Decimal(DecimalValue { unscaled: vec![123], precision: 3, scale: 2, bit_width: 16 }), _ => Value::Time { value: 0, unit: TimeUnit::Second, bit_width: 32 } };
            let uri = match &value { Value::Uri(text) => Some(rtdb_types::UriParts::parse(text).unwrap()), _ => None };
            let bound = match &uri { Some(uri) => uri.ascii_allocation_bound().unwrap(), None => rtdb_expr::text_allocation_bound(&value).unwrap() };
            let _reservation = memory.reserve(bound).unwrap();
            operations += 1;
            let text = match &value {
                Value::Uri(_) => uri.as_ref().unwrap().to_ascii_string(),
                Value::Decimal(value) => rtdb_expr::decimal_text(value),
                value => match rtdb_expr::cast(value, &LogicalType::Scalar(ScalarType::Utf8), "UTC").unwrap() { Value::Utf8(text) => text, _ => panic!() },
            };
            let pieces = [RenderPiece::JsonText(&text)];
            let first = work.pieces(&pieces).unwrap();
            let second = work.pieces(&pieces).unwrap();
            assert_eq!(first.as_str(), second.as_str());
            assert_eq!(operations, 1);
        }
    }

    #[test]
    fn physical_leaf_semantic_errors_follow_actual_left_to_right_demand() {
        for (values, expected) in [
            (vec![Value::Time { value: -1, unit: TimeUnit::Nanosecond, bit_width: 64 }, Value::Uri("%".into())], "xtdb.expression/overflow-error"),
            (vec![Value::Uri("%".into()), Value::Time { value: -1, unit: TimeUnit::Nanosecond, bit_width: 64 }], "xtdb/invalid-uri"),
        ] {
            let memory = MemoryBudget::new(1 << 20);
            {
                let work = work(&memory);
                let source = Value::list(values);
                assert_eq!(literal(RegexLiteralView::new(&source), &work).err().unwrap().code, expected);
            }
            assert_eq!(memory.used_bytes(), 0);
        }
    }

    #[test]
    fn consuming_output_factories_reject_a_foreign_work_region() {
        let first_memory = MemoryBudget::new(1 << 20);
        let second_memory = MemoryBudget::new(1 << 20);
        let first = work(&first_memory);
        let second = work(&second_memory);
        let text = first.copy_text("source").unwrap();
        assert_eq!(second.text(text).err().unwrap().code, "rtdb.query/explain-region");
        assert_eq!(strings(&second, ScratchVec::new(&first)).err().unwrap().code, "rtdb.query/explain-region");
        assert_eq!(joined(&second, "[", &ScratchVec::new(&first), " ", "]").err().unwrap().code, "rtdb.query/explain-region");
        assert_eq!(first_memory.used_bytes(), 6);
        assert_eq!(second_memory.used_bytes(), 0);
    }

    #[test]
    fn public_plan_owns_final_rows_after_work_exit_until_the_last_clone_drops() {
        let element = Field { id: ColumnId(0), name: "value".into(), data_type: LogicalType::Scalar(ScalarType::I64), qualifier: None, qualified_only: false, unnamed: false };
        let column_table = Relation::new(Schema::new(vec![element.clone()]).unwrap(), RelationNode::ColumnTable { expr: Expr::Literal(Value::Uri("%".into())), element }).unwrap();
        for source in [Relation::unit(), column_table] {
            let memory = MemoryBudget::new(1 << 20);
            let plan = ExplainPlan::new(&source, &memory).unwrap();
            assert_eq!(plan.rows[0], [Value::Utf8("->".into()), Value::Keyword(Keyword::from_name("table")), Value::Null]);
            assert_eq!(plan.schema.len(), 3);
            let retained = memory.used_bytes();
            assert!(retained >= plan.retained_bytes);
            assert!(plan.copy_bound().unwrap() >= plan.retained_bytes);
            let clone = Arc::clone(&plan);
            drop(plan);
            assert_eq!(memory.used_bytes(), retained);
            assert_eq!(clone.rows[0][0], Value::Utf8("->".into()));
            drop(clone);
            assert_eq!(memory.used_bytes(), 0);
        }
    }

    #[test]
    fn catalog_independent_explain_recharge_retains_the_same_report_rows() {
        let preparation = MemoryBudget::new(1 << 20);
        let execution = MemoryBudget::new(1 << 20);
        let plan = ExplainPlan::new(&Relation::unit(), &preparation).unwrap();
        let recharged = plan.recharged(&execution).unwrap();
        assert!(std::ptr::eq(plan.rows().as_ptr(), recharged.rows().as_ptr()));
        assert_eq!(preparation.used_bytes(), execution.used_bytes());
        drop(plan);
        assert_eq!(preparation.used_bytes(), 0);
        assert_eq!(recharged.rows()[0][0], Value::Utf8("->".into()));
        drop(recharged);
        assert_eq!(execution.used_bytes(), 0);
    }

    #[test]
    fn failed_publication_keeps_the_unique_building_lease_through_row_cleanup() {
        let memory = MemoryBudget::new(1 << 20);
        {
            let work = work(&memory);
            let mut rows = WorkRows::new(&work);
            rows.push([work.text(work.copy_text("->").unwrap()).unwrap(), work.keyword(work.copy_text("table").unwrap()).unwrap(), work.null()]).unwrap();
            let before = memory.used_bytes();
            let schema = Schema::new((0..3).map(|index| Field { id: ColumnId(index), name: "field".into(), data_type: LogicalType::Nothing, qualifier: None, qualified_only: false, unnamed: false }).collect()).unwrap();
            assert_eq!(rows.publish(schema, usize::MAX).err().unwrap().code, "rtdb.query/explain-size");
            assert!(matches!(*work.state.borrow(), WorkState::Building { .. }));
            assert_eq!(memory.used_bytes(), before);
        }
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn overwritten_output_fields_keep_cumulative_admission_until_finalization() {
        let memory = MemoryBudget::new(1 << 20);
        let work = work(&memory);
        let mut fields = WorkMap::new(&work);
        fields.insert("field", work.text(work.copy_text("discarded").unwrap()).unwrap()).unwrap();
        fields.insert("field", work.text(work.copy_text("ok").unwrap()).unwrap()).unwrap();
        let output = fields.finish().unwrap();
        assert_eq!(output.value, Value::Struct(StructFields::from([("field".into(), Value::Utf8("ok".into()))])));
        let state = work.state.borrow();
        let WorkState::Building { cumulative_buffers, .. } = &*state else { panic!() };
        assert!(output.final_buffers < *cumulative_buffers);
    }

    #[test]
    fn raw_and_physical_map_keys_keep_container_shapes_order_and_duplicates() {
        let value = Value::Map { entries: vec![
            (Value::list(vec![Value::Keyword(Keyword::from_name("x"))]), Value::Struct(StructFields::from([("nested".into(), Value::Set(vec![Value::I64(1), Value::I64(2)]))]))),
            (Value::list(vec![Value::Keyword(Keyword::from_name("x"))]), Value::I64(3)),
        ], sorted: true };
        assert_eq!(output(&value), "{[:x] {:nested #{1 2}}, [:x] 3}");
        let raw = raw(|builder| {
            builder.begin_map()?;
            builder.begin_list()?; builder.symbol("x")?; builder.end()?;
            builder.begin_vector()?; builder.character('z')?; builder.end()?;
            builder.begin_list()?; builder.symbol("x")?; builder.end()?;
            builder.literal(&Value::I64(3))?;
            builder.end()
        });
        let memory = MemoryBudget::new(1 << 20);
        let work = work(&memory);
        assert_eq!(regex_form(raw.view(), &work).unwrap().as_str(), "{(x) [\\u007a], (x) 3}");
    }

    #[test]
    fn twenty_thousand_raw_and_physical_levels_render_without_recursive_children() {
        const DEPTH: usize = 20_000;
        let source = raw(|builder| {
            for _ in 0..DEPTH { builder.begin_list()?; }
            builder.literal(&Value::I64(7))?;
            for _ in 0..DEPTH { builder.end()?; }
            Ok(())
        });
        let memory = MemoryBudget::new(128 << 20);
        {
            let work = work(&memory);
            let text = regex_form(source.view(), &work).unwrap();
            assert_eq!(text.as_str().bytes().filter(|byte| *byte == b'(').count(), DEPTH);
            assert_eq!(text.as_str().bytes().filter(|byte| *byte == b')').count(), DEPTH);
            assert_eq!(text.as_str().len(), DEPTH * 2 + 1);
            assert_eq!(text.as_str().as_bytes()[DEPTH], b'7');
        }
        assert_eq!(memory.used_bytes(), 0);
        struct NestedLists(Option<Value>);
        impl Drop for NestedLists {
            fn drop(&mut self) {
                let mut value = self.0.take();
                while let Some(Value::List(children)) = value { value = children.into_values().pop(); }
            }
        }
        let mut value = Value::I64(7);
        for _ in 0..DEPTH { value = Value::list(vec![value]); }
        let source = NestedLists(Some(value));
        {
            let work = work(&memory);
            let text = literal(RegexLiteralView::new(source.0.as_ref().unwrap()), &work).unwrap();
            assert_eq!(text.as_str().bytes().filter(|byte| *byte == b'[').count(), DEPTH);
            assert_eq!(text.as_str().bytes().filter(|byte| *byte == b']').count(), DEPTH);
            assert_eq!(text.as_str().len(), DEPTH * 2 + 1);
            assert_eq!(text.as_str().as_bytes()[DEPTH], b'7');
        }
        assert_eq!(memory.used_bytes(), 0);
    }
}
