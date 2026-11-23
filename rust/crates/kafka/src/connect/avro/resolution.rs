use std::{collections::{BTreeMap, BTreeSet}, sync::Arc};
use apache_avro::{Schema, schema::{Aliases, Name}};
use rtdb_columnar::MemoryReservation;
use rtdb_types::Result;
use super::{Avro, AvroDatumPlan, AvroDatum, ConversionContext, error};
#[path = "resolution_default.rs"] mod defaults;
#[path = "resolution_value.rs"] mod value;
#[path = "resolution_binary.rs"] mod binary;
#[cfg(test)] #[path = "resolution_tests.rs"] mod tests;
pub(super) use value::value_bound;
pub(super) use defaults::allocation_bound as default_allocation_bound;

pub(in crate::connect) struct AvroResolution { actions: Vec<Action>, root: usize, writer: Arc<AvroDatumPlan>, reader: Arc<AvroDatumPlan>, charges: Vec<Arc<MemoryReservation>> }

enum Action {
    Error(String),
    Scalar { writer: Kind, reader: Kind, fixed: Option<usize> },
    Array(usize), Map { child: usize, strings: bool },
    Enum(Vec<Option<(u32, String)>>),
    Record { writer_names: Vec<String>, fields: Vec<Option<(usize, usize)>>, reader_names: Vec<String>, defaults: Vec<(usize, Avro)> },
    WriterUnion { branches: Vec<usize>, equivalent: bool },
    ReaderUnion { index: u32, child: usize },
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Kind { Null, Boolean, Int, Long, Float, Double, String, Bytes, Fixed, Enum, Record, Array, Map, Union }

impl Kind {
    fn numeric(self) -> Option<super::datum::NumericKind> {
        use super::datum::NumericKind;
        Some(match self {
            Self::Int => NumericKind::Int, Self::Long => NumericKind::Long,
            Self::Float => NumericKind::Float, Self::Double => NumericKind::Double,
            _ => return None,
        })
    }
}

impl AvroResolution {
    pub fn compile(writer: &Arc<AvroDatumPlan>, reader: &Arc<AvroDatumPlan>, context: &mut ConversionContext) -> Result<Self> {
        writer.retain_source(context)?;
        reader.retain_source(context)?;
        let bytes = writer.schema_bytes.checked_add(reader.schema_bytes).and_then(|bytes| bytes.checked_mul(64)).and_then(|bytes| bytes.checked_add(65_536)).ok_or_else(overflow)?;
        let charge = context.reserve(bytes)?;
        let mut aliases = AliasIndex::default();
        if writer.root != reader.root || writer.names != reader.names || writer.defaults != reader.defaults { aliases.collect(&reader.root, &reader.names, &mut BTreeSet::new(), 0)?; }
        aliases.validate(&writer.root, &writer.names, &mut BTreeSet::new(), 0)?;
        let mut builder = ResolutionBuilder { writer, reader, aliases, nodes: Vec::new(), pairs: BTreeMap::new(), context, charges: vec![charge] };
        let root = builder.resolve(&writer.root, &reader.root, 0)?;
        let actions = builder.nodes.into_iter().map(|node| node.ok_or_else(|| error("avro-resolution", "Unfinished Avro resolution graph"))).collect::<Result<_>>()?;
        Ok(Self { actions, root, writer: Arc::clone(writer), reader: Arc::clone(reader), charges: builder.charges })
    }

    pub fn resolve(&self, datum: AvroDatum, context: &mut ConversionContext) -> Result<AvroDatum> {
        self.writer.retain_source(context)?;
        self.reader.retain_source(context)?;
        datum.plan.retain_source(context)?;
        for charge in &self.charges { context.retain(Arc::clone(charge))?; }
        let bound = self.bound(self.root, &datum.value, 0)?;
        context.reserve(bound)?;
        Ok(AvroDatum { value: self.apply(self.root, datum.value, 0)?, plan: Arc::clone(&self.reader) })
    }
}

struct ResolutionBuilder<'a> {
    writer: &'a AvroDatumPlan, reader: &'a AvroDatumPlan, aliases: AliasIndex,
    nodes: Vec<Option<Action>>, pairs: BTreeMap<(usize, usize), usize>,
    context: &'a mut ConversionContext, charges: Vec<Arc<MemoryReservation>>,
}
impl ResolutionBuilder<'_> {
    fn claim(&mut self, bytes: usize) -> Result<()> { self.charges.push(self.context.reserve(bytes)?); Ok(()) }
    fn resolve(&mut self, writer: &Schema, reader: &Schema, depth: usize) -> Result<usize> {
        nesting(depth)?;
        let writer_plan = self.writer; let reader_plan = self.reader;
        let writer = physical(super::resolve(writer, &writer_plan.names)?);
        let reader = physical(super::resolve(reader, &reader_plan.names)?);
        let key = pair(writer, reader);
        if let Some(id) = self.pairs.get(&key) { return Ok(*id); }
        self.claim(4096)?;
        let id = self.nodes.len(); self.nodes.push(None); self.pairs.insert(key, id);
        let wk = kind(writer); let rk = kind(reader);
        let action = if let Schema::Union(union) = writer {
            self.claim(union.variants().len().checked_mul(64).ok_or_else(overflow)?)?;
            let equivalent = self.equivalent(writer, reader, &mut BTreeMap::new(), depth + 1)?;
            let mut branches = Vec::with_capacity(union.variants().len());
            for (index, branch) in union.variants().iter().enumerate() {
                let target = if equivalent { let Schema::Union(reader) = reader else { return Err(invalid()); }; &reader.variants()[index] } else { reader };
                branches.push(self.resolve(branch, target, depth + 1)?);
            }
            Action::WriterUnion { branches, equivalent }
        } else if wk == rk {
            match (writer, reader) {
                (Schema::Array(writer), Schema::Array(reader)) => Action::Array(self.resolve(&writer.items, &reader.items, depth + 1)?),
                (Schema::Map(writer), Schema::Map(reader)) => Action::Map { child: self.resolve(&writer.types, &reader.types, depth + 1)?, strings: reader.attributes.get("avro.java.string").and_then(serde_json::Value::as_str) == Some("String") },
                (Schema::Record(writer_record), Schema::Record(reader_record)) => {
                    let bytes = writer_record.fields.iter().chain(&reader_record.fields).try_fold(4096usize, |bytes, field| bytes.checked_add(field.name.len()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(overflow))?;
                    self.claim(bytes)?;
                    let mut fields = Vec::with_capacity(writer_record.fields.len());
                    for field in &writer_record.fields {
                        let field_name = self.aliases.field(&writer_record.name, &field.name);
                        fields.push(match reader_record.fields.iter().position(|reader| reader.name == field_name) {
                            Some(index) => Some((index, self.resolve(&field.schema, &reader_record.fields[index].schema, depth + 1)?)), None => None,
                        });
                    }
                    let mut defaults = Vec::new(); let mut missing = None;
                    for (index, field) in reader_record.fields.iter().enumerate() {
                        if writer_record.fields.iter().any(|writer| self.aliases.field(&writer_record.name, &writer.name) == field.name) { continue; }
                        let Some(default) = reader_plan.defaults.get(&reader_record.name.fullname(None), &field.name) else { missing = Some(field.name.clone()); break; };
                        let bytes = defaults::bound(&field.schema, default, &reader_plan.names, &reader_plan.defaults, depth + 1)?;
                        self.claim(bytes)?;
                        defaults.push((index, defaults::read(&field.schema, default, &reader_plan.names, &reader_plan.defaults, depth + 1)?));
                    }
                    match missing {
                        Some(name) => Action::Error(format!("Missing required reader field '{name}'")),
                        None => Action::Record { writer_names: writer_record.fields.iter().map(|field| field.name.clone()).collect(), fields, reader_names: reader_record.fields.iter().map(|field| field.name.clone()).collect(), defaults },
                    }
                }
                (Schema::Enum(writer), Schema::Enum(reader)) => {
                    if self.aliases.short_name(&writer.name) != reader.name.name { Action::Error("Avro enum names differ".into()) }
                    else {
                        self.claim(writer.symbols.len().checked_mul(reader.symbols.iter().map(String::len).max().unwrap_or(0).checked_add(128).ok_or_else(overflow)?).ok_or_else(overflow)?)?;
                        let fallback = reader.default.as_ref().and_then(|symbol| reader.symbols.iter().position(|candidate| candidate == symbol));
                        let symbols = writer.symbols.iter().map(|symbol| reader.symbols.iter().position(|candidate| candidate == symbol).or(fallback).map(|index| (index as u32, reader.symbols[index].clone()))).collect();
                        Action::Enum(symbols)
                    }
                }
                (Schema::Fixed(writer), Schema::Fixed(reader)) if self.aliases.short_name(&writer.name) != reader.name.name || writer.size != reader.size => Action::Error("Avro fixed name or size differs".into()),
                (Schema::Fixed(reader), _) => Action::Scalar { writer: wk, reader: rk, fixed: Some(reader.size) },
                _ => Action::Scalar { writer: wk, reader: rk, fixed: None },
            }
        } else if let Schema::Union(union) = reader {
            match self.reader_branch(writer, union.variants(), depth + 1)? {
                Some(index) => Action::ReaderUnion { index: index as u32, child: self.resolve(writer, &union.variants()[index], depth + 1)? },
                None => Action::Error("No matching reader union branch".into()),
            }
        } else if promotes(wk, rk) { Action::Scalar { writer: wk, reader: rk, fixed: None } }
        else { Action::Error("Incompatible Avro writer and reader types".into()) };
        self.nodes[id] = Some(action); Ok(id)
    }

    fn reader_branch(&mut self, writer: &Schema, branches: &[Schema], depth: usize) -> Result<Option<usize>> {
        let reader_plan = self.reader; let wk = kind(writer); let mut structural = None;
        for (index, branch) in branches.iter().enumerate() {
            let branch = physical(super::resolve(branch, &reader_plan.names)?);
            if wk != kind(branch) { continue; }
            match (named(writer), named(branch)) {
                (Some((writer_name, _)), Some((reader_name, _))) => {
                    if self.aliases.name(writer_name) == reader_name.fullname(None) { return Ok(Some(index)); }
                    if wk == Kind::Record {
                        let candidate = self.resolve(writer, branch, depth + 1)?;
                        let failed = match &self.nodes[candidate] {
                            Some(Action::Error(_)) => true,
                            Some(Action::Record { fields, .. }) => fields.iter().flatten().any(|(_, child)| matches!(&self.nodes[*child], Some(Action::Error(_)))),
                            _ => false,
                        };
                        if !failed && (structural.is_none() || self.aliases.short_name(writer_name) == reader_name.name) { structural = Some(index); }
                    }
                }
                _ => return Ok(Some(index)),
            }
        }
        if structural.is_some() { return Ok(structural); }
        for (index, branch) in branches.iter().enumerate() { if promotes(wk, kind(physical(super::resolve(branch, &self.reader.names)?))) { return Ok(Some(index)); } }
        Ok(None)
    }

    fn equivalent(&self, writer: &Schema, reader: &Schema, seen: &mut BTreeMap<(usize, usize), bool>, depth: usize) -> Result<bool> {
        nesting(depth)?;
        let writer = physical(super::resolve(writer, &self.writer.names)?); let reader = physical(super::resolve(reader, &self.reader.names)?);
        if kind(writer) != kind(reader) { return Ok(false); }
        if let (Some((writer, _)), Some((reader, _))) = (named(writer), named(reader)) { if self.aliases.short_name(writer) != reader.name { return Ok(false); } }
        Ok(match (writer, reader) {
            (Schema::Record(writer_record), Schema::Record(reader_record)) => {
                let key = pair(writer, reader); if let Some(value) = seen.get(&key) { return Ok(*value); } seen.insert(key, true);
                let mut equal = writer_record.fields.len() == reader_record.fields.len();
                for (writer, reader) in writer_record.fields.iter().zip(&reader_record.fields) {
                    if self.aliases.field(&writer_record.name, &writer.name) != reader.name || !self.equivalent(&writer.schema, &reader.schema, seen, depth + 1)? { equal = false; break; }
                }
                seen.insert(key, equal); equal
            }
            (Schema::Union(writer), Schema::Union(reader)) => {
                if writer.variants().len() != reader.variants().len() { return Ok(false); }
                for (writer, reader) in writer.variants().iter().zip(reader.variants()) { if !self.equivalent(writer, reader, seen, depth + 1)? { return Ok(false); } } true
            }
            (Schema::Array(writer), Schema::Array(reader)) => self.equivalent(&writer.items, &reader.items, seen, depth + 1)?,
            (Schema::Map(writer), Schema::Map(reader)) => self.equivalent(&writer.types, &reader.types, seen, depth + 1)?,
            (Schema::Fixed(writer), Schema::Fixed(reader)) => writer.size == reader.size,
            (Schema::Enum(writer), Schema::Enum(reader)) => writer.symbols == reader.symbols,
            _ => true,
        })
    }
}

#[derive(Default)]
struct AliasIndex { names: BTreeMap<String, String>, fields: BTreeMap<String, BTreeMap<String, String>> }
impl AliasIndex {
    fn name(&self, name: &Name) -> String { let original = name.fullname(None); self.names.get(&original).cloned().unwrap_or(original) }
    fn short_name(&self, name: &Name) -> String { self.name(name).rsplit('.').next().unwrap_or_default().to_owned() }
    fn field(&self, record: &Name, field: &str) -> String { self.fields.get(&self.name(record)).and_then(|fields| fields.get(field)).cloned().unwrap_or_else(|| field.to_owned()) }
    fn collect(&mut self, schema: &Schema, names: &BTreeMap<String, Schema>, seen: &mut BTreeSet<usize>, depth: usize) -> Result<()> {
        nesting(depth)?; let schema = physical(super::resolve(schema, names)?);
        if let Some((name, aliases)) = named(schema) { for alias in aliases.iter().flatten() { self.names.insert(alias.fullname(name.namespace.clone()), name.fullname(None)); } }
        if !seen.insert(schema as *const Schema as usize) { return Ok(()); }
        match schema {
            Schema::Record(record) => {
                for field in &record.fields {
                    for alias in field.aliases.iter().flatten() { self.fields.entry(record.name.fullname(None)).or_default().insert(alias.clone(), field.name.clone()); }
                    self.collect(&field.schema, names, seen, depth + 1)?;
                }
                if let Some(fields) = self.fields.get(&record.name.fullname(None)).cloned() { for alias in record.aliases.iter().flatten() { self.fields.insert(alias.fullname(record.name.namespace.clone()), fields.clone()); } }
            }
            Schema::Array(array) => self.collect(&array.items, names, seen, depth + 1)?,
            Schema::Map(map) => self.collect(&map.types, names, seen, depth + 1)?,
            Schema::Union(union) => for schema in union.variants() { self.collect(schema, names, seen, depth + 1)?; },
            _ => {}
        }
        Ok(())
    }
    fn validate(&self, schema: &Schema, names: &BTreeMap<String, Schema>, seen: &mut BTreeSet<usize>, depth: usize) -> Result<()> {
        nesting(depth)?; let schema = physical(super::resolve(schema, names)?);
        if !seen.insert(schema as *const Schema as usize) { return Ok(()); }
        match schema {
            Schema::Record(record) => {
                let mut fields = BTreeSet::new();
                for field in &record.fields { if !fields.insert(self.field(&record.name, &field.name)) { return Err(error("avro-resolution", "Reader aliases create duplicate writer fields")); } self.validate(&field.schema, names, seen, depth + 1)?; }
            }
            Schema::Array(array) => self.validate(&array.items, names, seen, depth + 1)?, Schema::Map(map) => self.validate(&map.types, names, seen, depth + 1)?,
            Schema::Union(union) => {
                let mut branches = BTreeSet::new();
                for branch in union.variants() { let target = physical(super::resolve(branch, names)?); let name = named(target).map(|(name, _)| self.name(name)); if !branches.insert((kind(target), name)) { return Err(error("avro-resolution", "Reader aliases create duplicate writer union branches")); } self.validate(branch, names, seen, depth + 1)?; }
            }
            _ => {}
        }
        Ok(())
    }
}

fn pair(writer: &Schema, reader: &Schema) -> (usize, usize) { (writer as *const Schema as usize, reader as *const Schema as usize) }
fn physical(schema: &Schema) -> &Schema { if let Schema::Decimal(decimal) = schema { physical(&decimal.inner) } else { schema } }
fn named(schema: &Schema) -> Option<(&Name, &Aliases)> { match physical(schema) { Schema::Record(record) => Some((&record.name, &record.aliases)), Schema::Enum(value) => Some((&value.name, &value.aliases)), Schema::Fixed(value) => Some((&value.name, &value.aliases)), _ => None } }
fn kind(schema: &Schema) -> Kind {
    match physical(schema) {
        Schema::Null => Kind::Null, Schema::Boolean => Kind::Boolean, Schema::Int | Schema::Date | Schema::TimeMillis => Kind::Int,
        Schema::Long | Schema::TimeMicros | Schema::TimestampMillis | Schema::TimestampMicros | Schema::TimestampNanos | Schema::LocalTimestampMillis | Schema::LocalTimestampMicros | Schema::LocalTimestampNanos => Kind::Long,
        Schema::Float => Kind::Float, Schema::Double => Kind::Double, Schema::String | Schema::Uuid => Kind::String, Schema::Bytes | Schema::BigDecimal => Kind::Bytes,
        Schema::Fixed(_) | Schema::Duration => Kind::Fixed, Schema::Enum(_) => Kind::Enum, Schema::Record(_) => Kind::Record,
        Schema::Array(_) => Kind::Array, Schema::Map(_) => Kind::Map, Schema::Union(_) => Kind::Union,
        Schema::Decimal(_) | Schema::Ref { .. } => unreachable!("resolved physical schema required"),
    }
}
fn promotes(writer: Kind, reader: Kind) -> bool { matches!((writer, reader), (Kind::Int, Kind::Long | Kind::Float | Kind::Double) | (Kind::Long, Kind::Float | Kind::Double) | (Kind::Float, Kind::Double) | (Kind::Bytes, Kind::String) | (Kind::String, Kind::Bytes)) }
fn nesting(depth: usize) -> Result<()> { if depth > 128 { Err(error("avro-resolution", "Avro resolution nesting limit exceeded")) } else { Ok(()) } }
fn invalid() -> rtdb_types::Anomaly { error("avro-resolution", "Datum does not match its writer schema") }
pub(super) fn overflow() -> rtdb_types::Anomaly { error("allocation-overflow", "Avro resolution allocation exceeds addressable memory") }
