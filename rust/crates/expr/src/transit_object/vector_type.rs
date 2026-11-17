use std::{collections::BTreeMap,sync::Arc};
use rtdb_types::{Anomaly,Result};
use crate::{EvaluationScope,numeric::overflow};
use super::{Map,Object,allocation::Text,hash,invalid};
mod arrow;
mod read;
mod render;
use arrow::ArrowKind;

#[derive(Debug)]
pub(crate) enum Type {
    Null,Nothing,Mono(Arc<Mono>),Maybe(Arc<Mono>),Poly { legs: Arc<Legs>,encoding: Encoding },
}
#[derive(Debug)]
pub(crate) enum Mono {
    Scalar { arrow: Arc<ArrowKind>,encoding: Encoding },
    Listy { arrow: Arc<ArrowKind>,element: Arc<Type>,encoding: Encoding },
    Struct { fields: Arc<Fields>,encoding: Encoding },
}
#[derive(Clone,Debug)]
enum Leg { Null,Mono(Arc<Mono>) }
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub(crate) enum Encoding { Plain,RunEnd(RunEndWidth) }
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub(crate) enum RunEndWidth { I16,I32,I64 }
#[derive(Debug)]
pub(crate) struct Fields { ordered: Arc<[Arc<str>]>,values: BTreeMap<Arc<str>,Arc<Type>> }
#[derive(Debug)]
pub(crate) struct Legs { ordered: Vec<Leg>,buckets: BTreeMap<i32,Vec<usize>> }

fn retained<T>(value: T,scope: &EvaluationScope) -> Result<Arc<T>> {
    scope.reserve(std::mem::size_of::<T>().checked_add(32).ok_or_else(overflow)?)?;Ok(Arc::new(value))
}
fn text_arc(value: &str,scope: &EvaluationScope) -> Result<Arc<str>> { scope.reserve(value.len().checked_add(32).ok_or_else(overflow)?)?;Ok(Arc::from(value)) }
fn slots(count: usize,scope: &EvaluationScope) -> Result<()> {
    let slot = std::mem::size_of::<(Object,Object)>().checked_mul(4).and_then(|bytes|bytes.checked_add(256)).ok_or_else(overflow)?;
    scope.reserve(count.checked_mul(slot).and_then(|bytes|bytes.checked_add(1024)).ok_or_else(overflow)?)
}
fn assoc(map: &mut Map,key: Object,value: Object,scope: &EvaluationScope) -> Result<()> { slots(map.len().checked_add(1).ok_or_else(overflow)?,scope)?;map.assoc(key,value) }
fn keyword(value: &str,scope: &EvaluationScope) -> Result<Object> { scope.reserve(value.len())?;Ok(Object::Keyword(value.into())) }
fn integer(value: i32,scope: &EvaluationScope) -> Result<Object> { Ok(Object::Number(retained(super::Number::Long(i64::from(value)),scope)?)) }
fn vector<const N: usize>(values: [Object;N],scope: &EvaluationScope) -> Result<Object> { slots(N,scope)?;Ok(Object::Vector(Vec::from(values))) }
fn combine(left: i32,right: i32) -> i32 { left.wrapping_mul(31).wrapping_add(right) }
fn enum_hash(class: &str,name: &str) -> i32 { class.encode_utf16().chain([u16::from(b'.')]).chain(name.encode_utf16()).fold(0i32,|hash,ch|combine(hash,i32::from(ch))) }

impl Type {
    pub(super) fn read(spec: &Object,scope: &EvaluationScope) -> Result<Arc<Self>> { read::read(spec,scope) }
    fn encoding(&self) -> Encoding { match self { Self::Null | Self::Nothing => Encoding::Plain,Self::Mono(mono) | Self::Maybe(mono) => mono.encoding(),Self::Poly { encoding,.. } => *encoding } }
    fn with_encoding(value: &Arc<Self>,encoding: Encoding,scope: &EvaluationScope) -> Result<Arc<Self>> {
        retained(match value.as_ref() {
            Self::Null | Self::Nothing => {
                if let Encoding::RunEnd(width) = encoding {
                    let mut message = Text::new(scope);value.write_string(&mut message)?;message.push(" can't be laid out as RunEnd(width=")?;message.push(width.name())?;message.push(")")?;
                    return Err(Anomaly::unsupported("",message.finish()));
                }
                return Ok(Arc::clone(value));
            }
            Self::Mono(mono) => Self::Mono(mono.with_encoding(encoding,scope)?),Self::Maybe(mono) => Self::Maybe(mono.with_encoding(encoding,scope)?),
            Self::Poly { legs,.. } => Self::Poly { legs: Arc::clone(legs),encoding },
        },scope)
    }
    fn unencoded(value: &Arc<Self>,scope: &EvaluationScope) -> Result<Arc<Self>> {
        let result = match value.as_ref() {
            Self::Null | Self::Nothing => return Ok(Arc::clone(value)),
            Self::Mono(mono) | Self::Maybe(mono) => {
                let plain = Mono::unencoded(mono,scope)?;
                if Arc::ptr_eq(mono,&plain) { return Ok(Arc::clone(value)); }
                if matches!(value.as_ref(),Self::Mono(_)) { Self::Mono(plain) } else { Self::Maybe(plain) }
            }
            Self::Poly { legs,encoding } => {
                if *encoding == Encoding::Plain { return Ok(Arc::clone(value)); }
                Self::Poly { legs: Arc::clone(legs),encoding: Encoding::Plain }
            }
        };retained(result,scope)
    }
    fn maybe(value: &Arc<Self>,scope: &EvaluationScope) -> Result<Arc<Self>> {
        retained(match value.as_ref() {
            Self::Null | Self::Maybe(_) => return Ok(Arc::clone(value)),Self::Nothing => Self::Null,Self::Mono(mono) => Self::Maybe(Arc::clone(mono)),
            Self::Poly { legs,encoding } => {
                let mut nullable = Legs::new();for leg in &legs.ordered { nullable.insert(leg.clone(),scope)?; }nullable.insert(Leg::Null,scope)?;
                Self::Poly { legs: retained(nullable,scope)?,encoding: *encoding }
            }
        },scope)
    }
    pub(super) fn equivalent(&self,other: &Self) -> bool {
        if std::ptr::eq(self,other) { return true; }
        match (self,other) {
            (Self::Null,Self::Null) | (Self::Nothing,Self::Nothing) => true,
            (Self::Mono(left),Self::Mono(right)) | (Self::Maybe(left),Self::Maybe(right)) => left.equivalent(right),
            (Self::Poly { legs: left,encoding: le },Self::Poly { legs: right,encoding: re }) => le == re && left.equivalent(right),_ => false,
        }
    }
    pub(super) fn java_hash(&self) -> i32 {
        match self {
            Self::Null => hash::string("xtdb.arrow.VectorType.Null"),Self::Nothing => hash::string("xtdb.arrow.VectorType.Nothing"),
            Self::Mono(mono) | Self::Maybe(mono) => mono.java_hash(),Self::Poly { legs,encoding } => combine(legs.java_hash(),encoding.java_hash()),
        }
    }
    pub(super) fn wire(&self,scope: &EvaluationScope) -> Result<super::T> { Ok(super::tagged("xtdb/type",self.representation(scope)?.wire(scope)?)) }
}
impl Mono {
    fn encoding(&self) -> Encoding { match self { Self::Scalar { encoding,.. } | Self::Listy { encoding,.. } | Self::Struct { encoding,.. } => *encoding } }
    fn with_encoding(&self,encoding: Encoding,scope: &EvaluationScope) -> Result<Arc<Self>> {
        retained(match self {
            Self::Scalar { arrow,.. } => Self::Scalar { arrow: Arc::clone(arrow),encoding },
            Self::Listy { arrow,element,.. } => Self::Listy { arrow: Arc::clone(arrow),element: Arc::clone(element),encoding },
            Self::Struct { fields,.. } => Self::Struct { fields: Arc::clone(fields),encoding },
        },scope)
    }
    fn unencoded(value: &Arc<Self>,scope: &EvaluationScope) -> Result<Arc<Self>> {
        match value.as_ref() {
            Self::Scalar { encoding,.. } if *encoding == Encoding::Plain => Ok(Arc::clone(value)),
            Self::Scalar { .. } => value.with_encoding(Encoding::Plain,scope),
            Self::Listy { arrow,element,encoding } => {
                let plain = Type::unencoded(element,scope)?;
                if *encoding == Encoding::Plain && Arc::ptr_eq(element,&plain) { return Ok(Arc::clone(value)); }
                retained(Self::Listy { arrow: Arc::clone(arrow),element: plain,encoding: Encoding::Plain },scope)
            }
            Self::Struct { fields,encoding } => {
                let mut values = BTreeMap::new();let mut changed = *encoding != Encoding::Plain;
                for name in fields.ordered.iter() {
                    let child = &fields.values[name];let plain = Type::unencoded(child,scope)?;changed |= !Arc::ptr_eq(child,&plain);
                    slots(1,scope)?;values.insert(Arc::clone(name),plain);
                }
                if !changed { return Ok(Arc::clone(value)); }
                retained(Self::Struct { fields: retained(Fields { ordered: Arc::clone(&fields.ordered),values },scope)?,encoding: Encoding::Plain },scope)
            }
        }
    }
    fn equivalent(&self,other: &Self) -> bool {
        if std::ptr::eq(self,other) { return true; }
        match (self,other) {
            (Self::Scalar { arrow: a,encoding: ae },Self::Scalar { arrow: b,encoding: be }) => a == b && ae == be,
            (Self::Listy { arrow: a,element: ac,encoding: ae },Self::Listy { arrow: b,element: bc,encoding: be }) => a == b && ac.equivalent(bc) && ae == be,
            (Self::Struct { fields: a,encoding: ae },Self::Struct { fields: b,encoding: be }) => ae == be && a.values.len() == b.values.len() && a.values.iter().all(|(name,child)|b.values.get(name).is_some_and(|right|child.equivalent(right))),
            _ => false,
        }
    }
    fn java_hash(&self) -> i32 {
        match self {
            Self::Scalar { arrow,encoding } => combine(arrow.java_hash(),encoding.java_hash()),
            Self::Listy { arrow,element,encoding } => combine(combine(arrow.java_hash(),element.java_hash()),encoding.java_hash()),
            Self::Struct { fields,encoding } => combine(fields.values.iter().fold(0i32,|hash,(name,child)|hash.wrapping_add(hash::string(name) ^ child.java_hash())),encoding.java_hash()),
        }
    }
}
impl Encoding {
    fn java_hash(self) -> i32 { match self { Self::Plain => hash::string("xtdb.arrow.Encoding.Plain"),Self::RunEnd(width) => enum_hash("xtdb.arrow.RunEndWidth",width.name()) } }
}
impl RunEndWidth {
    fn name(self) -> &'static str { match self { Self::I16 => "I16",Self::I32 => "I32",Self::I64 => "I64" } }
    fn keyword(self) -> &'static str { match self { Self::I16 => "ree-i16",Self::I32 => "ree",Self::I64 => "ree-i64" } }
}
impl Leg {
    fn java_hash(&self) -> i32 { match self { Self::Null => Type::Null.java_hash(),Self::Mono(value) => value.java_hash() } }
    fn equivalent(&self,other: &Self) -> bool { match (self,other) { (Self::Null,Self::Null) => true,(Self::Mono(left),Self::Mono(right)) => left.equivalent(right),_ => false } }
}
impl Legs {
    fn new() -> Self { Self { ordered: Vec::new(),buckets: BTreeMap::new() } }
    fn insert(&mut self,leg: Leg,scope: &EvaluationScope) -> Result<()> {
        let hash = leg.java_hash();
        if self.buckets.get(&hash).is_some_and(|indices|indices.iter().any(|index|leg.equivalent(&self.ordered[*index]))) { return Ok(()); }
        slots(self.ordered.len().checked_add(1).ok_or_else(overflow)?,scope)?;
        self.buckets.entry(hash).or_default().push(self.ordered.len());self.ordered.push(leg);Ok(())
    }
    fn equivalent(&self,other: &Self) -> bool { self.ordered.len() == other.ordered.len() && self.ordered.iter().all(|leg|other.buckets.get(&leg.java_hash()).is_some_and(|indices|indices.iter().any(|index|leg.equivalent(&other.ordered[*index])))) }
    fn java_hash(&self) -> i32 { self.ordered.iter().fold(0i32,|hash,leg|hash.wrapping_add(leg.java_hash())) }
}
