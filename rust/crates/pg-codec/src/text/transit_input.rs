use std::mem::size_of;
use std::sync::Arc;

use rtdb_columnar::MemoryBudget;
use rtdb_format::transit::{self, TransitReadBuilder, TransitValue as T};
use rtdb_types::{Anomaly, ListBuilder, Result, StructFields, Value};

use super::{invalid, normalize_keyword, transit as native};
use crate::{DecodedValue, Format, size_error};

mod ids;

pub(super) fn decode_json(bytes: &[u8]) -> Result<Value> {
    transit::decode_json_with(bytes, &mut NativeInput)?.into_value()
}

pub fn decode_transit_sequence<T>(bytes: &[u8], format: Format, mut convert: impl FnMut(Value) -> Result<T>) -> Result<Vec<T>> {
    let inputs = match format {
        Format::Text => transit::decode_json_sequence_with(bytes, &mut NativeInput)?,
        Format::Binary => transit::decode_msgpack_sequence_with(bytes, &mut NativeInput)?,
    };
    inputs.into_iter().map(|input| convert(input.into_value()?)).collect()
}

pub fn decode_transit_msgpack(bytes: &[u8], memory: &Arc<MemoryBudget>) -> Result<DecodedValue> {
    decode_msgpack(bytes, memory, Shape::Value)
}

pub fn decode_transit_transaction(bytes: &[u8], memory: &Arc<MemoryBudget>) -> Result<DecodedValue> {
    decode_msgpack(bytes, memory, Shape::Transaction)
}

fn decode_msgpack(bytes: &[u8], memory: &Arc<MemoryBudget>, shape: Shape) -> Result<DecodedValue> {
    let (input, wire_reservation) = transit::decode_msgpack_with_in(bytes, memory, &mut NativeInput)?;
    let bound = input.conversion_bound()?.checked_add(8192).ok_or_else(size_error)?;
    let mut reservation = memory.reserve(bound)?;
    let value = input.convert(shape)?;
    drop(wire_reservation);
    let retained = value.retained_bytes()?.checked_add(8192).ok_or_else(size_error)?;
    Arc::get_mut(&mut reservation).expect("Transit conversion owns its reservation").shrink_to(retained)?;
    Ok(DecodedValue { value, _reservation: reservation })
}

// Native conversion follows wire validation, including for values inside a tag that remains opaque.
#[derive(Clone)]
enum Input {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    TaggedF32(f64),
    String(String),
    Keyword(String),
    Symbol(String),
    Bytes(Vec<u8>),
    Array(Vec<Input>),
    Map(Vec<(Input, Input)>),
    Tagged(String, Box<Input>),
    Floats(Vec<f64>),
    TaggedFloats(Vec<f64>),
}

const _: () = assert!(size_of::<Input>() == size_of::<T>());

struct NativeInput;

struct InputArray {
    items: Vec<Input>,
    limit: usize,
}

impl InputArray {
    fn push(&mut self, item: Input) {
        if self.items.len() == self.items.capacity() {
            let next = (self.items.len() * 2).clamp(1, self.limit);
            self.items.reserve_exact(next - self.items.len());
        }
        self.items.push(item);
    }
}


impl TransitReadBuilder for NativeInput {
    type Value = Input;
    type Array = InputArray;
    type Map = Vec<(Input, Input)>;

    fn null(&mut self) -> Result<Input> { Ok(Input::Null) }
    fn boolean(&mut self, value: bool) -> Result<Input> { Ok(Input::Bool(value)) }
    fn integer(&mut self, value: i64) -> Result<Input> { Ok(Input::Integer(value)) }
    fn float(&mut self, value: f64) -> Result<Input> { Ok(Input::Float(value)) }
    fn string(&mut self, value: String) -> Result<Input> { Ok(Input::String(value)) }
    fn keyword(&mut self, value: String) -> Result<Input> { Ok(Input::Keyword(value)) }
    fn symbol(&mut self, value: String) -> Result<Input> { Ok(Input::Symbol(value)) }
    fn bytes(&mut self, value: Vec<u8>) -> Result<Input> { Ok(Input::Bytes(value)) }

    fn start_array(&mut self, _: Option<&str>, size: usize) -> Result<Self::Array> {
        Ok(InputArray { items: Vec::new(), limit: size })
    }

    fn array_item(&mut self, array: &mut Self::Array, value: Input) -> Result<()> {
        match value {
            Input::TaggedF32(value) => match array.items.last_mut() {
                Some(item @ Input::TaggedF32(_)) => {
                    let Input::TaggedF32(first) = *item else { unreachable!() };
                    *item = Input::TaggedFloats(vec![first, value]);
                }
                Some(Input::TaggedFloats(values)) => values.push(value),
                _ => array.push(Input::TaggedF32(value)),
            },
            value => array.push(value),
        }
        Ok(())
    }

    fn array_floats(&mut self, array: &mut Self::Array, values: Vec<f64>) -> Result<()> {
        array.push(Input::Floats(values));
        Ok(())
    }

    fn finish_array(&mut self, array: Self::Array) -> Result<Input> { Ok(Input::Array(array.items)) }
    fn start_map(&mut self, size: usize) -> Result<Self::Map> { Ok(Vec::with_capacity(size)) }
    fn map_entry(&mut self, map: &mut Self::Map, key: Input, value: Input) -> Result<()> {
        map.push((key, value));
        Ok(())
    }
    fn finish_map(&mut self, map: Self::Map) -> Result<Input> { Ok(Input::Map(map)) }

    fn tagged_float(&mut self, tag: String, value: f64) -> Result<Input> {
        if tag == "f32" { Ok(Input::TaggedF32(value)) }
        else { self.tagged(tag, Input::Float(value)) }
    }

    fn tagged(&mut self, tag: String, representation: Input) -> Result<Input> {
        match (tag.as_str(), representation) {
            ("'", value) => Ok(value),
            ("cmap", Input::Array(items)) => {
                let count = len(&items);
                if count % 2 != 0 { return Err(wire_error("Composite map has an unmatched key")); }
                let mut items = elements(items);
                let mut entries = Vec::with_capacity(count / 2);
                while let Some(key) = items.next() {
                    let value = items.next().ok_or_else(|| wire_error("Composite map has an unmatched key"))?;
                    entries.push((key, value));
                }
                Ok(Input::Map(entries))
            }
            ("cmap", _) => Err(wire_error("Composite map representation is not an array")),
            (_, Input::String(value)) => Ok(match transit::resolve_tag(tag, T::String(value))? {
                T::Null => Input::Null,
                T::Bool(value) => Input::Bool(value),
                T::Integer(value) => Input::Integer(value),
                T::Float(value) => Input::Float(value),
                T::Tagged { tag, representation } => {
                    let T::String(value) = *representation else { unreachable!() };
                    Input::Tagged(tag, Box::new(Input::String(value)))
                }
                _ => unreachable!("string tag resolution returns a scalar or its original representation"),
            }),
            ("f32", Input::Float(value)) => Ok(Input::TaggedF32(value)),
            (_, representation) => Ok(Input::Tagged(tag, Box::new(representation))),
        }
    }
}

fn wire_error(message: &str) -> Anomaly { Anomaly::incorrect("rtdb.format/invalid-transit", message) }

fn elements(items: Vec<Input>) -> impl Iterator<Item = Input> {
    items.into_iter().flat_map(|item| {
        let (single, floats, tagged) = match item {
            Input::Floats(values) => (None, values, false),
            Input::TaggedFloats(values) => (None, values, true),
            value => (Some(value), Vec::new(), false),
        };
        single.into_iter().chain(floats.into_iter().map(move |value| {
            if tagged { Input::TaggedF32(value) } else { Input::Float(value) }
        }))
    })
}

fn len(items: &[Input]) -> usize {
    items.iter().map(|item| match item {
        Input::Floats(values) | Input::TaggedFloats(values) => values.len(),
        _ => 1,
    }).sum()
}

#[derive(Clone, Copy)]
enum Shape { Value, Transaction, Operations, Operation, DeleteIds }

fn list(items: Vec<Input>, shape: Shape) -> Result<Value> {
    if matches!(shape, Shape::DeleteIds)
        && let Some(ids) = ids::pack(&items) {
        return Ok(Value::List(ids));
    }
    let element = if matches!(shape, Shape::Operations) { Shape::Operation } else { Shape::Value };
    let mut list = ListBuilder::with_capacity(len(&items));
    for item in items {
        match item {
            Input::Float(value) => list.push_f64(value),
            Input::Floats(values) => values.into_iter().for_each(|value| list.push_f64(value)),
            Input::TaggedF32(value) => list.push_f32(value as f32),
            Input::TaggedFloats(values) => values.into_iter().for_each(|value| list.push_f32(value as f32)),
            value => list.push(value.convert(element)?),
        }
    }
    Ok(Value::List(list.finish()))
}

impl Input {
    fn into_value(self) -> Result<Value> { self.convert(Shape::Value) }

    fn convert(self, shape: Shape) -> Result<Value> {
        Ok(match self {
            Self::Array(items) => return list(items, shape),
            Self::Map(entries) => {
                let deletes = matches!(shape, Shape::Operation) && entries.iter().rev()
                    .find(|(key, _)| match key {
                        Self::String(key) => key == "op",
                        Self::Keyword(key) => normalize_keyword(key) == "op",
                        _ => false,
                    })
                    .is_some_and(|(_, value)| matches!(value, Self::String(kind) if matches!(kind.as_str(), "delete-docs" | "erase-docs")));
                let mut fields = StructFields::new();
                for (key, value) in entries {
                    let key = match key {
                        Self::String(key) => key,
                        Self::Keyword(key) => normalize_keyword(&key),
                        _ => return Err(Anomaly::unsupported("xtdb.postgres/unsupported-map-key", "Source maps require string or keyword keys")),
                    };
                    let field_shape = match (shape, key.as_str()) {
                        (Shape::Transaction, "operations") => Shape::Operations,
                        (Shape::Operation, "ids") if deletes => Shape::DeleteIds,
                        _ => Shape::Value,
                    };
                    fields.insert(key, value.convert(field_shape)?);
                }
                Value::Struct(fields)
            }
            Self::Tagged(tag, representation) => match tag.as_str() {
                "list" => {
                    let Self::Array(items) = *representation else { return Err(invalid("Transit list requires an array")); };
                    return list(items, shape);
                }
                "set" => {
                    let Self::Array(items) = *representation else { return Err(invalid("Transit set requires an array")); };
                    let values = elements(items).map(Self::into_value).collect::<Result<Vec<_>>>()?;
                    Value::Set(super::unique(values))
                }
                "i8" | "i16" | "i32" | "i64" | "f32" | "f64" => super::cast_transit_number(&tag, representation.into_value()?)?,
                _ => return native(Self::Tagged(tag, representation).into_wire()),
            },
            Self::TaggedF32(value) => Value::F32(value as f32),
            scalar => return native(scalar.into_wire()),
        })
    }

    fn into_wire(self) -> T {
        match self {
            Self::Null => T::Null,
            Self::Bool(value) => T::Bool(value),
            Self::Integer(value) => T::Integer(value),
            Self::Float(value) => T::Float(value),
            Self::TaggedF32(value) => T::Tagged { tag: "f32".into(), representation: Box::new(T::Float(value)) },
            Self::String(value) => T::String(value),
            Self::Keyword(value) => T::Keyword(value),
            Self::Symbol(value) => T::Symbol(value),
            Self::Bytes(value) => T::Bytes(value),
            Self::Array(items) => T::Array(elements(items).map(Self::into_wire).collect()),
            Self::Map(entries) => T::Map(entries.into_iter().map(|(key, value)| (key.into_wire(), value.into_wire())).collect()),
            Self::Tagged(tag, representation) => T::Tagged { tag, representation: Box::new(representation.into_wire()) },
            Self::Floats(_) | Self::TaggedFloats(_) => unreachable!("Float runs belong to arrays"),
        }
    }

    fn conversion_bound(&self) -> Result<usize> {
        const INLINE: usize = size_of::<Value>() * 8 + 256;
        const MAP_ENTRY: usize = size_of::<(String, Value)>() * 16 + 256;
        const TAGGED_F32: usize = INLINE * 2 + 3 * 8;
        let add = |left: usize, right: usize| left.checked_add(right).ok_or_else(size_error);
        let text = |length: usize| length.checked_mul(8).ok_or_else(size_error);
        match self {
            Self::String(value) | Self::Keyword(value) | Self::Symbol(value) => add(INLINE, text(value.len())?),
            Self::Bytes(value) => add(INLINE, text(value.len())?),
            Self::Array(items) => items.iter().try_fold(INLINE, |bytes, item| add(bytes, item.conversion_bound()?)),
            Self::Map(entries) => entries.iter().try_fold(INLINE, |bytes, (key, value)| {
                add(add(add(bytes, MAP_ENTRY)?, key.conversion_bound()?)?, value.conversion_bound()?)
            }),
            Self::Tagged(tag, representation) => add(add(INLINE, text(tag.len())?)?, representation.conversion_bound()?),
            Self::TaggedF32(_) => Ok(TAGGED_F32),
            Self::Floats(values) => values.len().checked_mul(INLINE).ok_or_else(size_error),
            Self::TaggedFloats(values) => values.len().checked_mul(TAGGED_F32).ok_or_else(size_error),
            _ => Ok(INLINE),
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{Array, cast::AsArray, types::{Float32Type, Float64Type}};
    use arrow_schema::DataType;
    use rtdb_types::ListView;

    use super::*;

    fn assert_same(actual: Result<Value>, expected: Result<Value>) {
        match (actual, expected) {
            (Ok(actual), Ok(expected)) => {
                assert_eq!(actual.logical_type(), expected.logical_type());
                let actual = transit::encode_msgpack(&transit::value_to_transit(&actual).unwrap()).unwrap();
                let expected = transit::encode_msgpack(&transit::value_to_transit(&expected).unwrap()).unwrap();
                assert_eq!(actual, expected);
            }
            (Err(actual), Err(expected)) => assert_eq!(actual, expected),
            (actual, expected) => panic!("native {actual:?}, legacy {expected:?}"),
        }
    }

    #[test]
    fn transit_json_matches_wire_then_native_conversion() {
        for json in [
            r#"["^ ","some-key",["~:xt/hello","^1"],"^0",[null,-0.0,"~zNaN","~z-INF","~d1.5"],"other",[["~#f32",1.125],["^3",-0.0],["^3","~zNaN"]]]"#,
            r#"{"same":[["~#f32",1.125],["^1",-0.0],1.25,7,null],"^0":[["~#list",[-0.0,null]],["~#set",[1,7,1]],["~#cmap",["~:xt/a",3.0]]] }"#,
            r#"["~#xtdb/clj-form",["~$foo",[1.0,["~#f32",3.0],-0.0]]]"#,
            r#"["~#'",[9223372036854775807,"~i-9223372036854775808","~f9223372036854775808",9007199254740993,9007199254740993.0,null,[]]]"#,
            r#"["~#what",[["~#f32",2.0],["^0",3.0],"~$bar"]]"#,
            r#"["~#u",[-0.0,0.0]]"#,
            r#"["~#xtdb/tstz-range",[null,"~t2021-01-01T00:00:00Z"]]"#,
            r#"["^ ","cached-key",0.0,"^0",1.0, "~:xt/A", 2.0]"#,
            r#"[["^0",1.0]]"#,
            r#"["^ ",null,1.0,2.0]"#,
            r#"["~#cmap",[-0.0,2.0,3.0]]"#,
            r#"["^ ","valid",["~#f32",1.0,2.0]]"#,
            r#"[1.0,9223372036854775808]"#,
            r#"["~n9223372036854775808",1.0]"#,
            r#"[["^0",1.0]]]"#,
        ] {
            assert_same(decode_json(json.as_bytes()), transit::decode_json(json.as_bytes()).and_then(crate::from_transit));
        }
        let json = br#"{"vec":[-0.0,null,"~zNaN",["~#f32",-0.0],["^0","~zNaN"]]}"#;
        let Value::Struct(fields) = decode_json(json).unwrap() else { panic!("expected document"); };
        let Some(Value::List(list)) = fields.get("vec") else { panic!("expected list"); };
        let ListView::Union(union) = list.view() else { panic!("expected a packed float union"); };
        assert_eq!(union.child(union.type_id(0)).as_primitive::<Float64Type>().value(0).to_bits(), (-0.0f64).to_bits());
        assert_eq!(union.child(union.type_id(1)).data_type(), &DataType::Null);
        assert!(union.logical_nulls().unwrap().is_null(1));
        assert_eq!(union.child(union.type_id(2)).as_primitive::<Float64Type>().value(1).to_bits(), f64::NAN.to_bits());
        assert_eq!(union.child(union.type_id(3)).as_primitive::<Float32Type>().value(0).to_bits(), (-0.0f32).to_bits());
    }

    #[test]
    fn transit_msgpack_preserves_float_bits_and_cache_admission() {
        let tagged = |tag: &str, value| T::Tagged { tag: tag.into(), representation: Box::new(value) };
        let keyword = || T::Keyword("xt/cached-keyword".into());
        let key = || T::String("repeated-key".into());
        let double = f64::from_bits(0xfff8_1234_5678_9abc);
        let single = f32::from_bits(0xffc0_1234);
        let double_marker = 1234.56789_f64;
        let single_marker = -41.125_f32;
        let wire = T::Array(vec![
            T::Map(vec![(key(), keyword()), (key(), keyword())]),
            T::Array(vec![T::Float(double_marker), T::Null, T::Float(-0.0)]),
            T::Array(vec![tagged("f32", T::Float32(single_marker)), T::Null, tagged("f32", T::Float(-0.0))]),
            T::Array(vec![T::Float32(single_marker), T::Integer(i64::MAX), T::Float(double_marker), T::Null]),
            tagged("f", T::String("922337203685477580812345".into())),
            tagged("xtdb/clj-form", T::Array(vec![T::Symbol("test/opaque".into()), T::Float(double_marker)])),
        ]);
        let mut bytes = transit::encode_msgpack(&wire).unwrap();
        for offset in 0..bytes.len() {
            if bytes.get(offset) == Some(&0xcb) && bytes.get(offset + 1..offset + 9) == Some(double_marker.to_bits().to_be_bytes().as_slice()) {
                bytes[offset + 1..offset + 9].copy_from_slice(&double.to_bits().to_be_bytes());
            }
            if bytes.get(offset) == Some(&0xca) && bytes.get(offset + 1..offset + 5) == Some(single_marker.to_bits().to_be_bytes().as_slice()) {
                bytes[offset + 1..offset + 5].copy_from_slice(&single.to_bits().to_be_bytes());
            }
        }
        let (input, charged) = transit::decode_msgpack_with_in(&bytes, &MemoryBudget::new(1 << 20), &mut NativeInput).unwrap();
        let baseline_memory = MemoryBudget::new(1 << 20);
        let (baseline, baseline_charged) = transit::decode_msgpack_in(&bytes, &baseline_memory).unwrap();
        assert_eq!(charged.bytes(), baseline_charged.bytes());
        assert_same(input.into_value(), crate::from_transit(baseline.clone()));
        let native = decode_transit_msgpack(&bytes, &MemoryBudget::new(1 << 20)).unwrap();
        let items = native.value().list_view().unwrap();
        let vector = |row| items.get(row).unwrap().into_owned();
        let f64s = vector(1);
        let ListView::F64(f64s) = f64s.list_view().unwrap() else { panic!("expected packed Float64"); };
        assert_eq!(f64s.iter().map(|v| v.map(f64::to_bits)).collect::<Vec<_>>(), [Some(double.to_bits()), None, Some((-0.0f64).to_bits())]);
        let f32s = vector(2);
        let ListView::F32(f32s) = f32s.list_view().unwrap() else { panic!("expected packed Float32"); };
        assert_eq!(f32s.iter().map(|v| v.map(f32::to_bits)).collect::<Vec<_>>(), [Some(single.to_bits()), None, Some((-0.0f32).to_bits())]);
        let Value::List(mixed) = vector(3) else { panic!("expected mixed list"); };
        assert!(matches!(mixed.array().unwrap().data_type(), DataType::Union(..)));

        for invalid in [&[0x92, 0xa2, b'^', b'0', 0xc0][..], &[0x92, 0xcb, 0][..], &[0xc0, 0xc0][..]] {
            assert_same(decode_transit_msgpack(invalid, &MemoryBudget::new(1 << 20)).map(|v| v.into_parts().0),
                transit::decode_msgpack(invalid).and_then(crate::from_transit));
        }
    }

    #[test]
    fn transit_cache_rolls_over_and_resets_at_top_level_boundaries() {
        fn cache_code(index: usize) -> String {
            let hi = (b'0' + (index / 44) as u8) as char;
            let lo = (b'0' + (index % 44) as u8) as char;
            if index < 44 { format!("^{lo}") } else { format!("^{hi}{lo}") }
        }
        let mut cached = (0..44 * 44).map(|i| format!("~:cache/key-{i}")).collect::<Vec<_>>();
        cached.push(cache_code(44 * 44 - 1));
        cached.extend(["~:cache/replacement".into(), "^0".into()]);
        let json = serde_json::to_vec(&cached).unwrap();
        assert_same(decode_json(&json), transit::decode_json(&json).and_then(crate::from_transit));
        let bytes = br#"["~:cache/first",[-0.0]] ["^0"]"#;
        let actual = decode_transit_sequence(bytes, Format::Text, Ok::<_, Anomaly>).unwrap_err();
        assert_eq!(actual.message.as_deref(), Some("Transit cache code references an unwritten entry"));
        let bytes = br#"["~$unsupported-symbol"] ]"#;
        let actual = decode_transit_sequence(bytes, Format::Text, Ok::<_, Anomaly>).unwrap_err();
        assert_eq!(actual.code, "xtdb.error/json-parse");
        assert_eq!(actual.message.as_deref(), Some("Unexpected close marker ']'"));
    }
}
