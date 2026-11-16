use std::collections::{BTreeMap,BTreeSet};

type Result<T> = std::result::Result<T, String>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Data { Text(String), Array(Vec<String>) }

#[derive(Debug)]
pub struct Bundle {
    pub name: String,
    pub entries: Vec<(String, Data)>,
    pub maps: BTreeMap<String, Vec<(String, Data)>>,
    pub available: Option<String>,
    pub nonlikely_script: Option<bool>,
}

enum Constant {
    Empty, Text(String), Integer(i32), Class(u16), String(u16),
    Member { class: u16, signature: u16 }, Signature { name: u16, descriptor: u16 },
}

struct Reader<'a> { bytes: &'a [u8], position: usize }
impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self { Self { bytes, position: 0 } }
    fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        let end = self.position.checked_add(count).ok_or("Locale class length overflow")?;
        let value = self.bytes.get(self.position..end).ok_or("Truncated locale class")?;
        self.position = end;
        Ok(value)
    }
    fn byte(&mut self) -> Result<u8> { Ok(self.take(1)?[0]) }
    fn short(&mut self) -> Result<u16> { Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap())) }
    fn integer(&mut self) -> Result<u32> { Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap())) }
    fn attributes<'p>(&mut self, pool: &'p Pool) -> Result<Vec<(&'p str, &'a [u8])>> {
        let count = self.short()?;
        if count > 64 { return Err("Too many locale class attributes".into()); }
        (0..count).map(|_| {
            let name = pool.text(self.short()?)?;
            let size = self.integer()? as usize;
            Ok((name, self.take(size)?))
        }).collect()
    }
}

struct Pool(Vec<Constant>);
impl Pool {
    fn read(input: &mut Reader<'_>) -> Result<Self> {
        let count = input.short()? as usize;
        if count == 0 { return Err("Empty locale constant pool".into()); }
        let mut constants = Vec::with_capacity(count);
        constants.push(Constant::Empty);
        while constants.len() < count {
            let tag = input.byte()?;
            let value = match tag {
                1 => { let size = input.short()? as usize; Constant::Text(modified_utf8(input.take(size)?)?) }
                3 => Constant::Integer(input.integer()? as i32),
                7 => Constant::Class(input.short()?),
                8 => Constant::String(input.short()?),
                9..=11 => Constant::Member { class: input.short()?, signature: input.short()? },
                12 => Constant::Signature { name: input.short()?, descriptor: input.short()? },
                4 => { input.take(4)?; Constant::Empty }
                5 | 6 => {
                    input.take(8)?;
                    constants.push(Constant::Empty);
                    if constants.len() == count { return Err("Wide locale constant lacks its second slot".into()); }
                    Constant::Empty
                }
                15 => { input.take(3)?; Constant::Empty }
                16 | 19 | 20 => { input.take(2)?; Constant::Empty }
                17 | 18 => { input.take(4)?; Constant::Empty }
                _ => return Err(format!("Unknown locale constant tag {tag}")),
            };
            constants.push(value);
        }
        Ok(Self(constants))
    }
    fn get(&self, index: u16) -> Result<&Constant> { self.0.get(usize::from(index)).ok_or_else(|| "Locale constant index is out of range".into()) }
    fn text(&self, index: u16) -> Result<&str> { match self.get(index)? { Constant::Text(value) => Ok(value), _ => Err("Locale constant is not UTF8".into()) } }
    fn class(&self, index: u16) -> Result<&str> { match self.get(index)? { Constant::Class(name) => self.text(*name), _ => Err("Locale constant is not a class name".into()) } }
    fn member(&self, index: u16) -> Result<(&str, &str, &str)> {
        let Constant::Member { class, signature } = self.get(index)? else { return Err("Locale constant is not a member".into()); };
        let Constant::Signature { name, descriptor } = self.get(*signature)? else { return Err("Locale member lacks its signature".into()); };
        Ok((self.class(*class)?, self.text(*name)?, self.text(*descriptor)?))
    }
}

fn modified_utf8(bytes: &[u8]) -> Result<String> {
    let mut units = Vec::with_capacity(bytes.len());
    let mut input = Reader::new(bytes);
    while input.position < bytes.len() {
        let first = input.byte()?;
        let value = match first {
            1..=127 => u16::from(first),
            0xc0..=0xdf => {
                let second = input.byte()?;
                if second & 0xc0 != 0x80 { return Err("Malformed locale modified UTF8 continuation".into()); }
                let value = (u16::from(first & 31) << 6) | u16::from(second & 63);
                if value < 128 && value != 0 { return Err("Overlong locale modified UTF8".into()); }
                value
            }
            0xe0..=0xef => {
                let second = input.byte()?;
                let third = input.byte()?;
                if second & 0xc0 != 0x80 || third & 0xc0 != 0x80 { return Err("Malformed locale modified UTF8 continuation".into()); }
                let value = (u16::from(first & 15) << 12) | (u16::from(second & 63) << 6) | u16::from(third & 63);
                if value < 2048 { return Err("Overlong locale modified UTF8".into()); }
                value
            }
            _ => return Err("Unsupported locale modified UTF8 encoding".into()),
        };
        units.push(value);
    }
    String::from_utf16(&units).map_err(|_| "Locale data contains an unpaired UTF16 surrogate".into())
}

#[derive(Clone)]
enum Literal<'a> { Empty, Integer(i32), Text(&'a str), Array(usize), Map(usize) }
struct Initializer<'a> {
    pool: &'a Pool,
    owner: &'a str,
    stack: Vec<Literal<'a>>,
    locals: Vec<Literal<'a>>,
    arrays: Vec<Vec<Literal<'a>>>,
    maps: Vec<Vec<(String, Data)>>,
    fields: BTreeMap<String, usize>,
    slots: usize,
    stack_limit: usize,
    remaining_bytes: usize,
}
impl<'a> Initializer<'a> {
    fn read(pool: &'a Pool, owner: &'a str, bytes: &[u8], metadata: bool) -> Result<Self> {
        let mut attribute = Reader::new(bytes);
        let stack_limit = usize::from(attribute.short()?);
        let local_count = usize::from(attribute.short()?);
        let code_length = attribute.integer()? as usize;
        if stack_limit > 256 || local_count > 4096 || code_length > 65_535 { return Err("Locale initializer exceeds its resource bounds".into()); }
        let code = attribute.take(code_length)?;
        if attribute.short()? != 0 { return Err("Locale initializer has exception handlers".into()); }
        attribute.attributes(pool)?;
        if attribute.position != bytes.len() { return Err("Trailing locale Code attribute data".into()); }
        let mut parser = Self { pool, owner, stack: Vec::with_capacity(stack_limit), locals: vec![Literal::Empty; local_count], arrays: Vec::new(), maps: Vec::new(), fields: BTreeMap::new(), slots: 0, stack_limit, remaining_bytes: 33_554_432 };
        let mut input = Reader::new(code);
        let mut returned = false;
        while input.position < code.len() {
            let instruction = input.byte()?;
            match instruction {
                0x01 => parser.push(Literal::Empty)?,
                0x02..=0x08 => parser.push(Literal::Integer(i32::from(instruction) - 3))?,
                0x10 => parser.push(Literal::Integer(i32::from(input.byte()? as i8)))?,
                0x11 => parser.push(Literal::Integer(i32::from(input.short()? as i16)))?,
                0x12 => { let index = u16::from(input.byte()?); parser.constant(index)?; }
                0x13 => { let index = input.short()?; parser.constant(index)?; }
                0x19 => { let index = usize::from(input.byte()?); parser.load(index)?; }
                0x2a..=0x2d => parser.load(usize::from(instruction - 0x2a))?,
                0x3a => { let index = usize::from(input.byte()?); parser.store(index)?; }
                0x4b..=0x4e => parser.store(usize::from(instruction - 0x4b))?,
                0x53 => parser.array_element()?,
                0x57 => { parser.pop()?; }
                0x59 => parser.push(parser.stack.last().cloned().ok_or("Locale literal stack underflow")?)?,
                0xbd => {
                    let name = pool.class(input.short()?)?;
                    if !matches!(name, "java/lang/String" | "java/lang/Object" | "[Ljava/lang/Object;") { return Err("Locale initializer constructs a nonliteral array".into()); }
                    let length = parser.integer()?;
                    let length = usize::try_from(length).map_err(|_| "Negative locale array length")?;
                    parser.slots = parser.slots.checked_add(length).filter(|slots| *slots <= 131_072).ok_or("Locale literal arrays exceed their resource bound")?;
                    let index = parser.arrays.len();
                    parser.arrays.push(vec![Literal::Empty; length]);
                    parser.push(Literal::Array(index))?;
                }
                0xb0 if !metadata => { if input.position != code.len() || parser.stack.len() != 1 { return Err("Locale resource return is not terminal".into()); } returned = true; }
                0xb1 if metadata => { if input.position != code.len() || !parser.stack.is_empty() { return Err("Locale metadata return is not terminal".into()); } returned = true; }
                0xb2 if metadata => { let member = input.short()?; parser.field(member, false)?; }
                0xb3 if metadata => { let member = input.short()?; parser.field(member, true)?; }
                0xb8 if metadata => { let member = input.short()?; parser.declaration(member)?; }
                0xb9 if metadata => {
                    let member = input.short()?;
                    if input.byte()? != 3 || input.byte()? != 0 { return Err("Malformed locale map declaration".into()); }
                    parser.map_entry(member)?;
                }
                0xc4 => {
                    let operation = input.byte()?;
                    let index = usize::from(input.short()?);
                    match operation { 0x19 => parser.load(index)?, 0x3a => parser.store(index)?, _ => return Err("Unsupported wide locale initializer instruction".into()) }
                }
                _ => return Err(format!("Nonliteral instruction 0x{instruction:02x} in locale resource")),
            }
        }
        if !returned { return Err("Locale initializer lacks a terminal return".into()); }
        Ok(parser)
    }
    fn push(&mut self, value: Literal<'a>) -> Result<()> {
        if self.stack.len() >= self.stack_limit { return Err("Locale literal stack exceeds its declared bound".into()); }
        self.stack.push(value); Ok(())
    }
    fn pop(&mut self) -> Result<Literal<'a>> { self.stack.pop().ok_or_else(|| "Locale literal stack underflow".into()) }
    fn integer(&mut self) -> Result<i32> { match self.pop()? { Literal::Integer(value) => Ok(value), _ => Err("Locale initializer requires an integer literal".into()) } }
    fn constant(&mut self, index: u16) -> Result<()> {
        let value = match self.pool.get(index)? {
            Constant::String(index) => Literal::Text(self.pool.text(*index)?),
            Constant::Integer(value) => Literal::Integer(*value),
            _ => return Err("Locale initializer references a nonliteral constant".into()),
        };
        self.push(value)
    }
    fn load(&mut self, index: usize) -> Result<()> { self.push(self.locals.get(index).cloned().ok_or("Locale local index is out of range")?) }
    fn store(&mut self, index: usize) -> Result<()> { let value = self.pop()?; *self.locals.get_mut(index).ok_or("Locale local index is out of range")? = value; Ok(()) }
    fn array_element(&mut self) -> Result<()> {
        let value = self.pop()?;
        let index = usize::try_from(self.integer()?).map_err(|_| "Negative locale array index")?;
        let Literal::Array(array) = self.pop()? else { return Err("Locale array element lacks its array".into()); };
        let target = self.arrays.get_mut(array).and_then(|array| array.get_mut(index)).ok_or("Locale array index is out of range")?;
        if !matches!(target, Literal::Empty) { return Err("Locale array element is assigned more than once".into()); }
        *target = value;
        Ok(())
    }
    fn data(&self, literal: &Literal<'_>, remaining: &mut usize) -> Result<Data> {
        Ok(match literal {
            Literal::Text(value) => Data::Text(retained_text(value, remaining)?),
            Literal::Array(index) => Data::Array(self.arrays.get(*index).ok_or("Locale literal array is absent")?.iter().map(|value| match value { Literal::Text(value) => retained_text(value, remaining), _ => Err("Locale data array contains a non-string".into()) }).collect::<Result<_>>()?),
            _ => return Err("Locale resource value is not textual data".into()),
        })
    }
    fn entries(&self) -> Result<Vec<(String, Data)>> {
        let Some(Literal::Array(index)) = self.stack.first() else { return Err("Locale getContents result is not an array".into()); };
        let mut remaining = self.remaining_bytes;
        self.arrays[*index].iter().map(|entry| {
            let Literal::Array(index) = entry else { return Err("Locale resource entry is not an array".into()); };
            let [Literal::Text(key), value] = self.arrays[*index].as_slice() else { return Err("Locale resource entry does not contain a key/value pair".into()); };
            Ok((retained_text(key, &mut remaining)?, self.data(value, &mut remaining)?))
        }).collect()
    }
    fn field(&mut self, index: u16, write: bool) -> Result<()> {
        let (owner, name, descriptor) = self.pool.member(index)?;
        if !write && (owner, name, descriptor) == ("java/util/Locale", "ROOT", "Ljava/util/Locale;") { return self.push(Literal::Text("")); }
        if owner != self.owner || descriptor != "Ljava/util/Map;" { return Err("Locale metadata references a non-data field".into()); }
        if write {
            let name = name.to_owned();
            let Literal::Map(map) = self.pop()? else { return Err("Locale map field lacks its literal map".into()); };
            if self.fields.insert(name, map).is_some() { return Err("Duplicate locale map declaration".into()); }
            Ok(())
        } else { self.push(Literal::Map(*self.fields.get(name).ok_or("Locale map referenced before declaration")?)) }
    }
    fn declaration(&mut self, index: u16) -> Result<()> {
        match self.pool.member(index)? {
            ("java/util/HashMap", "newHashMap", "(I)Ljava/util/HashMap;") => {
                if !(0..=65_536).contains(&self.integer()?) { return Err("Locale map capacity exceeds its resource bound".into()); }
                let map = self.maps.len(); self.maps.push(Vec::new()); self.push(Literal::Map(map))
            }
            ("java/util/Locale", "forLanguageTag", "(Ljava/lang/String;)Ljava/util/Locale;") => {
                if !matches!(self.stack.last(), Some(Literal::Text(_))) { return Err("Locale declaration lacks a language tag literal".into()); }
                Ok(())
            }
            _ => Err("Locale metadata contains a non-data invocation".into()),
        }
    }
    fn map_entry(&mut self, index: u16) -> Result<()> {
        if self.pool.member(index)? != ("java/util/Map", "put", "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;") { return Err("Locale metadata contains a nonliteral map operation".into()); }
        let value = self.pop()?;
        let mut remaining = self.remaining_bytes;
        let value = self.data(&value, &mut remaining)?;
        let Literal::Text(key) = self.pop()? else { return Err("Locale map key is not textual".into()); };
        let key = retained_text(key, &mut remaining)?;
        self.remaining_bytes = remaining;
        let Literal::Map(map) = self.pop()? else { return Err("Locale map entry lacks its map".into()); };
        if self.maps[map].len() >= 65_536 { return Err("Locale map exceeds its resource bound".into()); }
        self.maps[map].push((key, value));
        self.push(Literal::Empty)
    }
    fn field_maps(mut self) -> Result<BTreeMap<String,Vec<(String,Data)>>> {
        let mut result = BTreeMap::new();
        let mut used = BTreeSet::new();
        for (field,index) in self.fields {
            if !used.insert(index) { return Err("Locale metadata aliases one literal map through multiple fields".into()); }
            let map = self.maps.get_mut(index).ok_or("Locale metadata map index is absent")?;
            result.insert(field,std::mem::take(map));
        }
        Ok(result)
    }
}

fn retained_text(value: &str, remaining: &mut usize) -> Result<String> {
    *remaining = remaining.checked_sub(value.len().checked_add(64).ok_or("Locale text allocation overflow")?).ok_or("Expanded locale literals exceed 32 MiB")?;
    Ok(value.to_owned())
}

pub fn decode(bytes: &[u8]) -> Result<Bundle> {
    if bytes.len() > 1_048_576 { return Err("Locale resource class exceeds one MiB".into()); }
    let mut input = Reader::new(bytes);
    if input.integer()? != 0xcafebabe { return Err("Locale resource is not a class file".into()); }
    let minor = input.short()?;
    let major = input.short()?;
    if minor != 0 || major != 69 { return Err("Locale resource is not a Java25 class format".into()); }
    let pool = Pool::read(&mut input)?;
    input.short()?;
    let name = pool.class(input.short()?)?.to_owned();
    input.short()?;
    let interfaces = usize::from(input.short()?);
    input.take(interfaces * 2)?;
    let mut nonlikely_script = None;
    let fields = input.short()?;
    for _ in 0..fields {
        input.short()?;
        let field = pool.text(input.short()?)?;
        let descriptor = pool.text(input.short()?)?;
        for (attribute, bytes) in input.attributes(&pool)? {
            if field == "nonlikelyScript" && descriptor == "Z" && attribute == "ConstantValue" {
                let mut value = Reader::new(bytes);
                let Constant::Integer(integer) = pool.get(value.short()?)? else { return Err("Locale nonlikelyScript flag is not an integer".into()); };
                if bytes.len() != 2 || !matches!(*integer, 0 | 1) { return Err("Invalid locale nonlikelyScript flag".into()); }
                nonlikely_script = Some(*integer != 0);
            }
        }
    }
    let mut bundle = Bundle { name: name.clone(), entries: Vec::new(), maps: BTreeMap::new(), available: None, nonlikely_script };
    let methods = input.short()?;
    for _ in 0..methods {
        input.short()?;
        let method = pool.text(input.short()?)?;
        let descriptor = pool.text(input.short()?)?;
        for (attribute, bytes) in input.attributes(&pool)? {
            if attribute != "Code" { continue; }
            match (method, descriptor) {
                ("getContents", "()[[Ljava/lang/Object;") => bundle.entries = Initializer::read(&pool, &name, bytes, false)?.entries()?,
                ("<clinit>", "()V") => {
                    bundle.maps = Initializer::read(&pool, &name, bytes, true)?.field_maps()?;
                }
                ("availableLanguageTags", "(Ljava/lang/String;)Ljava/lang/String;") => {
                    let initializer = Initializer::read(&pool, &name, bytes, false)?;
                    let Some(Literal::Text(value)) = initializer.stack.first() else { return Err("Locale language tags are not literal data".into()); };
                    bundle.available = Some((*value).to_owned());
                }
                _ => {}
            }
        }
    }
    input.attributes(&pool)?;
    if input.position != bytes.len() { return Err("Trailing locale class data".into()); }
    Ok(bundle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn modified_utf8_retains_null_and_supplementary_locale_text() {
        assert_eq!(modified_utf8(&[0xc0, 0x80, 0xed, 0xa0, 0xbd, 0xed, 0xb8, 0x80]).unwrap(), "\0😀");
        for bytes in [&[0][..], &[0xc1, 0x81], &[0xed, 0xa0, 0x80], &[0xf0, 0x9f, 0x98, 0x80]] { assert!(modified_utf8(bytes).is_err()); }
    }
    #[test]
    fn resource_initializer_rejects_general_calls_and_back_edges() {
        let pool = Pool(vec![Constant::Empty]);
        for instruction in [0xb6, 0xb8, 0xa7, 0x99, 0xbb] {
            let mut bytes = vec![0, 1, 0, 1, 0, 0, 0, 2, instruction, 0xb0, 0, 0, 0, 0];
            assert!(Initializer::read(&pool, "Fixture", &bytes, false).is_err());
            bytes[8] = 0xb1;
            assert!(Initializer::read(&pool, "Fixture", &bytes, false).is_err());
        }
    }
    #[test]
    fn metadata_field_aliases_cannot_expand_a_literal_map_by_cloning() {
        let pool = Pool(vec![Constant::Empty,Constant::Text("Fixture".into()),Constant::Class(1),Constant::Text("a".into()),Constant::Text("Ljava/util/Map;".into()),
            Constant::Signature { name: 3,descriptor: 4 },Constant::Member { class: 2,signature: 5 },Constant::Text("b".into()),
            Constant::Signature { name: 7,descriptor: 4 },Constant::Member { class: 2,signature: 8 },Constant::Text("java/util/HashMap".into()),Constant::Class(10),
            Constant::Text("newHashMap".into()),Constant::Text("(I)Ljava/util/HashMap;".into()),Constant::Signature { name: 12,descriptor: 13 },Constant::Member { class: 11,signature: 14 }]);
        let code = [0x03,0xb8,0,15,0xb3,0,6,0xb2,0,6,0xb3,0,9,0xb1];
        let mut bytes = vec![0,1,0,0,0,0,0,code.len() as u8];bytes.extend(code);bytes.extend([0,0,0,0]);
        let initializer = Initializer::read(&pool,"Fixture",&bytes,true).unwrap();
        assert_eq!(initializer.maps.len(),1);assert_eq!(initializer.fields.len(),2);
        assert!(initializer.field_maps().unwrap_err().contains("aliases one literal map"));
    }
}
