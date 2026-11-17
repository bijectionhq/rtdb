use super::*;
use super::arrow::{Extension,Unit};

impl Type {
    pub(super) fn representation(&self,scope: &EvaluationScope) -> Result<Object> {
        let value = match self {
            Self::Null => keyword("null",scope)?,Self::Nothing => keyword("nothing",scope)?,
            Self::Mono(mono) => mono.representation(scope)?,
            Self::Maybe(mono) => {
                let base = mono.representation(scope)?;
                match base {
                    Object::Keyword(_) => vector([keyword("?",scope)?,base],scope)?,
                    Object::Vector(mut values) => { slots(values.len().checked_add(1).ok_or_else(overflow)?,scope)?;values.insert(0,keyword("?",scope)?);Object::Vector(values) }
                    _ => return Err(invalid()),
                }
            }
            Self::Poly { legs,.. } => {
                let mut set = Map::set();
                for leg in &legs.ordered {
                    let value = match leg { Leg::Null => keyword("null",scope)?,Leg::Mono(mono) => mono.representation(scope)? };
                    assoc(&mut set,value,Object::Null,scope)?;
                }Object::Set(set)
            }
        };
        match self.encoding() { Encoding::Plain => Ok(value),Encoding::RunEnd(width) => vector([keyword(width.keyword(),scope)?,value],scope) }
    }
    pub(in crate::transit_object) fn write_string(&self,text: &mut Text<'_>) -> Result<()> { self.write_mode(text,false) }
    pub(in crate::transit_object) fn write_printed(&self,text: &mut Text<'_>) -> Result<()> {
        text.push("#xt/type ")?;let representation = self.representation(text.scope())?;super::super::render::write_printed(text,&representation)
    }
    fn write_mode(&self,text: &mut Text<'_>,plain: bool) -> Result<()> {
        match self {
            Self::Null => text.push("Null"),Self::Nothing => text.push("Nothing"),Self::Mono(mono) => mono.write_string(text,plain),
            Self::Maybe(mono) => {
                let encoding = if plain { Encoding::Plain } else { mono.encoding() };encoding.open(text)?;
                text.push("[:? ")?;mono.write_string(text,true)?;text.push("]")?;encoding.close(text)
            }
            Self::Poly { legs,encoding } => {
                text.push("Poly(children=[")?;
                for (index,leg) in legs.ordered.iter().enumerate() {
                    if index != 0 { text.push(", ")?; }
                    match leg { Leg::Null => text.push("Null")?,Leg::Mono(mono) => mono.write_string(text,plain)? }
                }
                text.push("], encoding=")?;
                match if plain { Encoding::Plain } else { *encoding } {
                    Encoding::Plain => text.push("Plain")?,Encoding::RunEnd(width) => { text.push("RunEnd(width=")?;text.push(width.name())?;text.push(")")?; }
                }text.push(")")
            }
        }
    }
}
impl Mono {
    fn representation(&self,scope: &EvaluationScope) -> Result<Object> {
        match self {
            Self::Scalar { arrow,.. } => arrow.representation(scope),
            Self::Listy { arrow,element,.. } => {
                if *arrow.as_ref() == ArrowKind::Extension(Extension::TsTzRange) && instant(element) { return keyword("tstz-range",scope); }
                let child = element.representation(scope)?;
                let arrow = arrow.representation(scope)?;
                match arrow {
                    Object::Keyword(_) => vector([arrow,child],scope),
                    Object::Vector(mut values) => { slots(values.len().checked_add(1).ok_or_else(overflow)?,scope)?;values.push(child);Ok(Object::Vector(values)) }
                    _ => Err(invalid()),
                }
            }
            Self::Struct { fields,.. } => {
                let mut values = Map::new();
                for name in fields.ordered.iter() {
                    let child = fields.values[name].representation(scope)?;scope.reserve(name.len())?;
                    assoc(&mut values,Object::String(name.to_string()),child,scope)?;
                }
                vector([keyword("struct",scope)?,Object::Map(values)],scope)
            }
        }
    }
    fn write_string(&self,text: &mut Text<'_>,plain: bool) -> Result<()> {
        let encoding = if plain { Encoding::Plain } else { self.encoding() };encoding.open(text)?;
        match self {
            Self::Scalar { arrow,.. } => { text.push("<")?;arrow.write_string(text)?;text.push(">")?; }
            Self::Listy { arrow,element,.. } => { text.push("[<")?;arrow.write_string(text)?;text.push("> ")?;element.write_mode(text,plain)?;text.push("]")?; }
            Self::Struct { fields,.. } => {
                text.push("{")?;
                for (index,name) in fields.ordered.iter().enumerate() {
                    if index != 0 { text.push(", ")?; }text.push(name)?;text.push(": ")?;fields.values[name].write_mode(text,plain)?;
                }text.push("}")?;
            }
        }encoding.close(text)
    }
}
impl Encoding {
    fn open(self,text: &mut Text<'_>) -> Result<()> { if let Self::RunEnd(width) = self { text.push("[:ree ")?;text.push(width.name())?;text.push(" ")?; }Ok(()) }
    fn close(self,text: &mut Text<'_>) -> Result<()> { if matches!(self,Self::RunEnd(_)) { text.push("]")?; }Ok(()) }
}
fn instant(value: &Type) -> bool {
    matches!(value,Type::Mono(mono) if matches!(mono.as_ref(),Mono::Scalar { arrow,.. } if matches!(arrow.as_ref(),ArrowKind::Timestamp { unit: Unit::Micro,timezone: Some(zone) } if zone.as_ref() == "UTC")))
}
