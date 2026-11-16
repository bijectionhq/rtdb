use rtdb_types::{Anomaly,LogicalType as L,Result,ScalarType as S,Value};
use serde::{Deserialize,Serialize};

/// Owned raw REGEXP_REPLACE metadata, preserved independently of scalar expression binding.
/// Lists retain host-form syntax; characters and numeric spellings do not pass through vector admission.
#[derive(Clone,Debug,PartialEq,Serialize,Deserialize)]
pub enum RegexForm {
    Literal(Value),Character(char),Symbol(String),BigInteger(String),Decimal(String),
    List(Vec<Self>),Vector(Vec<Self>),Set(Vec<Self>),Map(Vec<(Self,Self)>),
    Tagged { tag: String,value: Box<Self> },AnonymousFunction(Box<Self>),
}

impl Default for RegexForm { fn default() -> Self { Self::Literal(Value::Null) } }

impl RegexForm {
    fn truthy(&self) -> bool { !matches!(self,Self::Literal(Value::Null | Value::Bool(false))) }
    /// Bounds this owned metadata tree, including its retained raw forms.
    pub fn retained_bytes(&self) -> Result<usize> {
        let mut bytes = std::mem::size_of::<Self>();
        let mut add = |extra| { bytes = bytes.checked_add(extra).ok_or_else(size_error)?;Ok::<(),Anomaly>(()) };
        match self {
            Self::Literal(value) => add(value.retained_bytes()?)?,
            Self::Character(_) => (),
            Self::Symbol(value) | Self::BigInteger(value) | Self::Decimal(value) => add(value.capacity())?,
            Self::List(values) | Self::Vector(values) | Self::Set(values) => {
                add(values.capacity().checked_mul(std::mem::size_of::<Self>()).ok_or_else(size_error)?)?;
                for value in values { add(value.retained_bytes()?)?; }
            }
            Self::Map(entries) => {
                add(entries.capacity().checked_mul(std::mem::size_of::<(Self,Self)>()).ok_or_else(size_error)?)?;
                for (key,value) in entries { add(key.retained_bytes()?)?;add(value.retained_bytes()?)?; }
            }
            Self::Tagged { tag,value } => { add(tag.capacity())?;add(value.retained_bytes()?)?; }
            Self::AnonymousFunction(value) => add(value.retained_bytes()?)?,
        }
        Ok(bytes)
    }
}

/// Raw metadata for one source-expression replacement.
/// Missing fields are nil; start and occurrence use Clojure truthiness when an applicable source leg is admitted.
#[derive(Clone,Debug,Default,PartialEq,Serialize,Deserialize)]
pub struct RegexReplacement {
    pub pattern: RegexForm,
    pub replacement: RegexForm,
    pub start: RegexForm,
    pub occurrence: RegexForm,
    pub flags: RegexForm,
}

impl RegexReplacement {
    /// Admits every source type leg without compiling a pattern or consuming a row.
    /// Null and empty source types leave all metadata uninterpreted; non-null sources must be UTF8.
    pub fn output_type(&self,input: &L) -> Result<L> {
        let mut output = L::Nothing;
        for leg in input.legs() {
            output = output.join(&match leg {
                L::Null => L::Null,
                L::Scalar(S::Utf8) => L::Scalar(S::Utf8),
                _ => return Err(Anomaly::incorrect("xtdb.expression/function-type-mismatch","regexp_replace requires a UTF8 source")),
            });
        }
        if input.legs().contains(&L::Scalar(S::Utf8)) { self.admit_metadata()?; }
        Ok(output)
    }

    /// Retains a provisional output type without admitting the source or metadata.
    /// A non-null COALESCE input can remove this expression before emitted-type admission.
    pub fn declared_type(&self,input: &L) -> L {
        input.legs().into_iter().fold(L::Nothing,|output,leg|output.join(&if leg == L::Null { L::Null } else { L::Scalar(S::Utf8) }))
    }

    fn admit_metadata(&self) -> Result<()> {
        self.replacement_text()?;
        if self.start.truthy() || self.occurrence.truthy() {
            return Err(Anomaly::unsupported("xtdb.expression/unsupported","regexp_replace does not yet support `start` or `n` arguments"));
        }
        self.flag_text()?;
        Ok(())
    }

    /// Returns the literal replacement consumed during metadata admission.
    pub fn replacement_text(&self) -> Result<&str> {
        match &self.replacement {
            RegexForm::Literal(Value::Utf8(value)) => Ok(value),
            _ => Err(metadata_error("REGEXP_REPLACE replacement is not a string")),
        }
    }

    /// Reduces raw seqable flags to the supported Java flags; sequence elements are not evaluated.
    /// Nil is empty, map entries contribute no character flags, and non-seqable metadata fails admission.
    pub fn flag_text(&self) -> Result<String> {
        let mut flags = [false;3];
        let mut flag = |ch| match ch { 'i' => flags[0] = true,'s' => flags[1] = true,'m' => flags[2] = true,_ => () };
        match &self.flags {
            RegexForm::Literal(Value::Null) | RegexForm::Map(_) => (),
            RegexForm::Literal(Value::Utf8(value)) => { for ch in value.chars() { flag(ch); } }
            RegexForm::List(values) | RegexForm::Vector(values) | RegexForm::Set(values) => {
                for value in values { if let RegexForm::Character(ch) = value { flag(*ch); } }
            }
            _ => return Err(metadata_error("REGEXP_REPLACE flags are not seqable")),
        }
        Ok(['i','s','m'].into_iter().zip(flags).filter_map(|(ch,present)|present.then_some(ch)).collect())
    }

    /// Bounds the retained recipe independently of its source expression and compiled invocation.
    pub fn retained_bytes(&self) -> Result<usize> {
        [&self.pattern,&self.replacement,&self.start,&self.occurrence,&self.flags].into_iter()
            .try_fold(std::mem::size_of::<Self>(),|bytes,value|bytes.checked_add(value.retained_bytes()?).ok_or_else(size_error))
    }
}

fn metadata_error(message: &str) -> Anomaly { Anomaly::fault("xtdb.error/unknown",message) }
fn size_error() -> Anomaly { Anomaly::incorrect("rtdb.plan/regex-metadata-size","Regex metadata exceeds addressable memory") }
