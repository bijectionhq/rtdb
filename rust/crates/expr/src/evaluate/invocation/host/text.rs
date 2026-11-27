use rtdb_plan::{RegexFormKind as FormKind, RegexFormView as FormView, RegexLiteralKind as LiteralKind};
use rtdb_types::{Result,Value,java_text::JavaText};
use crate::EvalContext;
use crate::jvm_number::Number;
use super::{Function,HostValue,arity,error,limit,literal_number,reserve_text,retain_units,str_value,text_bytes,unsupported};

const MAX_UNITS: usize = 16 * 1024 * 1024;

/// UTF-16 output whose growth is admitted against the evaluation scope before it allocates.
pub(super) struct Units<'c,'context> { units: Vec<u16>,context: &'c EvalContext<'context> }

impl<'c,'context> Units<'c,'context> {
    pub(super) fn new(context: &'c EvalContext<'context>) -> Self { Self { units: Vec::new(),context } }
    fn push(&mut self,unit: u16) -> Result<()> {
        if self.units.len() == self.units.capacity() {
            if self.units.len() >= MAX_UNITS { return Err(limit()); }
            let capacity = self.units.capacity().saturating_mul(2).clamp(64,MAX_UNITS);
            self.context.reserve((capacity - self.units.len()) * 2)?;
            self.units.try_reserve_exact(capacity - self.units.len()).map_err(|_|super::allocation_error())?;
        }
        self.units.push(unit);Ok(())
    }
    fn extend(&mut self,units: impl IntoIterator<Item=u16>) -> Result<()> { for unit in units { self.push(unit)?; }Ok(()) }
    fn push_str(&mut self,text: &str) -> Result<()> { self.extend(text.encode_utf16()) }
    pub(super) fn finish<'a>(self) -> Result<HostValue<'a>> {
        reserve_text(text_bytes(JavaText::from_utf16(&self.units))?,self.context)?;
        Ok(HostValue::Text(retain_units(&self.units,self.context)?))
    }
}

impl Function {
    pub(super) fn apply_text<'a>(self,args: Vec<HostValue<'a>>,context: &EvalContext<'_>) -> Result<HostValue<'a>> {
        let mut output = Units::new(context);
        match self {
            Self::PatternQuote => { arity(args.len(),1,1)?;pattern_quote(&args[0].string()?.units().collect::<Vec<_>>(),&mut output)?; }
            Self::Format => format(&args,&mut output,context)?,
            Self::Join => {
                arity(args.len(),1,2)?;
                let separator = if args.len() == 2 { Some(str_value(&args[0],context)?) } else { None };
                for (index,value) in spread(&args[args.len() - 1],context)?.iter().enumerate() {
                    if index > 0 && let Some(separator) = &separator { output.extend(separator.view().units())?; }
                    output.extend(str_value(value,context)?.view().units())?;
                }
            }
            Self::UpperCase | Self::LowerCase => { arity(args.len(),1,1)?;change_case(args[0].string()?,matches!(self,Self::UpperCase),&mut output)?; }
            Self::Replace => { arity(args.len(),3,3)?;replace(&args,&mut output)?; }
            Self::Trim => {
                arity(args.len(),1,1)?;
                let units = args[0].string()?.units().collect::<Vec<_>>();
                let end = units.iter().rposition(|unit|!java_whitespace(*unit)).map_or(0,|index|index + 1);
                let start = units[..end].iter().position(|unit|!java_whitespace(*unit)).unwrap_or(end);
                output.extend(units[start..end].iter().copied())?;
            }
            Self::Apply => {
                arity(args.len(),2,usize::MAX)?;
                let mut args = args;
                let last = args.pop().ok_or_else(||error("Missing host argument"))?;
                let function = match args.remove(0) {
                    HostValue::Function(function) => function,
                    HostValue::Constant(_) | HostValue::Vector(_) => return Err(unsupported("Host apply requires a host function")),
                    _ => return Err(error("Host value is not callable")),
                };
                args.extend(spread(&last,context)?);
                return function.apply(args,context);
            }
            _ => unreachable!(),
        }
        output.finish()
    }
}

/// `java.util.regex.Pattern/quote`, which closes and reopens the quote around each embedded `\E`.
fn pattern_quote(source: &[u16],output: &mut Units<'_,'_>) -> Result<()> {
    let end_quote = [u16::from(b'\\'),u16::from(b'E')];
    output.push_str("\\Q")?;
    let mut current = 0;
    while let Some(offset) = source[current..].windows(2).position(|pair|pair == end_quote) {
        output.extend(source[current..current + offset].iter().copied())?;
        current += offset + 2;
        output.push_str("\\E\\\\E\\Q")?;
    }
    output.extend(source[current..].iter().copied())?;
    output.push_str("\\E")
}

enum Specifier { Literal(u16),Percent,String,Decimal }

/// `clojure.core/format` delegates to `String.format`; only conversions whose output is locale-independent are admitted.
fn format(args: &[HostValue<'_>],output: &mut Units<'_,'_>,context: &EvalContext<'_>) -> Result<()> {
    arity(args.len(),1,usize::MAX)?;
    let source = args[0].string()?.units().collect::<Vec<_>>();
    let mut specifiers = Vec::new();
    let mut units = source.iter().copied();
    while let Some(unit) = units.next() {
        if unit != u16::from(b'%') { specifiers.push(Specifier::Literal(unit));continue; }
        specifiers.push(match units.next() {
            Some(0x25) => Specifier::Percent,Some(0x73) => Specifier::String,Some(0x64) => Specifier::Decimal,
            None => return Err(error("Host format specifier is incomplete")),
            Some(_) => return Err(unsupported("Host format supports only %s, %d and %% without an index, flags, width or precision")),
        });
    }
    let mut arguments = args[1..].iter();
    for specifier in specifiers {
        match specifier {
            Specifier::Literal(unit) => output.push(unit)?,
            Specifier::Percent => output.push(u16::from(b'%'))?,
            Specifier::String | Specifier::Decimal => {
                let argument = arguments.next().ok_or_else(||error("Host format is missing an argument"))?;
                if argument.nil() { output.push_str("null")?;continue; }
                if matches!(specifier,Specifier::String) { output.extend(str_value(argument,context)?.view().units())?;continue; }
                let integer = match argument {
                    HostValue::Number(value) => match value.as_ref() {
                        Number::Byte(value) => i64::from(*value),Number::Short(value) => i64::from(*value),Number::Int(value) => i64::from(*value),Number::Long(value) => *value,
                        _ => return Err(error("Host format %d requires an integral argument")),
                    },
                    HostValue::Constant(form) => match super::literal_leaf(*form).as_deref() { Some(Value::I64(value)) => *value,_ => return Err(error("Host format %d requires an integral argument")) },
                    _ => return Err(error("Host format %d requires an integral argument")),
                };
                output.push_str(&integer.to_string())?;
            }
        }
    }
    Ok(())
}

/// The elements `seq` yields for `apply` and `clojure.string/join`.
fn spread<'a>(value: &HostValue<'a>,context: &EvalContext<'_>) -> Result<Vec<HostValue<'a>>> {
    let forms = match value {
        HostValue::Vector(values) => return Ok(values.to_vec()),
        HostValue::Nil => return Ok(Vec::new()),
        HostValue::Text(_) => return Err(unsupported("Host sequences of String characters are not implemented")),
        HostValue::Constant(form) => match form.kind() {
            FormKind::List(forms) | FormKind::Vector(forms) => forms,
            FormKind::Map(entries) if entries.is_empty() => return Ok(Vec::new()),
            FormKind::Set(values) if values.is_empty() => return Ok(Vec::new()),
            FormKind::Map(_) | FormKind::Set(_) => return Err(unsupported("Host sequences of maps and sets are not implemented")),
            FormKind::Literal(literal) => match literal.kind() {
                LiteralKind::Leaf(leaf) if matches!(leaf.value().as_ref(),Value::Null) => return Ok(Vec::new()),
                LiteralKind::Text(_) => return Err(unsupported("Host sequences of String characters are not implemented")),
                _ => return Err(error("Host value is not a sequence")),
            },
            _ => return Err(error("Host value is not a sequence")),
        },
        _ => return Err(error("Host value is not a sequence")),
    };
    context.reserve(forms.len().checked_mul(std::mem::size_of::<HostValue<'_>>()).ok_or_else(limit)?)?;
    forms.iter().map(|form| Ok(match form.kind() {
        FormKind::BigInteger(_) | FormKind::Decimal(_) | FormKind::Ratio(_) => HostValue::Number(std::sync::Arc::new(literal_number(form,context)?)),
        _ => HostValue::Constant(form),
    })).collect()
}

/// `String.toUpperCase()` or `toLowerCase()` under the process default locale; an unpaired surrogate maps to itself and,
/// having combining class 0 and no case, bounds every conditional mapping as a run boundary does.
fn change_case(source: JavaText<'_>,upper: bool,output: &mut Units<'_,'_>) -> Result<()> {
    let language = crate::string::case_language()?;
    let mut run = String::new();
    let flush = |run: &mut String,output: &mut Units<'_,'_>| -> Result<()> {
        let changed = if upper { language.to_upper(run) } else { language.to_lower(run) };
        run.clear();output.push_str(&changed)
    };
    for point in source.code_points() {
        match char::from_u32(point) {
            Some(character) => run.push(character),
            None => { flush(&mut run,output)?;output.push(u16::try_from(point).map_err(|_|limit())?)?; }
        }
    }
    flush(&mut run,output)
}

/// `clojure.string/replace` with a String or Character match, which delegate to `String.replace`.
fn replace(args: &[HostValue<'_>],output: &mut Units<'_,'_>) -> Result<()> {
    let source = args[0].string()?.units().collect::<Vec<_>>();
    if let (Some(target),Some(replacement)) = (character(&args[1]),character(&args[2])) {
        return output.extend(source.iter().map(|unit|if *unit == target { replacement } else { *unit }));
    }
    if character(&args[1]).is_some() { return Err(error("Host replacement is not a Character")); }
    let Ok(target) = args[1].string() else { return Err(error("Invalid match arg")); };
    let target = target.units().collect::<Vec<_>>();
    let replacement = args[2].string().map_err(|_|error("Host replacement is not a String"))?.units().collect::<Vec<_>>();
    if target.is_empty() {
        for unit in &source { output.extend(replacement.iter().copied())?;output.push(*unit)?; }
        return output.extend(replacement.iter().copied());
    }
    let mut current = 0;
    while let Some(offset) = source[current..].windows(target.len()).position(|window|window == target.as_slice()) {
        output.extend(source[current..current + offset].iter().copied())?;
        output.extend(replacement.iter().copied())?;
        current += offset + target.len();
    }
    output.extend(source[current..].iter().copied())
}

fn character(value: &HostValue<'_>) -> Option<u16> {
    match value { HostValue::Constant(form) => match form.kind() { FormKind::Character(value) => u16::try_from(u32::from(value)).ok(),_ => None },_ => None }
}

/// `Character.isWhitespace` for one UTF-16 unit: separators other than the non-breaking ones, plus the ASCII controls Java names.
fn java_whitespace(unit: u16) -> bool {
    matches!(unit,0x09..=0x0d | 0x1c..=0x20 | 0x1680 | 0x2000..=0x2006 | 0x2008..=0x200a | 0x2028 | 0x2029 | 0x205f | 0x3000)
}

/// `pr-str` of a host value, which `str` uses for collections.
pub(super) fn printed<'a>(value: &HostValue<'a>,context: &EvalContext<'_>) -> Result<HostValue<'a>> {
    let mut output = Units::new(context);write_printed(value,&mut output,context,0)?;output.finish()
}

fn write_printed(value: &HostValue<'_>,output: &mut Units<'_,'_>,context: &EvalContext<'_>,depth: usize) -> Result<()> {
    if depth > 128 { return Err(limit()); }
    match value {
        HostValue::Nil => output.push_str("nil"),
        HostValue::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
        HostValue::Text(value) => quoted(value.view().units(),output),
        HostValue::Number(value) => { reserve_text(value.text_capacity().map_err(|_|limit())?,context)?;output.push_str(&value.printed()) }
        HostValue::Vector(values) => sequence(values.iter().map(|value|Ok::<_,rtdb_types::Anomaly>(value.clone())),"[","]",output,context,depth),
        HostValue::Constant(form) => write_form(*form,output,context,depth),
        HostValue::Function(_) => Err(unsupported("Host object string rendering is not implemented for this value")),
    }
}

fn sequence<'a>(values: impl Iterator<Item=Result<HostValue<'a>>>,open: &str,close: &str,output: &mut Units<'_,'_>,context: &EvalContext<'_>,depth: usize) -> Result<()> {
    output.push_str(open)?;
    for (index,value) in values.enumerate() {
        if index > 0 { output.push(u16::from(b' '))?; }
        write_printed(&value?,output,context,depth + 1)?;
    }
    output.push_str(close)
}

fn write_form(form: FormView<'_>,output: &mut Units<'_,'_>,context: &EvalContext<'_>,depth: usize) -> Result<()> {
    match form.kind() {
        FormKind::Symbol(name) => output.push_str(name),
        FormKind::Character(value) => {
            output.push(u16::from(b'\\'))?;
            match value { '\n' => output.push_str("newline"),'\t' => output.push_str("tab"),' ' => output.push_str("space"),'\u{8}' => output.push_str("backspace"),'\u{c}' => output.push_str("formfeed"),'\r' => output.push_str("return"),value => output.push_str(value.encode_utf8(&mut [0;4])) }
        }
        FormKind::BigInteger(_) | FormKind::Decimal(_) | FormKind::Ratio(_) => write_printed(&HostValue::Number(std::sync::Arc::new(literal_number(form,context)?)),output,context,depth),
        FormKind::List(forms) => sequence(forms.iter().map(|form|Ok(HostValue::Constant(form))),"(",")",output,context,depth),
        FormKind::Vector(forms) => sequence(forms.iter().map(|form|Ok(HostValue::Constant(form))),"[","]",output,context,depth),
        FormKind::Map(entries) if entries.is_empty() => output.push_str("{}"),
        FormKind::Set(values) if values.is_empty() => output.push_str("#{}"),
        FormKind::Map(_) | FormKind::Set(_) => Err(unsupported("Host map and set printing is not implemented")),
        FormKind::Tagged { .. } | FormKind::AnonymousFunction(_) => Err(unsupported("Host object string rendering is not implemented for this value")),
        FormKind::Literal(literal) => match literal.kind() {
            LiteralKind::Text(value) => quoted(value.encode_utf16(),output),
            LiteralKind::Keyword(value) => { output.push(u16::from(b':'))?;output.push_str(value) }
            LiteralKind::Leaf(leaf) => match leaf.value().as_ref() {
                Value::Null => output.push_str("nil"),
                Value::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
                Value::I64(value) => output.push_str(&value.to_string()),
                Value::F64(value) => write_printed(&HostValue::Number(std::sync::Arc::new(Number::Double(*value))),output,context,depth),
                _ => Err(unsupported("Host object string rendering is not implemented for this value")),
            },
            _ => Err(unsupported("Host object string rendering is not implemented for this value")),
        },
    }
}

fn quoted(units: impl Iterator<Item=u16>,output: &mut Units<'_,'_>) -> Result<()> {
    output.push(u16::from(b'"'))?;
    for unit in units {
        match unit {
            0x0a => output.push_str("\\n")?,0x09 => output.push_str("\\t")?,0x0d => output.push_str("\\r")?,
            0x08 => output.push_str("\\b")?,0x0c => output.push_str("\\f")?,0x22 => output.push_str("\\\"")?,0x5c => output.push_str("\\\\")?,
            unit => output.push(unit)?,
        }
    }
    output.push(u16::from(b'"'))
}
