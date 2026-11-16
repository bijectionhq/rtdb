use std::{borrow::Cow,collections::BTreeMap,sync::Arc};
use rtdb_plan::RegexForm as Form;
use rtdb_types::{Anomaly,Result,Value};
use crate::EvalContext;
use crate::jvm_number::{Cast,Number};

pub(super) struct Program<'a> { expression: Expression<'a>,locals: usize }
pub(super) enum PatternText<'a> { Borrowed(&'a str),Computed(Arc<str>) }
impl AsRef<str> for PatternText<'_> { fn as_ref(&self) -> &str { match self { Self::Borrowed(value) => value,Self::Computed(value) => value } } }
enum Expression<'a> {
    Constant(&'a Form),Number(Arc<Number>),Nil,Local { index: usize,primitive: Option<Cast> },Function(Function),
    Cast { kind: Cast,primitive: bool,value: Box<Expression<'a>> },
    If(Box<[Expression<'a>;3]>),Body(Vec<Expression<'a>>),
    Let { first: usize,bindings: Vec<Expression<'a>>,body: Box<Expression<'a>> },
    Logical { conjunction: bool,values: Vec<Expression<'a>> },
    Vector(Vec<Expression<'a>>),Call { function: Box<Expression<'a>>,arguments: Vec<Expression<'a>> },
}
#[derive(Clone,Copy)]
enum Function { Str,Identity,Name,Namespace,Subs,Not,Boolean,Cast(Cast) }
#[derive(Clone)]
enum HostValue<'a> { Constant(&'a Form),Number(Arc<Number>),Nil,Bool(bool),Text(Arc<str>),Function(Function),Vector(Arc<[HostValue<'a>]>) }
struct Compiler<'a,'c,'context> { symbols: BTreeMap<&'a str,(usize,Option<Cast>)>,active: usize,slots: usize,nodes: usize,context: &'c EvalContext<'context> }

impl<'a> Program<'a> {
    pub(super) fn compile(form: &'a Form,context: &EvalContext<'_>) -> Result<Self> {
        let mut compiler = Compiler { symbols: BTreeMap::new(),active: 0,slots: 0,nodes: 0,context };
        let expression = compiler.expression(form,0)?;
        Ok(Self { expression,locals: compiler.slots })
    }
    pub(super) fn pattern(&self,context: &EvalContext<'_>) -> Result<PatternText<'a>> {
        context.reserve(self.locals.checked_mul(std::mem::size_of::<HostValue<'_>>()).ok_or_else(limit)?)?;
        let mut locals = vec![HostValue::Nil;self.locals];
        match self.expression.evaluate(&mut locals,context)? {
            HostValue::Text(value) => Ok(PatternText::Computed(value)),
            HostValue::Constant(Form::Literal(Value::Utf8(value))) => Ok(PatternText::Borrowed(value)),
            _ => Err(error("REGEXP_REPLACE host pattern did not produce a String")),
        }
    }
}

impl<'a> Compiler<'a,'_,'_> {
    fn expression(&mut self,form: &'a Form,depth: usize) -> Result<Expression<'a>> {
        self.nodes += 1;
        if depth > 128 || self.nodes > 65_536 { return Err(limit()); }
        self.context.reserve(256)?;
        Ok(match form {
            Form::Symbol(name) => {
                if let Some((index,primitive)) = self.symbols.get(name.as_str()) { Expression::Local { index: *index,primitive: *primitive } }
                else if let Some(function) = function(name) { Expression::Function(function) }
                else { return Err(unsupported("Host symbol or namespace is not implemented")); }
            }
            Form::List(values) if !values.is_empty() => self.list(values,depth + 1)?,
            Form::BigInteger(_) | Form::Decimal(_) => Expression::Number(Arc::new(literal_number(form,self.context)?)),
            Form::Vector(values) => Expression::Vector(self.expressions(values,depth + 1)?),
            Form::Map(values) if values.is_empty() => Expression::Constant(form),
            Form::Set(values) if values.is_empty() => Expression::Constant(form),
            Form::Map(_) | Form::Set(_) | Form::Tagged { .. } | Form::AnonymousFunction(_) => return Err(unsupported("Host collection, tagged object or function syntax is not implemented")),
            _ => Expression::Constant(form),
        })
    }
    fn expressions(&mut self,forms: &'a [Form],depth: usize) -> Result<Vec<Expression<'a>>> {
        self.context.reserve(forms.len().checked_mul(std::mem::size_of::<Expression<'_>>()).ok_or_else(limit)?)?;
        forms.iter().map(|form|self.expression(form,depth)).collect()
    }
    fn body(&mut self,forms: &'a [Form],depth: usize) -> Result<Expression<'a>> { Ok(Expression::Body(self.expressions(forms,depth)?)) }
    fn list(&mut self,forms: &'a [Form],depth: usize) -> Result<Expression<'a>> {
        let operation = match &forms[0] { Form::Symbol(name) if matches!(name.as_str(),"quote" | "if" | "do" | "let*") || !self.symbols.contains_key(name.as_str()) => Some(name.as_str()),_ => None };
        let args = &forms[1..];
        if matches!(operation,Some("boolean" | "clojure.core/boolean")) { arity(args.len(),1,1)?; }
        if let Some(Function::Cast(kind)) = operation.and_then(function) {
            arity(args.len(),1,1)?;
            let value = self.expression(&args[0],depth)?;
            return Ok(Expression::Cast { kind,primitive: value.primitive().is_some(),value: Box::new(value) });
        }
        Ok(match operation {
            Some("quote") => { arity(args.len(),1,1)?;match &args[0] { Form::BigInteger(_) | Form::Decimal(_) => Expression::Number(Arc::new(literal_number(&args[0],self.context)?)),_ => Expression::Constant(&args[0]) } }
            Some("if") => {
                arity(args.len(),2,3)?;
                Expression::If(Box::new([self.expression(&args[0],depth)?,self.expression(&args[1],depth)?,match args.get(2) { Some(value) => self.expression(value,depth)?,None => Expression::Nil }]))
            }
            Some("do") => self.body(args,depth)?,
            Some("let" | "clojure.core/let" | "let*") => {
                let Some(Form::Vector(bindings)) = args.first() else { return Err(error("Host let requires a binding vector")); };
                if bindings.len() % 2 != 0 { return Err(error("Host let requires binding pairs")); }
                let first = self.active;
                self.context.reserve(bindings.len().checked_mul(std::mem::size_of::<Expression<'_>>() + 256).ok_or_else(limit)?)?;
                let mut expressions = Vec::with_capacity(bindings.len() / 2);
                let mut previous = Vec::with_capacity(bindings.len() / 2);
                for pair in bindings.chunks_exact(2) {
                    let Form::Symbol(name) = &pair[0] else { return Err(unsupported("Host binding destructuring is not implemented")); };
                    if name != "/" && name.contains('/') { return Err(error("Host let cannot bind a qualified symbol")); }
                    let expression = self.expression(&pair[1],depth)?;
                    let primitive = expression.primitive();
                    expressions.push(expression);
                    previous.push((name.as_str(),self.symbols.insert(name,(self.active,primitive))));
                    self.active += 1;
                    self.slots = self.slots.max(self.active);
                }
                let body = Box::new(self.body(&args[1..],depth)?);
                for (name,index) in previous.into_iter().rev() {
                    match index { Some(index) => { self.symbols.insert(name,index); },None => { self.symbols.remove(name); } }
                }
                self.active = first;
                Expression::Let { first,bindings: expressions,body }
            }
            Some("when" | "clojure.core/when" | "when-not" | "clojure.core/when-not") => {
                let Some(test) = args.first() else { return Err(error("Host when requires a test")); };
                let test = self.expression(test,depth)?;
                let body = self.body(&args[1..],depth)?;
                let branches = if operation.is_some_and(|name|name.ends_with("when-not")) { [test,Expression::Nil,body] } else { [test,body,Expression::Nil] };
                Expression::If(Box::new(branches))
            }
            Some("and" | "clojure.core/and" | "or" | "clojure.core/or") => Expression::Logical { conjunction: operation.is_some_and(|name|name.ends_with("and")),values: self.expressions(args,depth)? },
            _ => Expression::Call { function: Box::new(self.expression(&forms[0],depth)?),arguments: self.expressions(args,depth)? },
        })
    }
}

impl<'a> Expression<'a> {
    fn primitive(&self) -> Option<Cast> {
        match self {
            Self::Constant(Form::Literal(Value::I64(_))) => Some(Cast::Long),Self::Constant(Form::Literal(Value::F64(_))) => Some(Cast::Double),
            Self::Local { primitive,.. } => *primitive,Self::Cast { kind,.. } => Some(*kind),
            Self::If(branches) => branches[1].primitive().filter(|kind|branches[2].primitive() == Some(*kind)),
            Self::Body(values) => values.last().and_then(Self::primitive),Self::Let { body,.. } => body.primitive(),
            Self::Logical { values,.. } => values.first().and_then(Self::primitive).filter(|kind|values.iter().all(|value|value.primitive() == Some(*kind))),
            _ => None,
        }
    }
    fn evaluate(&self,locals: &mut [HostValue<'a>],context: &EvalContext<'_>) -> Result<HostValue<'a>> {
        Ok(match self {
            Self::Constant(form) => HostValue::Constant(form),Self::Number(value) => HostValue::Number(Arc::clone(value)),Self::Nil => HostValue::Nil,
            Self::Local { index,.. } => locals[*index].clone(),Self::Function(function) => HostValue::Function(*function),
            Self::Cast { kind,primitive,value } => numeric_cast(*kind,value.evaluate(locals,context)?,*primitive,context)?,
            Self::If(branches) => if branches[0].evaluate(locals,context)?.truthy() { branches[1].evaluate(locals,context)? } else { branches[2].evaluate(locals,context)? },
            Self::Body(expressions) => {
                let mut value = HostValue::Nil;
                for expression in expressions { value = expression.evaluate(locals,context)?; }value
            }
            Self::Let { first,bindings,body } => {
                for (offset,expression) in bindings.iter().enumerate() { locals[first + offset] = expression.evaluate(locals,context)?; }
                let result = body.evaluate(locals,context);
                locals[*first..*first + bindings.len()].fill(HostValue::Nil);
                result?
            }
            Self::Logical { conjunction,values } => {
                let mut result = if *conjunction { HostValue::Bool(true) } else { HostValue::Nil };
                for value in values { result = value.evaluate(locals,context)?;if result.truthy() != *conjunction { break; } }result
            }
            Self::Vector(expressions) => {
                let values = evaluate_arguments(expressions,locals,context)?;
                context.reserve(values.len().checked_mul(std::mem::size_of::<HostValue<'_>>()).ok_or_else(limit)?)?;
                HostValue::Vector(values.into())
            }
            Self::Call { function,arguments } => {
                let function = function.evaluate(locals,context)?;
                let arguments = evaluate_arguments(arguments,locals,context)?;
                let HostValue::Function(function) = function else { return Err(error("Host value is not callable")); };
                function.apply(arguments,context)?
            }
        })
    }
}
fn evaluate_arguments<'a>(expressions: &[Expression<'a>],locals: &mut [HostValue<'a>],context: &EvalContext<'_>) -> Result<Vec<HostValue<'a>>> {
    context.reserve(expressions.len().checked_mul(std::mem::size_of::<HostValue<'_>>()).ok_or_else(limit)?)?;
    expressions.iter().map(|expression|expression.evaluate(locals,context)).collect()
}
impl HostValue<'_> {
    fn truthy(&self) -> bool { !matches!(self,Self::Nil | Self::Bool(false) | Self::Constant(Form::Literal(Value::Null | Value::Bool(false)))) }
    fn string(&self) -> Result<&str> {
        match self { Self::Text(value) => Ok(value),Self::Constant(Form::Literal(Value::Utf8(value))) => Ok(value),_ => Err(error("Host value is not a String")) }
    }
    fn number(&self,context: &EvalContext<'_>) -> Result<Cow<'_,Number>> {
        match self { Self::Number(value) => Ok(Cow::Borrowed(value)),Self::Constant(form) => literal_number(form,context).map(Cow::Owned),_ => Err(error("Host value is not a Number")) }
    }
    fn index(&self,context: &EvalContext<'_>) -> Result<i32> {
        drop(self.number(context)?);
        let value = numeric_cast(Cast::Int,self.clone(),false,context)?;
        match value { Self::Number(value) => match value.as_ref() { Number::Int(value) => Ok(*value),_ => unreachable!() },_ => unreachable!() }
    }
}
impl Function {
    fn apply<'a>(self,args: Vec<HostValue<'a>>,context: &EvalContext<'_>) -> Result<HostValue<'a>> {
        Ok(match self {
            Self::Str => {
                context.reserve(args.len().checked_mul(std::mem::size_of::<Arc<str>>()).ok_or_else(limit)?)?;
                let mut strings = Vec::with_capacity(args.len());
                let mut length = 0usize;
                for value in &args { let value = str_value(value,context)?;length = length.checked_add(value.len()).ok_or_else(limit)?;strings.push(value); }
                reserve_text(length,context)?;
                let mut output = String::with_capacity(length);
                for value in strings { output.push_str(&value); }
                HostValue::Text(output.into())
            }
            Self::Identity => { arity(args.len(),1,1)?;args.into_iter().next().ok_or_else(||error("Missing host argument"))? }
            Self::Cast(kind) => { arity(args.len(),1,1)?;numeric_cast(kind,args.into_iter().next().ok_or_else(||error("Missing host argument"))?,false,context)? }
            Self::Boolean | Self::Not => { arity(args.len(),1,1)?;HostValue::Bool(args[0].truthy() == matches!(self,Self::Boolean)) }
            Self::Name | Self::Namespace => {
                arity(args.len(),1,1)?;
                let namespace = matches!(self,Self::Namespace);
                let name = match &args[0] {
                    HostValue::Constant(Form::Symbol(name)) => name.as_str(),
                    HostValue::Constant(Form::Literal(Value::Keyword(name))) => name.as_str(),
                    value if !namespace => return Ok(HostValue::Text(text(value.string()?,context)?)),
                    _ => return Err(error("Host namespace requires a symbol or keyword")),
                };
                let (prefix,name) = if name == "/" { (None,name) } else { name.split_once('/').map_or((None,name),|(prefix,name)|(Some(prefix),name)) };
                match if namespace { prefix } else { Some(name) } { Some(value) => HostValue::Text(text(value,context)?),None => HostValue::Nil }
            }
            Self::Subs => {
                arity(args.len(),2,3)?;
                let value = args[0].string()?;
                let start = args[1].index(context)?;
                let length = i32::try_from(value.encode_utf16().count()).map_err(|_|limit())?;
                let end = if args.len() == 3 { args[2].index(context)? } else { length };
                if start < 0 || end < start || end > length { return Err(error("Host substring indexes are out of range")); }
                context.reserve((length as usize).checked_mul(2).ok_or_else(limit)?)?;
                let mut units = Vec::with_capacity(length as usize);
                units.extend(value.encode_utf16());
                let capacity = ((end - start) as usize).checked_mul(3).ok_or_else(limit)?;
                reserve_text(capacity,context)?;
                let mut output = String::with_capacity(capacity);
                for character in char::decode_utf16(units[start as usize..end as usize].iter().copied()) {
                    output.push(character.map_err(|_|unsupported("Host substring requires the isolated-surrogate text carrier"))?);
                }
                HostValue::Text(output.into())
            }
        })
    }
}
fn function(name: &str) -> Option<Function> {
    let name = name.strip_prefix("clojure.core/").unwrap_or(name);
    Some(match name { "str" => Function::Str,"identity" => Function::Identity,"name" => Function::Name,"namespace" => Function::Namespace,"subs" => Function::Subs,"not" => Function::Not,"boolean" => Function::Boolean,
        "byte" => Function::Cast(Cast::Byte),"short" => Function::Cast(Cast::Short),"int" => Function::Cast(Cast::Int),"long" => Function::Cast(Cast::Long),"float" => Function::Cast(Cast::Float),"double" => Function::Cast(Cast::Double),_ => return None })
}
fn str_value(value: &HostValue<'_>,context: &EvalContext<'_>) -> Result<Arc<str>> {
    match value {
        HostValue::Text(value) => Ok(Arc::clone(value)),HostValue::Nil => text("",context),HostValue::Bool(value) => text(if *value { "true" } else { "false" },context),
        HostValue::Constant(Form::Literal(Value::Null)) => text("",context),
        HostValue::Constant(Form::Literal(Value::Utf8(value))) | HostValue::Constant(Form::Symbol(value)) => text(value,context),
        HostValue::Constant(Form::Literal(Value::Bool(value))) => text(if *value { "true" } else { "false" },context),
        HostValue::Constant(Form::Character(value)) => { context.reserve(32)?;text(&value.to_string(),context) }
        HostValue::Constant(Form::Literal(Value::I64(value))) => { context.reserve(32)?;text(&value.to_string(),context) }
        HostValue::Constant(Form::Literal(Value::F64(value))) => { context.reserve(64)?;text(&rtdb_types::java_f64_text(*value),context) }
        HostValue::Number(value) => { reserve_text(value.text_capacity().map_err(|_|limit())?,context)?;text(&value.text(),context) }
        HostValue::Constant(Form::Literal(Value::Keyword(value))) => { reserve_text(value.as_str().len().checked_add(1).ok_or_else(limit)?,context)?;text(&value.to_string(),context) }
        _ => Err(unsupported("Host object string rendering is not implemented for this value")),
    }
}
fn literal_number(form: &Form,context: &EvalContext<'_>) -> Result<Number> {
    Ok(match form {
        Form::Literal(Value::I64(value)) => Number::Long(*value),Form::Literal(Value::F64(value)) => Number::Double(*value),
        Form::BigInteger(value) | Form::Decimal(value) => {
            if value.len() > 16 * 1024 * 1024 { return Err(limit()); }
            context.reserve(value.len().checked_mul(32).and_then(|bytes|bytes.checked_add(256)).ok_or_else(limit)?)?;
            if matches!(form,Form::BigInteger(_)) { Number::integer(value) } else { Number::decimal(value) }.map_err(|_|error("Invalid host numeric literal"))?
        }
        _ => return Err(error("Host value is not a Number")),
    })
}
fn numeric_cast<'a>(kind: Cast,value: HostValue<'a>,primitive: bool,context: &EvalContext<'_>) -> Result<HostValue<'a>> {
    let source = match &value {
        HostValue::Constant(Form::Character(value)) if matches!(kind,Cast::Byte | Cast::Short | Cast::Int | Cast::Long) => {
            if u32::from(*value) > u32::from(u16::MAX) { return Err(unsupported("Host Character requires a single UTF16 code unit")); }
            Cow::Owned(Number::Long(i64::from(u32::from(*value))))
        }
        _ => value.number(context)?,
    };
    let capacity = source.text_capacity().map_err(|_|limit())?;
    if capacity > 16 * 1024 * 1024 { return Err(limit()); }
    context.reserve(capacity.checked_mul(8).and_then(|bytes|bytes.checked_add(256)).ok_or_else(limit)?)?;
    let result = if primitive { Number::cast_primitive(kind,&source) } else { Number::cast(kind,&source) };
    Ok(HostValue::Number(Arc::new(result.map_err(|_|error("Host numeric cast is out of range"))?)))
}
fn text(value: &str,context: &EvalContext<'_>) -> Result<Arc<str>> { reserve_text(value.len(),context)?;Ok(Arc::from(value)) }
fn reserve_text(length: usize,context: &EvalContext<'_>) -> Result<()> {
    if length > 16 * 1024 * 1024 { return Err(limit()); }
    context.reserve(length.checked_mul(2).and_then(|size|size.checked_add(64)).ok_or_else(limit)?)
}
fn arity(length: usize,min: usize,max: usize) -> Result<()> { if (min..=max).contains(&length) { Ok(()) } else { Err(error("Host form has the wrong number of arguments")) } }
fn error(message: &str) -> Anomaly { Anomaly::fault("xtdb.error/unknown",message) }
fn unsupported(message: &str) -> Anomaly { Anomaly::unsupported("rtdb.expr/regex-host-form",message) }
fn limit() -> Anomaly { Anomaly::incorrect("rtdb.expr/regex-host-limit","Host pattern exceeds its instruction, depth or temporary text limit") }
