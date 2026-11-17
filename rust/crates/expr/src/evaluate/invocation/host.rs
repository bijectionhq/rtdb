use std::{borrow::Cow,collections::BTreeMap,sync::Arc};
use rtdb_plan::RegexForm as Form;
use rtdb_types::{Anomaly,Result,Value};
use crate::EvalContext;
use crate::jvm_number::{Binary,Bit,Cast,Classification,Comparison,Dispatch,Error as NumberError,Extremum,ExtremumCall,Integral,Number,Predicate,Selected,Unary,bit_not,bit_test};

pub(super) struct Program<'a> { expression: Expression<'a>,locals: usize }
pub(super) enum PatternText<'a> { Borrowed(&'a str),Computed(Arc<str>) }
impl AsRef<str> for PatternText<'_> { fn as_ref(&self) -> &str { match self { Self::Borrowed(value) => value,Self::Computed(value) => value } } }
enum Expression<'a> {
    Constant(&'a Form),Number(Arc<Number>),Nil,Local { index: usize,emission: Emission },Function(Function),
    Cast { kind: Cast,resolution: Resolution<CastInput>,value: Box<Expression<'a>> },
    Boolean { resolution: Resolution<()>,value: Box<Expression<'a>> },
    Unary { operation: Unary,resolution: Resolution<Dispatch>,value: Box<Expression<'a>> },
    Binary { operation: Binary,resolution: Resolution<Dispatch>,values: Box<[Expression<'a>;2]> },
    Predicate { operation: Predicate,resolution: Resolution<Dispatch>,value: Box<Expression<'a>> },
    Comparison { operation: Comparison,resolution: Resolution<Dispatch>,values: Box<[Expression<'a>;2]> },
    Integral { operation: Integral,resolution: Resolution<Dispatch>,values: Box<[Expression<'a>;2]> },
    Absolute { resolution: Resolution<Dispatch>,value: Box<Expression<'a>> },
    Extremum { operation: Extremum,resolution: Resolution<ExtremumCall>,values: Box<[Expression<'a>;2]> },
    BitNot { resolution: Resolution<()>,value: Box<Expression<'a>> },
    Bit { operation: Bit,resolution: Resolution<()>,values: Box<[Expression<'a>;2]> },
    If(Box<[Expression<'a>;3]>),Body(Vec<Expression<'a>>),
    Let { first: usize,bindings: Vec<Expression<'a>>,body: Box<Expression<'a>> },
    Logical { conjunction: bool,values: Vec<Expression<'a>> },
    Vector(Vec<Expression<'a>>),Call { function: Box<Expression<'a>>,arguments: Vec<Expression<'a>> },
}
#[derive(Clone,Copy)]
enum Function { Str,Identity,Name,Namespace,Subs,Not,Boolean,Cast(Cast),Arithmetic(Arithmetic),Comparison(Comparison),Predicate(Predicate),Classification(Classification),Integral(Integral),Modulo,BitNot,Bit(Bit),BitTest,Absolute,Extremum(Extremum) }
#[derive(Clone,Copy)]
enum Arithmetic { Add(bool),Subtract(bool),Multiply(bool),Divide,Increment(bool),Decrement(bool) }
#[derive(Clone)]
enum HostValue<'a> { Constant(&'a Form),Number(Arc<Number>),Nil,Bool(bool),Text(Arc<str>),Function(Function),Vector(Arc<[HostValue<'a>]>) }
struct Compiler<'a,'c,'context> { symbols: BTreeMap<&'a str,(usize,Emission)>,active: usize,slots: usize,nodes: usize,context: &'c EvalContext<'context> }
#[derive(Clone,Copy,PartialEq,Eq)]
enum Emission { Number(Cast),Boolean,Reference,Unknown }
#[derive(Clone,Copy)]
enum Resolution<T> { Resolved(T),Reflective }
#[derive(Clone,Copy)]
enum CastInput { Primitive,Boxed }
impl Emission {
    fn number(self) -> Option<Cast> { match self { Self::Number(kind) => Some(kind),_ => None } }
    fn branch(self,other: Self) -> Self { if self == other && matches!(self,Self::Number(_) | Self::Boolean) { self } else { Self::Unknown } }
}
impl<T: Copy> Resolution<T> {
    fn emission(self,resolved: impl FnOnce(T)->Emission) -> Emission { match self { Self::Resolved(value) => resolved(value),Self::Reflective => Emission::Unknown } }
}
impl Resolution<Dispatch> {
    fn unary(value: Emission) -> Self { if value == Emission::Boolean { Self::Reflective } else { Self::Resolved(Dispatch::unary(value.number())) } }
    fn binary(left: Emission,right: Emission) -> Self { if left == Emission::Boolean || right == Emission::Boolean { Self::Reflective } else { Self::Resolved(Dispatch::binary(left.number(),right.number())) } }
    fn integral(left: Emission,right: Emission) -> Self {
        if left == Emission::Boolean || right == Emission::Boolean { Self::Reflective }
        else { Self::Resolved(match (left.number(),right.number()) { (Some(left),Some(right)) => Dispatch::binary(Some(left),Some(right)),_ => Dispatch::Object }) }
    }
    fn dispatch(self) -> Dispatch { match self { Self::Resolved(value) => value,Self::Reflective => Dispatch::Object } }
}
impl Resolution<()> {
    fn bit(value: Emission) -> Self { if matches!(value,Emission::Boolean | Emission::Number(Cast::Float | Cast::Double)) { Self::Reflective } else { Self::Resolved(()) } }
    fn bits(left: Emission,right: Emission) -> Self {
        match (Self::bit(left),Self::bit(right)) { (Self::Resolved(()),Self::Resolved(())) => Self::Resolved(()),_ => Self::Reflective }
    }
}
impl Resolution<ExtremumCall> {
    fn extremum(left: Emission,right: Emission) -> Self {
        if left == Emission::Boolean || right == Emission::Boolean { Self::Reflective }
        else { Self::Resolved(ExtremumCall { left: Dispatch::unary(left.number()),right: Dispatch::unary(right.number()) }) }
    }
    fn call(self) -> ExtremumCall { match self { Self::Resolved(value) => value,Self::Reflective => ExtremumCall::BOXED } }
}

impl<'a> Program<'a> {
    pub(super) fn compile(form: &'a Form,context: &EvalContext<'_>) -> Result<Self> {
        let mut compiler = Compiler { symbols: BTreeMap::new(),active: 0,slots: 0,nodes: 0,context };
        let expression = compiler.expression(form,0)?;
        if expression.stack_depth() > 128 { return Err(limit()); }
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
                if let Some((index,emission)) = self.symbols.get(name.as_str()) { Expression::Local { index: *index,emission: *emission } }
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
        if matches!(operation,Some("boolean" | "clojure.core/boolean")) {
            arity(args.len(),1,1)?;
            let value = self.expression(&args[0],depth)?;
            let resolution = if matches!(value.emission(),Emission::Number(_)) { Resolution::Reflective } else { Resolution::Resolved(()) };
            return Ok(Expression::Boolean { resolution,value: Box::new(value) });
        }
        if let Some(Function::Cast(kind)) = operation.and_then(function) {
            arity(args.len(),1,1)?;
            let value = self.expression(&args[0],depth)?;
            let resolution = match value.emission() { Emission::Number(_) => Resolution::Resolved(CastInput::Primitive),Emission::Boolean => Resolution::Reflective,_ => Resolution::Resolved(CastInput::Boxed) };
            return Ok(Expression::Cast { kind,resolution,value: Box::new(value) });
        }
        if let Some(Function::Predicate(operation)) = operation.and_then(function) {
            arity(args.len(),1,1)?;
            let value = self.expression(&args[0],depth)?;
            return Ok(Expression::Predicate { operation,resolution: Resolution::unary(value.emission()),value: Box::new(value) });
        }
        if let Some(Function::Comparison(operation)) = operation.and_then(function) {
            if args.len() == 2 {
                let left = self.expression(&args[0],depth)?;let right = self.expression(&args[1],depth)?;
                let resolution = Resolution::binary(left.emission(),right.emission());
                return Ok(Expression::Comparison { operation,resolution,values: Box::new([left,right]) });
            }
        }
        if let Some(Function::Integral(operation)) = operation.and_then(function) {
            arity(args.len(),2,2)?;
            let left = self.expression(&args[0],depth)?;let right = self.expression(&args[1],depth)?;
            let resolution = Resolution::integral(left.emission(),right.emission());
            return Ok(Expression::Integral { operation,resolution,values: Box::new([left,right]) });
        }
        if matches!(operation.and_then(function),Some(Function::Absolute)) && args.len() == 1 {
            let value = self.expression(&args[0],depth)?;
            return Ok(Expression::Absolute { resolution: Resolution::unary(value.emission()),value: Box::new(value) });
        }
        if let Some(Function::Extremum(operation)) = operation.and_then(function) {
            if args.len() > 1 { return self.extremum(operation,args,depth); }
        }
        if matches!(operation.and_then(function),Some(Function::BitNot)) {
            arity(args.len(),1,1)?;
            let value = self.expression(&args[0],depth)?;
            return Ok(Expression::BitNot { resolution: Resolution::bit(value.emission()),value: Box::new(value) });
        }
        if let Some(Function::Bit(operation)) = operation.and_then(function) {
            if operation.inline(args.len()) { return self.bit(operation,args,depth); }
        }
        if let Some(Function::Arithmetic(operation)) = operation.and_then(function) {
            if operation.inline(args.len()) { return self.arithmetic(operation,args,depth); }
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
                    let emission = expression.emission();
                    expressions.push(expression);
                    previous.push((name.as_str(),self.symbols.insert(name,(self.active,emission))));
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
    fn arithmetic(&mut self,operation: Arithmetic,args: &'a [Form],depth: usize) -> Result<Expression<'a>> {
        if matches!(operation,Arithmetic::Increment(_) | Arithmetic::Decrement(_)) { arity(args.len(),1,1)?; }
        if depth.checked_add(args.len()).is_none_or(|depth|depth > 128) { return Err(limit()); }
        let mut args = args.iter();
        let first = self.expression(args.next().ok_or_else(||error("Missing host argument"))?,depth)?;
        if let Some(unary) = operation.unary() {
            if args.len() == 0 { return Ok(Expression::Unary { operation: unary,resolution: Resolution::unary(first.emission()),value: Box::new(first) }); }
        }
        let binary = operation.binary().ok_or_else(||error("Invalid host arithmetic arity"))?;
        let mut result = first;
        for value in args {
            self.nodes += 1;
            if self.nodes > 65_536 { return Err(limit()); }
            self.context.reserve(256)?;
            let right = self.expression(value,depth)?;
            let resolution = Resolution::binary(result.emission(),right.emission());
            result = Expression::Binary { operation: binary,resolution,values: Box::new([result,right]) };
        }
        if result.stack_depth() > 128 { return Err(limit()); }
        Ok(result)
    }
    fn bit(&mut self,operation: Bit,args: &'a [Form],depth: usize) -> Result<Expression<'a>> {
        if !operation.variadic() { arity(args.len(),2,2)?; }
        if depth.checked_add(args.len()).is_none_or(|depth|depth > 128) { return Err(limit()); }
        let mut args = args.iter();
        let mut result = self.expression(args.next().ok_or_else(||error("Missing host argument"))?,depth)?;
        for value in args {
            self.nodes += 1;
            if self.nodes > 65_536 { return Err(limit()); }
            self.context.reserve(256)?;
            let right = self.expression(value,depth)?;
            let resolution = Resolution::bits(result.emission(),right.emission());
            result = Expression::Bit { operation,resolution,values: Box::new([result,right]) };
        }
        if result.stack_depth() > 128 { return Err(limit()); }
        Ok(result)
    }
    fn extremum(&mut self,operation: Extremum,args: &'a [Form],depth: usize) -> Result<Expression<'a>> {
        if depth.checked_add(args.len()).is_none_or(|depth|depth > 128) { return Err(limit()); }
        let mut args = args.iter();
        let mut result = self.expression(args.next().ok_or_else(||error("Missing host argument"))?,depth)?;
        for value in args {
            self.nodes += 1;
            if self.nodes > 65_536 { return Err(limit()); }
            self.context.reserve(256)?;
            let right = self.expression(value,depth)?;
            let resolution = Resolution::extremum(result.emission(),right.emission());
            result = Expression::Extremum { operation,resolution,values: Box::new([result,right]) };
        }
        if result.stack_depth() > 128 { return Err(limit()); }
        Ok(result)
    }
}

impl<'a> Expression<'a> {
    fn stack_depth(&self) -> usize {
        let children = match self {
            Self::Cast { value,.. } | Self::Boolean { value,.. } | Self::Unary { value,.. } | Self::Predicate { value,.. } | Self::BitNot { value,.. } | Self::Absolute { value,.. } => value.stack_depth(),
            Self::Binary { values,.. } | Self::Comparison { values,.. } | Self::Integral { values,.. } | Self::Bit { values,.. } | Self::Extremum { values,.. } => values.iter().map(Self::stack_depth).max().unwrap_or(0),
            Self::If(values) => values.iter().map(Self::stack_depth).max().unwrap_or(0),
            Self::Body(values) | Self::Vector(values) | Self::Logical { values,.. } => values.iter().map(Self::stack_depth).max().unwrap_or(0),
            Self::Let { bindings,body,.. } => bindings.iter().map(Self::stack_depth).max().unwrap_or(0).max(body.stack_depth()),
            Self::Call { function,arguments } => function.stack_depth().max(arguments.iter().map(Self::stack_depth).max().unwrap_or(0)),
            _ => 0,
        };
        children + 1
    }
    fn emission(&self) -> Emission {
        match self {
            Self::Constant(Form::Literal(Value::I64(_))) => Emission::Number(Cast::Long),Self::Constant(Form::Literal(Value::F64(_))) => Emission::Number(Cast::Double),
            Self::Local { emission,.. } => *emission,
            Self::Cast { kind,resolution,.. } => resolution.emission(|_|Emission::Number(*kind)),
            Self::Boolean { resolution,.. } => resolution.emission(|_|Emission::Boolean),
            Self::Predicate { resolution,.. } | Self::Comparison { resolution,.. } => resolution.emission(|_|Emission::Boolean),
            Self::Unary { operation,resolution,.. } => resolution.emission(|dispatch|operation.primitive(dispatch).map_or(Emission::Reference,Emission::Number)),
            Self::Binary { operation,resolution,.. } => resolution.emission(|dispatch|operation.primitive(dispatch).map_or(Emission::Reference,Emission::Number)),
            Self::Integral { operation,resolution,.. } => resolution.emission(|dispatch|operation.primitive(dispatch).map_or(Emission::Reference,Emission::Number)),
            Self::Absolute { resolution,.. } => resolution.emission(|dispatch|dispatch.result(false).map_or(Emission::Reference,Emission::Number)),
            Self::Extremum { resolution,.. } => resolution.emission(|call|call.primitive().map_or(Emission::Reference,Emission::Number)),
            Self::BitNot { resolution,.. } | Self::Bit { resolution,.. } => resolution.emission(|_|Emission::Number(Cast::Long)),
            Self::If(branches) => branches[1].emission().branch(branches[2].emission()),
            Self::Body(values) => values.last().map_or(Emission::Reference,Self::emission),Self::Let { body,.. } => body.emission(),
            Self::Logical { values,.. } => values.iter().map(Self::emission).reduce(Emission::branch).unwrap_or(Emission::Reference),
            Self::Call { .. } => Emission::Unknown,
            _ => Emission::Reference,
        }
    }
    fn evaluate(&self,locals: &mut [HostValue<'a>],context: &EvalContext<'_>) -> Result<HostValue<'a>> {
        Ok(match self {
            Self::Constant(form) => HostValue::Constant(form),Self::Number(value) => HostValue::Number(Arc::clone(value)),Self::Nil => HostValue::Nil,
            Self::Local { index,.. } => locals[*index].clone(),Self::Function(function) => HostValue::Function(*function),
            Self::Cast { kind,resolution,value } => numeric_cast(*kind,value.evaluate(locals,context)?,matches!(resolution,Resolution::Resolved(CastInput::Primitive)),context)?,
            Self::Boolean { value,.. } => HostValue::Bool(value.evaluate(locals,context)?.truthy()),
            Self::Unary { operation,resolution,value } => numeric_unary(*operation,resolution.dispatch(),value.evaluate(locals,context)?,context)?,
            Self::Binary { operation,resolution,values } => {
                let left = values[0].evaluate(locals,context)?;
                let right = values[1].evaluate(locals,context)?;
                numeric_binary(*operation,resolution.dispatch(),left,right,context)?
            }
            Self::Predicate { operation,value,.. } => numeric_predicate(*operation,&value.evaluate(locals,context)?,context)?,
            Self::Comparison { operation,resolution,values } => {
                let left = values[0].evaluate(locals,context)?;let right = values[1].evaluate(locals,context)?;
                HostValue::Bool(numeric_compare(*operation,resolution.dispatch(),&left,&right,context)?)
            }
            Self::Integral { operation,resolution,values } => {
                let left = values[0].evaluate(locals,context)?;let right = values[1].evaluate(locals,context)?;
                numeric_integral(*operation,resolution.dispatch(),&left,&right,context)?
            }
            Self::Absolute { resolution,value } => numeric_absolute(resolution.dispatch(),value.evaluate(locals,context)?,context)?,
            Self::Extremum { operation,resolution,values } => {
                let left = values[0].evaluate(locals,context)?;let right = values[1].evaluate(locals,context)?;
                numeric_extremum(*operation,resolution.call(),left,right,context)?
            }
            Self::BitNot { value,.. } => numeric_bit_not(&value.evaluate(locals,context)?,context)?,
            Self::Bit { operation,values,.. } => {
                let left = values[0].evaluate(locals,context)?;let right = values[1].evaluate(locals,context)?;
                numeric_bit(*operation,&left,&right,context)?
            }
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
    fn classifies(&self,classification: Classification) -> Result<bool> {
        Ok(match self {
            Self::Number(value) => value.classifies(classification),
            Self::Constant(Form::Literal(Value::I64(value))) => Number::Long(*value).classifies(classification),
            Self::Constant(Form::Literal(Value::F64(value))) => Number::Double(*value).classifies(classification),
            Self::Constant(Form::Literal(Value::Null | Value::Bool(_) | Value::Utf8(_) | Value::Keyword(_))) => false,
            Self::Constant(Form::Character(_) | Form::Symbol(_) | Form::List(_) | Form::Vector(_) | Form::Set(_) | Form::Map(_) | Form::AnonymousFunction(_)) => false,
            Self::Constant(Form::Literal(_) | Form::BigInteger(_) | Form::Decimal(_) | Form::Tagged { .. }) => return Err(unsupported("Host classification requires an established host literal carrier")),
            Self::Nil | Self::Bool(_) | Self::Text(_) | Self::Function(_) | Self::Vector(_) => false,
        })
    }
    fn nan(&self) -> bool { match self { Self::Number(value) => value.is_nan(),Self::Constant(Form::Literal(Value::F64(value))) => value.is_nan(),_ => false } }
    fn nil(&self) -> bool { matches!(self,Self::Nil | Self::Constant(Form::Literal(Value::Null))) }
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
            Self::Arithmetic(operation) => operation.apply(args,context)?,
            Self::Integral(operation) => { arity(args.len(),2,2)?;numeric_integral(operation,Dispatch::Object,&args[0],&args[1],context)? }
            Self::Absolute => { arity(args.len(),1,1)?;numeric_absolute(Dispatch::Object,args.into_iter().next().ok_or_else(||error("Missing host argument"))?,context)? }
            Self::Extremum(operation) => {
                arity(args.len(),1,usize::MAX)?;
                let mut args = args.into_iter();
                let mut result = args.next().ok_or_else(||error("Missing host argument"))?;
                for value in args { result = numeric_extremum(operation,ExtremumCall::BOXED,result,value,context)?; }
                result
            }
            Self::Modulo => {
                arity(args.len(),2,2)?;
                let remainder = numeric_integral(Integral::Remainder,Dispatch::Object,&args[0],&args[1],context)?;
                if numeric_predicate(Predicate::Zero,&remainder,context)?.truthy() || numeric_predicate(Predicate::Positive,&args[0],context)?.truthy() == numeric_predicate(Predicate::Positive,&args[1],context)?.truthy() { remainder }
                else { numeric_binary(Binary::Add(false),Dispatch::Object,remainder,args[1].clone(),context)? }
            }
            Self::BitNot => { arity(args.len(),1,1)?;numeric_bit_not(&args[0],context)? }
            Self::Bit(operation) => operation.apply_host(args,context)?,
            Self::BitTest => {
                arity(args.len(),2,2)?;
                let left = bit_operand(&args[0],context)?;let right = bit_operand(&args[1],context)?;
                HostValue::Bool(bit_test(left,right))
            }
            Self::Predicate(operation) => { arity(args.len(),1,1)?;numeric_predicate(operation,&args[0],context)? }
            Self::Classification(operation) => { arity(args.len(),1,1)?;HostValue::Bool(args[0].classifies(operation)?) }
            Self::Comparison(operation) => {
                arity(args.len(),1,usize::MAX)?;
                let mut result = true;
                for pair in args.windows(2) { if !numeric_compare(operation,Dispatch::Object,&pair[0],&pair[1],context)? { result = false;break; } }
                HostValue::Bool(result)
            }
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
        "byte" => Function::Cast(Cast::Byte),"short" => Function::Cast(Cast::Short),"int" => Function::Cast(Cast::Int),"long" => Function::Cast(Cast::Long),"float" => Function::Cast(Cast::Float),"double" => Function::Cast(Cast::Double),
        "+" => Function::Arithmetic(Arithmetic::Add(false)),"+'" => Function::Arithmetic(Arithmetic::Add(true)),"-" => Function::Arithmetic(Arithmetic::Subtract(false)),"-'" => Function::Arithmetic(Arithmetic::Subtract(true)),
        "*" => Function::Arithmetic(Arithmetic::Multiply(false)),"*'" => Function::Arithmetic(Arithmetic::Multiply(true)),"/" => Function::Arithmetic(Arithmetic::Divide),
        "inc" => Function::Arithmetic(Arithmetic::Increment(false)),"inc'" => Function::Arithmetic(Arithmetic::Increment(true)),"dec" => Function::Arithmetic(Arithmetic::Decrement(false)),"dec'" => Function::Arithmetic(Arithmetic::Decrement(true)),
        "<" => Function::Comparison(Comparison::Less),"<=" => Function::Comparison(Comparison::LessEqual),">" => Function::Comparison(Comparison::Greater),">=" => Function::Comparison(Comparison::GreaterEqual),"==" => Function::Comparison(Comparison::Equivalent),
        "zero?" => Function::Predicate(Predicate::Zero),"pos?" => Function::Predicate(Predicate::Positive),"neg?" => Function::Predicate(Predicate::Negative),
        "number?" => Function::Classification(Classification::Numeric),"integer?" => Function::Classification(Classification::Integer),"int?" => Function::Classification(Classification::FixedInteger),
        "double?" => Function::Classification(Classification::Double),"float?" => Function::Classification(Classification::Floating),"ratio?" => Function::Classification(Classification::Ratio),
        "decimal?" => Function::Classification(Classification::Decimal),"rational?" => Function::Classification(Classification::Rational),
        "pos-int?" => Function::Classification(Classification::PositiveInteger),"neg-int?" => Function::Classification(Classification::NegativeInteger),"nat-int?" => Function::Classification(Classification::NaturalInteger),
        "quot" => Function::Integral(Integral::Quotient),"rem" => Function::Integral(Integral::Remainder),"mod" => Function::Modulo,
        "min" => Function::Extremum(Extremum::Min),"max" => Function::Extremum(Extremum::Max),"abs" => Function::Absolute,
        "bit-not" => Function::BitNot,"bit-and" => Function::Bit(Bit::And),"bit-or" => Function::Bit(Bit::Or),"bit-xor" => Function::Bit(Bit::Xor),"bit-and-not" => Function::Bit(Bit::AndNot),
        "bit-clear" => Function::Bit(Bit::Clear),"bit-set" => Function::Bit(Bit::Set),"bit-flip" => Function::Bit(Bit::Flip),"bit-test" => Function::BitTest,
        "bit-shift-left" => Function::Bit(Bit::ShiftLeft),"bit-shift-right" => Function::Bit(Bit::ShiftRight),"unsigned-bit-shift-right" => Function::Bit(Bit::UnsignedShiftRight),_ => return None })
}
impl Bit {
    fn variadic(self) -> bool { matches!(self,Self::And | Self::Or | Self::Xor | Self::AndNot) }
    fn inline(self,count: usize) -> bool { if self.variadic() { count > 1 } else { matches!(self,Self::ShiftLeft | Self::ShiftRight | Self::UnsignedShiftRight) } }
    fn apply_host<'a>(self,args: Vec<HostValue<'a>>,context: &EvalContext<'_>) -> Result<HostValue<'a>> {
        arity(args.len(),2,if self.variadic() { usize::MAX } else { 2 })?;
        let mut args = args.into_iter();
        let mut result = args.next().ok_or_else(||error("Missing host argument"))?;
        for value in args { result = numeric_bit(self,&result,&value,context)?; }
        Ok(result)
    }
}
impl Arithmetic {
    fn inline(self,count: usize) -> bool {
        match self { Self::Add(_) | Self::Multiply(_) | Self::Divide => count > 1,Self::Subtract(_) => count > 0,Self::Increment(_) | Self::Decrement(_) => true }
    }
    fn unary(self) -> Option<Unary> {
        match self { Self::Subtract(promote) => Some(Unary::Negate(promote)),Self::Increment(promote) => Some(Unary::Increment(promote)),Self::Decrement(promote) => Some(Unary::Decrement(promote)),_ => None }
    }
    fn binary(self) -> Option<Binary> {
        match self { Self::Add(promote) => Some(Binary::Add(promote)),Self::Subtract(promote) => Some(Binary::Subtract(promote)),Self::Multiply(promote) => Some(Binary::Multiply(promote)),Self::Divide => Some(Binary::Divide),_ => None }
    }
    fn apply<'a>(self,args: Vec<HostValue<'a>>,context: &EvalContext<'_>) -> Result<HostValue<'a>> {
        match self { Self::Increment(_) | Self::Decrement(_) => arity(args.len(),1,1)?,Self::Subtract(_) | Self::Divide => arity(args.len(),1,usize::MAX)?,_ => {} }
        let mut args = args.into_iter();
        let Some(first) = args.next() else { context.reserve(64)?;return Ok(HostValue::Number(Arc::new(Number::Long(if matches!(self,Self::Multiply(_)) { 1 } else { 0 })))); };
        if args.len() == 0 {
            if let Some(operation) = self.unary() { return numeric_unary(operation,Dispatch::Object,first,context); }
            if matches!(self,Self::Divide) {
                context.reserve(64)?;
                let one = HostValue::Number(Arc::new(Number::Long(1)));
                return numeric_binary(Binary::Divide,Dispatch::Object,one,first,context);
            }
            if !first.nil() { drop(first.number(context)?); }
            return Ok(first);
        }
        let operation = self.binary().ok_or_else(||error("Invalid host arithmetic arity"))?;
        let mut result = first;
        for value in args { result = numeric_binary(operation,Dispatch::Object,result,value,context)?; }
        Ok(result)
    }
}
fn numeric_unary<'a>(operation: Unary,dispatch: Dispatch,value: HostValue<'a>,context: &EvalContext<'_>) -> Result<HostValue<'a>> {
    let number = value.number(context)?;
    reserve_numeric(number.unary_bound(operation).map_err(|_|limit())?,context)?;
    Ok(HostValue::Number(Arc::new(number.unary(operation,dispatch).map_err(numeric_error)?)))
}
fn numeric_binary<'a>(operation: Binary,dispatch: Dispatch,left: HostValue<'a>,right: HostValue<'a>,context: &EvalContext<'_>) -> Result<HostValue<'a>> {
    if operation == Binary::Divide && dispatch != Dispatch::Double {
        if left.nan() { return Ok(left); }
        if right.nan() { return Ok(right); }
    }
    let (left,right) = if operation == Binary::Divide && dispatch != Dispatch::Double {
        let right = right.number(context)?;
        right.admit_divisor().map_err(numeric_error)?;
        (left.number(context)?,right)
    } else { (left.number(context)?,right.number(context)?) };
    reserve_numeric(left.binary_bound(operation,&right).map_err(|_|limit())?,context)?;
    Ok(HostValue::Number(Arc::new(left.binary(operation,&right,dispatch).map_err(numeric_error)?)))
}
fn reserve_numeric(bytes: usize,context: &EvalContext<'_>) -> Result<()> {
    if bytes > 16 * 1024 * 1024 { return Err(limit()); }
    context.reserve(bytes)
}
fn numeric_extremum<'a>(operation: Extremum,call: ExtremumCall,left: HostValue<'a>,right: HostValue<'a>,context: &EvalContext<'_>) -> Result<HostValue<'a>> {
    let selected = if left.nan() { Selected::Left } else if right.nan() { Selected::Right } else {
        let left = left.number(context)?;let right = right.number(context)?;
        reserve_numeric(left.comparison_bound(&right).map_err(|_|limit())?,context)?;
        left.select_extremum(operation,&right,call).map_err(numeric_error)?
    };
    let (value,parameter) = selected.choose((left,call.left),(right,call.right));
    let kind = match parameter { Dispatch::Object => return Ok(value),Dispatch::Long => Cast::Long,Dispatch::Double => Cast::Double };
    context.reserve(std::mem::size_of::<Number>() + 2 * std::mem::size_of::<usize>())?;
    Ok(HostValue::Number(Arc::new(Number::cast(kind,&value.number(context)?).map_err(numeric_error)?)))
}
fn numeric_absolute<'a>(dispatch: Dispatch,value: HostValue<'a>,context: &EvalContext<'_>) -> Result<HostValue<'a>> {
    let result = {
        let number = value.number(context)?;
        let capacity = number.absolute_bound(dispatch).map_err(|_|limit())?;
        if capacity != 0 { reserve_numeric(capacity,context)?; }
        let absolute = number.absolute(dispatch).map_err(numeric_error)?;
        match absolute { Cow::Borrowed(_) => None,Cow::Owned(number) => Some(number) }
    };
    Ok(match result { Some(number) => HostValue::Number(Arc::new(number)),None => value })
}
fn numeric_integral<'a>(operation: Integral,dispatch: Dispatch,left: &HostValue<'a>,right: &HostValue<'a>,context: &EvalContext<'_>) -> Result<HostValue<'a>> {
    let (left,right) = if dispatch == Dispatch::Object {
        let right = right.number(context)?;
        right.admit_divisor().map_err(numeric_error)?;
        (left.number(context)?,right)
    } else { (left.number(context)?,right.number(context)?) };
    reserve_numeric(left.integral_bound(&right).map_err(|_|limit())?,context)?;
    Ok(HostValue::Number(Arc::new(left.integral(operation,&right,dispatch).map_err(numeric_error)?)))
}
fn bit_operand(value: &HostValue<'_>,context: &EvalContext<'_>) -> Result<i64> { value.number(context)?.bit_operand().map_err(numeric_error) }
fn bit_result<'a>(value: i64,context: &EvalContext<'_>) -> Result<HostValue<'a>> {
    context.reserve(std::mem::size_of::<Number>() + 2 * std::mem::size_of::<usize>())?;
    Ok(HostValue::Number(Arc::new(Number::Long(value))))
}
fn numeric_bit_not<'a>(value: &HostValue<'a>,context: &EvalContext<'_>) -> Result<HostValue<'a>> { bit_result(bit_not(bit_operand(value,context)?),context) }
fn numeric_bit<'a>(operation: Bit,left: &HostValue<'a>,right: &HostValue<'a>,context: &EvalContext<'_>) -> Result<HostValue<'a>> {
    let left = bit_operand(left,context)?;let right = bit_operand(right,context)?;
    bit_result(operation.apply(left,right),context)
}
fn numeric_error(error: NumberError) -> Anomaly { match error { NumberError::External(error) => error,NumberError::Invalid => self::error("Invalid host arithmetic operation") } }
fn numeric_predicate<'a>(operation: Predicate,value: &HostValue<'a>,context: &EvalContext<'_>) -> Result<HostValue<'a>> {
    Ok(HostValue::Bool(value.number(context)?.predicate(operation)))
}
fn numeric_compare(operation: Comparison,dispatch: Dispatch,left: &HostValue<'_>,right: &HostValue<'_>,context: &EvalContext<'_>) -> Result<bool> {
    let left = left.number(context)?;let right = right.number(context)?;
    reserve_numeric(left.comparison_bound(&right).map_err(|_|limit())?,context)?;
    left.compare_numeric(operation,&right,dispatch).map_err(numeric_error)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EvaluationScope;
    use rtdb_columnar::MemoryBudget;

    fn number(value: HostValue<'_>) -> Arc<Number> { match value { HostValue::Number(value) => value,_ => panic!("number expected") } }

    #[test]
    fn extrema_return_selected_object_identity_and_box_widened_primitive_parameters() {
        let context = EvalContext::default();
        let vector: Arc<[HostValue<'_>]> = vec![HostValue::Nil].into();
        let result = Function::Extremum(Extremum::Min).apply(vec![HostValue::Vector(Arc::clone(&vector))],&context).unwrap();
        assert!(matches!(result,HostValue::Vector(value) if Arc::ptr_eq(&value,&vector)));
        let left = Arc::new(Number::decimal("1.0").unwrap());let right = Arc::new(Number::decimal("1.00").unwrap());
        for operation in [Extremum::Min,Extremum::Max] {
            let result = number(numeric_extremum(operation,ExtremumCall::BOXED,HostValue::Number(Arc::clone(&left)),HostValue::Number(Arc::clone(&right)),&context).unwrap());
            assert!(Arc::ptr_eq(&result,&right));
        }
        let float = Arc::new(Number::Float(1.2));let one = Arc::new(Number::Long(1));
        let result = number(numeric_extremum(Extremum::Max,ExtremumCall { left: Dispatch::Double,right: Dispatch::Long },HostValue::Number(Arc::clone(&float)),HostValue::Number(Arc::clone(&one)),&context).unwrap());
        assert!(!Arc::ptr_eq(&result,&float));
        assert_eq!(*result,Number::Double(f64::from(1.2f32)));
        let result = number(numeric_extremum(Extremum::Max,ExtremumCall::BOXED,HostValue::Number(Arc::clone(&float)),HostValue::Number(one),&context).unwrap());
        assert!(Arc::ptr_eq(&result,&float));
        let byte = Arc::new(Number::Byte(2));
        let result = number(numeric_extremum(Extremum::Max,ExtremumCall { left: Dispatch::Long,right: Dispatch::Object },HostValue::Number(Arc::clone(&byte)),HostValue::Number(Arc::new(Number::Long(1))),&context).unwrap());
        assert_eq!(*result,Number::Long(2));
        assert!(!Arc::ptr_eq(&result,&byte));
    }
    #[test]
    fn extrema_nan_admission_and_nonnegative_decimal_abs_preserve_existing_objects() {
        let memory = MemoryBudget::new(64 << 10);
        let huge = Arc::new(Number::integer(&"9".repeat(8192)).unwrap());
        let decimal = Arc::new(Number::decimal(&"9".repeat(8192)).unwrap());
        let nan = Arc::new(Number::Float(f32::NAN));
        {
            let scope = EvaluationScope::new(&memory).unwrap();
            let context = EvalContext { allocation: Some(&scope),..Default::default() };
            let retained = memory.used_bytes();
            let result = number(numeric_extremum(Extremum::Min,ExtremumCall::BOXED,HostValue::Number(huge),HostValue::Number(Arc::clone(&nan)),&context).unwrap());
            assert!(Arc::ptr_eq(&result,&nan));
            let result = number(numeric_absolute(Dispatch::Object,HostValue::Number(Arc::clone(&decimal)),&context).unwrap());
            assert!(Arc::ptr_eq(&result,&decimal));
            assert_eq!(memory.used_bytes(),retained);
            let result = number(numeric_extremum(Extremum::Max,ExtremumCall::BOXED,HostValue::Number(Arc::clone(&nan)),HostValue::Bool(true),&context).unwrap());
            assert!(Arc::ptr_eq(&result,&nan));
            let result = number(numeric_extremum(Extremum::Max,ExtremumCall { left: Dispatch::Double,right: Dispatch::Object },HostValue::Number(Arc::clone(&nan)),HostValue::Bool(true),&context).unwrap());
            assert!(matches!(result.as_ref(),Number::Double(value) if value.is_nan()));
            assert!(!Arc::ptr_eq(&result,&nan));
        }
        assert_eq!(memory.used_bytes(),0);
    }
    #[test]
    fn absolute_numeric_copy_is_reserved_before_allocation_and_released_on_error() {
        let memory = MemoryBudget::new(64 << 10);
        let huge = Arc::new(Number::integer(&format!("-{}","9".repeat(8192))).unwrap());
        {
            let scope = EvaluationScope::new(&memory).unwrap();
            let context = EvalContext { allocation: Some(&scope),..Default::default() };
            let error = numeric_absolute(Dispatch::Object,HostValue::Number(Arc::clone(&huge)),&context).err().unwrap();
            assert_eq!(error.code,"rtdb/memory-budget-exceeded");
            assert_eq!(Arc::strong_count(&huge),1);
            assert!(memory.peak_bytes() <= memory.limit_bytes());
        }
        assert_eq!(memory.used_bytes(),0);
        let negative = Arc::new(Number::decimal("-1.00").unwrap());
        let result = number(numeric_absolute(Dispatch::Object,HostValue::Number(Arc::clone(&negative)),&EvalContext::default()).unwrap());
        assert!(!Arc::ptr_eq(&result,&negative));
        assert_eq!(*result,Number::decimal("1.00").unwrap());
    }
    #[test]
    fn classifications_borrow_retained_numbers_without_scratch_or_coefficient_copies() {
        let memory = MemoryBudget::new(4096);
        let values = [
            Arc::new(Number::integer(&"9".repeat(8192)).unwrap()),
            Arc::new(Number::decimal("1E-2147483647").unwrap()),
            Arc::new(Number::divide(&Number::Long(1),&Number::Long(3)).unwrap()),
        ];
        {
            let scope = EvaluationScope::new(&memory).unwrap();
            let context = EvalContext { allocation: Some(&scope),..Default::default() };
            let retained = memory.used_bytes();
            for _ in 0..4 {
                for value in &values {
                    for classification in [Classification::Numeric,Classification::Rational] {
                        let result = Function::Classification(classification).apply(vec![HostValue::Number(Arc::clone(value))],&context).unwrap();
                        assert!(matches!(result,HostValue::Bool(true)));
                        assert_eq!(Arc::strong_count(value),1);
                        assert_eq!(memory.used_bytes(),retained);
                    }
                }
            }
        }
        assert_eq!(memory.used_bytes(),0);
    }
}
