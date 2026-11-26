use std::{borrow::Cow,collections::BTreeMap,sync::Arc};
use rtdb_plan::{RegexForm as Form, RegexFormKind as FormKind, RegexFormView as FormView, RegexForms as Forms, RegexLiteralKind as LiteralKind};
use rtdb_types::{Anomaly,Result,Value,java_text::{JavaString,JavaText}};
use crate::EvalContext;
use crate::jvm_number::{Binary,Bit,Cast,Classification,Comparison,Dispatch,Error as NumberError,Extremum,ExtremumCall,Integral,Number,Predicate,Selected,Unary,bit_not,bit_test};

mod text;

pub(super) struct Program<'a> { expression: Expression<'a>,locals: usize }
pub(super) enum PatternText<'a> { Borrowed(JavaText<'a>),Computed(JavaString) }
/// Host forms nest up to the depth limit, and an unoptimised frame of either recursion exceeds 30 KiB.
const RED_ZONE: usize = 128 * 1024;
const STACK_SEGMENT: usize = 2 * 1024 * 1024;

impl PatternText<'_> { pub(super) fn view(&self) -> JavaText<'_> { match self { Self::Borrowed(value) => *value,Self::Computed(value) => value.view() } } }
enum Expression<'a> {
    Constant(FormView<'a>),Number(Arc<Number>),Nil,Local { index: usize,emission: Emission },Function(Function),
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
enum Function { Str,PatternQuote,Format,Apply,Join,UpperCase,LowerCase,Replace,Trim,Identity,Name,Namespace,Subs,Not,Boolean,Cast(Cast),Arithmetic(Arithmetic),Comparison(Comparison),Predicate(Predicate),Classification(Classification),Integral(Integral),Modulo,BitNot,Bit(Bit),BitTest,Absolute,Extremum(Extremum) }
#[derive(Clone,Copy)]
enum Arithmetic { Add(bool),Subtract(bool),Multiply(bool),Divide,Increment(bool),Decrement(bool) }
#[derive(Clone)]
enum HostValue<'a> { Constant(FormView<'a>),Number(Arc<Number>),Nil,Bool(bool),Text(JavaString),Function(Function),Vector(Arc<[HostValue<'a>]>) }
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
        let expression = compiler.expression(form.view(),0)?;
        if expression.stack_depth() > 128 { return Err(limit()); }
        Ok(Self { expression,locals: compiler.slots })
    }
    pub(super) fn pattern(&self,context: &EvalContext<'_>) -> Result<PatternText<'a>> {
        context.reserve(self.locals.checked_mul(std::mem::size_of::<HostValue<'_>>()).ok_or_else(limit)?)?;
        let mut locals = vec![HostValue::Nil;self.locals];
        match self.expression.evaluate(&mut locals,context)? {
            HostValue::Text(value) => Ok(PatternText::Computed(value)),
            HostValue::Constant(form) => match form.kind() {
                FormKind::Literal(value) => match value.kind() { LiteralKind::Text(value) => Ok(PatternText::Borrowed(JavaText::from_utf8(value))), _ => Err(error("REGEXP_REPLACE host pattern did not produce a String")) },
                _ => Err(error("REGEXP_REPLACE host pattern did not produce a String")),
            },
            _ => Err(error("REGEXP_REPLACE host pattern did not produce a String")),
        }
    }
}

impl<'a> Compiler<'a,'_,'_> {
    fn expression(&mut self,form: FormView<'a>,depth: usize) -> Result<Expression<'a>> {
        stacker::maybe_grow(RED_ZONE, STACK_SEGMENT, || self.expression_frame(form, depth))
    }
    fn expression_frame(&mut self,form: FormView<'a>,depth: usize) -> Result<Expression<'a>> {
        self.nodes += 1;
        if depth > 128 || self.nodes > 65_536 { return Err(limit()); }
        self.context.reserve(256)?;
        Ok(match form.kind() {
            FormKind::Symbol(name) => {
                if let Some((index,emission)) = self.symbols.get(name) { Expression::Local { index: *index,emission: *emission } }
                else if let Some(function) = function(name) { Expression::Function(function) }
                else { return Err(unsupported("Host symbol or namespace is not implemented")); }
            }
            FormKind::List(values) => match values.split_first() { Some((head,args)) => self.list(head,args,depth + 1)?, None => Expression::Constant(form) },
            FormKind::BigInteger(_) | FormKind::Decimal(_) => Expression::Number(Arc::new(literal_number(form,self.context)?)),
            FormKind::Vector(values) => Expression::Vector(self.expressions(values,depth + 1)?),
            FormKind::Map(values) if values.is_empty() => Expression::Constant(form),
            FormKind::Set(values) if values.is_empty() => Expression::Constant(form),
            FormKind::Map(_) | FormKind::Set(_) | FormKind::Tagged { .. } | FormKind::AnonymousFunction(_) => return Err(unsupported("Host collection, tagged object or function syntax is not implemented")),
            _ => Expression::Constant(form),
        })
    }
    fn expressions(&mut self,forms: Forms<'a>,depth: usize) -> Result<Vec<Expression<'a>>> {
        self.context.reserve(forms.len().checked_mul(std::mem::size_of::<Expression<'_>>()).ok_or_else(limit)?)?;
        forms.iter().map(|form|self.expression(form,depth)).collect()
    }
    fn body(&mut self,forms: Forms<'a>,depth: usize) -> Result<Expression<'a>> { Ok(Expression::Body(self.expressions(forms,depth)?)) }
    fn list(&mut self,head: FormView<'a>,args: Forms<'a>,depth: usize) -> Result<Expression<'a>> {
        let operation = match head.kind() { FormKind::Symbol(name) if matches!(name,"quote" | "if" | "do" | "let*") || !self.symbols.contains_key(name) => Some(name),_ => None };
        if matches!(operation,Some("boolean" | "clojure.core/boolean")) {
            arity(args.len(),1,1)?;
            let value = self.expression(args.at(0),depth)?;
            let resolution = if matches!(value.emission(),Emission::Number(_)) { Resolution::Reflective } else { Resolution::Resolved(()) };
            return Ok(Expression::Boolean { resolution,value: Box::new(value) });
        }
        if let Some(Function::Cast(kind)) = operation.and_then(function) {
            arity(args.len(),1,1)?;
            let value = self.expression(args.at(0),depth)?;
            let resolution = match value.emission() { Emission::Number(_) => Resolution::Resolved(CastInput::Primitive),Emission::Boolean => Resolution::Reflective,_ => Resolution::Resolved(CastInput::Boxed) };
            return Ok(Expression::Cast { kind,resolution,value: Box::new(value) });
        }
        if let Some(Function::Predicate(operation)) = operation.and_then(function) {
            arity(args.len(),1,1)?;
            let value = self.expression(args.at(0),depth)?;
            return Ok(Expression::Predicate { operation,resolution: Resolution::unary(value.emission()),value: Box::new(value) });
        }
        if let Some(Function::Comparison(operation)) = operation.and_then(function) {
            if args.len() == 2 {
                let left = self.expression(args.at(0),depth)?;let right = self.expression(args.at(1),depth)?;
                let resolution = Resolution::binary(left.emission(),right.emission());
                return Ok(Expression::Comparison { operation,resolution,values: Box::new([left,right]) });
            }
        }
        if let Some(Function::Integral(operation)) = operation.and_then(function) {
            arity(args.len(),2,2)?;
            let left = self.expression(args.at(0),depth)?;let right = self.expression(args.at(1),depth)?;
            let resolution = Resolution::integral(left.emission(),right.emission());
            return Ok(Expression::Integral { operation,resolution,values: Box::new([left,right]) });
        }
        if matches!(operation.and_then(function),Some(Function::Absolute)) && args.len() == 1 {
            let value = self.expression(args.at(0),depth)?;
            return Ok(Expression::Absolute { resolution: Resolution::unary(value.emission()),value: Box::new(value) });
        }
        if let Some(Function::Extremum(operation)) = operation.and_then(function) {
            if args.len() > 1 { return self.extremum(operation,args,depth); }
        }
        if matches!(operation.and_then(function),Some(Function::BitNot)) {
            arity(args.len(),1,1)?;
            let value = self.expression(args.at(0),depth)?;
            return Ok(Expression::BitNot { resolution: Resolution::bit(value.emission()),value: Box::new(value) });
        }
        if let Some(Function::Bit(operation)) = operation.and_then(function) {
            if operation.inline(args.len()) { return self.bit(operation,args,depth); }
        }
        if let Some(Function::Arithmetic(operation)) = operation.and_then(function) {
            if operation.inline(args.len()) { return self.arithmetic(operation,args,depth); }
        }
        Ok(match operation {
            Some("quote") => { arity(args.len(),1,1)?;match args.at(0).kind() { FormKind::BigInteger(_) | FormKind::Decimal(_) => Expression::Number(Arc::new(literal_number(args.at(0),self.context)?)),_ => Expression::Constant(args.at(0)) } }
            Some("if") => {
                arity(args.len(),2,3)?;
                Expression::If(Box::new([self.expression(args.at(0),depth)?,self.expression(args.at(1),depth)?,match args.get(2) { Some(value) => self.expression(value,depth)?,None => Expression::Nil }]))
            }
            Some("do") => self.body(args,depth)?,
            Some("let" | "clojure.core/let" | "let*") => {
                let Some(FormKind::Vector(bindings)) = args.first().map(|form|form.kind()) else { return Err(error("Host let requires a binding vector")); };
                if bindings.len() % 2 != 0 { return Err(error("Host let requires binding pairs")); }
                let first = self.active;
                self.context.reserve(bindings.len().checked_mul(std::mem::size_of::<Expression<'_>>() + 256).ok_or_else(limit)?)?;
                let mut expressions = Vec::with_capacity(bindings.len() / 2);
                let mut previous = Vec::with_capacity(bindings.len() / 2);
                for offset in (0..bindings.len()).step_by(2) {
                    let FormKind::Symbol(name) = bindings.at(offset).kind() else { return Err(unsupported("Host binding destructuring is not implemented")); };
                    if name != "/" && name.contains('/') { return Err(error("Host let cannot bind a qualified symbol")); }
                    let expression = self.expression(bindings.at(offset + 1),depth)?;
                    let emission = expression.emission();
                    expressions.push(expression);
                    previous.push((name,self.symbols.insert(name,(self.active,emission))));
                    self.active += 1;
                    self.slots = self.slots.max(self.active);
                }
                let body = Box::new(self.body(args.slice(1..args.len()),depth)?);
                for (name,index) in previous.into_iter().rev() {
                    match index { Some(index) => { self.symbols.insert(name,index); },None => { self.symbols.remove(name); } }
                }
                self.active = first;
                Expression::Let { first,bindings: expressions,body }
            }
            Some("when" | "clojure.core/when" | "when-not" | "clojure.core/when-not") => {
                let Some(test) = args.first() else { return Err(error("Host when requires a test")); };
                let test = self.expression(test,depth)?;
                let body = self.body(args.slice(1..args.len()),depth)?;
                let branches = if operation.is_some_and(|name|name.ends_with("when-not")) { [test,Expression::Nil,body] } else { [test,body,Expression::Nil] };
                Expression::If(Box::new(branches))
            }
            Some("if-let" | "clojure.core/if-let") => self.conditional_binding(args, depth, true)?,
            Some("when-let" | "clojure.core/when-let") => self.conditional_binding(args, depth, false)?,
            Some("and" | "clojure.core/and" | "or" | "clojure.core/or") => Expression::Logical { conjunction: operation.is_some_and(|name|name.ends_with("and")),values: self.expressions(args,depth)? },
            _ => Expression::Call { function: Box::new(self.expression(head,depth)?),arguments: self.expressions(args,depth)? },
        })
    }
    fn conditional_binding(&mut self, args: Forms<'a>, depth: usize, if_let: bool) -> Result<Expression<'a>> {
        arity(args.len(), if if_let { 2 } else { 1 }, usize::MAX)?;
        let FormKind::Vector(bindings) = args.at(0).kind() else {
            return Err(error("Host conditional binding requires a binding vector"));
        };
        if if_let { arity(args.len(), 2, 3)?; }
        if bindings.len() != 2 {
            return Err(error("Host conditional binding requires one binding pair"));
        }
        let FormKind::Symbol(name) = bindings.at(0).kind() else {
            return Err(unsupported("Host binding destructuring is not implemented"));
        };
        if name != "/" && name.contains('/') {
            return Err(error("Host let cannot bind a qualified symbol"));
        }
        let branch_depth = depth + 1;
        let synthetic_nodes = 2 + usize::from(!if_let) + usize::from(!if_let || args.len() == 2);
        self.nodes += synthetic_nodes;
        if branch_depth > 128 || self.nodes > 65_536 { return Err(limit()); }
        self.context.reserve(synthetic_nodes * 256 + std::mem::size_of::<Expression<'_>>() + 256)?;
        let initializer = self.expression(bindings.at(1), depth)?;
        let emission = initializer.emission();
        let first = self.active;
        let previous = self.symbols.insert(name, (first, emission));
        self.active += 1;
        self.slots = self.slots.max(self.active);
        let then = if if_let {
            self.expression(args.at(1), branch_depth)
        } else {
            self.body(args.slice(1..args.len()), branch_depth + 1)
        };
        match previous {
            Some(binding) => { self.symbols.insert(name, binding); }
            None => { self.symbols.remove(name); }
        }
        self.active = first;
        let then = then?;
        let otherwise = match args.get(2).filter(|_| if_let) {
            Some(form) => self.expression(form, branch_depth)?,
            None => Expression::Nil,
        };
        let test = Expression::Local { index: first, emission };
        let body = Expression::If(Box::new([test, then, otherwise]));
        Ok(Expression::Let { first, bindings: vec![initializer], body: Box::new(body) })
    }
    fn arithmetic(&mut self,operation: Arithmetic,args: Forms<'a>,depth: usize) -> Result<Expression<'a>> {
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
    fn bit(&mut self,operation: Bit,args: Forms<'a>,depth: usize) -> Result<Expression<'a>> {
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
    fn extremum(&mut self,operation: Extremum,args: Forms<'a>,depth: usize) -> Result<Expression<'a>> {
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
            Self::Constant(form) => match literal_leaf(*form).as_deref() { Some(Value::I64(_)) => Emission::Number(Cast::Long), Some(Value::F64(_)) => Emission::Number(Cast::Double), _ => Emission::Reference },
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
        stacker::maybe_grow(RED_ZONE, STACK_SEGMENT, || self.evaluate_frame(locals, context))
    }
    fn evaluate_frame(&self,locals: &mut [HostValue<'a>],context: &EvalContext<'_>) -> Result<HostValue<'a>> {
        Ok(match self {
            Self::Constant(form) => HostValue::Constant(*form),Self::Number(value) => HostValue::Number(Arc::clone(value)),Self::Nil => HostValue::Nil,
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
                function.admit_ifn_cast()?;
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
    fn admit_ifn_cast(&self) -> Result<()> {
        match self {
            Self::Function(_) | Self::Nil | Self::Vector(_) => Ok(()),
            Self::Number(_) | Self::Bool(_) | Self::Text(_) => Err(error("Host value is not callable")),
            Self::Constant(form) => match form.kind() {
                FormKind::Symbol(_) | FormKind::Vector(_) | FormKind::Map(_) | FormKind::Set(_) => Ok(()),
                FormKind::Character(_) | FormKind::List(_) | FormKind::AnonymousFunction(_) => Err(error("Host value is not callable")),
                FormKind::BigInteger(_) | FormKind::Decimal(_) | FormKind::Tagged { .. } => Err(unsupported("Host call admission requires an established host literal carrier")),
                FormKind::Literal(value) => match value.kind() {
                    LiteralKind::Keyword(_) => Ok(()),
                    LiteralKind::Text(_) => Err(error("Host value is not callable")),
                    LiteralKind::Leaf(leaf) => match leaf.value().as_ref() {
                        Value::Null => Ok(()),
                        Value::Bool(_) | Value::I64(_) | Value::F64(_) => Err(error("Host value is not callable")),
                        _ => Err(unsupported("Host call admission requires an established host literal carrier")),
                    },
                    _ => Err(unsupported("Host call admission requires an established host literal carrier")),
                },
            },
        }
    }
    fn truthy(&self) -> bool {
        match self { Self::Nil | Self::Bool(false) => false, Self::Constant(form) => !matches!(literal_leaf(*form).as_deref(), Some(Value::Null | Value::Bool(false))), _ => true }
    }
    fn string(&self) -> Result<JavaText<'_>> {
        match self {
            Self::Text(value) => Ok(value.view()),
            Self::Constant(form) => match form.kind() {
                FormKind::Literal(value) => match value.kind() { LiteralKind::Text(value) => Ok(JavaText::from_utf8(value)), _ => Err(error("Host value is not a String")) },
                _ => Err(error("Host value is not a String")),
            },
            _ => Err(error("Host value is not a String")),
        }
    }
    fn number(&self,context: &EvalContext<'_>) -> Result<Cow<'_,Number>> {
        match self { Self::Number(value) => Ok(Cow::Borrowed(value)),Self::Constant(form) => literal_number(*form,context).map(Cow::Owned),_ => Err(error("Host value is not a Number")) }
    }
    fn classifies(&self,classification: Classification) -> Result<bool> {
        Ok(match self {
            Self::Number(value) => value.classifies(classification),
            Self::Constant(form) => match form.kind() {
                FormKind::Literal(value) => match value.kind() {
                    LiteralKind::Text(_) | LiteralKind::Keyword(_) => false,
                    LiteralKind::Leaf(leaf) => match leaf.value().as_ref() {
                        Value::I64(value) => Number::Long(*value).classifies(classification),
                        Value::F64(value) => Number::Double(*value).classifies(classification),
                        Value::Null | Value::Bool(_) => false,
                        _ => return Err(unsupported("Host classification requires an established host literal carrier")),
                    },
                    _ => return Err(unsupported("Host classification requires an established host literal carrier")),
                },
                FormKind::Character(_) | FormKind::Symbol(_) | FormKind::List(_) | FormKind::Vector(_) | FormKind::Set(_) | FormKind::Map(_) | FormKind::AnonymousFunction(_) => false,
                FormKind::BigInteger(_) | FormKind::Decimal(_) | FormKind::Tagged { .. } => return Err(unsupported("Host classification requires an established host literal carrier")),
            },
            Self::Nil | Self::Bool(_) | Self::Text(_) | Self::Function(_) | Self::Vector(_) => false,
        })
    }
    fn nan(&self) -> bool { match self { Self::Number(value) => value.is_nan(),Self::Constant(form) => matches!(literal_leaf(*form).as_deref(), Some(Value::F64(value)) if value.is_nan()),_ => false } }
    fn nil(&self) -> bool { matches!(self,Self::Nil) || matches!(self,Self::Constant(form) if matches!(literal_leaf(*form).as_deref(),Some(Value::Null))) }
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
                context.reserve(args.len().checked_mul(std::mem::size_of::<JavaString>()).ok_or_else(limit)?)?;
                let mut strings = Vec::new();
                strings.try_reserve_exact(args.len()).map_err(|_|allocation_error())?;
                let mut length = 0usize;
                let mut units = 0usize;
                for value in &args {
                    let value = str_value(value,context)?;
                    length = length.checked_add(text_bytes(value.view())?).ok_or_else(limit)?;
                    units = units.checked_add(value.view().len_units()).ok_or_else(limit)?;
                    strings.push(value);
                }
                reserve_text(length,context)?;
                let mut output = Vec::new();
                output.try_reserve_exact(units).map_err(|_|allocation_error())?;
                for value in &strings { output.extend(value.view().units()); }
                HostValue::Text(retain_units(&output,context)?)
            }
            Self::PatternQuote | Self::Format | Self::Apply | Self::Join | Self::UpperCase | Self::LowerCase | Self::Replace | Self::Trim => self.apply_text(args,context)?,
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
                    HostValue::Constant(form) => match form.kind() {
                        FormKind::Symbol(name) => Some(name),
                        FormKind::Literal(value) => match value.kind() { LiteralKind::Keyword(name) => Some(name), _ => None },
                        _ => None,
                    },
                    _ => None,
                };
                let name = match name {
                    Some(name) => name,
                    None if !namespace => return Ok(HostValue::Text(text(args[0].string()?,context)?)),
                    None => return Err(error("Host namespace requires a symbol or keyword")),
                };
                let (prefix,name) = if name == "/" { (None,name) } else { name.split_once('/').map_or((None,name),|(prefix,name)|(Some(prefix),name)) };
                match if namespace { prefix } else { Some(name) } { Some(value) => HostValue::Text(text_utf8(value,context)?),None => HostValue::Nil }
            }
            Self::Subs => {
                arity(args.len(),2,3)?;
                let value = args[0].string()?;
                let start = args[1].index(context)?;
                let length = i32::try_from(value.len_units()).map_err(|_|limit())?;
                let end = if args.len() == 3 { args[2].index(context)? } else { length };
                if start < 0 || end < start || end > length { return Err(error("Host substring indexes are out of range")); }
                context.reserve((length as usize).checked_mul(2).ok_or_else(limit)?)?;
                let mut units = Vec::new();
                units.try_reserve_exact(length as usize).map_err(|_|allocation_error())?;
                units.extend(value.units());
                let capacity = ((end - start) as usize).checked_mul(3).ok_or_else(limit)?;
                reserve_text(capacity,context)?;
                HostValue::Text(retain_units(&units[start as usize..end as usize],context)?)
            }
        })
    }
}
fn function(name: &str) -> Option<Function> {
    match name {
        "java.util.regex.Pattern/quote" => return Some(Function::PatternQuote),
        "clojure.string/join" => return Some(Function::Join),"clojure.string/upper-case" => return Some(Function::UpperCase),"clojure.string/lower-case" => return Some(Function::LowerCase),
        "clojure.string/replace" => return Some(Function::Replace),"clojure.string/trim" => return Some(Function::Trim),
        _ => {}
    }
    let name = name.strip_prefix("clojure.core/").unwrap_or(name);
    Some(match name { "str" => Function::Str,"format" => Function::Format,"apply" => Function::Apply,"identity" => Function::Identity,"name" => Function::Name,"namespace" => Function::Namespace,"subs" => Function::Subs,"not" => Function::Not,"boolean" => Function::Boolean,
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
trait BitHost: Sized {
    fn variadic(self) -> bool;
    fn inline(self,count: usize) -> bool;
    fn apply_host<'a>(self,args: Vec<HostValue<'a>>,context: &EvalContext<'_>) -> Result<HostValue<'a>>;
}
impl BitHost for Bit {
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
    Ok(HostValue::Number(Arc::new(Number::cast(kind,value.number(context)?.as_ref()).map_err(numeric_error)?)))
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
fn str_value(value: &HostValue<'_>,context: &EvalContext<'_>) -> Result<JavaString> {
    match value {
        HostValue::Text(value) => Ok(value.clone()),HostValue::Nil => text_utf8("",context),HostValue::Bool(value) => text_utf8(if *value { "true" } else { "false" },context),
        HostValue::Vector(_) => printed_text(value,context),
        HostValue::Constant(form) => match form.kind() {
            FormKind::List(_) | FormKind::Vector(_) | FormKind::Map(_) | FormKind::Set(_) => printed_text(value,context),
            FormKind::Symbol(value) => text_utf8(value,context),
            FormKind::Character(value) => { context.reserve(32)?;text_utf8(&value.to_string(),context) }
            FormKind::Literal(value) => match value.kind() {
                LiteralKind::Text(value) => text_utf8(value,context),
                LiteralKind::Keyword(value) => { reserve_text(value.len().checked_add(1).ok_or_else(limit)?,context)?;text_utf8(&format!(":{value}"),context) }
                LiteralKind::Leaf(leaf) => match leaf.value().as_ref() {
                    Value::Null => text_utf8("",context),
                    Value::Bool(value) => text_utf8(if *value { "true" } else { "false" },context),
                    Value::I64(value) => { context.reserve(32)?;text_utf8(&value.to_string(),context) }
                    Value::F64(value) => { context.reserve(64)?;text_utf8(&rtdb_types::java_f64_text(*value),context) }
                    _ => Err(unsupported("Host object string rendering is not implemented for this value")),
                },
                _ => Err(unsupported("Host object string rendering is not implemented for this value")),
            },
            _ => Err(unsupported("Host object string rendering is not implemented for this value")),
        },
        HostValue::Number(value) => { reserve_text(value.text_capacity().map_err(|_|limit())?,context)?;text_utf8(&value.text(),context) }
        _ => Err(unsupported("Host object string rendering is not implemented for this value")),
    }
}
fn printed_text(value: &HostValue<'_>,context: &EvalContext<'_>) -> Result<JavaString> {
    match text::printed(value,context)? { HostValue::Text(value) => Ok(value),_ => unreachable!() }
}
fn literal_leaf<'a>(form: FormView<'a>) -> Option<std::borrow::Cow<'a, Value>> {
    match form.kind() { FormKind::Literal(value) => match value.kind() { LiteralKind::Leaf(leaf) => Some(leaf.value()), _ => None }, _ => None }
}
fn literal_number(form: FormView<'_>,context: &EvalContext<'_>) -> Result<Number> {
    Ok(match form.kind() {
        FormKind::Literal(value) => match value.kind() {
            LiteralKind::Leaf(leaf) => match leaf.value().as_ref() { Value::I64(value) => Number::Long(*value), Value::F64(value) => Number::Double(*value), _ => return Err(error("Host value is not a Number")) },
            _ => return Err(error("Host value is not a Number")),
        },
        FormKind::BigInteger(value) | FormKind::Decimal(value) => {
            if value.len() > 16 * 1024 * 1024 { return Err(limit()); }
            context.reserve(value.len().checked_mul(32).and_then(|bytes|bytes.checked_add(256)).ok_or_else(limit)?)?;
            if matches!(form.kind(),FormKind::BigInteger(_)) { Number::integer(value) } else { Number::decimal(value) }.map_err(|_|error("Invalid host numeric literal"))?
        }
        _ => return Err(error("Host value is not a Number")),
    })
}
fn numeric_cast<'a>(kind: Cast,value: HostValue<'a>,primitive: bool,context: &EvalContext<'_>) -> Result<HostValue<'a>> {
    let source = match &value {
        HostValue::Constant(form) if matches!(kind,Cast::Byte | Cast::Short | Cast::Int | Cast::Long) => match form.kind() {
            FormKind::Character(character) => {
                if u32::from(character) > u32::from(u16::MAX) { return Err(unsupported("Host Character requires a single UTF16 code unit")); }
                Cow::Owned(Number::Long(i64::from(u32::from(character))))
            }
            _ => value.number(context)?,
        },
        _ => value.number(context)?,
    };
    let capacity = source.text_capacity().map_err(|_|limit())?;
    if capacity > 16 * 1024 * 1024 { return Err(limit()); }
    context.reserve(capacity.checked_mul(8).and_then(|bytes|bytes.checked_add(256)).ok_or_else(limit)?)?;
    let result = if primitive { Number::cast_primitive(kind,&source) } else { Number::cast(kind,&source) };
    Ok(HostValue::Number(Arc::new(result.map_err(|_|error("Host numeric cast is out of range"))?)))
}
fn text_bytes(value: JavaText<'_>) -> Result<usize> {
    value.code_points().try_fold(0usize,|bytes,point|bytes.checked_add(match point { 0..=0x7f => 1,0x80..=0x7ff => 2,0x800..=0xffff => 3,_ => 4 }).ok_or_else(limit))
}
fn text(value: JavaText<'_>,context: &EvalContext<'_>) -> Result<JavaString> {
    if let Some(value) = value.utf8_source() { return text_utf8(value,context); }
    reserve_text(text_bytes(value)?,context)?;
    let mut units = Vec::new();
    units.try_reserve_exact(value.len_units()).map_err(|_|allocation_error())?;
    units.extend(value.units());
    retain_units(&units,context)
}
fn text_utf8(value: &str,context: &EvalContext<'_>) -> Result<JavaString> {
    reserve_text(value.len(),context)?;
    match context.allocation { Some(scope) => JavaString::copy_utf8(value,scope.memory_budget()),None => JavaString::copy_utf8_untracked(value) }
}
fn retain_units(units: &[u16],context: &EvalContext<'_>) -> Result<JavaString> {
    match context.allocation { Some(scope) => JavaString::copy_utf16(units,scope.memory_budget()),None => JavaString::copy_utf16_untracked(units) }
}
fn allocation_error() -> Anomaly { Anomaly::incorrect("rtdb.expr/allocation-failed","Unable to allocate host pattern text").with_kind(rtdb_types::AnomalyKind::ResourceExhausted) }
fn reserve_text(length: usize,context: &EvalContext<'_>) -> Result<()> {
    if length > 16 * 1024 * 1024 { return Err(limit()); }
    context.reserve(length.checked_mul(2).and_then(|size|size.checked_add(64)).ok_or_else(limit)?)
}
fn arity(length: usize,min: usize,max: usize) -> Result<()> { if (min..=max).contains(&length) { Ok(()) } else { Err(error("Host form has the wrong number of arguments")) } }
fn error(message: &str) -> Anomaly { Anomaly::fault("xtdb.error/unknown",message) }
fn unsupported(message: &str) -> Anomaly { Anomaly::unsupported("rtdb.expr/regex-host-form",message) }
fn limit() -> Anomaly { Anomaly::incorrect("rtdb.expr/regex-host-limit","Host pattern exceeds its instruction, depth or temporary text limit").with_kind(rtdb_types::AnomalyKind::ProgramLimitExceeded) }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EvaluationScope;
    use rtdb_columnar::MemoryBudget;

    use rtdb_plan::RegexFormBuilder;

    fn raw_build(emit: impl FnOnce(&mut RegexFormBuilder) -> Result<()>) -> Form {
        let mut builder = RegexFormBuilder::new_untracked();
        emit(&mut builder).unwrap();
        builder.finish().unwrap()
    }
    fn raw_literal(value: Value) -> Form { raw_build(|builder| builder.literal(&value)) }
    fn raw_character(value: char) -> Form { raw_build(|builder| builder.character(value)) }
    fn raw_symbol(value: &str) -> Form { raw_build(|builder| builder.symbol(value)) }
    fn raw_list() -> Form { raw_build(|builder| { builder.begin_list()?; builder.end() }) }
    fn raw_vector() -> Form { raw_build(|builder| { builder.begin_vector()?; builder.end() }) }
    fn raw_map() -> Form { raw_build(|builder| { builder.begin_map()?; builder.end() }) }
    fn raw_set() -> Form { raw_build(|builder| { builder.begin_set()?; builder.end() }) }
    fn raw_anonymous(value: Form) -> Form { raw_build(|builder| { builder.begin_anonymous()?; builder.append_view(value.view())?; builder.end() }) }

    #[test]
    fn conditional_binding_uses_one_slot_and_evaluates_one_initializer() {
        let initializer = raw_build(|builder| {
            builder.begin_list()?;
            builder.symbol("str")?;
            builder.literal(&Value::Utf8("a".into()))?;
            builder.end()
        });
        let bound = raw_build(|builder| {
            builder.begin_list()?;
            builder.symbol("if-let")?;
            builder.begin_vector()?;
            builder.symbol("p")?;
            builder.append_view(initializer.view())?;
            builder.end()?;
            builder.symbol("p")?;
            builder.end()
        });
        let compile_context = EvalContext::default();
        let direct = Program::compile(&initializer, &compile_context).unwrap();
        let conditional = Program::compile(&bound, &compile_context).unwrap();
        assert_eq!(direct.locals, 0);
        assert_eq!(conditional.locals, 1);
        let peak = |program: &Program<'_>| {
            let memory = MemoryBudget::new(1 << 20);
            let scope = EvaluationScope::new(&memory).unwrap();
            let context = EvalContext { allocation: Some(&scope), ..Default::default() };
            let pattern = program.pattern(&context).unwrap();
            assert_eq!(pattern.view().units().collect::<Vec<_>>(), [u16::from(b'a')]);
            memory.peak_bytes()
        };
        assert_eq!(peak(&conditional), peak(&direct) + std::mem::size_of::<HostValue<'_>>());
    }

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
    #[test]
    fn non_ifn_calls_fail_before_argument_budget_admission() {
        let raw = [raw_literal(Value::Bool(false)),raw_literal(Value::I64(1)),raw_literal(Value::Utf8("x".into())),raw_character('x'),raw_list(),raw_anonymous(raw_list())];
        let mut functions = raw.iter().map(|form|HostValue::Constant(form.view())).collect::<Vec<_>>();
        functions.extend([HostValue::Bool(true),HostValue::Text(JavaString::copy_utf8_untracked("x").unwrap()),HostValue::Number(Arc::new(Number::Long(1)))]);
        let argument = raw_literal(Value::I64(1));
        for function in functions {
            let memory = MemoryBudget::new(4096);
            let expression = Expression::Call { function: Box::new(Expression::Local { index: 0,emission: Emission::Unknown }),arguments: vec![Expression::Constant(argument.view())] };
            let mut locals = [function];
            {
                let scope = EvaluationScope::new(&memory).unwrap();
                let context = EvalContext { allocation: Some(&scope),..Default::default() };
                let error = expression.evaluate(&mut locals,&context).err().unwrap();
                assert_eq!(error.message.as_deref(),Some("Host value is not callable"));
                assert_eq!(memory.used_bytes(),4096);
                assert_eq!(memory.peak_bytes(),4096);
            }
            assert_eq!(memory.used_bytes(),0);
        }
    }
    #[test]
    fn null_and_ifn_values_reach_argument_budget_admission() {
        let raw = [raw_literal(Value::Null),raw_literal(Value::Keyword(rtdb_types::Keyword::from_name("k"))),raw_symbol("k"),raw_vector(),raw_map(),raw_set()];
        let mut functions = raw.iter().map(|form|HostValue::Constant(form.view())).collect::<Vec<_>>();
        functions.extend([HostValue::Nil,HostValue::Function(Function::Identity),HostValue::Vector(Vec::new().into())]);
        let argument = raw_literal(Value::I64(1));
        for function in functions {
            let memory = MemoryBudget::new(4096);
            let expression = Expression::Call { function: Box::new(Expression::Local { index: 0,emission: Emission::Unknown }),arguments: vec![Expression::Constant(argument.view())] };
            let mut locals = [function];
            {
                let scope = EvaluationScope::new(&memory).unwrap();
                let context = EvalContext { allocation: Some(&scope),..Default::default() };
                assert_eq!(expression.evaluate(&mut locals,&context).err().unwrap().code,"rtdb/memory-budget-exceeded");
                assert_eq!(memory.used_bytes(),4096);
                assert_eq!(memory.peak_bytes(),4096);
            }
            assert_eq!(memory.used_bytes(),0);
        }
    }

    fn index(value: i64) -> HostValue<'static> { HostValue::Number(Arc::new(Number::Long(value))) }

    #[test]
    fn substring_keeps_each_half_and_the_complete_supplementary_pair() {
        let source = raw_literal(Value::Utf8("😀".into()));
        let context = EvalContext::default();
        for (start,end,expected) in [(0,1,vec![0xd83d]),(1,2,vec![0xde00]),(0,2,vec![0xd83d,0xde00])] {
            let result = Function::Subs.apply(vec![HostValue::Constant(source.view()),index(start),index(end)],&context).unwrap();
            assert_eq!(result.string().unwrap().units().collect::<Vec<_>>(),expected);
        }
        let result = Function::Subs.apply(vec![HostValue::Constant(source.view()),index(1)],&context).unwrap();
        assert_eq!(result.string().unwrap().units().collect::<Vec<_>>(),[0xde00]);
    }

    #[test]
    fn str_and_name_preserve_unmatched_units_and_join_halves_before_compilation() {
        let context = EvalContext::default();
        let high = HostValue::Text(JavaString::copy_utf16_untracked(&[0xd83d]).unwrap());
        let low = HostValue::Text(JavaString::copy_utf16_untracked(&[0xde00]).unwrap());
        let result = Function::Str.apply(vec![high.clone(),low],&context).unwrap();
        assert_eq!(result.string().unwrap().units().collect::<Vec<_>>(),[0xd83d,0xde00]);
        let result = Function::Str.apply(vec![high.clone()],&context).unwrap();
        assert_eq!(result.string().unwrap().units().collect::<Vec<_>>(),[0xd83d]);
        let result = Function::Name.apply(vec![high],&context).unwrap();
        assert_eq!(result.string().unwrap().units().collect::<Vec<_>>(),[0xd83d]);
    }

    #[test]
    fn computed_pattern_retains_its_own_charge_after_the_creator_scope_and_program_drop() {
        let source = raw_build(|builder| {
            builder.begin_list()?; builder.symbol("subs")?; builder.literal(&Value::Utf8("😀".into()))?;
            builder.literal(&Value::I64(0))?; builder.literal(&Value::I64(1))?; builder.end()
        });
        let memory = MemoryBudget::new(1 << 20);
        let pattern = {
            let scope = EvaluationScope::new(&memory).unwrap();
            let context = EvalContext { allocation: Some(&scope),..Default::default() };
            Program::compile(&source,&context).unwrap().pattern(&context).unwrap()
        };
        assert_eq!(pattern.view().units().collect::<Vec<_>>(),[0xd83d]);
        let retained = JavaString::allocation_bound_utf16(&[0xd83d]).unwrap();
        assert_eq!(memory.used_bytes(),retained);
        let PatternText::Computed(value) = pattern else { panic!("computed pattern") };
        let clone = value.clone();
        drop(value);
        assert_eq!(memory.used_bytes(),retained);
        assert_eq!(clone.view().units().collect::<Vec<_>>(),[0xd83d]);
        drop(clone);
        assert_eq!(memory.used_bytes(),0);
    }

    #[test]
    fn retained_unit_copy_is_admitted_while_the_temporary_scope_charge_is_still_held() {
        let retained = JavaString::allocation_bound_utf16(&[0xd83d]).unwrap();
        let work = 4096 + 2 * 3 + 64 + 256;
        for available in [retained - 1,retained] {
            let memory = MemoryBudget::new(work + available);
            let output = {
                let scope = EvaluationScope::new(&memory).unwrap();
                let context = EvalContext { allocation: Some(&scope),..Default::default() };
                let result = text(JavaText::from_utf16(&[0xd83d]),&context);
                if available < retained {
                    assert_eq!(result.unwrap_err().code,"rtdb/memory-budget-exceeded");
                    assert_eq!(memory.used_bytes(),work);
                    None
                } else {
                    let output = result.unwrap();
                    assert_eq!(memory.used_bytes(),work + retained);
                    Some(output)
                }
            };
            assert_eq!(memory.used_bytes(),if output.is_some() { retained } else { 0 });
            drop(output);
            assert_eq!(memory.used_bytes(),0);
        }
    }

    #[test]
    fn str_keeps_the_scalar_text_limit_and_finishes_argument_rendering_before_the_final_guard() {
        let context = EvalContext::default();
        assert_eq!(text_bytes(JavaText::from_utf8("aé😀")).unwrap(),7);
        assert_eq!(text_bytes(JavaText::from_utf16(&[0xd83d])).unwrap(),3);
        assert_eq!(text_bytes(JavaText::from_utf16(&[0xd83d,0xde00])).unwrap(),4);
        assert!(reserve_text(16 * 1024 * 1024,&context).is_ok());
        let refused = reserve_text(16 * 1024 * 1024 + 1,&context).unwrap_err();
        assert_eq!(refused.code,"rtdb.expr/regex-host-limit");
        assert!(refused.is(rtdb_types::AnomalyKind::ProgramLimitExceeded));
        let source = JavaString::copy_utf8_untracked(&"a".repeat(8 * 1024 * 1024 + 1)).unwrap();
        let args = vec![HostValue::Text(source.clone()),HostValue::Text(source.clone()),HostValue::Function(Function::Identity)];
        assert_eq!(Function::Str.apply(args,&context).err().unwrap().message.as_deref(),Some("Host object string rendering is not implemented for this value"));
        assert_eq!(Function::Str.apply(vec![HostValue::Text(source.clone()),HostValue::Text(source)],&context).err().unwrap().code,"rtdb.expr/regex-host-limit");
    }

    #[test]
    fn substring_string_and_index_checks_keep_their_order_and_character_domain() {
        let context = EvalContext::default();
        let source = raw_literal(Value::Utf8("x".into()));
        assert_eq!(Function::Subs.apply(vec![HostValue::Bool(false),HostValue::Nil],&context).err().unwrap().message.as_deref(),Some("Host value is not a String"));
        assert_eq!(Function::Subs.apply(vec![HostValue::Constant(source.view()),HostValue::Bool(false)],&context).err().unwrap().message.as_deref(),Some("Host value is not a Number"));
        assert_eq!(Function::Subs.apply(vec![HostValue::Constant(source.view()),index(-1)],&context).err().unwrap().message.as_deref(),Some("Host substring indexes are out of range"));
        assert_eq!(Function::Subs.apply(vec![HostValue::Constant(source.view()),index(0),HostValue::Bool(false)],&context).err().unwrap().message.as_deref(),Some("Host value is not a Number"));
        let character = raw_character('😀');
        assert_eq!(numeric_cast(Cast::Int,HostValue::Constant(character.view()),false,&context).err().unwrap().message.as_deref(),Some("Host Character requires a single UTF16 code unit"));
    }

    enum S { Text(&'static str),Symbol(&'static str),Character(char),Int(i64),Float(f64),Nil,Keyword(&'static str),Big(&'static str),List(Vec<S>),Vector(Vec<S>),Map(Vec<S>) }
    fn emit(builder: &mut RegexFormBuilder,form: &S) -> Result<()> {
        match form {
            S::Text(value) => builder.literal(&Value::Utf8((*value).into())),S::Symbol(value) => builder.symbol(value),S::Character(value) => builder.character(*value),
            S::Int(value) => builder.literal(&Value::I64(*value)),S::Float(value) => builder.literal(&Value::F64(*value)),S::Nil => builder.literal(&Value::Null),
            S::Keyword(value) => builder.literal(&Value::Keyword(rtdb_types::Keyword::from_name(*value))),S::Big(value) => builder.big_integer(value),
            S::List(forms) | S::Vector(forms) | S::Map(forms) => {
                match form { S::List(_) => builder.begin_list()?,S::Vector(_) => builder.begin_vector()?,_ => builder.begin_map()? }
                for form in forms { emit(builder,form)?; }
                builder.end()
            }
        }
    }
    fn call(name: &'static str,args: Vec<S>) -> S { S::List(std::iter::once(S::Symbol(name)).chain(args).collect()) }
    fn quoted(form: S) -> S { call("quote",vec![form]) }
    fn host_pattern(form: S) -> Result<String> {
        let form = raw_build(|builder| emit(builder,&form));
        let context = EvalContext::default();
        let pattern = Program::compile(&form,&context)?.pattern(&context)?;
        Ok(String::from_utf16_lossy(&pattern.view().units().collect::<Vec<_>>()))
    }
    fn host_failure(form: S) -> Anomaly { host_pattern(form).unwrap_err() }

    #[test]
    fn pattern_quote_requires_the_qualified_class_and_reopens_quotes_around_end_markers() {
        assert_eq!(host_pattern(call("java.util.regex.Pattern/quote",vec![S::Text("a.b")])).unwrap(),r"\Qa.b\E");
        assert_eq!(host_pattern(call("java.util.regex.Pattern/quote",vec![S::Text(r"a\Eb\E")])).unwrap(),r"\Qa\E\\E\Qb\E\\E\Q\E");
        assert_eq!(host_failure(call("Pattern/quote",vec![S::Text("a")])).code,"rtdb.expr/regex-host-form");
        assert_eq!(host_failure(call("java.util.regex.Pattern/quote",vec![S::Nil])).message.as_deref(),Some("Host value is not a String"));
    }

    #[test]
    fn format_admits_only_locale_independent_string_and_integer_conversions() {
        assert_eq!(host_pattern(call("format",vec![S::Text("%s-%d%%"),S::Text("a"),S::Int(-5)])).unwrap(),"a--5%");
        assert_eq!(host_pattern(call("clojure.core/format",vec![S::Text("%s|%d|%s"),S::Nil,S::Nil,S::Vector(vec![S::Int(1),S::Text("x")])])).unwrap(),r#"null|null|[1 "x"]"#);
        assert_eq!(host_pattern(call("format",vec![S::Text("%s"),S::Big("12")])).unwrap(),"12");
        for (format,argument) in [("%5s",S::Text("a")),("%1$s",S::Text("a")),("%x",S::Int(1)),("%n",S::Nil)] {
            assert_eq!(host_failure(call("format",vec![S::Text(format),argument])).code,"rtdb.expr/regex-host-form","{format}");
        }
        assert_eq!(host_failure(call("format",vec![S::Text("%d"),S::Float(1.5)])).code,"xtdb.error/unknown");
        assert_eq!(host_failure(call("format",vec![S::Text("%d"),S::Big("1")])).code,"xtdb.error/unknown");
        assert_eq!(host_failure(call("format",vec![S::Text("%s")])).message.as_deref(),Some("Host format is missing an argument"));
        assert_eq!(host_failure(call("format",vec![S::Text("a%")])).message.as_deref(),Some("Host format specifier is incomplete"));
    }

    #[test]
    fn apply_spreads_vectors_quoted_lists_and_nil_into_host_functions() {
        assert_eq!(host_pattern(call("apply",vec![S::Symbol("str"),S::Text("a"),S::Vector(vec![S::Text("b"),S::Int(1),S::Nil])])).unwrap(),"ab1");
        assert_eq!(host_pattern(call("apply",vec![S::Symbol("str"),quoted(S::List(vec![S::Big("1"),S::Float(2.0)]))])).unwrap(),"12.0");
        assert_eq!(host_pattern(call("apply",vec![S::Symbol("str"),S::Text("a"),S::Nil])).unwrap(),"a");
        assert_eq!(host_failure(call("apply",vec![S::Symbol("str"),S::Text("abc")])).code,"rtdb.expr/regex-host-form");
        assert_eq!(host_failure(call("apply",vec![S::Symbol("str"),S::Int(1)])).message.as_deref(),Some("Host value is not a sequence"));
    }

    #[test]
    fn clojure_string_functions_follow_the_java_string_operations_they_delegate_to() {
        assert_eq!(host_pattern(call("clojure.string/join",vec![S::Text(","),S::Vector(vec![S::Text("a"),S::Int(1),S::Nil,S::Text("b")])])).unwrap(),"a,1,,b");
        assert_eq!(host_pattern(call("clojure.string/join",vec![S::Vector(vec![S::Text("a"),S::Keyword("k")])])).unwrap(),"a:k");
        assert_eq!(host_pattern(call("clojure.string/upper-case",vec![S::Text("straße")])).unwrap(),"STRASSE");
        assert_eq!(host_pattern(call("clojure.string/lower-case",vec![S::Text("ΟΔΟΣ İ")])).unwrap(),"οδος i\u{307}");
        assert_eq!(host_pattern(call("clojure.string/replace",vec![S::Text("a.b.c"),S::Text("."),S::Text(r"\.")])).unwrap(),r"a\.b\.c");
        assert_eq!(host_pattern(call("clojure.string/replace",vec![S::Text("aaa"),S::Text("aa"),S::Text("b")])).unwrap(),"ba");
        assert_eq!(host_pattern(call("clojure.string/replace",vec![S::Text("ab"),S::Text(""),S::Text("-")])).unwrap(),"-a-b-");
        assert_eq!(host_pattern(call("clojure.string/replace",vec![S::Text("a.b"),S::Character('.'),S::Character('|')])).unwrap(),"a|b");
        assert_eq!(host_failure(call("clojure.string/replace",vec![S::Text("a"),S::Int(1),S::Text("b")])).message.as_deref(),Some("Invalid match arg"));
        assert_eq!(host_failure(call("clojure.string/replace",vec![S::Text("a"),S::Character('a'),S::Text("b")])).message.as_deref(),Some("Host replacement is not a Character"));
        assert_eq!(host_pattern(call("clojure.string/trim",vec![S::Text(" \u{2007}a b\t\u{3000}\u{1f}")])).unwrap(),"\u{2007}a b");
        assert_eq!(host_pattern(call("clojure.string/trim",vec![S::Text(" \n ")])).unwrap(),"");
        assert_eq!(host_failure(call("clojure.string/upper-case",vec![S::Int(1)])).message.as_deref(),Some("Host value is not a String"));
        assert_eq!(host_failure(call("str/join",vec![S::Vector(vec![])])).code,"rtdb.expr/regex-host-form");
    }

    #[test]
    fn str_of_collections_uses_their_readable_print_form() {
        assert_eq!(host_pattern(call("str",vec![S::Vector(vec![S::Int(1),S::Text("x\n\"")])])).unwrap(),r#"[1 "x\n\""]"#);
        let form = S::Vector(vec![S::Symbol("a"),S::Text("b"),S::Character('c'),S::Character(' '),S::Big("1"),S::Float(f64::NAN),S::Nil,S::Keyword("k"),S::List(vec![]),S::Vector(vec![S::Int(2)])]);
        assert_eq!(host_pattern(call("str",vec![quoted(form)])).unwrap(),r#"[a "b" \c \space 1N ##NaN nil :k () [2]]"#);
        assert_eq!(host_pattern(call("str",vec![S::Map(vec![]),S::Text("-"),quoted(S::List(vec![S::Int(1),S::Int(2)]))])).unwrap(),"{}-(1 2)");
        assert_eq!(host_failure(call("str",vec![quoted(S::Map(vec![S::Keyword("a"),S::Int(1)]))])).code,"rtdb.expr/regex-host-form");
        assert_eq!(host_failure(call("str",vec![S::Vector(vec![S::Symbol("str")])])).code,"rtdb.expr/regex-host-form");
    }
}
