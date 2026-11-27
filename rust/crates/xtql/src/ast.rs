use crate::{Edn, EdnKind, FormAllocation, error, literal};
use rtdb_types::{MemoryBudget, Result, TableRef, Value};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Expr {
    Literal(Value),
    /// Resolves a function parameter before an in-scope logic variable.
    Variable(String),
    /// Binding shorthand always names a logic variable, including inside a query function.
    LogicVariable(String),
    Call { function: String, arguments: Vec<Expr> },
    RegexReplace { source: Box<Expr>,recipe: rtdb_plan::RegexReplacement },
    Cast { expr: Box<Expr>, target: rtdb_plan::CastTarget },
    Get { object: Box<Expr>, field: String }, List(Vec<Expr>), Set(Vec<Expr>), Map(Vec<(String, Expr)>),
    Subquery { kind: SubqueryKind, query: Box<Query>, arguments: Option<Vec<Expr>> },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubqueryKind { Scalar, Exists, Pull, PullMany }

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Binding { pub name: String, pub expression: Expr }

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum TemporalFilter { All, At(Expr), In { from: Option<Expr>, to: Option<Expr> } }

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct From {
    pub database: Option<String>, pub table: TableRef, pub bindings: Vec<Binding>, pub star: bool,
    pub valid_time: Option<TemporalFilter>, pub system_time: Option<TemporalFilter>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum RelationInput { Documents(Vec<Vec<(String, Expr)>>), Parameter(String) }

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Query {
    Function { parameters: Vec<String>, body: Box<Query> },
    Pipeline { head: Box<Query>, tails: Vec<Tail> },
    From(From), Rel { input: RelationInput, bindings: Vec<Binding> }, Unify(Vec<Clause>), UnionAll(Vec<Query>),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Clause {
    Source(Query), Where(Vec<Expr>), With(Vec<Binding>), Unnest(Binding),
    Join { left: bool, query: Query, arguments: Option<Vec<Expr>>, bindings: Vec<Binding> },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OrderSpec { pub expression: Expr, pub descending: bool, pub nulls_first: Option<bool> }

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Tail {
    Where(Vec<Expr>), With(Vec<Binding>), Without(Vec<String>), Return(Vec<Binding>), Aggregate(Vec<Binding>),
    Unnest(Binding), OrderBy(Vec<OrderSpec>), Limit(Expr), Offset(Expr),
}

/// Parses query syntax without consulting a catalog or assigning parameter positions.
pub fn parse_query(form: &Edn) -> Result<Query> {
    parse_query_with(form, FormAllocation::Untracked)
}

/// Parses query syntax with raw metadata retained in the supplied budget; no catalog is read.
pub fn parse_query_in(form: &Edn, memory: &std::sync::Arc<MemoryBudget>) -> Result<Query> {
    parse_query_with(form, FormAllocation::Tracked(memory))
}

pub(crate) fn parse_query_with(form: &Edn, forms: FormAllocation<'_>) -> Result<Query> {
    Syntax { forms, dialect: Dialect::Xtql, anonymous: false }.parse_query(form)
}

/// Parses a logical-plan scalar expression, under the reference expression parser's rules rather than XTQL's.
pub(crate) fn parse_expr_in(form: &Edn, memory: &std::sync::Arc<MemoryBudget>) -> Result<Expr> {
    Syntax { forms: FormAllocation::Tracked(memory), dialect: Dialect::Ra, anonymous: false }.parse_expr(form)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Dialect { Xtql, Ra }

struct Syntax<'a> { forms: FormAllocation<'a>, dialect: Dialect, anonymous: bool }

#[derive(Clone, Copy)]
enum BindingSpec { Output, Column, Variable }

impl Syntax<'_> {
fn symbol_name(&self, name: &str) -> String {
    if self.anonymous && name.starts_with('%') {
        return format!("%{}", anonymous_index(name).expect("anonymous parameters have been validated"));
    }
    name.to_owned()
}

fn parse_query(&self, form: &Edn) -> Result<Query> {
    if let EdnKind::AnonymousFunction(body) = &form.kind {
        let mut maximum = 0;
        anonymous_parameters(body, &mut maximum)?;
        let parameters = (1..=maximum).map(|index| format!("%{index}")).collect();
        let syntax = Syntax { anonymous: true, ..*self };
        return Ok(Query::Function { parameters, body: Box::new(syntax.parse_query(body)?) });
    }
    let (op, args) = form_list(form, "xtql/malformed-query")?;
    Ok(match op {
        "quote" if args.len() == 1 => return self.parse_query(&args[0]),
        "fn" | "fn*" => {
            if args.len() != 2 { return Err(error("xtql/malformed-fn-params", "Query functions require a parameter vector and one body")); }
            let parameters = args[0].vector().ok_or_else(|| error("xtql/malformed-fn-params", "Malformed fn params"))?
                .iter().map(|value| value.symbol().map(|name| self.symbol_name(name)).ok_or_else(|| error("xtql/malformed-fn-params", "Malformed fn params"))).collect::<Result<Vec<_>>>()?;
            if parameters.iter().collect::<BTreeSet<_>>().len() != parameters.len() { return Err(error("xtql/malformed-fn-params", "Function parameters must be distinct")); }
            Query::Function { parameters, body: Box::new(self.parse_query(&args[1])?) }
        }
        "->" => {
            let Some((head, tails)) = args.split_first() else { return Err(error("xtql/malformed-pipeline", "Pipeline must contain at least one operator")); };
            Query::Pipeline { head: Box::new(self.parse_query(head)?), tails: tails.iter().map(|tail| self.parse_tail(tail)).collect::<Result<_>>()? }
        }
        "from" => Query::From(self.parse_from(args)?),
        "rel" => {
            if args.len() != 2 { return Err(error("xtql/rel", "rel takes a relation and a binding vector")); }
            let input = match &args[0].kind {
                EdnKind::Symbol(name) => RelationInput::Parameter(self.symbol_name(name)),
                EdnKind::Vector(documents) => RelationInput::Documents(documents.iter().map(|document| {
                    let entries = document.map().ok_or_else(|| error("xtql/rel", "Explicit relation entries must be maps"))?;
                    entries.iter().map(|(key, value)| Ok((keyword(key, "xtql/rel")?.to_owned(), self.parse_expr(value)?))).collect::<Result<_>>()
                }).collect::<Result<_>>()?),
                _ => return Err(error("xtql/rel", "rel takes a parameter or explicit relation")),
            };
            Query::Rel { input, bindings: self.output_bindings(&args[1])? }
        }
        "unify" => {
            if args.is_empty() { return Err(error("xtql/malformed-unify", "Unify must contain at least one sub clause")); }
            Query::Unify(args.iter().map(|clause| self.parse_clause(clause)).collect::<Result<_>>()?)
        }
        "union-all" => {
            if args.is_empty() { return Err(error("xtql/malformed-union", "Union must contain at least one sub query")); }
            Query::UnionAll(args.iter().map(|query| self.parse_query(query)).collect::<Result<_>>()?)
        }
        _ => return Err(error("xtql/unknown-query-op", format!("Unknown query op: {op}"))),
    })
}

fn parse_expr(&self, form: &Edn) -> Result<Expr> {
    Ok(match &form.kind {
        EdnKind::Symbol(name) if name == "xtdb/end-of-time" => Expr::Literal(Value::Timestamp { value: i64::MAX, unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()) }),
        EdnKind::Symbol(name) => Expr::Variable(self.symbol_name(name)),
        EdnKind::List(_) => {
            let (op, args) = form_list(form, "xtql/malformed-call")?;
            match op {
                "regexp-replace" | "regexp_replace" => Expr::RegexReplace {
                    source: Box::new(args.first().map(|source| self.parse_expr(source)).transpose()?.unwrap_or(Expr::Literal(Value::Null))),
                    recipe: crate::regex_replace::metadata(args, self.forms)?,
                },
                "cast" => {
                    if !(2..=3).contains(&args.len()) { return Err(error("xtdb.expression/arity-error", "cast requires an expression, target type, and optional cast options")); }
                    Expr::Cast { expr: Box::new(self.parse_expr(&args[0])?), target: crate::cast::target(&args[1], args.get(2))? }
                }
                "cast-tstz" | "cast_tstz" | "cast-interval" | "cast_interval" => {
                    if !(1..=2).contains(&args.len()) { return Err(error("xtdb.expression/arity-error", "Temporal cast requires an expression and optional cast options")); }
                    let target = if matches!(op,"cast-tstz" | "cast_tstz") { crate::cast::session_timestamp(args.get(1))? } else { crate::cast::interval_alias(args.get(1))? };
                    Expr::Cast { expr: Box::new(self.parse_expr(&args[0])?), target }
                }
                "." if self.dialect == Dialect::Ra => {
                    if args.len() != 2 { return Err(error("xtdb.expression/arity-error", "'.' expects 2 args")); }
                    let field = args[1].symbol().or_else(|| args[1].keyword()).ok_or_else(|| error("xtdb.expression/arity-error", "'.' expects symbol or keyword fields"))?;
                    Expr::Get { object: Box::new(self.parse_expr(&args[0])?), field: field.into() }
                }
                "." => {
                    if args.len() != 2 || matches!(args[0].kind,EdnKind::Nil | EdnKind::Bool(false)) || args[1].symbol().is_none() { return Err(error("xtql/malformed-get", "Malformed get")); }
                    Expr::Get { object: Box::new(self.parse_expr(&args[0])?), field: args[1].symbol().unwrap_or_default().into() }
                }
                "q" | "exists?" | "pull" | "pull*" => {
                    if args.is_empty() || args.len() > 2 || (args.len() == 2 && args[1].map().is_none() && !matches!(args[1].kind, EdnKind::Nil)) {
                        return Err(error("xtql/malformed-subquery", "Malformed subquery"));
                    }
                    let (query, arguments) = self.query_arguments(&args[0])?;
                    let kind = match op { "q" => SubqueryKind::Scalar, "exists?" => SubqueryKind::Exists, "pull" => SubqueryKind::Pull, _ => SubqueryKind::PullMany };
                    Expr::Subquery { kind, query: Box::new(query), arguments }
                }
                _ => Expr::Call { function: op.into(), arguments: args.iter().map(|value| self.parse_expr(value)).collect::<Result<_>>()? },
            }
        }
        EdnKind::Vector(values) => Expr::List(values.iter().map(|value| self.parse_expr(value)).collect::<Result<_>>()?),
        EdnKind::Set(values) => Expr::Set(values.iter().map(|value| self.parse_expr(value)).collect::<Result<_>>()?),
        EdnKind::Map(entries) => {
            let mut fields = Vec::<(String, Expr)>::new();
            for (key, value) in entries {
                let name = if self.dialect == Dialect::Xtql {
                    expression_map_key(key)?
                } else {
                    keyword(key, "xtql/malformed-col-spec")?.into()
                };
                let value = self.parse_expr(value)?;
                if let Some((_, previous)) = fields.iter_mut().find(|(key, _)| key.as_str() == name.as_str()) {
                    *previous = value;
                } else {
                    fields.push((name, value));
                }
            }
            Expr::Map(fields)
        }
        _ => Expr::Literal(literal::value(form)?),
    })
}

fn parse_from(&self, args: &[Edn]) -> Result<From> {
    let Some(table_form) = args.first() else { return Err(error("xtql/malformed-from", "`table` must be a keyword or map")); };
    if args.len() > 2 { return Err(error("xtql/malformed-table-opts", "Malformed table opts")); }
    let (database, table) = if table_form.map().is_some() {
        let table = table_form.option("table").filter(|form| !matches!(form.kind, EdnKind::Nil | EdnKind::Bool(false)))
            .ok_or_else(|| error("xtql/malformed-from", "`table` key must be present in `table` map"))?;
        let database = table_form.option("db").filter(|form| !matches!(form.kind, EdnKind::Nil)).map(|db| match &db.kind {
            EdnKind::String(name) => Ok(name.clone()),
            EdnKind::Keyword(name) | EdnKind::Symbol(name) => Ok(rtdb_plan::Identifier::unquoted(name).normalized()),
            _ => Err(error("xtql/malformed-from", "Database name must be a string, keyword or symbol")),
        }).transpose()?;
        (database, table_ref(table, true)?)
    } else { (None, table_ref(table_form, false)?) };
    let options = args.get(1).filter(|form| !matches!(form.kind, EdnKind::Nil)).ok_or_else(|| error("xtql/missing-bind", "Missing bind"))?;
    let (bindings, valid_time, system_time) = if options.map().is_some() {
        check_options(options, &["bind", "for-valid-time", "for-system-time"])?;
        let bind = options.option("bind").filter(|form| !matches!(form.kind, EdnKind::Nil))
            .ok_or_else(|| error("xtql/missing-bind", "Missing bind"))?;
        let bindings = bind.vector().ok_or_else(|| error("xtql/malformed-bind", "Malformed bind"))?;
        let temporal = |key| options.option(key).filter(|form| !matches!(form.kind, EdnKind::Nil))
            .map(|form| self.parse_temporal(form)).transpose();
        (bindings, temporal("for-valid-time")?, temporal("for-system-time")?)
    } else {
        let bindings = options.vector().ok_or_else(|| error("xtql/malformed-table-opts", "Malformed table opts"))?;
        (bindings, None, None)
    };
    let star = bindings.iter().any(|value| value.symbol() == Some("*"));
    let bindings = self.binding_forms(bindings.iter().filter(|value| value.symbol() != Some("*")), BindingSpec::Output)?;
    Ok(From { database, table, bindings, star, valid_time, system_time })
}
}

pub(crate) fn table_ref(form: &Edn, in_options: bool) -> Result<TableRef> {
    match &form.kind {
        EdnKind::Keyword(name) => {
            let (schema, table) = name.split_once('/').unwrap_or(("public", name));
            Ok(TableRef::new(schema.to_lowercase().replace('-', "_"), table.to_lowercase().replace('-', "_")))
        }
        EdnKind::Tagged { tag, value } if tag == "xt/table" => table_ref(value, true),
        EdnKind::String(name) | EdnKind::Symbol(name) if in_options => {
            let (schema, table) = name.split_once('/').unwrap_or(("public", name));
            Ok(TableRef::new(schema, table))
        }
        _ => Err(error("xtql/malformed-from", "table must be a keyword, table reference, or option map")),
    }
}

impl Syntax<'_> {
fn parse_tail(&self, form: &Edn) -> Result<Tail> {
    let (op, args) = form_list(form, "xtql/malformed-query-tail")?;
    Ok(match op {
        "where" => Tail::Where(self.predicates(args)?),
        "with" => Tail::With(self.binding_forms(args.iter(), BindingSpec::Column)?),
        "return" => Tail::Return(self.binding_forms(args.iter(), BindingSpec::Column)?),
        "aggregate" => Tail::Aggregate(self.binding_forms(args.iter(), BindingSpec::Column)?),
        "without" => Tail::Without(args.iter().map(|arg| arg.keyword().map(str::to_owned)
            .ok_or_else(|| error("xtql/malformed-without", "Columns must be keywords in without"))).collect::<Result<_>>()?),
        "unnest" => Tail::Unnest(self.unnest(args, BindingSpec::Column)?),
        "order-by" => Tail::OrderBy(args.iter().map(|arg| {
            let (expression, descending, nulls_first) = if arg.map().is_some() {
                check_options(arg, &["val", "dir", "nulls"])?;
                let expression = arg.option("val").ok_or_else(|| error("xtql/order-by-val-missing", "order-by :val missing"))?;
                let descending = match arg.option("dir").map(|value| &value.kind) {
                    None | Some(EdnKind::Nil) => false,
                    Some(EdnKind::Keyword(value)) if value == "asc" => false,
                    Some(EdnKind::Keyword(value)) if value == "desc" => true,
                    _ => return Err(error("xtql/malformed-order-by-direction", "Malformed order-by direction")),
                };
                let nulls_first = match arg.option("nulls").map(|value| &value.kind) {
                    None | Some(EdnKind::Nil) => None,
                    Some(EdnKind::Keyword(value)) if value == "first" => Some(true),
                    Some(EdnKind::Keyword(value)) if value == "last" => Some(false),
                    _ => return Err(error("xtql/malformed-order-by-nulls", "Malformed order-by nulls")),
                };
                (self.parse_expr(expression)?, descending, nulls_first)
            } else { (self.parse_expr(arg)?, false, None) };
            Ok(OrderSpec { expression, descending, nulls_first })
        }).collect::<Result<_>>()?),
        "limit" | "offset" => {
            if args.len() != 1 {
                return Err(if op == "limit" { error("xtql/limit", "Limit can only take a single value") }
                    else { error("xtql/offset", "Offset can only take a single value") });
            }
            if op == "limit" { Tail::Limit(self.parse_expr(&args[0])?) } else { Tail::Offset(self.parse_expr(&args[0])?) }
        }
        _ => return Err(error("xtql/unknown-query-tail", format!("Unknown query tail: {op}"))),
    })
}

fn parse_clause(&self, form: &Edn) -> Result<Clause> {
    let (op, args) = form_list(form, "xtql/malformed-unify-clause")?;
    Ok(match op {
        "from" | "rel" => Clause::Source(self.parse_query(form)?),
        "where" => Clause::Where(self.predicates(args)?),
        "with" => Clause::With(self.binding_forms(args.iter(), BindingSpec::Variable)?),
        "unnest" => Clause::Unnest(self.unnest(args, BindingSpec::Variable)?),
        "join" | "left-join" => {
            if !(1..=2).contains(&args.len()) { return Err(error("xtql/malformed-bind", "Join requires a query and optional binding vector")); }
            let bind = args.get(1).filter(|form| !matches!(form.kind, EdnKind::Nil));
            if bind.is_some_and(|form| form.vector().is_none()) {
                return Err(error("xtql/malformed-bind", "Malformed bind"));
            }
            let (query, arguments) = self.query_arguments(&args[0])?;
            let bindings = bind.map(|form| self.output_bindings(form)).transpose()?.unwrap_or_default();
            Clause::Join { left: op == "left-join", query, arguments, bindings }
        }
        _ => return Err(error("xtql/unknown-unify-clause", format!("Unknown unify clause: {op}"))),
    })
}

fn query_arguments(&self, form: &Edn) -> Result<(Query, Option<Vec<Expr>>)> {
    let (query, arguments) = if let Some(forms) = form.vector() {
        let Some((query, arguments)) = forms.split_first() else { return Err(error("xtql/malformed-subquery", "Empty query application")); };
        (self.parse_query(query)?, Some(arguments.iter().map(|argument| self.parse_expr(argument)).collect::<Result<Vec<_>>>()?))
    } else { (self.parse_query(form)?, None) };
    let arity = if let Query::Function { parameters, .. } = &query { parameters.len() } else { 0 };
    if arguments.as_ref().is_some_and(|arguments| arguments.len() != arity) { return Err(error("xtql/subquery-arity-error", "Subquery arity error")); }
    Ok((query, arguments))
}

fn predicates(&self, args: &[Edn]) -> Result<Vec<Expr>> {
    if args.is_empty() { return Err(error("xtql/malformed-where", "Where must contain at least one predicate")); }
    args.iter().map(|value| self.parse_expr(value)).collect()
}

fn output_bindings(&self, form: &Edn) -> Result<Vec<Binding>> {
    self.binding_forms(form.vector().ok_or_else(|| error("xtql/malformed-bind", "Malformed bind"))?.iter(), BindingSpec::Output)
}

fn binding_forms<'a>(&self, forms: impl IntoIterator<Item = &'a Edn>, spec: BindingSpec) -> Result<Vec<Binding>> {
    let mut result = Vec::new();
    for form in forms {
        match &form.kind {
            EdnKind::Symbol(name) if !matches!(spec, BindingSpec::Variable) => {
                let name = self.symbol_name(name);
                result.push(Binding { expression: Expr::LogicVariable(name.clone()), name });
            }
            EdnKind::Map(entries) => for (key, value) in entries {
                let name = match spec {
                    BindingSpec::Variable => key.symbol().map(|name| self.symbol_name(name))
                        .ok_or_else(|| error("xtql/malformed-var-spec", "Attribute in var spec must be symbol")),
                    BindingSpec::Column => key.keyword().map(str::to_owned)
                        .ok_or_else(|| error("xtql/malformed-col-spec", "Attribute in col spec must be keyword")),
                    BindingSpec::Output => key.keyword().map(str::to_owned)
                        .ok_or_else(|| error("xtql/malformed-bind-spec", "Attribute in bind spec must be keyword")),
                }?;
                result.push(Binding { name, expression: self.parse_expr(value)? });
            },
            _ => return Err(match spec {
                BindingSpec::Variable => error("xtql/malformed-var-spec", "Var specs must be pairs of bindings"),
                BindingSpec::Column => error("xtql/malformed-col-spec", "Short form of col spec must be a symbol"),
                BindingSpec::Output => error("xtql/malformed-bind-spec", "Bind spec must be a symbol or a map"),
            }),
        }
    }
    Ok(result)
}

fn unnest(&self, args: &[Edn], spec: BindingSpec) -> Result<Binding> {
    if args.len() != 1 || args[0].map().is_none_or(|entries| entries.len() != 1) { return Err(error("xtql/unnest", "Unnest takes only a single binding")); }
    self.binding_forms(args.iter(), spec)?.pop().ok_or_else(|| error("xtql/unnest", "Missing unnest binding"))
}

fn parse_temporal(&self, form: &Edn) -> Result<TemporalFilter> {
    if form.keyword() == Some("all-time") { return Ok(TemporalFilter::All); }
    let (op, args) = form_list(form, "xtql/malformed-temporal-filter")?;
    Ok(match (op, args) {
        ("at", [at]) => TemporalFilter::At(self.temporal_time(at)?.unwrap_or_else(current_time)),
        ("in", [from, to]) => TemporalFilter::In { from: self.temporal_time(from)?, to: self.temporal_time(to)? },
        ("from", [from]) => TemporalFilter::In { from: self.temporal_time(from)?, to: None },
        ("to", [to]) => TemporalFilter::In { from: None, to: self.temporal_time(to)? },
        _ => return Err(error("xtql/malformed-temporal-filter", "Malformed temporal filter")),
    })
}

/// A temporal filter bound; `:now` is the query's current time, as the reference scan reads it.
fn temporal_time(&self, form: &Edn) -> Result<Option<Expr>> {
    if form.keyword() == Some("now") { return Ok(Some(current_time())); }
    self.optional_expr(form)
}

fn optional_expr(&self, form: &Edn) -> Result<Option<Expr>> { if matches!(form.kind, EdnKind::Nil) { Ok(None) } else { self.parse_expr(form).map(Some) } }
}
fn current_time() -> Expr { Expr::Call { function: "current-timestamp".into(), arguments: Vec::new() } }
fn expression_map_key(form: &Edn) -> Result<String> {
    if let Some(name) = form.keyword() { return Ok(name.to_owned()); }
    let text = crate::cast::field_name(form)?;
    let mut chars = text.chars();
    if !chars.next().is_some_and(|first| first.len_utf16() == 1) {
        return Err(error("xtql/malformed-col-spec", "Expression map key must have a removable leading UTF-16 code unit"));
    }
    Ok(chars.as_str().to_owned())
}

fn keyword<'a>(form: &'a Edn, code: &str) -> Result<&'a str> { form.keyword().ok_or_else(|| error(code, "Expected a keyword")) }
fn check_options(form: &Edn, allowed: &[&str]) -> Result<()> {
    for (key, _) in form.map().unwrap_or_default() {
        if !key.keyword().is_some_and(|key| allowed.contains(&key)) { return Err(error("invalid-opt-keys", "Invalid keys provided to option map")); }
    }
    Ok(())
}
fn form_list<'a>(form: &'a Edn, code: &str) -> Result<(&'a str, &'a [Edn])> {
    if let EdnKind::List(values) = &form.kind {
        if let Some((head, rest)) = values.split_first() {
            if let Some(symbol) = head.symbol() { return Ok((symbol, rest)); }
        }
    }
    Err(error(code, "Expected a nonempty list beginning with an operator symbol"))
}
fn anonymous_parameters(form: &Edn, maximum: &mut usize) -> Result<()> {
    match &form.kind {
        EdnKind::Symbol(name) if name.starts_with('%') => {
            let index = anonymous_index(name)?;
            *maximum = (*maximum).max(index);
        }
        EdnKind::List(values) | EdnKind::Vector(values) | EdnKind::Set(values) => for value in values { anonymous_parameters(value, maximum)?; },
        EdnKind::Map(entries) => for (key, value) in entries { anonymous_parameters(key, maximum)?; anonymous_parameters(value, maximum)?; },
        EdnKind::Tagged { value, .. } => anonymous_parameters(value, maximum)?,
        EdnKind::AnonymousFunction(_) => return Err(error("xtql/malformed-fn-params", "Nested anonymous query functions are not supported by the EDN reader")),
        _ => {}
    }
    Ok(())
}

fn anonymous_index(name: &str) -> Result<usize> {
    if name == "%" { return Ok(1); }
    let index = name[1..].parse::<usize>().map_err(|_| error("xtql/malformed-fn-params", "Anonymous query parameters must be positional"))?;
    if !(1..=1024).contains(&index) { return Err(error("xtql/malformed-fn-params", "Anonymous parameter index is outside 1..1024")); }
    Ok(index)
}
