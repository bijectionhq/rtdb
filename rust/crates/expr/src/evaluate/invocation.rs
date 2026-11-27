use std::sync::Arc;

use super::{EvalContext,ExpressionRow,Schema,Expr,Value,Result,Anomaly,expression_type,reader,overflow,validate_projection_value};
use rtdb_types::{ListBuilder,ListValue,LogicalType,ScalarType};
use crate::string::pattern::{CompiledReplacement,compile_replacement_pattern};
mod host;

struct Replacement<'a> { expression: &'a Expr,compiled: CompiledReplacement }
struct PendingBinding<'a> { expression: &'a Expr,recipe: &'a rtdb_plan::RegexReplacement,program: host::Program<'a> }

#[derive(Clone, Copy)]
pub(super) struct PreparedCall<'a> {
    expression: &'a Expr,
    pub(super) function: Option<crate::functions::ScalarFunction>,
    pub(super) dispatch_prepared: bool,
    pub(super) requires_reader: bool,
}

pub(super) struct PreparedList {
    pub(super) values: ListValue,
    pub(super) element: LogicalType,
    pub(super) type_bound: usize,
}

pub(super) struct Replacements<'a> {
    entries: Vec<Replacement<'a>>,
    calls: Vec<PreparedCall<'a>>,
    lists: std::collections::HashMap<usize, PreparedList>,
    types: std::sync::Mutex<std::collections::HashMap<usize,LogicalType>>,
    validated_calls: std::sync::Mutex<std::collections::HashSet<usize>>,
}

impl<'a> Replacements<'a> {
    pub(super) fn prepared_call(&self, expression: &Expr) -> Option<PreparedCall<'a>> {
        let address = expression as *const Expr as usize;
        self.calls.binary_search_by_key(&address, |entry| entry.expression as *const Expr as usize)
            .ok()
            .map(|index| self.calls[index])
    }

    pub(super) fn list(&self, expression: &Expr) -> Option<&PreparedList> {
        self.lists.get(&(expression as *const Expr as usize))
    }

    pub(super) fn validate_call(&self, expression: &Expr, name: &str, args: &[Expr]) -> Result<()> {
        let address = expression as *const Expr as usize;
        let mut calls = self.validated_calls.lock()
            .map_err(|_| Anomaly::fault("rtdb.expr/type-cache", "Expression type cache was poisoned"))?;
        if !calls.contains(&address) {
            rtdb_plan::function_type_with_args(name, args)?;
            calls.insert(address);
        }
        Ok(())
    }

    /// An emitted type depends on the invocation's fixed schema and parameter types, never on a row, so each subexpression's is derived once.
    pub(super) fn emitted_type(&self,expression: &Expr,derive: impl FnOnce() -> Result<LogicalType>) -> Result<LogicalType> {
        let address = expression as *const Expr as usize;
        if let Some(ty) = self.types.lock().map_err(|_|Anomaly::fault("rtdb.expr/type-cache","Expression type cache was poisoned"))?.get(&address) { return Ok(ty.clone()); }
        let ty = derive()?;
        self.types.lock().map_err(|_|Anomaly::fault("rtdb.expr/type-cache","Expression type cache was poisoned"))?.insert(address,ty.clone());
        Ok(ty)
    }

    pub(super) fn get(&self,expression: &Expr) -> Result<&CompiledReplacement> {
        let address = expression as *const Expr as usize;
        self.entries.binary_search_by_key(&address,|entry|entry.expression as *const Expr as usize)
            .map(|index|&self.entries[index].compiled)
            .map_err(|_|Anomaly::fault("rtdb.expr/replacement-binding","Replacement expression was not prepared for this invocation"))
    }
}

pub(super) fn resolve_call<'b>(prepared: Option<&Replacements<'_>>, expression: &'b Expr, name: &'b str) -> crate::functions::ResolvedFunction<'b> {
    match prepared.and_then(|invocation| invocation.prepared_call(expression)) {
        Some(call) => crate::functions::ResolvedFunction::from_option(call.function, name),
        None => crate::functions::ResolvedFunction::resolve(name),
    }
}

/// One expression invocation over a fixed input and parameter schema.
/// Preparation admits metadata and compiles replacement patterns before any row, including an empty input.
/// The caller keeps the preparation scope alive and supplies the same vector types to every row evaluation.
pub struct ExpressionInvocation<'expr> {
    expression: &'expr Expr,
    schema: &'expr Schema,
    output_type: LogicalType,
    requires_reader: bool,
    replacements: Replacements<'expr>,
}

impl<'expr> ExpressionInvocation<'expr> {
    /// Admits types and metadata, then compiles every emitted host program before executing pattern bindings in emitted order.
    /// All charges use `context.allocation`; retain that scope for the invocation's lifetime.
    pub fn prepare(expression: &'expr Expr,schema: &'expr Schema,context: &EvalContext<'_>) -> Result<Self> {
        let output_type = expression_type(expression,schema,context)?;
        let mut pending = Vec::new();
        prepare(expression,schema,context,&mut pending)?;
        context.reserve(pending.len().checked_mul(std::mem::size_of::<Replacement<'_>>()).ok_or_else(overflow)?)?;
        let mut replacements = Replacements {
            entries: Vec::with_capacity(pending.len()),
            calls: Vec::new(),
            lists: Default::default(),
            types: Default::default(),
            validated_calls: Default::default(),
        };
        for PendingBinding { expression,recipe,program } in pending {
            let pattern = program.pattern(context)?;
            let compiled = compile_replacement_pattern(pattern.view(),recipe.replacement_text()?,&recipe.flag_text()?,context)?;
            replacements.entries.push(Replacement { expression,compiled });
        }
        replacements.entries.sort_unstable_by_key(|entry|entry.expression as *const Expr as usize);
        let requires_reader = prepare_calls(expression, context, &mut replacements.calls)?;
        replacements.calls.sort_unstable_by_key(|entry| entry.expression as *const Expr as usize);
        prepare_lists(expression, context, &mut replacements.lists)?;
        Ok(Self { expression,schema,output_type,requires_reader,replacements })
    }

    /// Returns the emitted type derived from preparation's complete input and parameter vectors.
    pub fn data_type(&self) -> &LogicalType { &self.output_type }

    /// Returns an owned result under the row context's allocation scope.
    pub fn evaluate(&self,row: &[Value],context: &EvalContext<'_>) -> Result<Value> {
        let row = ExpressionRow::Values(row);
        let mut value = if self.requires_reader { reader::execute_sync_prepared(self.expression,self.schema,row,context,&self.replacements)? }
            else { super::evaluate_inner(self.expression,self.schema,row,context,Some(&self.replacements))? };
        value.admit_expression_dates()?;
        Ok(value)
    }

    /// Uses caller-owned suspension and cancellation; dropping the future retains no pending row work.
    pub async fn evaluate_async(&self,row: &[Value],context: &EvalContext<'_>) -> Result<Value> {
        self.evaluate_row_async(ExpressionRow::Values(row), context).await
    }

    /// Offers a reached Arrow list before materializing it, using the same evaluation and fallback as `evaluate_async`.
    pub async fn evaluate_async_with_arrow_list<T>(
        &self, row: &[Value], context: &EvalContext<'_>,
        retain: impl FnOnce(super::ArrowParameterCell<'_>) -> Result<Option<T>> + Send,
    ) -> Result<std::ops::ControlFlow<T, Value>> {
        use std::ops::ControlFlow;

        let row = ExpressionRow::Values(row);
        if !self.requires_reader {
            return self.evaluate_row_async(row, context).await.map(ControlFlow::Continue);
        }
        match reader::execute_with_arrow_list(self.expression, self.schema, row, context, &self.replacements, |cell, _| retain(cell)).await? {
            ControlFlow::Break(output) => Ok(ControlFlow::Break(output)),
            ControlFlow::Continue(mut value) => {
                value.admit_expression_dates()?;
                Ok(ControlFlow::Continue(value))
            }
        }
    }

    async fn evaluate_row_async(&self,row: ExpressionRow<'_>,context: &EvalContext<'_>) -> Result<Value> {
        let mut value = if self.requires_reader { reader::execute_async_prepared(self.expression,self.schema,row,context,&self.replacements).await? }
            else { super::evaluate_async_inner(self.expression,self.schema,row,context,Some(&self.replacements)).await? };
        value.admit_expression_dates()?;
        Ok(value)
    }

    /// Applies the computed-output validation of `evaluate_projection` to a prepared row.
    pub fn evaluate_projection(&self,row: &[Value],context: &EvalContext<'_>) -> Result<Value> {
        let value = self.evaluate(row,context)?;
        self.validate(&value)?;
        Ok(value)
    }

    /// Applies projection validation with the same caller-owned future as `evaluate_async`.
    pub async fn evaluate_projection_async(&self,row: &[Value],context: &EvalContext<'_>) -> Result<Value> {
        self.evaluate_projection_row_async(ExpressionRow::Values(row), context).await
    }

    pub async fn evaluate_projection_row_async(&self, row: ExpressionRow<'_>, context: &EvalContext<'_>) -> Result<Value> {
        let value = self.evaluate_row_async(row, context).await?;
        self.validate(&value)?;
        Ok(value)
    }

    /// Writes a reached borrowed text result while its input is available, or returns the validated owned result.
    pub async fn evaluate_projection_row_with_text(
        &self, row: ExpressionRow<'_>, context: &EvalContext<'_>, write: impl FnOnce(&str) -> Result<()> + Send,
    ) -> Result<Option<Value>> {
        if !matches!(self.expression, Expr::Case { .. } | Expr::SimpleCase { .. }) {
            return self.evaluate_projection_row_async(row, context).await.map(Some);
        }
        let mut value = reader::execute_with_text(self.expression, self.schema, row, context, &self.replacements, write).await?;
        if let Some(value) = &mut value {
            value.admit_expression_dates()?;
            self.validate(value)?;
        }
        Ok(value)
    }

    /// Offers a reached Arrow list and its backing to a caller that can preserve projection semantics.
    /// A declined list uses the ordinary materialization and validation of that same evaluation.
    pub async fn evaluate_projection_row_with_arrow_list<T>(
        &self, row: ExpressionRow<'_>, context: &EvalContext<'_>,
        retain: impl FnOnce(super::ArrowParameterCell<'_>, Option<&Arc<rtdb_types::MemoryReservation>>) -> Result<Option<T>> + Send,
    ) -> Result<std::ops::ControlFlow<T, Value>> {
        use std::ops::ControlFlow;

        if !self.requires_reader {
            return self.evaluate_projection_row_async(row, context).await.map(ControlFlow::Continue);
        }
        match reader::execute_with_arrow_list(self.expression, self.schema, row, context, &self.replacements, retain).await? {
            ControlFlow::Break(output) => Ok(ControlFlow::Break(output)),
            ControlFlow::Continue(mut value) => {
                value.admit_expression_dates()?;
                self.validate(&value)?;
                Ok(ControlFlow::Continue(value))
            }
        }
    }

    /// Retains generated list descriptors under the same scope contract as `evaluate_list_async`.
    pub async fn evaluate_list_async(&self,row: &[Value],context: &EvalContext<'_>) -> Result<super::EvaluatedList> {
        self.evaluate_list_row_async(ExpressionRow::Values(row), context).await
    }

    pub async fn evaluate_list_row_async(&self, row: ExpressionRow<'_>, context: &EvalContext<'_>) -> Result<super::EvaluatedList> {
        reader::execute_list_async(self.expression,self.schema,row,context,Some(&self.replacements)).await
    }

    /// Appends the items of a numeric list constructor in expression order to a caller-owned builder.
    /// The caller bounds the builder for all rows and admits constructors with only primitive numeric or null results.
    pub async fn append_numeric_list_items(
        &self, row: ExpressionRow<'_>, context: &EvalContext<'_>, builder: &mut ListBuilder,
    ) -> Result<()> {
        let Expr::List(items) = self.expression else {
            return Err(Anomaly::fault("rtdb.expr/list-state", "Expected a list constructor"));
        };
        reader::append_list_items(items, self.schema, row, context, &self.replacements, builder).await
    }

    /// Opens one list descriptor without demanding any element.
    /// Requires `context.allocation` and a positive `batch_rows`; each pull emits at most that many cells, and non-list inputs yield no batches.
    /// Descriptor errors occur while opening; element errors occur when their complete page is pulled.
    /// Retain this invocation, its preparation scope, row and context while pulling the returned pages.
    pub async fn open_list<'a>(&'a self, row: &'a [Value], context: &'a EvalContext<'a>, batch_rows: usize) -> Result<super::ListExpression<'a>> {
        use futures::StreamExt;
        let mut events = reader::list_events(self.expression, self.schema, row, context, &self.replacements, batch_rows);
        match events.next().await.transpose()? {
            Some(super::ListEvent::Opened(batches)) => {
                let batches = match batches {
                    Some(batches) => super::ListBatches::Indexed(batches),
                    None => super::ListBatches::Stream(events),
                };
                Ok(super::ListExpression { batches })
            },
            _ => Err(Anomaly::fault("rtdb.expr/list-state", "List descriptor did not open")),
        }
    }

    fn validate(&self,value: &Value) -> Result<()> {
        if matches!(self.expression,Expr::Column {..} | Expr::RowCopy(_)) { Ok(()) } else { validate_projection_value(value) }
    }
}

fn prepare_calls<'a>(expression: &'a Expr, context: &EvalContext<'_>, calls: &mut Vec<PreparedCall<'a>>) -> Result<bool> {
    stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || {
        Ok(match expression {
            Expr::List(_) | Expr::Struct(_) | Expr::RegexReplace { .. }
            | Expr::Unary { .. } | Expr::Binary { .. } | Expr::Cast { .. } | Expr::Field { .. }
            | Expr::Subscript { .. } | Expr::Case { .. } | Expr::SimpleCase { .. }
            | Expr::Between { .. } | Expr::Coalesce(_) | Expr::Like { .. } => {
                for child in expression.children() { prepare_calls(child, context, calls)?; }
                true
            }
            Expr::Literal(Value::List(_) | Value::Set(_) | Value::FixedSizeList(_) | Value::Struct(_)) => true,
            Expr::Call { name, args, return_type } => {
                let mut args_require_reader = false;
                for arg in args {
                    args_require_reader |= prepare_calls(arg, context, calls)?;
                }
                let resolved = crate::functions::ResolvedFunction::resolve(name);
                let null_typed = reader::null_typed(resolved, return_type);
                let dispatch_prepared = null_typed || resolved.is_prepared_call() || resolved.is_strict_temporal();
                let requires_reader = null_typed
                    || resolved.is_cosine()
                    || (!args.is_empty() && resolved.is_strict_temporal())
                    || resolved.is_prepared_call()
                    || resolved.is_collection_call()
                    || args_require_reader;
                context.reserve(std::mem::size_of::<PreparedCall<'_>>())?;
                calls.push(PreparedCall {
                    expression,
                    function: match resolved {
                        crate::functions::ResolvedFunction::Known(function) => Some(function),
                        crate::functions::ResolvedFunction::Unknown(_) => None,
                    },
                    dispatch_prepared,
                    requires_reader,
                });
                requires_reader
            }
            Expr::In { expr, values, .. } => {
                let mut required = prepare_calls(expr, context, calls)?;
                for value in values { required |= prepare_calls(value, context, calls)?; }
                required
            }
            _ => false,
        })
    })
}

fn prepare_lists(expression: &Expr, context: &EvalContext<'_>, lists: &mut std::collections::HashMap<usize, PreparedList>) -> Result<()> {
    stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || {
        if let Expr::List(items) = expression
            && let Some(list) = prepare_list(expression, items, context)?
        {
            lists.insert(expression as *const Expr as usize, list);
            return Ok(());
        }
        for child in expression.children() { prepare_lists(child, context, lists)?; }
        Ok(())
    })
}

fn literal_item(expression: &Expr) -> Option<(&Value, bool)> {
    match expression {
        Expr::Literal(Value::List(_) | Value::FixedSizeList(_) | Value::Struct(_) | Value::Set(_) | Value::Map { .. } | Value::Decimal(_)) => None,
        Expr::Literal(value) => Some((value, false)),
        Expr::Unary { op: rtdb_plan::UnaryOp::Negate, expr } => match expr.as_ref() {
            Expr::Literal(value @ (Value::I64(_) | Value::F64(_))) => Some((value, true)),
            _ => None,
        },
        _ => None,
    }
}

fn prepare_list(expression: &Expr, items: &[Expr], context: &EvalContext<'_>) -> Result<Option<PreparedList>> {
    let mut bytes = 512usize;
    for item in items {
        let Some((value, _)) = literal_item(item) else { return Ok(None); };
        bytes = bytes.checked_add(value.retained_bytes()?).and_then(|bytes| bytes.checked_add(512)).ok_or_else(overflow)?;
    }
    let scratch = context.allocation.map(crate::EvaluationScope::scratch);
    // A cache miss leaves the original descriptor available under a tight budget or a failing cell.
    if scratch.as_ref().is_some_and(|scope| scope.reserve(bytes).is_err()) { return Ok(None); }
    let mut builder = ListBuilder::with_capacity(items.len());
    for item in items {
        let (value, negate) = literal_item(item).expect("checked literal cells");
        let mut value = if negate {
            let Ok(value) = super::unary_value(rtdb_plan::UnaryOp::Negate, value.clone()) else { return Ok(None); };
            value
        } else { value.clone() };
        if value.admit_box_date().is_err() { return Ok(None); }
        builder.push(value);
    }
    let values = builder.finish();
    let element = values.element_type();
    let type_bound = reader::expression_type_bound(expression, None)?;
    let retained = values.retained_payload_bytes()?.checked_add(element.retained_bytes()?)
        .and_then(|bytes| bytes.checked_add(512)).ok_or_else(overflow)?;
    if context.reserve(retained).is_err() { return Ok(None); }
    Ok(Some(PreparedList { values, element, type_bound }))
}

fn prepare<'a>(expression: &'a Expr,schema: &Schema,context: &EvalContext<'_>,bindings: &mut Vec<PendingBinding<'a>>) -> Result<()> {
    context.reserve(4096)?;
    if let Expr::RegexReplace { source,recipe,.. } = expression {
        if expression_type(source,schema,context)?.legs().contains(&LogicalType::Scalar(ScalarType::Utf8)) {
            let program = host::Program::compile(&recipe.pattern,context)?;
            context.reserve(std::mem::size_of::<PendingBinding<'_>>().checked_mul(2).and_then(|bytes|bytes.checked_add(256)).ok_or_else(overflow)?)?;
            bindings.push(PendingBinding { expression,recipe,program });
        }
        return prepare(source,schema,context,bindings);
    }
    if let Expr::Coalesce(args) = expression {
        return prepare_coalesce(args,schema,context,bindings);
    }
    if let Expr::Call { name,args,.. } = expression {
        if crate::functions::ScalarFunction::resolve(name) == Some(crate::functions::ScalarFunction::Coalesce) {
            return prepare_coalesce(args,schema,context,bindings);
        }
    }
    for child in expression.children() { prepare(child,schema,context,bindings)?; }
    Ok(())
}

fn prepare_coalesce<'a>(args: &'a [Expr],schema: &Schema,context: &EvalContext<'_>,bindings: &mut Vec<PendingBinding<'a>>) -> Result<()> {
    let Some((first,rest)) = args.split_first() else { return Ok(()); };
    if expression_type(first,schema,context)?.nullable() { prepare_coalesce(rest,schema,context,bindings)?; }
    prepare(first,schema,context,bindings)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::functions::ScalarFunction;
    use crate::session_info::SessionInfoFunction;
    use crate::string::StringFunction;
    use rtdb_plan::{ColumnId, Field};

    #[test]
    fn invocation_prepares_every_call_node_once_and_dispatches_across_rows() {
        let text_type = LogicalType::Scalar(ScalarType::Utf8).maybe();
        let schema = Schema::new(vec![Field {
            id: ColumnId(0),
            name: "s".into(),
            qualifier: None,
            qualified_only: false,
            unnamed: false,
            data_type: text_type.clone(),
        }]).unwrap();
        let inner = Expr::Call {
            name: "CONCAT".into(),
            args: vec![Expr::column(ColumnId(0), text_type), Expr::Literal(Value::Utf8("!".into()))],
            return_type: LogicalType::Scalar(ScalarType::Utf8).maybe(),
        };
        let outer = Expr::Call {
            name: "UPPER".into(),
            args: vec![inner],
            return_type: LogicalType::Scalar(ScalarType::Utf8).maybe(),
        };
        let context = EvalContext::default();
        let invocation = ExpressionInvocation::prepare(&outer, &schema, &context).unwrap();
        assert!(invocation.requires_reader);
        assert_eq!(invocation.replacements.calls.len(), 2);
        let Expr::Call { args, .. } = &outer else { unreachable!() };
        assert_eq!(invocation.replacements.prepared_call(&outer).unwrap().function, Some(ScalarFunction::Upper));
        assert_eq!(invocation.replacements.prepared_call(&args[0]).unwrap().function, Some(ScalarFunction::String(StringFunction::Concat)));
        assert_eq!(invocation.evaluate(&[Value::Utf8("hi".into())], &context).unwrap(), Value::Utf8("HI!".into()));
        assert_eq!(invocation.evaluate(&[Value::Null], &context).unwrap(), Value::Null);

        let non_reader = Expr::Call {
            name: "CURRENT-USER".into(),
            args: vec![],
            return_type: LogicalType::Scalar(ScalarType::Utf8),
        };
        let schema = Schema::default();
        let invocation = ExpressionInvocation::prepare(&non_reader, &schema, &context).unwrap();
        assert!(!invocation.requires_reader);
        assert_eq!(
            invocation.replacements.prepared_call(&non_reader).unwrap().function,
            Some(ScalarFunction::SessionInfo(SessionInfoFunction::CurrentUser))
        );
        assert_eq!(invocation.evaluate(&[], &context).unwrap(), Value::Utf8("xtdb".into()));

        let unregistered = Expr::Call {
            name: "unregistered-fn".into(),
            args: vec![Expr::Literal(Value::Null)],
            return_type: LogicalType::Null,
        };
        let invocation = ExpressionInvocation::prepare(&unregistered, &schema, &context).unwrap();
        assert!(invocation.requires_reader);
        assert_eq!(invocation.replacements.prepared_call(&unregistered).unwrap().function, None);
        assert_eq!(invocation.evaluate(&[], &context).unwrap(), Value::Null);
    }
}
