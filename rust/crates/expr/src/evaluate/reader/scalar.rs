use crate::admission::Admission;
use crate::collection::CollectionFunction;
use crate::functions::{FloatMath, ResolvedFunction, ScalarFunction};
use crate::string::StringFunction;
use crate::temporal_functions::TemporalFunction;
use super::*;
use super::computed::Output;
use rtdb_types::{NumericCell, ScalarType, decimal_arrow::DecimalParameters};
use crate::decimal::ScalarDecimal;

pub(super) struct Continuation<'a> { ty: LogicalType, payload: Pending<'a,Output<'a>> }
impl<'a> Continuation<'a> {
    pub(super) fn is_null(&self) -> bool { self.ty == LogicalType::Null }
    pub(super) fn is_list(&self) -> bool { matches!(self.ty, LogicalType::List { kind: ListKind::List | ListKind::Set | ListKind::FixedSizeList(_), .. }) }
    pub(super) async fn demand(self) -> Result<EvalValue<'a>> { self.demand_output().await?.into_value() }
    pub(super) async fn demand_output(self) -> Result<Output<'a>> { self.payload.await }
}
fn deferred<'a>(ty: LogicalType,payload: impl Future<Output=Result<EvalValue<'a>>> + Send + 'a,frame: &EvaluationFrame<'_>) -> Result<Continuation<'a>> {
    deferred_output(ty, async move { payload.await.map(Output::Value) }, frame)
}
fn deferred_output<'a>(ty: LogicalType,payload: impl Future<Output=Result<Output<'a>>> + Send + 'a,frame: &EvaluationFrame<'_>) -> Result<Continuation<'a>> {
    Ok(Continuation { ty,payload: boxed(payload,frame)? })
}
fn ready_output<'a>(value: Output<'a>, frame: &EvaluationFrame<'_>) -> Result<Continuation<'a>> {
    match value {
        Output::Value(value) => ready(value, frame),
        Output::Text(..) => deferred_output(LogicalType::Scalar(ScalarType::Utf8), async move { Ok(value) }, frame),
        Output::Number(number) => {
            frame.context.reserve(4096 + number_value_bytes(number) * 8)?;
            let ty = number.scalar_type().map_or(LogicalType::Null, LogicalType::Scalar);
            deferred_output(ty, async move { Ok(Output::Number(number)) }, frame)
        }
        Output::Decimal(decimal) => {
            let ty = LogicalType::Scalar(decimal.scalar_type());
            deferred_output(ty, async move { Ok(Output::Decimal(decimal)) }, frame)
        }
    }
}
fn number_value_bytes(number: NumericCell) -> usize {
    std::mem::size_of::<Value>() + match number {
        NumericCell::Decimal(_, params) => usize::from(params.bit_width / 8),
        _ => 0,
    }
}
fn ready<'a>(value: EvalValue<'a>,frame: &EvaluationFrame<'_>) -> Result<Continuation<'a>> {
    deferred(value_type(&value,frame)?,async move { Ok(value) },frame)
}
fn nil<'a>(frame: &EvaluationFrame<'_>) -> Result<Continuation<'a>> { ready(scalar(Value::Null),frame) }

fn typed(ty: LogicalType) -> Box<Expr> { Box::new(Expr::Parameter { index: 0,data_type: ty }) }
fn coalesce_type(args: &[Expr],frame: &EvaluationFrame<'_>) -> Result<LogicalType> {
    let mut result = LogicalType::Nothing;
    for (index,arg) in args.iter().enumerate() {
        let ty = emitted_type(arg,frame)?;
        for leg in ty.legs() { if leg != LogicalType::Null || index + 1 == args.len() { result = result.join(&leg); } }
        if !ty.nullable() { break; }
    }
    Ok(if result == LogicalType::Nothing { LogicalType::Null } else { result })
}
fn retained_type(ty: &LogicalType,frame: &EvaluationFrame<'_>) -> Result<LogicalType> {
    frame.context.reserve(ty.retained_bytes()?.checked_mul(8).ok_or_else(overflow)?)?;
    Ok(ty.unencoded())
}
pub(super) fn emitted_type(expr: &Expr,frame: &EvaluationFrame<'_>) -> Result<LogicalType> {
    stacker::maybe_grow(256 * 1024, 2 * 1024 * 1024, || match frame.replacements {
        Some(invocation) => invocation.emitted_type(expr,|| emitted_type_at(expr,frame)),
        None => emitted_type_at(expr,frame),
    })
}
fn emitted_type_at(expr: &Expr,frame: &EvaluationFrame<'_>) -> Result<LogicalType> {
    let child = |expr: &Expr| emitted_type(expr,frame);
    Ok(match expr {
        Expr::Literal(value) => { frame.context.reserve(value_type_bound(value)?.checked_mul(8).ok_or_else(overflow)?)?;value.logical_type() }
        Expr::DurationLiteral(_) => expr.data_type(),
        Expr::RegexReplace { source,recipe,.. } => recipe.output_type(&child(source)?)?,
        Expr::Column { id,.. } => retained_type(&frame.schema.get(*id).ok_or_else(||Anomaly::fault("rtdb.expr/missing-column","Expression input column is unavailable"))?.data_type,frame)?,
        Expr::Parameter { index,data_type } => {
            if let Some(ty) = frame.context.parameter_types.and_then(|types| types.get(*index)) { retained_type(ty,frame)? }
            else if *data_type == LogicalType::Nothing {
                if let Some(cell) = frame.context.parameters.arrow_cell(*index)
                    && (cell.is_null()? || packed_numeric_cell(cell)) {
                    frame.context.reserve(cell.observed_type_bytes()?.checked_mul(8).ok_or_else(overflow)?)?;
                    return Ok(rtdb_columnar::execution_type(&cell.observed_type()?));
                }
                let value = frame.context.parameters.get(*index)?.ok_or_else(||Anomaly::incorrect("xtdb/missing-parameter",format!("Missing parameter ${}",index + 1)))?;
                frame.context.reserve(value_type_bound(value)?.checked_mul(8).ok_or_else(overflow)?)?;
                value.logical_type()
            } else { retained_type(data_type,frame)? }
        }
        Expr::Correlated { depth,id,.. } => retained_type(&frame.context.outer_rows.get(*depth).and_then(|row|row.schema.get(*id)).ok_or_else(||Anomaly::fault("rtdb.expr/missing-correlated-column","Correlated row is unavailable"))?.data_type,frame)?,
        Expr::Coalesce(args) => coalesce_type(args,frame)?,
        Expr::Case { branches,otherwise } => {
            let mut ty = LogicalType::Nothing;
            for (condition,value) in branches { child(condition)?;ty = ty.join(&child(value)?); }
            ty.join(&child(otherwise)?)
        }
        Expr::Between { operand,low,high,symmetric } => {
            let (operand,low,high) = (child(operand)?,child(low)?,child(high)?);
            rtdb_plan::validate_ordered_predicate_types(&operand,&low)?;
            rtdb_plan::validate_ordered_predicate_types(&operand,&high)?;
            Expr::Between { operand: typed(operand),low: typed(low),high: typed(high),symmetric: *symmetric }.data_type()
        },
        Expr::SimpleCase { operand,branches,otherwise } => {
            let operand = child(operand)?;
            let mut ty = LogicalType::Nothing;
            for (candidate,value) in branches { rtdb_plan::validate_equality_types(&operand,&child(candidate)?)?;ty = ty.join(&child(value)?); }
            ty.join(&child(otherwise)?)
        }
        Expr::Cast { expr,target } => { let source = child(expr)?;target.validate_source(&source)?;target.result_type(&source) }
        Expr::Field { expr,name,.. } => rtdb_plan::field_type(&child(expr)?,name),
        Expr::Subscript { expr,index,.. } => rtdb_plan::function_type("nth", &[child(expr)?,child(index)?])?,
        Expr::Unary { op,expr } => {
            let input = child(expr)?;
            if *op == UnaryOp::BitNot { rtdb_plan::function_type("bit_not",&[input])? }
            else { Expr::Unary { op: *op,expr: typed(input) }.data_type() }
        }
        Expr::Binary { op,left,right } => {
            let (left,right) = (child(left)?,child(right)?);
            let expression = Expr::Binary { op: *op,left: typed(left),right: typed(right) };
            expression.validate_columns(&Default::default())?;
            expression.data_type()
        }
        Expr::Call { name, args, .. } => {
            let function = frame.resolved_call(expr, name);
            if function == ResolvedFunction::Known(ScalarFunction::Coalesce) { coalesce_type(args, frame)? }
            else {
                let types = args.iter().map(child).collect::<Result<Vec<_>>>()?;
                if function.is_equality_call() && types.len() == 2 { rtdb_plan::validate_equality_types(&types[0], &types[1])?; }
                let arguments = args.iter().zip(types).enumerate().map(|(index, (arg, ty))| {
                    if function.literal_argument(index) {
                        frame.context.reserve(expression_type_bound(arg, frame.replacements)?.checked_mul(8).ok_or_else(overflow)?)?;
                        frame.context.reserve(literal_payload_bound(arg)?)?;
                        Ok(arg.clone())
                    } else { Ok(Expr::Parameter { index: 0, data_type: ty }) }
                }).collect::<Result<Vec<_>>>()?;
                rtdb_plan::function_type_with_args(&function.canonical_name(), &arguments)?
            }
        }
        Expr::Like { expr,pattern,case_insensitive,.. } => rtdb_plan::function_type(if *case_insensitive { "ilike" } else { "like" }, &[child(expr)?,child(pattern)?])?,
        Expr::List(items) => {
            let element = if let Some(list) = frame.prepared_list(expr) {
                frame.context.reserve(list.element.retained_bytes()?)?;
                list.element.clone()
            } else {
                LogicalType::join_all(&items.iter().map(child).collect::<Result<Vec<_>>>()?)
            };
            LogicalType::list(element)
        }
        Expr::Struct(fields) => LogicalType::Struct(fields.iter().map(|(name,expr)|Ok((name.clone(),child(expr)?))).collect::<Result<_>>()?),
        Expr::RowCopy(fields) => LogicalType::Struct(fields.iter().map(|field|Ok((field.name.clone(),retained_type(&frame.schema.get(field.id).ok_or_else(||Anomaly::fault("rtdb.expr/missing-column","Raw record input column is unavailable"))?.data_type,frame)?))).collect::<Result<_>>()?),
        Expr::In { expr,values,negated } => {
            let expression = Expr::In { expr: typed(child(expr)?),values: values.iter().map(|value|child(value).map(|ty|*typed(ty))).collect::<Result<_>>()?,negated: *negated };
            rtdb_plan::validate_equality_expression(&expression)?;
            expression.data_type()
        }
    })
}

// expression.clj's continuations select a type before consuming its payload; polymorphic returns box the payload first.
pub(super) fn prepare<'a>(expr: &'a Expr,frame: &'a EvaluationFrame<'a>) -> Result<Pending<'a,Continuation<'a>>> {
    boxed(async move {
        frame.context.reserve(expression_type_bound(expr, frame.replacements)?.checked_mul(8).ok_or_else(overflow)?)?;
        let ty = emitted_type(expr,frame)?;
        let boxed_return = ty.legs().len() > 1;
        let continuation = prepare_inner(expr,ty,frame)?.await?;
        if !boxed_return { return Ok(continuation); }
        let output = match continuation.demand_output().await? {
            Output::Value(value) => Output::Value(box_cell(value)?),
            output => output,
        };
        ready_output(output, frame)
    },frame)
}

fn prepare_inner<'a>(expr: &'a Expr,ty: LogicalType,frame: &'a EvaluationFrame<'a>) -> Result<Pending<'a,Continuation<'a>>> {
    boxed(async move {
        if matches!(expr, Expr::Column { .. } | Expr::Field { .. })
            && let Some(text) = frame.arrow_input(expr).and_then(ArrowCell::utf8)
        {
            return ready_output(Output::Text(text, Admission::for_context(frame.context.allocation)), frame);
        }
        match expr {
            Expr::Case { branches,otherwise } => {
                for (condition,value) in branches {
                    let condition = prepare(condition,frame)?.await?;
                    let selected = if condition.is_null() { false }
                        else if condition.ty == LogicalType::Scalar(rtdb_types::ScalarType::Bool) { super::super::is_true(scalar_ref(&condition.demand().await?)?)? }
                        else { true };
                    if selected { return prepare(value,frame)?.await; }
                }
                prepare(otherwise,frame)?.await
            }
            Expr::Between { operand,low,high,symmetric } => {
                let operand = prepare(operand,frame)?.await?.demand_output().await?;
                if operand.is_null() { return nil(frame); }
                let nullable = emitted_type(low,frame)?.nullable() || emitted_type(high,frame)?.nullable();
                let payload = async move {
                    let forward = between_bounds(&operand,low,high,frame).await?;
                    if !*symmetric || forward == Some(true) { return Ok(scalar(boolean(forward))); }
                    let reverse = between_bounds(&operand,high,low,frame).await?;
                    Ok(scalar(boolean(if reverse == Some(true) { Some(true) } else if forward.is_none() || reverse.is_none() { None } else { Some(false) })))
                };
                if nullable { ready(payload.await?,frame) } else { deferred(ty,payload,frame) }
            }
            Expr::SimpleCase { operand,branches,otherwise } => {
                let operand = prepare(operand,frame)?.await?.demand_output().await?;
                if operand.is_null() { return prepare(otherwise,frame)?.await; }
                for (candidate,result) in branches {
                    let selected = prepare(candidate,frame)?.await?;
                    if selected.is_null() { continue; }
                    let right = selected.demand_output().await?;
                    if computed::equal(&operand, right, false, matches!(candidate, Expr::Literal(Value::Utf8(_))), frame).await? == Some(true) {
                        return prepare(result,frame)?.await;
                    }
                }
                prepare(otherwise,frame)?.await
            }
            Expr::Coalesce(args) => coalesce(args,frame)?.await,
            Expr::Cast { expr: input,target } => {
                target.validate_source(&emitted_type(input,frame)?)?;
                let input = prepare(input,frame)?.await?;
                if input.is_null() { return nil(frame); }
                deferred_output(ty,async move {
                    let input = input.demand_output().await?;
                    cast_output(input, target, frame).await
                },frame)
            }
            Expr::Unary { op,expr: input } => {
                let input = prepare(input,frame)?.await?;
                if matches!(op,UnaryOp::IsNull | UnaryOp::IsUnknown | UnaryOp::IsNotNull) {
                    return ready(scalar(Value::Bool(input.is_null() != (*op == UnaryOp::IsNotNull))),frame);
                }
                if matches!(op,UnaryOp::IsTrue | UnaryOp::IsFalse) && input.ty != LogicalType::Scalar(rtdb_types::ScalarType::Bool) { return ready(scalar(Value::Bool(false)),frame); }
                if input.is_null() { return nil(frame); }
                deferred_output(ty,async move {
                    let input = input.demand_output().await?;
                    if *op == UnaryOp::Negate && input.is_decimal() {
                        return Ok(Output::Decimal(input.into_decimal(frame)?.negate()));
                    }
                    if *op == UnaryOp::Negate && let Some(number) = input.number() {
                        if number == NumericCell::Null { return Ok(Output::Number(number)); }
                        let ty = number.scalar_type().expect("present number");
                        if let Some(integer) = number.integer() {
                            return crate::numeric::integer_cell(integer.checked_neg().ok_or_else(overflow)?, &ty).map(Output::Number);
                        }
                        return Ok(Output::Number(computed::floating(-number.as_f64().unwrap(), &ty)));
                    }
                    let input = materialize(input.into_value()?,frame)?.await?;
                    from_value(super::super::unary_value(*op,input)?,frame).map(Output::Value)
                },frame)
            }
            Expr::Binary { op: BinaryOp::And | BinaryOp::Or,left,right } => {
                let Expr::Binary { op,.. } = expr else { unreachable!() };
                let nullable = emitted_type(left,frame)?.nullable() || emitted_type(right,frame)?.nullable();
                let payload = async move {
                    let left = prepare(left,frame)?.await?;
                    let left = if left.is_null() { scalar(Value::Null) } else { left.demand().await? };
                    let truth = payload_truth(&left);
                    if (*op == BinaryOp::And && truth == Some(false)) || (*op == BinaryOp::Or && truth == Some(true)) { return Ok(scalar(boolean(truth))); }
                    let right = prepare(right,frame)?.await?;
                    let right = if right.is_null() { scalar(Value::Null) } else { right.demand().await? };
                    let right = payload_truth(&right);
                    let result = if *op == BinaryOp::And {
                        if right == Some(false) { Some(false) } else if truth.is_none() || right.is_none() { None } else { Some(true) }
                    } else if right == Some(true) { Some(true) } else if truth.is_none() || right.is_none() { None } else { Some(false) };
                    Ok(scalar(boolean(result)))
                };
                if nullable { ready(payload.await?,frame) } else { deferred(ty,payload,frame) }
            }
            Expr::Binary { op,left: left_expr,right: right_expr } => {
                let null_safe = matches!(op,BinaryOp::IsDistinctFrom | BinaryOp::IsNotDistinctFrom);
                let left = prepare(left_expr,frame)?.await?;
                if left.is_null() && !null_safe { return nil(frame); }
                let right = prepare(right_expr,frame)?.await?;
                if right.is_null() && !null_safe { return nil(frame); }
                if matches!(op,BinaryOp::Eq | BinaryOp::NotEq | BinaryOp::IsDistinctFrom | BinaryOp::IsNotDistinctFrom) { rtdb_plan::validate_equality_types(&left.ty,&right.ty)?; }
                if null_safe && (left.is_null() || right.is_null()) {
                    let equal = left.is_null() == right.is_null();
                    return ready(scalar(Value::Bool(if *op == BinaryOp::IsNotDistinctFrom { equal } else { !equal })),frame);
                }
                if matches!(op, BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply | BinaryOp::Divide | BinaryOp::Modulo) {
                    return deferred_output(ty, async move {
                        let left = left.demand_output().await?;
                        let right = right.demand_output().await?;
                        computed::arithmetic(*op, left, right, frame).await
                    }, frame);
                }
                deferred(ty,async move {
                    let left = left.demand_output().await?;
                    let right = right.demand_output().await?;
                    if matches!(op, BinaryOp::Eq | BinaryOp::NotEq | BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq | BinaryOp::IsDistinctFrom | BinaryOp::IsNotDistinctFrom) {
                        return computed::predicate(*op, left, right, Some(left_expr), right_expr, frame).await;
                    }
                    if *op == BinaryOp::Concat && (matches!(left, Output::Text(..)) || matches!(right, Output::Text(..)))
                        && let (Some(left), Some(right)) = (left.text(), right.text())
                    {
                        let size = left.len().checked_add(right.len()).ok_or_else(overflow)?;
                        frame.context.reserve(size)?;
                        let mut result = String::with_capacity(size);
                        result.push_str(left);
                        result.push_str(right);
                        return Ok(scalar(Value::Utf8(result)));
                    }
                    calls::binary_values(*op, left.into_value()?, right.into_value()?, frame)?.await
                },frame)
            }
            Expr::Call { name, args, .. } => {
                frame.context.reserve(name.len().checked_mul(8).and_then(|bytes| bytes.checked_add(256)).ok_or_else(overflow)?)?;
                let function = frame.resolved_call(expr, name);
                calls::validate(expr, function, args, frame)?;
                match function {
                    ResolvedFunction::Known(ScalarFunction::Coalesce) => coalesce(args, frame)?.await,
                    ResolvedFunction::Known(ScalarFunction::Nullif) => nullif(args, frame)?.await,
                    ResolvedFunction::Known(ScalarFunction::Greatest) => greatest(true, args, frame)?.await,
                    ResolvedFunction::Known(ScalarFunction::Least) => greatest(false, args, frame)?.await,
                    ResolvedFunction::Known(ScalarFunction::Collection(CollectionFunction::TrimArray) | ScalarFunction::Set) => ready(evaluate(expr, frame)?.await?, frame),
                    _ => call(function, args, ty, frame)?.await,
                }
            }
            Expr::RegexReplace { source,.. } => {
                let source = prepare(source,frame)?.await?;
                if source.is_null() { return nil(frame); }
                deferred(ty,async move {
                    let source = source.demand_output().await?;
                    let text = match source.text() {
                        Some(text) => text,
                        None => { scalar_ref(&source.into_value()?)?; return Err(invalid("regexp_replace")); }
                    };
                    frame.replacements.ok_or_else(||Anomaly::fault("rtdb.expr/replacement-binding","Replacement invocation is unavailable"))?.get(expr)?.replace(text,frame.context).map(scalar)
                },frame)
            }
            Expr::Subscript { expr,index,.. } => {
                let input = prepare(expr,frame)?.await?;
                if input.is_null() { return nil(frame); }
                let index = prepare(index,frame)?.await?;
                if index.is_null() { return nil(frame); }
                rtdb_plan::function_type("nth", &[input.ty.clone(),index.ty.clone()])?;
                if !matches!(input.ty,LogicalType::List { kind: ListKind::List | ListKind::FixedSizeList(_),.. }) { return nil(frame); }
                ready(calls::subscript(input.demand().await?,index.demand().await?,frame)?.await?,frame)
            }
            Expr::Field { expr: input,name,.. } => {
                if let Some(cell) = frame.arrow_input(expr) {
                    return ready(box_cell(from_arrow(cell, frame.arrow_backing(expr), frame)?)?, frame);
                }
                let input = prepare(input,frame)?.await?;
                if rtdb_plan::field_type(&input.ty,name) == LogicalType::Null { return nil(frame); }
                deferred(ty,async move {
                    calls::field(input.demand().await?,Some(name),frame)
                },frame)
            }
            Expr::List(_) | Expr::Struct(_) | Expr::RowCopy(_) | Expr::Column {..} | Expr::Parameter {..} | Expr::Correlated {..} => ready(evaluate(expr,frame)?.await?,frame),
            Expr::Like { expr: input,pattern,escape,case_insensitive,negated } => {
                let input = prepare(input,frame)?.await?;
                if input.is_null() { return nil(frame); }
                let pattern = prepare(pattern,frame)?.await?;
                if pattern.is_null() { return nil(frame); }
                deferred(ty,async move {
                    let pattern = pattern.demand().await?;
                    let scratch = frame.context.allocation.map(crate::EvaluationScope::scratch);
                    let context = crate::EvalContext { allocation: scratch.as_ref().or(frame.context.allocation), ..*frame.context };
                    let pattern = crate::string::pattern::compile_like(scalar_ref(&pattern)?,*escape,*case_insensitive,&context)?;
                    let input = input.demand().await?;
                    pattern.search(scalar_ref(&input)?,*negated,&context).map(scalar)
                },frame)
            }
            Expr::Literal(value) => {
                if let Some(number) = NumericCell::from_value(value) {
                    return deferred_output(ty, async move {
                        frame.context.reserve(value.retained_bytes()?)?;
                        Ok(Output::Number(number))
                    }, frame);
                }
                deferred(ty,async move { evaluate(expr,frame)?.await },frame)
            }
            Expr::DurationLiteral(_) | Expr::In {..} => deferred(ty,async move { evaluate(expr,frame)?.await },frame),
        }
    },frame)
}

async fn cast_output<'a>(input: Output<'a>, target: &'a CastTarget, frame: &'a EvaluationFrame<'a>) -> Result<Output<'a>> {
    use crate::list_cast::{Target as NumberTarget, scalar_target};
    if let Output::Decimal(decimal) = input {
        let source = LogicalType::Scalar(decimal.scalar_type());
        target.validate_source(&source)?;
        let output = target.result_type(&source);
        return match plain_type(&output) {
            LogicalType::Scalar(ScalarType::Utf8) => decimal.text(frame.context).map(|text| Output::Value(scalar(text))),
            LogicalType::Scalar(ScalarType::Decimal { precision, scale, bit_width }) => {
                let params = DecimalParameters { precision: *precision, scale: *scale, bit_width: *bit_width };
                if matches!(scalar_target(target), Some(NumberTarget::DefaultDecimal)) { return Ok(Output::Decimal(decimal)); }
                decimal.rescale(params, bigdecimal::RoundingMode::HalfEven, frame.context).map(Output::Decimal)
            }
            LogicalType::Scalar(ty @ (ScalarType::F16 | ScalarType::F32 | ScalarType::F64)) => {
                Ok(Output::Number(computed::floating(decimal.as_f64(frame.context)?, ty)))
            }
            LogicalType::Scalar(ty @ (ScalarType::I8 | ScalarType::I16 | ScalarType::I32 | ScalarType::I64
                | ScalarType::U8 | ScalarType::U16 | ScalarType::U32 | ScalarType::U64)) => {
                let integer = crate::cast::decimal_integer(decimal.as_f64(frame.context)?)?;
                crate::numeric::integer_cell(integer, ty).map(Output::Number)
            }
            _ => cast_value(Output::Decimal(decimal).into_value()?, Target::Cast(target), frame)?.await.map(Output::Value),
        };
    }
    if let Some(target_type) = scalar_target(target) {
        let decimal = match target_type {
            NumberTarget::Scalar(ScalarType::Decimal { precision, scale, bit_width }) => {
                Some(DecimalParameters { precision: *precision, scale: *scale, bit_width: *bit_width })
            }
            NumberTarget::DefaultDecimal if !input.is_decimal() => Some(DecimalParameters { precision: 64, scale: 9, bit_width: 256 }),
            _ => None,
        };
        if let Some(number) = input.number() && let Some(source) = number.scalar_type() {
            frame.context.reserve(4096 + number_value_bytes(number) * 8)?;
            target.validate_source(&LogicalType::Scalar(source.clone()))?;
            if let Some(params) = decimal
                && !matches!(target_type, NumberTarget::Scalar(ty) if *ty == source)
            {
                return ScalarDecimal::from_number(number, params, frame.context).map(Output::Decimal);
            }
            frame.context.reserve(4096 + number_value_bytes(number) * 64)?;
            return crate::list_cast::cast_number(number, &source, target_type).map(Output::Number);
        }
        if let (Some(params), Output::Value(EvalValue::Scalar(Value::Utf8(text)))) = (decimal, &input) {
            target.validate_source(&LogicalType::Scalar(ScalarType::Utf8))?;
            return ScalarDecimal::from_text(text, params, frame.context).map(Output::Decimal);
        }
    }
    cast_value(input.into_value()?, Target::Cast(target), frame)?.await.map(Output::Value)
}

async fn between_bound<'a>(operand: &Output<'a>,bound: &'a Expr,op: BinaryOp,frame: &'a EvaluationFrame<'a>) -> Result<Option<bool>> {
    let bound_expr = bound;
    let bound = prepare(bound,frame)?.await?;
    if bound.is_null() { return Ok(None); }
    let right = bound.demand_output().await?;
    if let Some(result) = computed::decimal_predicate(op, operand, &right, frame)? { return Ok(result); }
    let (mut left, mut right) = (operand.clone_narrow(frame)?, right.into_value()?);
    calls::literal_coercion(None, bound_expr, &mut left, &mut right, frame)?;
    Ok(payload_truth(&calls::binary_values(op,left,right,frame)?.await?))
}
async fn between_bounds<'a>(operand: &Output<'a>,low: &'a Expr,high: &'a Expr,frame: &'a EvaluationFrame<'a>) -> Result<Option<bool>> {
    let lower = between_bound(operand,low,BinaryOp::GtEq,frame).await?;
    if lower == Some(false) { return Ok(Some(false)); }
    let upper = between_bound(operand,high,BinaryOp::LtEq,frame).await?;
    Ok(if upper == Some(false) { Some(false) } else if lower.is_none() || upper.is_none() { None } else { Some(true) })
}

fn coalesce<'a>(args: &'a [Expr],frame: &'a EvaluationFrame<'a>) -> Result<Pending<'a,Continuation<'a>>> {
    boxed(async move {
        for (index,expr) in args.iter().enumerate() {
            let value = prepare(expr,frame)?.await?;
            if index + 1 == args.len() { return Ok(value); }
            if !value.is_null() { return ready_output(value.demand_output().await?,frame); }
        }
        nil(frame)
    },frame)
}
fn nullif<'a>(args: &'a [Expr],frame: &'a EvaluationFrame<'a>) -> Result<Pending<'a,Continuation<'a>>> {
    boxed(async move {
        let left = prepare(&args[0],frame)?.await?.demand_output().await?;
        if left.is_null() { return ready_output(left,frame); }
        let right = prepare(&args[1],frame)?.await?;
        if right.is_null() { return ready_output(left,frame); }
        let right = right.demand_output().await?;
        let equal = computed::equal(&left, right, false, matches!(&args[1], Expr::Literal(Value::Utf8(_))), frame).await?;
        if equal == Some(true) { nil(frame) } else { ready_output(left, frame) }
    },frame)
}
fn greatest<'a>(greatest: bool,args: &'a [Expr],frame: &'a EvaluationFrame<'a>) -> Result<Pending<'a,Continuation<'a>>> {
    boxed(async move {
        let Some(first) = args.first() else { return nil(frame); };
        if args.len() == 1 { return prepare(first,frame)?.await; }
        let first = prepare(first,frame)?.await?;
        if first.is_null() { return nil(frame); }
        let mut best = first.demand_output().await?;
        for expr in &args[1..] {
            let next = prepare(expr,frame)?.await?;
            if next.is_null() { return nil(frame); }
            let next = next.demand_output().await?;
            let unordered = [best.number(), next.number()].into_iter().flatten().any(|number| number.as_f64().is_some_and(f64::is_nan));
            let ordering = if unordered { std::cmp::Ordering::Equal } else { computed::order(&best, &next, frame).await? };
            if ordering != if greatest { std::cmp::Ordering::Greater } else { std::cmp::Ordering::Less } { best = next; }
        }
        ready_output(best,frame)
    },frame)
}
fn payload_truth(value: &EvalValue<'_>) -> Option<bool> {
    if is_null(value) { None } else { Some(!matches!(value,EvalValue::Scalar(Value::Bool(false)))) }
}

fn call<'a>(function: ResolvedFunction<'a>, args: &'a [Expr], ty: LogicalType, frame: &'a EvaluationFrame<'a>) -> Result<Pending<'a, Continuation<'a>>> {
    boxed(async move {
        if function == ResolvedFunction::Known(ScalarFunction::Temporal(TemporalFunction::DateBin)) {
            frame.context.reserve(4096)?;
            let stride = prepare(&args[0], frame)?.await?;
            let stride = stride.demand().await?;
            let origin = if let Some(origin) = args.get(2) {
                let origin = prepare(origin, frame)?.await?;
                origin.demand().await?
            } else { scalar(Value::Timestamp { value: 0, unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()) }) };
            if is_null(&stride) || is_null(&origin) { return nil(frame); }
            let source = prepare(&args[1], frame)?.await?;
            if source.is_null() { return nil(frame); }
            return deferred(ty, async move { calls::apply(function, vec![stride, source.demand().await?, origin], frame)?.await }, frame);
        }
        if function == ResolvedFunction::Known(ScalarFunction::Random) && args.is_empty() {
            return deferred_output(ty, async move {
                frame.context.reserve(4096)?;
                crate::volatile::random().map(|value| Output::Number(NumericCell::F64(value)))
            }, frame);
        }
        let shortcut_null = function.shortcut_null();
        frame.context.reserve(args.len().checked_mul(std::mem::size_of::<Continuation<'_>>()).ok_or_else(overflow)?)?;
        let mut arguments = Vec::with_capacity(args.len());
        for expr in args {
            let argument = prepare(expr, frame)?.await?;
            if argument.is_null() && shortcut_null { return nil(frame); }
            arguments.push(argument);
        }
        if function == ResolvedFunction::Known(ScalarFunction::String(StringFunction::Concat))
            && plain_type(&ty) == &LogicalType::Scalar(ScalarType::Utf8)
        {
            return deferred(ty, async move {
                frame.context.reserve(arguments.len().checked_mul(std::mem::size_of::<Output<'_>>()).ok_or_else(overflow)?)?;
                let mut inputs = Vec::with_capacity(arguments.len());
                for argument in arguments {
                    let output = argument.demand_output().await?;
                    inputs.push(match output {
                        Output::Text(..) => output,
                        other => Output::Value(other.into_value()?),
                    });
                }
                if inputs.iter().all(|input| input.text().is_some()) {
                    return crate::string::concat_utf8(inputs.iter().filter_map(Output::text), frame.context).map(scalar);
                }
                frame.context.reserve(inputs.len().checked_mul(std::mem::size_of::<EvalValue<'_>>()).ok_or_else(overflow)?)?;
                let values = inputs.into_iter().map(Output::into_value).collect::<Result<Vec<_>>>()?;
                calls::apply(function, values, frame)?.await
            }, frame);
        }
        if let ResolvedFunction::Known(ScalarFunction::Interval(interval_fn)) = function {
            let name = interval_fn.canonical_name();
            let recipe = rtdb_plan::interval_constructor(name, args)?.ok_or_else(|| invalid(name))?;
            let input = arguments.into_iter().next().ok_or_else(|| invalid(name))?;
            return deferred(ty, async move {
                let value = input.demand().await?;
                from_value(crate::interval_constructor::construct(scalar_ref(&value)?, &recipe)?, frame)
            }, frame);
        }
        if matches!(function, ResolvedFunction::Known(ScalarFunction::NullEq | ScalarFunction::DistinctFrom)) && arguments.len() == 2 {
            rtdb_plan::validate_equality_types(&arguments[0].ty, &arguments[1].ty)?;
        }
        if let [argument] = arguments.as_slice() {
            if matches!(function, ResolvedFunction::Known(ScalarFunction::MetaDouble | ScalarFunction::GetField))
                && rtdb_plan::function_type(&function.canonical_name(), std::slice::from_ref(&argument.ty))? == LogicalType::Null
            {
                return nil(frame);
            }
            let boolean = argument.ty == LogicalType::Scalar(rtdb_types::ScalarType::Bool);
            let constant = match function {
                ResolvedFunction::Known(ScalarFunction::NilQ | ScalarFunction::IsNull) => Some(argument.is_null()),
                ResolvedFunction::Known(ScalarFunction::TrueQ | ScalarFunction::IsTrue | ScalarFunction::FalseQ | ScalarFunction::IsFalse) if !boolean => Some(false),
                ResolvedFunction::Known(ScalarFunction::Boolean) if !boolean => Some(!argument.is_null()),
                _ => None,
            };
            if let Some(value) = constant { return ready(scalar(Value::Bool(value)), frame); }
        }
        if function == ResolvedFunction::Known(ScalarFunction::LikeRegex) {
            if arguments.len() != 3 { return Err(invalid("like_regex")); }
            let _flags = arguments.pop().ok_or_else(|| invalid("like_regex"))?;
            let pattern = arguments.pop().ok_or_else(|| invalid("like_regex"))?;
            let input = arguments.pop().ok_or_else(|| invalid("like_regex"))?;
            let flags = match &args[2] { Expr::Literal(Value::Utf8(flags)) => flags.as_str(), _ => "" };
            return deferred(ty, async move {
                let pattern = pattern.demand().await?;
                let Value::Utf8(pattern) = scalar_ref(&pattern)? else { return Err(invalid("like_regex")); };
                let pattern = crate::string::pattern::compile(pattern, flags, frame.context)?;
                let input = input.demand().await?;
                let Value::Utf8(input) = scalar_ref(&input)? else { return Err(invalid("like_regex")); };
                pattern.search(input, frame.context).map(|value| scalar(Value::Bool(value)))
            }, frame);
        }
        if let ResolvedFunction::Known(ScalarFunction::String(func @ (StringFunction::Like | StringFunction::Ilike))) = function {
            let name = func.canonical_name();
            let case_insensitive = func == StringFunction::Ilike;
            let pattern = arguments.pop().ok_or_else(|| invalid(name))?;
            let input = arguments.pop().ok_or_else(|| invalid(name))?;
            return deferred(ty, async move {
                let pattern = pattern.demand().await?;
                let scratch = frame.context.allocation.map(crate::EvaluationScope::scratch);
                let context = crate::EvalContext { allocation: scratch.as_ref().or(frame.context.allocation), ..*frame.context };
                let pattern = crate::string::pattern::compile_like(scalar_ref(&pattern)?, None, case_insensitive, &context)?;
                let input = input.demand().await?;
                pattern.search(scalar_ref(&input)?, false, &context).map(scalar)
            }, frame);
        }
        if let ResolvedFunction::Known(ScalarFunction::Collection(func @ (CollectionFunction::ArrayLower | CollectionFunction::ArrayUpper | CollectionFunction::ArrayLength))) = function {
            let name = func.canonical_name();
            let dimension = arguments.pop().expect("validated array dimension");
            let input = arguments.pop().expect("validated array input");
            return deferred(ty, async move {
                let dimension = dimension.demand().await?;
                let dimension = integer(scalar_ref(&dimension)?).and_then(|value| i64::try_from(value).ok()).ok_or_else(|| invalid(name))?;
                crate::collection::dimension(name, dimension)?;
                if func == CollectionFunction::ArrayLower { return Ok(scalar(Value::I32(1))); }
                Ok(scalar(Value::I32(size(&input.demand().await?)?)))
            }, frame);
        }
        if function == ResolvedFunction::Known(ScalarFunction::NullEq) && arguments.iter().any(Continuation::is_null) {
            return ready(scalar(Value::Bool(arguments.iter().all(Continuation::is_null))), frame);
        }
        if function == ResolvedFunction::Known(ScalarFunction::DistinctFrom) && arguments.iter().any(Continuation::is_null) {
            return ready(scalar(Value::Bool(!arguments.iter().all(Continuation::is_null))), frame);
        }
        if matches!(function, ResolvedFunction::Known(ScalarFunction::CompareNullsFirst | ScalarFunction::CompareNullsLast)) && arguments.iter().any(Continuation::is_null) {
            let nulls_first = function == ResolvedFunction::Known(ScalarFunction::CompareNullsFirst);
            let result = if arguments.iter().all(Continuation::is_null) { 0 }
                else if arguments[0].is_null() == nulls_first { -1 } else { 1 };
            return ready(scalar(Value::I32(result)), frame);
        }
        if function == ResolvedFunction::Known(ScalarFunction::StrictEq) && arguments.iter().all(Continuation::is_null) {
            return ready(scalar(Value::Bool(true)), frame);
        }
        if let ResolvedFunction::Known(func @ (ScalarFunction::NullEq | ScalarFunction::StrictEq | ScalarFunction::DistinctFrom
            | ScalarFunction::Compare | ScalarFunction::CompareNullsFirst | ScalarFunction::CompareNullsLast | ScalarFunction::Mod)) = function
            && arguments.len() == 2
        {
            let right = arguments.pop().unwrap();
            let left = arguments.pop().unwrap();
            return deferred_output(ty, async move {
                let left = left.demand_output().await?;
                let right = right.demand_output().await?;
                if func == ScalarFunction::Mod {
                    return computed::arithmetic(BinaryOp::Modulo, left, right, frame).await;
                }
                if left.has_wide_decimal() || right.has_wide_decimal() {
                    let value = match func {
                        ScalarFunction::NullEq | ScalarFunction::StrictEq | ScalarFunction::DistinctFrom => {
                            let equal = computed::equal(&left, right, func == ScalarFunction::StrictEq, false, frame).await? == Some(true);
                            Value::Bool(if func == ScalarFunction::DistinctFrom { !equal } else { equal })
                        }
                        _ => {
                            let comparator_input = |input: Output<'a>| match input {
                                Output::Decimal(decimal) => decimal.as_f64(frame.context).map(Value::F64),
                                input => input.into_value().and_then(|value| match value {
                                    EvalValue::Scalar(value) => Ok(value),
                                    _ => Err(invalid("compare")),
                                }),
                            };
                            let left = comparator_input(left)?;
                            let right = comparator_input(right)?;
                            Value::I32(crate::ordered::scalar_compare(
                                &left, &right, func == ScalarFunction::CompareNullsFirst, frame.context.timezone,
                            )?)
                        }
                    };
                    return Ok(Output::Value(scalar(value)));
                }
                frame.context.reserve(2 * std::mem::size_of::<EvalValue<'_>>())?;
                calls::apply(function, vec![left.into_value()?, right.into_value()?], frame)?.await.map(Output::Value)
            }, frame);
        }
        let round_with_scale = function == ResolvedFunction::Known(ScalarFunction::Round) && arguments.len() == 2;
        let precision = arguments.first().and_then(|arg| match &arg.ty {
            LogicalType::Scalar(ScalarType::Decimal { precision, .. }) => Some(*precision),
            LogicalType::Scalar(ScalarType::I32) if round_with_scale => Some(10),
            LogicalType::Scalar(ScalarType::I64) if round_with_scale => Some(19),
            _ => None,
        });
        if let ResolvedFunction::Known(func @ (ScalarFunction::Abs | ScalarFunction::Round | ScalarFunction::Floor | ScalarFunction::Ceil | ScalarFunction::Ceiling)) = function
            && let Some(precision) = precision
        {
            let explicit_scale = if arguments.len() == 2 { arguments.pop() } else { None };
            let input = arguments.pop().expect("validated decimal argument");
            return deferred_output(ty, async move {
                let decimal = input.demand_output().await?.into_decimal(frame)?;
                return match func {
                    ScalarFunction::Abs => Ok(Output::Decimal(decimal.abs())),
                    ScalarFunction::Round => {
                        let scale = match explicit_scale {
                            Some(scale) => {
                                let scale = scale.demand().await?;
                                integer(scalar_ref(&scale)?).and_then(|value| i32::try_from(value).ok()).ok_or_else(overflow)?
                            }
                            None => 0,
                        };
                        let params = DecimalParameters { scale, precision, ..decimal.params };
                        decimal.rescale(params, bigdecimal::RoundingMode::HalfUp, frame.context).map(Output::Decimal)
                    }
                    _ => {
                        let number = decimal.as_f64(frame.context)?;
                        Ok(Output::Number(NumericCell::F64(if func == ScalarFunction::Floor { number.floor() } else { number.ceil() })))
                    }
                };
            }, frame);
        }
        if matches!(function, ResolvedFunction::Known(ScalarFunction::ColDescription | ScalarFunction::PgIndexamHasProperty))
            || matches!(function, ResolvedFunction::Known(ScalarFunction::FormatType | ScalarFunction::String(StringFunction::StringToArray))) && arguments.first().is_some_and(Continuation::is_null)
        {
            return nil(frame);
        }
        if function == ResolvedFunction::Known(ScalarFunction::Round) && arguments.len() == 2 && matches!(arguments[0].ty, LogicalType::Scalar(rtdb_types::ScalarType::F32 | rtdb_types::ScalarType::F64)) {
            let scale = arguments.pop().expect("validated scale");
            let input = arguments.pop().expect("validated input");
            return deferred(ty, async move {
                let input = input.demand().await?;
                let number = crate::numeric::number(scalar_ref(&input)?).ok_or_else(|| invalid("round"))?;
                if !number.is_finite() { return Ok(scalar(Value::F64(number))); }
                frame.context.reserve(2 * std::mem::size_of::<EvalValue<'_>>())?;
                calls::apply(function, vec![input, scale.demand().await?], frame)?.await
            }, frame);
        }
        if let ResolvedFunction::Known(ScalarFunction::FloatMath(math)) = function && matches!(arguments.len(), 1 | 2) {
            let second = if arguments.len() == 2 { arguments.pop() } else { None };
            let first = arguments.pop().expect("validated numeric argument");
            return deferred_output(ty, async move {
                let (left, right) = if math == FloatMath::Log && second.is_some() {
                    let right = second.unwrap().demand_output().await?;
                    (first.demand_output().await?, Some(right))
                } else {
                    let left = first.demand_output().await?;
                    let right = match second { Some(value) => Some(value.demand_output().await?), None => None };
                    (left, right)
                };
                let count = 1 + usize::from(right.is_some());
                frame.context.reserve(count * std::mem::size_of::<EvalValue<'_>>())?;
                let numbers = [left.number(), right.as_ref().and_then(Output::number)];
                let types = [left.scalar_type(), right.as_ref().and_then(Output::scalar_type)].map(|ty| ty.map(LogicalType::Scalar));
                if types[..count].iter().all(Option::is_some) {
                    frame.context.reserve(count * std::mem::size_of::<Value>())?;
                    let bytes = numbers[..count].iter().flatten().copied().map(number_value_bytes).sum::<usize>();
                    frame.context.reserve((bytes + 256) * 16)?;
                    let types = types.map(|ty| ty.unwrap_or(LogicalType::Null));
                    rtdb_plan::function_type(math.canonical_name(), &types[..count])?;
                    let x = left.as_f64(frame)?.expect("numeric argument");
                    let y = match &right { Some(right) => right.as_f64(frame)?, None => None };
                    return math.apply(x, y).map(|value| Output::Number(NumericCell::F64(value)));
                }
                let mut values = Vec::with_capacity(count);
                values.push(left.into_value()?);
                if let Some(right) = right { values.push(right.into_value()?); }
                calls::apply(function, values, frame)?.await.map(Output::Value)
            }, frame);
        }
        let selects_element = function == ResolvedFunction::Known(ScalarFunction::Nth);
        if selects_element { rtdb_plan::function_type("nth", &[arguments[0].ty.clone(), arguments[1].ty.clone()])?; }
        if selects_element && !matches!(arguments[0].ty, LogicalType::List { kind: ListKind::List | ListKind::FixedSizeList(_), .. }) { return nil(frame); }
        let demand_null = function == ResolvedFunction::Known(ScalarFunction::String(StringFunction::Format));
        let payload = async move {
            frame.context.reserve(arguments.len().checked_mul(std::mem::size_of::<EvalValue<'_>>()).ok_or_else(overflow)?)?;
            let mut values = Vec::with_capacity(arguments.len());
            for (index, argument) in arguments.into_iter().enumerate() {
                if argument.is_null() && !demand_null {
                    values.push(scalar(Value::Null));
                    continue;
                }
                let output = argument.demand_output().await?;
                let renders_text = function == ResolvedFunction::Known(ScalarFunction::String(StringFunction::Str))
                    || (index > 0 && function == ResolvedFunction::Known(ScalarFunction::String(StringFunction::Format)));
                values.push(match output {
                    Output::Decimal(decimal) if renders_text => scalar(decimal.text(frame.context)?),
                    other => other.into_value()?,
                });
            }
            calls::apply(function, values, frame)?.await
        };
        if selects_element { ready(payload.await?, frame) } else { deferred(ty, payload, frame) }
    }, frame)
}

#[cfg(test)]
mod tests {
    use arrow_array::StringArray;
    use arrow_schema::DataType;
    use rtdb_columnar::ArrowCell;
    use rtdb_plan::{BinaryOp, CastTarget, ColumnId, Expr, Field, RegexFormBuilder, RegexReplacement, Schema, function_type_with_args};
    use rtdb_types::{Anomaly, LogicalType, Result, ScalarType, Value};

    use super::{EvalContext, EvaluationFrame, ExpressionRow, Output, evaluate, is_null, materialize, prepare};

    struct TextRow<'a>(ArrowCell<'a>);

    impl crate::RowInput for TextRow<'_> {
        fn arrow_cell(&self, index: usize) -> Option<ArrowCell<'_>> {
            (index == 0).then_some(self.0)
        }

        fn value(&self, _: usize, _: Option<&crate::EvaluationScope>) -> Result<Option<Value>> {
            Err(Anomaly::fault("test/scalar-read", "Expected an Arrow text cell"))
        }
    }

    #[tokio::test]
    async fn arrow_coalesce_text_lane_borrows_the_winner_through_scalar_continuations() {
        let data_type = LogicalType::Scalar(ScalarType::Utf8);
        let field = Field { id: ColumnId(0), name: "source".into(), qualifier: None,
            qualified_only: false, unnamed: false, data_type: data_type.clone().maybe() };
        let column = field.expression();
        let schema = Schema::new(vec![field]).unwrap();
        let field = arrow_schema::Field::new("source", DataType::Utf8, true);
        let original = "e\u{301}\0😀";
        let array = StringArray::from(vec![Some("unused"), Some(original), None]);
        let context = EvalContext::default();
        let failing_text = Expr::Cast {
            expr: Box::new(Expr::binary(BinaryOp::Divide, Expr::Literal(Value::I64(1)), Expr::Literal(Value::I64(0)))),
            target: CastTarget::fixed(data_type),
        };
        let coalesce = Expr::Coalesce(vec![column.clone(), failing_text.clone()]);
        let suffix = Expr::Literal(Value::Utf8("—ß".into()));
        let named_concat = |args: Vec<Expr>| Expr::Call {
            name: "concat".into(), return_type: function_type_with_args("concat", &args).unwrap(), args,
        };

        let input = TextRow(ArrowCell { field: &field, array: &array, row: 1 });
        let frame = EvaluationFrame { schema: &schema, row: ExpressionRow::Borrowed(&input), context: &context,
            asynchronous: true, replacements: None };
        let Output::Text(winner, _) = prepare(&coalesce, &frame).unwrap().await.unwrap().demand_output().await.unwrap() else {
            panic!("Expected COALESCE to keep the original borrowed string");
        };
        assert_eq!(winner.as_ptr(), array.value(1).as_ptr());
        for expression in [
            Expr::binary(BinaryOp::Concat, coalesce.clone(), suffix.clone()),
            named_concat(vec![coalesce.clone(), suffix.clone()]),
        ] {
            assert_eq!(materialize(evaluate(&expression, &frame).unwrap().await.unwrap(), &frame).unwrap().await.unwrap(),
                Value::Utf8(format!("{original}—ß")));
        }

        let form = |text: &str| {
            let mut builder = RegexFormBuilder::new_untracked();
            builder.literal(&Value::Utf8(text.into())).unwrap();
            builder.finish().unwrap()
        };
        let replaced = Expr::regex_replace(coalesce.clone(), RegexReplacement {
            pattern: form("😀"), replacement: form("Q"), ..Default::default()
        });
        let invocation = crate::ExpressionInvocation::prepare(&replaced, &schema, &context).unwrap();
        let value = invocation.evaluate_projection_row_async(ExpressionRow::Borrowed(&input), &context).await.unwrap();
        assert_eq!(value, Value::Utf8("e\u{301}\0Q".into()));

        let input = TextRow(ArrowCell { field: &field, array: &array, row: 2 });
        let frame = EvaluationFrame { row: ExpressionRow::Borrowed(&input), ..frame };
        let error = prepare(&coalesce, &frame).unwrap().await.unwrap().demand().await.err().unwrap();
        assert_eq!(error.code, "xtdb.expression/division-by-zero");
        for expression in [
            Expr::binary(BinaryOp::Concat, column.clone(), failing_text.clone()),
            named_concat(vec![column, failing_text]),
        ] {
            let result = evaluate(&expression, &frame).unwrap().await.unwrap();
            assert!(is_null(&result));
        }
    }
}
