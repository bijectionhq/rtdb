use super::*;

pub(super) fn ordinary_call(name: &str) -> bool {
    let matches = |candidate: &str| name.bytes().map(|byte| if byte == b'-' { b'_' } else { byte.to_ascii_lowercase() }).eq(candidate.bytes());
    ["abs","floor","ceil","ceiling","round","power","sqrt","exp","ln","log","log10","sin","cos","tan","asin","acos","atan","sinh","cosh","tanh","double","sleep",
        "lower","upper","length","cardinality","reverse","trim","trim_leading","trim_trailing","substring","overlay","replace","position","octet_position","quote_ident","parse_ident","character_length","octet_length","default_overlay_length",
        "_iid","keyword","namespace","local_name","current_schemas","current_setting","uri_scheme","uri_user_info","uri_host","uri_port","uri_path","uri_query","uri_fragment",
        "like_regex","_patch","boolean","true?","false?","nil?","is_null","null_eq","===","str","concat","format","format_type","string_to_array","period","lower_inf","upper_inf","coalesce","nullif","greatest","least"].into_iter().any(matches)
        || ["contains","strictly_contains","overlaps","strictly_overlaps","equals","precedes","strictly_precedes","immediately_precedes","succeeds","strictly_succeeds","immediately_succeeds","leads","strictly_leads","immediately_leads","lags","strictly_lags","immediately_lags"]
            .into_iter().any(|candidate| name.strip_suffix('?').unwrap_or(name).bytes().map(|byte| if byte == b'-' { b'_' } else { byte.to_ascii_lowercase() }).eq(candidate.bytes()))
}

pub(super) struct Continuation<'a> { ty: LogicalType, payload: Pending<'a,EvalValue<'a>> }
impl<'a> Continuation<'a> {
    pub(super) fn is_null(&self) -> bool { self.ty == LogicalType::Null }
    pub(super) fn is_list(&self) -> bool { matches!(self.ty, LogicalType::List { kind: ListKind::List | ListKind::Set | ListKind::FixedSizeList(_), .. }) }
    pub(super) async fn demand(self) -> Result<EvalValue<'a>> { self.payload.await }
}
fn deferred<'a>(ty: LogicalType,payload: impl Future<Output=Result<EvalValue<'a>>> + Send + 'a,frame: &EvaluationFrame<'_>) -> Result<Continuation<'a>> {
    Ok(Continuation { ty,payload: boxed(payload,frame)? })
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
        Expr::Call { name,args,.. } => {
            if name.eq_ignore_ascii_case("coalesce") { coalesce_type(args,frame)? }
            else {
                let types = args.iter().map(child).collect::<Result<Vec<_>>>()?;
                if ["null_eq","null-eq","distinct_from","distinct-from","nullif"].iter().any(|candidate|name.eq_ignore_ascii_case(candidate)) && types.len() == 2 { rtdb_plan::validate_equality_types(&types[0],&types[1])?; }
                let arguments = args.iter().zip(types).enumerate().map(|(index,(arg,ty))| {
                    if literal_argument(name,index) {
                        frame.context.reserve(expression_type_bound(arg)?.checked_mul(8).ok_or_else(overflow)?)?;
                        frame.context.reserve(literal_payload_bound(arg)?)?;
                        Ok(arg.clone())
                    } else { Ok(Expr::Parameter { index: 0,data_type: ty }) }
                }).collect::<Result<Vec<_>>>()?;
                rtdb_plan::function_type_with_args(name,&arguments)?
            }
        }
        Expr::Like { expr,pattern,case_insensitive,.. } => rtdb_plan::function_type(if *case_insensitive { "ilike" } else { "like" }, &[child(expr)?,child(pattern)?])?,
        Expr::List(items) => LogicalType::list(LogicalType::join_all(&items.iter().map(child).collect::<Result<Vec<_>>>()?)),
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
        frame.context.reserve(expression_type_bound(expr)?.checked_mul(8).ok_or_else(overflow)?)?;
        let ty = emitted_type(expr,frame)?;
        let boxed_return = ty.legs().len() > 1;
        let continuation = prepare_inner(expr,ty,frame)?.await?;
        if boxed_return { ready(box_cell(continuation.demand().await?)?,frame) } else { Ok(continuation) }
    },frame)
}

fn prepare_inner<'a>(expr: &'a Expr,ty: LogicalType,frame: &'a EvaluationFrame<'a>) -> Result<Pending<'a,Continuation<'a>>> {
    boxed(async move {
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
                let operand = prepare(operand,frame)?.await?.demand().await?;
                if is_null(&operand) { return nil(frame); }
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
                let operand = prepare(operand,frame)?.await?.demand().await?;
                if is_null(&operand) { return prepare(otherwise,frame)?.await; }
                for (candidate,result) in branches {
                    let selected = prepare(candidate,frame)?.await?;
                    if selected.is_null() { continue; }
                    let mut right = selected.demand().await?;
                    let mut left = clone_value(&operand,frame)?;
                    if let (EvalValue::Scalar(left),EvalValue::Scalar(right)) = (&mut left,&mut right) {
                        super::super::coerce_comparison_literals(false,matches!(candidate,Expr::Literal(Value::Utf8(_))),left,right,frame.context.timezone)?;
                    }
                    if compare::equal(left,right,false,frame)?.await? == Some(true) { return prepare(result,frame)?.await; }
                }
                prepare(otherwise,frame)?.await
            }
            Expr::Coalesce(args) => coalesce(args,frame)?.await,
            Expr::Cast { expr: input,target } => {
                target.validate_source(&emitted_type(input,frame)?)?;
                let input = prepare(input,frame)?.await?;
                if input.is_null() { return nil(frame); }
                deferred(ty,async move { cast_value(input.demand().await?,Target::Cast(target),frame)?.await },frame)
            }
            Expr::Unary { op,expr: input } => {
                let input = prepare(input,frame)?.await?;
                if matches!(op,UnaryOp::IsNull | UnaryOp::IsUnknown | UnaryOp::IsNotNull) {
                    return ready(scalar(Value::Bool(input.is_null() != (*op == UnaryOp::IsNotNull))),frame);
                }
                if matches!(op,UnaryOp::IsTrue | UnaryOp::IsFalse) && input.ty != LogicalType::Scalar(rtdb_types::ScalarType::Bool) { return ready(scalar(Value::Bool(false)),frame); }
                if input.is_null() { return nil(frame); }
                deferred(ty,async move {
                    let input = materialize(input.demand().await?,frame)?.await?;
                    from_value(super::super::unary_value(*op,input)?,frame)
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
                deferred(ty,async move {
                    let mut left = if left.is_null() { scalar(Value::Null) } else { left.demand().await? };
                    let mut right = if right.is_null() { scalar(Value::Null) } else { right.demand().await? };
                    if matches!(op,BinaryOp::Eq | BinaryOp::NotEq | BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq) { calls::literal_coercion(left_expr,right_expr,&mut left,&mut right,frame)?; }
                    calls::binary_values(*op,left,right,frame)?.await
                },frame)
            }
            Expr::Call { name,args,.. } => {
                frame.context.reserve(name.len().checked_mul(8).and_then(|bytes|bytes.checked_add(256)).ok_or_else(overflow)?)?;
                let name = name.to_lowercase().replace('-',"_");
                rtdb_plan::function_type_with_args(&name,args)?;
                match name.as_str() {
                    "coalesce" => coalesce(args,frame)?.await,
                    "nullif" => nullif(args,frame)?.await,
                    "greatest" | "least" => greatest(name == "greatest",args,frame)?.await,
                    "trim_array" | "set" => ready(evaluate(expr,frame)?.await?,frame),
                    _ => call(name,args,ty,frame)?.await,
                }
            }
            Expr::RegexReplace { source,.. } => {
                let source = prepare(source,frame)?.await?;
                if source.is_null() { return nil(frame); }
                deferred(ty,async move {
                    let source = source.demand().await?;
                    let Value::Utf8(source) = scalar_ref(&source)? else { return Err(invalid("regexp_replace")); };
                    frame.replacements.ok_or_else(||Anomaly::fault("rtdb.expr/replacement-binding","Replacement invocation is unavailable"))?.get(expr)?.replace(source,frame.context).map(scalar)
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
                    return ready(box_cell(from_arrow(cell, frame)?)?, frame);
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
            Expr::Literal(_) | Expr::DurationLiteral(_) | Expr::In {..} => deferred(ty,async move { evaluate(expr,frame)?.await },frame),
        }
    },frame)
}

async fn between_bound<'a>(operand: &EvalValue<'a>,bound: &'a Expr,op: BinaryOp,frame: &'a EvaluationFrame<'a>) -> Result<Option<bool>> {
    let bound_expr = bound;
    let bound = prepare(bound,frame)?.await?;
    if bound.is_null() { return Ok(None); }
    let mut right = bound.demand().await?;
    let mut left = clone_value(operand,frame)?;
    if let (EvalValue::Scalar(left),EvalValue::Scalar(right)) = (&mut left,&mut right) {
        super::super::coerce_comparison_literals(false,matches!(bound_expr,Expr::Literal(Value::Utf8(_))),left,right,frame.context.timezone)?;
    }
    Ok(payload_truth(&calls::binary_values(op,left,right,frame)?.await?))
}
async fn between_bounds<'a>(operand: &EvalValue<'a>,low: &'a Expr,high: &'a Expr,frame: &'a EvaluationFrame<'a>) -> Result<Option<bool>> {
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
            if !value.is_null() { return ready(value.demand().await?,frame); }
        }
        nil(frame)
    },frame)
}
fn nullif<'a>(args: &'a [Expr],frame: &'a EvaluationFrame<'a>) -> Result<Pending<'a,Continuation<'a>>> {
    boxed(async move {
        let left = prepare(&args[0],frame)?.await?.demand().await?;
        if is_null(&left) { return ready(left,frame); }
        let right = prepare(&args[1],frame)?.await?;
        if right.is_null() { return ready(left,frame); }
        let mut right = right.demand().await?;
        let mut left = left;
        if let (EvalValue::Scalar(left),EvalValue::Scalar(right)) = (&mut left,&mut right) {
            super::super::coerce_comparison_literals(false,matches!(&args[1],Expr::Literal(Value::Utf8(_))),left,right,frame.context.timezone)?;
        }
        let equal = compare::equal(clone_value(&left,frame)?,right,false,frame)?.await?;
        ready(if equal == Some(true) { scalar(Value::Null) } else { left },frame)
    },frame)
}
fn greatest<'a>(greatest: bool,args: &'a [Expr],frame: &'a EvaluationFrame<'a>) -> Result<Pending<'a,Continuation<'a>>> {
    boxed(async move {
        let Some(first) = args.first() else { return nil(frame); };
        if args.len() == 1 { return prepare(first,frame)?.await; }
        let first = prepare(first,frame)?.await?;
        if first.is_null() { return nil(frame); }
        let mut best = first.demand().await?;
        for expr in &args[1..] {
            let next = prepare(expr,frame)?.await?;
            if next.is_null() { return nil(frame); }
            let next = next.demand().await?;
            let unordered = [&best,&next].iter().any(|value| matches!(value,EvalValue::Scalar(value) if crate::numeric::number(value).is_some_and(f64::is_nan)));
            let ordering = if unordered { std::cmp::Ordering::Equal } else { compare::order(clone_value(&best,frame)?,clone_value(&next,frame)?,frame)?.await? };
            if ordering != if greatest { std::cmp::Ordering::Greater } else { std::cmp::Ordering::Less } { best = next; }
        }
        ready(best,frame)
    },frame)
}
fn payload_truth(value: &EvalValue<'_>) -> Option<bool> {
    if is_null(value) { None } else { Some(!matches!(value,EvalValue::Scalar(Value::Bool(false)))) }
}
fn literal_argument(name: &str,index: usize) -> bool {
    let name = name.to_lowercase().replace('-',"_");
    match name.as_str() {
        "round" => index == 1,"extract" | "date_trunc" => index == 0 || index == 2,
        "single_field_interval" | "multi_field_interval" => index != 0,
        "current_timestamp" | "now" | "current_time" | "local_timestamp" | "localtimestamp" | "local_time" | "localtime" => true,
        _ => false,
    }
}

fn call<'a>(name: String,args: &'a [Expr],ty: LogicalType,frame: &'a EvaluationFrame<'a>) -> Result<Pending<'a,Continuation<'a>>> {
    boxed(async move {
        if name == "date_bin" {
            frame.context.reserve(4096)?;
            let stride = prepare(&args[0],frame)?.await?;
            let stride = stride.demand().await?;
            let origin = if let Some(origin) = args.get(2) {
                let origin = prepare(origin,frame)?.await?;
                origin.demand().await?
            } else { scalar(Value::Timestamp { value: 0,unit: rtdb_types::TimeUnit::Microsecond,timezone: Some("UTC".into()) }) };
            if is_null(&stride) || is_null(&origin) { return nil(frame); }
            let source = prepare(&args[1],frame)?.await?;
            if source.is_null() { return nil(frame); }
            return deferred(ty,async move { calls::apply(name,vec![stride,source.demand().await?,origin],frame)?.await },frame);
        }
        let shortcut_null = !matches!(name.as_str(),"is_true" | "is_false" | "is_null" | "true?" | "false?" | "nil?" | "boolean" | "===" | "null_eq" | "compare_nulls_first" | "compare_nulls_last" | "distinct_from" | "period" | "str" | "_patch" | "string_to_array" | "format_type" | "format");
        frame.context.reserve(args.len().checked_mul(std::mem::size_of::<Continuation<'_>>()).ok_or_else(overflow)?)?;
        let mut arguments = Vec::with_capacity(args.len());
        for expr in args {
            let argument = prepare(expr,frame)?.await?;
            if argument.is_null() && shortcut_null { return nil(frame); }
            arguments.push(argument);
        }
        if crate::interval_constructor::is_name(&name) {
            let recipe = rtdb_plan::interval_constructor(&name,args)?.ok_or_else(|| invalid(&name))?;
            let input = arguments.into_iter().next().ok_or_else(|| invalid(&name))?;
            return deferred(ty,async move {
                let value = input.demand().await?;
                from_value(crate::interval_constructor::construct(scalar_ref(&value)?,&recipe)?,frame)
            },frame);
        }
        if matches!(name.as_str(),"null_eq" | "distinct_from") && arguments.len() == 2 { rtdb_plan::validate_equality_types(&arguments[0].ty,&arguments[1].ty)?; }
        if let [argument] = arguments.as_slice() {
            if matches!(name.as_str(),"_meta_double" | "get_field") && rtdb_plan::function_type(&name,std::slice::from_ref(&argument.ty))? == LogicalType::Null { return nil(frame); }
            let boolean = argument.ty == LogicalType::Scalar(rtdb_types::ScalarType::Bool);
            let constant = match name.as_str() {
                "nil?" | "is_null" => Some(argument.is_null()),
                "true?" | "is_true" | "false?" | "is_false" if !boolean => Some(false),
                "boolean" if !boolean => Some(!argument.is_null()),
                _ => None,
            };
            if let Some(value) = constant { return ready(scalar(Value::Bool(value)),frame); }
        }
        if name == "like_regex" {
            if arguments.len() != 3 { return Err(invalid(&name)); }
            let _flags = arguments.pop().ok_or_else(||invalid(&name))?;
            let pattern = arguments.pop().ok_or_else(||invalid(&name))?;
            let input = arguments.pop().ok_or_else(||invalid(&name))?;
            let flags = match &args[2] { Expr::Literal(Value::Utf8(flags)) => flags.as_str(), _ => "" };
            return deferred(ty,async move {
                let pattern = pattern.demand().await?;
                let Value::Utf8(pattern) = scalar_ref(&pattern)? else { return Err(invalid("like_regex")); };
                let pattern = crate::string::pattern::compile(pattern,flags,frame.context)?;
                let input = input.demand().await?;
                let Value::Utf8(input) = scalar_ref(&input)? else { return Err(invalid("like_regex")); };
                pattern.search(input,frame.context).map(|value|scalar(Value::Bool(value)))
            },frame);
        }
        if matches!(name.as_str(),"like" | "ilike") {
            let pattern = arguments.pop().ok_or_else(||invalid(&name))?;
            let input = arguments.pop().ok_or_else(||invalid(&name))?;
            return deferred(ty,async move {
                let pattern = pattern.demand().await?;
                let scratch = frame.context.allocation.map(crate::EvaluationScope::scratch);
                let context = crate::EvalContext { allocation: scratch.as_ref().or(frame.context.allocation), ..*frame.context };
                let pattern = crate::string::pattern::compile_like(scalar_ref(&pattern)?,None,name == "ilike",&context)?;
                let input = input.demand().await?;
                pattern.search(scalar_ref(&input)?,false,&context).map(scalar)
            },frame);
        }
        if matches!(name.as_str(),"array_lower" | "array_upper" | "array_length") {
            let dimension = arguments.pop().expect("validated array dimension");
            let input = arguments.pop().expect("validated array input");
            return deferred(ty,async move {
                let dimension = dimension.demand().await?;
                let dimension = integer(scalar_ref(&dimension)?).and_then(|value| i64::try_from(value).ok()).ok_or_else(|| invalid(&name))?;
                crate::collection::dimension(&name,dimension)?;
                if name == "array_lower" { return Ok(scalar(Value::I32(1))); }
                Ok(scalar(Value::I32(size(&input.demand().await?)?)))
            },frame);
        }
        if name == "null_eq" && arguments.iter().any(Continuation::is_null) {
            return ready(scalar(Value::Bool(arguments.iter().all(Continuation::is_null))),frame);
        }
        if name == "distinct_from" && arguments.iter().any(Continuation::is_null) {
            return ready(scalar(Value::Bool(!arguments.iter().all(Continuation::is_null))),frame);
        }
        if matches!(name.as_str(),"compare_nulls_first" | "compare_nulls_last") && arguments.iter().any(Continuation::is_null) {
            let result = if arguments.iter().all(Continuation::is_null) { 0 }
                else if arguments[0].is_null() == (name == "compare_nulls_first") { -1 } else { 1 };
            return ready(scalar(Value::I32(result)),frame);
        }
        if name == "===" && arguments.iter().all(Continuation::is_null) { return ready(scalar(Value::Bool(true)),frame); }
        if matches!(name.as_str(),"col_description" | "pg_indexam_has_property") || matches!(name.as_str(),"format_type" | "string_to_array") && arguments.first().is_some_and(Continuation::is_null) { return nil(frame); }
        if name == "round" && arguments.len() == 2 && matches!(arguments[0].ty,LogicalType::Scalar(rtdb_types::ScalarType::F32 | rtdb_types::ScalarType::F64)) {
            let scale = arguments.pop().expect("validated scale");
            let input = arguments.pop().expect("validated input");
            return deferred(ty,async move {
                let input = input.demand().await?;
                let number = crate::numeric::number(scalar_ref(&input)?).ok_or_else(|| invalid("round"))?;
                if !number.is_finite() { return Ok(scalar(Value::F64(number))); }
                frame.context.reserve(2 * std::mem::size_of::<EvalValue<'_>>())?;
                calls::apply(name,vec![input,scale.demand().await?],frame)?.await
            },frame);
        }
        if name == "log" && arguments.len() == 2 {
            let input = arguments.pop().expect("validated input");
            let base = arguments.pop().expect("validated base");
            return deferred(ty,async move {
                let input = input.demand().await?;
                frame.context.reserve(2 * std::mem::size_of::<EvalValue<'_>>())?;
                calls::apply(name,vec![base.demand().await?,input],frame)?.await
            },frame);
        }
        let selects_element = name == "nth";
        if selects_element { rtdb_plan::function_type("nth", &[arguments[0].ty.clone(),arguments[1].ty.clone()])?; }
        if selects_element && !matches!(arguments[0].ty,LogicalType::List { kind: ListKind::List | ListKind::FixedSizeList(_),.. }) { return nil(frame); }
        let demand_null = name == "format";
        let payload = async move {
            frame.context.reserve(arguments.len().checked_mul(std::mem::size_of::<EvalValue<'_>>()).ok_or_else(overflow)?)?;
            let mut values = Vec::with_capacity(arguments.len());
            for argument in arguments { values.push(if argument.is_null() && !demand_null { scalar(Value::Null) } else { argument.demand().await? }); }
            calls::apply(name,values,frame)?.await
        };
        if selects_element { ready(payload.await?,frame) } else { deferred(ty,payload,frame) }
    },frame)
}
