use std::cmp::Ordering;
use super::*;

pub(super) fn field<'a>(value: EvalValue<'a>,name: Option<&str>,frame: &EvaluationFrame<'_>) -> Result<EvalValue<'a>> {
    match value {
        EvalValue::Struct(fields) => fields.get(name.unwrap_or("")).map(|value|clone_value(value,frame)).unwrap_or_else(||Ok(scalar(Value::Null))),
        EvalValue::Scalar(value) => from_value(crate::field::get(&value,name,frame.context)?,frame),
        _ => Ok(scalar(Value::Null)),
    }
}

pub(super) fn subscript<'a>(value: EvalValue<'a>,index: EvalValue<'a>,frame: &'a EvaluationFrame<'a>) -> Result<Pending<'a,EvalValue<'a>>> {
    boxed(async move {
        if is_null(&value) || is_null(&index) { return Ok(scalar(Value::Null)); }
        let index = integer(scalar_ref(&index)?).ok_or_else(|| invalid("nth"))?;
        if !matches!(value,EvalValue::List(Kind::List | Kind::Fixed,_)) { return Ok(scalar(Value::Null)); }
        let Ok(index) = usize::try_from(index) else { return Ok(scalar(Value::Null)); };
        if index >= len(&value)? { return Ok(scalar(Value::Null)); }
        nth(value,index,frame)?.await
    },frame)
}

pub(super) fn literal_coercion(left_expr: &Expr,right_expr: &Expr,left: &mut EvalValue<'_>,right: &mut EvalValue<'_>,frame: &EvaluationFrame<'_>) -> Result<()> {
    if let (EvalValue::Scalar(left),EvalValue::Scalar(right)) = (left,right) {
        super::super::coerce_comparison_literals(matches!(left_expr,Expr::Literal(Value::Utf8(_))),matches!(right_expr,Expr::Literal(Value::Utf8(_))),left,right,frame.context.timezone)?;
    }
    Ok(())
}

pub(super) fn binary_values<'a>(op: BinaryOp,left: EvalValue<'a>,right: EvalValue<'a>,frame: &'a EvaluationFrame<'a>) -> Result<Pending<'a,EvalValue<'a>>> {
    boxed(async move {
        use BinaryOp as B;
        if matches!((&left,&right),(EvalValue::Scalar(_),EvalValue::Scalar(_))) {
            return from_value(super::super::binary_value(op,materialize(left,frame)?.await?,materialize(right,frame)?.await?,frame.context)?,frame);
        }
        match op {
            B::Eq | B::NotEq => Ok(scalar(boolean(compare::equal(left,right,false,frame)?.await?.map(|equal| if op == B::Eq { equal } else { !equal })))),
            B::IsDistinctFrom | B::IsNotDistinctFrom => {
                let equal = if is_null(&left) || is_null(&right) { Some(is_null(&left) && is_null(&right)) }
                    else { compare::equal(left,right,false,frame)?.await? };
                Ok(scalar(boolean(equal.map(|equal| if op == B::IsNotDistinctFrom { equal } else { !equal }))))
            }
            _ if is_null(&left) || is_null(&right) => Ok(scalar(Value::Null)),
            B::Lt | B::LtEq | B::Gt | B::GtEq => {
                let order = compare::order(left,right,frame)?.await?;
                Ok(scalar(Value::Bool(match op { B::Lt => order == Ordering::Less,B::LtEq => order != Ordering::Greater,B::Gt => order == Ordering::Greater,_ => order != Ordering::Less })))
            }
            B::Concat if matches!((&left,&right),(EvalValue::List(Kind::List,_),EvalValue::List(Kind::List,_))) => {
                let data_type = value_type(&left,frame)?.join(&value_type(&right,frame)?);
                Err(rtdb_types::Anomaly::incorrect("xtdb.error/illegal-arg", format!("No matching clause: {}", crate::evaluate::vector_type_text(&data_type))))
            }
            _ => Err(invalid("binary operation")),
        }
    },frame)
}

fn type_predicate(name: &str,value: &EvalValue<'_>) -> Option<bool> {
    match name {
        "nil?" | "is_null" => Some(is_null(value)),
        "true?" | "is_true" => Some(matches!(value,EvalValue::Scalar(Value::Bool(true)))),
        "false?" | "is_false" => Some(matches!(value,EvalValue::Scalar(Value::Bool(false)))),
        "boolean" => Some(!is_null(value) && !matches!(value,EvalValue::Scalar(Value::Bool(false)))),
        _ => None,
    }
}

pub(super) fn call<'a>(name: &'a str,args: &'a [Expr],frame: &'a EvaluationFrame<'a>) -> Result<Pending<'a,EvalValue<'a>>> {
    boxed(async move {
        frame.context.reserve(name.len().checked_mul(8).and_then(|n|n.checked_add(256)).ok_or_else(overflow)?)?;
        let name = name.to_lowercase().replace('-',"_");
        frame.context.reserve(args.iter().try_fold(0usize,|bytes,expr|bytes.checked_add(expression_type_bound(expr)?).ok_or_else(overflow))?.checked_mul(8).ok_or_else(overflow)?)?;
        rtdb_plan::function_type_with_args(&name,args)?;
        if name == "_id_eq_any" {
            let id = evaluate(&args[0], frame)?.await?;
            let array = evaluate(&args[1], frame)?.await?;
            if is_null(&array) { return Ok(scalar(Value::Null)); }
            if !matches!(&array, EvalValue::List(Kind::List | Kind::Fixed, _)) { return Err(invalid(&name)); }
            let mut answer = Some(false);
            for index in 0..len(&array)? {
                let value = nth(clone_value(&array, frame)?, index, frame)?.await?;
                match compare::equal(clone_value(&id, frame)?, value, false, frame)?.await? {
                    Some(true) => return Ok(scalar(Value::Bool(true))),
                    None => answer = None,
                    Some(false) => (),
                }
            }
            return Ok(scalar(boolean(answer)));
        }
        if name == "set" { return make_list(Kind::Set,ListReader::Expressions(args),frame); }
        if name == "coalesce" {
            for expr in args { let value = evaluate(expr,frame)?.await?;if !is_null(&value) { return Ok(value); } }
            return Ok(scalar(Value::Null));
        }
        if name == "nullif" {
            let mut left = evaluate(&args[0],frame)?.await?;
            let mut right = evaluate(&args[1],frame)?.await?;
            if let (EvalValue::Scalar(left),EvalValue::Scalar(right)) = (&mut left,&mut right) {
                super::super::coerce_comparison_literals(false,matches!(&args[1],Expr::Literal(Value::Utf8(_))),left,right,frame.context.timezone)?;
            }
            return Ok(if compare::equal(clone_value(&left,frame)?,right,false,frame)?.await? == Some(true) { scalar(Value::Null) } else { left });
        }
        if name == "greatest" || name == "least" {
            let Some(first) = args.first() else { return Ok(scalar(Value::Null)); };
            let mut best = evaluate(first,frame)?.await?;
            for expr in &args[1..] {
                if is_null(&best) { return Ok(scalar(Value::Null)); }
                let next = evaluate(expr,frame)?.await?;
                if is_null(&next) { return Ok(next); }
                let unordered = [&best,&next].iter().any(|value| matches!(value,EvalValue::Scalar(value) if crate::numeric::number(value).is_some_and(f64::is_nan)));
                let ordering = if unordered { Ordering::Equal } else { compare::order(clone_value(&best,frame)?,clone_value(&next,frame)?,frame)?.await? };
                if ordering != if name == "greatest" { Ordering::Greater } else { Ordering::Less } { best = next; }
            }
            return Ok(best);
        }
        if name == "trim_array" { return trim(args,frame)?.await; }
        frame.context.reserve(args.len().checked_mul(std::mem::size_of::<EvalValue<'_>>()).ok_or_else(overflow)?)?;
        let mut values = Vec::with_capacity(args.len());
        for expr in args {
            let value = evaluate(expr,frame)?.await?;
            if matches!(name.as_str(),"concat" | "array_upper" | "array_lower" | "array_length") && is_null(&value) { return Ok(scalar(Value::Null)); }
            values.push(value);
        }
        apply(name,values,frame)?.await
    },frame)
}

fn trim<'a>(args: &'a [Expr],frame: &'a EvaluationFrame<'a>) -> Result<Pending<'a,EvalValue<'a>>> {
    boxed(async move {
        let input = super::scalar::prepare(&args[0],frame)?.await?;
        if input.is_null() { return Ok(scalar(Value::Null)); }
        let count = super::scalar::prepare(&args[1],frame)?.await?;
        if count.is_null() { return Ok(scalar(Value::Null)); }
        let input = input.demand().await?;
        if !matches!(&input,EvalValue::List(Kind::List,_)) { return Err(invalid("trim_array")); }
        let size = len(&input)?;
        let count = count.demand().await?;
        let count = integer(scalar_ref(&count)?).and_then(|count| i64::try_from(count).ok()).ok_or_else(|| invalid("trim_array"))?;
        let size = crate::collection::trim_size(size,count)?;
        if let EvalValue::List(_, reader) = &input
            && let ListReader::Stored(list) = reader.as_ref()
            && let Some(slice) = list.slice_packed(0, crate::collection::view_size(size)?)
        {
            return make_list(Kind::List, ListReader::Stored(slice), frame);
        }
        make_list(Kind::List,ListReader::Trim { input,size },frame)
    },frame)
}

pub(super) fn apply<'a>(name: String,mut values: Vec<EvalValue<'a>>,frame: &'a EvaluationFrame<'a>) -> Result<Pending<'a,EvalValue<'a>>> {
    boxed(async move {
        if name == "get_field" { return field(values.remove(0),None,frame); }
        if name == "range_bins" {
            frame.context.reserve(values.len().checked_mul(std::mem::size_of::<Value>()).ok_or_else(overflow)?)?;
            let arguments = values.iter().map(|value|scalar_ref(value).and_then(|value|super::super::retained_clone(value,frame.context))).collect::<Result<Vec<_>>>()?;
            return match crate::temporal_functions::RangeBins::prepare(&arguments,frame.context)? {
                Some(bins) => make_list(Kind::List,ListReader::RangeBins(bins),frame),None => Ok(scalar(Value::Null)),
            };
        }
        if name == "generate_series" {
            frame.context.reserve(values.len().checked_mul(std::mem::size_of::<Value>()).ok_or_else(overflow)?)?;
            let arguments = values.iter().map(|value|scalar_ref(value).and_then(|value|super::super::retained_clone(value,frame.context))).collect::<Result<Vec<_>>>()?;
            return match crate::series::prepare(&arguments,frame.context)? {
                crate::series::GeneratedSeries::Null => Ok(scalar(Value::Null)),
                crate::series::GeneratedSeries::Integer(series) => make_list(Kind::List,ListReader::IntegerSeries(series),frame),
                series => {
                    let value = if frame.asynchronous { series.materialize_async(frame.context).await? } else { series.materialize(frame.context)? };
                    from_value(value,frame)
                }
            };
        }
        if let [value] = values.as_slice() {
            if let Some(value) = type_predicate(&name,value) { return Ok(scalar(Value::Bool(value))); }
            if matches!(name.as_str(),"length" | "cardinality") {
                if is_null(value) { return Ok(scalar(Value::Null)); }
                if matches!(value,EvalValue::List(_,_)) { return Ok(scalar(Value::I32(size(value)?))); }
                let count = match value {
                    EvalValue::List(_,_) => Some(len(value)?),
                    EvalValue::Struct(fields) if name == "length" => Some(fields.values().filter(|value| !is_null(value)).count()),
                    _ => None,
                };
                if let Some(count) = count { return Ok(scalar(Value::I32(i32::try_from(count).map_err(|_| overflow())?))); }
            }
        }
        if name == "cosine_distance" && let [left, right] = values.as_slice() {
            if let (Some(left), Some(right)) = (borrowed_vector(left), borrowed_vector(right)) {
                return Ok(scalar(crate::vector::score_pair(left, right)?));
            }
        }
        if name == "nth" { return subscript(values.remove(0),values.remove(0),frame)?.await; }
        if matches!(name.as_str(),"array_upper" | "array_lower" | "array_length") {
            if values.iter().any(is_null) { return Ok(scalar(Value::Null)); }
            let count = integer(scalar_ref(&values[1])?).and_then(|count| i64::try_from(count).ok()).ok_or_else(|| invalid(&name))?;
            let input = values.remove(0);
            if !matches!(&input,EvalValue::List(Kind::List,_)) { return Err(invalid(&name)); }
            crate::collection::dimension(&name,count)?;
            return Ok(scalar(Value::I32(if name == "array_lower" { 1 } else { size(&input)? })));
        }
        if name == "===" || name == "null_eq" {
            let left = values.remove(0);let right = values.remove(0);
            let equal = if name == "null_eq" && (is_null(&left) || is_null(&right)) { is_null(&left) && is_null(&right) }
                else { compare::equal(left,right,name == "===",frame)?.await? == Some(true) };
            return Ok(scalar(Value::Bool(equal)));
        }
        if name == "distinct_from" {
            return binary_values(BinaryOp::IsDistinctFrom,values.remove(0),values.remove(0),frame)?.await;
        }
        if name == "_patch" {
            let left = values.remove(0);let right = values.remove(0);
            let EvalValue::Struct(right) = right else { return Err(invalid("_patch")); };
            let mut fields = rtdb_types::StructFields::new();
            if let EvalValue::Struct(left) = &left {
                for (key,value) in left.iter() {
                    frame.context.reserve(key.len().checked_add(4096).ok_or_else(overflow)?)?;
                    fields.insert(key.clone(),clone_value(value,frame)?);
                }
            } else if !is_null(&left) { return Err(invalid("_patch")); }
            for (key,value) in right.iter() {
                if !is_null(value) || !fields.contains_key(key) {
                    frame.context.reserve(key.len().checked_add(4096).ok_or_else(overflow)?)?;
                    fields.insert(key.clone(),clone_value(value,frame)?);
                }
            }
            frame.context.reserve(256)?;
            return Ok(EvalValue::Struct(Arc::new(fields)));
        }
        frame.context.reserve(values.len().checked_mul(std::mem::size_of::<Value>()).ok_or_else(overflow)?)?;
        let mut materialized = Vec::with_capacity(values.len());
        for value in values { materialized.push(materialize(value,frame)?.await?); }
        let result = if frame.asynchronous { crate::functions::scalar_call_async(&name,&materialized,frame.context).await? }
            else { crate::functions::scalar_call(&name,&materialized,frame.context)? };
        from_value(result,frame)
    },frame)
}



