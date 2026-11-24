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

pub(super) fn is_cosine(name: &str) -> bool {
    name.eq_ignore_ascii_case("cosine_distance") || name.eq_ignore_ascii_case("cosine-distance")
}

pub(super) fn validate(expr: &Expr, name: &str, args: &[Expr], frame: &EvaluationFrame<'_>) -> Result<()> {
    if name == "cosine_distance" {
        if args.len() != 2 { rtdb_plan::function_type(name, &[])?; }
        return Ok(());
    }
    if let Some(invocation) = frame.replacements
        && args.iter().any(|arg| invocation.list(arg).is_some())
    {
        return invocation.validate_call(expr, name, args);
    }
    rtdb_plan::function_type_with_args(name, args)?;
    Ok(())
}

enum CosineArgument<'a> { Arrow(ArrowCell<'a>), Value(Value) }

impl<'a> CosineArgument<'a> {
    async fn read(expr: &Expr, cell: Option<ArrowCell<'a>>, frame: &EvaluationFrame<'_>) -> Result<Self> {
        Ok(match cell {
            Some(cell) if cell.is_null()? => Self::Value(Value::Null),
            Some(cell) => Self::Arrow(cell),
            None => Self::Value(scalar_value(expr, frame).await?),
        })
    }

    fn is_null(&self) -> bool { matches!(self, Self::Value(value) if nullish(value)) }

    fn vector(&self) -> Result<crate::vector::VectorInput<'_>> {
        use crate::vector::VectorInput;
        match self {
            Self::Arrow(cell) => VectorInput::from_arrow(*cell).ok_or_else(crate::vector::invalid_vector_type),
            Self::Value(value) => crate::vector::components(value).map(VectorInput::Stored),
        }
    }
}

pub(super) fn call<'a>(expr: &'a Expr,name: &'a str,args: &'a [Expr],frame: &'a EvaluationFrame<'a>) -> Result<Pending<'a,EvalValue<'a>>> {
    boxed(async move {
        if is_cosine(name) && let [left, right] = args && args.iter().all(|arg| !required(arg)) {
            let left_cell = frame.arrow_input(left).filter(|cell| packed_numeric_cell(*cell));
            let right_cell = frame.arrow_input(right).filter(|cell| packed_numeric_cell(*cell));
            if left_cell.is_none() && right_cell.is_none() { return evaluate_scalar(expr, frame).await; }
            // Both arguments are evaluated before null propagation or score validation.
            let left = CosineArgument::read(left, left_cell, frame).await?;
            let right = CosineArgument::read(right, right_cell, frame).await?;
            if left.is_null() || right.is_null() { return Ok(scalar(Value::Null)); }
            return Ok(scalar(crate::vector::score_pair(left.vector()?, right.vector()?)?));
        }
        frame.context.reserve(name.len().checked_mul(8).and_then(|n|n.checked_add(256)).ok_or_else(overflow)?)?;
        let name = name.to_lowercase().replace('-',"_");
        frame.context.reserve(args.iter().try_fold(0usize,|bytes,expr|bytes.checked_add(expression_type_bound(expr, frame.replacements)?).ok_or_else(overflow))?.checked_mul(8).ok_or_else(overflow)?)?;
        validate(expr, &name, args, frame)?;
        if name == "_id_eq_any" { return id_eq_any(&args[0], &args[1], frame).await; }
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum IdCell<'a> {
    Null,
    Integer(i128),
    Bytes(&'a [u8]),
}

fn borrowed_id_type(field: &arrow_schema::Field) -> bool {
    use arrow_schema::DataType as D;

    if field.metadata().is_empty() {
        return match field.data_type() {
            D::Null => field.is_nullable(),
            D::Int8 | D::Int16 | D::Int32 | D::Int64 | D::UInt8 | D::UInt16 | D::UInt32 | D::UInt64
            | D::Utf8 | D::Binary | D::FixedSizeBinary(_) => true,
            _ => false,
        };
    }
    field.metadata().len() == 1 && matches!((field.data_type(), field.extension_type_name()),
        (D::Utf8, Some("xt/clj-keyword")) | (D::FixedSizeBinary(16), Some("uuid")))
}

fn borrowed_id_array_child(field: &arrow_schema::Field) -> bool {
    if let arrow_schema::DataType::Union(fields, arrow_schema::UnionMode::Dense) = field.data_type()
        && field.metadata().is_empty()
    {
        return fields.iter().all(|(_, field)| {
            if !borrowed_id_type(field) { return false; }
            let Ok(ty) = rtdb_columnar::from_field(field) else { return false; };
            let leg = match &ty { LogicalType::Maybe(leg) => leg.as_ref(), leg => leg };
            rtdb_columnar::leg_name(leg).is_ok_and(|name| &name == field.name())
        });
    }
    borrowed_id_type(field)
}

impl<'a> IdCell<'a> {
    fn value(value: &'a Value) -> Option<Self> {
        if nullish(value) { return Some(Self::Null); }
        crate::numeric::integer(value).map(Self::Integer)
            .or_else(|| crate::compare::emitted_byte_buffer(value).map(Self::Bytes))
    }

    fn arrow(cell: ArrowCell<'a>) -> Result<Option<Self>> {
        use arrow_array::cast::AsArray;
        use arrow_schema::DataType;

        let cell = cell.physical()?;
        if cell.is_null()? { return Ok(Some(Self::Null)); }
        if !borrowed_id_type(cell.field) { return Ok(None); }
        if let Some(value) = cell.signed_integer() {
            return Ok(Some(Self::Integer(i128::from(value))));
        }
        let extension = cell.field.metadata().get(rtdb_columnar::EXTENSION_NAME_KEY).map(String::as_str);
        let bytes = match (cell.field.data_type(), extension) {
            (DataType::Utf8, None | Some("xt/clj-keyword")) => cell.array.as_string::<i32>().value(cell.row).as_bytes(),
            (DataType::Binary, None) => cell.array.as_binary::<i32>().value(cell.row),
            (DataType::FixedSizeBinary(_), None) | (DataType::FixedSizeBinary(16), Some("uuid")) =>
                cell.array.as_fixed_size_binary().value(cell.row),
            _ => return Ok(None),
        };
        Ok(Some(Self::Bytes(bytes)))
    }
}

async fn id_eq_any<'a>(id_expr: &'a Expr, array_expr: &'a Expr, frame: &'a EvaluationFrame<'a>) -> Result<EvalValue<'a>> {
    let borrowed_id = if let Some(cell) = frame.arrow_input(id_expr) {
        IdCell::arrow(cell)?
    } else {
        match (id_expr, frame.row) {
            (Expr::Literal(value), _) => IdCell::value(value),
            (Expr::Column { id, .. }, ExpressionRow::Values(row)) =>
                frame.schema.position(*id).and_then(|index| row.get(index)).and_then(IdCell::value),
            _ => None,
        }
    };
    let mut id = if borrowed_id.is_none() { Some(evaluate(id_expr, frame)?.await?) } else { None };
    let mut evaluated_array = None;
    let mut arrow_array = None;
    let mut stored_array = None;
    if let Expr::Parameter { index, .. } = array_expr {
        if let Some(cell) = frame.context.parameters.arrow_cell(*index) {
            let cell = cell.physical()?;
            if cell.field.metadata().is_empty()
                && let arrow_schema::DataType::List(child) | arrow_schema::DataType::FixedSizeList(child, _) = cell.field.data_type()
                && borrowed_id_array_child(child)
            {
                if cell.is_null()? { return Ok(scalar(Value::Null)); }
                arrow_array = cell.sequence_range();
            }
        } else if let Some(value) = frame.context.parameters.get(*index)? {
            match value {
                Value::List(values) | Value::FixedSizeList(values) => stored_array = Some(values.view()),
                value if nullish(value) => return Ok(scalar(Value::Null)),
                _ => return Err(invalid("_id_eq_any")),
            }
        } else {
            return Err(Anomaly::incorrect("xtdb/missing-parameter", format!("Missing parameter ${}", index + 1)));
        }
    }
    if arrow_array.is_none() && stored_array.is_none() {
        let array = evaluate(array_expr, frame)?.await?;
        if is_null(&array) { return Ok(scalar(Value::Null)); }
        if !matches!(array, EvalValue::List(Kind::List | Kind::Fixed, _)) { return Err(invalid("_id_eq_any")); }
        evaluated_array = Some(array);
        if let Some(EvalValue::List(_, reader)) = &evaluated_array {
            match reader.as_ref() {
                ListReader::Arrow(cell) => arrow_array = cell.sequence_range(),
                ListReader::Stored(values) => stored_array = Some(values.view()),
                _ => (),
            }
        }
    }

    let length = if let Some((_, _, range)) = &arrow_array { range.len() }
        else if let Some(values) = stored_array { values.len() }
        else { len(evaluated_array.as_ref().expect("evaluated array"))? };
    let mut answer = Some(false);
    for index in 0..length {
        let child = arrow_array.as_ref().map(|(field, array, range)| ArrowCell { field, array: *array, row: range.start + index });
        let stored = stored_array.and_then(|values| values.get(index));
        let left = borrowed_id.or_else(|| id.as_ref().and_then(|id| scalar_ref(id).ok()).and_then(IdCell::value));
        let right = if let Some(child) = child { IdCell::arrow(child)? }
            else { stored.as_deref().and_then(IdCell::value) };
        let matched = if let (Some(left), Some(right)) = (left, right) {
            if left == IdCell::Null || right == IdCell::Null { None } else { Some(left == right) }
        } else {
            let value = if let Some(child) = child {
                box_cell(from_arrow(child, frame)?)?
            } else if let Some(value) = &stored {
                box_cell(from_value(super::super::retained_clone(value.as_ref(), frame.context)?, frame)?)?
            } else {
                nth(clone_value(evaluated_array.as_ref().expect("evaluated array"), frame)?, index, frame)?.await?
            };
            if id.is_none() { id = Some(evaluate(id_expr, frame)?.await?); }
            let id = id.as_ref().expect("evaluated ID");
            match (id, &value) {
                (EvalValue::Scalar(left), EvalValue::Scalar(right)) => {
                    crate::compare::sql_eq_scoped(left, right, frame.context.timezone, frame.context.allocation)?
                }
                _ => compare::equal(clone_value(id, frame)?, value, false, frame)?.await?,
            }
        };
        match matched {
            Some(true) => return Ok(scalar(Value::Bool(true))),
            None => answer = None,
            Some(false) => (),
        }
    }
    Ok(scalar(boolean(answer)))
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
            let allows_packed = |input: &EvalValue<'_>, other: &EvalValue<'_>| {
                !matches!((input, other), (EvalValue::List(_, input), EvalValue::List(_, other))
                    if matches!(input.as_ref(), ListReader::Prepared(_))
                        && matches!(other.as_ref(), ListReader::Arrow(cell) if !packed_numeric_cell(*cell)))
            };
            if allows_packed(left, right) && allows_packed(right, left)
                && let (Some(left), Some(right)) = (borrowed_vector(left), borrowed_vector(right))
            {
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




#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, ListArray, types::Int64Type};
    use arrow_schema::Field;

    struct BoundList {
        field: Field,
        array: ListArray,
    }

    impl crate::ParameterLookup for BoundList {
        fn width(&self) -> usize { 1 }
        fn value(&self, _: usize) -> Result<Option<&Value>> { Err(Anomaly::fault("test/row-read", "Expected Arrow parameter access")) }
        fn known_value(&self, _: usize) -> Option<&Value> { None }
        fn arrow_cell(&self, index: usize) -> Option<ArrowCell<'_>> {
            (index == 0).then_some(ArrowCell { field: &self.field, array: &self.array, row: 1 })
        }
    }

    #[test]
    fn wave4_id_membership_reads_only_the_bound_list_slice() {
        let array = ListArray::from_iter_primitive::<Int64Type, _, _>([
            Some(vec![Some(91)]), Some(vec![None, Some(7)]), Some(vec![Some(93)]),
        ]);
        let params = BoundList { field: Field::new("arg", array.data_type().clone(), false), array };
        let context = EvalContext { parameters: crate::Parameters::Lookup(&params), ..Default::default() };
        let schema = Schema::default();
        let array_type = LogicalType::list(LogicalType::Scalar(rtdb_types::ScalarType::I64).maybe());
        let any = |id, array| Expr::Call { name: "_id_eq_any".into(), args: vec![Expr::Literal(id), array],
            return_type: LogicalType::Scalar(rtdb_types::ScalarType::Bool).maybe(),
        };
        for (id, expected) in [(Value::I64(7), Value::Bool(true)), (Value::I64(91), Value::Null), (Value::Null, Value::Null)] {
            let array = Expr::Parameter { index: 0, data_type: array_type.clone() };
            assert_eq!(crate::evaluate(&any(id, array), &schema, &[], &context).unwrap(), expected);
        }

        let empty = Expr::Literal(Value::list(vec![]));
        assert_eq!(crate::evaluate(&any(Value::Null, empty), &schema, &[], &context).unwrap(), Value::Bool(false));
    }
}
