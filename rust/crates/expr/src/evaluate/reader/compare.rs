use std::cmp::Ordering;
use super::*;
use crate::compare::NumericListInput;

fn typed_numbers<'a>(value: &'a EvalValue<'a>) -> Option<NumericListInput<'a>> {
    let EvalValue::List(Kind::List | Kind::Fixed, reader) = value else { return None; };
    match reader.as_ref() {
        ListReader::Stored(list) => NumericListInput::stored(list.view()),
        ListReader::Arrow(cell) => cell.numeric_list_span().map(NumericListInput::Opened),
        _ => None,
    }
}

pub(super) fn equal<'a>(left: EvalValue<'a>,right: EvalValue<'a>,strict: bool,frame: &'a EvaluationFrame<'a>) -> Result<Pending<'a,Option<bool>>> {
    if !strict { rtdb_plan::validate_equality_types(&value_type(&left,frame)?,&value_type(&right,frame)?)?; }
    equal_cells(left,right,strict,frame)
}

fn equal_cells<'a>(left: EvalValue<'a>,right: EvalValue<'a>,strict: bool,frame: &'a EvaluationFrame<'a>) -> Result<Pending<'a,Option<bool>>> {
    boxed(async move {
        if strict {
            if let (EvalValue::Scalar(a),EvalValue::Scalar(b)) = (&left,&right) {
                return crate::compare::strict_eq_scoped(a,b,frame.context.allocation).map(Some);
            }
        }
        if is_null(&left) || is_null(&right) {
            return Ok(if strict { Some(is_null(&left) && is_null(&right)) } else { None });
        }
        match (&left,&right) {
            (EvalValue::Scalar(a),EvalValue::Scalar(b)) => {
                crate::compare::sql_eq_scoped(a,b,frame.context.timezone, frame.context.allocation)
            },
            (EvalValue::List(a,_),EvalValue::List(b,_)) if a == b && *a != Kind::Set => {
                let size = len(&left)?;
                if size != len(&right)? { return Ok(Some(false)); }
                let mut unknown = false;
                if !strict
                    && let Some((left, left_offset, _)) = borrowed_vector(&left).and_then(crate::vector::VectorInput::float_cells)
                    && let Some((right, right_offset, _)) = borrowed_vector(&right).and_then(crate::vector::VectorInput::float_cells)
                {
                    for index in 0..size {
                        match (left.float_component(left_offset + index), right.float_component(right_offset + index)) {
                            (Some(left), Some(right)) if left == right => {}
                            (Some(_), Some(_)) => return Ok(Some(false)),
                            _ => unknown = true,
                        }
                    }
                    return Ok(if unknown { None } else { Some(true) });
                }
                if let (Some(left), Some(right)) = (typed_numbers(&left), typed_numbers(&right)) {
                    return Ok(left.equal(&right, strict));
                }
                for index in 0..size {
                    let a = nth(clone_value(&left,frame)?,index,frame)?.await?;
                    let b = nth(clone_value(&right,frame)?,index,frame)?.await?;
                    match equal_cells(a,b,strict,frame)?.await? {
                        Some(false) => return Ok(Some(false)),None => unknown = true,_ => (),
                    }
                }
                Ok(if unknown { None } else { Some(true) })
            }
            (EvalValue::Struct(a),EvalValue::Struct(b)) => {
                if a.len() != b.len() || a.keys().any(|key| !b.contains_key(key)) { return Ok(Some(false)); }
                let mut unknown = false;
                for (key,a) in a.iter() {
                    let b = &b[key];
                    match equal_cells(clone_value(a,frame)?,clone_value(b,frame)?,strict,frame)?.await? {
                        Some(false) => return Ok(Some(false)),None => unknown = true,_ => (),
                    }
                }
                Ok(if unknown { None } else { Some(true) })
            }
            (EvalValue::List(Kind::Set,a),EvalValue::List(Kind::Set,b)) if strict => Ok(Some(Arc::ptr_eq(a,b))),
            (EvalValue::List(Kind::Set,_),EvalValue::List(Kind::Set,_)) => {
                let a = materialize(left,frame)?.await?;
                let b = materialize(right,frame)?.await?;
                crate::compare::sql_eq_value(&a,&b,frame.context.timezone, frame.context.allocation)
            }
            _ => Ok(Some(false)),
        }
    },frame)
}

pub(super) fn order<'a>(left: EvalValue<'a>,right: EvalValue<'a>,frame: &'a EvaluationFrame<'a>) -> Result<Pending<'a,Ordering>> {
    boxed(async move {
        match (is_null(&left),is_null(&right)) {
            (true,true) => return Ok(Ordering::Equal),(true,false) => return Ok(Ordering::Greater),(false,true) => return Ok(Ordering::Less),_ => (),
        }
        match (&left,&right) {
            (EvalValue::Scalar(a),EvalValue::Scalar(b)) => crate::compare::predicate_order(a,b,frame.context.timezone),
            (EvalValue::List(a,_),EvalValue::List(b,_)) if a == b && *a != Kind::Set => {
                let (a,b) = (len(&left)?,len(&right)?);
                if let Some((left, left_offset, _)) = borrowed_vector(&left).and_then(crate::vector::VectorInput::float_cells)
                    && let Some((right, right_offset, _)) = borrowed_vector(&right).and_then(crate::vector::VectorInput::float_cells)
                {
                    for index in 0..a.min(b) {
                        let order = match (left.float_component(left_offset + index), right.float_component(right_offset + index)) {
                            (Some(left), Some(right)) => crate::compare::float_order(left, right),
                            (None, Some(_)) => Ordering::Greater,
                            (Some(_), None) => Ordering::Less,
                            (None, None) => Ordering::Equal,
                        };
                        if order != Ordering::Equal { return Ok(order); }
                    }
                    return Ok(a.cmp(&b));
                }
                if let (Some(left), Some(right)) = (typed_numbers(&left), typed_numbers(&right)) {
                    return Ok(left.order(&right, false));
                }
                for index in 0..a.min(b) {
                    let a = nth(clone_value(&left,frame)?,index,frame)?.await?;
                    let b = nth(clone_value(&right,frame)?,index,frame)?.await?;
                    let ordering = order(a,b,frame)?.await?;
                    if ordering != Ordering::Equal { return Ok(ordering); }
                }
                Ok(a.cmp(&b))
            }
            _ => Err(invalid("compare")),
        }
    },frame)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_references_compare_as_emitted_integers_in_scalar_predicates() {
        let lhs = Value::RegClass(17);
        let memory = rtdb_types::MemoryBudget::new(1 << 20);
        let scope = crate::EvaluationScope::new(&memory).unwrap();
        for rhs in [Value::I16(17), Value::I64(17), Value::Oid(17), Value::RegProc(17), Value::DateDay(17)] {
            assert_eq!(crate::compare::sql_eq(&lhs, &rhs, &scope).unwrap(), Some(true));
        }
        assert_eq!(crate::compare::sql_not_eq_in_timezone(&lhs, &Value::I64(17), "UTC", &scope).unwrap(), Some(false));
        assert_eq!(crate::compare::sql_eq(&lhs, &Value::F64(17.0), &scope).unwrap(), Some(false));
        assert_eq!(crate::compare::predicate_order(&lhs, &Value::RegProc(18), "UTC").unwrap(), Ordering::Less);
        assert_eq!(crate::compare::predicate_order(&lhs, &Value::F64(16.5), "UTC").unwrap(), Ordering::Greater);
    }

    #[tokio::test]
    async fn strict_set_reader_aliases_compare_by_identity_without_reading_children() {
        let schema = Schema::default();
        let context = EvalContext::default();
        let frame = EvaluationFrame { schema: &schema, row: ExpressionRow::Values(&[]), context: &context, asynchronous: false, replacements: None };
        let set = make_list(Kind::Set, ListReader::Values(vec![Value::Transit(vec![0xff])]), &frame).unwrap();
        let alias = clone_value(&set, &frame).unwrap();
        assert_eq!(equal(set, alias, true, &frame).unwrap().await.unwrap(), Some(true));
    }
}
