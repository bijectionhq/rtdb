use std::cmp::Ordering;
use super::*;

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
                if !a.keys().eq(b.keys()) { return Ok(Some(false)); }
                let mut unknown = false;
                for (a,b) in a.values().zip(b.values()) {
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

    #[tokio::test]
    async fn strict_set_reader_aliases_compare_by_identity_without_reading_children() {
        let schema = Schema::default();
        let context = EvalContext::default();
        let frame = EvaluationFrame { schema: &schema, row: &[], context: &context, asynchronous: false, replacements: None };
        let set = make_list(Kind::Set, ListReader::Values(vec![Value::Transit(vec![0xff])]), &frame).unwrap();
        let alias = clone_value(&set, &frame).unwrap();
        assert_eq!(equal(set, alias, true, &frame).unwrap().await.unwrap(), Some(true));
    }
}
