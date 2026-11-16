use rtdb_plan::BinaryOp;
use rtdb_types::{Anomaly, LogicalType, Result, ScalarType, Value};

pub(crate) fn is_name(name: &str) -> bool {
    canonical_name(name).is_some()
}
fn canonical_name(name: &str) -> Option<&'static str> {
    [("bit_not","bit-not"),("bit_and","bit-and"),("bit_or","bit-or"),("bit_xor","bit-xor"),("bit_shift_left","bit-shift-left"),("bit_shift_right","bit-shift-right")]
        .into_iter().find_map(|(canonical,alias)| (name.eq_ignore_ascii_case(canonical) || name.eq_ignore_ascii_case(alias)).then_some(canonical))
}

pub(crate) fn call(name: &str, args: &[Value]) -> Option<Result<Value>> {
    let name = canonical_name(name)?;
    Some((|| {
        let result = rtdb_plan::function_type(name, &args.iter().map(Value::logical_type).collect::<Vec<_>>())?;
        if args.iter().any(crate::compare::nullish) { return Ok(Value::Null); }
        let LogicalType::Scalar(ty) = result else { return Err(crate::numeric::inapplicable(name, &args.iter().collect::<Vec<_>>())); };
        let left = crate::numeric::integer(&args[0]).unwrap() as i64;
        if matches!(name, "bit_not" | "bit-not") { return crate::numeric::integer_value(i128::from(!left), &ty); }
        let right = crate::numeric::integer(&args[1]).unwrap() as i64;
        let value = match name {
            "bit_and" | "bit-and" => left & right,
            "bit_or" | "bit-or" => left | right,
            "bit_xor" | "bit-xor" => left ^ right,
            _ => {
                let right = if ty != ScalarType::I64 { i64::from(i32::try_from(right).map_err(|_| Anomaly::incorrect("xtdb.expression/overflow-error", "data exception - overflow error"))?) } else { right };
                let count = right as u32 & 63;
                if matches!(name, "bit_shift_left" | "bit-shift-left") { left.wrapping_shl(count) } else { left >> count }
            }
        };
        Ok(match ty { ScalarType::I8 => Value::I8(value as i8), ScalarType::I16 => Value::I16(value as i16), ScalarType::I32 => Value::I32(value as i32), _ => Value::I64(value) })
    })())
}

pub(crate) fn binary(op: BinaryOp, left: &Value, right: &Value) -> Option<Result<Value>> {
    let name = match op {
        BinaryOp::BitAnd => "bit_and", BinaryOp::BitOr => "bit_or", BinaryOp::BitXor => "bit_xor",
        BinaryOp::ShiftLeft => "bit_shift_left", BinaryOp::ShiftRight => "bit_shift_right", _ => return None,
    };
    if crate::compare::nullish(left) || crate::compare::nullish(right) { return Some(Ok(Value::Null)); }
    if !matches!(left, Value::I8(_) | Value::I16(_) | Value::I32(_) | Value::I64(_))
        || !matches!(right, Value::I8(_) | Value::I16(_) | Value::I32(_) | Value::I64(_)) {
        return Some(Err(crate::numeric::inapplicable(name, &[left, right])));
    }
    call(name, &[left.clone(), right.clone()])
}
