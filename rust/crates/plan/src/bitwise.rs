use rtdb_types::{Anomaly, LogicalType as L, Result, ScalarType as S};
use crate::BinaryOp;

pub(crate) fn binary_name(op: BinaryOp) -> Option<&'static str> {
    Some(match op {
        BinaryOp::BitAnd => "bit_and", BinaryOp::BitOr => "bit_or", BinaryOp::BitXor => "bit_xor",
        BinaryOp::ShiftLeft => "bit_shift_left", BinaryOp::ShiftRight => "bit_shift_right", _ => return None,
    })
}

pub(crate) fn signature(name: &str, arguments: &[L]) -> Option<Result<L>> {
    if !matches!(name, "bit_not" | "bit_and" | "bit_or" | "bit_xor" | "bit_shift_left" | "bit_shift_right") { return None; }
    Some((|| {
        let arity = if name == "bit_not" { 1 } else { 2 };
        if arguments.len() != arity {
            return Err(Anomaly::incorrect("xtdb/invalid-function-arity", format!("Wrong argument count for {name}: {}", arguments.len())));
        }
        let signed = |ty: &L| matches!(ty, L::Scalar(S::I8 | S::I16 | S::I32 | S::I64));
        let invalid = || Anomaly::incorrect("xtdb.expression/function-type-mismatch", format!("{name} requires signed integer arguments"));
        let mut output = L::Nothing;
        for left in arguments[0].legs() {
            if arity == 1 {
                if left != L::Null && !signed(&left) { return Err(invalid()); }
                output = output.join(&left);
            } else {
                for right in arguments[1].legs() {
                    let result = if left == L::Null || right == L::Null { L::Null }
                        else if !signed(&left) || !signed(&right) { return Err(invalid()); }
                        else if name.starts_with("bit_shift_") { left.clone() }
                        else { L::least_upper_bound(&[left.clone(), right]).ok_or_else(invalid)? };
                    output = output.join(&result);
                }
            }
        }
        Ok(output)
    })())
}
