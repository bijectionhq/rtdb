use rtdb_plan::BinaryOp;
use rtdb_types::{Anomaly, LogicalType, Result, ScalarType, Value};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BitwiseFunction {
    BitNot,
    BitAnd,
    BitOr,
    BitXor,
    BitShiftLeft,
    BitShiftRight,
}

impl BitwiseFunction {
    pub(crate) fn from_canonical(name: &str) -> Option<Self> {
        Some(match name {
            "bit_not" => Self::BitNot,
            "bit_and" => Self::BitAnd,
            "bit_or" => Self::BitOr,
            "bit_xor" => Self::BitXor,
            "bit_shift_left" => Self::BitShiftLeft,
            "bit_shift_right" => Self::BitShiftRight,
            _ => return None,
        })
    }

    pub(crate) fn canonical_name(self) -> &'static str {
        match self {
            Self::BitNot => "bit_not",
            Self::BitAnd => "bit_and",
            Self::BitOr => "bit_or",
            Self::BitXor => "bit_xor",
            Self::BitShiftLeft => "bit_shift_left",
            Self::BitShiftRight => "bit_shift_right",
        }
    }

    pub(crate) fn eval(self, args: &[Value]) -> Result<Value> {
        let result = rtdb_plan::function_type(self.canonical_name(), &args.iter().map(Value::logical_type).collect::<Vec<_>>())?;
        self.eval_typed(args, result)
    }

    pub(crate) fn eval_typed(self, args: &[Value], result: LogicalType) -> Result<Value> {
        if args.iter().any(crate::compare::nullish) { return Ok(Value::Null); }
        let LogicalType::Scalar(ty) = result else { return Err(crate::numeric::inapplicable(self.canonical_name(), &args.iter().collect::<Vec<_>>())); };
        let left = crate::numeric::integer(&args[0]).unwrap() as i64;
        if self == Self::BitNot { return crate::numeric::integer_value(i128::from(!left), &ty); }
        let right = crate::numeric::integer(&args[1]).unwrap() as i64;
        let value = match self {
            Self::BitAnd => left & right,
            Self::BitOr => left | right,
            Self::BitXor => left ^ right,
            Self::BitShiftLeft | Self::BitShiftRight => {
                let right = if ty != ScalarType::I64 { i64::from(i32::try_from(right).map_err(|_| Anomaly::incorrect("xtdb.expression/overflow-error", "data exception - overflow error"))?) } else { right };
                let count = right as u32 & 63;
                if self == Self::BitShiftLeft { left.wrapping_shl(count) } else { left >> count }
            }
            Self::BitNot => unreachable!(),
        };
        Ok(match ty { ScalarType::I8 => Value::I8(value as i8), ScalarType::I16 => Value::I16(value as i16), ScalarType::I32 => Value::I32(value as i32), _ => Value::I64(value) })
    }
}

pub(crate) fn binary(op: BinaryOp, left: &Value, right: &Value) -> Option<Result<Value>> {
    let function = match op {
        BinaryOp::BitAnd => BitwiseFunction::BitAnd,
        BinaryOp::BitOr => BitwiseFunction::BitOr,
        BinaryOp::BitXor => BitwiseFunction::BitXor,
        BinaryOp::ShiftLeft => BitwiseFunction::BitShiftLeft,
        BinaryOp::ShiftRight => BitwiseFunction::BitShiftRight,
        _ => return None,
    };
    if crate::compare::nullish(left) || crate::compare::nullish(right) { return Some(Ok(Value::Null)); }
    if !matches!(left, Value::I8(_) | Value::I16(_) | Value::I32(_) | Value::I64(_))
        || !matches!(right, Value::I8(_) | Value::I16(_) | Value::I32(_) | Value::I64(_)) {
        return Some(Err(crate::numeric::inapplicable(function.canonical_name(), &[left, right])));
    }
    Some(function.eval(&[left.clone(), right.clone()]))
}
