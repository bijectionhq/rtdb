use rtdb_plan::{BinaryOp as B, Expr, UnaryOp, function_type};
use rtdb_types::{LogicalType as L, ScalarType as S, Value as V};

#[test]
fn bitwise_signatures_keep_signed_widths_null_legs_and_operator_admission() {
    for ty in [S::I8,S::I16,S::I32,S::I64] {
        let input = L::Scalar(ty);
        assert_eq!(function_type("bit_not", &[input.clone()]).unwrap(), input);
        for name in ["bit_shift_left","bit-shift-right"] {
            assert_eq!(function_type(name, &[input.clone(),L::Scalar(S::I64).maybe()]).unwrap(), input.clone().maybe());
        }
        for name in ["bit_and","bit_or","bit_xor"] {
            assert_eq!(function_type(name, &[input.clone(),L::Scalar(S::I64)]).unwrap(), L::Scalar(S::I64));
        }
    }
    for name in ["bit_and","bit_or","bit_xor","bit_shift_left","bit_shift_right"] {
        assert_eq!(function_type(name, &[L::Scalar(S::Bool),L::Null]).unwrap(),L::Null);
        assert_eq!(function_type(name, &[L::Nothing,L::Scalar(S::I64)]).unwrap(),L::Nothing);
        for ty in [S::U8,S::U64,S::F64,S::Bool,S::Utf8] { assert!(function_type(name, &[L::Scalar(ty),L::Scalar(S::I64)]).is_err()); }
        assert!(function_type(name, &[]).is_err());
        assert!(function_type(name, &[L::Null]).is_err());
    }
    assert_eq!(Expr::binary(B::ShiftLeft,Expr::Literal(V::I8(1)),Expr::Literal(V::Null)).data_type(),L::Null);
    assert!(Expr::binary(B::BitAnd,Expr::Literal(V::U8(1)),Expr::Literal(V::I64(1))).validate_columns(&Default::default()).is_err());
    assert!(Expr::unary(UnaryOp::BitNot,Expr::Literal(V::F64(1.0))).validate_columns(&Default::default()).is_err());
}
