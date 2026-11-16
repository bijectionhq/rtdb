use rtdb_plan::function_type;
use rtdb_types::{LogicalType as L,ScalarType as S};

#[test]
fn unregistered_generic_names_have_only_strict_null_or_unbound_specializations() {
    for name in ["trunc","atan2","degrees","radians","uuid","uri","substr","char_length","bit_length","boolean?","number?","integer?","float?","string?","keyword?","uuid?","list?","set?","map?","unregistered_fn"] {
        assert_eq!(function_type(name,&[L::Scalar(S::I64)]).unwrap_err().code,"xtdb.expression/function-type-mismatch","{name}");
        assert_eq!(function_type(name,&[L::Scalar(S::I64).maybe()]).unwrap_err().code,"xtdb.expression/function-type-mismatch","{name}");
        assert_eq!(function_type(name,&[L::Null]).unwrap(),L::Null,"{name}");
        assert_eq!(function_type(name,&[L::Scalar(S::I64),L::Null]).unwrap(),L::Null,"{name}");
        assert_eq!(function_type(name,&[L::Nothing]).unwrap(),L::Nothing,"{name}");
        assert_eq!(function_type(name,&[]).unwrap_err().code,"xtdb.expression/function-type-mismatch","{name}");
    }
    assert_eq!(function_type("character_length",&[L::Scalar(S::Utf8)]).unwrap(),L::Scalar(S::I32));
}
