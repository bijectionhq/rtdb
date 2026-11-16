use rtdb_plan::function_type;
use rtdb_types::{LogicalType as L,ScalarType as S};

#[test]
fn regex_search_requires_three_utf8_legs_unless_the_selected_tuple_is_null() {
    let text = L::Scalar(S::Utf8);
    assert_eq!(function_type("like_regex",&[text.clone(),text.clone(),text.clone()]).unwrap(),L::Scalar(S::Bool));
    assert_eq!(function_type("like_regex",&[text.clone().maybe(),text.clone(),text.clone()]).unwrap(),L::Scalar(S::Bool).maybe());
    for types in [vec![],vec![text.clone(),text.clone()],vec![text.clone();4],vec![text.clone(),text.clone(),L::Scalar(S::I64)],vec![text.clone().maybe(),L::Scalar(S::I64),text.clone()]] {
        assert_eq!(function_type("like_regex",&types).unwrap_err().code,"xtdb.expression/function-type-mismatch");
    }
    for types in [vec![L::Null],vec![text.clone(),L::Null],vec![L::Scalar(S::I64),L::Null,text.clone(),text.clone()]] {
        assert_eq!(function_type("like_regex",&types).unwrap(),L::Null);
    }
    assert_eq!(function_type("like_regex",&[L::Nothing,L::Null]).unwrap(),L::Nothing);
}
