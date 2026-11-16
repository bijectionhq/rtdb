use rtdb_plan::function_type;
use rtdb_types::{LogicalType as L, ScalarType as S};

#[test]
fn uri_extractors_accept_only_uri_legs_and_keep_nullable_result_types() {
    for name in ["uri_scheme","uri_user_info","uri_host","uri_port","uri_path","uri_query","uri_fragment"] {
        let output = L::Scalar(if name == "uri_port" { S::I32 } else { S::Utf8 }).maybe();
        assert_eq!(function_type(name,&[L::Scalar(S::Uri)]).unwrap(),output);
        assert_eq!(function_type(name,&[L::Scalar(S::Uri).maybe()]).unwrap(),output);
        assert_eq!(function_type(name,&[L::Null]).unwrap(),L::Null);
        assert_eq!(function_type(name,&[L::Nothing]).unwrap(),L::Nothing);
        assert_eq!(function_type(&name.replace('_',"-"),&[L::Scalar(S::Uri)]).unwrap(),output);
        for ty in [L::Scalar(S::Utf8),L::Scalar(S::I64),L::Scalar(S::Uri).join(&L::Scalar(S::Utf8))] { assert!(function_type(name,&[ty]).is_err()); }
        assert!(function_type(name,&[]).is_err());
        assert!(function_type(name,&[L::Null,L::Null]).is_err());
    }
}
