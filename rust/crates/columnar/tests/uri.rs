use arrow_array::StringArray;
use rtdb_columnar::{build_array, copy_array, read_value, to_field};
use rtdb_types::{LogicalType as L, ScalarType as S, Value as V};

#[test]
fn uri_object_and_stored_cell_boundaries_reject_invalid_uri_spellings() {
    let ty = L::Scalar(S::Uri);
    let field = to_field("uri",&ty).unwrap();
    for text in ["bad space","http://[bad]","http://host/%zz"] {
        let value = V::Uri(text.into());
        assert!(build_array(&ty,&[value.clone()]).is_err());
        assert!(copy_array(&ty,&[value]).is_err());
        assert!(read_value(&field,&StringArray::from(vec![text]),0).is_err());
    }
    for text in ["HtTp://EXAMPLE.com/a/../é","http://bad_host", ""] {
        let value = V::Uri(text.into());
        let array = build_array(&ty,&[value.clone()]).unwrap();
        assert_eq!(read_value(&field,array.as_ref(),0).unwrap(),value);
    }
}
