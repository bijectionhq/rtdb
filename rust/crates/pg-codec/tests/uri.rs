use rtdb_format::transit::TransitValue as T;
use rtdb_types::Value;

fn uri(text: &str) -> T { T::Tagged { tag: "r".into(),representation: Box::new(T::String(text.into())) } }

#[test]
fn transit_uri_object_construction_validates_without_rewriting_spelling() {
    for text in ["HtTp://EXAMPLE.com/a/../é","http://bad_host",""] {
        assert_eq!(rtdb_pg_codec::from_transit(uri(text)).unwrap(),Value::Uri(text.into()));
    }
    for text in ["bad space","http://[bad]","http://host/%zz"] { assert!(rtdb_pg_codec::from_transit(uri(text)).is_err()); }
}
