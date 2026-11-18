use super::*;
use crate::EvalContext;
use rtdb_types::MemoryBudget;
use rtdb_format::transit::{encode_json, encode_msgpack};

fn tag(name: &str, value: T) -> T { tagged(name, value) }
fn text(value: &str) -> T { T::String(value.into()) }
fn keyword(value: &str) -> T { T::Keyword(value.into()) }
fn bytes(value: &T, json: bool) -> Vec<u8> {
    if json { encode_json(value).unwrap() } else { encode_msgpack(value).unwrap() }
}
fn readable(value: &Object, admission: Admission<'_>) -> Result<String> {
    let mut output = allocation::Text::new(admission);
    render::write_printed(&mut output, value)?;
    Ok(output.finish())
}

#[test]
fn context_admission_preserves_standalone_and_exact_tracked_bookkeeping() {
    EvalContext::default().reserve(usize::MAX).unwrap();
    let memory = MemoryBudget::new(4096 + 256);
    {
        let allocation = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&allocation), ..Default::default() };
        context.reserve(0).unwrap();
        assert_eq!(memory.used_bytes(), 4096 + 256);
        let error = context.reserve(1).unwrap_err();
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        assert_eq!(error.message.as_deref(), Some("Cannot reserve 257 bytes from budget 4352 with 4352 retained"));
        assert_eq!(context.reserve(usize::MAX).unwrap_err().code, "rtdb.expr/allocation-size");
        assert_eq!(memory.used_bytes(), 4096 + 256);
        assert_eq!(memory.peak_bytes(), 4096 + 256);
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn one_object_builder_preserves_names_types_and_readable_text_in_both_policies() {
    let table = tag("xt/table", T::Map(vec![
        (keyword("schema-name"), T::Null),
        (keyword("table-name"), T::Array(vec![T::Integer(7), T::Bool(false)])),
    ]));
    let canonical_table = tag("xt/table", T::Map(vec![
        (keyword("schema-name"), text("public")), (keyword("table-name"), text("[7 false]")),
    ]));
    let ty = tag("xtdb/type", T::Array(vec![keyword("struct"), T::Map(vec![(keyword("field"), keyword("i64"))])]));
    let canonical_type = tag("xtdb/type", T::Array(vec![keyword("struct"), T::Map(vec![(text(":field"), keyword("i64"))])]));
    for json in [false, true] {
        for (input, expected, printed) in [
            (table.clone(), canonical_table.clone(), "#xt/table [7 false]"),
            (ty.clone(), canonical_type.clone(), "#xt/type [:struct {\":field\" :i64}]"),
        ] {
            let encoded = bytes(&input, json);
            let memory = MemoryBudget::new(64 << 20);
            {
                let allocation = EvaluationScope::new(&memory).unwrap();
                let baseline = memory.used_bytes();
                let standalone = decode_with(&encoded, &mut ObjectReadBuilder { admission: Admission::Standalone }).unwrap();
                let standalone_text = readable(&standalone, Admission::Standalone).unwrap();
                let standalone_wire = standalone.wire_with(Admission::Standalone).unwrap();
                assert_eq!(standalone_text, printed);
                assert_eq!(standalone_wire, expected);
                assert_eq!(memory.used_bytes(), baseline);
                let tracked = Decoded::read(&encoded, &allocation).unwrap();
                assert!(standalone.equivalent(&tracked.value).unwrap());
                assert_eq!(readable(&tracked.value, Admission::Tracked(&allocation)).unwrap(), printed);
                assert_eq!(tracked.value.wire(&allocation).unwrap(), expected);
                assert!(memory.used_bytes() > baseline);
            }
            assert_eq!(memory.used_bytes(), 0);
        }
    }
}

#[test]
fn invalid_type_diagnostics_share_writers_and_preserve_tracked_denial() {
    for json in [false, true] {
        for spec in [
            text("bad"),
            tag("unknown", T::Null),
            tag("xt/error", T::Array(vec![keyword("incorrect"), T::Null, text("message"), T::Map(vec![])])),
        ] {
            let encoded = bytes(&tag("xtdb/type", spec), json);
            let standalone = decode_with(&encoded, &mut ObjectReadBuilder { admission: Admission::Standalone }).unwrap_err();
            assert_eq!(standalone.code, "invalid-type");
            assert_eq!(standalone.message.as_deref(), Some("Invalid type"));
            let memory = MemoryBudget::new(64 << 20);
            {
                let allocation = EvaluationScope::new(&memory).unwrap();
                let Err(tracked) = Decoded::read(&encoded, &allocation) else { panic!("expected an invalid type"); };
                assert_eq!(tracked, standalone);
            }
            assert_eq!(memory.used_bytes(), 0);
        }
        let encoded = bytes(&tag("xtdb/type", text("bad")), json);
        let standalone = decode_with(&encoded, &mut ObjectReadBuilder { admission: Admission::Standalone }).unwrap_err();
        assert_eq!(standalone.context["type-spec"], serde_json::json!("bad"));
        let count = encoded.len();
        let envelope = 16 * count * count + 64 * count + 4096;
        let limit = 4096 + envelope + 256;
        let memory = MemoryBudget::new(limit);
        {
            let allocation = EvaluationScope::new(&memory).unwrap();
            let Err(error) = Decoded::read(&encoded, &allocation) else { panic!("expected wire admission to fail"); };
            assert_eq!(error.code, "rtdb/memory-budget-exceeded");
            assert!(error.message.as_deref().unwrap().starts_with("Cannot reserve 1286 bytes from budget "));
            assert_eq!(memory.used_bytes(), limit);
            assert_eq!(memory.peak_bytes(), limit);
        }
        assert_eq!(memory.used_bytes(), 0);
    }
}
