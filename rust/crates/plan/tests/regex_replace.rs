use rtdb_plan::{RegexForm as F,RegexReplacement};
use rtdb_types::{LogicalType as L,ScalarType as S,Value as V};

fn text(value: &str) -> F { raw_literal(V::Utf8(value.into())) }
fn recipe() -> RegexReplacement { RegexReplacement { pattern: text("a"),replacement: text("b"),..Default::default() } }

#[test]
fn null_only_replacement_sources_do_not_admit_raw_metadata() {
    let invalid = RegexReplacement { pattern: raw_symbol("unresolved".into()),replacement: F::default(),start: raw_literal(V::I64(1)),flags: raw_literal(V::Bool(false)),..Default::default() };
    assert_eq!(invalid.output_type(&L::Null).unwrap(),L::Null);
    assert_eq!(invalid.output_type(&L::Nothing).unwrap(),L::Nothing);
    assert_eq!(invalid.output_type(&L::Scalar(S::Utf8)).unwrap_err().code,"xtdb.error/unknown");
    assert!(invalid.output_type(&L::Scalar(S::Utf8).maybe()).is_err());
    assert_eq!(recipe().output_type(&L::Scalar(S::Utf8).maybe()).unwrap(),L::Scalar(S::Utf8).maybe());
    assert_eq!(recipe().output_type(&L::Scalar(S::I64)).unwrap_err().code,"xtdb.expression/function-type-mismatch");
}

#[test]
fn replacement_flags_reduce_raw_characters_without_evaluating_sequence_elements() {
    for (flags,expected) in [
        (text("misi?"),"ism"),(F::default(),""),(raw_list(vec![text("i"),raw_symbol("s".into())]),""),
        (raw_vector(vec![raw_character('i'),raw_character('s')]),"is"),
        (raw_set(vec![raw_character('m'),raw_character('x')]),"m"),
        (raw_map(vec![(raw_character('i'),text("s"))]),""),
    ] {
        let mut recipe = recipe();recipe.flags = flags;
        assert_eq!(recipe.flag_text().unwrap(),expected);
        assert!(recipe.output_type(&L::Scalar(S::Utf8)).is_ok());
    }
    let mut recipe = recipe();recipe.flags = raw_character('i');
    assert!(recipe.flag_text().is_err());
}

#[test]
fn raw_start_and_occurrence_use_truthiness_and_patterns_wait_for_invocation() {
    for flag in [F::default(),raw_literal(V::Bool(false))] {
        let mut recipe = recipe();recipe.start = flag.clone();recipe.occurrence = flag;
        recipe.pattern = raw_list(vec![raw_symbol("str".into()),text("[")]);
        assert_eq!(recipe.output_type(&L::Scalar(S::Utf8)).unwrap(),L::Scalar(S::Utf8));
    }
    for flag in [raw_literal(V::I64(0)),raw_vector(vec![]),text("")] {
        let mut recipe = recipe();recipe.start = flag;
        assert_eq!(recipe.output_type(&L::Scalar(S::Utf8)).unwrap_err().code,"xtdb.expression/unsupported");
    }
}

fn raw_build(emit: impl FnOnce(&mut rtdb_plan::RegexFormBuilder<'_>) -> rtdb_types::Result<()>) -> rtdb_plan::RegexForm {
    let mut builder = rtdb_plan::RegexFormBuilder::new_untracked();
    emit(&mut builder).unwrap();
    builder.finish().unwrap()
}
fn raw_literal(value: rtdb_types::Value) -> rtdb_plan::RegexForm { raw_build(|builder| builder.literal(&value)) }
fn raw_character(value: char) -> rtdb_plan::RegexForm { raw_build(|builder| builder.character(value)) }
fn raw_symbol(value: String) -> rtdb_plan::RegexForm { raw_build(|builder| builder.symbol(&value)) }
fn raw_list(values: Vec<rtdb_plan::RegexForm>) -> rtdb_plan::RegexForm { raw_build(|builder| { builder.begin_list()?; for value in values { builder.append_view(value.view())?; } builder.end() }) }
fn raw_vector(values: Vec<rtdb_plan::RegexForm>) -> rtdb_plan::RegexForm { raw_build(|builder| { builder.begin_vector()?; for value in values { builder.append_view(value.view())?; } builder.end() }) }
fn raw_set(values: Vec<rtdb_plan::RegexForm>) -> rtdb_plan::RegexForm { raw_build(|builder| { builder.begin_set()?; for value in values { builder.append_view(value.view())?; } builder.end() }) }
fn raw_map(values: Vec<(rtdb_plan::RegexForm, rtdb_plan::RegexForm)>) -> rtdb_plan::RegexForm { raw_build(|builder| { builder.begin_map()?; for (key, value) in values { builder.append_view(key.view())?; builder.append_view(value.view())?; } builder.end() }) }
