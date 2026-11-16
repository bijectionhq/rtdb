use rtdb_plan::{RegexForm as F,RegexReplacement};
use rtdb_types::{LogicalType as L,ScalarType as S,Value as V};

fn text(value: &str) -> F { F::Literal(V::Utf8(value.into())) }
fn recipe() -> RegexReplacement { RegexReplacement { pattern: text("a"),replacement: text("b"),..Default::default() } }

#[test]
fn null_only_replacement_sources_do_not_admit_raw_metadata() {
    let invalid = RegexReplacement { pattern: F::Symbol("unresolved".into()),replacement: F::default(),start: F::Literal(V::I64(1)),flags: F::Literal(V::Bool(false)),..Default::default() };
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
        (text("misi?"),"ism"),(F::default(),""),(F::List(vec![text("i"),F::Symbol("s".into())]),""),
        (F::Vector(vec![F::Character('i'),F::Character('s')]),"is"),
        (F::Set(vec![F::Character('m'),F::Character('x')]),"m"),
        (F::Map(vec![(F::Character('i'),text("s"))]),""),
    ] {
        let mut recipe = recipe();recipe.flags = flags;
        assert_eq!(recipe.flag_text().unwrap(),expected);
        assert!(recipe.output_type(&L::Scalar(S::Utf8)).is_ok());
    }
    let mut recipe = recipe();recipe.flags = F::Character('i');
    assert!(recipe.flag_text().is_err());
}

#[test]
fn raw_start_and_occurrence_use_truthiness_and_patterns_wait_for_invocation() {
    for flag in [F::default(),F::Literal(V::Bool(false))] {
        let mut recipe = recipe();recipe.start = flag.clone();recipe.occurrence = flag;
        recipe.pattern = F::List(vec![F::Symbol("str".into()),text("[")]);
        assert_eq!(recipe.output_type(&L::Scalar(S::Utf8)).unwrap(),L::Scalar(S::Utf8));
    }
    for flag in [F::Literal(V::I64(0)),F::Vector(vec![]),text("")] {
        let mut recipe = recipe();recipe.start = flag;
        assert_eq!(recipe.output_type(&L::Scalar(S::Utf8)).unwrap_err().code,"xtdb.expression/unsupported");
    }
}
