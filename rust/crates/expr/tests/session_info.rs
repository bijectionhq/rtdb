use rtdb_columnar::MemoryBudget;
use rtdb_expr::{EvalContext, EvaluationScope, function_type, scalar_call};
use rtdb_types::{LogicalType as L, ScalarType as S, Value as V, runtime_version, runtime_revision};

fn text(value: &str) -> V { V::Utf8(value.into()) }

#[test]
fn session_information_uses_reference_constants_and_actual_artifact_version() {
    let context = EvalContext::default();
    for (name, expected) in [("current_user", "xtdb"), ("current_database", "xtdb"), ("current_schema", "public")] {
        assert_eq!(scalar_call(name, &[], &context).unwrap(), text(expected));
    }
    assert_eq!(scalar_call("current_schemas", &[V::Bool(false)], &context).unwrap(), V::list(vec![text("public")]));
    assert_eq!(scalar_call("current_schemas", &[V::Bool(true)], &context).unwrap(), V::list(vec![text("pg_catalog"), text("public")]));
    for (setting, expected) in [("server_version_num", "160000"), ("search_path", "public"), ("xtdb.version", runtime_version())] {
        assert_eq!(scalar_call("current_setting", &[text(setting)], &context).unwrap(), text(expected));
    }
    assert_eq!(scalar_call("version", &[], &context).unwrap(), text(&format!("PostgreSQL 16 (XTDB {})", runtime_version())));
    let expected = match runtime_revision() { Some(revision) => format!("XTDB @ {} [{revision}]", runtime_version()), None => format!("XTDB @ {}", runtime_version()) };
    assert_eq!(scalar_call("xt.version", &[], &context).unwrap(), text(&expected));
}

#[test]
fn session_information_preserves_null_dispatch_and_setting_diagnostics() {
    for name in ["current_schemas", "current_setting"] {
        assert_eq!(scalar_call(name, &[V::Null], &EvalContext::default()).unwrap(), V::Null);
        assert_eq!(function_type(name, &[L::Null]).unwrap(), L::Null);
        assert!(function_type(name, &[L::Scalar(S::I64)]).is_err());
        assert!(function_type(name, &[]).is_err());
    }
    let error = scalar_call("current_setting", &[text("SEARCH_PATH")], &EvalContext::default()).unwrap_err();
    assert_eq!(error.code, "xtdb.expression/unsupported-setting");
    assert_eq!(error.context["setting-name"], "SEARCH_PATH");
}

#[test]
fn session_information_cannot_allocate_outside_a_small_scope() {
    let budget = MemoryBudget::new(4096);
    let allocation = EvaluationScope::new(&budget).unwrap();
    let context = EvalContext { allocation: Some(&allocation), ..Default::default() };
    for (name, args) in [("current_user", vec![]), ("current_schemas", vec![V::Bool(true)]), ("version", vec![])] {
        assert!(scalar_call(name, &args, &context).is_err());
    }
}
