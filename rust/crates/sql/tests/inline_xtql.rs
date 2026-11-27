use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_plan::{Dml, Relation, RelationNode, Statement};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_sql::{PrepareOptions, parse, parse_syntax_in, prepare};
use rtdb_storage::{MemoryStore, cache::BufferPool};
use rtdb_types::{Result, Timestamp, Value as V};
use tokio_util::sync::CancellationToken;

fn context() -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions:Vec::new(),storage:Arc::new(BufferPool::new(Arc::new(MemoryStore::default()),1<<20)) };
    let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),database)]),false).unwrap();
    QueryContext::new(catalog,MemoryBudget::new(1<<27),Timestamp(0),"UTC",QueryOptions { batch_rows:1,..Default::default() },&CancellationToken::new()).unwrap()
}

async fn query(sql: &str, arguments: Vec<V>) -> Result<Vec<Vec<V>>> {
    let context = context();
    let options = PrepareOptions { parameter_types:arguments.iter().enumerate().map(|(index,value)| (index,value.logical_type())).collect(),..Default::default() };
    let Statement::Query(relation) = prepare(sql,&context.catalog,&options)?.statement else { panic!("Expected query") };
    let mut cursor = PreparedQuery::new(relation)?.execute(Arc::clone(&context),arguments).await?;
    let mut rows = Vec::new();
    while let Some(page) = cursor.next_batch().await? {
        let batch = page.batch();
        rows.push(batch.schema().fields().iter().zip(batch.columns()).map(|(field,array)| read_value(field,array.as_ref(),0)).collect::<Result<Vec<_>>>()?);
    }
    drop(cursor);
    assert_eq!(context.memory.used_bytes(),0);
    assert_eq!(context.spill_bytes(),0);
    Ok(rows)
}

#[tokio::test]
async fn group_by_test_vec_agg_keeps_empty_global_vector_and_null_elements() {
    for (source, expected) in [
        ("(-> (rel [] [a]) (aggregate {:vec-out (vec-agg a)}))", vec![vec![V::list(vec![])]]),
        ("(-> (rel [] [a b]) (aggregate b {:vec-out (vec-agg a)}))", vec![]),
        ("(-> (rel [{:a nil}] [a]) (aggregate {:vec-out (vec-agg a)}))", vec![vec![V::list(vec![V::Null])]]),
        ("(-> (rel [{:a 42 :b nil}] [a b]) (aggregate a {:vec-out (vec-agg b)}))", vec![vec![V::I64(42), V::list(vec![V::Null])]]),
        ("(-> (rel [{:a 42 :b nil} {:a 45 :b 1} {:a 45 :b nil}] [a b]) (aggregate a {:vec-out (vec-agg b)}) (order-by a))",
            vec![vec![V::I64(42), V::list(vec![V::Null])], vec![V::I64(45), V::list(vec![V::I64(1), V::Null])]]),
    ] {
        assert_eq!(query(&format!("XTQL $${source}$$"), vec![]).await.unwrap(), expected, "{source}");
    }
    assert_eq!(query("XTQL $$(-> (rel [] [a]) (aggregate {:array-out (array-agg a)} {:vec-out (vec-agg a)}))$$", vec![]).await.unwrap(),
        [vec![V::Null, V::list(vec![])]]);
}

#[test]
fn xtql_vec_agg_metadata_is_nonnullable_and_does_not_invent_a_distinct_factory() {
    let context = context();
    let Statement::Query(relation) = prepare("XTQL $$(-> (rel [{:a 1}] [a]) (aggregate {:result (vec-agg a)}))$$", &context.catalog, &PrepareOptions::default()).unwrap().statement else { panic!("Expected query") };
    assert_eq!(relation.schema().fields()[0].data_type, rtdb_types::LogicalType::list(rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::I64)));
    assert!(prepare("XTQL $$(-> (rel [{:a 1}] [a]) (aggregate {:result (vec-agg-distinct a)}))$$", &context.catalog, &PrepareOptions::default()).is_err());
}

#[tokio::test]
async fn inline_xtql_is_a_query_expression_in_derived_tables_ctes_sets_and_scalar_subqueries() {
    for sql in [
        "XTQL $$(rel [{:v 7}] [v])$$",
        "SELECT v FROM (XTQL '(rel [{:v 7}] [v])') AS t",
        "WITH t AS (XTQL $q$(rel [{:v 7}] [v])$q$) SELECT v FROM t",
        "SELECT (XTQL $$(rel [{:v 7}] [v])$$) AS v",
        "SELECT v FROM (VALUES (7)) AS t(v) WHERE v IN (XTQL $$(rel [{:v 7}] [v])$$)",
        "SELECT 7 AS v WHERE EXISTS (XTQL $$(rel [{}] [])$$)",
    ] { assert_eq!(query(sql,vec![]).await.unwrap(),[vec![V::I64(7)]],"{sql}"); }
    assert_eq!(query("XTQL $$(rel [{:v 2}] [v])$$ UNION ALL XTQL $$(rel [{:v 1}] [v])$$ ORDER BY v",vec![]).await.unwrap(),[vec![V::I64(1)],vec![V::I64(2)]]);
    assert_eq!(query("SELECT l.v AS a,r.v AS b FROM (XTQL $$(rel [{:v 1}] [v])$$) l JOIN (XTQL $$(rel [{:v 2}] [v])$$) r ON l.v+1=r.v",vec![]).await.unwrap(),[vec![V::I64(1),V::I64(2)]]);
}

#[tokio::test]
async fn inline_xtql_runs_a_regexp_pattern_computed_from_raw_ratio_data() {
    let sql = r#"XTQL $$(-> (rel [{}] []) (return {:v (regexp-replace "1/2" (str (+ 1/6 2/6)) "x")}))$$"#;
    assert_eq!(query(sql, vec![]).await.unwrap(), [vec![V::Utf8("x".into())]]);
}

#[tokio::test]
async fn explicit_inline_fn_arguments_keep_their_lexical_or_numbered_sql_positions_5995() {
    for sql in [
        "SELECT ? AS prefix,t.* FROM (XTQL ($$#(rel [{:a %1 :b %2}] [a b])$$,?,?)) t",
        "SELECT $1 AS prefix,t.* FROM (XTQL ($$#(rel [{:a %1 :b %2}] [a b])$$,$2,$3)) t",
    ] {
        assert_eq!(parse(sql).unwrap()[0].parameter_count(),3);
        assert_eq!(query(sql,vec![V::Utf8("x".into()),V::I64(2),V::I64(3)]).await.unwrap(),[vec![V::Utf8("x".into()),V::I64(2),V::I64(3)]]);
    }
    let sql = "SELECT ? AS prefix,t.* FROM (XTQL ($$(fn [a b] (rel [{:a a :b b}] [a b]))$$,$3,$2)) t";
    assert_eq!(query(sql,vec![V::Utf8("x".into()),V::I64(2),V::I64(3)]).await.unwrap(),[vec![V::Utf8("x".into()),V::I64(3),V::I64(2)]]);
    let sql = "FROM (XTQL ($$#(rel [{:a %}] [a])$$,?)) t SELECT *,? AS suffix";
    assert_eq!(query(sql,vec![V::I64(2),V::Utf8("x".into())]).await.unwrap(),[vec![V::I64(2),V::Utf8("x".into())]]);
}

#[tokio::test]
async fn implicit_inline_fn_arguments_claim_slots_at_the_query_expression_position() {
    let sql = "SELECT ? AS prefix,t.* FROM (XTQL $$#(rel [{:a %1 :b %2}] [a b])$$) t";
    assert_eq!(parse(sql).unwrap()[0].parameter_count(),3);
    assert_eq!(query(sql,vec![V::Utf8("x".into()),V::I64(2),V::I64(3)]).await.unwrap(),[vec![V::Utf8("x".into()),V::I64(2),V::I64(3)]]);
    let sql = "FROM (XTQL $$#(rel [{:a %}] [a])$$) t SELECT *,? AS suffix";
    assert_eq!(parse(sql).unwrap()[0].parameter_count(),2);
    assert_eq!(query(sql,vec![V::I64(2),V::Utf8("x".into())]).await.unwrap(),[vec![V::I64(2),V::Utf8("x".into())]]);
    assert_eq!(query("XTQL $$#(rel [{:a %}] [a])$$ UNION ALL XTQL $$#(rel [{:a %}] [a])$$",vec![V::I64(2),V::I64(3)]).await.unwrap(),[vec![V::I64(2)],vec![V::I64(3)]]);
}

#[tokio::test]
async fn non_function_inline_queries_ignore_explicit_arguments_without_consuming_external_arity() {
    let sql = "XTQL ($$(rel [{:a 7}] [a])$$,?)";
    assert_eq!(parse(sql).unwrap()[0].parameter_count(),0);
    assert_eq!(query(sql,vec![]).await.unwrap(),[vec![V::I64(7)]]);
    let sql = "FROM (XTQL ($$(rel [{:a 7}] [a])$$,?)) t SELECT *,? AS suffix";
    assert_eq!(parse(sql).unwrap()[0].parameter_count(),2);
    assert_eq!(query(sql,vec![V::Null,V::I64(9)]).await.unwrap(),[vec![V::I64(7),V::I64(9)]]);
    assert_eq!(parse("XTQL ($$(rel [{}] [])$$,$100)").unwrap()[0].parameter_count(),0);
}

#[tokio::test]
async fn nested_xtql_and_sql_apply_parameters_share_a_hygienic_allocator() {
    let sql = "SELECT ? AS prefix,(SELECT q.result FROM (XTQL ($$(fn [wanted] (-> (rel [{:a wanted}] [a]) (with {:result (q [(fn [inner] (rel [{:result inner}] [result])) a])})))$$,?)) q WHERE q.a=t.v) AS result FROM (VALUES (7),(8)) AS t(v) ORDER BY t.v";
    assert_eq!(parse(sql).unwrap()[0].parameter_count(),2);
    assert_eq!(query(sql,vec![V::Utf8("x".into()),V::I64(7)]).await.unwrap(),[vec![V::Utf8("x".into()),V::I64(7)],vec![V::Utf8("x".into()),V::Null]]);
}

#[tokio::test]
async fn synthetic_inline_table_names_cannot_capture_real_ctes_or_literals() {
    let sql = "WITH \"_rtdb_inline_xtql_0\" AS (VALUES (9)) SELECT real._column_1,embedded.a FROM \"_rtdb_inline_xtql_0\" real CROSS JOIN (XTQL $$(rel [{:a 7}] [a])$$) embedded";
    assert_eq!(query(sql,vec![]).await.unwrap(),[vec![V::I64(9),V::I64(7)]]);
    assert_eq!(query("SELECT 'XTQL $$(rel [{}] [])$$' AS text",vec![]).await.unwrap(),[vec![V::Utf8("XTQL $$(rel [{}] [])$$".into())]]);
}

#[test]
fn inline_xtql_arity_and_reader_errors_retain_original_sql_spans() {
    let sql = "SELECT 1 FROM (XTQL ($$#(rel [{:a %}] [a])$$,?,?)) AS t";
    let error = parse(sql).unwrap_err();
    assert_eq!(error.code,"xtql/fn-arity-error");
    assert_eq!(error.message.as_deref(),Some("XTQL fn expects 1 argument(s), got 2"));
    assert_eq!(error.context["position"],serde_json::json!(sql.find("XTQL").unwrap()+1));
    let malformed = "SELECT 1 FROM (XTQL $$(rel [$$) AS t";
    let error = parse(malformed).unwrap_err();
    assert!(error.context.contains_key("offset"));
    assert_eq!(error.context["position"],serde_json::json!(malformed.find("$$").unwrap()+1));
    assert!(parse("XTQL ($$(rel [{}] [])$$,1)").is_err());
}

#[test]
fn surrounding_sql_syntax_precedes_inline_xtql_reader_errors() {
    let memory = MemoryBudget::new(0);
    for source in ["XTQL ($$($$, 1)", "XTQL $$($$; SELEC 8"] {
        let error = parse_syntax_in(source, &memory).unwrap_err();
        assert_eq!(error.code, "xtdb/sql-error", "{source}");
    }

    let source = "XTQL $$($$; SELECT 7";
    let mut statements = parse_syntax_in(source, &memory).unwrap().into_iter();
    let error = statements.next().unwrap().resolve().unwrap_err();
    assert_eq!(error.code, "xtql/edn-reader");
    assert_eq!(error.context["position"], serde_json::json!(source.find("$$").unwrap() + 1));
    assert_eq!(statements.next().unwrap().resolve().unwrap().parameter_count(), 0);
    assert_eq!(memory.peak_bytes(), 0);
}

#[tokio::test]
async fn anonymous_and_numbered_grouping_expressions_use_the_same_assigned_slot() {
    let sql = "SELECT v + ? AS x FROM (VALUES (1)) t(v) GROUP BY v + $1";
    assert_eq!(query(sql, vec![V::I64(2)]).await.unwrap(), [vec![V::I64(3)]]);
}

#[test]
fn numbered_parameter_values_are_checked_after_sql_grammar() {
    let memory = MemoryBudget::new(0);
    let error = parse_syntax_in("SELECT $0; SELEC 7", &memory).unwrap_err();
    assert_eq!(error.code, "xtdb/sql-error");
    assert!(error.message.as_deref().unwrap().contains("SELEC"));
    for parameter in ["$0", "$99999999999999999999999999"] {
        let source = format!("SELECT {parameter}");
        let syntax = parse_syntax_in(&source, &memory).unwrap().into_iter().next().unwrap();
        let error = syntax.resolve().unwrap_err();
        assert_eq!(error.message.as_deref(), Some("SQL parameters use ? or a positive $n index"));
    }
}

#[test]
fn inline_xtql_insert_source_uses_native_mutation_planning() {
    let context = context();
    let sql = "INSERT INTO destination (_id,v) XTQL $$(rel [{:_id 1 :v 7}] [_id v])$$";
    let Statement::Dml(Dml::Insert { input,.. }) = prepare(sql,&context.catalog,&PrepareOptions::default()).unwrap().statement else { panic!("Expected insert") };
    fn has_values(relation: &Relation) -> bool { matches!(relation.node(),RelationNode::Values { .. }) || relation.children().iter().any(|child| has_values(child)) }
    assert!(has_values(&input));
    assert_eq!(input.schema().fields().iter().map(|field| field.name.as_str()).collect::<Vec<_>>(),["_id","v"]);
}

#[test]
fn explicit_inline_xtql_checks_its_sql_arguments_and_closing_parenthesis_before_reading_edn() {
    let sql = "XTQL ($$[$$, 1)";
    let error = parse(sql).unwrap_err();
    assert_eq!(error.code, "xtdb/sql-error");
    assert_eq!(error.message.as_deref(), Some("XTQL arguments must be SQL parameters"));
    assert_eq!(error.context["position"], serde_json::json!(sql.find('1').unwrap() + 1));
    assert!(!error.context.contains_key("offset"));

    let error = parse("XTQL ($$[$$, ?").unwrap_err();
    assert_eq!(error.code, "xtdb/sql-error");
    assert_eq!(error.message.as_deref(), Some("Expected closing parenthesis after XTQL parameters"));
    assert_eq!(error.context["position"], serde_json::json!(1));
    assert!(!error.context.contains_key("offset"));

    let sql = "XTQL ($$[$$, ?)";
    let error = parse(sql).unwrap_err();
    assert_eq!(error.code, "xtql/edn-reader");
    assert_eq!(error.context["offset"], serde_json::json!(1));
    assert_eq!(error.context["position"], serde_json::json!(sql.find("$$").unwrap() + 1));
}

#[test]
fn explicit_inline_xtql_admits_body_metadata_after_valid_sql_arguments() {
    let embedded = r#"$$(-> (rel [{}] []) (return {:v (regexp-replace "abc" "a" "x")}))$$"#;
    let invalid = format!("XTQL ({embedded}, 1)");
    let valid = format!("XTQL ({embedded})");
    let context = context();
    let options = PrepareOptions::default();
    let denied = MemoryBudget::new(0);

    let error = rtdb_sql::parse_in(&invalid, &denied).unwrap_err();
    assert_eq!(error.code, "xtdb/sql-error");
    assert_eq!(error.message.as_deref(), Some("XTQL arguments must be SQL parameters"));
    assert_eq!(error.context["position"], serde_json::json!(invalid.find('1').unwrap() + 1));
    assert_eq!(rtdb_sql::prepare_in(&invalid, &context.catalog, &options, &denied).unwrap_err(), error);

    let error = rtdb_sql::parse_in(&valid, &denied).unwrap_err();
    assert_eq!(error.code, "rtdb/memory-budget-exceeded");
    assert_eq!(error.context["position"], serde_json::json!(valid.find("$$").unwrap() + 1));
    assert_eq!(rtdb_sql::prepare_in(&valid, &context.catalog, &options, &denied).unwrap_err(), error);
    assert_eq!(denied.used_bytes(), 0);

    let memory = MemoryBudget::new(1 << 20);
    let parsed = rtdb_sql::parse_in(&valid, &memory).unwrap();
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0].parameter_count(), 0);
    assert!(memory.used_bytes() > 0);
    drop(parsed);
    assert_eq!(memory.used_bytes(), 0);
}
