use std::collections::BTreeSet;
use rtdb_plan::{Catalog,QualifiedTable,TableBinding};
use rtdb_sql::{PrepareOptions,prepare};
use rtdb_types::Result;

struct Empty;
impl Catalog for Empty {
    fn default_database(&self) -> &str { "xtdb" }
    fn databases(&self) -> BTreeSet<String> { BTreeSet::from(["xtdb".into()]) }
    fn table(&self,_: &QualifiedTable) -> Result<Option<TableBinding>> { Ok(None) }
    fn tables(&self,_: &str) -> Result<Vec<TableBinding>> { Ok(Vec::new()) }
}
#[test]
fn sql_interval_grammar_does_not_admit_internal_constructor_names() {
    for expression in ["single_field_interval(1,'DAY',2,6)","multi_field_interval('1:02','HOUR',2,'MINUTE',6)"] {
        for source in [format!("SELECT {expression}"),format!("SETTING SNAPSHOT_TIME={expression} SELECT 1")] {
            assert!(prepare(&source,&Empty,&PrepareOptions::default()).is_err(),"{source}");
        }
    }
    for source in ["SELECT INTERVAL '1.25' SECOND(2)", "SELECT INTERVAL '1:02.3' MINUTE TO SECOND(1)"] {
        prepare(source,&Empty,&PrepareOptions::default()).unwrap();
    }
    for source in ["SELECT INTERVAL 1 DAY", "SELECT INTERVAL '1' DAY(3)", "SELECT FLOOR(1,2)", "SELECT CASE WHEN FALSE THEN INTERVAL 'bad' ELSE NULL END"] {
        assert!(prepare(source,&Empty,&PrepareOptions::default()).is_err(),"{source}");
    }
    prepare("SELECT INTERVAL '1' DAY, INTERVAL '1:02' HOUR TO MINUTE",&Empty,&PrepareOptions::default()).unwrap();
}

#[test]
fn invalid_unqualified_interval_literals_retain_planning_errors_before_execution() {
    for (text, message) in [
        ("garbage",None),("p1d",None),("P1Dgarbage",None),
        ("P1D1Y",Some("Text cannot be parsed to a Period")),
        ("PT1.1234567890S",Some("Text cannot be parsed to a Duration")),
        ("P200000000Y",Some("integer overflow")),
        ("P2147483648D",Some("Text cannot be parsed to a Period")),
        ("PT3000000H",Some("long overflow")),
        ("-P-2147483648D",Some("Text cannot be parsed to a Period")),
        ("P306783379W",Some("integer overflow")),
        ("PT9223372036854775807S",Some("long overflow")),
        ("PT9223372036854775808H",Some("Text cannot be parsed to a Duration: hours")),
        ("PT2562047788015216H",Some("Text cannot be parsed to a Duration: hours")),
        ("P200000000YT1.1234567890S",Some("Text cannot be parsed to a Duration")),
        ("P200000000YT3000000H",Some("integer overflow")),
        ("P200000000YT9223372036854775808H",Some("Text cannot be parsed to a Duration: hours")),
        ("2147483648 days",Some("integer overflow")),
        ("9223372036854775808 years",Some("For input string: \"9223372036854775808\"")),
        ("768614336404564651 years",Some("long overflow")),
        ("1\u{85}day",None),("1\u{1c}day",None),
    ] {
        for source in [format!("SELECT INTERVAL '{text}'"),format!("SELECT CASE WHEN FALSE THEN INTERVAL '{text}' ELSE NULL END"),format!("SETTING SNAPSHOT_TIME=INTERVAL '{text}' SELECT 1")] {
            let error = prepare(&source,&Empty,&PrepareOptions::default()).unwrap_err();
            assert_eq!(error.category,rtdb_types::AnomalyCategory::Incorrect,"{source}");
            assert_eq!(error.code,"xtdb/sql-error","{source}");
            assert_eq!(error.message.as_deref(),Some(format!("Errors planning SQL statement:\n  - Cannot parse interval: {text} - failed with message {}",message.unwrap_or("null")).as_str()),"{source}");
            assert_eq!(serde_json::to_value(&error.context).unwrap(),serde_json::json!({"errors":[{"i-str":text,"msg":message}]}),"{source}");
        }
    }
}

#[test]
fn interval_diagnostics_accumulate_in_planner_encounter_order() {
    for (source, inputs) in [
        ("SELECT INTERVAL 'bad' AS a,INTERVAL 'worse' AS b",vec!["bad","worse"]),
        ("SELECT INTERVAL 'bad' AS a,INTERVAL 'bad' AS b",vec!["bad","bad"]),
        ("SELECT CASE WHEN FALSE THEN INTERVAL 'bad' ELSE INTERVAL 'worse' END AS value",vec!["bad","worse"]),
        ("SETTING SNAPSHOT_TIME=INTERVAL 'bad',SNAPSHOT_TOKEN=INTERVAL 'worse' SELECT INTERVAL 'also-bad'",vec!["bad","worse","also-bad"]),
        ("SELECT INTERVAL 'select-bad' AS s GROUP BY INTERVAL 'group-bad'",vec!["select-bad","group-bad"]),
        ("SELECT INTERVAL 'bad' AS s GROUP BY INTERVAL 'bad'",vec!["bad","bad"]),
        ("SELECT INTERVAL 'select-bad' AS s GROUP BY INTERVAL 'group-bad' HAVING INTERVAL 'having-bad' IS NULL",vec!["having-bad","select-bad","group-bad"]),
        ("SELECT INTERVAL 'outer-left' AS a,(SELECT INTERVAL 'inner-select' GROUP BY INTERVAL 'inner-group') AS b,INTERVAL 'outer-right' AS c GROUP BY INTERVAL 'outer-group'",vec!["outer-left","inner-select","inner-group","outer-right","outer-group"]),
        ("SELECT INTERVAL 'select-bad' AS s FROM UNNEST(ARRAY[INTERVAL 'from-bad']) AS u(v) WHERE INTERVAL 'where-bad' IS NULL HAVING INTERVAL 'having-bad' IS NULL",vec!["from-bad","where-bad","having-bad","select-bad"]),
        ("SELECT ARRAY_AGG(INTERVAL 'argument-bad' ORDER BY INTERVAL 'sort-bad') FILTER (WHERE INTERVAL 'filter-bad' IS NULL) AS r",vec!["argument-bad","filter-bad","sort-bad"]),
        ("SELECT INTERVAL 'needle-bad' IN (SELECT INTERVAL 'candidate-bad')",vec!["needle-bad","candidate-bad"]),
        ("SELECT TRIM(INTERVAL 'char-bad' FROM INTERVAL 'source-bad')",vec!["char-bad","source-bad"]),
    ] {
        let error = prepare(source,&Empty,&PrepareOptions::default()).unwrap_err();
        let mut message = String::from("Errors planning SQL statement:");
        for input in &inputs { message.push_str(&format!("\n  - Cannot parse interval: {input} - failed with message null")); }
        assert_eq!(error.message,Some(message),"{source}");
        assert_eq!(serde_json::to_value(&error.context).unwrap(),serde_json::json!({"errors":inputs.iter().map(|input| serde_json::json!({"i-str":input,"msg":null})).collect::<Vec<_>>()}),"{source}");
    }
}
