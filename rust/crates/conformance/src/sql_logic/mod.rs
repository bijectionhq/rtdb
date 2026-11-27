mod engine;
mod format;

use md5::Md5;
use rtdb_format::keys::CURRENT_STORAGE_VERSION;
use rtdb_types::Anomaly;
use serde::{Deserialize, Serialize};
use serde_json::{Value as Json, json};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fs::{self, File, OpenOptions}, io::{BufRead, BufReader, Read, Write}, path::{Component, Path}};

type Error = Box<dyn std::error::Error>;
const MAX_LINE: u64 = 64 * 1024 * 1024;
const C84_REFERENCE: &str = "c84f9334eb060ceb92d28ecdd4a06e6b625932fd";
const UPSTREAM_COMMIT: &str = include_str!("../../../../conformance/upstream-commit.txt");

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Options {
    pub direct_sql: bool,
    pub decorrelate: bool,
    pub query_limit: Option<u64>,
    pub max_rows: usize,
    pub max_result_bytes: usize,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Plan { schema_version: u32, reference: String, selection: String, scripts: Vec<Script> }
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Script { source: String, source_sha256: String, records: u64, assertions: u64, parse_errors: u64, records_file: String, records_sha256: String, selection: String, options: Options }
#[derive(Deserialize)]
struct Entry { identity: Json, ordinal: u64, assertion_ordinal: Option<u64>, line: u64, record: Option<Record>, parse_error: Option<String> }
#[derive(Deserialize)]
pub(super) struct Record {
    #[serde(rename = "type")] kind: String,
    mode: Option<String>, pub sql: Option<String>, pub type_string: Option<String>, pub sort: Option<String>,
    label: Option<String>, expected: Option<Json>, #[serde(default)] skipif: Vec<String>, onlyif: Option<String>,
    threshold: Option<i64>, variable: Option<String>, value: Option<String>,
}

fn md5(text: &str) -> String { format!("{:x}", Md5::digest(text.as_bytes())) }
fn digest(path: &Path) -> Result<String, Error> {
    let mut file = File::open(path)?; let mut digest = Sha256::new(); let mut buffer = [0; 64 * 1024];
    loop { let count = file.read(&mut buffer)?; if count == 0 { break; } digest.update(&buffer[..count]); }
    Ok(format!("{:x}", digest.finalize()))
}
fn write_json(path: &Path, value: &Json) -> Result<(), Error> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    serde_json::to_writer(&mut file, value)?; file.write_all(b"\n")?; file.sync_all()?; Ok(())
}
pub(super) fn combine<T>(result: Result<T, Error>, close: Result<(), Error>) -> Result<T, Error> {
    match (result, close) { (Ok(value), Ok(())) => Ok(value), (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error), (Err(error), Err(close)) => Err(format!("{error}; owner close also failed: {close}").into()) }
}
fn error_value(error: &Anomaly) -> Json { json!({"category":error.category,"code":error.code,"message":error.message}) }
fn caught(record: &Record, error: &Anomaly) -> &'static str {
    if error.code.starts_with("rtdb.conformance/slt-harness") || error.code == "rtdb.conformance/slt-format" { "harness_gap" }
    else if record.skipif.iter().any(|engine| engine == "postgresql") { "suppressed_postgresql_error" }
    else if error.message.as_deref().is_some_and(|message| message.contains("Duplicate column projection")) { "suppressed_duplicate_projection" }
    else { "error" }
}
fn expected_matches(record: &Record, result: &Json) -> bool {
    let Some(expected) = &record.expected else { return false; };
    let Some(text) = result.get("text").and_then(Json::as_str) else { return false; };
    if let Some(expected) = expected.get("md5").and_then(Json::as_str) { expected == md5(text) }
    else { expected.get("text").and_then(Json::as_str) == Some(text) }
}

async fn script(plan: &Plan, entry: &Script, input: &Path, output: &Path) -> Result<Json, Error> {
    if entry.options.max_rows == 0 || entry.options.max_rows as u64 > i64::MAX as u64 || entry.options.query_limit.is_some_and(|limit| limit > i64::MAX as u64)
        || entry.options.max_result_bytes == 0 || entry.options.max_result_bytes > 512 * 1024 * 1024 { return Err("invalid SLT result retention limit".into()); }
    if entry.parse_errors != 0 { return record_unavailable(plan, entry, input, output, "script_parse_error"); }
    let running = engine::Running::open().await?;
    let result = async {
        let mut engine = engine::Engine::new(running.node.clone())?;
        let mut stream = BufReader::new(File::open(input)?);
        let mut output = OpenOptions::new().write(true).create_new(true).open(output)?;
        let mut queries = 0u64;
        let mut count = 0u64;
        let mut assertions = 0u64;
        let mut variables = BTreeMap::new();
        let mut threshold = None;
        let mut dispositions = BTreeMap::<String, u64>::new();
        while let Some(original) = read_entry(&mut stream)? {
            validate_identity(plan, entry, &original, count + 1)?;
            if let Some(ordinal) = original.assertion_ordinal {
                assertions += 1;
                if ordinal != assertions { return Err("SLT assertion ordinal differs from original record order".into()); }
            }
            count += 1;
            let mut charges = Vec::new();
            let mut receipt = json!({"identity":original.identity,"executed":false,"engine_executed":false,"disposition":"pending","result":null,"error":null,"query_count_before":queries,"hash_threshold":threshold});
            if entry.options.query_limit == Some(queries) {
                receipt["disposition"] = "query_limit".into();
            } else if let Some(record) = original.record {
                receipt["kind"] = record.kind.clone().into();
                receipt["label"] = json!(record.label);
                receipt["expected"] = json!(record.expected);
                receipt["executed"] = true.into();
                match record.kind.as_str() {
                    "hash-threshold" => { threshold = record.threshold; receipt["disposition"] = "directive".into(); }
                    "variable" => {
                        variables.insert(record.variable.unwrap_or_default(), record.value);
                        let bytes = variables.iter().map(|(name, value)| name.len().saturating_add(value.as_ref().map_or(0, String::len)).saturating_add(256))
                            .fold(0usize, usize::saturating_add);
                        if bytes > 16 * 1024 * 1024 { return Err("SLT variable state exceeds retained size limit".into()); }
                        receipt["disposition"] = "directive".into();
                    }
                    "halt" => { receipt["disposition"] = "halt_error".into(); receipt["error"] = json!({"message":"Pinned runner updates a Reduced context after halt"}); }
                    "statement" | "query" => {
                        let skip = record.skipif.iter().any(|value| value == "xtdb") || record.onlyif.as_deref().is_some_and(|value| value != "xtdb");
                        if !engine.valid() {
                            let error = Anomaly::incorrect("rtdb.conformance/slt-nil-engine", "No implementation of DbEngine for nil");
                            receipt["disposition"] = caught(&record, &error).into(); receipt["error"] = error_value(&error);
                        } else if skip { receipt["disposition"] = "engine_condition".into(); }
                        else if record.kind == "statement" {
                            receipt["engine_executed"] = true.into();
                            match engine.statement(record.sql.as_deref().unwrap_or(""), &entry.options).await {
                                Ok((state, effect)) => {
                                    receipt["effect"] = effect.into();
                                    if record.mode.as_deref() == Some("ok") { engine.state = state; receipt["disposition"] = "statement_ok".into(); }
                                    else { receipt["disposition"] = "statement_expected_error_missing".into(); }
                                }
                                Err(error) => {
                                    let disposition = if error.code.starts_with("rtdb.conformance/slt-harness") { "harness_gap" }
                                        else if record.mode.as_deref() == Some("error") && error.code != "rtdb.conformance/slt-assertion" { "statement_expected_error" }
                                        else { caught(&record, &error) };
                                    receipt["disposition"] = disposition.into(); receipt["error"] = error_value(&error);
                                }
                            }
                        } else {
                            receipt["engine_executed"] = true.into();
                            match engine.query(&record, &variables, &entry.options).await {
                                Ok((mut result, reservations)) => {
                                    result["md5"] = md5(result["text"].as_str().expect("query text")).into();
                                    receipt["disposition"] = if expected_matches(&record, &result) { "query_ok" } else { "query_mismatch" }.into();
                                    receipt["result"] = result; charges = reservations;
                                }
                                Err(error) => { receipt["disposition"] = caught(&record, &error).into(); receipt["error"] = error_value(&error); }
                            }
                        }
                        if record.kind == "query" { queries += 1; }
                    }
                    _ => return Err("unknown SLT record kind in validated plan".into()),
                }
            } else { receipt["disposition"] = "record_parse_error".into(); receipt["error"] = json!(original.parse_error); }
            *dispositions.entry(receipt["disposition"].as_str().expect("disposition").into()).or_default() += 1;
            receipt["query_count_after"] = queries.into();
            serde_json::to_writer(&mut output, &receipt)?; output.write_all(b"\n")?;
            drop(receipt);
            drop(charges);
        }
        if count != entry.records || assertions != entry.assertions { return Err("SLT record or assertion count differs from the retained plan".into()); }
        output.sync_all()?;
        Ok(json!({"source":entry.source,"selection":entry.selection,"records":count,"queries_run":queries,"dispositions":dispositions,"execution_complete":true}))
    }.await;
    combine(result, running.close().await)
}

fn read_entry(reader: &mut BufReader<File>) -> Result<Option<Entry>, Error> {
    let mut line = Vec::new();
    let count = Read::by_ref(reader).take(MAX_LINE + 1).read_until(b'\n', &mut line)?;
    if count == 0 { return Ok(None); }
    if count as u64 > MAX_LINE || line.last() != Some(&b'\n') { return Err("SLT record input is oversized or lacks a complete final line".into()); }
    Ok(Some(serde_json::from_slice(&line)?))
}
fn validate_identity(plan: &Plan, script: &Script, record: &Entry, ordinal: u64) -> Result<(), Error> {
    let identity = &record.identity;
    if record.ordinal != ordinal || record.line == 0 || identity["reference"] != plan.reference || identity["source"] != script.source
        || identity["source_sha256"] != script.source_sha256 || identity["selection"] != script.selection || identity["ordinal"] != ordinal
        || identity["line"] != record.line || identity["assertion_ordinal"] != json!(record.assertion_ordinal)
        || identity["options"] != serde_json::to_value(&script.options)?
    { return Err("SLT record identity differs from its retained source selection".into()); }
    Ok(())
}
fn record_unavailable(plan: &Plan, script: &Script, input: &Path, output: &Path, reason: &str) -> Result<Json, Error> {
    let mut reader = BufReader::new(File::open(input)?);
    let mut output = OpenOptions::new().write(true).create_new(true).open(output)?;
    let mut count = 0;
    let mut assertions = 0;
    while let Some(record) = read_entry(&mut reader)? {
        count += 1; validate_identity(plan, script, &record, count)?;
        if let Some(ordinal) = record.assertion_ordinal {
            assertions += 1;
            if ordinal != assertions { return Err("SLT unavailable assertion ordinal differs from original record order".into()); }
        }
        serde_json::to_writer(&mut output, &json!({"identity":record.identity,"executed":false,"engine_executed":false,"disposition":reason,"error":record.parse_error}))?;
        output.write_all(b"\n")?;
    }
    if count != script.records || assertions != script.assertions { return Err("SLT unavailable record or assertion count differs from plan".into()); }
    output.sync_all()?;
    Ok(json!({"source":script.source,"selection":script.selection,"records":count,"execution_complete":true,"dispositions":{(reason):count}}))
}

fn execution_environment() -> Json {
    json!({
        "storage_version": CURRENT_STORAGE_VERSION.0,
        "timezone": "UTC",
        "locale": "en_US",
        "source_clock": "2020-01-01T00:00:00Z plus one day per submitted transaction",
        "query_clock": "wall_clock_or_CURRENT_TIMESTAMP_variable",
    })
}

/// Executes every retained record in its script's production node lifetime, then joins that node.
/// The caller must await completion; externally terminated runs have no completed execution receipt.
/// Writes new files beneath an absent output directory and never changes source expectations.
pub async fn execute(plan_path: &Path, output: &Path) -> Result<Json, Error> {
    let mut bytes = Vec::new();
    File::open(plan_path)?.take(16 * 1024 * 1024 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > 16 * 1024 * 1024 { return Err("SLT plan exceeds bounded size".into()); }
    let plan: Plan = serde_json::from_slice(&bytes)?;
    if plan.schema_version != 1 || ![C84_REFERENCE, UPSTREAM_COMMIT.trim()].contains(&plan.reference.as_str()) || plan.scripts.is_empty() { return Err("invalid SLT selection plan".into()); }
    let plan_sha = format!("{:x}", Sha256::digest(&bytes));
    fs::create_dir(output)?;
    let mut scripts = Vec::new();
    let mut complete = true;
    for (index, script_entry) in plan.scripts.iter().enumerate() {
        if Path::new(&script_entry.records_file).components().any(|part| !matches!(part, Component::Normal(_))) { return Err("SLT record path escapes plan directory".into()); }
        let input = plan_path.parent().unwrap_or(Path::new(".")).join(&script_entry.records_file);
        if digest(&input)? != script_entry.records_sha256 { return Err("SLT record artifact changed before execution".into()); }
        let filename = format!("script-{index:05}.jsonl");
        let path = output.join(&filename);
        let result = script(&plan, script_entry, &input, &path).await;
        let mut receipt = match result {
            Ok(receipt) => receipt,
            Err(error) => { complete = false; json!({"source":script_entry.source,"selection":script_entry.selection,"execution_complete":false,"error":error.to_string()}) }
        };
        receipt["requested_decorrelate"] = script_entry.options.decorrelate.into();
        receipt["native_optimizer_mode"] = "production_correlated_apply; no decorrelation optimizer or plan parity asserted".into();
        if path.exists() { receipt["observations"] = json!({"path":filename,"sha256":digest(&path)?}); }
        if digest(&input)? != script_entry.records_sha256 { return Err("SLT record artifact changed during execution".into()); }
        scripts.push(receipt);
    }
    if digest(plan_path)? != plan_sha { return Err("SLT plan changed during execution".into()); }
    let execution = json!({"schema_version":1,"engine":"rust","selection":plan.selection,"plan_sha256":plan_sha,
        "artifact_revision":env!("RTDB_CONFORMANCE_REVISION"),"source_dirty":env!("RTDB_CONFORMANCE_DIRTY") != "false",
        "binary_sha256":digest(&std::env::current_exe()?)?,"execution_complete":complete,"complete_parity":false,"scripts":scripts,
        "environment":execution_environment()});
    write_json(&output.join("execution.json"), &execution)?;
    Ok(execution)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn execution_environment_records_storage_version_and_utc_query_configuration() {
        let environment = execution_environment();
        assert_eq!(environment["storage_version"].as_i64(), Some(i64::from(CURRENT_STORAGE_VERSION.0)));
        assert_eq!(environment["timezone"].as_str(), Some("UTC"));
    }

    #[test] fn pinned_md5_ignores_declared_size_and_keeps_hash_case() {
        let mut record: Record = serde_json::from_value(json!({"type":"query","expected":{"count":999,"md5":md5("1\n")}})).unwrap();
        assert!(expected_matches(&record, &json!({"text":"1\n"})));
        record.expected.as_mut().unwrap()["md5"] = md5("1\n").to_uppercase().into();
        assert!(!expected_matches(&record, &json!({"text":"1\n"})));
    }
    #[test] fn suppressed_errors_and_harness_gaps_are_distinct() {
        let record: Record = serde_json::from_value(json!({"type":"query","skipif":["postgresql"]})).unwrap();
        assert_eq!(caught(&record, &Anomaly::incorrect("xtdb/sql-error", "syntax")), "suppressed_postgresql_error");
        assert_eq!(caught(&record, &engine::gap("unqualified codec")), "harness_gap");
    }
}
