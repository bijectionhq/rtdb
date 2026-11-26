//! The reference's `CrashLogger`: a document write that fails for a reason other than the caller's leaves `crashes/<node-id>/<instant>/crash.edn` in the database's object store before the failure stops ingestion.

use std::fmt::Write as _;
use std::sync::Arc;

use bytes::Bytes;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_storage::{ObjectKey, ObjectStore};
use rtdb_types::{Anomaly, AnomalyCategory, Instant, Result, Timestamp, transit_value::{TransitValue, from_json_value}};

/// The context the resolver attaches to a failed put, delete or erase, which is what the reference's `withCrashLog` wraps.
const DATA_KEYS: [&str; 3] = ["table", "tx-key", "tx-op-idx"];

pub(crate) fn applies(error: &Anomaly) -> bool {
    !error.is_caller_error() && DATA_KEYS.iter().all(|key| error.context.contains_key(*key))
}

/// A failure to write the log is reported and swallowed, so that it never masks the error being logged.
pub(crate) async fn write(store: &dyn ObjectStore, node_id: &str, now: Timestamp, error: &Anomaly, memory: &Arc<MemoryBudget>) {
    let directory = match directory(node_id, now) {
        Ok(directory) => directory,
        Err(failure) => { eprintln!("crash log for {error} could not be named: {failure}"); return; }
    };
    eprintln!("writing crash log: {directory}");
    match put(store, &directory, error, memory).await {
        Ok(()) => eprintln!("crash log written: {directory}"),
        Err(failure) => eprintln!("crash log {directory} could not be written: {failure}"),
    }
}

fn directory(node_id: &str, now: Timestamp) -> Result<String> {
    let seconds = Timestamp(now.0.div_euclid(1_000_000).saturating_mul(1_000_000));
    Ok(format!("crashes/{node_id}/{}", Instant::from(seconds).to_iso_string()?))
}

struct Payload { bytes: Vec<u8>, _reservation: Arc<MemoryReservation> }
impl AsRef<[u8]> for Payload { fn as_ref(&self) -> &[u8] { &self.bytes } }

async fn put(store: &dyn ObjectStore, directory: &str, error: &Anomaly, memory: &Arc<MemoryBudget>) -> Result<()> {
    let key = ObjectKey::new(format!("{directory}/crash.edn"))?;
    let edn = crash_edn(error)?;
    let reservation = memory.reserve(edn.len().saturating_mul(2).saturating_add(4096))?;
    store.put(&key, Bytes::from_owner(Payload { bytes: edn.into_bytes(), _reservation: reservation })).await?;
    Ok(())
}

/// The reference's map: the wrapped operation's data, its message, then the anomaly as `#xt/error`.
/// The anomaly is printed on one line, where the reference pretty-prints it.
fn crash_edn(error: &Anomaly) -> Result<String> {
    let mut out = String::from("{");
    for key in DATA_KEYS {
        write_keyword(&mut out, key);
        out.push(' ');
        write_value(&mut out, &from_json_value(&error.context[key])?);
        out.push_str(", ");
    }
    out.push_str(":msg ");
    write_string(&mut out, "error indexing documents");
    out.push_str(", :ex #xt/error [");
    write_keyword(&mut out, category(error.category));
    out.push(' ');
    write_keyword(&mut out, &error.code);
    out.push(' ');
    match &error.message { Some(message) => write_string(&mut out, message), None => out.push_str("nil") }
    out.push_str(" {");
    for (index, (key, value)) in error.context.iter().enumerate() {
        if index > 0 { out.push_str(", "); }
        write_keyword(&mut out, key);
        out.push(' ');
        write_value(&mut out, &from_json_value(value)?);
    }
    out.push_str("}]}\n");
    Ok(out)
}

fn category(category: AnomalyCategory) -> &'static str {
    match category {
        AnomalyCategory::Incorrect => "incorrect", AnomalyCategory::Unsupported => "unsupported", AnomalyCategory::NotFound => "not-found",
        AnomalyCategory::Forbidden => "forbidden", AnomalyCategory::Conflict => "conflict", AnomalyCategory::Busy => "busy",
        AnomalyCategory::Unavailable => "unavailable", AnomalyCategory::Interrupted => "interrupted", AnomalyCategory::Fault => "fault",
    }
}

fn write_keyword(out: &mut String, name: &str) {
    out.push(':');
    out.push_str(name);
}

fn write_string(out: &mut String, text: &str) {
    out.push('"');
    for character in text.chars() {
        match character {
            '"' => out.push_str("\\\""), '\\' => out.push_str("\\\\"), '\n' => out.push_str("\\n"), '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"), '\u{c}' => out.push_str("\\f"), '\u{8}' => out.push_str("\\b"),
            character => out.push(character),
        }
    }
    out.push('"');
}

fn write_float(out: &mut String, value: f64) {
    if value.is_nan() { out.push_str("##NaN") }
    else if value.is_infinite() { out.push_str(if value > 0.0 { "##Inf" } else { "##-Inf" }) }
    else { let _ = write!(out, "{value:?}"); }
}

fn write_value(out: &mut String, value: &TransitValue) {
    match value {
        TransitValue::Null => out.push_str("nil"),
        TransitValue::Bool(value) => out.push_str(if *value { "true" } else { "false" }),
        TransitValue::Integer(value) => { let _ = write!(out, "{value}"); }
        TransitValue::Float(value) => write_float(out, *value),
        TransitValue::Float32(value) => write_float(out, f64::from(*value)),
        TransitValue::String(value) => write_string(out, value),
        TransitValue::Keyword(value) => write_keyword(out, value),
        TransitValue::Symbol(value) => out.push_str(value),
        TransitValue::Bytes(value) => {
            out.push_str("#bytes \"");
            for byte in value { let _ = write!(out, "{byte:02x}"); }
            out.push('"');
        }
        TransitValue::Array(values) => {
            out.push('[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 { out.push(' '); }
                write_value(out, value);
            }
            out.push(']');
        }
        TransitValue::Map(entries) => {
            out.push('{');
            for (index, (key, value)) in entries.iter().enumerate() {
                if index > 0 { out.push_str(", "); }
                write_value(out, key);
                out.push(' ');
                write_value(out, value);
            }
            out.push('}');
        }
        TransitValue::Tagged { tag, representation } => write_tagged(out, tag, representation),
    }
}

fn write_tagged(out: &mut String, tag: &str, representation: &TransitValue) {
    match (tag, representation) {
        ("xt/table", TransitValue::Map(entries)) => {
            let field = |name: &str| entries.iter().find_map(|(key, value)| match (key, value) {
                (TransitValue::Keyword(key), TransitValue::String(value)) if key == name => Some(value.as_str()),
                _ => None,
            });
            out.push_str("#xt/table ");
            match (field("schema-name"), field("table-name")) {
                (Some("public") | None, Some(table)) => out.push_str(table),
                (Some(schema), Some(table)) => { out.push_str(schema); out.push('/'); out.push_str(table); }
                _ => write_value(out, representation),
            }
        }
        _ => {
            let tag = match tag { "time/instant" => "xt/instant", "xtdb/tx-key" => "xt/tx-key", tag => tag };
            out.push('#');
            out.push_str(tag);
            out.push(' ');
            write_value(out, representation);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_storage::MemoryStore;
    use rtdb_types::{TableRef, transit_value::to_json_value};

    fn failed_put(category: AnomalyCategory) -> Anomaly {
        let tx_key = TransitValue::Tagged { tag: "xtdb/tx-key".into(), representation: Box::new(TransitValue::Map(vec![
            (TransitValue::Keyword("tx-id".into()), TransitValue::Integer(1)),
            (TransitValue::Keyword("system-time".into()), TransitValue::instant(Timestamp(0).into()).unwrap()),
        ])) };
        Anomaly::new(category, "rtdb.live/append", "Live append failed")
            .with_context("table", to_json_value(&TransitValue::table_ref(&TableRef::new("public", "foo"))).unwrap())
            .with_context("tx-key", to_json_value(&tx_key).unwrap())
            .with_context("tx-op-idx", 0.into())
    }

    #[test]
    fn only_document_writes_that_fail_for_reasons_other_than_the_callers_are_logged() {
        assert!(applies(&failed_put(AnomalyCategory::Fault)));
        assert!(applies(&failed_put(AnomalyCategory::Interrupted)));
        assert!(!applies(&failed_put(AnomalyCategory::Incorrect)));
        assert!(!applies(&Anomaly::fault("rtdb.live/append", "Live append failed")));
    }

    #[tokio::test]
    async fn a_failed_document_write_leaves_crash_edn_under_the_node_and_the_second_it_failed() {
        let store = MemoryStore::default();
        let memory = MemoryBudget::new(1 << 20);
        write(&store, "xtdb-foo-node", Timestamp(1_500_000), &failed_put(AnomalyCategory::Fault), &memory).await;
        let edn = store.get(&ObjectKey::new("crashes/xtdb-foo-node/1970-01-01T00:00:01Z/crash.edn").unwrap()).await.unwrap();
        let tx_key = "#xt/tx-key {:tx-id 1, :system-time #xt/instant \"1970-01-01T00:00:00Z\"}";
        assert_eq!(std::str::from_utf8(&edn).unwrap(), format!(
            "{{:table #xt/table foo, :tx-key {tx_key}, :tx-op-idx 0, :msg \"error indexing documents\", \
             :ex #xt/error [:fault :rtdb.live/append \"Live append failed\" {{:table #xt/table foo, :tx-key {tx_key}, :tx-op-idx 0}}]}}\n"));
    }
}
