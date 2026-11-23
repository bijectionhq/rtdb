//! Check exact task carriers and atomic schedule coverage on owned orders.
//! Cadence and executable interpretation belong to the application owner.

use std::collections::{BTreeMap, BTreeSet};

use rtdb_types::{Anomaly, EntityId, Result, Value};
use serde_json::Value as Json;

use crate::{HostedControlWrite, TxOp, source::Operation};

const ADMISSION: &str = "bj_fact_root_admission";
const CATALOG: &str = "schedule-catalog:";
const COVERAGE: &str = "schedule-coverage:";
const CLOCK: &str = "clock-task:";
const ACTION: &str = "action-task:";
const MAX_CONTROL_BYTES: usize = 64 << 10;

fn invalid() -> Anomaly {
    Anomaly::incorrect(
        "rtdb.hosted/schedule-order",
        "Schedule coverage and its occurrence admissions must share one ordered publication",
    )
}

fn body(control: &HostedControlWrite) -> Result<Json> {
    let Some(Value::Binary(bytes)) = control.replacement().and_then(|row| row.get("admission")) else {
        return Err(invalid());
    };
    if bytes.len() > MAX_CONTROL_BYTES {
        return Err(invalid());
    }
    serde_json::from_slice(bytes).map_err(|_| invalid())
}

fn digest(digest: &str) -> bool {
    digest.len() == 64 && digest.bytes().all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn action_command_matches(record: &Json) -> bool {
    use sha2::{Digest, Sha256};

    let Some(task) = record.get("task") else { return false };
    let Some(cutoff) = task.get("cutoff") else { return false };
    let exact = match (task["emitted_key"].as_str(), task["emitted_command"].as_str()) {
        (None, None) => true,
        (Some(_), Some(command)) => {
            command.starts_with("[\"bijection.task.emit.v2\",")
                && task["command"].as_array().is_some_and(|bytes| {
                    let digest = Sha256::digest(command.as_bytes());
                    bytes.len() == digest.len()
                        && bytes.iter().zip(digest.iter()).all(|(byte, expected)| {
                            byte.as_u64() == Some(u64::from(*expected))
                        })
                })
        }
        (None, Some(_)) | (Some(_), None) => false,
    };
    exact && (cutoff.is_null() || (
        task["external"].is_object() && task["emitted_command"].is_string()
            && cutoff.as_object().is_some_and(|term| term.len() == 1
                && (term.get("fixed").is_some_and(|at| !at.is_null())
                    || term.get("fromAcceptance").and_then(Json::as_i64).is_some_and(|offset| offset > 0)))
    ))
}

fn continuation_policy_matches(continuation: &Json) -> bool {
    let Some(record) = continuation.as_object() else { return false };
    let named = record.get("name").is_some_and(|name| {
        name.is_null() || name.as_str().is_some_and(|name| {
            !name.is_empty() && name.len() <= 128 && !name.chars().any(char::is_control)
        })
    });
    let policy = matches!(record.get("on_failure").and_then(Json::as_str), Some("retry" | "skip"));
    named && policy && record.get("then").is_none_or(continuation_policy_matches)
}

pub(super) fn validate(operations: &[Operation]) -> Result<()> {
    let controls = operations.iter().filter_map(|op| match op {
        Operation::Public(TxOp::HostedControl(control))
            if control.name() == ADMISSION && !control.is_check() => {
                let EntityId::String(id) = control.id() else { return None };
                Some((id.as_str(), control))
            }
        _ => None,
    }).collect::<Vec<_>>();
    let mut catalogs = BTreeMap::new();
    for (id, control) in &controls {
        if let Some(catalog) = id.strip_prefix(CATALOG) {
            let data = body(control)?;
            if !digest(catalog) || catalogs.insert(catalog, (*control, data)).is_some() {
                return Err(invalid());
            }
        }
    }

    let mut expected = BTreeSet::new();
    for (id, control) in &controls {
        let Some(slot) = id.strip_prefix(COVERAGE) else { continue };
        let data = body(control)?;
        // The coverage collector holds the old cell before dropping its body.
        if data.is_null() {
            if control.expected().is_none() {
                return Err(invalid());
            }
            continue;
        }
        let (catalog, sequence) = slot.split_once(':').ok_or_else(invalid)?;
        let (catalog_control, catalog_body) = catalogs.get(catalog).ok_or_else(invalid)?;
        let sequence = sequence.parse::<u64>().map_err(|_| invalid())?;
        if control.expected().is_some() || control.scope() != catalog_control.scope()
            || data["catalog"].as_str() != Some(catalog)
            || data["sequence"].as_u64() != Some(sequence)
            || &data["label"] != &catalog_body["label"]
            || sequence.checked_add(1) != catalog_body["next_coverage"].as_u64()
        {
            return Err(invalid());
        }
        let entries = data["entries"].as_array().ok_or_else(invalid)?;
        if entries.is_empty() || entries.len() > 32 {
            return Err(invalid());
        }
        for entry in entries {
            match entry["disposition"]["status"].as_str() {
                Some("admitted") => {
                    let task = entry["disposition"]["task"].as_str().ok_or_else(invalid)?;
                    if !expected.insert(task.to_owned()) {
                        return Err(invalid());
                    }
                }
                Some("coalesced" | "missed_deadline" | "cancelled") => {}
                Some(_) | None => return Err(invalid()),
            }
        }
    }

    let mut admitted = BTreeSet::new();
    for (id, control) in controls {
        if control.expected().is_some() || !(id.starts_with(CLOCK) || id.starts_with(ACTION)) {
            continue;
        }
        let data = body(control)?;
        if let Some(continuation) = data.pointer("/task/continuation").filter(|value| !value.is_null())
            && !continuation_policy_matches(continuation)
        {
            return Err(Anomaly::incorrect(
                "rtdb.hosted/task-delivery-policy",
                "A task must order its delivery name and failure policy with its command",
            ));
        }
        if id.starts_with(ACTION) {
            if !action_command_matches(&data) {
                return Err(Anomaly::incorrect("rtdb.hosted/task-command", "The action carrier differs from its authored command"));
            }
            continue;
        }
        if data.pointer("/task/continuation/kind").and_then(Json::as_str) == Some("cron") {
            let task = data.pointer("/task/key").and_then(Json::as_str).ok_or_else(invalid)?;
            if !admitted.insert(task.to_owned()) {
                return Err(invalid());
            }
        }
    }
    if expected != admitted {
        return Err(invalid());
    }
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    #[test]
    fn an_emitted_action_cannot_substitute_its_command() {
        let command = r#"["bijection.task.emit.v2",{"deadline":"contract"},null]"#;
        let hash = Sha256::digest(command.as_bytes()).to_vec();
        let mut row = serde_json::json!({
            "task": {
                "emitted_key": "original",
                "emitted_command": command,
                "command": hash,
                "cutoff": null,
            },
        });
        assert!(action_command_matches(&row));
        row["task"]["emitted_command"] = serde_json::json!(
            r#"["bijection.task.emit.v2",{"deadline":{"in":"1"}},null]"#
        );
        assert!(!action_command_matches(&row));
    }
}
