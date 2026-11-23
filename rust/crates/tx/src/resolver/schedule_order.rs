//! Check that an owned schedule publication orders coverage with its admissions.
//! Cadence and executable semantics belong to the schedule owner.

use std::collections::{BTreeMap, BTreeSet};

use rtdb_types::{Anomaly, EntityId, Result, Value};
use serde_json::Value as Json;

use crate::{HostedControlWrite, TxOp, source::Operation};

const ADMISSION: &str = "bj_fact_root_admission";
const CATALOG: &str = "schedule-catalog:";
const COVERAGE: &str = "schedule-coverage:";
const CLOCK: &str = "clock-task:";
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
        if !id.starts_with(CLOCK) || control.expected().is_some() {
            continue;
        }
        let data = body(control)?;
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
