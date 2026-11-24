use crate::{ObservationReceipt, Outcome};
use rtdb_format::envelope::ReplicaEnvelope;
use rtdb_temporal::PolygonCalculator;
use rtdb_types::{EventHeader, EventOp, Iid, Interval, TemporalBounds, Timestamp};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, fs::File, io::Read, path::Path};

mod table_column;

type Error = Box<dyn std::error::Error>;

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Fixture {
    TemporalCeilings {
        events: Vec<Event>,
        query_bounds: Option<TemporalBounds>,
        observations: Vec<PolygonObservation>,
    },
    ReplicaTermSequence { envelopes: Vec<Envelope> },
    TableColumnAssertions { cases: Vec<table_column::Input> },
    AsOfErase(crate::history::as_of_erase::Fixture),
}

#[derive(Deserialize)]
struct Event {
    iid: String,
    system_from: i64,
    valid_from: i64,
    valid_to: i64,
    operation: Operation,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Operation { Put, Delete, Erase }

#[derive(Deserialize)]
struct PolygonObservation { id: String, event_index: usize, field: PolygonField }

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum PolygonField { ValidTimes, SystemTimeCeilings }

#[derive(Deserialize)]
struct Envelope { name: String, bytes_hex: String }

#[derive(Serialize, Deserialize)]
pub struct ControlExecution {
    pub schema_version: u32,
    pub fixture_sha256: String,
    pub binary_sha256: String,
    pub artifact_revision: String,
    pub source_dirty: bool,
    pub observations: Vec<ObservationReceipt>,
}

pub fn digest_file(path: &Path) -> Result<String, Error> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0; 65536];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 { break; }
        digest.update(&buffer[..count]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn hex_bytes(value: &str) -> Result<Vec<u8>, Error> {
    if value.len() % 2 != 0 || !value.is_ascii() { return Err("invalid hexadecimal fixture bytes".into()); }
    value.as_bytes().chunks_exact(2).map(|pair| {
        Ok(u8::from_str_radix(std::str::from_utf8(pair)?, 16)?)
    }).collect()
}

fn returned(id: String, typed_value: serde_json::Value) -> ObservationReceipt {
    ObservationReceipt { id, executed: true, outcome: Outcome::Returned { typed_value } }
}

/// Executes the control named by the `kind` field of a JSON fixture against production operators, codecs or a local database session.
/// Expected values and control mutations are ignored.
/// Returns an error for an invalid fixture, a failed operation or missing, empty or duplicate observation IDs.
pub async fn observe(bytes: &[u8]) -> Result<Vec<ObservationReceipt>, Error> {
    let fixture: Fixture = serde_json::from_slice(bytes)?;
    let result = match fixture {
        Fixture::TemporalCeilings { events, query_bounds, observations } => {
            let mut calculator = PolygonCalculator::new(query_bounds);
            let mut polygons = Vec::with_capacity(events.len());
            for event in events {
                let iid: [u8; 16] = hex_bytes(&event.iid)?.try_into().map_err(|_| "IID fixture must have 16 bytes")?;
                let event = EventHeader { position: None,
                    commit_position: 0,
                    iid: Iid(iid), system_from: Timestamp(event.system_from),
                    valid_time: Interval::from_micros(event.valid_from, event.valid_to)?,
                    op: match event.operation { Operation::Put => EventOp::Put, Operation::Delete => EventOp::Delete, Operation::Erase => EventOp::Erase },
                };
                polygons.push(calculator.calculate(&event)?);
            }
            observations.into_iter().map(|observation| {
                let polygon = polygons.get(observation.event_index).ok_or("fixture observation indexes a missing event")?;
                let value = polygon.as_ref().map(|polygon| match observation.field {
                    PolygonField::ValidTimes => polygon.valid_times(),
                    PolygonField::SystemTimeCeilings => polygon.system_time_ceilings(),
                }.iter().map(|time| time.0).collect::<Vec<_>>());
                Ok(returned(observation.id, json!(value)))
            }).collect::<Result<Vec<_>, Error>>()?
        }
        Fixture::ReplicaTermSequence { envelopes } => {
            envelopes.into_iter().map(|envelope| {
                let message = ReplicaEnvelope::decode(&hex_bytes(&envelope.bytes_hex)?)?.ok_or("fixture envelope has no body")?;
                let round_trip = ReplicaEnvelope::decode(&message.encode())?.ok_or("round-trip envelope has no body")?;
                Ok(returned(format!("{}/decoded", envelope.name), json!({
                    "term": round_trip.term.0,
                    "term_sequence": round_trip.term_sequence.map(|sequence| sequence.0),
                })))
            }).collect::<Result<Vec<_>, Error>>()?
        }
        Fixture::TableColumnAssertions { cases } => table_column::observe(cases).await?,
        Fixture::AsOfErase(fixture) => crate::history::as_of_erase::observe(&fixture).await?,
    };
    let mut ids = BTreeSet::new();
    if result.is_empty() || result.iter().any(|value| value.id.is_empty() || !ids.insert(&value.id)) {
        return Err("fixture must produce distinct nonempty observations".into());
    }
    Ok(result)
}

pub async fn execute(fixture: &Path) -> Result<ControlExecution, Error> {
    let bytes = std::fs::read(fixture)?;
    let observations = observe(&bytes).await?;
    Ok(ControlExecution {
        schema_version: 1,
        fixture_sha256: format!("{:x}", Sha256::digest(&bytes)),
        binary_sha256: digest_file(&std::env::current_exe()?)?,
        artifact_revision: env!("RTDB_CONFORMANCE_REVISION").into(),
        source_dirty: env!("RTDB_CONFORMANCE_DIRTY") != "false",
        observations,
    })
}
