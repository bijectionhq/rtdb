use serde::Deserialize;

pub(super) const C84_REFERENCE: &str = "c84f9334eb060ceb92d28ecdd4a06e6b625932fd";
pub(super) const UPSTREAM_COMMIT: &str = include_str!("../../../../conformance/upstream-commit.txt");
const MAX_EVENTS: usize = 65_536;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Tape {
    pub schema_version: u32,
    pub reference_revision: String,
    pub unique_id: String,
    pub seed: i32,
    pub reference_capture_sha256: String,
    pub tape_sha256: String,
    pub rows_per_block: u64,
    pub boot: Vec<Event<Boot>>,
    pub inputs: Vec<Event<Input>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Event<T> {
    pub sequence: usize,
    pub event: T,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Boot {
    TailOpen { actor: usize, tail: usize, start: i64 },
    Claim { actor: usize, term: i64, term_sequence: i64, time: [i64; 2] },
    Refusal { actor: usize, term: i64, term_sequence: i64 },
    Durable { offset: i64 },
    Receipt { offset: i64 },
    Delivery { actor: usize, tail: usize, from: i64, to: i64 },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Input {
    Flush { offset: i64, time: [i64; 2] },
    ExternalStart { action: u32, token: String, tx_id: i64, time: [i64; 2], documents: Option<Vec<uuid::Uuid>> },
    ExternalApplied { action: u32, tx_id: i64, time: [i64; 2], committed: bool },
}

pub(super) fn micros([seconds, nanos]: [i64; 2]) -> Option<i64> {
    if !(0..1_000_000_000).contains(&nanos) { return None; }
    i64::try_from(i128::from(seconds) * 1_000_000 + i128::from(nanos / 1_000)).ok()
}

fn hash(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(super) fn token(action: u32) -> String { format!("{action:08x}") }

impl Tape {
    pub fn observes_live_indexes(&self) -> bool { self.schema_version == 2 }

    pub fn validate(&self) -> Result<(), String> {
        if !matches!(self.schema_version, 1 | 2) || ![C84_REFERENCE, UPSTREAM_COMMIT.trim()].contains(&self.reference_revision.as_str()) || self.unique_id.is_empty()
            || self.unique_id.len() > 4096 || !hash(&self.reference_capture_sha256) || !hash(&self.tape_sha256)
            || !(15..=24).contains(&self.rows_per_block) || self.boot.len() != 11 || self.inputs.len() > 157
        {
            return Err("invalid refused-claim replay identity or bounds".into());
        }
        let mut all: Vec<_> = self.boot.iter().map(|frame| frame.sequence).chain(self.inputs.iter().map(|frame| frame.sequence)).collect();
        all.sort_unstable();
        if all.iter().any(|&sequence| sequence >= MAX_EVENTS) || all.windows(2).any(|pair| pair[0] == pair[1])
            || self.boot.windows(2).any(|pair| pair[0].sequence >= pair[1].sequence)
            || self.inputs.windows(2).any(|pair| pair[0].sequence >= pair[1].sequence)
        {
            return Err("replay projections contain duplicate or unordered event identities".into());
        }

        let mut tails = [None; 3];
        let mut claims = [false; 3];
        let mut delivered = [false; 3];
        let mut durable = false;
        let mut receipt = false;
        for frame in &self.boot {
            match frame.event {
                Boot::TailOpen { actor, tail, start: 0 } if actor < 3 && tails[actor].is_none()
                    && tail < MAX_EVENTS && !tails.contains(&Some(tail)) => tails[actor] = Some(tail),
                Boot::Claim { actor: 0, term: 1, term_sequence: 0, time } if tails[0].is_some() && !claims[0] && micros(time).is_some() => {
                    claims[0] = true;
                }
                Boot::Refusal { actor, term: 1, term_sequence: 0 } if (1..3).contains(&actor) && tails[actor].is_some() && !claims[actor] => {
                    claims[actor] = true;
                }
                Boot::Durable { offset: 0 } if claims[0] && !durable => durable = true,
                Boot::Receipt { offset: 0 } if durable && !receipt => receipt = true,
                Boot::Delivery { actor, tail, from: 0, to } if actor < 3 && tails[actor] == Some(tail) && claims[actor]
                    && durable && !delivered[actor] && (1..=MAX_EVENTS as i64).contains(&to) => delivered[actor] = true,
                _ => return Err(format!("invalid boot event at reference sequence {}", frame.sequence)),
            }
        }
        if !receipt || !delivered.into_iter().all(|value| value) {
            return Err("boot projection omitted a claim delivery or receipt".into());
        }
        let incumbent_delivery = self.boot.iter().find_map(|frame| match frame.event {
            Boot::Delivery { actor: 0, .. } => Some(frame.sequence), _ => None,
        }).expect("validated incumbent delivery");
        let mut source = 0;
        let mut next = 0u32;
        let mut pending = None;
        let mut last_time = None;
        for frame in &self.inputs {
            match &frame.event {
                Input::Flush { offset, time } if *offset == source && micros(*time).is_some() => source += 1,
                Input::ExternalStart { action, token: observed, tx_id, time, documents }
                    if *action == next && *tx_id == i64::from(next) && observed == &token(next) && pending.is_none()
                        && frame.sequence > incumbent_delivery
                        && documents.as_ref().is_none_or(|rows| (1..=5).contains(&rows.len())) =>
                {
                    let time_micros = micros(*time).filter(|time| last_time.is_none_or(|last| *time > last))
                        .ok_or_else(|| format!("external timestamp at reference sequence {} is not strictly increasing in microseconds", frame.sequence))?;
                    last_time = Some(time_micros);
                    pending = Some((*tx_id, *time, documents.is_some()));
                }
                Input::ExternalApplied { action, tx_id, time, committed }
                    if *action == next && pending == Some((*tx_id, *time, *committed)) =>
                {
                    next += 1;
                    pending = None;
                }
                _ => return Err(format!("invalid authored input at reference sequence {}", frame.sequence)),
            }
        }
        if pending.is_some() || !(30..=59).contains(&next) || source > 39 { return Err("invalid authored input counts".into()); }
        Ok(())
    }

    pub fn claim_time(&self) -> i64 {
        self.boot.iter().find_map(|frame| match frame.event {
            Boot::Claim { time, .. } => micros(time), _ => None,
        }).expect("validated boot claim time")
    }
}
