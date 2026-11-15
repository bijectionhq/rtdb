use rtdb_types::{Anomaly, EventHeader, EventOp, Iid, Interval, Result, TemporalBounds, Timestamp};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "PolygonParts", into = "PolygonParts")]
pub struct Polygon {
    valid_times: Vec<Timestamp>,
    system_time_ceilings: Vec<Timestamp>,
}

#[derive(Serialize, Deserialize)]
struct PolygonParts { valid_times: Vec<Timestamp>, system_time_ceilings: Vec<Timestamp> }

impl Polygon {
    pub fn new(valid_times: Vec<Timestamp>, system_time_ceilings: Vec<Timestamp>) -> Result<Self> {
        if valid_times.len() != system_time_ceilings.len() + 1 && !(valid_times.is_empty() && system_time_ceilings.is_empty()) {
            return Err(Anomaly::incorrect("xtdb/invalid-polygon", "A polygon needs one more valid-time bound than system-time ceilings"));
        }
        Ok(Self { valid_times, system_time_ceilings })
    }
    pub fn valid_times(&self) -> &[Timestamp] { &self.valid_times }
    pub fn system_time_ceilings(&self) -> &[Timestamp] { &self.system_time_ceilings }
    pub fn range_count(&self) -> usize { self.system_time_ceilings.len() }

    pub fn recency(&self) -> Timestamp {
        let mut recency = Timestamp::MIN;
        for idx in (0..self.range_count()).rev() {
            recency = recency.max(self.system_time_ceilings[idx].min(self.valid_times[idx + 1]));
            if recency >= self.valid_times[idx] { return recency; }
        }
        recency
    }
}

impl TryFrom<PolygonParts> for Polygon {
    type Error = Anomaly;
    fn try_from(parts: PolygonParts) -> Result<Self> { Self::new(parts.valid_times, parts.system_time_ceilings) }
}
impl From<Polygon> for PolygonParts {
    fn from(polygon: Polygon) -> Self { Self { valid_times: polygon.valid_times, system_time_ceilings: polygon.system_time_ceilings } }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ceiling {
    valid_times: Vec<Timestamp>,
    system_time_ceilings: Vec<Timestamp>,
}

impl Default for Ceiling {
    fn default() -> Self {
        Self { valid_times: vec![Timestamp::MIN, Timestamp::MAX], system_time_ceilings: vec![Timestamp::MAX] }
    }
}

impl Ceiling {
    pub fn reset(&mut self) { *self = Self::default(); }
    pub fn valid_times(&self) -> &[Timestamp] { &self.valid_times }
    pub fn system_time_ceilings(&self) -> &[Timestamp] { &self.system_time_ceilings }

    fn index_at(&self, at: Timestamp) -> usize {
        self.valid_times.partition_point(|bound| *bound <= at).saturating_sub(1)
            .min(self.system_time_ceilings.len() - 1)
    }

    pub fn polygon(&self, interval: Interval) -> Polygon {
        let mut valid_times = vec![interval.lower()];
        let mut system_time_ceilings = Vec::new();
        let mut idx = self.index_at(interval.lower());
        if interval.is_empty() {
            return Polygon { valid_times: vec![interval.lower(), interval.upper()], system_time_ceilings: vec![self.system_time_ceilings[idx]] };
        }
        loop {
            system_time_ceilings.push(self.system_time_ceilings[idx]);
            let next = self.valid_times[idx + 1].min(interval.upper());
            valid_times.push(next);
            if next == interval.upper() { break; }
            idx += 1;
        }
        Polygon { valid_times, system_time_ceilings }
    }

    fn split_at(&mut self, at: Timestamp) -> usize {
        match self.valid_times.binary_search(&at) {
            Ok(idx) => idx,
            Err(idx) => {
                self.valid_times.insert(idx, at);
                self.system_time_ceilings.insert(idx, self.system_time_ceilings[idx - 1]);
                idx
            }
        }
    }

    pub fn apply_event(&mut self, system_from: Timestamp, interval: Interval) {
        if interval.is_empty() { return; }
        let start = self.split_at(interval.lower());
        let end = self.split_at(interval.upper());
        self.system_time_ceilings[start] = system_from;
        self.valid_times.drain(start + 1..end);
        self.system_time_ceilings.drain(start + 1..end);
    }
}

#[derive(Clone, Debug)]
struct EntityState {
    iid: Iid,
    last_system_from: Timestamp,
    erased: bool,
    ceiling: Ceiling,
}

#[derive(Clone, Debug)]
pub struct PolygonCalculator {
    bounds: Option<TemporalBounds>,
    entity: Option<EntityState>,
}

impl PolygonCalculator {
    pub fn new(bounds: Option<TemporalBounds>) -> Self { Self { bounds, entity: None } }
    pub fn reset(&mut self) { self.entity = None; }

    /// Consumes IID-ascending, system-time-descending events from one applied snapshot.
    /// Erases are considered even beyond the query's system-time upper bound.
    /// Equal-time events retain the caller's transaction operation ordering.
    pub fn calculate(&mut self, header: &EventHeader) -> Result<Option<Polygon>> {
        if let Some(entity) = &self.entity {
            if header.iid < entity.iid || (header.iid == entity.iid && header.system_from > entity.last_system_from) {
                return Err(Anomaly::incorrect("xtdb/unsorted-events", "Temporal resolution requires IID ascending and system-time descending")
                    .with_context("iid", header.iid.to_string().into()));
            }
        }
        if self.entity.as_ref().map(|entity| entity.iid) != Some(header.iid) {
            self.entity = Some(EntityState { iid: header.iid, last_system_from: header.system_from,
                erased: false, ceiling: Ceiling::default() });
        }
        let Some(entity) = self.entity.as_mut() else {
            return Err(Anomaly::fault("xtdb/missing-entity-state", "Temporal entity initialization failed"));
        };
        entity.last_system_from = header.system_from;
        if entity.erased { return Ok(None); }
        if header.op != EventOp::Erase && self.bounds.is_some_and(|bounds| header.system_from >= bounds.system_time.upper()) {
            return Ok(None);
        }
        let polygon = entity.ceiling.polygon(header.valid_time);
        entity.ceiling.apply_event(header.system_from, header.valid_time);
        entity.erased = header.op == EventOp::Erase;
        Ok(Some(polygon))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedRow<R> {
    pub source: R,
    pub valid_time: Interval,
    pub system_time: Interval,
}

pub struct HistoryResolver {
    calculator: PolygonCalculator,
    bounds: TemporalBounds,
    clamp_valid_time: bool,
}

impl HistoryResolver {
    pub fn new(bounds: TemporalBounds, clamp_valid_time: bool) -> Self {
        Self { calculator: PolygonCalculator::new(Some(bounds)), bounds, clamp_valid_time }
    }

    pub fn resolve<R: Clone>(&mut self, header: &EventHeader, source: R) -> Result<Vec<ResolvedRow<R>>> {
        let Some(polygon) = self.calculator.calculate(header)? else { return Ok(Vec::new()); };
        if header.op != EventOp::Put { return Ok(Vec::new()); }
        let mut result = Vec::new();
        for idx in 0..polygon.range_count() {
            let valid_time = Interval::new(polygon.valid_times[idx], polygon.valid_times[idx + 1])?;
            let system_to = polygon.system_time_ceilings[idx];
            if header.system_from >= system_to { continue; }
            let system_time = Interval::new(header.system_from, system_to)?;
            if !self.bounds.valid_time.intersects(valid_time) || !self.bounds.system_time.intersects(system_time) { continue; }
            let valid_time = if self.clamp_valid_time {
                match valid_time.intersection(self.bounds.valid_time) { Some(interval) => interval, None => continue }
            } else { valid_time };
            result.push(ResolvedRow { source: source.clone(), valid_time, system_time });
        }
        Ok(result)
    }
}

/// Runs the general resolver without applying value predicates to input events.
pub fn resolve_ordered<'a, R: Clone>(
    events: impl IntoIterator<Item = (&'a EventHeader, R)>,
    bounds: TemporalBounds,
    clamp_valid_time: bool,
) -> Result<Vec<ResolvedRow<R>>> {
    let mut resolver = HistoryResolver::new(bounds, clamp_valid_time);
    let mut result = Vec::new();
    for (header, source) in events { result.extend(resolver.resolve(header, source)?); }
    Ok(result)
}
