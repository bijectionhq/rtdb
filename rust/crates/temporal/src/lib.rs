use std::sync::Arc;

use rtdb_types::{Anomaly, EventHeader, EventOp, Iid, Interval, MemoryBudget, MemoryReservation, Result, TemporalBounds, Timestamp};
use serde::{Deserialize, Serialize};

mod event_step;
mod ceiling_step;

pub fn event_applies_at_cut(header: &EventHeader, through: rtdb_types::TxId) -> Result<bool> {
    event_step::position_visible(header.position.map(|position| position.transaction.0), through.0, header.op == EventOp::Erase)
        .ok_or_else(|| Anomaly::fault("rtdb.temporal/event-position",
            "An exact publication read requires original event positions"))
}

#[allow(unexpected_cfgs)]
#[cfg(kani)]
mod proofs;

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
        range_recency(self.valid_times.windows(2).zip(&self.system_time_ceilings)
            .map(|(valid, &system_to)| (valid[0], valid[1], system_to)))
    }
}

impl TryFrom<PolygonParts> for Polygon {
    type Error = Anomaly;
    fn try_from(parts: PolygonParts) -> Result<Self> { Self::new(parts.valid_times, parts.system_time_ceilings) }
}
impl From<Polygon> for PolygonParts {
    fn from(polygon: Polygon) -> Self { Self { valid_times: polygon.valid_times, system_time_ceilings: polygon.system_time_ceilings } }
}

fn range_recency(ranges: impl DoubleEndedIterator<Item = (Timestamp, Timestamp, Timestamp)>) -> Timestamp {
    let mut recency = Timestamp::MIN;
    for (valid_from, valid_to, system_to) in ranges.rev() {
        let (bound, finished) = event_step::recency_step(recency.0, valid_from.0, valid_to.0, system_to.0);
        recency = Timestamp(bound);
        if finished { return recency; }
    }
    recency
}

/// The clipped ranges from the ceiling immediately before an event.
pub struct PolygonView<'a> {
    valid_times: &'a [Timestamp],
    system_time_ceilings: &'a [Timestamp],
    interval: Interval,
}

impl<'a> PolygonView<'a> {
    fn new(valid_times: &'a [Timestamp], system_time_ceilings: &'a [Timestamp], interval: Interval) -> Self {
        let (first, last) = ceiling_step::cell_window(valid_times, system_time_ceilings, interval.lower(), interval.upper());
        Self { valid_times: &valid_times[first..last + 2], system_time_ceilings: &system_time_ceilings[first..=last], interval }
    }

    /// Yields each clipped valid-time lower bound, upper bound, and system-time ceiling in ascending valid-time order.
    /// An empty valid-time interval produces one zero-width range.
    pub fn ranges(&self) -> impl DoubleEndedIterator<Item = (Timestamp, Timestamp, Timestamp)> + '_ {
        self.valid_times.windows(2).zip(self.system_time_ceilings)
            .map(move |(valid, &system_to)| {
                let (lower, upper) = ceiling_step::clip_cell((valid[0], valid[1]), (self.interval.lower(), self.interval.upper()));
                (lower, upper, system_to)
            })
    }

    pub fn recency(&self) -> Timestamp { range_recency(self.ranges()) }
}

const INITIAL_VALID_TIMES: &[Timestamp] = &[Timestamp::MIN, Timestamp::MAX];
const INITIAL_SYSTEM_TIME_CEILINGS: &[Timestamp] = &[Timestamp::MAX];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ceiling {
    valid_times: Vec<Timestamp>,
    system_time_ceilings: Vec<Timestamp>,
}

impl Default for Ceiling {
    fn default() -> Self {
        Self { valid_times: INITIAL_VALID_TIMES.to_vec(), system_time_ceilings: INITIAL_SYSTEM_TIME_CEILINGS.to_vec() }
    }
}

impl Ceiling {
    pub fn reset(&mut self) { *self = Self::default(); }
    pub fn valid_times(&self) -> &[Timestamp] { &self.valid_times }
    pub fn system_time_ceilings(&self) -> &[Timestamp] { &self.system_time_ceilings }

    pub fn polygon(&self, interval: Interval) -> Polygon {
        let view = PolygonView::new(&self.valid_times, &self.system_time_ceilings, interval);
        let mut valid_times = Vec::with_capacity(view.valid_times.len());
        let mut system_time_ceilings = Vec::with_capacity(view.system_time_ceilings.len());
        valid_times.push(interval.lower());
        for (_, valid_to, system_to) in view.ranges() {
            valid_times.push(valid_to);
            system_time_ceilings.push(system_to);
        }
        Polygon { valid_times, system_time_ceilings }
    }

    pub fn apply_event(&mut self, system_from: Timestamp, interval: Interval) {
        ceiling_step::overwrite(&mut self.valid_times, &mut self.system_time_ceilings, interval.lower(), interval.upper(), system_from);
    }
}

#[derive(Clone, Debug)]
struct EntityState<C> {
    iid: Iid,
    last_system_from: Timestamp,
    erased: bool,
    ceiling: Option<C>,
}

struct EventStep {
    same_entity: bool,
    applies: bool,
}

#[derive(Clone, Debug)]
struct CalculatorState<C> {
    bounds: Option<TemporalBounds>,
    entity: Option<EntityState<C>>,
}

impl<C> CalculatorState<C> {
    fn new(bounds: Option<TemporalBounds>) -> Self { Self { bounds, entity: None } }

    fn classify(&self, header: &EventHeader) -> Result<EventStep> {
        let mut same_entity = false;
        let mut erased = false;
        if let Some(entity) = &self.entity {
            same_entity = header.iid == entity.iid;
            if header.iid < entity.iid || (same_entity && header.system_from > entity.last_system_from) {
                return Err(Anomaly::incorrect("xtdb/unsorted-events", "Temporal resolution requires IID ascending and system-time descending")
                    .with_context("iid", header.iid.to_string().into()));
            }
            erased = same_entity && entity.erased;
        }
        let applies = event_step::applies(
            erased, header.op == EventOp::Erase, header.system_from.0,
            self.bounds.map(|bounds| bounds.system_time.upper().0),
        );
        Ok(EventStep { same_entity, applies })
    }

    fn advance(&mut self, header: &EventHeader, step: EventStep) -> &mut EntityState<C> {
        if !step.same_entity { self.entity = None; }
        let entity = self.entity.get_or_insert_with(|| EntityState {
            iid: header.iid, last_system_from: header.system_from, erased: false, ceiling: None,
        });
        entity.last_system_from = header.system_from;
        entity.erased |= step.applies && header.op == EventOp::Erase;
        entity
    }
}

#[derive(Clone, Debug)]
pub struct PolygonCalculator { state: CalculatorState<Ceiling> }

impl PolygonCalculator {
    pub fn new(bounds: Option<TemporalBounds>) -> Self { Self { state: CalculatorState::new(bounds) } }
    pub fn reset(&mut self) { self.state.entity = None; }

    /// Consumes IID-ascending, system-time-descending events from one applied snapshot.
    /// Erases are considered even beyond the query's system-time upper bound.
    /// Equal-time events retain the caller's transaction operation ordering.
    pub fn calculate(&mut self, header: &EventHeader) -> Result<Option<Polygon>> {
        let step = self.state.classify(header)?;
        let applies = step.applies;
        let entity = self.state.advance(header, step);
        if !applies { return Ok(None); }
        let ceiling = entity.ceiling.get_or_insert_with(Ceiling::default);
        let polygon = ceiling.polygon(header.valid_time);
        ceiling.apply_event(header.system_from, header.valid_time);
        Ok(Some(polygon))
    }
}

struct AdmittedCeiling {
    ceiling: Ceiling,
    capacity: usize,
    _reservation: Arc<MemoryReservation>,
}

impl AdmittedCeiling {
    /// The ceiling entries applying an event over `interval` needs.
    fn required(valid_times: &[Timestamp], system_time_ceilings: &[Timestamp], interval: Interval) -> Result<usize> {
        let inserts = usize::from(valid_times.binary_search(&interval.lower()).is_err())
            + usize::from(valid_times.binary_search(&interval.upper()).is_err());
        system_time_ceilings.len().checked_add(inserts).ok_or_else(ceiling_size)
    }

    fn replacement(
        valid_times: &[Timestamp],
        system_time_ceilings: &[Timestamp],
        capacity: usize,
        interval: Interval,
        memory: &Arc<MemoryBudget>,
    ) -> Result<Option<Self>> {
        if interval.is_empty() { return Ok(None); }
        let inserts = usize::from(valid_times.binary_search(&interval.lower()).is_err())
            + usize::from(valid_times.binary_search(&interval.upper()).is_err());
        let required = system_time_ceilings.len().checked_add(inserts).ok_or_else(ceiling_size)?;
        if required <= capacity { return Ok(None); }
        let capacity = capacity.checked_mul(2).ok_or_else(ceiling_size)?.max(required);
        let valid_capacity = capacity.checked_add(1).ok_or_else(ceiling_size)?;
        let bytes = capacity.checked_add(valid_capacity).and_then(|count| count.checked_mul(std::mem::size_of::<Timestamp>()))
            .and_then(|bytes| bytes.checked_add(256)).ok_or_else(ceiling_size)?;
        let reservation = memory.reserve(bytes)?;
        let mut valid = Vec::new();
        let mut system = Vec::new();
        valid.try_reserve_exact(valid_capacity).map_err(|error| Anomaly::incorrect("rtdb/allocation-failed", error.to_string()))?;
        system.try_reserve_exact(capacity).map_err(|error| Anomaly::incorrect("rtdb/allocation-failed", error.to_string()))?;
        valid.extend_from_slice(valid_times);
        system.extend_from_slice(system_time_ceilings);
        Ok(Some(Self { ceiling: Ceiling { valid_times: valid, system_time_ceilings: system }, capacity, _reservation: reservation }))
    }
}

fn ceiling_size() -> Anomaly {
    Anomaly::incorrect("rtdb.temporal/ceiling-size", "Temporal ceiling capacity exceeds addressable memory")
}

/// Resolves events in order and retains the current ceiling's storage on the supplied budget.
pub struct AdmittedPolygonCalculator {
    state: CalculatorState<AdmittedCeiling>,
    /// The current entity's first applied event while it has no ceiling of its own.
    pending: Option<(Timestamp, Interval)>,
    spare: Option<AdmittedCeiling>,
    memory: Arc<MemoryBudget>,
}

impl AdmittedPolygonCalculator {
    pub fn new(bounds: Option<TemporalBounds>, memory: Arc<MemoryBudget>) -> Self {
        Self { state: CalculatorState::new(bounds), pending: None, spare: None, memory }
    }

    /// Clears the event position and releases the retained ceiling.
    pub fn reset(&mut self) { self.state.entity = None; self.pending = None; self.spare = None; }

    /// Visits the pre-event ranges of each eligible event, including an event with an empty valid-time interval.
    /// Uses the event order defined by [`PolygonCalculator::calculate`].
    /// Out-of-order input returns `xtdb/unsorted-events` before advancing the ordering position.
    /// A put or delete at or above the system-time upper bound returns `None`, as does any event for the same IID after an erase.
    /// These skipped events still advance the stream's ordering position.
    /// Ceiling admission errors leave the event unconsumed, and visitor errors are returned after consuming it.
    pub fn visit<T>(&mut self, header: &EventHeader, visitor: impl for<'a> FnOnce(PolygonView<'a>) -> Result<T>) -> Result<Option<T>> {
        let step = self.state.classify(header)?;
        if !step.applies {
            if !step.same_entity { self.retire(); }
            self.state.advance(header, step);
            return Ok(None);
        }
        if !step.same_entity {
            // An entity's first event sees the initial ceiling; it is applied to a ceiling only if the entity has another.
            let result = visitor(PolygonView::new(INITIAL_VALID_TIMES, INITIAL_SYSTEM_TIME_CEILINGS, header.valid_time));
            self.retire();
            self.state.advance(header, step);
            self.pending = (!header.valid_time.is_empty()).then_some((header.system_from, header.valid_time));
            return result.map(Some);
        }
        if let Some((system_from, interval)) = self.pending && self.state.entity.as_ref().is_some_and(|entity| entity.ceiling.is_none()) {
            let mut ceiling = match self.spare.take() {
                Some(mut spare) if spare.capacity >= AdmittedCeiling::required(INITIAL_VALID_TIMES, INITIAL_SYSTEM_TIME_CEILINGS, interval)? => {
                    spare.ceiling.valid_times.clear(); spare.ceiling.valid_times.extend_from_slice(INITIAL_VALID_TIMES);
                    spare.ceiling.system_time_ceilings.clear(); spare.ceiling.system_time_ceilings.extend_from_slice(INITIAL_SYSTEM_TIME_CEILINGS);
                    spare
                }
                spare => {
                    self.spare = spare;
                    AdmittedCeiling::replacement(INITIAL_VALID_TIMES, INITIAL_SYSTEM_TIME_CEILINGS, 0, interval, &self.memory)?
                        .ok_or_else(|| Anomaly::fault("rtdb/temporal-ceiling", "A pending event has an empty interval"))?
                }
            };
            ceiling.ceiling.apply_event(system_from, interval);
            if let Some(entity) = self.state.entity.as_mut() { entity.ceiling = Some(ceiling); }
            self.pending = None;
        }
        let current = self.state.entity.as_ref().and_then(|entity| entity.ceiling.as_ref());
        let (valid, system, capacity) = current.map_or((INITIAL_VALID_TIMES, INITIAL_SYSTEM_TIME_CEILINGS, 0), |current| {
            (current.ceiling.valid_times(), current.ceiling.system_time_ceilings(), current.capacity)
        });
        let replacement = AdmittedCeiling::replacement(valid, system, capacity, header.valid_time, &self.memory)?;
        let result = visitor(PolygonView::new(valid, system, header.valid_time));
        let entity = self.state.advance(header, step);
        if let Some(replacement) = replacement { entity.ceiling = Some(replacement); }
        if let Some(admitted) = &mut entity.ceiling {
            admitted.ceiling.apply_event(header.system_from, header.valid_time);
        }
        result.map(Some)
    }

    /// Ends the current entity, keeping its ceiling's buffers for the next entity that needs one.
    fn retire(&mut self) {
        self.pending = None;
        if let Some(ceiling) = self.state.entity.as_mut().and_then(|entity| entity.ceiling.take()) { self.spare = Some(ceiling); }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedRow<R> {
    pub source: R,
    pub valid_time: Interval,
    pub system_time: Interval,
}

/// Resolved fragments of one event with their row storage reservation.
pub struct ResolvedRows<R> {
    rows: Vec<ResolvedRow<R>>,
    capacity: usize,
    _reservation: Option<Arc<MemoryReservation>>,
}

impl<R> ResolvedRows<R> {
    /// Borrows the rows while their capacity remains reserved.
    pub fn as_slice(&self) -> &[ResolvedRow<R>] { &self.rows }

    fn push(&mut self, memory: &Arc<MemoryBudget>, row: impl FnOnce() -> ResolvedRow<R>) -> Result<()> {
        if self.rows.len() == self.capacity {
            let overflow = || Anomaly::incorrect("rtdb.temporal/history-row-size", "Resolved history capacity exceeds addressable memory");
            let capacity = self.capacity.checked_mul(2).ok_or_else(overflow)?.max(1);
            let bytes = capacity.checked_mul(std::mem::size_of::<ResolvedRow<R>>())
                .and_then(|bytes| bytes.checked_add(256)).ok_or_else(overflow)?;
            let reservation = memory.reserve(bytes)?;
            self.rows.try_reserve_exact(capacity - self.rows.len())
                .map_err(|error| Anomaly::incorrect("rtdb/allocation-failed", error.to_string()))?;
            self.capacity = capacity;
            self._reservation = Some(reservation);
        }
        self.rows.push(row());
        Ok(())
    }
}

enum HistoryCalculator {
    Legacy(PolygonCalculator),
    Admitted(AdmittedPolygonCalculator),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResolvedImage {
    Value,
    Absent,
    Erased,
}

pub struct HistoryResolver {
    calculator: HistoryCalculator,
    bounds: TemporalBounds,
    clamp_valid_time: bool,
}

impl HistoryResolver {
    pub fn new(bounds: TemporalBounds, clamp_valid_time: bool) -> Self {
        Self { calculator: HistoryCalculator::Legacy(PolygonCalculator::new(Some(bounds))), bounds, clamp_valid_time }
    }

    /// Charges the retained ceiling to `memory`.
    /// [`Self::resolve_in`] accepts the budget for its separate output allocation.
    pub fn new_in(bounds: TemporalBounds, clamp_valid_time: bool, memory: Arc<MemoryBudget>) -> Self {
        Self { calculator: HistoryCalculator::Admitted(AdmittedPolygonCalculator::new(Some(bounds), memory)), bounds, clamp_valid_time }
    }

    pub fn resolve<R: Clone>(&mut self, header: &EventHeader, source: R) -> Result<Vec<ResolvedRow<R>>> {
        let mut result = Vec::new();
        self.resolve_fragments(header, |valid_time, system_time| {
            result.push(ResolvedRow { source: source.clone(), valid_time, system_time });
            Ok(())
        })?;
        Ok(result)
    }

    /// Reserves inline row storage on `memory` before cloning the source for each emitted fragment.
    /// The returned rows retain the reservation.
    /// Uses the event order defined by [`PolygonCalculator::calculate`] and returns `xtdb/unsorted-events` for out-of-order input.
    /// A ceiling admission error leaves the event unconsumed.
    /// The calculator has consumed the event if result admission fails.
    pub fn resolve_in<R: Clone>(&mut self, header: &EventHeader, source: R, memory: &Arc<MemoryBudget>) -> Result<ResolvedRows<R>> {
        let mut result = ResolvedRows { rows: Vec::new(), capacity: 0, _reservation: None };
        self.resolve_fragments(header, |valid_time, system_time| {
            result.push(memory, || ResolvedRow { source: source.clone(), valid_time, system_time })
        })?;
        Ok(result)
    }

    /// Value-only projection for callers that cannot carry erased segments.
    pub fn resolve_fragments(&mut self, header: &EventHeader, mut emit: impl FnMut(Interval, Interval) -> Result<()>) -> Result<()> {
        self.visit_winners(header, |image, fragment| match image {
            ResolvedImage::Value => emit(fragment.valid_time, fragment.system_time),
            ResolvedImage::Absent => Ok(()),
            ResolvedImage::Erased => Err(Anomaly::incorrect(
                "rtdb.temporal/erased-assertion", "A selected assertion was erased",
            )),
        })
    }

    /// Emits the selected event's clipped rectangles with their disclosure state.
    /// The caller retains the input event's identity for every emitted rectangle.
    pub fn visit_winners(&mut self, header: &EventHeader, mut emit: impl FnMut(ResolvedImage, Rectangle) -> Result<()>) -> Result<()> {
        let bounds = self.bounds;
        let clamp_valid_time = self.clamp_valid_time;
        let mut emit_polygon = |polygon: PolygonView<'_>| {
            let image = match header.op {
                EventOp::Put => ResolvedImage::Value,
                EventOp::Delete => ResolvedImage::Absent,
                EventOp::Redacted => ResolvedImage::Erased,
                EventOp::Erase => return Ok(()),
            };
            for (valid_from, valid_to, system_to) in polygon.ranges() {
                let valid_time = Interval::new(valid_from, valid_to)?;
                if header.system_from >= system_to { continue; }
                let system_time = Interval::new(header.system_from, system_to)?;
                if let Some(fragment) = bounds.admit_fragment(TemporalBounds { valid_time, system_time }, clamp_valid_time) {
                    emit(image, Rectangle { valid_time: fragment.valid_time, system_time: fragment.system_time })?;
                }
            }
            Ok(())
        };
        match &mut self.calculator {
            HistoryCalculator::Legacy(calculator) => {
                let Some(polygon) = calculator.calculate(header)? else { return Ok(()); };
                emit_polygon(PolygonView { valid_times: polygon.valid_times(), system_time_ceilings: polygon.system_time_ceilings(), interval: header.valid_time })
            }
            HistoryCalculator::Admitted(calculator) => calculator.visit(header, emit_polygon).map(|_| ()),
        }
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

/// A region of valid time × system time where an event is the visible version of its entity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rectangle { pub valid_time: Interval, pub system_time: Interval }

impl Rectangle {
    /// Whether a scan at `bounds` emits this rectangle, as `HistoryResolver` decides it.
    pub fn meets(&self, bounds: TemporalBounds) -> bool { bounds.valid_time.intersects(self.valid_time) && bounds.system_time.intersects(self.system_time) }
}

/// Every rectangle where each event of an IID-ascending, system-time-descending history is visible, as the scan resolves it with no bounds.
/// A scan at any bounds emits exactly the rectangles that meet them, so an index holding these answers visibility as the scan does.
pub fn visibility<'a, R: Clone>(events: impl IntoIterator<Item = (&'a EventHeader, R)>) -> Result<Vec<(R, Rectangle)>> {
    let all = TemporalBounds { valid_time: Interval::ALL, system_time: Interval::ALL };
    Ok(resolve_ordered(events, all, false)?.into_iter().map(|row| (row.source, Rectangle { valid_time: row.valid_time, system_time: row.system_time })).collect())
}
