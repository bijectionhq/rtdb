use std::cmp::Ordering;

use rtdb_types::{Iid, TemporalBounds, Timestamp, Value};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TemporalSummary {
    pub min_valid_from: Timestamp,
    pub max_valid_to: Timestamp,
    pub min_system_from: Timestamp,
    pub max_system_from: Timestamp,
}

impl TemporalSummary {
    fn intersects(self, bounds: TemporalBounds) -> bool {
        self.min_valid_from < bounds.valid_time.upper() && bounds.valid_time.lower() < self.max_valid_to
            && self.intersects_system_time(bounds)
    }
    fn intersects_system_time(self, bounds: TemporalBounds) -> bool { self.min_system_from < bounds.system_time.upper() }
    fn may_hold_erase(self) -> bool { self.min_valid_from == Timestamp::MIN && self.max_valid_to == Timestamp::MAX }
    fn can_affect(self, emit: Self) -> bool {
        emit.min_system_from <= self.max_system_from
            && self.min_valid_from < emit.max_valid_to && emit.min_valid_from < self.max_valid_to
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PageSummary {
    pub page_index: u32,
    pub temporal: Option<TemporalSummary>,
    pub recency: Option<Timestamp>,
    /// Inclusive extrema; absence requires retaining the page for an IID lookup.
    pub iid_range: Option<(Iid, Iid)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PruningOptions {
    pub bounds: TemporalBounds,
    pub projects_temporal_columns: bool,
    pub clamp_valid_time: bool,
    pub bypass_temporal_pruning: bool,
    pub iid: Option<Iid>,
}

/// Returns candidate pages, preserving superseders, bound constraints, and erases.
/// `content_matches` is conservative: false means no put in the page can match.
/// Missing temporal statistics retain all IID-compatible pages.
pub fn select_pages<'a>(pages: &'a [PageSummary], options: PruningOptions, mut content_matches: impl FnMut(&PageSummary) -> bool) -> Vec<&'a PageSummary> {
    let pages: Vec<_> = pages.iter().filter(|page| match (options.iid, page.iid_range) {
        (Some(iid), Some((lower, upper))) => lower <= iid && iid <= upper,
        _ => true,
    }).collect();
    if options.bypass_temporal_pruning || pages.iter().any(|page| page.temporal.is_none() || page.recency.is_none()) {
        return pages;
    }
    let minimum_recency = options.bounds.valid_time.lower().min(options.bounds.system_time.lower());
    let intersects = |page: &PageSummary, temporal: TemporalSummary| {
        temporal.intersects(options.bounds) && page.recency.is_none_or(|recency| minimum_recency < recency)
    };
    let mut emit = Vec::new();
    let mut candidates = Vec::new();
    for page in pages {
        let Some(temporal) = page.temporal else { continue; };
        if intersects(page, temporal) && content_matches(page) {
            emit.push(page);
        } else if temporal.intersects_system_time(options.bounds) || temporal.may_hold_erase() {
            candidates.push(page);
        }
    }
    if emit.is_empty() { return emit; }
    let min_valid_from = emit.iter().filter_map(|page| page.temporal.map(|t| t.min_valid_from)).min().unwrap_or(Timestamp::MIN);
    let min_system_from = emit.iter().filter_map(|page| page.temporal.map(|t| t.min_system_from)).min().unwrap_or(Timestamp::MIN);
    let mut result = emit.clone();
    for page in candidates {
        let Some(temporal) = page.temporal else { continue; };
        if temporal.max_system_from < min_system_from { continue; }
        let retains = temporal.may_hold_erase() || if options.projects_temporal_columns && !options.clamp_valid_time {
            page.recency.is_none_or(|recency| recency >= min_valid_from || recency >= min_system_from)
                && emit.iter().any(|candidate| candidate.temporal.is_none_or(|t| temporal.can_affect(t)))
        } else { intersects(page, temporal) };
        if retains { result.push(page); }
    }
    result
}

#[derive(Clone, Debug, PartialEq)]
pub struct ScalarBounds { pub minimum: Value, pub maximum: Value }

#[derive(Clone, Debug, PartialEq)]
pub struct ColumnSummary {
    pub bounds: Option<ScalarBounds>,
    pub null_count: Option<u64>,
    pub value_count: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Comparison { Equal, NotEqual, Less, LessEqual, Greater, GreaterEqual }

impl ColumnSummary {
    /// False proves that no value matches; incomparable types, NaNs and unsigned physical bounds return true.
    pub fn may_match(&self, comparison: Comparison, value: &Value) -> bool {
        if self.value_count == Some(0) || matches!(value, Value::Null | Value::Undefined) { return false; }
        let Some(bounds) = &self.bounds else { return true; };
        if [&bounds.minimum, &bounds.maximum].iter().any(|value| matches!(value, Value::U8(_) | Value::U16(_) | Value::U32(_) | Value::U64(_))) { return true; }
        let Some(minimum) = scalar_compare(&bounds.minimum, value) else { return true; };
        let Some(maximum) = scalar_compare(&bounds.maximum, value) else { return true; };
        match comparison {
            Comparison::Equal => minimum != Ordering::Greater && maximum != Ordering::Less,
            Comparison::NotEqual => minimum != Ordering::Equal || maximum != Ordering::Equal,
            Comparison::Less => minimum == Ordering::Less,
            Comparison::LessEqual => minimum != Ordering::Greater,
            Comparison::Greater => maximum == Ordering::Greater,
            Comparison::GreaterEqual => maximum != Ordering::Less,
        }
    }

    pub fn may_be_null(&self) -> bool { self.null_count != Some(0) }
}

fn scalar_compare(left: &Value, right: &Value) -> Option<Ordering> {
    match (left, right) {
        (Value::Bool(a), Value::Bool(b)) => Some(a.cmp(b)),
        (Value::I8(a), Value::I8(b)) => Some(a.cmp(b)),
        (Value::I16(a), Value::I16(b)) => Some(a.cmp(b)),
        (Value::I32(a), Value::I32(b)) => Some(a.cmp(b)),
        (Value::I64(a), Value::I64(b)) => Some(a.cmp(b)),
        (Value::U8(a), Value::U8(b)) => Some(a.cmp(b)),
        (Value::U16(a), Value::U16(b)) => Some(a.cmp(b)),
        (Value::U32(a), Value::U32(b)) => Some(a.cmp(b)),
        (Value::U64(a), Value::U64(b)) => Some(a.cmp(b)),
        (Value::F32(a), Value::F32(b)) => a.partial_cmp(b),
        (Value::F64(a), Value::F64(b)) => a.partial_cmp(b),
        (Value::Utf8(a), Value::Utf8(b)) => Some(a.as_bytes().cmp(b.as_bytes())),
        (Value::Binary(a), Value::Binary(b)) | (Value::FixedSizeBinary(a), Value::FixedSizeBinary(b)) => Some(a.cmp(b)),
        (Value::Uuid(a), Value::Uuid(b)) => Some(a.cmp(b)),
        (Value::DateDay(a), Value::DateDay(b)) => Some(a.cmp(b)),
        (Value::DateMilli(a), Value::DateMilli(b)) => Some(a.cmp(b)),
        (Value::Time { value: a, unit: au, bit_width: aw }, Value::Time { value: b, unit: bu, bit_width: bw }) if au == bu && aw == bw => Some(a.cmp(b)),
        (Value::Timestamp { value: a, unit: au, timezone: az }, Value::Timestamp { value: b, unit: bu, timezone: bz }) if au == bu && az == bz => Some(a.cmp(b)),
        (Value::Duration { value: a, unit: au }, Value::Duration { value: b, unit: bu }) if au == bu => Some(a.cmp(b)),
        _ => None,
    }
}
