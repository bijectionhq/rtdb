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

/// Returns matching pages followed by pages that can supersede, constrain or erase their rows.
/// Both groups keep their input order.
/// Calls `content_matches` in input order for IID-compatible pages whose temporal and recency bounds intersect.
/// A false result means no put in the page can match.
/// With temporal pruning bypassed or any IID-compatible page missing temporal bounds or recency, returns all IID-compatible pages in input order and skips the predicate.
pub fn select_pages<'a>(pages: &'a [PageSummary], options: PruningOptions, content_matches: impl FnMut(&PageSummary) -> bool) -> Vec<&'a PageSummary> {
    select_pages_from_iter(pages.iter(), options, content_matches)
}

/// Applies the selection rules of [`select_pages`] to summaries borrowed from the caller's storage.
#[doc(hidden)]
pub fn select_pages_from_iter<'a>(
    pages: impl Iterator<Item = &'a PageSummary>, options: PruningOptions, content_matches: impl FnMut(&PageSummary) -> bool,
) -> Vec<&'a PageSummary> {
    let mut eligible = Vec::new();
    let mut emitted = Vec::new();
    let mut dependent = Vec::new();
    select_pages_impl(pages, options, content_matches, &mut eligible, &mut emitted, &mut dependent);
    emitted
}

/// Applies the selection rules of [`select_pages`] using caller-owned worklists.
/// Clears all three worklists on entry and places the result in `emitted`.
/// Returning all IID-compatible pages can swap the allocations of `eligible` and `emitted`.
/// Capacity for one reference per page in each worklist avoids growth during selection.
#[doc(hidden)]
pub fn select_pages_with_scratch<'a>(
    pages: &'a [PageSummary], options: PruningOptions, content_matches: impl FnMut(&PageSummary) -> bool,
    eligible: &mut Vec<&'a PageSummary>, emitted: &mut Vec<&'a PageSummary>, dependent: &mut Vec<&'a PageSummary>,
) {
    select_pages_impl(pages.iter(), options, content_matches, eligible, emitted, dependent);
}

fn select_pages_impl<'a>(
    pages: impl Iterator<Item = &'a PageSummary>, options: PruningOptions, mut content_matches: impl FnMut(&PageSummary) -> bool,
    eligible: &mut Vec<&'a PageSummary>, emitted: &mut Vec<&'a PageSummary>, dependent: &mut Vec<&'a PageSummary>,
) {
    eligible.clear();
    emitted.clear();
    dependent.clear();
    eligible.extend(pages.filter(|page| match (options.iid, page.iid_range) {
        (Some(iid), Some((lower, upper))) => lower <= iid && iid <= upper,
        _ => true,
    }));
    if options.bypass_temporal_pruning || eligible.iter().any(|page| page.temporal.is_none() || page.recency.is_none()) {
        std::mem::swap(eligible, emitted);
        return;
    }
    let minimum_recency = options.bounds.valid_time.lower().min(options.bounds.system_time.lower());
    let intersects = |page: &PageSummary, temporal: TemporalSummary| {
        temporal.intersects(options.bounds) && page.recency.is_none_or(|recency| minimum_recency < recency)
    };
    for page in eligible.iter().copied() {
        let Some(temporal) = page.temporal else { continue; };
        if intersects(page, temporal) && content_matches(page) {
            emitted.push(page);
        } else if temporal.intersects_system_time(options.bounds) || temporal.may_hold_erase() {
            dependent.push(page);
        }
    }
    let initial_matches = emitted.len();
    if initial_matches == 0 { return; }
    let min_valid_from = emitted.iter().filter_map(|page| page.temporal.map(|t| t.min_valid_from)).min().unwrap_or(Timestamp::MIN);
    let min_system_from = emitted.iter().filter_map(|page| page.temporal.map(|t| t.min_system_from)).min().unwrap_or(Timestamp::MIN);
    for page in dependent.iter().copied() {
        let Some(temporal) = page.temporal else { continue; };
        if temporal.max_system_from < min_system_from { continue; }
        let retains = temporal.may_hold_erase() || if options.projects_temporal_columns && !options.clamp_valid_time {
            page.recency.is_none_or(|recency| recency >= min_valid_from || recency >= min_system_from)
                && emitted[..initial_matches].iter().any(|candidate| candidate.temporal.is_none_or(|t| temporal.can_affect(t)))
        } else { intersects(page, temporal) };
        if retains { emitted.push(page); }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_types::Interval;

    #[test]
    fn selection_checks_content_in_input_order_and_compares_only_initial_matches() {
        let page = |page_index, min_valid_from, max_valid_to| PageSummary {
            page_index,
            temporal: Some(TemporalSummary { min_valid_from, max_valid_to, min_system_from: Timestamp(5), max_system_from: Timestamp(5) }),
            recency: Some(Timestamp(100)),
            iid_range: None,
        };
        let pages = [
            page(0, Timestamp::MIN, Timestamp::MAX),
            page(1, Timestamp(10), Timestamp(20)),
            page(2, Timestamp(0), Timestamp(5)),
        ];
        let options = PruningOptions {
            bounds: TemporalBounds {
                valid_time: Interval::new(Timestamp(10), Timestamp(30)).unwrap(),
                system_time: Interval::new(Timestamp(0), Timestamp(10)).unwrap(),
            },
            projects_temporal_columns: true, clamp_valid_time: false, bypass_temporal_pruning: false, iid: None,
        };
        let mut visited = Vec::new();
        let selected = select_pages(&pages, options, |page| {
            visited.push(page.page_index);
            page.page_index == 1
        });
        assert_eq!(visited, [0, 1]);
        assert_eq!(selected, [&pages[1], &pages[0]]);

        let mut eligible = Vec::with_capacity(pages.len());
        let mut emitted = Vec::with_capacity(pages.len());
        let mut dependent = Vec::with_capacity(pages.len());
        select_pages_with_scratch(&pages, PruningOptions { bypass_temporal_pruning: true, ..options },
            |_| panic!("Bypassed pruning must skip content checks"), &mut eligible, &mut emitted, &mut dependent);
        assert_eq!(emitted, [&pages[0], &pages[1], &pages[2]]);
    }

    #[test]
    fn borrowed_selection_preserves_candidate_identity_and_order() {
        let page = |page_index| PageSummary {
            page_index,
            temporal: Some(TemporalSummary {
                min_valid_from: Timestamp(10), max_valid_to: Timestamp(20),
                min_system_from: Timestamp(5), max_system_from: Timestamp(5),
            }),
            recency: Some(Timestamp(100)),
            iid_range: None,
        };
        let pages = [
            page(8),
            page(3),
            PageSummary { temporal: Some(TemporalSummary {
                min_valid_from: Timestamp::MIN, max_valid_to: Timestamp::MAX,
                min_system_from: Timestamp(50), max_system_from: Timestamp(50),
            }), ..page(5) },
            PageSummary { temporal: None, recency: None, iid_range: Some((Iid::MIN, Iid::MIN)), ..page(2) },
            PageSummary { recency: None, ..page(6) },
        ];
        let options = PruningOptions {
            bounds: TemporalBounds {
                valid_time: Interval::new(Timestamp(10), Timestamp(30)).unwrap(),
                system_time: Interval::new(Timestamp(0), Timestamp(10)).unwrap(),
            },
            projects_temporal_columns: true, clamp_valid_time: false, bypass_temporal_pruning: false,
            iid: Some(Iid([9; 16])),
        };
        let mut visited = Vec::new();
        let selected = select_pages_from_iter([2, 0, 3, 1].into_iter().map(|index| &pages[index]), options, |page| {
            visited.push(page.page_index);
            std::ptr::eq(page, &pages[0])
        });
        assert_eq!(visited, [8, 3]);
        assert_eq!(selected.len(), 3);
        assert!(selected.into_iter().zip([0, 2, 1]).all(|(page, index)| std::ptr::eq(page, &pages[index])));

        let selected = select_pages_from_iter([2, 0, 3, 1, 4].into_iter().map(|index| &pages[index]), options,
            |_| panic!("Missing recency must skip content checks"));
        assert_eq!(selected, [&pages[2], &pages[0], &pages[1], &pages[4]]);
    }
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
