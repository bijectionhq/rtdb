#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(valid => ensures
    valid == (start == next && (!resuming || exact_previous_cut)),
))]
pub(super) fn starts_at(next: u64, start: u64, resuming: bool, exact_previous_cut: bool) -> bool {
    start == next && (!resuming || exact_previous_cut)
}

#[cfg_attr(verus_keep_ghost, verus_spec(valid => ensures
    valid == (from as int + 1 == offered as int && offered <= through
        && (offered < through || terminal_matches)),
))]
pub(super) fn next_in_hold(from: u64, offered: u64, through: u64, terminal_matches: bool) -> bool {
    from.checked_add(1) == Some(offered) && offered <= through
        && (offered < through || terminal_matches)
}

#[cfg_attr(verus_keep_ghost, verus_spec(valid => ensures
    valid == (opened as int + 1 == through as int && base <= through
        && match checkpoint {
            None => genesis && base == 0,
            Some(boundary) => !genesis && exact_root && boundary as int + 1 == base as int,
        }),
))]
pub(crate) fn recovery_coordinates_match(
    opened: i64, through: u64, base: u64, genesis: bool, checkpoint: Option<i64>, exact_root: bool,
) -> bool {
    opened as i128 + 1 == through as i128 && base <= through && match checkpoint {
        None => genesis && base == 0,
        Some(boundary) => !genesis && exact_root && boundary as i128 + 1 == base as i128,
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(selected => ensures
    selected == if exact_checkpoint && exact_through && exact_record_cut
        && observed_time == prepared_time && observed_bytes == sealed_bytes
    { Some(record) } else { None },
))]
pub(super) fn select_record<T>(
    record: T,
    exact_checkpoint: bool,
    exact_through: bool,
    exact_record_cut: bool,
    observed_time: i64,
    prepared_time: i64,
    observed_bytes: usize,
    sealed_bytes: usize,
) -> Option<T> {
    if exact_checkpoint && exact_through && exact_record_cut
        && observed_time == prepared_time && observed_bytes == sealed_bytes
    {
        Some(record)
    } else {
        None
    }
}

/// The logical replay position. Hold handles and decrypted records belong to
/// the driver, while only this state can advance the delivered cut.
pub(super) struct ReplayCursor<C> {
    next: u64,
    from: Option<C>,
}

pub(super) struct PreparedHead<C> {
    from: C,
    next: u64,
    end: C,
    through: C,
    timestamp: i64,
    sealed_bytes: usize,
    pub(super) record_bytes: u64,
}

pub(super) struct SelectedCut<C> {
    from: C,
    next: u64,
    end: C,
}

#[cfg(verus_keep_ghost)]
verus! {
impl<C> ReplayCursor<C> {
    pub(super) closed spec fn view_next(&self) -> u64 { self.next }
    pub(super) closed spec fn view_from(&self) -> Option<C> { self.from }
}

impl<C> PreparedHead<C> {
    pub(super) closed spec fn view_from(&self) -> C { self.from }
    pub(super) closed spec fn view_next(&self) -> u64 { self.next }
    pub(super) closed spec fn view_end(&self) -> C { self.end }
    pub(super) closed spec fn view_through(&self) -> C { self.through }
    pub(super) closed spec fn view_timestamp(&self) -> i64 { self.timestamp }
    pub(super) closed spec fn view_sealed_bytes(&self) -> usize { self.sealed_bytes }
    pub(super) closed spec fn view_record_bytes(&self) -> u64 { self.record_bytes }
}

impl<C> SelectedCut<C> {
    pub(super) closed spec fn view_from(&self) -> C { self.from }
    pub(super) closed spec fn view_next(&self) -> u64 { self.next }
    pub(super) closed spec fn view_end(&self) -> C { self.end }
}
}

impl<C: Copy> SelectedCut<C> {
    #[cfg_attr(verus_keep_ghost, verus_spec(from => ensures from == self.view_from()))]
    pub(super) fn from(&self) -> C {
        self.from
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(cursor => ensures
    cursor.view_next() == next && cursor.view_from() is None,
))]
pub(super) fn new_replay_cursor<C>(next: u64) -> ReplayCursor<C> {
    ReplayCursor { next, from: None }
}

impl<C: Copy> ReplayCursor<C> {
    #[cfg_attr(verus_keep_ghost, verus_spec(next => ensures next == self.view_next()))]
    pub(super) fn next(&self) -> u64 {
        self.next
    }

    #[cfg_attr(verus_keep_ghost, verus_spec(from => ensures from == self.view_from()))]
    pub(super) fn from(&self) -> Option<C> {
        self.from
    }

    #[cfg_attr(verus_keep_ghost, verus_spec(bound => ensures
        bound == (start_position == old(self).view_next() && (old(self).view_from() is None || same_history)
            && start_position <= terminal_position && terminal_position == requested_terminal),
        final(self).view_next() == old(self).view_next(),
        final(self).view_from() == if bound { Some(start) } else { old(self).view_from() },
    ))]
    pub(super) fn bind(
        &mut self, start: C, start_position: u64, terminal_position: u64,
        requested_terminal: u64, same_history: bool,
    ) -> bool {
        if !starts_at(self.next, start_position, self.from.is_some(), same_history)
            || terminal_position < start_position || terminal_position != requested_terminal
        {
            return false;
        }
        self.from = Some(start);
        true
    }

    #[cfg_attr(verus_keep_ghost, verus_spec(prepared => ensures
        match prepared {
            Some(head) => self.view_from() == Some(head.view_from()) && head.view_next() == offered_position
                && self.view_next() as int + 1 == offered_position as int
                && offered_position <= terminal_position
                && (offered_position == terminal_position ==> terminal_matches)
                && head.view_end() == end && head.view_through() == through && head.view_timestamp() == timestamp
                && head.view_sealed_bytes() == sealed_bytes && head.view_record_bytes() == record_bytes,
            None => self.view_from() is None || self.view_next() as int + 1 != offered_position as int
                || terminal_position < offered_position
                || (offered_position == terminal_position && !terminal_matches),
        },
    ))]
    pub(super) fn prepare(
        &self, end: C, offered_position: u64, through: C, terminal_position: u64,
        terminal_matches: bool, timestamp: i64, sealed_bytes: usize, record_bytes: u64,
    ) -> Option<PreparedHead<C>> {
        let from = self.from?;
        if !next_in_hold(self.next, offered_position, terminal_position, terminal_matches) {
            return None;
        }
        Some(PreparedHead { from, next: offered_position, end, through, timestamp, sealed_bytes, record_bytes })
    }

    #[cfg_attr(verus_keep_ghost, verus_spec(out => ensures
        out == match result {
            Ok(record) => if same_predecessor && old(self).view_from() is Some
                && old(self).view_next() as int + 1 == cut.view_next() as int { Ok(Some(record)) } else { Ok(None) },
            Err(error) => Err(error),
        },
        final(self).view_next() == if out matches Ok(Some(_)) { cut.view_next() } else { old(self).view_next() },
        final(self).view_from() == if out matches Ok(Some(_)) { Some(cut.view_end()) } else { old(self).view_from() },
    ))]
    pub(super) fn finish<T, E>(
        &mut self, cut: SelectedCut<C>, same_predecessor: bool, result: Result<T, E>,
    ) -> Result<Option<T>, E> {
        let record = result?;
        if !same_predecessor || self.from.is_none() || self.next.checked_add(1) != Some(cut.next) {
            return Ok(None);
        }
        self.next = cut.next;
        self.from = Some(cut.end);
        Ok(Some(record))
    }
}

impl<C: Copy> PreparedHead<C> {
    #[cfg_attr(verus_keep_ghost, verus_spec(from => ensures from == self.view_from()))]
    pub(super) fn from(&self) -> C {
        self.from
    }

    #[cfg_attr(verus_keep_ghost, verus_spec(end => ensures end == self.view_end()))]
    pub(super) fn end(&self) -> C {
        self.end
    }

    #[cfg_attr(verus_keep_ghost, verus_spec(through => ensures through == self.view_through()))]
    pub(super) fn through(&self) -> C {
        self.through
    }

    #[cfg_attr(verus_keep_ghost, verus_spec(selected => ensures
        match selected {
            Some((cut, record)) => cut.view_from() == self.view_from() && cut.view_next() == self.view_next() && cut.view_end() == self.view_end()
                && record == original && exact_checkpoint && exact_through && exact_record_cut
                && observed_time == self.view_timestamp() && observed_bytes == self.view_sealed_bytes(),
            None => !exact_checkpoint || !exact_through || !exact_record_cut
                || observed_time != self.view_timestamp() || observed_bytes != self.view_sealed_bytes(),
        },
    ))]
    pub(super) fn select<T>(
        &self, original: T, exact_checkpoint: bool, exact_through: bool, exact_record_cut: bool,
        observed_time: i64, observed_bytes: usize,
    ) -> Option<(SelectedCut<C>, T)> {
        let record = select_record(
            original, exact_checkpoint, exact_through, exact_record_cut,
            observed_time, self.timestamp, observed_bytes, self.sealed_bytes,
        )?;
        Some((SelectedCut { from: self.from, next: self.next, end: self.end }, record))
    }
}
