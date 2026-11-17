use std::sync::{Once, atomic::{AtomicU16, Ordering}};
use super::Mapping;
mod patches;

pub(in super::super) static FAMILY: Big5Family = Big5Family::new(&super::tables::TABLES[0]);

pub(in super::super::super) struct Big5Family {
    base: &'static Mapping,
    entries: [AtomicU16; 7],
    initialized: Once,
}

impl Big5Family {
    const fn new(base: &'static Mapping) -> Self {
        Self { base, entries: [const { AtomicU16::new(patches::BASE_UNIT) }; 7], initialized: Once::new() }
    }

    fn initialize_solaris(&self) {
        self.initialized.call_once(|| {
            // Big5_Solaris.Holder patches a shared row; ordinary Big5 readers do not wait for initialization.
            for (entry, unit) in self.entries.iter().zip(patches::SOLARIS_UNITS) {
                entry.store(unit, Ordering::Relaxed);
            }
        });
    }

    pub(in super::super) fn pair(&self, first: u8, second: u8) -> u16 {
        if first == patches::LEAD && (patches::FIRST..=patches::LAST).contains(&second) {
            self.entries[usize::from(second - patches::FIRST)].load(Ordering::Relaxed)
        } else { self.base.pair(first, second) }
    }

    pub(in super::super) fn decode_big5(&self, bytes: &[u8]) -> String {
        self.base.decode_with_pairs(bytes, |first, second| self.pair(first, second))
    }

    pub(in super::super) fn decode_solaris(&self, bytes: &[u8]) -> String {
        if !bytes.is_empty() { self.initialize_solaris(); }
        self.decode_big5(bytes)
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, sync::Barrier};
    use super::*;

    fn family() -> Big5Family { Big5Family::new(super::super::lookup("big5").unwrap()) }

    #[test]
    fn solaris_patch_preserves_big5_row_identity_and_all_other_entries() {
        assert!(std::ptr::eq(FAMILY.base, super::super::lookup("big5").unwrap()));
        let family = family();
        assert!(family.base.row(patches::LEAD).is_some());
        for second in patches::FIRST..=patches::LAST {
            assert_eq!(family.base.pair(patches::LEAD, second), patches::BASE_UNIT);
            assert!(family.base.row(second).is_some());
        }
        family.initialize_solaris();
        for first in 0u8..=255 {
            for second in 0u8..=255 {
                let expected = if first == patches::LEAD && (patches::FIRST..=patches::LAST).contains(&second) {
                    patches::SOLARIS_UNITS[usize::from(second - patches::FIRST)]
                } else { family.base.pair(first, second) };
                assert_eq!(family.pair(first, second), expected, "{first:02x}{second:02x}");
            }
        }
    }

    #[test]
    fn ordinary_big5_reads_partially_published_entries_without_waiting() {
        let family = family();
        family.entries[0].store(patches::SOLARIS_UNITS[0], Ordering::Relaxed);
        assert!(!family.initialized.is_completed());
        assert_eq!(family.decode_big5(&[0xf9, 0xd6, 0, 0xf9, 0xd7]), "碁\0\u{fffd}\u{fffd}");
        family.entries[1].store(patches::SOLARIS_UNITS[1], Ordering::Relaxed);
        assert_eq!(family.decode_big5(&[0xf9, 0xd6, 0, 0xf9, 0xd7]), "碁\0銹");
    }

    #[test]
    fn pair_lookup_observes_publication_between_pairs_in_one_record() {
        let family = family();
        let first_lookup = Cell::new(true);
        let output = family.base.decode_with_pairs(&[0xf9, 0xd6, 0, 0xf9, 0xd7], |first, second| {
            let unit = family.pair(first, second);
            if first_lookup.replace(false) { family.entries[1].store(patches::SOLARIS_UNITS[1], Ordering::Relaxed); }
            unit
        });
        assert_eq!(output, "\u{fffd}\u{fffd}\0銹");
    }

    #[test]
    fn empty_solaris_records_leave_initialization_pending_and_ascii_records_complete_it() {
        let family = family();
        assert_eq!(family.decode_solaris(&[]), "");
        assert!(!family.initialized.is_completed());
        assert_eq!(family.decode_big5(&[0xf9, 0xd6]), "\u{fffd}\u{fffd}");
        assert_eq!(family.decode_solaris(b"a"), "a");
        assert!(family.initialized.is_completed());
        assert_eq!(family.decode_big5(&[0xf9, 0xd6]), "碁");
    }

    #[test]
    fn concurrent_solaris_callers_observe_every_completed_patch() {
        let family = family();
        let start = Barrier::new(8);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    start.wait();
                    assert_eq!(family.decode_solaris(&[0xf9, 0xd6, 0xf9, 0xd7, 0xf9, 0xd8, 0xf9, 0xd9, 0xf9, 0xda, 0xf9, 0xdb, 0xf9, 0xdc]), "碁銹裏墻恒粧嫺");
                });
            }
        });
        assert!(family.initialized.is_completed());
        for (index, unit) in patches::SOLARIS_UNITS.into_iter().enumerate() {
            assert_eq!(family.pair(patches::LEAD, patches::FIRST + index as u8), unit);
        }
    }
}
