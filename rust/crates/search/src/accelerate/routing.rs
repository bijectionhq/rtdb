use std::collections::BTreeMap;
use std::sync::{Arc, Weak};

use rtdb_format::keys::{ArtefactOwner, TrieKey};
use rtdb_index::{Snapshot, TrieCatalog};
use rtdb_storage::ObjectKey;
use rtdb_types::{MemoryBudget, MemoryReservation, Result, TableRef, TemporalBounds};

use crate::{KIND, SearchDefinition, file::{self, PACKED_FORMAT_VERSION, Section, TREE_FORMAT_VERSION, TreeLocator}};

pub(crate) type RoutingKey = (usize, TableRef, String, i64, u8);

pub(crate) struct TableRouting {
    catalog: Weak<TrieCatalog>,
    packs: Vec<Arc<Pack>>,
    pub bytes: usize,
    _reservation: Option<Arc<MemoryReservation>>,
}

pub(crate) struct Pack {
    pub key: ObjectKey,
    pub owner: ArtefactOwner,
    pub definition_id: String,
    pub locator: TreeLocator,
    pub members: BTreeMap<u32, TrieKey>,
    sections: Option<Vec<Section>>,
}

impl Pack {
    pub fn visible_members(&self, bounds: TemporalBounds) -> BTreeMap<u32, TrieKey> {
        let earliest = bounds.valid_time.lower().0.min(bounds.system_time.lower().0);
        self.members.iter().filter(|(_, trie)| {
            trie.recency().is_none_or(|date| date.epoch_day().saturating_mul(86_400_000_000) > earliest)
        }).map(|(&member, trie)| (member, trie.clone())).collect()
    }

    pub fn can_nominate(&self, bounds: TemporalBounds, column: usize, query: &rtdb_text::Query) -> bool {
        self.sections.as_ref().is_none_or(|sections| sections.iter().any(|section| section.meets(bounds)))
            && query.terms().iter().all(|term| self.locator.might_contain(column as u32, term))
    }
}

impl TableRouting {
    pub fn key(snapshot: &Snapshot, definition: &SearchDefinition, layout: u8) -> RoutingKey {
        (Arc::as_ptr(snapshot.trie_catalog_ref()) as usize, definition.table.clone(), definition.name.clone(), definition.version, layout)
    }

    pub fn belongs_to(&self, snapshot: &Snapshot) -> bool {
        self.catalog.ptr_eq(&Arc::downgrade(snapshot.trie_catalog_ref()))
    }

    pub fn packs(&self) -> impl Iterator<Item = &Arc<Pack>> { self.packs.iter() }

    pub fn open(snapshot: &Snapshot, definition: &SearchDefinition, include_packs: bool, budget: Option<&Arc<MemoryBudget>>) -> Result<Self> {
        let ids: Vec<_> = [PACKED_FORMAT_VERSION, TREE_FORMAT_VERSION].into_iter()
            .filter(|format| include_packs || *format == TREE_FORMAT_VERSION).map(|format| (definition.artefact_id_for(format), format)).collect();
        Self::open_formats(snapshot, definition, &ids, budget)
    }

    pub fn history(snapshot: &Snapshot, definition: &SearchDefinition, budget: Option<&Arc<MemoryBudget>>) -> Result<Self> {
        Self::open_formats(snapshot, definition, &[(crate::lane::history_id(definition), file::LOGICAL_FORMAT_VERSION)], budget)
    }

    fn open_formats(snapshot: &Snapshot, definition: &SearchDefinition, ids: &[(String, i32)], budget: Option<&Arc<MemoryBudget>>) -> Result<Self> {
        let mut packs = BTreeMap::<ObjectKey, Pack>::new();
        let mut bytes = 1024usize;
        let mut reservation = budget.map(|budget| budget.reserve(bytes)).transpose()?;
        let table = &definition.table;
        if let Some(entry) = snapshot.table_entry(table) {
            for trie in snapshot.trie_catalog().iter_tries(table).filter(|trie| trie.state == rtdb_index::TrieState::Live) {
                let Some((record, expected)) = ids.iter().find_map(|(id, format)| trie.artefact(KIND, id, definition.version).map(|record| (record, *format))) else { continue; };
                let Some(stats) = record.stats.as_deref() else { continue; };
                if let Some(reservation) = &mut reservation {
                    let reservation = Arc::get_mut(reservation).expect("routing construction owns its admission");
                    let root = if expected >= PACKED_FORMAT_VERSION { file::MAX_ROOT_BYTES as usize } else { 0 };
                    let peak = bytes.saturating_add(stats.len().saturating_mul(2)).saturating_add(2048).saturating_add(root);
                    if peak > reservation.bytes() { reservation.grow_to(peak)?; }
                }
                let Some(stats_reader) = file::CatalogStatsReader::open(stats) else { continue; };
                let Some(locator) = stats_reader.tree_locator().filter(|locator| {
                    i64::try_from(locator.length).ok() == record.file_size
                        && locator.format() == expected
                }) else { continue; };
                let owner = ArtefactOwner::of(&trie.key, record)?;
                let Some(member) = owner.tries().iter().position(|key| key == &trie.key) else { continue; };
                let key = ObjectKey::new(entry.slug.recorded_artefact_key(&trie.key, record)?)?;
                let pack = packs.entry(key.clone()).or_insert_with(|| {
                    bytes = bytes.saturating_add(stats.len()).saturating_add(768).saturating_add(locator.inline_root_bytes());
                    Pack { key, owner, definition_id: record.definition_id().to_owned(), locator: locator.clone(),
                        members: BTreeMap::new(), sections: stats_reader.sections() }
                });
                if pack.locator == locator { pack.members.insert(member as u32, trie.key.clone()); }
                bytes = bytes.saturating_add(192);
            }
        }
        Ok(Self { catalog: Arc::downgrade(snapshot.trie_catalog_ref()), packs: packs.into_values().map(Arc::new).collect(), bytes, _reservation: reservation })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_format::log::proto::{ArtefactRecord, ArtefactScope};
    use rtdb_index::{CatalogTrie, PartitionValue, TableCatalog};
    use rtdb_types::{Interval, Timestamp};

    #[test]
    fn a_pack_keeps_the_first_members_optional_sections_and_admits_matching_locators() {
        let table = TableRef::new("public", "docs");
        let definition = SearchDefinition {
            table: table.clone(), name: "body".into(), columns: vec!["body".into()],
            analyzer: rtdb_text::Analyzer::Simple, version: 1, history: true,
        };
        let keys: [TrieKey; 4] = std::array::from_fn(|member| format!("l02-rc-p{member}-b00").parse().unwrap());
        let tries = keys.iter().map(|key| CatalogTrie::notification(key.clone(), 10, None).unwrap());
        let catalog = TrieCatalog::new(20).unwrap().with_tries(table.clone(), tries, Timestamp(1)).unwrap();
        let pack_members: Vec<_> = keys.iter().map(ToString::to_string).collect();
        let locator = format!("v15:128:16:8:0:{}", "00".repeat(32));
        let other_locator = format!("v15:128:32:8:0:{}", "00".repeat(32));
        let section = Section { class: 0, valid_from: 0, valid_to: 1, system_from: 0, system_to: 1, birth: 0, death: 1 };
        let record = |sections: &[Section], locator: &str, file_size| ArtefactRecord {
            kind: Some(KIND.into()), definition_id: Some(definition.artefact_id_for(PACKED_FORMAT_VERSION)),
            definition_version: Some(definition.version), stats: Some(file::encode_tree_stats(sections, locator.into()).unwrap()),
            scope: Some(ArtefactScope::Pack as i32), file_size: Some(file_size), published_version: None,
            pack_members: pack_members.clone(),
        };
        let catalog = catalog.with_artefacts(&table, [
            (keys[0].clone(), record(&[section, section], &locator, 128)),
            (keys[1].clone(), record(&[], &locator, 128)),
            (keys[2].clone(), record(&[], &other_locator, 128)),
            (keys[3].clone(), record(&[], &locator, 127)),
        ]);
        let tables = TableCatalog::default().with_entries_for([table]).unwrap();
        let snapshot = PartitionValue::loaded(catalog, tables, Default::default()).unwrap().snapshot();
        let routing = TableRouting::open(&snapshot, &definition, true, None).unwrap();
        let [pack] = routing.packs.as_slice() else { panic!("expected one pack"); };

        assert_eq!(pack.members, BTreeMap::from([(0, keys[0].clone()), (1, keys[1].clone())]));
        assert_eq!(pack.sections, None);
        let bounds = TemporalBounds { valid_time: Interval::ALL, system_time: Interval::ALL };
        assert!(pack.can_nominate(bounds, 0, &rtdb_text::Query::parse("report")));
    }
}
