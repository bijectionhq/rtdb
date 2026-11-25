use std::{collections::{BTreeMap, BTreeSet}, sync::Arc};

/// Persistent ordered map: cloning is O(1) and an insert through a shared clone copies only
/// the path to the changed leaf, so a decision's copy-on-write costs O(log n) instead of
/// copying the whole map. Internal to this module; no signature exposes it.
type Map<K, V> = imbl::OrdMap<K, V>;

use rtdb_types::{
    Anomaly, AnomalyCategory, EntityId, Iid, Interval, LogicalType, Result, StructFields, TableRef,
    Timestamp, TxId, Value,
    rtdb_v1::{At, Basis, Branch, ClaimEvidence, ClaimRegion, LabelScope, Layer, MAIN_BRANCH, NativeCheckpointCustody, NativeDependentCustodyOrder},
};
use serde::{Deserialize, Serialize};

mod custody;
mod custody_placement;
mod custody_release;
mod custody_transfer;
mod scoped_hold;
mod visibility;
pub use scoped_hold::{ScopedHold, ScopedHoldCommand};
pub use custody::{CustodyGroupId, CustodyInventory, CustodyPayload};
pub use custody_placement::{CustodyOccurrence, CustodyPlacementEntry, CustodyReservation, CustodyPlacementId, CustodyReview, CustodyTarget, DerivedCustodyReservation, ReviewedCustodyReservation};
pub use custody_release::{CustodyCarrierAuthority, CustodyRelease, CustodyReleasePhase, CustodyReleaseRegistration, CustodyResponseAdmission, CustodyResponsePart};
pub use visibility::{VisibleChange, VisibleChangeGroup, VisibleHistory};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchCut {
    pub basis: Basis,
    pub system_time: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexDefinition {
    pub name: String,
    pub column: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub additional_columns: Vec<String>,
}

impl IndexDefinition {
    pub fn columns(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.column.as_str()).chain(self.additional_columns.iter().map(String::as_str))
    }

    pub fn is_compound(&self) -> bool { !self.additional_columns.is_empty() }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RankedIndexKind {
    Search { analyzer: String, history: bool },
    Spatial { geometry_encoding: String },
    Ivf { dimensions: u32, recall_bits: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RankedIndexDefinition {
    pub columns: Vec<String>,
    pub kind: RankedIndexKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RankedIndexDeclaration {
    pub definition: RankedIndexDefinition,
    pub position: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CountRange {
    pub table: TableRef,
    pub index: String,
    pub lower: Option<Vec<u8>>,
    pub upper: Option<Vec<u8>>,
    pub valid_at: Option<Timestamp>,
    pub n: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupedCount {
    pub column: String,
    pub prefix_bytes: Option<u32>,
    pub equals: BTreeMap<String, Vec<u8>>,
    pub valid_at: Option<Timestamp>,
    pub n: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchTable {
    pub columns: StructFields<LogicalType>,
    pub indexes: BTreeMap<String, IndexDefinition>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub ranked_indexes: BTreeMap<String, RankedIndexDeclaration>,
    pub ttl_micros: Option<u64>,
    pub constraints: BTreeMap<String, CountRange>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub grouped_constraints: BTreeMap<String, GroupedCount>,
    pub generation: u64,
    pub dropped: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub visibility: Option<Visibility>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub single_label_per_tx: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Visibility {
    pub column: String,
}

impl Visibility {
    pub fn label<'a>(&self, cells: &'a StructFields<Value>) -> Option<&'a [u8]> {
        match cells.get(&self.column)? {
            Value::Binary(label) | Value::FixedSizeBinary(label) if !label.is_empty() => Some(label),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RowImage {
    pub cells: StructFields<Value>,
    pub expires_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custody_group: Option<CustodyGroupId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custody_payload: Option<CustodyPayload>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BranchEvent {
    pub id: EntityId,
    pub valid: Interval,
    pub cut: BranchCut,
    pub image: Option<Arc<RowImage>>,
    #[serde(default, skip_serializing_if = "WrittenVersion::is_branch")]
    pub written: WrittenVersion,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum WrittenVersion {
    #[default]
    Branch,
    Custody(Option<u64>),
    Classic(TxId),
    Unpositioned,
}

impl WrittenVersion {
    fn is_branch(&self) -> bool { *self == Self::Branch }
}

const CLASSIC_WRITTEN_BIT: u64 = 1 << 63;

impl BranchEvent {
    pub fn written_basis(&self) -> Option<Basis> {
        match self.written {
            WrittenVersion::Branch | WrittenVersion::Custody(_) => Some(self.cut.basis),
            WrittenVersion::Classic(transaction) => u64::try_from(transaction.0).ok()
                .map(|position| Basis::new(self.cut.basis.incarnation(), CLASSIC_WRITTEN_BIT | position)),
            WrittenVersion::Unpositioned => None,
        }
    }

    pub fn image_at(&self, valid_at: Timestamp) -> Option<&RowImage> {
        self.image.as_deref().filter(|image| image.expires_at.is_none_or(|until| valid_at < until))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchLineage {
    pub branch: Branch,
    pub base: Option<(Branch, Basis)>,
    pub created: Basis,
    pub writable: bool,
    pub expires_at: Option<Timestamp>,
    pub dropped: Option<Basis>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum BranchEffect {
    Write { branch: Branch, table: TableRef, event: BranchEvent },
    Fork { child: Branch, from: (Branch, Basis), writable: bool, expires_at: Option<Timestamp> },
    Renew { branch: Branch, expires_at: Timestamp },
    Drop { branch: Branch },
    Declare { table: TableRef, definition: BranchTable },
    Erase { branch: Option<Branch>, table: TableRef, id: EntityId },
    CreateCustodyGroup { group: CustodyGroupId },
    RevokeCustodyGroup { group: CustodyGroupId },
    ReserveCustodyPlacement { group: CustodyGroupId, reservation: CustodyReservation },
    ReserveReviewedCustodyPlacement { group: CustodyGroupId, reservation: CustodyReservation, reviewed: ReviewedCustodyReservation },
    ActivateCustodyOccurrence { group: CustodyGroupId, occurrence: CustodyOccurrence },
    ActivateDerivedCustodyOccurrence { group: CustodyGroupId, occurrence: CustodyOccurrence, reservation: DerivedCustodyReservation },
    CustodyRelease(CustodyRelease),
    NativeCustodyTransfer { custody: NativeCheckpointCustody, destination: Option<Basis> },
    ScopedHold { command: ScopedHoldCommand, observed_at: Timestamp },
    NativeDependentCustody(NativeDependentCustodyOrder),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BranchWrite {
    pub branch: Branch,
    pub table: TableRef,
    pub id: EntityId,
    pub valid: Option<Interval>,
    pub erased_written_basis: Option<Basis>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BranchData {
    base: Option<(Branch, Basis)>,
    created: Basis,
    writable: bool,
    dropped: Option<Basis>,
    expiries: Vec<(Basis, Option<Timestamp>)>,
    changed: Option<Basis>,
    #[serde(with = "entries")]
    tables: Map<TableRef, Arc<BranchRows>>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct BranchRows {
    #[serde(with = "entries")]
    events: Map<EntityId, Arc<Vec<(u64, BranchEvent)>>>,
    #[serde(with = "entries")]
    erased: Map<EntityId, Basis>,
    #[serde(default, with = "entries", skip_serializing_if = "Map::is_empty")]
    retired_labels: Map<EntityId, Arc<BTreeMap<usize, custody::RetiredLabel>>>,
    #[serde(default, with = "entries", skip_serializing_if = "Map::is_empty")]
    visibility_erasures: Map<EntityId, Arc<Vec<visibility::VisibilityErasure>>>,
}

impl BranchData {
    fn expires_at(&self, basis: Basis) -> Option<Timestamp> {
        self.expiries.iter().rev().find(|(at, _)| *at <= basis).and_then(|(_, expiry)| *expiry)
    }

    fn active(&self, cut: BranchCut, now: Timestamp) -> bool {
        self.created <= cut.basis
            && self.dropped.is_none_or(|dropped| dropped > cut.basis)
            && self.expires_at(cut.basis).is_none_or(|expiry| now < expiry)
    }
}

/// The immutable state visible at an applied transaction cut.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BranchIndex {
    head: BranchCut,
    times: Map<u64, Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    classic_checkpoint: Option<Basis>,
    branches: Map<Branch, Arc<BranchData>>,
    #[serde(with = "entries")]
    catalog: Map<TableRef, Arc<Vec<(Basis, BranchTable)>>>,
    #[serde(default, with = "entries", skip_serializing_if = "Map::is_empty")]
    classic_iids: Map<(TableRef, EntityId), Iid>,
    #[serde(default, with = "entries", skip_serializing_if = "Map::is_empty")]
    custody: Map<CustodyGroupId, Arc<custody::CustodyGroup>>,
    #[serde(default, with = "entries", skip_serializing_if = "Map::is_empty")]
    custody_transfers: Map<[u8; 16], custody_transfer::Forward>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    inherited_custody: Option<NativeCheckpointCustody>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    latest_custody_revocation: Option<Basis>,
    #[serde(default, skip_serializing_if = "imbl::OrdSet::is_empty")]
    used_custody_placements: imbl::OrdSet<CustodyPlacementId>,
    #[serde(default, with = "entries", skip_serializing_if = "Map::is_empty")]
    scoped_holds: Map<([u8; 32], [u8; 16]), ScopedHold>,
    changed_catalog: Option<Basis>,
    erased: Option<Basis>,
    #[serde(skip)]
    writes: Arc<Vec<BranchWrite>>,
    #[serde(skip)]
    visibility_projection: bool,
}

impl BranchIndex {
    pub fn new(incarnation: [u8; 16]) -> Self {
        Self::with_initial_time(incarnation, Timestamp::MIN)
    }

    fn with_initial_time(incarnation: [u8; 16], system_time: Timestamp) -> Self {
        let basis = Basis::new(incarnation, 0);
        let head = BranchCut { basis, system_time };
        let main = BranchData {
            base: None, created: basis, writable: true, dropped: None,
            expiries: vec![(basis, None)], changed: None, tables: Map::new(),
        };
        Self {
            head, times: Map::unit(0, head.system_time), classic_checkpoint: None,
            branches: Map::unit(MAIN_BRANCH.into(), Arc::new(main)),
            catalog: Map::new(), classic_iids: Map::new(), custody: Map::new(), latest_custody_revocation: None,
            custody_transfers: Map::new(), inherited_custody: None,
            scoped_holds: Map::new(), changed_catalog: None, erased: None, writes: Arc::default(),
            visibility_projection: false, used_custody_placements: imbl::OrdSet::new(),
        }
    }

    pub fn head(&self) -> BranchCut { self.head }

    pub fn applied_position(&self, cut: BranchCut) -> Result<u64> {
        self.require_incarnation(cut.basis)?;
        self.times.get_key_value(&cut.basis.position())
            .filter(|(_, time)| **time == cut.system_time)
            .map(|(&position, _)| position)
            .ok_or_else(|| Anomaly::fault("rtdb/transaction-order", "The selected cut has no applied position"))
    }

    /// Enumerates the stored images a checkpoint carries, including inactive branches and superseded rows.
    pub fn retained_images(&self) -> impl Iterator<Item = (&str, &TableRef, &BranchEvent, &RowImage)> {
        self.branches.iter().flat_map(|(branch, data)| {
            data.tables.iter().flat_map(move |(table, rows)| {
                rows.events.values().flat_map(|history| history.iter()).filter_map(move |(_, event)| {
                    event.image.as_deref().map(|image| (branch.as_str(), table, event, image))
                })
            })
        })
    }

    /// Retained definitions also carry visibility rules for historical reads.
    pub fn has_visibility_history(&self) -> bool {
        self.catalog.values().any(|history| history.iter().any(|(_, definition)| definition.visibility.is_some()))
    }

    /// Reports canonical state that requires translation beyond the retained Classic rows.
    pub fn has_classic_migration_controls(&self) -> bool {
        self.branches.len() != 1 || self.erased.is_some() || !self.custody.is_empty()
            || !self.custody_transfers.is_empty() || self.inherited_custody.is_some()
            || self.latest_custody_revocation.is_some() || !self.used_custody_placements.is_empty()
            || !self.scoped_holds.is_empty()
            || self.catalog.values().any(|history| history.iter().any(|(_, definition)| {
                !definition.indexes.is_empty() || !definition.ranked_indexes.is_empty() || definition.ttl_micros.is_some()
                    || !definition.constraints.is_empty() || !definition.grouped_constraints.is_empty()
                    || definition.generation != 0 || definition.dropped || definition.visibility.is_some()
                    || definition.single_label_per_tx
            }))
            || self.retained_images().any(|(_, table, _, image)| {
                image.expires_at.is_some() || image.custody_group.is_some() || image.custody_payload.is_some()
                    || Self::unowned_checkpoint_image(table, image)
            })
    }

    /// Select an exact settled prefix for checkpoint export. Erasure fences
    /// constrain export even when a historical cut is still queryable.
    pub fn checkpoint_at(&self, at: Basis) -> Result<Self> {
        let head = self.exact(at)?;
        if [self.erased, self.latest_custody_revocation].into_iter().flatten().any(|fence| fence > at) {
            return Err(Anomaly::new(AnomalyCategory::NotFound, "rtdb/erased-under-view", "The export cut precedes an erasure fence"));
        }
        if self.has_custody_groups() {
            return Err(Anomaly::unsupported("rtdb/export-custody", "A protected export needs an admitted destination custody binding"));
        }
        if head == self.head {
            return Ok(self.clone());
        }

        let mut retained = self.clone();
        retained.scoped_holds.clear();
        retained.head = head;
        retained.writes = Arc::default();
        retained.times = self.times.range(..=at.position()).map(|(&position, &time)| (position, time)).collect();
        retained.catalog = self.catalog.iter().filter_map(|(table, definitions)| {
            let count = definitions.partition_point(|(written, _)| *written <= at);
            (count != 0).then(|| (table.clone(), Arc::new(definitions[..count].to_vec())))
        }).collect();
        retained.branches = self.branches.iter().filter(|(_, branch)| branch.created <= at)
            .map(|(name, branch)| (name.clone(), Arc::new(branch.checkpoint_at(at)))).collect();
        retained.changed_catalog = retained.catalog.values().filter_map(|history| history.last().map(|(basis, _)| *basis))
            .chain(retained.branches.iter().flat_map(|(name, branch)| {
                (name != MAIN_BRANCH).then_some(branch.created).into_iter()
                    .chain(branch.dropped)
                    .chain(branch.expiries.iter().skip(1).map(|(at, _)| *at))
            })).max();
        Ok(retained)
    }

    /// Import a plain checkpoint into a new incarnation, retaining each
    /// historical position but changing the authority that can use it.
    pub fn restore_in(&mut self, incarnation: [u8; 16]) -> Result<()> {
        if self.has_custody_groups() {
            return Err(Anomaly::unsupported("rtdb/restore-custody", "A protected restore needs an admitted destination custody binding"));
        }
        self.rebase_history(incarnation);
        Ok(())
    }

    fn rebase_history(&mut self, incarnation: [u8; 16]) {
        let rebase = |basis: Basis| Basis::new(incarnation, basis.position());
        self.head.basis = rebase(self.head.basis);
        self.classic_checkpoint = self.classic_checkpoint.map(rebase);
        self.erased = self.erased.map(rebase);
        self.changed_catalog = self.changed_catalog.map(rebase);
        self.writes = Arc::default();
        self.scoped_holds.clear();
        for table in self.catalog.keys().cloned().collect::<Vec<_>>() {
            let definitions = self.catalog.get_mut(&table).expect("listed table");
            for (written, _) in Arc::make_mut(definitions) {
                *written = rebase(*written);
            }
        }
        for name in self.branches.keys().cloned().collect::<Vec<_>>() {
            let branch = Arc::make_mut(self.branches.get_mut(&name).expect("listed branch"));
            branch.created = rebase(branch.created);
            branch.dropped = branch.dropped.map(rebase);
            branch.changed = branch.changed.map(rebase);
            if let Some((_, basis)) = &mut branch.base { *basis = rebase(*basis); }
            for (written, _) in &mut branch.expiries { *written = rebase(*written); }
            for table in branch.tables.keys().cloned().collect::<Vec<_>>() {
                let rows = Arc::make_mut(branch.tables.get_mut(&table).expect("listed branch table"));
                rows.erased = rows.erased.iter().map(|(id, &written)| (id.clone(), rebase(written))).collect();
                for id in rows.visibility_erasures.keys().cloned().collect::<Vec<_>>() {
                    let history = rows.visibility_erasures.get_mut(&id).expect("listed erased entity");
                    for erasure in Arc::make_mut(history) {
                        erasure.rebase(rebase);
                    }
                }
                for id in rows.events.keys().cloned().collect::<Vec<_>>() {
                    let events = rows.events.get_mut(&id).expect("listed entity");
                    for (_, event) in Arc::make_mut(events) {
                        event.cut.basis = rebase(event.cut.basis);
                    }
                }
            }
        }
    }

    pub fn writes_at(&self, basis: Basis) -> Vec<BranchWrite> {
        if basis == self.head.basis { self.writes.as_ref().clone() } else { Vec::new() }
    }

    pub fn before(&self, basis: Basis) -> Result<Option<BranchCut>> {
        self.require_incarnation(basis)?;
        Ok(self.times.range(..basis.position()).next_back().map(|(&position, &system_time)| {
            BranchCut { basis: Basis::new(basis.incarnation(), position), system_time }
        }))
    }

    pub fn cut(&self, at: At) -> Result<BranchCut> {
        match at {
            At::Latest => Ok(self.head),
            At::Linearizable => Err(Anomaly::unsupported(
                "rtdb/linearizable-barrier", "The mode driver must capture a fresh prefix before opening the view",
            )),
            At::Exactly(basis) => self.exact(basis),
            At::AtLeast(basis) => {
                self.require_incarnation(basis)?;
                if basis > self.head.basis {
                    return Err(Anomaly::new(AnomalyCategory::Busy, "rtdb/basis-ahead", "The requested basis has not applied"));
                }
                Ok(self.head)
            }
            At::AsOf(time) => {
                if let Some(checkpoint) = self.classic_checkpoint && time < self.exact(checkpoint)?.system_time {
                    return Err(classic_cut_expired());
                }
                let (&position, &system_time) = self.times.iter().rev().find(|(_, at)| **at <= time)
                    .ok_or_else(|| Anomaly::fault("rtdb/basis", "The initial branch cut is missing"))?;
                Ok(BranchCut { basis: Basis::new(self.head.basis.incarnation(), position), system_time })
            }
        }
    }

    pub fn exact(&self, basis: Basis) -> Result<BranchCut> {
        self.require_incarnation(basis)?;
        if self.classic_checkpoint.is_some_and(|checkpoint| basis < checkpoint) { return Err(classic_cut_expired()); }
        let system_time = self.times.get(&basis.position()).copied().ok_or_else(|| {
            Anomaly::new(AnomalyCategory::NotFound, "rtdb/unknown-basis", "The database has no transaction at this basis")
        })?;
        Ok(BranchCut { basis, system_time })
    }

    pub fn seal_classic_checkpoint(&mut self) { self.classic_checkpoint = Some(self.head.basis); }

    pub fn validate_written_basis(&self, basis: Basis) -> Result<()> {
        self.require_incarnation(basis)?;
        if basis.position() & CLASSIC_WRITTEN_BIT == 0 || self.classic_iids.is_empty() { self.exact(basis)?; }
        Ok(())
    }

    fn require_incarnation(&self, basis: Basis) -> Result<()> {
        if basis.incarnation() != self.head.basis.incarnation() {
            return Err(Anomaly::incorrect("rtdb/foreign-basis", "The basis belongs to another database incarnation"));
        }
        Ok(())
    }

    pub fn view<'a>(&'a self, branch: &'a str, at: At, layer: Layer, now: Timestamp) -> Result<BranchView<'a>> {
        let cut = self.cut(at)?;
        let data = self.active_branch(branch, cut, cut.system_time)?;
        self.active_branch(branch, self.head, now.max(self.head.system_time))?;
        if let Some(erased) = self.erased
            && erased > cut.basis
        {
            return Err(Anomaly::new(AnomalyCategory::NotFound, "rtdb/erased-under-view", "The basis precedes erasure"));
        }
        Ok(BranchView { index: self, branch, cut, layer, data })
    }

    fn active_branch(&self, branch: &str, cut: BranchCut, now: Timestamp) -> Result<&BranchData> {
        self.branches.get(branch).filter(|data| data.active(cut, now))
            .map(Arc::as_ref).ok_or_else(|| unknown_branch(branch))
    }

    pub fn writable(&self, branch: &str) -> Result<()> {
        if !self.active_branch(branch, self.head, self.head.system_time)?.writable {
            return Err(Anomaly::new(AnomalyCategory::Forbidden, "rtdb/read-only-branch", "The branch is read-only"));
        }
        Ok(())
    }

    pub fn receipt_written_local_point(
        &self, branch: &str, created: Basis, basis: Basis, table: &TableRef, id: &EntityId,
    ) -> Result<Option<&BranchEvent>> {
        let cut = self.exact(basis)?;
        let data = self.active_branch(branch, cut, cut.system_time)?;
        if data.created != created || self.has_visibility_history()
            || self.erased.is_some_and(|erased| erased > basis)
        {
            return Err(unknown_branch(branch));
        }
        let Some(definition) = self.definition(table, basis).filter(|definition| !definition.dropped) else {
            return Ok(None);
        };
        let Some(rows) = data.tables.get(table).filter(|rows| !rows.erased.contains_key(id)) else {
            return Ok(None);
        };
        Ok(rows.events.get(id).and_then(|events| events.iter().rev().find_map(|(generation, event)| {
            (*generation == definition.generation && event.written_basis() == Some(basis)
                && event.valid.contains(cut.system_time)).then_some(event)
        })).filter(|event| event.image_at(cut.system_time).is_some()))
    }

    pub fn has_branch(&self, branch: &str) -> bool { self.branches.contains_key(branch) }

    pub fn lineage_at(&self, branch: &str, at: Basis) -> Option<BranchLineage> {
        self.branch_lineage(branch, self.branches.get(branch)?, at)
    }

    fn branch_lineage(&self, branch: &str, data: &BranchData, at: Basis) -> Option<BranchLineage> {
        (data.created <= at).then(|| BranchLineage {
            branch: branch.to_owned(),
            base: data.base.clone(),
            created: data.created,
            writable: data.writable,
            expires_at: data.expires_at(at),
            dropped: data.dropped.filter(|dropped| *dropped <= at),
        })
    }

    pub fn lineage(&self, at: Basis) -> Vec<BranchLineage> {
        self.branches.iter().filter_map(|(branch, data)| self.branch_lineage(branch, data, at)).collect()
    }

    pub fn active_names(&self, now: Timestamp) -> impl Iterator<Item = &str> {
        self.branches.iter().filter(move |(_, data)| data.active(self.head, now))
            .map(|(branch, _)| branch.as_str())
    }

    pub fn is_unchanged(&self, branch: &str, basis: Basis, layer: Layer, now: Timestamp) -> Result<bool> {
        let cut = self.exact(basis)?;
        let data = self.active_branch(branch, cut, cut.system_time)?;
        if !data.active(self.head, now)
            || data.changed.is_some_and(|last| last > basis)
            || self.changed_catalog.is_some_and(|changed| changed > basis)
            || self.erased.is_some_and(|erased| erased > basis)
            || self.latest_custody_revocation.is_some_and(|revocation| revocation > basis)
        {
            return Ok(false);
        }
        let crossed = |at: Timestamp| cut.system_time < at && at <= now;
        let view = self.view(branch, At::Exactly(basis), layer, now)?;
        for (table, _) in view.tables() {
            for id in view.ids(table) {
                if view.history(table, id).into_iter().any(|event| {
                    crossed(event.valid.lower()) || crossed(event.valid.upper())
                        || event.image.as_ref().and_then(|image| image.expires_at).is_some_and(crossed)
                }) {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    pub fn claim_unchanged(&self, claim: &ClaimEvidence, now: Timestamp) -> Result<bool> {
        let basis = claim.scope.basis;
        let branch = &claim.scope.branch;
        let layer = claim.scope.layer;
        let Some(regions) = claim.footprint.regions() else {
            return self.is_unchanged(branch, basis, layer, now);
        };
        let cut = self.exact(basis)?;
        let data = self.active_branch(branch, cut, cut.system_time)?;
        if !data.active(self.head, now)
            || data.expiries.last().is_some_and(|(at, _)| *at > basis)
            || self.erased.is_some_and(|at| at > basis)
            || self.latest_custody_revocation.is_some_and(|revocation| revocation > basis)
        {
            return Ok(false);
        }
        let view = self.view(branch, At::Latest, layer, now)?;
        let crossed = |at: Timestamp| cut.system_time < at && at <= now;
        for region in regions {
            let table = region.table();
            if self.catalog.get(table).and_then(|defs| defs.last()).is_some_and(|(at, _)| *at > basis) {
                return Ok(false);
            }
            let range = match region {
                ClaimRegion::IndexRange { index, lower, upper, .. } => {
                    let column = view.table(table).and_then(|definition| definition.indexes.get(index))
                        .ok_or_else(|| Anomaly::new(AnomalyCategory::NotFound, "rtdb/unknown-index", "Claim names an unknown index"))?;
                    // A table footprint covers changes to any component of a compound key.
                    (!column.is_compound()).then_some((&column.column, lower.as_deref(), upper.as_deref()))
                }
                _ => None,
            };
            for id in view.ids(table) {
                if matches!(region, ClaimRegion::Row { iid, .. } if *iid != id.iid()) { continue; }
                if let (ClaimRegion::IidRange { lower, upper, .. }, EntityId::Uuid(bytes)) = (region, id)
                    && (bytes < &lower.0 || bytes > &upper.0)
                {
                    continue;
                }
                let history = view.history(table, id);
                if let Some((column, lower, upper)) = range
                    && !history.iter().filter_map(|event| event.image.as_ref())
                        .filter_map(|image| image.cells.get(column).and_then(index_key))
                        .any(|key| lower.is_none_or(|lower| key >= lower) && upper.is_none_or(|upper| key < upper))
                {
                    continue;
                }
                if history.iter().any(|event| {
                    event.cut.basis > basis || crossed(event.valid.lower()) || crossed(event.valid.upper())
                        || event.image.as_ref().and_then(|image| image.expires_at).is_some_and(crossed)
                }) {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    pub fn definition(&self, table: &TableRef, basis: Basis) -> Option<&BranchTable> {
        self.catalog.get(table)?.iter().rev().find(|(at, _)| *at <= basis).map(|(_, definition)| definition)
    }

    pub fn tables(&self, basis: Basis) -> impl Iterator<Item = (&TableRef, &BranchTable)> {
        self.catalog.keys().filter_map(move |table| {
            let definition = self.definition(table, basis)?;
            (!definition.dropped).then_some((table, definition))
        })
    }

    pub fn has_row_history(&self, table: &TableRef) -> bool {
        self.branches.values().any(|branch| branch.tables.contains_key(table))
    }

    pub fn stored_labels(&self, table: &TableRef, branch: Option<&str>, id: &EntityId, visibility: &Visibility) -> BTreeSet<Vec<u8>> {
        self.branches.iter()
            .filter(|(name, _)| branch.is_none_or(|branch| name.as_str() == branch))
            .filter_map(|(_, branch)| branch.tables.get(table)?.events.get(id))
            .flat_map(|events| events.iter())
            .filter_map(|(_, event)| visibility.label(&event.image.as_ref()?.cells))
            .map(<[u8]>::to_vec)
            .collect()
    }

    pub fn advance(&mut self, cut: BranchCut) -> Result<()> {
        if cut.system_time == self.head.system_time {
            return Err(Anomaly::fault("rtdb/transaction-order", "The applied transaction cut must advance system time"));
        }
        self.advance_classic(cut)
    }

    pub fn advance_classic(&mut self, cut: BranchCut) -> Result<()> {
        self.require_incarnation(cut.basis)?;
        if cut.basis <= self.head.basis || cut.system_time < self.head.system_time {
            return Err(Anomaly::fault("rtdb/transaction-order", "The applied transaction cut must advance position and system time"));
        }
        self.head = cut;
        Arc::make_mut(&mut self.writes).clear();
        self.times.insert(cut.basis.position(), cut.system_time);
        Ok(())
    }

    pub fn physical_iid(&self, table: &TableRef, id: &EntityId) -> Iid {
        self.classic_iids.get(&(table.clone(), id.clone())).copied().unwrap_or_else(|| id.iid())
    }

    pub fn apply_classic_event(&mut self, table: &TableRef, event: rtdb_format::data::StoredEvent, classic_transaction: Option<TxId>) -> Result<()> {
        use rtdb_format::data::StoredOperation;

        let cut = self.head;
        let iid = event.iid;
        let written = classic_transaction.map_or(WrittenVersion::Unpositioned, WrittenVersion::Classic);
        match event.operation {
            StoredOperation::Put(cells) => {
                let id = cells.get("_id").ok_or_else(classic_identity)?.entity_id().map_err(|_| classic_identity())?;
                let address = (table.clone(), id.clone());
                if self.classic_iids.get(&address).is_some_and(|known| *known != iid) {
                    return Err(classic_identity());
                }
                self.classic_iids.insert(address, iid);
                if let Some(definition) = self.definition(table, cut.basis).filter(|definition| definition.dropped) {
                    let generation = definition.generation;
                    self.apply(BranchEffect::Declare { table: table.clone(), definition: BranchTable { generation, ..Default::default() } })?;
                }
                let image = Some(Arc::new(RowImage { cells, expires_at: None, custody_group: None, custody_payload: None }));
                self.apply(BranchEffect::Write { branch: MAIN_BRANCH.into(), table: table.clone(), event: BranchEvent {
                    id, valid: event.valid_time, cut, image, written,
                } })
            }
            operation @ (StoredOperation::Delete | StoredOperation::Erase) => {
                let erase = matches!(operation, StoredOperation::Erase);
                let ids = self.branches[MAIN_BRANCH].tables.get(table).into_iter()
                    .flat_map(|rows| rows.events.keys()).filter(|id| self.physical_iid(table, id) == iid)
                    .cloned().collect::<Vec<_>>();
                for id in ids {
                    let effect = if erase {
                        BranchEffect::Erase { branch: Some(MAIN_BRANCH.into()), table: table.clone(), id }
                    } else {
                        BranchEffect::Write { branch: MAIN_BRANCH.into(), table: table.clone(), event: BranchEvent {
                            id, valid: event.valid_time, cut, image: None, written,
                        } }
                    };
                    self.apply(effect)?;
                }
                if erase { self.erased = Some(cut.basis); }
                Ok(())
            }
            StoredOperation::Redacted => Err(Anomaly::unsupported("rtdb/classic-redacted-history", "The retained Classic history includes a redacted event")),
        }
    }

    pub fn apply(&mut self, effect: BranchEffect) -> Result<()> {
        let basis = self.head.basis;
        match effect {
            BranchEffect::Write { branch, table, mut event } => {
                if event.cut != self.head || matches!(event.written, WrittenVersion::Custody(_)) {
                    return Err(Anomaly::fault("rtdb/event-cut", "Resolved event differs from the transaction cut"));
                }
                if let Some(group) = event.image.as_ref().and_then(|image| image.custody_group) {
                    self.admit_custody_member(group, custody::CustodyMember {
                        branch: branch.clone(), table: table.clone(), id: event.id.clone(), written: basis,
                        valid: event.valid, ordinal: self.writes.len() as u64,
                    })?;
                    if event.written == WrittenVersion::Branch {
                        event.written = WrittenVersion::Custody(None);
                    }
                }
                let catalog = &mut self.catalog;
                let definitions = catalog.entry(table.clone()).or_default();
                if definitions.is_empty() {
                    Arc::make_mut(definitions).push((basis, BranchTable::default()));
                    self.changed_catalog = Some(basis);
                }
                let generation = definitions.last().expect("table definition").1.generation;
                let write = BranchWrite {
                    branch: branch.clone(), table: table.clone(), id: event.id.clone(),
                    valid: Some(event.valid), erased_written_basis: None,
                };
                let branch = self.changed_branch(&branch)?;
                let rows = Arc::make_mut(branch.tables.entry(table).or_default());
                Arc::make_mut(rows.events.entry(event.id.clone()).or_default()).push((generation, event));
                Arc::make_mut(&mut self.writes).push(write);
            }
            BranchEffect::Fork { child, from, writable, expires_at } => {
                if from.1 >= basis || self.has_branch(&child) || !self.has_branch(&from.0) {
                    return Err(Anomaly::fault("rtdb/branch-fold", "Invalid resolved fork"));
                }
                self.changed_catalog = Some(basis);
                self.branches.insert(child, Arc::new(BranchData {
                    base: Some(from), created: basis, writable, dropped: None,
                    expiries: vec![(basis, expires_at)], changed: Some(basis), tables: Map::new(),
                }));
            }
            BranchEffect::Renew { branch, expires_at } => {
                self.changed_branch(&branch)?.expiries.push((basis, Some(expires_at)));
                self.changed_catalog = Some(basis);
            }
            BranchEffect::Drop { branch } => {
                self.changed_branch(&branch)?.dropped = Some(basis);
                self.changed_catalog = Some(basis);
            }
            BranchEffect::Declare { table, definition } => {
                Arc::make_mut(self.catalog.entry(table).or_default()).push((basis, definition));
                self.changed_catalog = Some(basis);
            }
            BranchEffect::Erase { branch, table, id } => {
                if branch.as_deref().is_none_or(|branch| branch == MAIN_BRANCH) {
                    self.classic_iids.remove(&(table.clone(), id.clone()));
                }
                let names = self.branches.keys()
                    .filter(|name| branch.as_ref().is_none_or(|branch| branch == *name))
                    .cloned().collect::<Vec<_>>();
                for name in names {
                    if name == MAIN_BRANCH && !self.visibility_projection {
                        self.record_visibility_erasure(&table, &id, basis);
                    }
                    let data = Arc::make_mut(self.branches.get_mut(&name).expect("listed branch"));
                    let rows = Arc::make_mut(data.tables.entry(table.clone()).or_default());
                    rows.events.remove(&id);
                    rows.retired_labels.remove(&id);
                    rows.erased.insert(id.clone(), basis);
                    Arc::make_mut(&mut self.writes).push(BranchWrite {
                        branch: name.clone(), table: table.clone(), id: id.clone(),
                        valid: None, erased_written_basis: None,
                    });
                }
                self.erased = Some(basis);
            }
            BranchEffect::CreateCustodyGroup { group } => self.create_custody_group(group)?,
            BranchEffect::RevokeCustodyGroup { group } => self.revoke_custody_group(group)?,
            BranchEffect::ReserveCustodyPlacement { group, reservation } => self.reserve_custody_placement(group, reservation)?,
            BranchEffect::ReserveReviewedCustodyPlacement { group, reservation, reviewed } => {
                self.reserve_reviewed_custody_placement(group, reservation, reviewed)?;
            }
            BranchEffect::ActivateCustodyOccurrence { group, occurrence } => self.activate_custody_occurrence(group, occurrence, None)?,
            BranchEffect::ActivateDerivedCustodyOccurrence { group, occurrence, reservation } => {
                self.activate_custody_occurrence(group, occurrence, Some(reservation))?;
            }
            BranchEffect::CustodyRelease(release) => self.apply_custody_release(release)?,
            BranchEffect::NativeCustodyTransfer { custody, destination } => self.apply_native_custody_transfer(custody, destination)?,
            BranchEffect::ScopedHold { command, observed_at } => self.apply_scoped_hold(command, observed_at),
            BranchEffect::NativeDependentCustody(order) => self.apply_native_dependent_custody(order)?,
        }
        Ok(())
    }

    fn changed_branch(&mut self, branch: &str) -> Result<&mut BranchData> {
        let basis = self.head.basis;
        let data = Arc::make_mut(self.branches.get_mut(branch)
            .ok_or_else(|| Anomaly::fault("rtdb/branch-fold", "Resolved write names an unknown branch"))?);
        data.changed = Some(basis);
        Ok(data)
    }
}

impl BranchData {
    fn checkpoint_at(&self, at: Basis) -> Self {
        let dropped = self.dropped.filter(|dropped| *dropped <= at);
        let expiries = self.expiries.iter().copied().take_while(|(written, _)| *written <= at).collect::<Vec<_>>();
        let tables: Map<_, _> = self.tables.iter().filter_map(|(table, rows)| {
            let rows = rows.checkpoint_at(at);
            (!rows.events.is_empty() || !rows.erased.is_empty()).then(|| (table.clone(), Arc::new(rows)))
        }).collect();
        let changed = self.changed.filter(|changed| *changed <= at).or_else(|| {
            tables.values().flat_map(|rows: &Arc<BranchRows>| {
                rows.events.values().filter_map(|events| events.last().map(|(_, event)| event.cut.basis))
                    .chain(rows.erased.values().copied())
                    .chain(rows.retired_labels.values().flat_map(|labels| labels.values().map(|label| label.revoked)))
            }).chain(expiries.iter().skip(1).map(|(basis, _)| *basis))
                .chain(self.base.as_ref().map(|_| self.created))
                .chain(dropped).max()
        });
        Self { base: self.base.clone(), created: self.created, writable: self.writable, dropped, expiries, changed, tables }
    }
}

impl BranchRows {
    fn checkpoint_at(&self, at: Basis) -> Self {
        let events = self.events.iter().filter_map(|(id, events)| {
            let count = events.partition_point(|(_, event)| event.cut.basis <= at);
            (count != 0).then(|| (id.clone(), Arc::new(events[..count].to_vec())))
        }).collect();
        Self {
            events,
            // The export admission already excluded cuts preceding an erasure.
            erased: self.erased.clone(),
            retired_labels: self.retired_labels.clone(),
            visibility_erasures: self.visibility_erasures.clone(),
        }
    }
}

pub struct BranchView<'a> {
    index: &'a BranchIndex,
    branch: &'a str,
    cut: BranchCut,
    layer: Layer,
    data: &'a BranchData,
}

impl<'a> BranchView<'a> {
    pub fn cut(&self) -> BranchCut { self.cut }
    pub fn branch(&self) -> &str { self.branch }
    pub fn layer(&self) -> Layer { self.layer }
    pub fn physical_iid(&self, table: &TableRef, id: &EntityId) -> Iid { self.index.physical_iid(table, id) }
    pub fn base(&self) -> Option<&(Branch, Basis)> { self.data.base.as_ref() }
    pub fn expires_at(&self) -> Option<Timestamp> { self.data.expires_at(self.cut.basis) }
    pub fn lineage(&self) -> Vec<BranchLineage> { self.index.lineage(self.cut.basis) }
    pub fn custody_inventories(&self) -> impl Iterator<Item = CustodyInventory> + '_ {
        self.index.custody_inventories(self.cut.basis).filter(move |_| !self.index.visibility_projection)
    }

    pub fn custody_placements(&self) -> impl Iterator<Item = CustodyPlacementEntry<'_>> + '_ {
        self.index.custody_placements(self.cut.basis).filter(move |_| !self.index.visibility_projection)
    }

    pub fn tables(&self) -> impl Iterator<Item = (&TableRef, &BranchTable)> {
        self.index.tables(self.cut.basis)
    }

    pub fn count_constraints(&self) -> impl Iterator<Item = (&TableRef, &str, &CountRange)> {
        self.tables().filter(move |_| !self.index.visibility_projection).flat_map(|(table, definition)| {
            definition.constraints.iter().map(move |(name, range)| (table, name.as_str(), range))
        })
    }

    pub fn grouped_constraints(&self) -> impl Iterator<Item = (&TableRef, &str, &GroupedCount)> {
        self.tables().filter(move |_| !self.index.visibility_projection).flat_map(|(table, definition)| {
            definition.grouped_constraints.iter().map(move |(name, rule)| (table, name.as_str(), rule))
        })
    }

    pub fn table(&self, table: &TableRef) -> Option<&BranchTable> {
        self.index.definition(table, self.cut.basis).filter(|definition| !definition.dropped)
    }

    pub fn index_version(&self, table: &TableRef, index: &IndexDefinition) -> Option<Basis> {
        self.index.catalog.get(table)?.iter().rev()
            .skip_while(|(basis, _)| *basis > self.cut.basis)
            .take_while(|(_, definition)| !definition.dropped && definition.indexes.get(&index.name) == Some(index))
            .last().map(|(basis, _)| *basis)
    }

    fn layers(&self) -> impl Iterator<Item = (Basis, &'a BranchData)> + use<'a> {
        let index = self.index;
        let layer = self.layer;
        std::iter::successors(Some((self.cut.basis, self.data)), move |(_, data)| {
            if layer == Layer::Local { return None; }
            let (parent, basis) = data.base.as_ref()?;
            Some((*basis, index.branches.get(parent)?.as_ref()))
        })
    }

    pub fn ids(&self, table: &TableRef) -> BTreeSet<&'a EntityId> {
        self.layers().filter_map(|(_, data)| data.tables.get(table))
            .flat_map(|rows| rows.events.keys()).collect()
    }

    pub fn row(&self, table: &TableRef, id: &EntityId, valid_at: Timestamp) -> Option<&'a BranchEvent> {
        let generation = self.table(table)?.generation;
        for (basis, data) in self.layers() {
            let Some(rows) = data.tables.get(table) else { continue; };
            if let Some(event) = rows.events.get(id).into_iter().flat_map(|events| events.iter().rev()).find_map(|(g, event)| {
                (*g == generation && event.cut.basis <= basis && event.valid.contains(valid_at)).then_some(event)
            }) {
                return event.image_at(valid_at).map(|_| event);
            }
            if rows.erased.contains_key(id) { return None; }
        }
        None
    }

    pub fn visible_row(&self, table: &TableRef, id: &EntityId, valid_at: Timestamp, labels: &BTreeSet<Vec<u8>>) -> Option<&'a BranchEvent> {
        let visibility = self.table(table)?.visibility.as_ref()?;
        let event = self.row(table, id, valid_at)?;
        let label = visibility.label(&event.image.as_ref()?.cells)?;
        labels.contains(label).then_some(event)
    }

    pub fn has_erased_point(&self, table: &TableRef, id: &EntityId) -> bool {
        self.layers().any(|(_, data)| data.tables.get(table)
            .is_some_and(|rows| rows.erased.contains_key(id)))
    }

    pub fn erased_after(&self, table: &TableRef, id: &EntityId, basis: Basis) -> bool {
        self.layers().any(|(_, data)| {
            data.tables.get(table).and_then(|rows| rows.erased.get(id)).is_some_and(|at| *at > basis)
        })
    }

    pub fn history(&self, table: &TableRef, id: &EntityId) -> Vec<&'a BranchEvent> {
        let Some(definition) = self.table(table) else { return Vec::new(); };
        let mut reversed = Vec::new();
        for (basis, data) in self.layers() {
            let Some(rows) = data.tables.get(table) else { continue; };
            reversed.extend(rows.events.get(id).into_iter().flat_map(|events| events.iter().rev())
                .filter(|(generation, event)| *generation == definition.generation && event.cut.basis <= basis)
                .map(|(_, event)| event));
            if rows.erased.contains_key(id) { break; }
        }
        reversed.reverse();
        reversed
    }

    pub fn has_retained_puts(&self, table: &TableRef) -> bool {
        let Some(definition) = self.table(table) else { return false; };
        self.layers().any(|(basis, data)| {
            data.tables.get(table).is_some_and(|rows| {
                rows.events.values().flat_map(|events| events.iter()).any(|(generation, event)| {
                    *generation == definition.generation && event.cut.basis <= basis && event.image.is_some()
                })
            })
        })
    }

    pub fn count_at_most(&self, range: &CountRange, default_at: Timestamp) -> Result<bool> {
        if range.lower.as_ref().zip(range.upper.as_ref()).is_some_and(|(lower, upper)| lower > upper) {
            return Err(Anomaly::incorrect("rtdb/invalid-range", "Index lower bound exceeds its upper bound"));
        }
        let index = self.table(&range.table).and_then(|table| table.indexes.get(&range.index))
            .ok_or_else(|| Anomaly::new(AnomalyCategory::NotFound, "rtdb/unknown-index", "Count guard names an unknown index"))?;
        if index.is_compound() && (range.lower.is_some() || range.upper.is_some()) {
            return Err(Anomaly::unsupported("rtdb/compound-index-range", "A compound index count needs an unbounded range"));
        }
        let at = range.valid_at.unwrap_or(default_at);
        let mut count = 0;
        for id in self.ids(&range.table) {
            let Some(event) = self.row(&range.table, id, at) else { continue; };
            let image = event.image_at(at).expect("visible row");
            let matches = if index.is_compound() {
                index.columns().all(|column| image.cells.get(column).is_some_and(|value| {
                    !matches!(value, Value::Null | Value::Undefined)
                }))
            } else {
                image.cells.get(&index.column).and_then(index_key).is_some_and(|key| {
                    range.lower.as_ref().is_none_or(|lower| key >= lower.as_slice())
                        && range.upper.as_ref().is_none_or(|upper| key < upper.as_slice())
                })
            };
            if matches {
                if count == range.n { return Ok(false); }
                count += 1;
            }
        }
        Ok(true)
    }

    pub fn grouped_count_at_most(&self, table: &TableRef, rule: &GroupedCount, default_at: Timestamp) -> Result<bool> {
        let at = rule.valid_at.unwrap_or(default_at);
        let mut counts = BTreeMap::<&[u8], u64>::new();
        let invalid = || Anomaly::incorrect("rtdb/constraint-key", "A qualifying row needs a binary group key of the declared width");
        for id in self.ids(table) {
            let Some(image) = self.row(table, id, at).and_then(|event| event.image_at(at)) else { continue; };
            if !rule.equals.iter().all(|(column, expected)| binary_cell(image.cells.get(column)) == Some(expected.as_slice())) {
                continue;
            }
            let key = binary_cell(image.cells.get(&rule.column)).ok_or_else(invalid)?;
            let width = rule.prefix_bytes.map_or(key.len(), |width| width as usize);
            let key = key.get(..width).ok_or_else(invalid)?;
            let count = counts.entry(key).or_default();
            if *count == rule.n { return Ok(false); }
            *count += 1;
        }
        Ok(true)
    }
}

fn binary_cell(value: Option<&Value>) -> Option<&[u8]> {
    match value {
        Some(Value::Binary(bytes) | Value::FixedSizeBinary(bytes)) => Some(bytes),
        _ => None,
    }
}

pub fn index_key(value: &Value) -> Option<&[u8]> {
    match value {
        Value::Binary(key) | Value::FixedSizeBinary(key) => Some(key),
        Value::Utf8(key) => Some(key.as_bytes()),
        _ => None,
    }
}

fn classic_identity() -> Anomaly {
    Anomaly::unsupported("rtdb/classic-identity", "The stored Classic identity cannot be represented by one branch row")
}

fn classic_cut_expired() -> Anomaly {
    Anomaly::new(AnomalyCategory::NotFound, "rtdb/cursor-expired", "The requested cut predates the retained Classic checkpoint")
}

fn unknown_branch(branch: &str) -> Anomaly {
    Anomaly::new(AnomalyCategory::NotFound, "rtdb/unknown-branch", format!("Unknown or expired branch {branch}"))
}

/// Maps serialize as a sequence of `(key, value)` pairs, the checkpoint shape these fields
/// had as `BTreeMap`s; the representation change does not change the encoding.
mod entries {
    use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error};

    use super::Map;

    pub fn serialize<K: Serialize + Ord + Clone, V: Serialize + Clone, S: Serializer>(
        map: &Map<K, V>, serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_seq(map.iter())
    }

    pub fn deserialize<'de, K, V, D>(deserializer: D) -> std::result::Result<Map<K, V>, D::Error>
    where
        K: Deserialize<'de> + Ord + Clone,
        V: Deserialize<'de> + Clone,
        D: Deserializer<'de>,
    {
        let mut map = Map::new();
        for (key, value) in Vec::<(K, V)>::deserialize(deserializer)? {
            if map.insert(key, value).is_some() {
                return Err(D::Error::custom("duplicate checkpoint entry"));
            }
        }
        Ok(map)
    }
}

#[cfg(test)]
mod readback_tests {
    use super::*;

    #[test]
    fn retained_local_point_requires_literal_write_after_branch_drop() -> Result<()> {
        let incarnation = [1; 16];
        let mut index = BranchIndex::new(incarnation);
        let cut = |position| BranchCut { basis: Basis::new(incarnation, position), system_time: Timestamp(position as i64) };
        let table = TableRef::new("app", "member_mappings");
        let id = EntityId::String("member".into());
        let created = cut(1);
        index.advance(created)?;
        index.apply(BranchEffect::Fork {
            child: "proposal".into(), from: (MAIN_BRANCH.into(), cut(0).basis), writable: true, expires_at: None,
        })?;
        let authored = cut(2);
        index.advance(authored)?;
        index.apply(BranchEffect::Write {
            branch: "proposal".into(), table: table.clone(),
            event: BranchEvent {
                id: id.clone(), valid: Interval::new(authored.system_time, Timestamp::MAX)?,
                cut: authored, written: WrittenVersion::Branch,
                image: Some(Arc::new(RowImage {
                    cells: StructFields::default(), expires_at: None, custody_group: None, custody_payload: None,
                })),
            },
        })?;
        let dropped = cut(3);
        index.advance(dropped)?;
        index.apply(BranchEffect::Drop { branch: "proposal".into() })?;

        assert!(index.view("proposal", At::Exactly(authored.basis), Layer::Merged, dropped.system_time).is_err());
        assert_eq!(index.receipt_written_local_point("proposal", created.basis, authored.basis, &table, &id)?
            .and_then(BranchEvent::written_basis), Some(authored.basis));
        assert!(index.receipt_written_local_point("proposal", cut(0).basis, authored.basis, &table, &id).is_err());
        assert!(index.receipt_written_local_point("proposal", created.basis, created.basis, &table, &id)?.is_none());

        index.advance(cut(4))?;
        index.apply(BranchEffect::Erase { branch: None, table: table.clone(), id: id.clone() })?;
        assert!(index.receipt_written_local_point("proposal", created.basis, authored.basis, &table, &id).is_err());
        Ok(())
    }
}
