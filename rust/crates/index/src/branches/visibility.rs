use super::*;

#[derive(Debug)]
pub struct VisibleHistory {
    index: BranchIndex,
    changes: Vec<VisibleChangeGroup>,
    source_bases: BTreeMap<Basis, Basis>,
    contributors: BTreeMap<(CustodyGroupId, [u8; 32]), Option<[u8; 32]>>,
}

#[derive(Debug, PartialEq)]
pub struct VisibleChangeGroup {
    pub cut: BranchCut,
    pub rows: Vec<VisibleChange>,
}

#[derive(Debug, PartialEq)]
pub struct VisibleChange {
    pub table: TableRef,
    pub id: EntityId,
    pub valid: Interval,
    pub before: Option<Arc<RowImage>>,
    pub after: Option<Arc<RowImage>>,
    pub erased_written_basis: Option<Basis>,
    pub erased: bool,
    unpositioned: bool,
    after_contributor: Option<(CustodyGroupId, [u8; 32])>,
}

#[derive(Default)]
struct Touched<'a> {
    rows: BTreeSet<(&'a TableRef, &'a EntityId)>,
    declarations: BTreeSet<&'a TableRef>,
    revocations: BTreeSet<CustodyGroupId>,
    erasures: BTreeSet<(&'a TableRef, &'a EntityId)>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct VisibilityErasure {
    at: Basis,
    events: Vec<(u64, BranchEvent)>,
    retired: BTreeMap<usize, custody::RetiredLabel>,
}

impl VisibilityErasure {
    pub(super) fn rebase(&mut self, rebase: impl Fn(Basis) -> Basis) {
        self.at = rebase(self.at);
        for (_, event) in &mut self.events {
            event.cut.basis = rebase(event.cut.basis);
        }
        for label in self.retired.values_mut() {
            label.revoked = rebase(label.revoked);
        }
    }
}

impl VisibleHistory {
    pub fn index(&self) -> &BranchIndex {
        &self.index
    }

    pub fn changes(&self) -> &[VisibleChangeGroup] {
        &self.changes
    }

    pub fn custody_contributor(&self, image: &RowImage) -> Option<(CustodyGroupId, [u8; 32])> {
        let group = image.custody_group?;
        let binding = image.custody_payload.as_ref()?.binding;
        Some((group, self.contributors.get(&(group, binding)).copied().flatten()?))
    }

    pub fn source_basis(&self, basis: Basis) -> Result<Basis> {
        self.index.exact(basis)?;
        self.source_bases.get(&basis).copied().ok_or_else(|| {
            Anomaly::fault(
                "rtdb/visibility-basis",
                "The visible cut has no source basis",
            )
        })
    }

    pub fn projected_cut(&self, source: Basis) -> Result<BranchCut> {
        let (&basis, _) = self.source_bases.iter().rev().find(|(_, original)| {
            original.incarnation() == source.incarnation() && **original <= source
        }).ok_or_else(|| Anomaly::incorrect("rtdb/foreign-basis", "The basis belongs to another database incarnation"))?;
        self.index.exact(basis)
    }
}

impl BranchIndex {
    pub(super) fn record_visibility_erasure(&mut self, table: &TableRef, id: &EntityId, at: Basis) {
        if self.catalog.get(table).is_none_or(|definitions| !definitions.iter().any(|(_, definition)| definition.visibility.is_some())) {
            return;
        }
        let rows = self.branches[MAIN_BRANCH].tables.get(table);
        let events = rows.and_then(|rows| rows.events.get(id));
        let retired = rows.and_then(|rows| rows.retired_labels.get(id));
        let events = events.into_iter().flat_map(|events| events.iter()).map(|(generation, event)| {
            let image = event.image.as_ref().and_then(|image| {
                let visibility = self.definition(table, event.cut.basis)?.visibility.as_ref()?;
                let label = visibility.label(&image.cells)?;
                Some(Arc::new(RowImage {
                    cells: StructFields::from([(visibility.column.clone(), Value::Binary(label.to_vec()))]),
                    expires_at: image.expires_at,
                    custody_group: image.custody_group,
                    custody_payload: None,
                }))
            });
            (*generation, BranchEvent { image, ..event.clone() })
        }).collect();
        let retired = retired.map(|retired| retired.as_ref().clone()).unwrap_or_default();
        let main = Arc::make_mut(self.branches.get_mut(MAIN_BRANCH).expect("main exists"));
        let rows = Arc::make_mut(main.tables.entry(table.clone()).or_default());
        Arc::make_mut(rows.visibility_erasures.entry(id.clone()).or_default())
            .push(VisibilityErasure { at, events, retired });
    }

    pub fn project_main_visibility(&self, scope: &LabelScope) -> Result<VisibleHistory> {
        let incarnation = scope.main_read_incarnation(self.head.basis.incarnation());
        if incarnation == self.head.basis.incarnation() {
            return Err(Anomaly::fault(
                "rtdb/visibility-basis",
                "A scoped cut requires its own incarnation namespace",
            ));
        }
        let main = &self.branches[MAIN_BRANCH];
        let touched = self.visibility_touches(main)?;
        let mut index = BranchIndex::with_initial_time(incarnation, Timestamp(0));
        index.visibility_projection = true;
        let mut source_bases = BTreeMap::from([(
            index.head.basis,
            Basis::new(self.head.basis.incarnation(), 0),
        )]);
        let mut changes: Vec<VisibleChangeGroup> = Vec::new();
        let mut contributors = BTreeMap::new();

        let mut previous =
            self.main_visibility_view(main, Basis::new(self.head.basis.incarnation(), 0))?;
        for (basis, touched) in touched {
            let current = self.main_visibility_view(main, basis)?;
            let erasures = touched.erasures.into_iter().filter(|(table, id)| {
                index.branches[MAIN_BRANCH].tables.get(*table).is_some_and(|rows| rows.events.contains_key(*id))
            }).collect::<BTreeSet<_>>();
            let mut rows = touched
                .rows
                .into_iter()
                .filter(|address| !erasures.contains(address))
                .map(|(table, id)| visible_changes(&previous, &current, table, id, &scope.read))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
            let declarations = touched
                .declarations
                .into_iter()
                .filter_map(|table| {
                    let old = self
                        .definition(table, previous.cut.basis)
                        .filter(|def| def.visibility.is_some());
                    let new = self
                        .definition(table, basis)
                        .filter(|def| def.visibility.is_some())?;
                    (old != Some(new)).then_some((table, old, new))
                })
                .collect::<Vec<_>>();
            let revocations = touched.revocations.into_iter().filter(|group| {
                index.custody_group_live(*group).unwrap_or(false)
            }).collect::<Vec<_>>();
            previous = current;
            if rows.is_empty() && declarations.is_empty() && revocations.is_empty() && erasures.is_empty() {
                continue;
            }

            let system_time = index.head.system_time.next()?;
            let position = system_time.0 as u64;
            let cut = BranchCut {
                basis: Basis::new(incarnation, position),
                system_time,
            };
            index.advance(cut)?;
            for (table, old, new) in declarations {
                let prior = index.definition(table, cut.basis);
                let generation = match (old, prior) {
                    (Some(old), Some(prior)) if old.generation == new.generation => {
                        prior.generation
                    }
                    _ => position,
                };
                let definition = BranchTable {
                    generation,
                    ..new.clone()
                };
                index.apply(BranchEffect::Declare {
                    table: table.clone(),
                    definition,
                })?;
            }
            for row in &rows {
                if index
                    .definition(&row.table, cut.basis)
                    .is_some_and(|table| table.dropped)
                {
                    continue;
                }
                if let Some(group) = row.after.as_ref().and_then(|image| image.custody_group)
                    && index.custody_group_live(group).is_err()
                {
                    index.create_custody_group(group)?;
                }
                if let Some(image) = &row.after
                    && let (Some(group), Some(payload)) = (image.custody_group, &image.custody_payload)
                {
                    let occurrence = row.after_contributor.map(|(_, commitment)| commitment);
                    contributors.entry((group, payload.binding)).and_modify(|prior| {
                        if *prior != occurrence { *prior = None; }
                    }).or_insert(occurrence);
                }
                index.apply(BranchEffect::Write {
                    branch: MAIN_BRANCH.into(),
                    table: row.table.clone(),
                    event: BranchEvent {
                        cut,
                        id: row.id.clone(),
                        valid: row.valid,
                        image: row.after.clone(),
                        written: if row.unpositioned { WrittenVersion::Unpositioned } else { WrittenVersion::Branch },
                    },
                })?;
            }
            for group in revocations {
                index.revoke_custody_group(group)?;
                for notice in index.custody_revocation_writes(group, cut)? {
                    let valid = notice.valid.ok_or_else(|| {
                        Anomaly::fault("rtdb/visibility-erasure", "A custody erasure needs its written validity")
                    })?;
                    rows.push(VisibleChange {
                        table: notice.table, id: notice.id, valid, before: None, after: None,
                        erased_written_basis: notice.erased_written_basis, erased: true, unpositioned: false, after_contributor: None,
                    });
                }
            }
            for (table, id) in erasures {
                for row in changes.iter_mut().flat_map(|group| &mut group.rows)
                    .filter(|row| &row.table == table && &row.id == id)
                {
                    row.before = None;
                    row.after = None;
                }
                index.apply(BranchEffect::Erase { branch: Some(MAIN_BRANCH.into()), table: table.clone(), id: id.clone() })?;
                rows.push(VisibleChange {
                    table: table.clone(), id: id.clone(), valid: Interval::ALL, before: None, after: None,
                    erased_written_basis: None, erased: true, unpositioned: false, after_contributor: None,
                });
            }
            source_bases.insert(cut.basis, basis);
            changes.push(VisibleChangeGroup { cut, rows });
        }
        Ok(VisibleHistory {
            index,
            changes,
            source_bases,
            contributors,
        })
    }

    fn main_visibility_view<'a>(
        &'a self,
        data: &'a BranchData,
        basis: Basis,
    ) -> Result<BranchView<'a>> {
        let cut = self.exact(basis)?;
        Ok(BranchView {
            index: self,
            branch: MAIN_BRANCH,
            cut,
            layer: Layer::Merged,
            data,
        })
    }

    fn visibility_touches<'a>(
        &'a self,
        main: &'a BranchData,
    ) -> Result<BTreeMap<Basis, Touched<'a>>> {
        let mut touched = BTreeMap::<_, Touched<'a>>::new();
        for (table, definitions) in self.catalog.iter() {
            if !definitions.iter().any(|(_, def)| def.visibility.is_some()) {
                continue;
            }
            let rows = main.tables.get(table);
            if rows.is_some_and(|rows| rows.erased.iter().any(|(id, at)| {
                rows.visibility_erasures.get(id).and_then(|history| history.last()).is_none_or(|entry| entry.at != *at)
            })) {
                return Err(Anomaly::unsupported(
                    "rtdb/scoped-erasure-history",
                    "The scoped history requires a retained visibility projection after erasure",
                ));
            }
            for (basis, declaration) in definitions.iter() {
                if declaration.visibility.is_none() {
                    continue;
                }
                let change = touched.entry(*basis).or_default();
                change.declarations.insert(table);
                change.rows.extend(
                    rows.into_iter()
                        .flat_map(|rows| rows.events.keys().chain(rows.visibility_erasures.keys()))
                        .map(|id| (table, id)),
                );
            }
            for (id, events) in rows.into_iter().flat_map(|rows| &rows.events) {
                for (_, event) in events.iter() {
                    touched
                        .entry(event.cut.basis)
                        .or_default()
                        .rows
                        .insert((table, id));
                }
            }
            for (id, history) in rows.into_iter().flat_map(|rows| &rows.visibility_erasures) {
                for erasure in history.iter() {
                    let change = touched.entry(erasure.at).or_default();
                    change.rows.insert((table, id));
                    change.erasures.insert((table, id));
                    for (_, event) in &erasure.events {
                        touched.entry(event.cut.basis).or_default().rows.insert((table, id));
                    }
                    for label in erasure.retired.values() {
                        let change = touched.entry(label.revoked).or_default();
                        change.rows.insert((table, id));
                        change.revocations.extend(label.image.custody_group);
                    }
                }
            }
            for (id, labels) in rows.into_iter().flat_map(|rows| &rows.retired_labels) {
                for label in labels.values() {
                    let change = touched.entry(label.revoked).or_default();
                    change.rows.insert((table, id));
                    change.revocations.extend(label.image.custody_group);
                }
            }
        }
        Ok(touched)
    }
}

fn visible_changes(
    before: &BranchView<'_>,
    after: &BranchView<'_>,
    table: &TableRef,
    id: &EntityId,
    labels: &BTreeSet<Vec<u8>>,
) -> Result<Vec<VisibleChange>> {
    let prior_images = visibility_images(before, table, id);
    let next_images = visibility_images(after, table, id);
    let boundaries = prior_images.iter().chain(&next_images)
        .flat_map(|(event, image)| {
            let expires = image
                .and_then(|image| image.expires_at)
                .filter(|at| event.valid.contains(*at));
            [
                Some(event.valid.lower()),
                expires,
                Some(event.valid.upper()),
            ]
            .into_iter()
            .flatten()
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut changes: Vec<VisibleChange> = Vec::new();
    let mut last_pair = (None, None);
    let prior_label = before.table(table).and_then(|definition| definition.visibility.as_ref());
    let next_label = after.table(table).and_then(|definition| definition.visibility.as_ref());
    for bounds in boundaries.windows(2) {
        let prior = visible_version(&prior_images, bounds[0], prior_label, labels);
        let next = visible_version(&next_images, bounds[0], next_label, labels);
        if same_version(prior, next) {
            continue;
        }
        let project = |(_, image): VisibleVersion<'_>| {
            if image.expires_at.is_none() {
                return Arc::clone(image);
            }
            Arc::new(RowImage {
                cells: image.cells.clone(),
                expires_at: None,
                custody_group: image.custody_group,
                custody_payload: image.custody_payload.clone(),
            })
        };
        if let Some(last) = changes.last_mut()
            && last.valid.upper() == bounds[0]
            && same_version(last_pair.0, prior)
            && same_version(last_pair.1, next)
        {
            last.valid = Interval::new(last.valid.lower(), bounds[1])?;
        } else {
            changes.push(VisibleChange {
                table: table.clone(),
                id: id.clone(),
                valid: Interval::new(bounds[0], bounds[1])?,
                before: prior.map(project),
                after: next.map(project),
                erased_written_basis: None,
                erased: false,
                unpositioned: next.is_some_and(|(event, _)| event.written_basis().is_none()),
                after_contributor: next.and_then(|(event, _)| after.custody_event_contributor(table, event)),
            });
        }
        last_pair = (prior, next);
    }
    Ok(changes)
}

type VisibleVersion<'a> = (&'a BranchEvent, &'a Arc<RowImage>);

fn visibility_images<'a>(
    view: &BranchView<'a>, table: &TableRef, id: &EntityId,
) -> Vec<(&'a BranchEvent, Option<&'a Arc<RowImage>>)> {
    let Some(definition) = view.table(table) else { return Vec::new(); };
    let Some(rows) = view.data.tables.get(table) else { return Vec::new(); };
    let erased = rows.visibility_erasures.get(id)
        .and_then(|history| history.iter().find(|erase| erase.at > view.cut.basis));
    let (events, retired_labels) = match erased {
        Some(erasure) => (Some(&erasure.events), Some(&erasure.retired)),
        None => (rows.events.get(id).map(AsRef::as_ref), rows.retired_labels.get(id).map(AsRef::as_ref)),
    };
    events.into_iter().flat_map(|events| events.iter().enumerate())
        .filter(|(_, (generation, event))| *generation == definition.generation && event.cut.basis <= view.cut.basis)
        .map(|(ordinal, (_, event))| {
            let image = event.image.as_ref().or_else(|| {
                let retired = retired_labels?.get(&ordinal)?;
                (view.cut.basis < retired.revoked).then_some(&retired.image)
            });
            (event, image)
        }).collect()
}

fn visible_version<'a>(
    images: &[(&'a BranchEvent, Option<&'a Arc<RowImage>>)],
    at: Timestamp, visibility: Option<&Visibility>, labels: &BTreeSet<Vec<u8>>,
) -> Option<VisibleVersion<'a>> {
    let (event, image) = images.iter().rev().find(|(event, _)| event.valid.contains(at))?;
    let image = (*image)?;
    if image.expires_at.is_some_and(|until| at >= until) { return None; }
    let label = visibility?.label(&image.cells)?;
    labels.contains(label).then_some((*event, image))
}

fn same_version(left: Option<VisibleVersion<'_>>, right: Option<VisibleVersion<'_>>) -> bool {
    match (left, right) {
        (Some((left, _)), Some((right, _))) => std::ptr::eq(left, right),
        (None, None) => true,
        _ => false,
    }
}
