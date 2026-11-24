use super::*;

#[derive(Debug)]
pub struct VisibleHistory {
    index: BranchIndex,
    changes: Vec<VisibleChangeGroup>,
    source_bases: BTreeMap<Basis, Basis>,
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
}

#[derive(Default)]
struct Touched<'a> {
    rows: BTreeSet<(&'a TableRef, &'a EntityId)>,
    declarations: BTreeSet<&'a TableRef>,
}

impl VisibleHistory {
    pub fn index(&self) -> &BranchIndex {
        &self.index
    }

    pub fn changes(&self) -> &[VisibleChangeGroup] {
        &self.changes
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
}

impl BranchIndex {
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
        let mut source_bases = BTreeMap::from([(
            index.head.basis,
            Basis::new(self.head.basis.incarnation(), 0),
        )]);
        let mut changes = Vec::new();

        let mut previous =
            self.main_visibility_view(main, Basis::new(self.head.basis.incarnation(), 0))?;
        for (basis, touched) in touched {
            let current = self.main_visibility_view(main, basis)?;
            let rows = touched
                .rows
                .into_iter()
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
            previous = current;
            if rows.is_empty() && declarations.is_empty() {
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
                index.apply(BranchEffect::Write {
                    branch: MAIN_BRANCH.into(),
                    table: row.table.clone(),
                    event: BranchEvent {
                        cut,
                        id: row.id.clone(),
                        valid: row.valid,
                        image: row.after.clone(),
                    },
                })?;
            }
            source_bases.insert(cut.basis, basis);
            changes.push(VisibleChangeGroup { cut, rows });
        }
        Ok(VisibleHistory {
            index,
            changes,
            source_bases,
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
            if rows.is_some_and(|rows| !rows.erased.is_empty()) {
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
                        .flat_map(|rows| rows.events.keys())
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
    let boundaries = before
        .history(table, id)
        .into_iter()
        .chain(after.history(table, id))
        .flat_map(|event| {
            let expires = event
                .image
                .as_ref()
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
    for bounds in boundaries.windows(2) {
        let prior = before.visible_row(table, id, bounds[0], labels);
        let next = after.visible_row(table, id, bounds[0], labels);
        if same_version(prior, next) {
            continue;
        }
        let project = |event: &BranchEvent| {
            event.image.as_ref().map(|image| {
                if image.expires_at.is_none() {
                    return Arc::clone(image);
                }
                Arc::new(RowImage {
                    cells: image.cells.clone(),
                    expires_at: None,
                })
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
                before: prior.and_then(project),
                after: next.and_then(project),
            });
        }
        last_pair = (prior, next);
    }
    Ok(changes)
}

fn same_version(left: Option<&BranchEvent>, right: Option<&BranchEvent>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => std::ptr::eq(left, right),
        (None, None) => true,
        _ => false,
    }
}
