use rtdb_index::branches::BranchEvent;

use super::*;

impl Candidate<'_> {
    pub(super) fn execute(&mut self, ops: &[Op], request_key: &[u8]) -> Result<()> {
        let cut = self.index.head();
        for op in ops {
            match op {
                Op::Put { branch, table, rows, if_absent } => {
                    self.require_write_branch(branch)?;
                    self.index.writable(branch)?;
                    let definition = self.write_definition(table)?;
                    for row in rows {
                        let (valid, image) = self.put_image(branch, table, &definition, row)?;
                        if *if_absent && self.present(branch, table, &row.id, valid)? {
                            return Err(conflict("rtdb/already-present", "The put overlaps an existing row"));
                        }
                        self.write(branch, table, row.id.clone(), valid, Some(image))?;
                    }
                }
                Op::PlaceCustodyRow { branch, table, row, placement } => {
                    self.require_unrestricted()?;
                    self.index.writable(branch)?;
                    let definition = self.write_definition(table)?;
                    let (valid, image) = self.put_image(branch, table, &definition, row)?;
                    let (Some(group), Some(payload), Some(commitments)) = (image.custody_group, &image.custody_payload, placement.commitments) else {
                        return Err(Anomaly::incorrect("rtdb/custody-placement", "Custody placement requires a sealed carrier and its commitments"));
                    };
                    let occurrence = CustodyOccurrence {
                        placement_id: placement.placement_id,
                        target: CustodyTarget { branch: branch.clone(), table: table.clone(), id: row.id.clone(), valid },
                        written: cut.basis,
                        receipt_key: request_key.to_vec(),
                        ordinal: self.index.next_custody_ordinal(),
                        carrier_sha256: commitments.carrier_sha256,
                        envelope_sha256: Sha256::digest(&payload.envelope).into(),
                        reservation: placement.reservation,
                        link_sha256: commitments.link_sha256,
                    };
                    self.index.check_custody_occurrence(group, &occurrence)?;
                    self.write(branch, table, row.id.clone(), valid, Some(image))?;
                    self.push(BranchEffect::ActivateCustodyOccurrence { group, occurrence })?;
                }
                Op::Delete { branch, table, ids, valid } => {
                    self.require_write_branch(branch)?;
                    self.index.writable(branch)?;
                    let definition = self.write_definition(table)?;
                    if definition.visibility.is_some() && !valid.is_some_and(ValidTime::is_literal) {
                        return Err(labelled_time_error());
                    }
                    let valid = effective_valid(*valid, cut.system_time)?;
                    for id in ids {
                        self.authorize_write(branch, table, id, valid, &definition, None)?;
                        self.write(branch, table, id.clone(), valid, None)?;
                    }
                }
                Op::Erase { branch, table, ids } => {
                    match branch {
                        Some(branch) => self.require_write_branch(branch)?,
                        None => self.require_unrestricted()?,
                    }
                    let definition = self.write_definition(table)?;
                    if let Some(branch) = branch { self.view(branch, Layer::Local)?; }
                    for id in ids {
                        if let Some(visibility) = &definition.visibility {
                            let mut labels = self.index.stored_labels(table, branch.as_deref(), id, visibility);
                            if let Some(branch) = branch {
                                labels.extend(self.view(branch, Layer::Merged)?.history(table, id).iter()
                                    .filter_map(|event| visibility.label(&event.image.as_ref()?.cells))
                                    .map(<[u8]>::to_vec));
                            }
                            self.authorize_labels(&definition, labels)?;
                        }
                        self.push(BranchEffect::Erase { branch: branch.clone(), table: table.clone(), id: id.clone() })?;
                    }
                }
                Op::Declare { table, change } => {
                    self.require_unrestricted()?;
                    self.declare(table, change)?;
                }
                Op::Fork { child, from, writable, expires_at } => {
                    self.require_unrestricted()?;
                    if !valid_branch_name(child) {
                        return Err(Anomaly::incorrect("rtdb/invalid-branch", "Invalid child branch name"));
                    }
                    if self.index.has_branch(child) {
                        return Err(conflict("rtdb/branch-exists", "The child branch name has already been used"));
                    }
                    if from.1 >= cut.basis && from.1.incarnation() == cut.basis.incarnation() {
                        return Err(Anomaly::incorrect("rtdb/future-fork", "A branch must fork from a committed basis"));
                    }
                    self.index.view(&from.0, At::Exactly(from.1), Layer::Merged, cut.system_time)?;
                    if let Some(expires_at) = expires_at { validate_expiry(*expires_at, cut.system_time)?; }
                    self.push(BranchEffect::Fork { child: child.clone(), from: from.clone(), writable: *writable, expires_at: *expires_at })?;
                    self.touched.insert(child.clone());
                }
                Op::Renew { branch, expires_at } => {
                    self.require_unrestricted()?;
                    self.require_child(branch)?;
                    validate_expiry(*expires_at, cut.system_time)?;
                    self.push(BranchEffect::Renew { branch: branch.clone(), expires_at: *expires_at })?;
                }
                Op::Merge { from, into } => {
                    self.require_unrestricted()?;
                    self.merge(from, into)?;
                }
                Op::DropBranch { branch } => {
                    self.require_unrestricted()?;
                    self.require_child(branch)?;
                    self.push(BranchEffect::Drop { branch: branch.clone() })?;
                }
                Op::CreateCustodyGroup { group } => {
                    self.require_unrestricted()?;
                    if *group == [0; 16] {
                        return Err(Anomaly::incorrect("rtdb/invalid-custody-group", "A custody group needs a nonzero identity"));
                    }
                    match self.index.custody_group_live(*group) {
                        Ok(true) => (),
                        Ok(false) => return Err(erased_custody_group()),
                        Err(error) if error.category == AnomalyCategory::NotFound => {
                            self.push(BranchEffect::CreateCustodyGroup { group: *group })?;
                        }
                        Err(error) => return Err(error),
                    }
                }
                Op::RevokeCustodyGroup { group, inventory_sha256 } => {
                    self.require_unrestricted()?;
                    let live = self.index.custody_group_live(*group)?;
                    let inventory = self.index.custody_inventory(*group).expect("known group has an inventory");
                    if inventory.sha256 != *inventory_sha256 {
                        return Err(conflict("rtdb/custody-inventory-changed", "The custody inventory changed after erasure was planned"));
                    }
                    if live { self.push(BranchEffect::RevokeCustodyGroup { group: *group })?; }
                }
                Op::ReserveCustodyPlacement { group, reservation } => {
                    self.require_unrestricted()?;
                    if self.index.check_custody_reservation(*group, reservation)? {
                        self.index.writable(&reservation.target.branch)?;
                        self.write_definition(&reservation.target.table)?;
                        self.push(BranchEffect::ReserveCustodyPlacement { group: *group, reservation: reservation.clone() })?;
                    }
                }
                Op::CustodyRelease(release) => {
                    self.require_unrestricted()?;
                    if self.index.check_custody_release(*release, cut.system_time)? {
                        self.push(BranchEffect::CustodyRelease(*release))?;
                    }
                }
                Op::AdmitCustodyResponse(admission) => {
                    self.require_unrestricted()?;
                    // The part receipt records admission while the existing promoted holds keep the drain charged.
                    self.index.check_custody_response_admission(admission)?;
                }
                Op::NativeCustodyTransfer { custody, destination } => {
                    self.require_unrestricted()?;
                    self.push(BranchEffect::NativeCustodyTransfer { custody: custody.clone(), destination: *destination })?;
                }
            }
        }
        Ok(())
    }

    fn put_image(&mut self, branch: &str, table: &TableRef, definition: &BranchTable, row: &Row) -> Result<(Interval, Arc<RowImage>)> {
        let cut = self.index.head();
        if let Some(group) = row.custody_group {
            self.require_unrestricted()?;
            if !self.index.custody_group_live(group)? {
                return Err(erased_custody_group());
            }
        }
        if row.protected_columns.is_empty() != row.custody_payload.is_none()
            || row.custody_payload.is_some() && row.custody_group.is_none()
            || row.protected_columns.iter().any(|column| {
                row.cells.contains_key(column)
                    || definition.visibility.as_ref().is_some_and(|visibility| &visibility.column == column)
            })
        {
            return Err(Anomaly::incorrect("rtdb/custody-payload", "Protected columns must be sealed and distinct from clear columns and the visibility label"));
        }
        for column in &row.protected_columns { validate_column(column)?; }
        if let Some(visibility) = &definition.visibility
            && !matches!(row.cells.get(&visibility.column), Some(Cell::Value(_)))
        {
            return Err(invalid_label());
        }
        if definition.visibility.is_some() && (!row.valid.is_some_and(ValidTime::is_literal)
            || row.cells.values().any(|cell| !matches!(cell, Cell::Value(_))))
        {
            return Err(labelled_time_error());
        }
        let valid = effective_valid(row.valid, cut.system_time)?;
        let resolve = |(name, value): (&String, &Cell)| (name.clone(), value.resolve(cut));
        let mut cells = if row.cells.keys().is_sorted() {
            row.cells.iter().map(resolve).collect::<StructFields<_>>()
        } else {
            row.cells.sorted().into_iter().map(resolve).collect()
        };
        if cells.contains_key("_id") {
            return Err(Anomaly::incorrect("rtdb/reserved-field", "The put supplies its ID separately"));
        }
        validate_cells(definition, &cells)?;
        cells.insert("_id".into(), row.id.as_value());
        let expires_at = definition.ttl_micros.map(|ttl| expiry(cut.system_time, ttl)).transpose()?;
        self.authorize_write(branch, table, &row.id, valid, definition, Some(&cells))?;
        Ok((valid, Arc::new(RowImage {
            cells, expires_at, custody_group: row.custody_group, custody_payload: row.custody_payload.clone(),
        })))
    }

    fn require_child(&self, branch: &str) -> Result<()> {
        if branch == MAIN_BRANCH {
            return Err(Anomaly::incorrect("rtdb/main-branch", "The main branch has a permanent lifetime"));
        }
        self.view(branch, Layer::Local).map(drop)
    }

    fn view<'a>(&'a self, branch: &'a str, layer: Layer) -> Result<BranchView<'a>> {
        self.index.view(branch, At::Latest, layer, self.index.head().system_time)
    }

    fn write(&mut self, branch: &str, table: &TableRef, id: EntityId, valid: Interval, image: Option<Arc<RowImage>>) -> Result<()> {
        self.push(BranchEffect::Write {
            branch: branch.into(), table: table.clone(),
            event: BranchEvent { cut: self.index.head(), id, valid, image, written: Default::default() },
        })?;
        self.touched.insert(branch.into());
        Ok(())
    }

    fn present(&self, branch: &str, table: &TableRef, id: &EntityId, valid: Interval) -> Result<bool> {
        let view = self.view(branch, Layer::Merged)?;
        let history = view.history(table, id);
        Ok(endpoints(&history, valid).into_iter().any(|at| {
            at < valid.upper() && view.row(table, id, at).is_some()
        }))
    }

    fn require_unrestricted(&self) -> Result<()> {
        if self.scope.is_some() { return Err(write_scope_error()); }
        Ok(())
    }

    fn require_write_branch(&self, branch: &str) -> Result<()> {
        if self.scope.is_some() && branch != MAIN_BRANCH {
            return Err(Anomaly::unsupported("rtdb/scoped-branch", "Scoped row writes select the main branch"));
        }
        Ok(())
    }

    fn authorize_write(&mut self, branch: &str, table: &TableRef, id: &EntityId, valid: Interval,
                       definition: &BranchTable, cells: Option<&StructFields<Value>>) -> Result<()> {
        let Some(visibility) = &definition.visibility else { return Ok(()); };
        let view = self.view(branch, Layer::Merged)?;
        let history = view.history(table, id);
        let mut labels = endpoints(&history, valid).into_iter().filter(|at| *at < valid.upper())
            .filter_map(|at| view.row(table, id, at))
            .filter_map(|event| visibility.label(&event.image.as_ref()?.cells))
            .map(<[u8]>::to_vec).collect::<BTreeSet<_>>();
        if let Some(cells) = cells {
            labels.insert(visibility.label(cells).ok_or_else(invalid_label)?.to_vec());
        }
        self.authorize_labels(definition, labels)
    }

    fn authorize_labels(&mut self, definition: &BranchTable, labels: BTreeSet<Vec<u8>>) -> Result<()> {
        if self.scope.is_some_and(|scope| labels.is_empty() || !labels.is_subset(&scope.write)) {
            return Err(write_scope_error());
        }
        self.affected_labels.extend(labels);
        self.single_label |= definition.single_label_per_tx;
        if self.single_label && self.affected_labels.len() > 1 {
            return Err(Anomaly::incorrect("rtdb/mixed-visibility-labels", "The transaction must use a single visibility label"));
        }
        Ok(())
    }

    fn merge(&mut self, from: &str, into: &str) -> Result<()> {
        if from == into || from == MAIN_BRANCH {
            return Err(Anomaly::incorrect("rtdb/invalid-merge", "Merge requires a child source and a different target"));
        }
        self.index.writable(into)?;
        let source = self.view(from, Layer::Local)?;
        let target = self.view(into, Layer::Merged)?;
        let (_, base) = source.base().expect("child source");
        let mut effects = Vec::new();
        for (table, definition) in source.tables() {
            for id in source.ids(table) {
                let history = source.history(table, id);
                let target_history = target.history(table, id);
                let boundaries = endpoints(&history, Interval::ALL);
                for bounds in boundaries.windows(2) {
                    let valid = Interval::new(bounds[0], bounds[1])?;
                    let Some(event) = history.iter().rev().find(|event| event.valid.contains(valid.lower())) else {
                        continue;
                    };
                    if target_history.iter().any(|event| {
                        event.cut.basis > *base && event.cut != target.cut() && event.valid.intersects(valid)
                    }) || target.erased_after(table, id, *base) {
                        return Err(conflict("rtdb/merge-conflict", "The target changed a valid-time region written by the source"));
                    }
                    let image = event.image.clone().filter(|_| event.image_at(valid.lower()).is_some());
                    if let Some(image) = &image {
                        if image.custody_group.is_some() {
                            return Err(Anomaly::unsupported("rtdb/custody-placement", "Publishing a protected copy requires its own custody placement"));
                        }
                        validate_cells(definition, &image.cells)?;
                    }
                    effects.push((table.clone(), id.clone(), valid, image));
                }
            }
        }
        for (table, id, valid, image) in effects {
            let definition = self.definition(&table)?;
            self.authorize_write(into, &table, &id, valid, &definition, image.as_ref().map(|row| &row.cells))?;
            self.write(into, &table, id, valid, image)?;
        }
        Ok(())
    }

    fn definition(&self, table: &TableRef) -> Result<BranchTable> {
        validate_table(table)?;
        let definition = self.index.definition(table, self.index.head().basis).cloned().unwrap_or_default();
        if definition.dropped {
            return Err(Anomaly::new(AnomalyCategory::NotFound, "rtdb/unknown-table", "The table has been dropped"));
        }
        Ok(definition)
    }

    fn write_definition(&self, table: &TableRef) -> Result<BranchTable> {
        let definition = self.definition(table);
        if self.scope.is_some() && !definition.as_ref().is_ok_and(|def| def.visibility.is_some()) {
            return Err(write_scope_error());
        }
        definition
    }

    fn declare(&mut self, table: &TableRef, change: &Declaration) -> Result<()> {
        validate_table(table)?;
        let cut = self.index.head();
        let mut definition = self.index.definition(table, cut.basis).cloned().unwrap_or_default();
        if definition.dropped && !matches!(change, Declaration::Schema(_)) {
            return Err(Anomaly::new(AnomalyCategory::NotFound, "rtdb/unknown-table", "Redeclaring a dropped table requires its schema"));
        }
        match change {
            Declaration::Schema(columns) => {
                definition.dropped = false;
                for (name, ty) in columns {
                    validate_column(name)?;
                    let ty = definition.columns.get(name).map_or_else(|| ty.clone(), |prior| prior.join(ty));
                    definition.columns.insert(name.clone(), ty);
                }
            }
            Declaration::Index(index) => {
                if index.name.is_empty() {
                    return Err(Anomaly::incorrect("rtdb/invalid-index", "An index needs a name"));
                }
                let columns = index.columns().collect::<BTreeSet<_>>();
                if columns.len() != 1 + index.additional_columns.len() {
                    return Err(Anomaly::incorrect("rtdb/invalid-index", "Index columns must be distinct"));
                }
                for name in columns {
                    validate_column(name)?;
                }
                if self.index.tables(cut.basis).any(|(other, definition)| {
                    (other != table && definition.indexes.contains_key(&index.name))
                        || definition.ranked_indexes.contains_key(&index.name)
                }) {
                    return Err(conflict("rtdb/index-exists", "An index with that name already covers another table"));
                }
                if definition.indexes.get(&index.name).is_some_and(|old| old != index) {
                    return Err(conflict("rtdb/index-exists", "An index with that name already has a different definition"));
                }
                if !index.is_compound() {
                    for branch in self.index.active_names(cut.system_time) {
                        let view = self.view(branch, Layer::Merged)?;
                        for id in view.ids(table) {
                            for event in view.history(table, id) {
                                if let Some(cell) = event.image.as_ref().and_then(|image| image.cells.get(&index.column)) {
                                    validate_index_key(&index.column, cell)?;
                                }
                            }
                        }
                    }
                }
                definition.indexes.insert(index.name.clone(), index.clone());
            }
            Declaration::RankedIndex { name, definition: ranked } => {
                crate::resolver::check_search_index_name(name)?;
                let mut ranked = ranked.clone();
                if !(1..=16).contains(&ranked.columns.len())
                    || ranked.columns.iter().collect::<BTreeSet<_>>().len() != ranked.columns.len()
                {
                    return Err(Anomaly::incorrect("rtdb/invalid-index", "A ranked index requires 1 to 16 distinct stored columns"));
                }
                for column in &ranked.columns {
                    validate_column(column)?;
                }
                match &mut ranked.kind {
                    RankedIndexKind::Search { analyzer, .. } => {
                        *analyzer = rtdb_plan::parse_analyzer(analyzer)?.name().into();
                    }
                    RankedIndexKind::Spatial { geometry_encoding } => {
                        *geometry_encoding = rtdb_types::SpatialGeometryEncoding::parse(geometry_encoding)?.name().into();
                    }
                    RankedIndexKind::Ivf { dimensions, recall_bits } => {
                        let recall = f64::from_bits(*recall_bits);
                        if !(1..=4096).contains(dimensions) || !(recall > 0.0 && recall <= 1.0) {
                            return Err(Anomaly::incorrect("rtdb.vector/definition", "IVF needs dimensions from 1 to 4096 and a recall target in (0, 1]"));
                        }
                    }
                }
                if self.index.tables(cut.basis).any(|(other, definition)| {
                    definition.indexes.contains_key(name) || (other != table && definition.ranked_indexes.contains_key(name))
                }) {
                    return Err(conflict("rtdb/index-exists", "An index with that name already has a different definition"));
                }
                if let Some(prior) = definition.ranked_indexes.get(name) {
                    if prior.definition != ranked {
                        return Err(conflict("rtdb/index-exists", "An index with that name already has a different definition"));
                    }
                    return Ok(());
                }
                definition.ranked_indexes.insert(name.clone(), RankedIndexDeclaration { definition: ranked, position: cut.basis.position() });
            }
            Declaration::Ttl(ttl) => {
                if definition.visibility.is_some() && ttl.is_some() { return Err(labelled_time_error()); }
                if let Some(ttl) = ttl { expiry(cut.system_time, *ttl)?; }
                definition.ttl_micros = *ttl;
            }
            Declaration::Constraint { name, range } => {
                if name.is_empty() {
                    return Err(Anomaly::incorrect("rtdb/invalid-constraint", "A constraint needs a name"));
                }
                if let Some(range) = range {
                    if &range.table != table || !definition.indexes.contains_key(&range.index) {
                        return Err(Anomaly::incorrect("rtdb/invalid-constraint", "Declare the constraint's range index on the same table"));
                    }
                    if definition.grouped_constraints.contains_key(name) {
                        return Err(conflict("rtdb/constraint-exists", "The name belongs to a grouped constraint"));
                    }
                    definition.constraints.insert(name.clone(), range.clone());
                } else {
                    definition.constraints.remove(name);
                    definition.grouped_constraints.remove(name);
                }
            }
            Declaration::GroupedConstraint { name, rule } => {
                if name.is_empty() || rule.prefix_bytes == Some(0) {
                    return Err(Anomaly::incorrect("rtdb/invalid-constraint", "A grouped constraint needs a name and a positive prefix width"));
                }
                validate_column(&rule.column)?;
                for column in rule.equals.keys() { validate_column(column)?; }
                if definition.constraints.contains_key(name) {
                    return Err(conflict("rtdb/constraint-exists", "The name belongs to a range constraint"));
                }
                definition.grouped_constraints.insert(name.clone(), rule.clone());
            }
            Declaration::Drop => definition = BranchTable {
                generation: cut.basis.position(), dropped: true,
                visibility: definition.visibility, single_label_per_tx: definition.single_label_per_tx,
                ..BranchTable::default()
            },
            Declaration::Visibility(visibility) => {
                validate_column(&visibility.column)?;
                if definition.ttl_micros.is_some() { return Err(labelled_time_error()); }
                if definition.visibility.as_ref() != Some(visibility) {
                    if definition.visibility.is_some() || self.index.has_row_history(table) {
                        return Err(conflict("rtdb/visibility-declaration", "Declare a stable visibility column before writing the table"));
                    }
                    definition.visibility = Some(visibility.clone());
                }
            }
            Declaration::SingleLabelPerTx => {
                if definition.visibility.is_none() {
                    return Err(Anomaly::incorrect("rtdb/visibility-declaration", "The table requires a visibility column"));
                }
                definition.single_label_per_tx = true;
                if self.affected_labels.len() > 1 {
                    return Err(Anomaly::incorrect("rtdb/mixed-visibility-labels", "The transaction must use a single visibility label"));
                }
                self.single_label = true;
            }
        }
        self.push(BranchEffect::Declare { table: table.clone(), definition })?;
        self.declared.insert(table.clone());
        Ok(())
    }

    pub(super) fn check_constraints(&self) -> Result<()> {
        let cut = self.index.head();
        for (table, definition) in self.index.tables(cut.basis) {
            if definition.constraints.is_empty() && definition.grouped_constraints.is_empty() { continue; }
            for branch in self.index.active_names(cut.system_time) {
                if !self.declared.contains(table) && !self.touched.contains(branch) { continue; }
                let view = self.view(branch, Layer::Merged)?;
                for (name, range) in &definition.constraints {
                    if !view.count_at_most(range, cut.system_time)? {
                        return Err(conflict("rtdb/count-constraint", format!("Count constraint {name} failed on branch {branch}")));
                    }
                }
                for (name, rule) in &definition.grouped_constraints {
                    if !view.grouped_count_at_most(table, rule, cut.system_time)? {
                        return Err(conflict("rtdb/grouped-count-constraint", format!("Grouped count constraint {name} failed on branch {branch}")));
                    }
                }
            }
        }
        Ok(())
    }
}

pub(super) fn erased_custody_group() -> Anomaly {
    Anomaly::new(AnomalyCategory::NotFound, "rtdb/erased-under-view", "The custody group was revoked")
}

fn endpoints(history: &[&BranchEvent], within: Interval) -> Vec<Timestamp> {
    let mut boundaries = BTreeSet::from([within.lower(), within.upper()]);
    for event in history {
        if let Some(valid) = event.valid.intersection(within) {
            boundaries.extend([valid.lower(), valid.upper()]);
            if let Some(at) = event.image.as_ref().and_then(|image| image.expires_at).filter(|at| valid.contains(*at)) {
                boundaries.insert(at);
            }
        }
    }
    boundaries.into_iter().collect()
}

fn effective_valid(valid: Option<ValidTime>, time: Timestamp) -> Result<Interval> {
    let valid = valid.map_or_else(|| Interval::new(time, Timestamp::MAX), |valid| valid.resolve(time))?;
    if valid.is_empty() {
        return Err(Anomaly::incorrect("rtdb/empty-valid-time", "A row write needs a nonempty valid interval"));
    }
    Ok(valid)
}

fn validate_table(table: &TableRef) -> Result<()> {
    if crate::source::is_reserved_write_schema(&table.schema) {
        return Err(Anomaly::new(AnomalyCategory::Forbidden, "rtdb/system-table", "System tables cannot be written by Transact"));
    }
    if table.schema.is_empty() || table.table.is_empty() {
        return Err(Anomaly::incorrect("rtdb/invalid-table", "A table needs a schema and name"));
    }
    Ok(())
}

fn validate_column(name: &str) -> Result<()> {
    if name.is_empty() || matches!(name, "_id" | "_iid" | "_valid_from" | "_valid_to" | "_system_from" | "_system_to" | rtdb_types::rtdb_v1::WRITTEN_BASIS_COLUMN) {
        return Err(Anomaly::incorrect("rtdb/reserved-field", format!("Invalid document column {name}")));
    }
    Ok(())
}

fn validate_cells(definition: &BranchTable, cells: &StructFields<Value>) -> Result<()> {
    for (name, cell) in cells {
        if name == "_id" { continue; }
        validate_column(name)?;
        if let Some(ty) = definition.columns.get(name)
            && !matches!(cell, Value::Null | Value::Undefined) && ty.join(&cell.logical_type()) != *ty
        {
            return Err(Anomaly::incorrect("rtdb/schema-type", format!("Invalid type for declared column {name}")));
        }
        if definition.indexes.values().any(|index| !index.is_compound() && index.column == *name) {
            validate_index_key(name, cell)?;
        }
    }
    Ok(())
}

fn validate_index_key(name: &str, cell: &Value) -> Result<()> {
    if !matches!(cell, Value::Null | Value::Undefined) && rtdb_index::branches::index_key(cell).is_none() {
        return Err(Anomaly::incorrect("rtdb/index-key", format!("Index column {name} requires a binary or text value")));
    }
    Ok(())
}

fn expiry(time: Timestamp, ttl: u64) -> Result<Timestamp> {
    let duration = i64::try_from(ttl).ok().filter(|ttl| *ttl > 0);
    duration.and_then(|ttl| time.0.checked_add(ttl)).map(Timestamp).ok_or_else(|| {
        Anomaly::incorrect("rtdb/invalid-ttl", "TTL must be a positive interval in the timestamp range")
    })
}

fn validate_expiry(until: Timestamp, time: Timestamp) -> Result<()> {
    if until <= time {
        return Err(Anomaly::incorrect("rtdb/invalid-expiry", "Branch expiry must follow the transaction time"));
    }
    Ok(())
}

fn conflict(code: &str, message: impl Into<String>) -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, code, message)
}

fn invalid_label() -> Anomaly {
    Anomaly::incorrect("rtdb/visibility-label", "The declared visibility column requires a nonempty binary label")
}

fn write_scope_error() -> Anomaly {
    Anomaly::new(AnomalyCategory::Forbidden, "rtdb/write-scope", "The row operation is outside the write scope")
}

fn labelled_time_error() -> Anomaly {
    Anomaly::unsupported("rtdb/labelled-time", "Labelled rows require explicit valid time and literal cells")
}
