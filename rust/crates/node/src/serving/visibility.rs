use std::sync::Arc;

use rtdb_crypt::CustodyVault;
use rtdb_index::branches::BranchIndex;
use rtdb_tx::transact::ScopedTransact;
use rtdb_types::{
    Anomaly, MemoryBudget,
    rtdb_v1::{LabelScope, MAIN_BRANCH},
};

use super::*;

pub const ROW_VISIBILITY: &str = "row-visibility";

#[derive(Clone)]
pub struct ServingScope {
    pub labels: LabelScope,
    pub receipt_namespace: [u8; 32],
    pub custody_evidence: bool,
    pub hold: Option<[u8; 16]>,
}

impl ServingScope {
    pub fn transaction_scope(&self) -> ScopedTransact<'_> {
        ScopedTransact { labels: &self.labels, receipt_namespace: self.receipt_namespace }
    }

    pub async fn query(
        &self, index: &BranchIndex, source: QuerySource, now: Timestamp, memory: &Arc<MemoryBudget>,
        vault: Option<&CustodyVault>,
    ) -> Result<(QueryView, Basis)> {
        check_source(&source.branch, source.layer)?;
        let projected = index.project_main_visibility(&self.labels)?;
        let at = if source.at == At::Linearizable { At::Latest } else { source.at };
        let view = projected.index().view(MAIN_BRANCH, at, Layer::Merged, now)?;
        let cut = view.cut();
        if let Some(hold) = self.hold {
            index.check_scoped_hold_view(hold, self.receipt_namespace, now, &projected, cut.basis)?;
        }
        let raw_cut = if matches!(at, At::Latest | At::AtLeast(_)) {
            index.head().basis
        } else {
            projected.source_basis(cut.basis)?
        };
        let protected = projected.index().has_custody_groups() && view.tables().any(|(table, _)| {
            view.ids(table).into_iter().any(|id| view.history(table, id).iter().any(|event| {
                event.image.as_ref().is_some_and(|image| image.custody_group.is_some())
            }))
        });
        if protected && !self.custody_evidence { return Err(protected_read_required()); }
        let (snapshot, custody_groups, custody_contributors) = if protected {
            let vault = vault.ok_or_else(|| Anomaly::unsupported("rtdb/row-custody", "The scoped query has no custody vault"))?;
            super::custody::snapshot_with_contributors(&view, memory, vault, |_, event| {
                projected.custody_contributor(event.image.as_deref()?)
            }).await?
        } else {
            (rtdb_query::branch_snapshot(&view, memory)?, BTreeSet::new(), Some(BTreeSet::new()))
        };
        Ok((QueryView { snapshot, cut, claim: None, custody_groups, custody_contributors, scoped_source_basis: Some(raw_cut), key_generation: None }, raw_cut))
    }

    pub fn changes(&self, index: &BranchIndex, request: &Changes) -> Result<ChangePage> {
        check_source(&request.branch, Layer::Merged)?;
        if request.limit == 0 {
            return Err(Anomaly::incorrect("rtdb/change-limit", "Changes requires a positive group limit"));
        }
        let projected = index.project_main_visibility(&self.labels)?;
        let head = projected.index().head().basis;
        let position = |basis: Basis| {
            if basis.incarnation() != head.incarnation() {
                return Err(Anomaly::incorrect("rtdb/foreign-basis", "The basis belongs to another read scope"));
            }
            Ok(basis.position())
        };
        let after = request.after.map(position).transpose()?.unwrap_or(0);
        let until = request.until.map(position).transpose()?;
        if until.is_some_and(|until| until < after) {
            return Err(Anomaly::incorrect("rtdb/change-range", "The change interval is inverted"));
        }
        let upper = until.unwrap_or(u64::MAX).min(head.position());
        let descending = request.order == ChangeOrder::Descending;
        let mut groups = Vec::new();
        let mut more = false;
        let mut contributors = Some(BTreeSet::new());
        let ordered: Box<dyn Iterator<Item = _>> = if descending {
            Box::new(projected.changes().iter().rev())
        } else {
            Box::new(projected.changes().iter())
        };
        for group in ordered.filter(|group| {
            group.cut.basis.position() > after && group.cut.basis.position() <= upper
        }) {
            let rows = group.rows.iter().filter(|row| {
                request.tables.is_empty() || request.tables.contains(&row.table)
            }).map(|row| ChangeRow {
                table: row.table.clone(),
                id: row.id.clone(),
                valid: Some(row.valid),
                kind: match (row.erased, &row.after) {
                    (true, _) => ChangeKind::Erased,
                    (false, Some(_)) => ChangeKind::Opened,
                    (false, None) => ChangeKind::Closed,
                },
                erased_written_basis: row.erased_written_basis,
                before: row.before.as_ref().filter(|_| matches!(request.images, ChangeImages::Before | ChangeImages::Both))
                    .cloned(),
                after: row.after.as_ref().filter(|_| matches!(request.images, ChangeImages::After | ChangeImages::Both))
                    .cloned(),
            }).collect::<Vec<_>>();
            if rows.is_empty() { continue; }
            if groups.len() == request.limit {
                more = true;
                break;
            }
            for image in rows.iter().flat_map(|row| [&row.before, &row.after]).flatten() {
                let Some(group) = image.custody_group else { continue; };
                if !index.custody_group_live(group)? { continue; }
                if !self.custody_evidence { return Err(protected_read_required()); }
                if let Some(inventory) = &mut contributors {
                    match projected.custody_contributor(image) {
                        Some(contributor) if inventory.len() < 512 || inventory.contains(&contributor) => {
                            inventory.insert(contributor);
                        }
                        _ => contributors = None,
                    }
                }
            }
            groups.push(ChangeGroup { cut: group.cut, rows });
        }
        let through = match (descending, more, groups.last()) {
            (true, true, Some(group)) => group.cut.basis.position().saturating_sub(1),
            (false, true, Some(group)) => group.cut.basis.position(),
            (true, _, _) => after,
            _ => upper.max(after),
        };
        Ok(ChangePage {
            read_custody: None,
            scoped_custody: contributors.map(|inventory| (index.head().basis, inventory)),
            groups,
            through: Basis::new(head.incarnation(), through),
            complete: !more && until.is_none_or(|until| until <= head.position()),
            custody_groups: BTreeSet::new(),
            key_generation: None,
        })
    }
}

fn check_source(branch: &str, layer: Layer) -> Result<()> {
    if branch != MAIN_BRANCH || layer != Layer::Merged {
        return Err(Anomaly::unsupported("rtdb/scoped-branch", "Scoped reads select the merged main branch"));
    }
    Ok(())
}

fn protected_read_required() -> Anomaly {
    Anomaly::new(rtdb_types::AnomalyCategory::Forbidden, "rtdb/scoped-custody", "Protected scoped reads require custody evidence")
}

pub(super) fn unsupported() -> Anomaly {
    Anomaly::unsupported("rtdb/feature-required", "Required feature row-visibility is unavailable")
}

#[cfg(test)]
mod tests {
    use rtdb_index::branches::{BranchCut, BranchEffect, BranchEvent, BranchTable, RowImage, Visibility};

    use super::*;
    use rtdb_types::{StructFields, Value};

    #[test]
    fn scoped_change_page_hides_unrelated_raw_progress_and_private_images() {
        let mut index = BranchIndex::new([1; 16]);
        let table = TableRef::new("public", "docs");
        let id = EntityId::Integer(1);
        let advance = |index: &mut BranchIndex, position: u64| {
            let cut = BranchCut { basis: Basis::new([1; 16], position), system_time: Timestamp(position as i64) };
            index.advance(cut).unwrap();
            cut
        };
        let write = |index: &mut BranchIndex, cut, id: EntityId, label| {
            let cells = StructFields::from([("label".into(), Value::Binary(vec![label]))]);
            index.apply(BranchEffect::Write {
                branch: MAIN_BRANCH.into(), table: table.clone(),
                event: BranchEvent { cut, id, valid: Interval::ALL, written: Default::default(), image: Some(Arc::new(RowImage { cells, expires_at: None, custody_group: None, custody_payload: None })) },
            }).unwrap();
        };
        let first = advance(&mut index, 1);
        index.apply(BranchEffect::Declare { table: table.clone(), definition: BranchTable {
            visibility: Some(Visibility { column: "label".into() }), ..Default::default()
        } }).unwrap();
        write(&mut index, first, id.clone(), 1);
        let scope = ServingScope {
            labels: LabelScope { read: BTreeSet::from([vec![1]]), ..Default::default() }, receipt_namespace: [2; 32], custody_evidence: false, hold: None,
        };
        let mut request = Changes { branch: MAIN_BRANCH.into(), tables: BTreeSet::new(), after: None, until: None,
            order: ChangeOrder::Ascending, images: ChangeImages::Both, limit: 3, wait: Duration::ZERO };
        let initial = scope.changes(&index, &request).unwrap();
        request.after = Some(initial.through);
        let hidden = advance(&mut index, 2);
        write(&mut index, hidden, EntityId::Integer(2), 2);
        let page = scope.changes(&index, &request).unwrap();
        assert_eq!(page.through, initial.through);
        assert!(page.groups.is_empty());
        let mask = advance(&mut index, 3);
        write(&mut index, mask, id, 2);
        let page = scope.changes(&index, &request).unwrap();
        assert_eq!(page.through.position(), 2);
        let [group] = page.groups.as_slice() else { panic!("expected one projected group") };
        let [row] = group.rows.as_slice() else { panic!("expected one masked row") };
        assert!(matches!(row.kind, ChangeKind::Closed));
        assert!(row.before.is_some());
        assert!(row.after.is_none());
    }

    #[test]
    fn scoped_changes_keep_their_cut_across_hidden_erasure() {
        let mut index = BranchIndex::new([1; 16]);
        let table = TableRef::new("public", "docs");
        let advance = |index: &mut BranchIndex, position: u64| {
            let cut = BranchCut { basis: Basis::new([1; 16], position), system_time: Timestamp(position as i64) };
            index.advance(cut).unwrap();
            cut
        };
        let write = |index: &mut BranchIndex, cut, id, label| {
            index.apply(BranchEffect::Write {
                branch: MAIN_BRANCH.into(), table: table.clone(),
                event: BranchEvent {
                    cut, id: EntityId::Integer(id), valid: Interval::ALL, written: Default::default(),
                    image: Some(Arc::new(RowImage {
                        cells: StructFields::from([
                            ("label".into(), Value::Binary(vec![label])),
                            ("content".into(), Value::Utf8("erased payload".into())),
                        ]),
                        expires_at: None, custody_group: None, custody_payload: None,
                    })),
                },
            }).unwrap();
        };
        let erase = |index: &mut BranchIndex, id| index.apply(BranchEffect::Erase {
            branch: Some(MAIN_BRANCH.into()), table: table.clone(), id: EntityId::Integer(id),
        }).unwrap();
        let first = advance(&mut index, 1);
        index.apply(BranchEffect::Declare { table: table.clone(), definition: BranchTable {
            visibility: Some(Visibility { column: "label".into() }), ..Default::default()
        } }).unwrap();
        write(&mut index, first, 1, 1);
        let scope = ServingScope {
            labels: LabelScope { read: BTreeSet::from([vec![1]]), ..Default::default() },
            receipt_namespace: [2; 32], custody_evidence: false, hold: None,
        };
        let request = Changes {
            branch: MAIN_BRANCH.into(), tables: BTreeSet::new(), after: None, until: None,
            order: ChangeOrder::Ascending, images: ChangeImages::Both, limit: 8, wait: Duration::ZERO,
        };
        let baseline = scope.changes(&index, &request).unwrap();
        let hidden = advance(&mut index, 2);
        write(&mut index, hidden, 2, 2);
        advance(&mut index, 3);
        erase(&mut index, 2);
        let after_hidden = scope.changes(&index, &request).unwrap();
        assert_eq!(after_hidden.through, baseline.through);
        assert_eq!(after_hidden.groups.len(), baseline.groups.len());
        assert_eq!(after_hidden.groups[0].rows[0].after, baseline.groups[0].rows[0].after);
        advance(&mut index, 4);
        erase(&mut index, 1);
        let after_visible = scope.changes(&index, &request).unwrap();
        assert_eq!(after_visible.through.position(), baseline.through.position() + 1);
        assert!(after_visible.groups[0].rows[0].after.is_none());
        assert!(matches!(after_visible.groups[1].rows[0].kind, ChangeKind::Erased));
    }

}
