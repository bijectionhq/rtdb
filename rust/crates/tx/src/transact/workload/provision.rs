use super::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LineProvision {
    pub branch: String,
    pub registry: TableRef,
    pub generation: [u8; 16],
    pub owner: Point,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub clear_source_tables: Vec<TableRef>,
}

impl LineProvision {
    pub(super) fn valid(&self) -> bool {
        self.branch != MAIN_BRANCH
            && valid_branch_name(&self.branch)
            && self.generation != [0; 16]
            && application_table(&self.registry)
            && application_table(&self.owner.table)
            && self.registry != self.owner.table
            && self.owner.valid_at != Timestamp::MAX
            && matches!(&self.owner.id, EntityId::String(id) if bounded(id))
            && self.clear_source_tables.len() <= 16
            && self.clear_source_tables.iter().all(application_table)
            && self.clear_source_tables.windows(2).all(|pair| pair[0] < pair[1])
    }

    pub(super) fn check(
        &self,
        point: &Point,
        writer: &Applied,
        index: &BranchIndex,
        request: &Transact,
        now: Timestamp,
    ) -> Result<()> {
        let writer = CustodyWriterFence {
            table: point.table.clone(),
            id: point.id.clone(),
            valid_at: point.valid_at,
            written: Basis::from_bytes(&writer.basis)?,
        };
        index.require_local_custody()?;
        if index.has_visibility_history()
            || index.has_branch(&self.branch)
            || !request.otherwise.is_empty()
            || request.custody_request_digest.is_some()
            || !request
                .requires
                .first()
                .is_some_and(|guard| writer.authenticates(guard))
            || request
                .guards
                .iter()
                .chain(&request.requires)
                .any(|guard| match guard {
                    Guard::Version {
                        branch, table, id, ..
                    } => branch != MAIN_BRANCH || index.has_custody_address(table, id),
                    Guard::CountAtMost { .. } => true,
                    Guard::Unchanged(_) => false,
                })
        {
            return Err(denied());
        }

        let mut fork = None;
        let mut registration = None;
        let mut owner = false;
        let mut declared = false;
        for op in &request.ops {
            match op {
                Op::Fork {
                    child,
                    from: (parent, basis),
                    writable: true,
                    expires_at: Some(expires),
                } if child == &self.branch && parent == MAIN_BRANCH && fork.is_none() => {
                    if index.has_custody_groups_at(*basis)
                        || *expires == Timestamp::MAX
                        || *expires <= now
                        || request.window.before.is_none_or(|before| before > *expires)
                    {
                        return Err(denied());
                    }
                    let source = index.view(MAIN_BRANCH, At::Exactly(*basis), Layer::Local, now)?;
                    if self.clear_source_tables.iter().any(|table| source.has_retained_puts(table)) {
                        return Err(denied());
                    }
                    fork = Some(*expires);
                }
                Op::Declare {
                    table,
                    change: Declaration::Schema(_),
                } if table == &self.registry
                    && table != &point.table
                    && !declared
                    && index.definition(table, index.head().basis).is_none() =>
                {
                    declared = true;
                }
                Op::Put {
                    branch,
                    table,
                    rows,
                    if_absent,
                } if branch == MAIN_BRANCH && table != &point.table && application_table(table) => {
                    if table == &self.registry {
                        let [row] = rows.as_slice() else {
                            return Err(denied());
                        };
                        if !if_absent
                            || registration.is_some()
                            || !self.registration(row, &request.key)
                        {
                            return Err(denied());
                        }
                        let Some(Cell::Value(Value::I64(expires))) = row.cells.get("expires")
                        else {
                            return Err(denied());
                        };
                        registration = Some(Timestamp(*expires));
                    }
                    for row in rows {
                        if row.custody_group.is_some()
                            || row.custody_payload.is_some()
                            || !row.protected_columns.is_empty()
                            || index.has_custody_address(table, &row.id)
                        {
                            return Err(denied());
                        }
                        if table == &self.owner.table
                            && row.id == self.owner.id
                            && matches!(row.valid, Some(ValidTime::Literal(valid)) if valid.contains(self.owner.valid_at))
                        {
                            if owner {
                                return Err(denied());
                            }
                            owner = true;
                        }
                    }
                }
                _ => return Err(denied()),
            }
        }
        if !owner || fork.is_none() || registration != fork {
            return Err(denied());
        }
        Ok(())
    }

    fn registration(&self, row: &Row, key: &[u8]) -> bool {
        matches!(&row.id, EntityId::String(branch) if branch == &self.branch)
            && matches!(row.valid, Some(ValidTime::Literal(valid)) if valid.contains(Timestamp(0)))
            && matches!(row.cells.get("created_basis"), Some(Cell::TxBasis))
            && matches!(row.cells.get("line_generation"), Some(Cell::Value(Value::Binary(bytes) | Value::FixedSizeBinary(bytes))) if bytes.as_slice() == self.generation)
            && matches!(row.cells.get("creation_key"), Some(Cell::Value(Value::Binary(bytes) | Value::FixedSizeBinary(bytes))) if bytes.as_slice() == key)
            && matches!((&self.owner.id, row.cells.get("proposal_owner")),
                (EntityId::String(id), Some(Cell::Value(Value::Utf8(owner)))) if id == owner)
            && matches!(
                row.cells.get("writable"),
                Some(Cell::Value(Value::Bool(true)))
            )
    }
}

fn application_table(table: &TableRef) -> bool {
    !matches!(
        table.schema.as_str(),
        "xt" | "information_schema" | "pg_catalog"
    ) && bounded(&table.schema)
        && bounded(&table.table)
}
