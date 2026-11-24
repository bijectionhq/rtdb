use super::*;

use rtdb_directory::{RetainedWriterHandoff, WriterHandoffPlan, WriterHandoffState};

use crate::control::{WriterHandoffAction, WriterHandoffCommand};

impl NodeAgent {
    pub(super) async fn writer_handoff(
        &self,
        name: &str,
        command: WriterHandoffCommand,
    ) -> Result<Response> {
        use WriterHandoffState::*;

        let secret = self.secret.as_ref().ok_or_else(handoff_changed)?;
        if !command.authentic(secret, name) {
            return Err(handoff_changed());
        }
        let plan = &command.plan;
        let action = command.action;
        if self.member() != plan.successor {
            return Err(handoff_changed());
        }
        if action == WriterHandoffAction::Prepare {
            let initial = self.record(name).await?;
            if initial
                .writer_handoffs
                .last()
                .filter(|entry| &entry.plan == plan)
                .is_none_or(|entry| matches!(entry.state, Reserved { .. }))
            {
                if !plan.matches(&initial)
                    || !predecessor(&initial, plan)
                    || upper_now(plan)? >= plan.identity.switch_at_ns()
                    || self.execution_memory_pressured()
                {
                    return Err(handoff_changed());
                }
                self.subscribe_for(name, SubscriptionUse::Fragment).await?;
                let database = self.node.catalog().database(name)?;
                require_recovered_checkpoint(&initial, &database)?;
                database
                    .confirm_replica_snapshot(&CancellationToken::new())
                    .await?;
            }
        }

        let turn = self.turn(name);
        let _turn = turn.lock().await;
        let members = self.members().await?;
        for _ in 0..8 {
            let record = self.record(name).await?;
            let selected = record
                .writer_handoffs
                .last()
                .filter(|entry| &entry.plan == plan);
            let result = |witness: Vec<u8>| {
                let proof = secret.sign_route(&command.binding(name, Some(&witness)));
                Ok(Response::WriterHandoff { witness, proof })
            };
            if let Some(entry) = selected {
                if action == WriterHandoffAction::Prepare
                    && let Some(bytes) = entry.preparation()
                {
                    return result(bytes);
                }
                if matches!(
                    action,
                    WriterHandoffAction::Read | WriterHandoffAction::Switch
                ) && matches!(entry.state, Opened { .. })
                {
                    let leading = self.is_handoff_writer(name, &record);
                    if !successor(&record, plan) || !leading {
                        return Err(handoff_changed());
                    }
                    self.confirm_holder(name, &record.id, self.options.audience_journal_port)
                        .await?;
                    let current = self.record(name).await?;
                    if current.writer_handoffs.last() != Some(entry) || !successor(&current, plan) {
                        return Err(handoff_changed());
                    }
                    return result(entry.settlement().ok_or_else(handoff_changed)?);
                }
                if action == WriterHandoffAction::Abandon && entry.state == Abandoned {
                    return result(Vec::new());
                }
            }

            if !plan.matches(&record) {
                return Err(handoff_changed());
            }
            let coordinate = record.version.checked_add(1).ok_or_else(handoff_changed)?;
            let mut next = record.clone();
            match (action, selected.map(|entry| entry.state)) {
                (WriterHandoffAction::Prepare, None) => {
                    if !predecessor(&record, plan)
                        || upper_now(plan)? >= plan.identity.switch_at_ns()
                        || !members
                            .get(&plan.incumbent)
                            .is_some_and(|member| record.may_hold_on(member, now_ms()))
                        || !members
                            .get(&plan.successor)
                            .is_some_and(|member| record.may_place_on(member, now_ms()))
                        || !self
                            .subscriptions
                            .lock()
                            .get(name)
                            .is_some_and(|subscription| {
                                subscription.id == record.id
                                    && subscription.database.upgrade().is_some_and(|database| {
                                        require_recovered_checkpoint(&record, &database).is_ok()
                                    })
                            })
                    {
                        return Err(handoff_changed());
                    }
                    crate::deployment::require_storage_namespaces(
                        &record,
                        self.storage_namespaces,
                    )?;
                    next.writer_handoffs.push(RetainedWriterHandoff {
                        plan: plan.clone(),
                        state: Reserved { coordinate },
                    });
                }
                (WriterHandoffAction::Prepare, Some(Reserved { coordinate })) => {
                    let prepared_upper_ns = upper_now(plan)?;
                    if prepared_upper_ns >= plan.identity.switch_at_ns() {
                        return Err(handoff_changed());
                    }
                    next.writer_handoffs
                        .last_mut()
                        .expect("selected reservation")
                        .state = Prepared {
                        coordinate,
                        prepared_upper_ns,
                    };
                }
                (
                    WriterHandoffAction::Switch,
                    Some(Prepared {
                        coordinate: prepared,
                        prepared_upper_ns,
                    }),
                ) => {
                    let now = utc_ns()?;
                    let lower = now.saturating_sub(plan.maximum_clock_error_ns);
                    let upper = now.saturating_add(plan.maximum_clock_error_ns);
                    if !super::writer_handoff_decision::fence_at_switch(
                        lower,
                        upper,
                        plan.identity.switch_at_ns(),
                        plan.switch_deadline_ns,
                    ) || !predecessor(&record, plan)
                        || !super::writer_handoff_decision::next_writer_epoch(
                            record.epoch,
                            plan.identity.successor_epoch(),
                        )
                        || !members
                            .get(&plan.successor)
                            .is_some_and(|member| record.may_place_on(member, now_ms()))
                        || self.execution_memory_pressured()
                        || !self
                            .subscriptions
                            .lock()
                            .get(name)
                            .is_some_and(|subscription| {
                                subscription.id == record.id
                                    && subscription.database.upgrade().is_some_and(|database| {
                                        require_recovered_checkpoint(&record, &database).is_ok()
                                    })
                            })
                    {
                        return Err(handoff_changed());
                    }
                    next.epoch = plan.identity.successor_epoch();
                    next.holder = Some(plan.successor.clone());
                    next.writer_handoffs
                        .last_mut()
                        .expect("prepared handoff")
                        .state = Fenced {
                        coordinate: prepared,
                        prepared_upper_ns,
                        fence: coordinate,
                    };
                }
                (
                    WriterHandoffAction::Switch,
                    Some(Fenced {
                        coordinate: prepared,
                        prepared_upper_ns,
                        fence,
                    }),
                ) => {
                    if !successor(&record, plan) {
                        return Err(handoff_changed());
                    }
                    if !self.is_handoff_writer(name, &record) {
                        self.open_with_handoff(record.clone(), &members, true)
                            .await?;
                    }
                    self.confirm_holder(name, &record.id, self.options.audience_journal_port)
                        .await?;
                    next.writer_handoffs
                        .last_mut()
                        .expect("fenced handoff")
                        .state = Opened {
                        coordinate: prepared,
                        prepared_upper_ns,
                        fence,
                        opening: coordinate,
                        opened_upper_ns: upper_now(plan)?,
                    };
                }
                (WriterHandoffAction::Abandon, Some(Reserved { .. } | Prepared { .. })) => {
                    next.writer_handoffs
                        .last_mut()
                        .expect("selected handoff")
                        .state = Abandoned;
                }
                _ => return Err(handoff_changed()),
            }
            match self
                .directory
                .replace_deployment(next, record.version)
                .await?
            {
                Cas::Applied(_) | Cas::Conflict(Some(_)) => {}
                Cas::Conflict(None) => return Err(handoff_changed()),
            }
        }
        Err(handoff_changed())
    }

    fn is_handoff_writer(&self, name: &str, record: &DeploymentRecord) -> bool {
        self.holdings.lock().get(name).is_some_and(|holding| {
            holding.id == record.id
                && holding.epoch == record.epoch
                && holding.database.upgrade().is_some_and(|database| {
                    matches!(database.status().role, DriverRole::Leading(_))
                })
        })
    }
}

pub(super) fn retains_incumbent_read(
    record: &DeploymentRecord,
    member: &MemberId,
    epoch: u64,
) -> bool {
    record
        .writer_handoffs
        .iter()
        .rev()
        .find(|entry| {
            entry.plan.incarnation == record.incarnation && entry.retains_incumbent(member, epoch)
        })
        .is_some_and(|entry| {
            upper_now(&entry.plan).is_ok_and(|upper| {
                super::writer_handoff_decision::retained_read_live(
                    upper,
                    entry.plan.incumbent_drain_until_ns,
                )
            })
        })
}

fn predecessor(record: &DeploymentRecord, plan: &WriterHandoffPlan) -> bool {
    record.epoch == plan.identity.predecessor_epoch()
        && record.holder.as_ref() == Some(&plan.incumbent)
}

fn successor(record: &DeploymentRecord, plan: &WriterHandoffPlan) -> bool {
    record.epoch == plan.identity.successor_epoch()
        && record.holder.as_ref() == Some(&plan.successor)
}

fn upper_now(plan: &WriterHandoffPlan) -> Result<u64> {
    utc_ns()?
        .checked_add(plan.maximum_clock_error_ns)
        .filter(|upper| *upper <= i64::MAX as u64)
        .ok_or_else(handoff_changed)
}

fn utc_ns() -> Result<u64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| u64::try_from(elapsed.as_nanos()).ok())
        .ok_or_else(handoff_changed)
}

fn handoff_changed() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb.multi/writer-handoff",
        "The node cannot establish the exact prepared writer handoff",
    )
}
