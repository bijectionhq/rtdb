use vstd::prelude::*;

use crate::decision::{self, Limits, Work};
use crate::drain::retain_until_drained;
use crate::foreign::{Arc, MutexSection, PermitBalance, Pin, PollContext, Scoped, TaskLocal,
                     detach, release_strong_reference, spawn};

verus! {

#[derive(Clone, Copy)]
struct Session {
    audience: bool,
    seat: u64,
}

impl Session {
    fn audience_source(&self) -> (out: Option<()>)
        ensures out.is_some() == self.audience,
    {
        if self.audience { Some(()) } else { None }
    }
}

#[derive(Clone, Copy)]
struct Call {
    admission: u64,
    session: Session,
    _active_call: Option<u64>,
    _reservation: u64,
}

type Owners = (u64, Session, Option<u64>, u64);

fn capture(call: &Call) -> (out: Option<Owners>)
    ensures out == if call.session.audience {
        Some((call.admission, call.session, call._active_call, call._reservation))
    } else { None },
{
    capture_audience_io_owner!(call)
}

spec fn still_holds<T>(owner: Option<T>, original: T) -> bool {
    owner == Some(original)
}

pub fn w1_pending_preserves_custody<T>(custody: T, pending: u64, finish: bool) -> (out: (Option<T>, bool))
    ensures out.0 == if finish { None } else { Some(custody) }, out.1 == finish,
{
    let ghost original = custody;
    macro_rules! pin { ($pending:expr) => { Pin { pending: $pending } }; }
    macro_rules! poll_fn {
        ($owner:ident, $work:ident, $cx:ident => $poll:block) => { vstd::prelude::verus_exec_expr!({
            let mut context = PollContext { completed: false };
            while $work.pending > 0
                invariant still_holds($owner, original), !context.completed,
                decreases $work.pending,
            {
                let $cx = &mut context;
                let _ = $poll;
            }
            if finish {
                let $cx = &mut context;
                let _ = $poll;
            }
            ($owner, context.completed)
        }) };
    }
    provider_custody!(custody, pending; pin, poll_fn)
}

#[derive(Clone, Copy)]
struct Budget {
    action: ActionLedger,
    turn: Option<u64>,
}

#[derive(Clone, Copy)]
enum ActionLedger {
    Existing(u64),
    Fresh(Option<Limits>),
}

struct Mutex;
impl Mutex {
    fn new(available: Option<Limits>) -> (out: ActionLedger)
        ensures out == ActionLedger::Fresh(available),
    {
        ActionLedger::Fresh(available)
    }
}

fn within_turn(existing: Option<Budget>, limits: Option<Limits>, turn: Option<u64>) -> (out: Option<Budget>)
    ensures out == if existing.is_some() { existing } else if limits.is_some() {
        Some(Budget { action: ActionLedger::Fresh(limits), turn })
    } else { None },
{
    select_audience_budget!(existing, limits, turn)
}

// Exercise the captured worker body after dropping the waiter.
macro_rules! spawn_then_detach {
    ($worker:ident; ($op:ident, $budget:ident, $work:ident, $sender:ident, $audience:ident) => $body:block) => {{
        let handle = spawn(($op, $budget, $work, $sender, $audience));
        let task = detach::<_, Budget>(handle);
        let ($op, $budget, $work, $sender, $audience) = task.captures;
        let $worker = task.local;
        $body
    }};
}

macro_rules! observe_work {
    ($sender:ident, $audience:ident, $work:ident) => {{
        let _ = ($sender, $audience);
        $work
    }};
}

macro_rules! keep_owner {
    ($custody:ident, $body:block) => {{
        let _ = $body;
        let mut lifetimes = $custody.0.lifetimes;
        lifetimes.pop()
    }};
}

macro_rules! raw_work {
    ($worker:ident, $budget:expr, $work:ident) => {{
        let _: Option<Budget> = $budget;
        let _ = $worker;
        $work
    }};
}

struct IoAdmission {
    lifetimes: Vec<Owners>,
}

#[derive(Clone, Copy)]
struct ScopedOwners {
    flight: Option<Owners>,
}

fn scope_retained_owner(previous: TaskLocal<ScopedOwners>, owner: ScopedOwners) -> (out: TaskLocal<ScopedOwners>)
    ensures out.value == Some(ScopedOwners { flight: if owner.flight.is_some() {
        owner.flight
    } else if previous.value.is_some() {
        previous.value.unwrap().flight
    } else { None } }),
{
    // Project the composed Arc onto the Flight charge, omitting unrelated scratch owners.
    macro_rules! combine { ($outer:ident, $inner:ident) => {{
        if $inner.flight.is_some() { $inner } else { $outer }
    }}; }
    scope_io_owner!(previous, owner, (); combine).local
}

fn w2_caller_cancellation_keeps_flight_resources(call: Call, pending: u64, nested: bool) -> (out: Option<Owners>)
    requires call.session.audience,
    ensures out == Some((call.admission, call.session, call._active_call, call._reservation)),
{
    let outer = scope_retained_owner(TaskLocal { value: None }, ScopedOwners { flight: capture(&call) });
    let local = if nested { scope_retained_owner(outer, ScopedOwners { flight: None }) } else { outer };
    let mut lifetimes = Vec::new();
    if let Some(ScopedOwners { flight: Some(owner) }) = retain_admitted_owner!(local) {
        lifetimes.push(owner);
    }
    let operation = IoAdmission { lifetimes };
    let work = ();
    let sender = ();
    let audience = true;
    let retained = spawn_provider_task!(operation, work, sender, audience;
        None::<Budget>, spawn_then_detach, keep_owner, raw_work, observe_work
    );
    let (custody, _) = w1_pending_preserves_custody(retained, pending, false);
    custody.unwrap_or(None)
}

fn scoped_poll(scoped: Scoped<Budget, ()>) -> (out: Option<Budget>)
    ensures out == scoped.local.value,
{
    scoped.local.try_with(Arc::clone).ok()
}

fn inherit_on_worker(worker: TaskLocal<Budget>, budget: Option<Budget>, work: ()) -> (out: Option<Budget>)
    ensures out == if budget.is_some() { budget } else { worker.value },
{
    macro_rules! await_poll {
        (scoped, $future:expr) => { scoped_poll($future) };
        (unscoped, $future:expr) => {{ let _ = $future; worker.try_with(Arc::clone).ok() }};
    }
    inherit_audience_budget!(worker, budget, work, await_poll)
}

macro_rules! keep_budget {
    ($custody:ident, $body:block) => {{
        let _ = $custody;
        $body
    }};
}

macro_rules! inherit_budget {
    ($worker:ident, $budget:expr, $work:ident) => { inherit_on_worker($worker, $budget, $work) };
}

fn w4_detached_worker_observes_original_budget(
    existing: Option<(u64, Option<u64>)>, limits: Limits, turn: u64,
) -> (out: Option<Budget>)
    ensures out == Some(match existing {
        Some((id, original_turn)) => Budget { action: ActionLedger::Existing(id), turn: original_turn },
        None => Budget { action: ActionLedger::Fresh(Some(limits)), turn: Some(turn) },
    }),
{
    let existing = match existing {
        Some((id, original_turn)) => Some(Budget { action: ActionLedger::Existing(id), turn: original_turn }),
        None => None,
    };
    let local = TaskLocal { value: within_turn(existing, Some(limits), Some(turn)) };
    let operation = IoAdmission { lifetimes: Vec::new() };
    let work = ();
    let sender = ();
    let audience = true;
    spawn_provider_task!(operation, work, sender, audience;
        capture_current_budget!(local), spawn_then_detach, keep_budget, inherit_budget, observe_work
    )
}

fn w3_completion_releases_one_reference(aliases: u64, ready: bool) -> (out: PermitBalance)
    requires aliases > 0,
    ensures out.strong == aliases - if ready { 1int } else { 0int },
        out.returned + if out.strong > 0 { 1int } else { 0int } == 1,
{
    let (owner, _) = w1_pending_preserves_custody((), 0, ready);
    let before = PermitBalance { strong: aliases, returned: 0 };
    if owner.is_some() { before } else { release_strong_reference(before) }
}

fn debit_locked(section: &mut MutexSection<Option<Limits>>, cost: Limits) -> (out: Result<(), Error>)
    ensures
        final(section).value.is_some() == (old(section).value.is_some()
            && decision::fits(old(section).value.unwrap(), cost)),
        final(section).value.is_some() ==> decision::exact_debit(
            old(section).value.unwrap(), cost, final(section).value.unwrap(),
        ),
        out.is_ok() == final(section).value.is_some(),
{
    debit_audience_allowance!(section, cost)
}

#[derive(Clone, Copy)]
enum Error { AudienceWorkLimit }

fn w3_work_debits_action_and_turn(
    action: &mut MutexSection<Option<Limits>>, turn: &mut MutexSection<Option<Limits>>, work: Work,
) -> (out: Result<(), Error>)
    ensures two_ledger_debit(old(action).value, old(turn).value,
        final(action).value, final(turn).value, work_cost(work), out.is_ok()),
{
    macro_rules! debit { ($section:expr, $cost:expr) => { debit_locked($section, $cost) }; }
    charge_audience_budget!(action, Some(turn), work, debit)
}

spec fn two_ledger_debit(action: Option<Limits>, turn: Option<Limits>,
                         next_action: Option<Limits>, next_turn: Option<Limits>,
                         cost: Limits, success: bool) -> bool {
    let action_open = action.is_some() && decision::fits(action.unwrap(), cost);
    let turn_open = turn.is_some() && decision::fits(turn.unwrap(), cost);
    &&& next_action.is_some() == action_open
    &&& (action_open ==> decision::exact_debit(action.unwrap(), cost, next_action.unwrap()))
    &&& (action_open ==> next_turn.is_some() == turn_open)
    &&& (!action_open ==> next_turn == turn)
    &&& (success ==> decision::exact_debit(turn.unwrap(), cost, next_turn.unwrap()))
    &&& success == (action_open && turn_open)
}

spec fn work_cost(work: Work) -> Limits {
    let (requests, bytes, metadata, scan, decode) = match work {
        Work::ProviderRead(bytes) => (1usize, bytes, 16_384usize, 0usize, 0usize),
        Work::Scan(items) => (0usize, 0usize, 0usize, items, 0usize),
        Work::Decode(bytes) => (0usize, 0usize, 0usize, 0usize, bytes),
    };
    Limits { provider_requests: requests, provider_bytes: bytes, provider_metadata_bytes: metadata,
        scan_items: scan, decode_bytes: decode }
}

}
