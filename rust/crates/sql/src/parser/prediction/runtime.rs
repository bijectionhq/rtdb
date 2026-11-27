use std::{alloc::Layout, marker::PhantomData, sync::Arc};
use rtdb_types::{AllocationScope, Anomaly, MemoryBudget};
use rtdb_types::java_text::JavaText;
use crate::lexer::{IndexedLexeme, StreamError, TokenStream};
use crate::parser::grammar::{Grammar, GrammarError, OwnedGrammar, State, StateKindView, TokenSet, TokenType, TransitionView};
use super::{Alternative, FactoryError, PredictionError, PredictionMode, PredictionStorage, Result, Semantic, push};
use super::adaptive;
use super::closure::{ClosureOptions, ClosureRound, LoopEntryOption, SourceAssertions, with_closure_round_result};
use super::dfa::PredictionDfas;
use super::storage::{Retained, with_prediction_storage, with_prediction_storage_result};

#[path = "bail_fallback.rs"]
mod bail_fallback;
pub(super) use bail_fallback::{PassError, PassResult, with_bail_fallback};

#[path = "direct_control.rs"]
mod direct_control;
pub use direct_control::{ContextualStatementDiagnostic, StatementSyntaxDiagnostic};

#[path = "inline_match.rs"]
mod inline_match;

#[path = "no_viable_report.rs"]
mod no_viable_report;
pub(super) use no_viable_report::NoViableDiagnostic;

#[path = "required_block_sync.rs"]
mod required_block_sync;
pub(super) use required_block_sync::TokenDiagnostic;

#[path = "generated_actions.rs"]
mod generated_actions;

#[path = "generated_expr.rs"]
mod generated_expr;

#[path = "generated_dml.rs"]
mod generated_dml;

#[path = "generated_names.rs"]
mod generated_names;
pub(in crate::parser) use generated_names::is_identifier_token;

#[path = "generated_offset.rs"]
mod generated_offset;

#[path = "generated_select.rs"]
mod generated_select;

#[path = "generated_statement.rs"]
mod generated_statement;

#[cfg(test)]
#[path = "runtime_tests.rs"]
mod tests;

pub(super) type RuntimeResult<'sql, T> = std::result::Result<T, RuntimeError<'sql>>;
#[derive(Debug)]
pub(super) enum RuntimeError<'sql> {
    Prediction(PredictionError),
    Input(StreamError<'sql>),
    Control(RuntimeProblem),
    NoViableAlternative(NoViableAlternative<'sql>),
    ReportedNoViableAlternative(NoViableDiagnostic<'sql>),
    InputMismatch(InputMismatch<'sql>),
    ReportedTokenSyntax(TokenDiagnostic<'sql>),
}

#[derive(Clone, Copy, Debug)]
pub(super) struct NoViableAlternative<'sql> {
    pub(super) start: IndexedLexeme<'sql>,
    pub(super) offending: IndexedLexeme<'sql>,
    pub(super) state: ParserState,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RuntimeProblem { Aborted, Invocation, Recursion, UninitializedOrigin, PredictionSite }
impl From<PredictionError> for RuntimeError<'_> { fn from(error: PredictionError) -> Self { Self::Prediction(error) } }
impl<'sql> From<StreamError<'sql>> for RuntimeError<'sql> { fn from(error: StreamError<'sql>) -> Self { Self::Input(error) } }
impl From<GrammarError> for RuntimeError<'_> { fn from(error: GrammarError) -> Self { Self::Prediction(error.into()) } }
impl super::factory_error_sealed::Sealed for RuntimeError<'_> {}
impl FactoryError for RuntimeError<'_> {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::parser) struct CapturedClassPolicy { options: ClosureOptions }
impl CapturedClassPolicy {
    #[cfg(test)]
    pub(super) fn from_captured(options: ClosureOptions) -> Self { Self { options } }

    pub(in crate::parser) fn capture_parser_atn<'text, E>(
        parser_atn_assertions: SourceAssertions,
        get_safe_env: impl FnOnce(&'static str) -> std::result::Result<Option<JavaText<'text>>, E>,
    ) -> std::result::Result<Self, E> {
        let value = get_safe_env("TURN_OFF_LR_LOOP_ENTRY_BRANCH_OPT")?;
        let disabled = value.is_some_and(|text| {
            text.len_units() == 4 && text.units().zip(b"true").all(|(unit, &ascii)| {
                unit == u16::from(ascii) || unit == u16::from(ascii.to_ascii_uppercase())
            })
        });
        Ok(Self { options: ClosureOptions {
            pruning: if disabled { LoopEntryOption::Disabled } else { LoopEntryOption::Enabled },
            assertions: parser_atn_assertions,
        } })
    }
}
pub(in crate::parser) struct ParserEnvironment { storage: PredictionStorage, policy: CapturedClassPolicy }
impl ParserEnvironment {
    pub(in crate::parser) fn open_in(policy: CapturedClassPolicy, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let initialization = AllocationScope::new(memory).map_err(PredictionError::Admission)?;
        let grammar = OwnedGrammar::load(memory)?;
        Self::new(PredictionStorage::new(grammar, memory), policy, &initialization)
    }

    pub(super) fn new(mut storage: PredictionStorage, policy: CapturedClassPolicy, initialization: &AllocationScope) -> Result<Self> {
        with_prediction_storage(&mut storage, initialization, |access| {
            let (dfas, configs, _) = access.dfa_parts();
            dfas.initialize(configs)
        })?;
        Ok(Self { storage, policy })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ParserState { RootSentinel, State(u32) }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct InvocationKey<'parse> { index: usize, brand: PhantomData<fn(&'parse mut ()) -> &'parse mut ()> }
impl InvocationKey<'_> { fn at(index: usize) -> Self { Self { index, brand: PhantomData } } }
#[derive(Clone, Copy)]
struct SavedOuterLink<'parse> { parent: Option<InvocationKey<'parse>>, invoking: ParserState }
#[derive(Clone, Copy)]
enum RecursionPhase<'parse> { Allocated, Entering, Active { head: InvocationKey<'parse> }, Closing { head: InvocationKey<'parse> }, Closed }
#[derive(Clone, Copy)]
enum ReplacementUse { Fresh, Installing, Installed }
#[derive(Clone, Copy)]
enum InvocationRole<'parse> {
    Ordinary,
    OrdinaryTreeAttachmentFailed,
    RecursiveLabel,
    RecursiveEntry { saved: SavedOuterLink<'parse>, phase: RecursionPhase<'parse> },
    RecursionReplacement { entry: InvocationKey<'parse>, previous: InvocationKey<'parse>, state: ReplacementUse },
}
#[derive(Clone, Copy, Debug)]
pub(super) struct InputMismatch<'sql> {
    pub(super) offending: IndexedLexeme<'sql>,
    pub(super) state: ParserState,
}

pub(super) enum InvocationCause<'sql> {
    NoViableAlternative(NoViableAlternative<'sql>),
    InputMismatch(InputMismatch<'sql>),
}

enum InvocationException<'parse, 'sql> {
    Owned(InvocationCause<'sql>),
    CapturedMismatch { cause: InvocationCause<'sql>, context: InvocationKey<'parse> },
    Shared(InvocationKey<'parse>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ContextClass {
    RegularIdentifier, DelimitedIdentifier,
    QueryExpr, InsertStatement, UpdateStatement, DeleteStatement, StartTransactionStatement, CommitStatement, RollbackStatement, DiscardStatement, SetRoleStatement,
    QuerySpecification,
    DmlReturningStatement,
    DiscardAll, DiscardPlans, DiscardTemp, DiscardSequences,
    IntegerLiteral0, FloatLiteral, BooleanLiteral, NullLiteral, CharacterStringLiteral, SqlStandardString, CEscapesString, DollarString,
    TimeLiteral, DurationLiteral, UuidLiteral, UriLiteral, KeywordLiteral,
    BinaryStringLiteral, DateTimeLiteral0, DateLiteral, TimestampLiteral, WithTimeZone, WithoutTimeZone,
    IntervalLiteral0, MultiFieldInterval, SingleFieldInterval,
    CommonValueExpr0, LiteralExpr, NumericFactorExpr, NumericTermExpr,
    DynamicParameter, PostgresParameter,
}
impl ContextClass {
    fn rule(self) -> u16 {
        match self {
            Self::QueryExpr | Self::InsertStatement | Self::UpdateStatement | Self::DeleteStatement | Self::StartTransactionStatement | Self::CommitStatement | Self::RollbackStatement | Self::DiscardStatement | Self::SetRoleStatement => 2,
            Self::QuerySpecification => 126,
            Self::DmlReturningStatement => 153,
            Self::DiscardAll | Self::DiscardPlans | Self::DiscardTemp | Self::DiscardSequences => 3,
            Self::DateLiteral | Self::TimestampLiteral => 12,
            Self::IntegerLiteral0 | Self::FloatLiteral | Self::BooleanLiteral | Self::NullLiteral | Self::CharacterStringLiteral
            | Self::TimeLiteral | Self::DurationLiteral | Self::UuidLiteral | Self::UriLiteral | Self::KeywordLiteral
            | Self::BinaryStringLiteral | Self::DateTimeLiteral0 | Self::IntervalLiteral0 => 14,
            Self::SqlStandardString | Self::CEscapesString | Self::DollarString => 16,
            Self::MultiFieldInterval | Self::SingleFieldInterval => 17,
            Self::WithTimeZone | Self::WithoutTimeZone => 30,
            Self::CommonValueExpr0 => 33,
            Self::RegularIdentifier | Self::DelimitedIdentifier => 22,
            Self::LiteralExpr | Self::NumericFactorExpr | Self::NumericTermExpr => 34,
            Self::DynamicParameter | Self::PostgresParameter => 39,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) enum ContextChild<'parse, 'sql> { Context(InvocationKey<'parse>), Terminal(IndexedLexeme<'sql>) }

struct InvocationRecord<'parse, 'sql> {
    parent: Option<InvocationKey<'parse>>, invoking: ParserState, rule: u16,
    start: Option<IndexedLexeme<'sql>>, stop: Option<IndexedLexeme<'sql>>, role: InvocationRole<'parse>,
    exception: Option<InvocationException<'parse, 'sql>>,
    class: Option<ContextClass>, children: Option<Vec<ContextChild<'parse, 'sql>>>,
}
pub(super) struct PendingRecursionEntry<'parse> { key: InvocationKey<'parse> }
pub(super) struct RecursionEntry<'parse> { key: InvocationKey<'parse> }
pub(super) struct RecursionReplacement<'parse> { key: InvocationKey<'parse> }
#[derive(Clone, Copy)]
enum PrecedenceFrame<'parse> { Base0, Recursion { value: i32, entry: InvocationKey<'parse> } }
impl PrecedenceFrame<'_> { fn value(self) -> i32 { match self { Self::Base0 => 0, Self::Recursion { value, .. } => value } } }

struct PendingNoViableReport<'parse, 'sql> {
    invocation: InvocationKey<'parse>,
    cause: NoViableAlternative<'sql>,
    mode: PredictionMode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct NextTokensCapture<'parse> { context: InvocationKey<'parse>, state: u32 }

#[derive(Clone, Copy)]
enum MismatchExpectation<Context> { Current(TokenSet), Captured(Context) }

struct PendingInputMismatch<'parse, 'sql> {
    invocation: InvocationKey<'parse>,
    match_state: ParserState,
    cause: InputMismatch<'sql>,
    expectation: MismatchExpectation<NextTokensCapture<'parse>>,
}

enum BailRecovery { Clear, Entered }

enum ParserControl<'parse, 'sql> {
    Ready,
    Failed,
    PendingNoViable(PendingNoViableReport<'parse, 'sql>),
    PendingInputMismatch(PendingInputMismatch<'parse, 'sql>),
    InErrorRecovery,
    BailCancelled { exception: InvocationKey<'parse>, recovery: BailRecovery },
}

impl ParserControl<'_, '_> {
    fn is_ready(&self) -> bool { matches!(self, Self::Ready) }

    fn abort(&mut self) {
        *self = match self {
            Self::InErrorRecovery | Self::BailCancelled { recovery: BailRecovery::Entered, .. } => Self::InErrorRecovery,
            _ => Self::Failed,
        };
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ParserProfile { Kernel, SllBail, LlDefault }

impl ParserProfile {
    fn accepts(self, mode: PredictionMode) -> bool {
        matches!((self, mode),
            (Self::Kernel, _) | (Self::SllBail, PredictionMode::Sll) | (Self::LlDefault, PredictionMode::Ll))
    }
}

pub(super) struct ParserSession<'parse, 'sql, 'env, 'work> {
    environment: &'env mut ParserEnvironment, input: TokenStream<'sql, 'work>, records: Vec<InvocationRecord<'parse, 'sql>>,
    current: Option<InvocationKey<'parse>>, state: ParserState, precedence: Vec<PrecedenceFrame<'parse>>,
    matched_eof: bool, control: ParserControl<'parse, 'sql>, syntax_errors: i32, profile: ParserProfile,
    next_tokens_capture: Option<NextTokensCapture<'parse>>, work: &'work AllocationScope,
}
impl Drop for ParserSession<'_, '_, '_, '_> { fn drop(&mut self) { let _ = self.work; } }
pub(super) fn with_parser_session<'sql, 'env, 'work, R>(
    environment: &'env mut ParserEnvironment, sql: &'sql str, work: &'work AllocationScope,
    use_session: impl for<'parse> FnOnce(&mut ParserSession<'parse, 'sql, 'env, 'work>) -> RuntimeResult<'sql, R>,
) -> RuntimeResult<'sql, R> {
    let input = TokenStream::new(sql, work).map_err(PredictionError::Admission)?;
    let mut precedence = Vec::new();
    push(&mut precedence, PrecedenceFrame::Base0, work)?;
    let mut session = ParserSession { environment, input, records: Vec::new(), current: None, state: ParserState::RootSentinel, precedence, matched_eof: false, control: ParserControl::Ready, syntax_errors: 0, profile: ParserProfile::Kernel, next_tokens_capture: None, work };
    use_session(&mut session)
}

impl<'parse, 'sql, 'work> ParserSession<'parse, 'sql, '_, 'work> {
    fn ready(&self) -> RuntimeResult<'sql, ()> { if self.control.is_ready() { Ok(()) } else { Err(RuntimeError::Control(RuntimeProblem::Aborted)) } }
    fn finish<T>(&mut self, result: RuntimeResult<'sql, T>) -> RuntimeResult<'sql, T> { if result.is_err() { self.control.abort(); } result }
    fn generated_mode(&mut self) -> RuntimeResult<'sql, PredictionMode> {
        let result = self.ready().and_then(|()| match self.profile {
            ParserProfile::SllBail => Ok(PredictionMode::Sll),
            ParserProfile::LlDefault => Ok(PredictionMode::Ll),
            ParserProfile::Kernel => Err(RuntimeError::Control(RuntimeProblem::PredictionSite)),
        });
        self.finish(result)
    }
    fn record(&self, key: InvocationKey<'parse>) -> RuntimeResult<'sql, &InvocationRecord<'parse, 'sql>> { self.records.get(key.index).ok_or(RuntimeError::Control(RuntimeProblem::Invocation)) }
    fn append(&mut self, rule: u16, saved: SavedOuterLink<'parse>, role: InvocationRole<'parse>) -> RuntimeResult<'sql, InvocationKey<'parse>> {
        let key = InvocationKey::at(self.records.len());
        push(&mut self.records, InvocationRecord { parent: saved.parent, invoking: saved.invoking, rule, start: None, stop: None, role, exception: None, class: None, children: None }, self.work)?;
        Ok(key)
    }
    fn call_binding(&self, rule: u16, recursive: bool) -> RuntimeResult<'sql, u32> {
        let grammar = self.environment.storage.grammar_view();
        let start = grammar.rule(rule)?.start();
        if !matches!(start.kind(), StateKindView::RuleStart { left_recursive, .. } if left_recursive == recursive) { return Err(RuntimeError::Control(RuntimeProblem::Invocation)); }
        if self.current.is_some() {
            let ParserState::State(number) = self.state else { return Err(RuntimeError::Control(RuntimeProblem::Invocation)); };
            if !matches!(grammar.state(number)?.transitions().next(), Some(TransitionView::Rule { rule: called, .. }) if called.number() == rule) { return Err(RuntimeError::Control(RuntimeProblem::Invocation)); }
        }
        Ok(start.number())
    }
    pub(super) fn set_state(&mut self, number: u32) -> RuntimeResult<'sql, ()> {
        let result = (|| { self.ready()?; self.environment.storage.grammar_view().state(number)?; self.state = ParserState::State(number); Ok(()) })(); self.finish(result)
    }
    pub(super) fn new_invocation(&mut self, rule: u16) -> RuntimeResult<'sql, InvocationKey<'parse>> {
        let result = (|| { self.ready()?; self.call_binding(rule, false)?; self.append(rule, SavedOuterLink { parent: self.current, invoking: self.state }, InvocationRole::Ordinary) })(); self.finish(result)
    }
    pub(super) fn enter_rule(&mut self, frame: InvocationKey<'parse>) -> RuntimeResult<'sql, ()> {
        let result = (|| {
            self.ready()?;
            let record = self.record(frame)?;
            if !matches!(record.role, InvocationRole::Ordinary) || record.start.is_some() || record.stop.is_some() || Some(frame) == self.current || record.parent != self.current || record.invoking != self.state { return Err(RuntimeError::Control(RuntimeProblem::Invocation)); }
            let start = self.call_binding(record.rule, false)?;
            self.state = ParserState::State(start); self.current = Some(frame);
            let token = self.input.lt(1)?; self.records[frame.index].start = token; Ok(())
        })(); self.finish(result)
    }
    fn enter_tree_rule(&mut self, frame: InvocationKey<'parse>) -> RuntimeResult<'sql, ()> {
        self.enter_rule(frame)?;
        let Some(parent) = self.records[frame.index].parent else { return Ok(()); };
        let result = self.append_child(parent, ContextChild::Context(frame));
        if result.is_err() {
            self.records[frame.index].role = InvocationRole::OrdinaryTreeAttachmentFailed;
        }
        result
    }
    fn append_child(&mut self, parent: InvocationKey<'parse>, child: ContextChild<'parse, 'sql>) -> RuntimeResult<'sql, ()> {
        let result = self.ready().and_then(|()| self.append_child_storage(parent, child));
        self.finish(result)
    }

    fn append_child_storage(&mut self, parent: InvocationKey<'parse>, child: ContextChild<'parse, 'sql>) -> RuntimeResult<'sql, ()> {
        self.record(parent)?;
        let children = self.records[parent.index].children.get_or_insert_with(Vec::new);
        if children.len() == children.capacity() {
            let capacity = children.capacity().checked_mul(2).ok_or_else(super::capacity_error)?.max(1);
            let layout = Layout::array::<ContextChild<'parse, 'sql>>(capacity).map_err(|_| super::capacity_error())?;
            self.work.reserve(layout.size()).map_err(PredictionError::Admission)?;
            let mut replacement = Vec::new();
            replacement.try_reserve_exact(capacity).map_err(|_| PredictionError::Admission(
                Anomaly::incorrect("rtdb/allocation-failed", "Unable to allocate parser context children").with_kind(rtdb_types::AnomalyKind::ResourceExhausted)))?;
            replacement.append(children);
            *children = replacement;
        }
        children.push(child);
        Ok(())
    }
    fn labelled_context(&mut self, generic: InvocationKey<'parse>, class: ContextClass) -> RuntimeResult<'sql, InvocationKey<'parse>> {
        let result = (|| {
            self.ready()?;
            let old = self.record(generic)?;
            if self.current != Some(generic) || !matches!(old.role, InvocationRole::Ordinary) || old.rule != class.rule()
                || old.class.is_some() || old.children.is_some() || old.exception.is_some() || old.start.is_none() {
                return Err(RuntimeError::Control(RuntimeProblem::Invocation));
            }
            let copy = InvocationRecord { parent: old.parent, invoking: old.invoking, rule: old.rule,
                start: old.start, stop: old.stop, role: InvocationRole::Ordinary, exception: None, class: Some(class), children: None };
            let key = InvocationKey::at(self.records.len());
            push(&mut self.records, copy, self.work)?;
            Ok(key)
        })(); self.finish(result)
    }
    fn enter_outer_alt(&mut self, labelled: InvocationKey<'parse>) -> RuntimeResult<'sql, ()> {
        let result = (|| {
            self.ready()?;
            let generic = self.current.ok_or(RuntimeError::Control(RuntimeProblem::Invocation))?;
            let old = self.record(generic)?;
            let new = self.record(labelled)?;
            if labelled.index <= generic.index || old.class.is_some() || new.class.map(ContextClass::rule) != Some(old.rule)
                || !matches!(old.role, InvocationRole::Ordinary) || !matches!(new.role, InvocationRole::Ordinary)
                || old.children.is_some() || new.children.is_some() || old.exception.is_some() || new.exception.is_some()
                || old.rule != new.rule || old.parent != new.parent || old.invoking != new.invoking
                || old.start.map(IndexedLexeme::index) != new.start.map(IndexedLexeme::index)
                || old.stop.map(IndexedLexeme::index) != new.stop.map(IndexedLexeme::index) {
                return Err(RuntimeError::Control(RuntimeProblem::Invocation));
            }
            if let Some(parent) = old.parent {
                let children = self.records[parent.index].children.as_mut().ok_or(RuntimeError::Control(RuntimeProblem::Invocation))?;
                if !matches!(children.last(), Some(ContextChild::Context(key)) if *key == generic) {
                    return Err(RuntimeError::Control(RuntimeProblem::Invocation));
                }
                children.pop();
                children.push(ContextChild::Context(labelled));
            }
            self.current = Some(labelled);
            Ok(())
        })(); self.finish(result)
    }
    pub(super) fn exit_rule(&mut self) -> RuntimeResult<'sql, ()> {
        let result = (|| {
            let key = self.current.ok_or(RuntimeError::Control(RuntimeProblem::Invocation))?;
            let record = self.record(key)?;
            if !matches!(record.role, InvocationRole::Ordinary | InvocationRole::OrdinaryTreeAttachmentFailed)
                || record.start.is_none() { return Err(RuntimeError::Control(RuntimeProblem::Invocation)); }
            let token = self.input.lt(if self.matched_eof { 1 } else { -1 })?;
            self.records[key.index].stop = token;
            self.state = self.records[key.index].invoking; self.current = self.records[key.index].parent; Ok(())
        })(); self.finish(result)
    }
    pub(super) fn new_recursion_entry(&mut self, rule: u16) -> RuntimeResult<'sql, PendingRecursionEntry<'parse>> {
        let result = (|| {
            self.ready()?; self.call_binding(rule, true)?;
            let saved = SavedOuterLink { parent: self.current, invoking: self.state };
            let key = self.append(rule, saved, InvocationRole::RecursiveEntry { saved, phase: RecursionPhase::Allocated })?;
            Ok(PendingRecursionEntry { key })
        })(); self.finish(result)
    }
    pub(super) fn enter_recursion(&mut self, pending: PendingRecursionEntry<'parse>, precedence: i32) -> RuntimeResult<'sql, RecursionEntry<'parse>> {
        let result = (|| {
            self.ready()?;
            let key = pending.key; let record = self.record(key)?;
            let InvocationRole::RecursiveEntry { saved, phase: RecursionPhase::Allocated } = record.role else { return Err(RuntimeError::Control(RuntimeProblem::Recursion)); };
            if saved.parent != self.current || saved.invoking != self.state { return Err(RuntimeError::Control(RuntimeProblem::Recursion)); }
            let start = self.call_binding(record.rule, true)?;
            self.state = ParserState::State(start);
            push(&mut self.precedence, PrecedenceFrame::Recursion { value: precedence, entry: key }, self.work)?;
            self.records[key.index].role = InvocationRole::RecursiveEntry { saved, phase: RecursionPhase::Entering };
            self.current = Some(key);
            let token = self.input.lt(1)?;
            self.records[key.index].start = token;
            self.records[key.index].role = InvocationRole::RecursiveEntry { saved, phase: RecursionPhase::Active { head: key } };
            Ok(RecursionEntry { key })
        })(); self.finish(result)
    }
    fn active_entry(&self, entry: &RecursionEntry<'parse>) -> RuntimeResult<'sql, (SavedOuterLink<'parse>, InvocationKey<'parse>, u16)> {
        let record = self.record(entry.key)?;
        let InvocationRole::RecursiveEntry { saved, phase: RecursionPhase::Active { head } } = record.role else { return Err(RuntimeError::Control(RuntimeProblem::Recursion)); };
        if self.current != Some(head) || !matches!(self.precedence.last(), Some(PrecedenceFrame::Recursion { entry: top, .. }) if *top == entry.key) { return Err(RuntimeError::Control(RuntimeProblem::Recursion)); }
        Ok((saved, head, record.rule))
    }
    pub(super) fn new_recursion_replacement(&mut self, entry: &RecursionEntry<'parse>) -> RuntimeResult<'sql, RecursionReplacement<'parse>> {
        let result = (|| {
            self.ready()?; let (saved, head, rule) = self.active_entry(entry)?;
            let key = self.append(rule, saved, InvocationRole::RecursionReplacement { entry: entry.key, previous: head, state: ReplacementUse::Fresh })?;
            Ok(RecursionReplacement { key })
        })(); self.finish(result)
    }
    pub(super) fn push_recursion(&mut self, entry: &RecursionEntry<'parse>, replacement: RecursionReplacement<'parse>) -> RuntimeResult<'sql, ()> {
        let result = (|| {
            self.ready()?; let (saved, previous, rule) = self.active_entry(entry)?;
            let next = replacement.key; let record = self.record(next)?;
            if !matches!(record.role, InvocationRole::RecursionReplacement { entry: owner, previous: old, state: ReplacementUse::Fresh } if owner == entry.key && old == previous)
                || record.rule != rule || record.parent != saved.parent || record.invoking != saved.invoking || next.index <= previous.index || next.index <= entry.key.index { return Err(RuntimeError::Control(RuntimeProblem::Recursion)); }
            let start = self.environment.storage.grammar_view().rule(rule)?.start().number();
            self.records[next.index].role = InvocationRole::RecursionReplacement { entry: entry.key, previous, state: ReplacementUse::Installing };
            self.records[previous.index].parent = Some(next); self.records[previous.index].invoking = ParserState::State(start);
            let token = self.input.lt(-1)?; self.records[previous.index].stop = token;
            self.current = Some(next); self.records[next.index].start = self.records[previous.index].start;
            self.records[next.index].role = InvocationRole::RecursionReplacement { entry: entry.key, previous, state: ReplacementUse::Installed };
            self.records[entry.key.index].role = InvocationRole::RecursiveEntry { saved, phase: RecursionPhase::Active { head: next } };
            Ok(())
        })(); self.finish(result)
    }
    #[cfg(test)]
    pub(super) fn unroll_recursion(&mut self, entry: &mut RecursionEntry<'parse>) -> RuntimeResult<'sql, InvocationKey<'parse>> {
        let result = (|| {
            let (saved, _, _) = self.active_entry(entry)?;
            self.unroll_entered_recursion(entry.key, saved)
        })(); self.finish(result)
    }

    fn unroll_entered_recursion(
        &mut self,
        entry: InvocationKey<'parse>,
        saved: SavedOuterLink<'parse>,
    ) -> RuntimeResult<'sql, InvocationKey<'parse>> {
        let retctx = self.current.ok_or(RuntimeError::Control(RuntimeProblem::Recursion))?;
        self.precedence.pop();
        self.records[entry.index].role = InvocationRole::RecursiveEntry { saved, phase: RecursionPhase::Closing { head: retctx } };
        let token = self.input.lt(-1)?;
        self.records[retctx.index].stop = token;
        self.current = saved.parent;
        self.records[retctx.index].parent = saved.parent;
        self.records[entry.index].role = InvocationRole::RecursiveEntry { saved, phase: RecursionPhase::Closed };
        Ok(retctx)
    }

    fn label_recursion_base(&mut self, entry: &RecursionEntry<'parse>, class: ContextClass) -> RuntimeResult<'sql, InvocationKey<'parse>> {
        let result = (|| {
            self.ready()?;
            let (saved, generic, rule) = self.active_entry(entry)?;
            let old = self.record(generic)?;
            if generic != entry.key || rule != class.rule() || old.class.is_some()
                || old.children.is_some() || old.exception.is_some() || old.start.is_none()
            {
                return Err(RuntimeError::Control(RuntimeProblem::Recursion));
            }

            let copy = InvocationRecord {
                parent: old.parent, invoking: old.invoking, rule, start: old.start, stop: old.stop,
                role: InvocationRole::RecursiveLabel, exception: None, class: Some(class), children: None,
            };
            let key = InvocationKey::at(self.records.len());
            push(&mut self.records, copy, self.work)?;
            self.current = Some(key);
            self.records[entry.key.index].role = InvocationRole::RecursiveEntry { saved, phase: RecursionPhase::Active { head: key } };
            Ok(key)
        })(); self.finish(result)
    }

    fn push_recursion_tree(&mut self, entry: &RecursionEntry<'parse>, replacement: RecursionReplacement<'parse>) -> RuntimeResult<'sql, ()> {
        let next = replacement.key;
        let previous = self.current.ok_or(RuntimeError::Control(RuntimeProblem::Recursion));
        let previous = self.finish(previous)?;
        self.push_recursion(entry, replacement)?;
        self.append_child(next, ContextChild::Context(previous))
    }

    #[cfg(test)]
    pub(super) fn note_eof_match(&mut self) -> RuntimeResult<'sql, ()> { let result = self.ready().map(|()| self.matched_eof = true); self.finish(result) }
    pub(super) fn lookahead(&mut self, offset: isize) -> RuntimeResult<'sql, Option<IndexedLexeme<'sql>>> { let result = self.ready().and_then(|()| self.input.lt(offset).map_err(RuntimeError::Input)); self.finish(result) }
    fn lookahead_token(&mut self, offset: isize) -> RuntimeResult<'sql, (IndexedLexeme<'sql>, TokenType)> {
        let token = self.lookahead(offset)?.expect("positive lookahead");
        let kind = self.environment.storage.grammar_view().token_type(token.lexeme().kind())
            .map_err(RuntimeError::from);
        Ok((token, self.finish(kind)?))
    }
    pub(super) fn advance_input(&mut self) -> RuntimeResult<'sql, ()> { let result = self.ready().and_then(|()| self.input.consume().map_err(RuntimeError::Input)); self.finish(result) }
    #[cfg(test)]
    pub(super) fn rewind_input(&mut self) -> RuntimeResult<'sql, ()> {
        if matches!(self.control, ParserControl::BailCancelled { .. }) {
            return self.finish(Err(RuntimeError::Control(RuntimeProblem::Aborted)));
        }
        let result = self.input.seek(0).map_err(RuntimeError::Input);
        self.finish(result)
    }
    #[cfg(test)]
    pub(super) fn input_index(&self) -> Option<usize> { self.input.index() }
    #[cfg(test)]
    pub(super) fn buffered_token(&self, index: usize) -> Option<IndexedLexeme<'sql>> { self.input.buffered(index) }
    #[cfg(test)]
    pub(super) fn current(&self) -> Option<InvocationKey<'parse>> { self.current }
    #[cfg(test)]
    pub(super) fn state(&self) -> ParserState { self.state }
    pub(super) fn precedence(&self) -> i32 { self.precedence.last().copied().expect("base precedence").value() }
    pub(super) fn invocation_view(&self) -> InvocationView<'_, 'parse, 'sql> { InvocationView { records: &self.records, current: self.current } }
    #[cfg(test)]
    pub(super) fn clear_rule_control(&mut self) -> RuntimeResult<'sql, ()> {
        if matches!(self.control, ParserControl::BailCancelled { .. }) {
            return self.finish(Err(RuntimeError::Control(RuntimeProblem::Aborted)));
        }
        let result = if self.precedence.len() != 1 { Err(RuntimeError::Control(RuntimeProblem::Recursion)) } else { self.current = None; self.matched_eof = false; self.precedence.clear(); self.precedence.push(PrecedenceFrame::Base0); Ok(()) }; self.finish(result)
    }
    pub(super) fn adaptive_predict(&mut self, decision: u16, mode: PredictionMode) -> RuntimeResult<'sql, Alternative> {
        let site = self.ready().and_then(|()| {
            if self.profile.accepts(mode) { Ok(()) } else { Err(RuntimeError::Control(RuntimeProblem::PredictionSite)) }
        });
        self.finish(site)?;
        let result = self.with_prediction(decision, |round, dfas, input| adaptive::adaptive_predict(round, dfas, input, mode));
        if let Err(RuntimeError::NoViableAlternative(cause)) = &result {
            let invocation = self.current.expect("active prediction invocation");
            self.control = ParserControl::PendingNoViable(PendingNoViableReport { invocation, cause: *cause, mode });
        }
        result
    }

    fn generated_ordinary(
        &mut self, rule: u16,
        body: impl FnOnce(&mut Self, &mut InvocationKey<'parse>) -> PassResult<'parse, 'sql, ()>,
    ) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        let mut local = self.new_invocation(rule)?;
        self.enter_tree_rule(local)?;
        let result = match body(self, &mut local) {
            Err(PassError::Runtime(RuntimeError::NoViableAlternative(_) | RuntimeError::InputMismatch(_))) => self.catch_recognition(local),
            result => result,
        };
        self.exit_rule()?;
        result.map(|()| local)
    }

    fn generated_no_viable<T>(&mut self) -> RuntimeResult<'sql, T> {
        let mode = self.generated_mode()?;
        let invocation = self.finish(self.current.ok_or(RuntimeError::Control(RuntimeProblem::Invocation)))?;
        let start = self.lookahead(1)?.expect("positive lookahead");
        let offending = self.lookahead(1)?.expect("positive lookahead");
        let cause = NoViableAlternative { start, offending, state: self.state };
        self.control = ParserControl::PendingNoViable(PendingNoViableReport { invocation, cause, mode });
        Err(RuntimeError::NoViableAlternative(cause))
    }

    fn capture_input_mismatch(&mut self, state: ParserState) -> RuntimeResult<'sql, (InvocationKey<'parse>, InputMismatch<'sql>)> {
        let current = self.current.ok_or(RuntimeError::Control(RuntimeProblem::Invocation));
        let invocation = self.finish(current)?;
        let offending = self.lookahead(1)?.expect("positive lookahead");
        Ok((invocation, InputMismatch { offending, state }))
    }

    fn pend_input_mismatch<T>(&mut self, expectation: MismatchExpectation<NextTokensCapture<'parse>>) -> RuntimeResult<'sql, T> {
        let match_state = self.state;
        let offending_state = match expectation {
            MismatchExpectation::Current(_) => match_state,
            MismatchExpectation::Captured(capture) => ParserState::State(capture.state),
        };
        let (invocation, cause) = self.capture_input_mismatch(offending_state)?;
        self.control = ParserControl::PendingInputMismatch(PendingInputMismatch { invocation, match_state, cause, expectation });
        Err(RuntimeError::InputMismatch(cause))
    }

    fn pending_no_viable_report(&self) -> Option<&PendingNoViableReport<'parse, 'sql>> {
        let ParserControl::PendingNoViable(report) = &self.control else { return None; };
        (self.current == Some(report.invocation) && self.input.index() == Some(report.cause.start.index()))
            .then_some(report)
    }

    pub(super) fn with_prediction<R>(&mut self, decision: u16,
        use_prediction: impl for<'cfg, 'ctx, 'access, 'call, 'round, 'input, 'origin> FnOnce(
            &mut ClosureRound<'call, 'round, 'cfg, 'ctx, 'access, 'access, Retained>,
            &mut PredictionDfas<'cfg, 'access, 'access>,
            &mut PredictionInput<'input, 'origin, 'round, 'parse, 'sql, 'work>,
        ) -> RuntimeResult<'sql, R>,
    ) -> RuntimeResult<'sql, R> {
        let entered = (|| {
            self.ready()?;
            let origin = self.input.index().ok_or(RuntimeError::Control(RuntimeProblem::UninitializedOrigin))?;
            let record = self.record(self.current.ok_or(RuntimeError::Control(RuntimeProblem::Invocation))?)?;
            if record.start.is_none() { return Err(RuntimeError::Control(RuntimeProblem::Invocation)); }
            if self.environment.storage.grammar_view().decision(decision)?.state().rule().number() != record.rule { return Err(RuntimeError::Control(RuntimeProblem::PredictionSite)); }
            Ok(origin)
        })();
        let origin = self.finish(entered)?;
        let round_work = self.work.scratch();
        let state = self.state; let precedence = self.precedence(); let options = self.environment.policy.options;
        let view = InvocationView { records: &self.records, current: self.current };
        let mut restoration = PredictionRestoration::new(&mut self.input, &mut self.control, origin);
        let result: RuntimeResult<'sql, R> = with_prediction_storage_result(&mut self.environment.storage, &round_work, |access| {
            let (dfas, configs, contexts) = access.dfa_parts();
            let selected = match contexts.grammar().decision(decision) {
                Ok(selected) => selected,
                Err(error) => { dfas.abort(configs, contexts); return Err(RuntimeError::Prediction(error.into())); }
            };
            let result = with_closure_round_result(configs, contexts, selected, options, |round| {
                let mut input = PredictionInput { restoration: &mut restoration, invocations: view, decision, state, precedence, brand: PhantomData };
                let result = use_prediction(round, dfas, &mut input);
                if result.is_err() { input.abort(); round.abort(); let (configs, contexts) = round.parts(); dfas.abort(configs, contexts); }
                result
            });
            if result.is_err() { dfas.abort(configs, contexts); }
            result
        });
        restoration.finish(result)
    }
}

#[derive(Clone, Copy)]
pub(super) struct InvocationView<'a, 'parse, 'sql> { records: &'a [InvocationRecord<'parse, 'sql>], current: Option<InvocationKey<'parse>> }
impl<'a, 'parse, 'sql> InvocationView<'a, 'parse, 'sql> {
    #[cfg(test)]
    pub(super) fn context(&self, invocation: InvocationKey<'parse>) -> Option<RuleContextView<'a, 'parse, 'sql>> {
        self.records.get(invocation.index).map(|record| RuleContextView { key: invocation, record })
    }
    #[cfg(test)]
    pub(super) fn exception(&self, invocation: InvocationKey<'parse>) -> Option<&InvocationCause<'sql>> {
        let exception = self.records.get(invocation.index)?.exception.as_ref()?;
        let owner = match exception {
            InvocationException::Owned(cause) | InvocationException::CapturedMismatch { cause, .. } => return Some(cause),
            InvocationException::Shared(owner) => owner,
        };
        let exception = self.records.get(owner.index)?.exception.as_ref()?;
        match exception {
            InvocationException::Owned(cause) | InvocationException::CapturedMismatch { cause, .. } => Some(cause),
            InvocationException::Shared(_) => None,
        }
    }

    pub(super) fn invocations<'view, 'g: 'view, 'scope: 'view>(&'view self, grammar: &'g Grammar<'scope>) -> impl Iterator<Item = State<'g, 'scope>> + 'view {
        let mut current = self.current;
        std::iter::from_fn(move || {
            let record = &self.records[current?.index];
            if record.parent.is_none() { current = None; return None; }
            current = record.parent;
            let ParserState::State(number) = record.invoking else { panic!("nonroot invocation has a validated call state") };
            Some(grammar.state(number).expect("immutable owner-bound invocation state"))
        })
    }
}

#[cfg(test)]
#[derive(Clone, Copy)]
pub(super) struct RuleContextView<'a, 'parse, 'sql> { key: InvocationKey<'parse>, record: &'a InvocationRecord<'parse, 'sql> }
#[cfg(test)]
impl<'a, 'parse, 'sql> RuleContextView<'a, 'parse, 'sql> {
    pub(super) fn key(self) -> InvocationKey<'parse> { self.key }
    pub(super) fn parent(self) -> Option<InvocationKey<'parse>> { self.record.parent }
    pub(super) fn invoking_state(self) -> ParserState { self.record.invoking }
    pub(super) fn rule(self) -> u16 { self.record.rule }
    pub(super) fn class(self) -> Option<ContextClass> { self.record.class }
    pub(super) fn start(self) -> Option<IndexedLexeme<'sql>> { self.record.start }
    pub(super) fn stop(self) -> Option<IndexedLexeme<'sql>> { self.record.stop }
    pub(super) fn children(self) -> &'a [ContextChild<'parse, 'sql>] { self.record.children.as_deref().unwrap_or(&[]) }
}

enum RestorationState { Armed { origin: usize, marker: usize }, Finished }
struct PredictionRestoration<'origin, 'parse, 'sql, 'work> { input: &'origin mut TokenStream<'sql, 'work>, control: &'origin mut ParserControl<'parse, 'sql>, state: RestorationState }
impl<'origin, 'parse, 'sql, 'work> PredictionRestoration<'origin, 'parse, 'sql, 'work> {
    fn new(input: &'origin mut TokenStream<'sql, 'work>, control: &'origin mut ParserControl<'parse, 'sql>, origin: usize) -> Self { let marker = input.mark(); Self { input, control, state: RestorationState::Armed { origin, marker } } }
    fn restore(&mut self) -> RuntimeResult<'sql, ()> {
        let state = std::mem::replace(&mut self.state, RestorationState::Finished);
        if let RestorationState::Armed { origin, marker } = state { self.input.seek(origin)?; self.input.release(marker); }
        Ok(())
    }
    fn finish<T>(&mut self, result: RuntimeResult<'sql, T>) -> RuntimeResult<'sql, T> { if result.is_err() { self.control.abort(); } if let Err(error) = self.restore() { self.control.abort(); return Err(error); } result }
    fn origin(&self) -> usize { match self.state { RestorationState::Armed { origin, .. } => origin, RestorationState::Finished => panic!("active prediction origin") } }
}
impl Drop for PredictionRestoration<'_, '_, '_, '_> {
    fn drop(&mut self) { if matches!(self.state, RestorationState::Armed { .. }) { self.control.abort(); assert!(self.restore().is_ok(), "buffered prediction origin remains seekable during unwind"); } }
}
pub(super) struct PredictionInput<'input, 'origin, 'round, 'parse, 'sql, 'work> {
    restoration: &'input mut PredictionRestoration<'origin, 'parse, 'sql, 'work>, invocations: InvocationView<'input, 'parse, 'sql>,
    decision: u16, state: ParserState, precedence: i32, brand: PhantomData<fn(&'round mut ()) -> &'round mut ()>,
}
impl<'sql> PredictionInput<'_, '_, '_, '_, 'sql, '_> {
    pub(super) fn ready(&self) -> RuntimeResult<'sql, ()> { if self.restoration.control.is_ready() { Ok(()) } else { Err(RuntimeError::Control(RuntimeProblem::Aborted)) } }
    pub(super) fn abort(&mut self) { self.restoration.control.abort(); }
    pub(super) fn decision(&self) -> u16 { self.decision }
    pub(super) fn index(&self) -> Option<usize> { self.restoration.input.index() }
    pub(super) fn origin(&self) -> usize { self.restoration.origin() }
    pub(super) fn precedence(&self) -> i32 { self.precedence }
    pub(super) fn invocations<'view, 'g: 'view, 'scope: 'view>(&'view self, grammar: &'g Grammar<'scope>) -> impl Iterator<Item = State<'g, 'scope>> + 'view { self.invocations.invocations(grammar) }
    fn finish<T>(&mut self, result: RuntimeResult<'sql, T>) -> RuntimeResult<'sql, T> { if result.is_err() { self.abort(); } result }
    pub(super) fn seek(&mut self, index: usize) -> RuntimeResult<'sql, ()> { let result = self.ready().and_then(|()| self.restoration.input.seek(index).map_err(RuntimeError::Input)); self.finish(result) }
    pub(super) fn lookahead(&mut self, offset: isize) -> RuntimeResult<'sql, Option<IndexedLexeme<'sql>>> { let result = self.ready().and_then(|()| self.restoration.input.lt(offset).map_err(RuntimeError::Input)); self.finish(result) }
    pub(super) fn advance(&mut self) -> RuntimeResult<'sql, ()> { let result = self.ready().and_then(|()| self.restoration.input.consume().map_err(RuntimeError::Input)); self.finish(result) }
    pub(super) fn no_viable_alternative(&mut self) -> RuntimeResult<'sql, NoViableAlternative<'sql>> {
        self.ready()?;
        let start = self.restoration.input.buffered(self.origin()).expect("buffered prediction origin");
        let offending = self.lookahead(1)?.expect("positive lookahead");
        Ok(NoViableAlternative { start, offending, state: self.state })
    }
    pub(super) fn evaluate_precedence(&mut self, predicate: Semantic) -> RuntimeResult<'sql, bool> {
        let result = (|| {
            self.ready()?;
            let current = self.index().ok_or(RuntimeError::Control(RuntimeProblem::UninitializedOrigin))?;
            self.seek(self.origin())?;
            let result = predicate.evaluate(self.precedence);
            self.seek(current)?;
            Ok(result)
        })(); self.finish(result)
    }
}
