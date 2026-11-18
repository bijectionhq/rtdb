use super::generated_actions::{CallAction, SyncAction, SyncShape, TerminalAction, TokenAction};
use super::{ContextClass, InvocationKey, ParserSession, PassError, PassResult};

#[cfg(test)]
#[path = "generated_offset_tests.rs"]
mod tests;

const PARAMETER_SPECIFICATION: u16 = 39;
const OFFSET_AND_LIMIT: u16 = 130;
const RESULT_OFFSET_CLAUSE: u16 = 131;
const FETCH_FIRST_CLAUSE: u16 = 132;
const OFFSET_ROW_COUNT: u16 = 133;
const FETCH_FIRST_ROW_COUNT: u16 = 134;

const UNSIGNED_INTEGER: i32 = 5;
const POSTGRES_PARAMETER: i32 = 9;
const QUESTION: i32 = 14;
const FETCH: i32 = 140;
const LIMIT: i32 = 194;
const OFFSET: i32 = 228;
const ROW: i32 = 283;
const ROWS: i32 = 284;

const fn call(rule: u16, state: u32, called: u16, follow: u32) -> CallAction {
    CallAction { state, rule, called, start: 2 * called as u32, follow, precedence: 0 }
}

const fn sync(rule: u16, state: u32, shape: SyncShape) -> SyncAction {
    SyncAction { state, rule, shape }
}

const fn atom(rule: u16, state: u32, token: i32, target: u32) -> TerminalAction {
    TerminalAction { state, rule, target, tokens: TokenAction::Atom(token) }
}

const fn set(rule: u16, state: u32, tokens: &'static [i32], target: u32) -> TerminalAction {
    TerminalAction { state, rule, target, tokens: TokenAction::Set(tokens) }
}

const LIMIT_BASE: SyncAction = sync(OFFSET_AND_LIMIT, 2544, SyncShape::RequiredBlock);
const LEADING_OFFSET_CALL: CallAction = call(OFFSET_AND_LIMIT, 2536, RESULT_OFFSET_CLAUSE, 2538);
const MAY_TRAILING_FETCH: SyncAction = sync(OFFSET_AND_LIMIT, 2538, SyncShape::NullableBlock);
const TRAILING_FETCH_CALL: CallAction = call(OFFSET_AND_LIMIT, 2537, FETCH_FIRST_CLAUSE, 2539);
const LEADING_FETCH_CALL: CallAction = call(OFFSET_AND_LIMIT, 2540, FETCH_FIRST_CLAUSE, 2542);
const MAY_TRAILING_OFFSET: SyncAction = sync(OFFSET_AND_LIMIT, 2542, SyncShape::NullableBlock);
const TRAILING_OFFSET_CALL: CallAction = call(OFFSET_AND_LIMIT, 2541, RESULT_OFFSET_CLAUSE, 2543);

const OFFSET_KEYWORD: TerminalAction = atom(RESULT_OFFSET_CLAUSE, 2546, OFFSET, 2547);
const OFFSET_COUNT_CALL: CallAction = call(RESULT_OFFSET_CLAUSE, 2547, OFFSET_ROW_COUNT, 2549);
const MAY_OFFSET_ROWS: SyncAction = sync(RESULT_OFFSET_CLAUSE, 2549, SyncShape::NullableBlock);
const OFFSET_ROWS: TerminalAction = set(RESULT_OFFSET_CLAUSE, 2548, &[ROW, ROWS], 2550);

const FETCH_BASE: SyncAction = sync(FETCH_FIRST_CLAUSE, 2560, SyncShape::RequiredBlock);
const FETCH_KEYWORD: TerminalAction = atom(FETCH_FIRST_CLAUSE, 2551, FETCH, 2552);
const FETCH_DIRECTION: TerminalAction = set(FETCH_FIRST_CLAUSE, 2552, &[142, 213], 2554);
const MAY_FETCH_COUNT: SyncAction = sync(FETCH_FIRST_CLAUSE, 2554, SyncShape::RequiredBlock);
const FETCH_COUNT_CALL: CallAction = call(FETCH_FIRST_CLAUSE, 2553, FETCH_FIRST_ROW_COUNT, 2555);
const FETCH_ROWS: TerminalAction = set(FETCH_FIRST_CLAUSE, 2556, &[ROW, ROWS], 2557);
const FETCH_ONLY: TerminalAction = atom(FETCH_FIRST_CLAUSE, 2557, 231, 2561);
const LIMIT_KEYWORD: TerminalAction = atom(FETCH_FIRST_CLAUSE, 2558, LIMIT, 2559);
const LIMIT_COUNT_CALL: CallAction = call(FETCH_FIRST_CLAUSE, 2559, FETCH_FIRST_ROW_COUNT, 2561);

const OFFSET_COUNT_BASE: SyncAction = sync(OFFSET_ROW_COUNT, 2564, SyncShape::RequiredBlock);
const OFFSET_INTEGER: TerminalAction = atom(OFFSET_ROW_COUNT, 2562, UNSIGNED_INTEGER, 2565);
const OFFSET_PARAMETER_CALL: CallAction = call(OFFSET_ROW_COUNT, 2563, PARAMETER_SPECIFICATION, 2565);
const FETCH_COUNT_BASE: SyncAction = sync(FETCH_FIRST_ROW_COUNT, 2568, SyncShape::RequiredBlock);
const FETCH_INTEGER: TerminalAction = atom(FETCH_FIRST_ROW_COUNT, 2566, UNSIGNED_INTEGER, 2569);
const FETCH_PARAMETER_CALL: CallAction = call(FETCH_FIRST_ROW_COUNT, 2567, PARAMETER_SPECIFICATION, 2569);

const PARAMETER_BASE: SyncAction = sync(PARAMETER_SPECIFICATION, 1731, SyncShape::RequiredBlock);
const DYNAMIC_PARAMETER: TerminalAction = atom(PARAMETER_SPECIFICATION, 1729, QUESTION, 1732);
const POSTGRES_PARAMETER_TERMINAL: TerminalAction = atom(PARAMETER_SPECIFICATION, 1730, POSTGRES_PARAMETER, 1732);

struct RowCount { rule: u16, base: SyncAction, integer: TerminalAction, parameter: CallAction }

const OFFSET_COUNT: RowCount = RowCount {
    rule: OFFSET_ROW_COUNT, base: OFFSET_COUNT_BASE, integer: OFFSET_INTEGER, parameter: OFFSET_PARAMETER_CALL,
};
const FETCH_COUNT: RowCount = RowCount {
    rule: FETCH_FIRST_ROW_COUNT, base: FETCH_COUNT_BASE, integer: FETCH_INTEGER, parameter: FETCH_PARAMETER_CALL,
};

impl<'parse, 'sql> ParserSession<'parse, 'sql, '_, '_> {
    pub(super) fn offset_and_limit(&mut self) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_ordinary(OFFSET_AND_LIMIT, |session, _| {
            match session.lookahead_action(LIMIT_BASE)?.number() {
                OFFSET => {
                    session.generated_call(LEADING_OFFSET_CALL)?;
                    session.result_offset_clause()?;
                    if matches!(session.lookahead_action(MAY_TRAILING_FETCH)?.number(), FETCH | LIMIT) {
                        session.generated_call(TRAILING_FETCH_CALL)?;
                        session.fetch_first_clause()?;
                    }
                }
                FETCH | LIMIT => {
                    session.generated_call(LEADING_FETCH_CALL)?;
                    session.fetch_first_clause()?;
                    if session.lookahead_action(MAY_TRAILING_OFFSET)?.number() == OFFSET {
                        session.generated_call(TRAILING_OFFSET_CALL)?;
                        session.result_offset_clause()?;
                    }
                }
                _ => return session.generated_no_viable().map_err(PassError::from),
            }
            Ok(())
        })
    }

    fn result_offset_clause(&mut self) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_ordinary(RESULT_OFFSET_CLAUSE, |session, local| {
            session.match_tree_action(*local, OFFSET_KEYWORD)?;
            session.generated_call(OFFSET_COUNT_CALL)?;
            session.row_count(OFFSET_COUNT)?;
            if matches!(session.lookahead_action(MAY_OFFSET_ROWS)?.number(), ROW | ROWS) {
                session.match_tree_action(*local, OFFSET_ROWS)?;
            }
            Ok(())
        })
    }

    fn fetch_first_clause(&mut self) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_ordinary(FETCH_FIRST_CLAUSE, |session, local| {
            match session.lookahead_action(FETCH_BASE)?.number() {
                FETCH => {
                    session.match_tree_action(*local, FETCH_KEYWORD)?;
                    session.match_tree_action(*local, FETCH_DIRECTION)?;
                    if matches!(session.lookahead_action(MAY_FETCH_COUNT)?.number(), UNSIGNED_INTEGER | POSTGRES_PARAMETER | QUESTION) {
                        session.generated_call(FETCH_COUNT_CALL)?;
                        session.row_count(FETCH_COUNT)?;
                    }
                    session.match_tree_action(*local, FETCH_ROWS)?;
                    session.match_tree_action(*local, FETCH_ONLY)
                }
                LIMIT => {
                    session.match_tree_action(*local, LIMIT_KEYWORD)?;
                    session.generated_call(LIMIT_COUNT_CALL)?;
                    session.row_count(FETCH_COUNT).map(|_| ())
                }
                _ => session.generated_no_viable().map_err(PassError::from),
            }
        })
    }

    fn row_count(&mut self, count: RowCount) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_ordinary(count.rule, |session, local| {
            match session.lookahead_action(count.base)?.number() {
                UNSIGNED_INTEGER => session.match_tree_action(*local, count.integer),
                POSTGRES_PARAMETER | QUESTION => {
                    session.generated_call(count.parameter)?;
                    session.parameter_specification().map(|_| ())
                }
                _ => session.generated_no_viable().map_err(PassError::from),
            }
        })
    }

    fn parameter_specification(&mut self) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_ordinary(PARAMETER_SPECIFICATION, |session, local| {
            let (class, terminal) = match session.lookahead_action(PARAMETER_BASE)?.number() {
                QUESTION => (ContextClass::DynamicParameter, DYNAMIC_PARAMETER),
                POSTGRES_PARAMETER => (ContextClass::PostgresParameter, POSTGRES_PARAMETER_TERMINAL),
                _ => return session.generated_no_viable().map_err(PassError::from),
            };
            *local = session.labelled_context(*local, class)?;
            session.enter_outer_alt(*local)?;
            session.match_tree_action(*local, terminal)
        })
    }
}
