use super::generated_actions::{CallAction, PredictionAction, SyncAction, SyncShape, TerminalAction, TokenAction};
use super::{ContextClass, InvocationKey, ParserSession, PassError, PassResult, RuntimeError, RuntimeProblem, RuntimeResult};
use super::super::{Alternative, PredictionMode};

const STATEMENT: u16 = 2;
const TARGET_TABLE: u16 = 4;
const IDENTIFIER: u16 = 22;
const COLUMN_NAME: u16 = 24;
const EXPR: u16 = 33;
const ROW_VALUE: u16 = 69;
const ROW_LIST: u16 = 70;
const VALUES_SOURCE: u16 = 71;
const COLUMN_LIST: u16 = 84;
const SELECT_LIST: u16 = 111;
const RETURNING: u16 = 153;
const INSERT_SOURCE: u16 = 157;

const COMMA: i32 = 10;
const LPAREN: i32 = 15;
const RPAREN: i32 = 16;
const RETURNING_KEYWORD: i32 = 278;

const fn call(rule: u16, state: u32, called: u16, follow: u32) -> CallAction {
    CallAction { state, rule, called, start: 2 * called as u32, follow, precedence: 0 }
}

const fn sync(rule: u16, state: u32, shape: SyncShape) -> SyncAction {
    SyncAction { state, rule, shape }
}

const fn decision(sync: SyncAction, number: u16, alternatives: usize) -> PredictionAction {
    PredictionAction { sync, decision_state: sync.state, decision: number, alternatives }
}

const fn atom(rule: u16, state: u32, token: i32, target: u32) -> TerminalAction {
    TerminalAction { state, rule, target, tokens: TokenAction::Atom(token) }
}

const INSERT: TerminalAction = atom(STATEMENT, 381, 167, 382);
const INTO: TerminalAction = atom(STATEMENT, 382, 175, 383);
const TARGET_CALL: CallAction = call(STATEMENT, 383, TARGET_TABLE, 384);
const SOURCE_CALL: CallAction = call(STATEMENT, 384, INSERT_SOURCE, 386);
const MAY_RETURN: SyncAction = sync(STATEMENT, 386, SyncShape::NullableBlock);
const RETURNING_CALL: CallAction = call(STATEMENT, 385, RETURNING, 387);
const MATCH_RETURNING: TerminalAction = atom(RETURNING, 2745, RETURNING_KEYWORD, 2746);
const RETURN_LIST: CallAction = call(RETURNING, 2746, SELECT_LIST, 2747);

const MAY_SCHEMA: PredictionAction = decision(sync(TARGET_TABLE, 585, SyncShape::RequiredBlock), 36, 2);
const SCHEMA_NAME: CallAction = call(TARGET_TABLE, 582, IDENTIFIER, 583);
const SCHEMA_DOT: TerminalAction = atom(TARGET_TABLE, 583, 11, 584);
const TABLE_NAME: CallAction = call(TARGET_TABLE, 587, IDENTIFIER, 588);

const SOURCE_CHOICE: PredictionAction = decision(sync(INSERT_SOURCE, 2804, SyncShape::RequiredBlock), 335, 3);
const MAY_COLUMNS: SyncAction = sync(INSERT_SOURCE, 2787, SyncShape::RequiredBlock);
const OPEN_COLUMNS: TerminalAction = atom(INSERT_SOURCE, 2783, LPAREN, 2784);
const COLUMNS: CallAction = call(INSERT_SOURCE, 2784, COLUMN_LIST, 2785);
const CLOSE_COLUMNS: TerminalAction = atom(INSERT_SOURCE, 2785, RPAREN, 2786);
const VALUES_CALL: CallAction = call(INSERT_SOURCE, 2789, VALUES_SOURCE, 2805);

const FIRST_COLUMN: CallAction = call(COLUMN_LIST, 2151, COLUMN_NAME, 2156);
const COLUMN_LOOP: SyncAction = sync(COLUMN_LIST, 2156, SyncShape::NullableLoopEntry);
const COLUMN_BACKEDGE: SyncAction = sync(COLUMN_LIST, 2158, SyncShape::NullableLoopback);
const COLUMN_COMMA: TerminalAction = atom(COLUMN_LIST, 2152, COMMA, 2153);
const NEXT_COLUMN: CallAction = call(COLUMN_LIST, 2153, COLUMN_NAME, 2155);
const COLUMN_IDENTIFIER: CallAction = call(COLUMN_NAME, 850, IDENTIFIER, 851);

const VALUES: TerminalAction = atom(VALUES_SOURCE, 1975, 346, 1976);
const ROWS_CALL: CallAction = call(VALUES_SOURCE, 1976, ROW_LIST, 1977);
const FIRST_ROW: CallAction = call(ROW_LIST, 1967, ROW_VALUE, 1972);
const ROW_LOOP: PredictionAction = decision(sync(ROW_LIST, 1972, SyncShape::NullableLoopEntry), 199, 2);
const ROW_BACKEDGE: PredictionAction = PredictionAction {
    sync: sync(ROW_LIST, 1974, SyncShape::NullableLoopback),
    ..ROW_LOOP
};
const ROW_COMMA: TerminalAction = atom(ROW_LIST, 1968, COMMA, 1969);
const NEXT_ROW: CallAction = call(ROW_LIST, 1969, ROW_VALUE, 1971);
const ROW_CHOICE: PredictionAction = decision(sync(ROW_VALUE, 1965, SyncShape::RequiredBlock), 198, 3);
const SINGLE_EXPRESSION: CallAction = call(ROW_VALUE, 1953, EXPR, 1966);

impl<'parse, 'sql> ParserSession<'parse, 'sql, '_, '_> {
    pub(super) fn generated_insert(&mut self, local: InvocationKey<'parse>, mode: PredictionMode) -> PassResult<'parse, 'sql, ()> {
        self.match_tree_action(local, INSERT)?;
        self.match_tree_action(local, INTO)?;
        self.generated_call(TARGET_CALL)?;
        self.target_table(mode)?;
        self.generated_call(SOURCE_CALL)?;
        self.insert_source(mode)?;
        if self.lookahead_action(MAY_RETURN)?.number() == RETURNING_KEYWORD {
            self.generated_call(RETURNING_CALL)?;
            self.generated_ordinary(RETURNING, |session, returning| {
                *returning = session.labelled_context(*returning, ContextClass::DmlReturningStatement)?;
                session.enter_outer_alt(*returning)?;
                session.match_tree_action(*returning, MATCH_RETURNING)?;
                session.generated_call(RETURN_LIST)?;
                session.select_list(mode).map(|_| ())
            })?;
        }
        Ok(())
    }

    fn target_table(&mut self, mode: PredictionMode) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_ordinary(TARGET_TABLE, |session, local| {
            if session.predict_insert(MAY_SCHEMA, mode)?.number() == 1 {
                session.generated_call(SCHEMA_NAME)?;
                session.generated_identifier()?;
                session.match_tree_action(*local, SCHEMA_DOT)?;
            }
            session.generated_call(TABLE_NAME)?;
            session.generated_identifier().map(|_| ())
        })
    }

    fn insert_source(&mut self, mode: PredictionMode) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_ordinary(INSERT_SOURCE, |session, local| {
            if session.predict_insert(SOURCE_CHOICE, mode)?.number() != 1 {
                return session.finish(Err(RuntimeError::Control(RuntimeProblem::PredictionSite))).map_err(PassError::from);
            }
            *local = session.labelled_context(*local, ContextClass::InsertValues)?;
            session.enter_outer_alt(*local)?;
            if session.lookahead_action(MAY_COLUMNS)?.number() == LPAREN {
                session.match_tree_action(*local, OPEN_COLUMNS)?;
                session.generated_call(COLUMNS)?;
                session.column_list()?;
                session.match_tree_action(*local, CLOSE_COLUMNS)?;
            }
            session.generated_call(VALUES_CALL)?;
            session.generated_ordinary(VALUES_SOURCE, |session, values| {
                session.match_tree_action(*values, VALUES)?;
                session.generated_call(ROWS_CALL)?;
                session.row_list(mode).map(|_| ())
            }).map(|_| ())
        })
    }

    fn column_list(&mut self) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_ordinary(COLUMN_LIST, |session, local| {
            session.column_name(FIRST_COLUMN)?;
            let mut repeat = COLUMN_LOOP;
            while session.lookahead_action(repeat)?.number() == COMMA {
                session.match_tree_action(*local, COLUMN_COMMA)?;
                session.column_name(NEXT_COLUMN)?;
                repeat = COLUMN_BACKEDGE;
            }
            Ok(())
        })
    }

    fn column_name(&mut self, call: CallAction) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_call(call)?;
        self.generated_ordinary(COLUMN_NAME, |session, _| {
            session.generated_call(COLUMN_IDENTIFIER)?;
            session.generated_identifier().map(|_| ())
        })
    }

    fn row_list(&mut self, mode: PredictionMode) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_ordinary(ROW_LIST, |session, local| {
            session.row_value(FIRST_ROW, mode)?;
            let mut repeat = ROW_LOOP;
            while session.predict_insert(repeat, mode)?.number() == 1 {
                session.match_tree_action(*local, ROW_COMMA)?;
                session.row_value(NEXT_ROW, mode)?;
                repeat = ROW_BACKEDGE;
            }
            Ok(())
        })
    }

    fn row_value(&mut self, call: CallAction, mode: PredictionMode) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_call(call)?;
        self.generated_ordinary(ROW_VALUE, |session, local| {
            if session.predict_insert(ROW_CHOICE, mode)?.number() != 2 {
                return session.finish(Err(RuntimeError::Control(RuntimeProblem::PredictionSite))).map_err(PassError::from);
            }
            *local = session.labelled_context(*local, ContextClass::SingleExprRowConstructor)?;
            session.enter_outer_alt(*local)?;
            session.generated_row_expression(SINGLE_EXPRESSION, mode).map(|_| ())
        })
    }

    fn predict_insert(&mut self, action: PredictionAction, mode: PredictionMode) -> RuntimeResult<'sql, Alternative> {
        self.sync_action(action.sync)?;
        let agrees = action.agrees(&self.environment.storage.grammar_view());
        if !agrees {
            return self.finish(Err(RuntimeError::Control(RuntimeProblem::PredictionSite)));
        }
        self.adaptive_predict(action.decision, mode)
    }
}
