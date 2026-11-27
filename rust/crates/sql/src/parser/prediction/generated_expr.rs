use super::generated_actions::{CallAction, PredicateAction, PredictionAction, SyncAction, SyncShape, TerminalAction, TokenAction};
use super::{
    ContextChild, ContextClass, InvocationKey, InvocationRecord, InvocationRole, ParserControl, ParserSession, ParserState,
    PassError, PassResult, RecursionEntry, RecursionPhase, RuntimeError, RuntimeProblem, RuntimeResult,
};
use super::super::{Alternative, PredictionMode};

#[cfg(test)]
#[path = "generated_expr_tests.rs"]
mod tests;

const INTERVAL_LITERAL: u16 = 11;
const DATE_TIME_LITERAL: u16 = 12;
const INTEGER_LITERAL: u16 = 13;
const LITERAL: u16 = 14;
const DOLLAR_STRING_TEXT: u16 = 15;
const CHARACTER_STRING: u16 = 16;
const INTERVAL_QUALIFIER: u16 = 17;
const INTERVAL_PRECISION: u16 = 18;
const NON_SECOND_DATETIME_FIELD: u16 = 19;
const SINGLE_DATETIME_FIELD: u16 = 20;
const TIME_ZONE: u16 = 30;
const EXPR: u16 = 33;
const COMMON_VALUE: u16 = 34;
const SIGN_TOKENS: &[i32] = &[24, 27];

const fn prediction(rule: u16, state: u32, decision: u16, alternatives: usize, shape: SyncShape) -> PredictionAction {
    PredictionAction { sync: SyncAction { state, rule, shape }, decision_state: state, decision, alternatives }
}

const EXPR_BASE: PredictionAction = prediction(EXPR, 1001, 89, 3, SyncShape::RequiredBlock);
const EXPR_LOOP: PredictionAction = prediction(EXPR, 1131, 99, 2, SyncShape::NullablePrecedenceEntry);
const EXPR_TAIL: PredictionAction = prediction(EXPR, 1129, 98, 28, SyncShape::RequiredStarBlock);
const VALUE_BASE: PredictionAction = prediction(COMMON_VALUE, 1634, 151, 63, SyncShape::RequiredBlock);
const VALUE_LOOP: PredictionAction = prediction(COMMON_VALUE, 1661, 153, 2, SyncShape::NullablePrecedenceEntry);
const VALUE_BACKEDGE: PredictionAction = PredictionAction {
    sync: SyncAction { state: 1663, rule: COMMON_VALUE, shape: SyncShape::NullableLoopback },
    ..VALUE_LOOP
};
const VALUE_TAIL: PredictionAction = prediction(COMMON_VALUE, 1659, 152, 7, SyncShape::RequiredStarBlock);
const LITERAL_BASE: PredictionAction = prediction(LITERAL, 709, 55, 13, SyncShape::RequiredBlock);
const INTERVAL_HAS_QUALIFIER: PredictionAction = prediction(INTERVAL_LITERAL, 672, 50, 2, SyncShape::NullableBlock);
const INTERVAL_QUALIFIER_BASE: PredictionAction = prediction(INTERVAL_QUALIFIER, 730, 59, 2, SyncShape::RequiredBlock);
const INTERVAL_HAS_PRECISION: PredictionAction = prediction(SINGLE_DATETIME_FIELD, 742, 60, 2, SyncShape::NullableBlock);
const INTERVAL_OPTIONAL_SIGN: SyncAction = SyncAction { state: 668, rule: INTERVAL_LITERAL, shape: SyncShape::RequiredBlock };
const INTERVAL_FIELD_BASE: SyncAction = SyncAction { state: 744, rule: SINGLE_DATETIME_FIELD, shape: SyncShape::RequiredBlock };
const FLOAT_SIGN: SyncAction = SyncAction { state: 689, rule: LITERAL, shape: SyncShape::RequiredBlock };
const INTEGER_SIGN: SyncAction = SyncAction { state: 684, rule: INTEGER_LITERAL, shape: SyncShape::RequiredBlock };
const CHARACTER_STRING_BASE: SyncAction = SyncAction { state: 723, rule: CHARACTER_STRING, shape: SyncShape::RequiredBlock };
const DATE_TIME_BASE: SyncAction = SyncAction { state: 681, rule: DATE_TIME_LITERAL, shape: SyncShape::RequiredBlock };
const TIMESTAMP_OPTIONAL_ZONE: SyncAction = SyncAction { state: 678, rule: DATE_TIME_LITERAL, shape: SyncShape::RequiredBlock };
const TIME_ZONE_BASE: SyncAction = SyncAction { state: 978, rule: TIME_ZONE, shape: SyncShape::RequiredBlock };
const DOLLAR_BODY: SyncAction = SyncAction { state: 720, rule: CHARACTER_STRING, shape: SyncShape::RequiredBlock };
const DOLLAR_TEXT_START: SyncAction = SyncAction { state: 712, rule: DOLLAR_STRING_TEXT, shape: SyncShape::RequiredPlusBlock };
const DOLLAR_TEXT_BACKEDGE: SyncAction = SyncAction { state: 714, rule: DOLLAR_STRING_TEXT, shape: SyncShape::NullablePlusLoopback };

const EXPR_VALUE: CallAction = CallAction {
    state: 1000, rule: EXPR, called: COMMON_VALUE, start: 68, follow: 1002, precedence: 0,
};
const WRAPPED_EXPRESSION: CallAction = CallAction {
    state: 1137, rule: COMMON_VALUE, called: EXPR, start: 66, follow: 1138, precedence: 0,
};
const VALUE_LITERAL: CallAction = CallAction {
    state: 1140, rule: COMMON_VALUE, called: LITERAL, start: 28, follow: 1635, precedence: 0,
};
const LITERAL_INTEGER: CallAction = CallAction {
    state: 692, rule: LITERAL, called: INTEGER_LITERAL, start: 26, follow: 710, precedence: 0,
};
const LITERAL_CHARACTER_STRING: CallAction = CallAction {
    state: 693, rule: LITERAL, called: CHARACTER_STRING, start: 32, follow: 710, precedence: 0,
};
const LITERAL_DATE_TIME: CallAction = CallAction {
    state: 695, rule: LITERAL, called: DATE_TIME_LITERAL, start: 24, follow: 710, precedence: 0,
};
const DATE_STRING: CallAction = CallAction {
    state: 675, rule: DATE_TIME_LITERAL, called: CHARACTER_STRING, start: 32, follow: 682, precedence: 0,
};
const TIMESTAMP_ZONE: CallAction = CallAction {
    state: 677, rule: DATE_TIME_LITERAL, called: TIME_ZONE, start: 60, follow: 679, precedence: 0,
};
const TIMESTAMP_STRING: CallAction = CallAction {
    state: 680, rule: DATE_TIME_LITERAL, called: CHARACTER_STRING, start: 32, follow: 682, precedence: 0,
};
const LITERAL_INTERVAL: CallAction = CallAction {
    state: 698, rule: LITERAL, called: INTERVAL_LITERAL, start: 22, follow: 710, precedence: 0,
};
const INTERVAL_STRING: CallAction = CallAction {
    state: 670, rule: INTERVAL_LITERAL, called: CHARACTER_STRING, start: 32, follow: 672, precedence: 0,
};
const INTERVAL_QUALIFIER_CALL: CallAction = CallAction {
    state: 671, rule: INTERVAL_LITERAL, called: INTERVAL_QUALIFIER, start: 34, follow: 673, precedence: 0,
};
const INTERVAL_START_FIELD: CallAction = CallAction {
    state: 725, rule: INTERVAL_QUALIFIER, called: NON_SECOND_DATETIME_FIELD, start: 38, follow: 726, precedence: 0,
};
const INTERVAL_END_FIELD: CallAction = CallAction {
    state: 727, rule: INTERVAL_QUALIFIER, called: SINGLE_DATETIME_FIELD, start: 40, follow: 728, precedence: 0,
};
const INTERVAL_SINGLE_FIELD: CallAction = CallAction {
    state: 729, rule: INTERVAL_QUALIFIER, called: SINGLE_DATETIME_FIELD, start: 40, follow: 731, precedence: 0,
};
const INTERVAL_NON_SECOND_FIELD: CallAction = CallAction {
    state: 736, rule: SINGLE_DATETIME_FIELD, called: NON_SECOND_DATETIME_FIELD, start: 38, follow: 745, precedence: 0,
};
const INTERVAL_PRECISION_CALL: CallAction = CallAction {
    state: 739, rule: SINGLE_DATETIME_FIELD, called: INTERVAL_PRECISION, start: 36, follow: 740, precedence: 0,
};
const DOLLAR_TEXT: CallAction = CallAction {
    state: 719, rule: CHARACTER_STRING, called: DOLLAR_STRING_TEXT, start: 30, follow: 721, precedence: 0,
};
const FACTOR_RHS: CallAction = CallAction {
    state: 1641, rule: COMMON_VALUE, called: COMMON_VALUE, start: 68, follow: 1660, precedence: 62,
};
const TERM_RHS: CallAction = CallAction {
    state: 1644, rule: COMMON_VALUE, called: COMMON_VALUE, start: 68, follow: 1660, precedence: 61,
};

const MATCH_FLOAT_SIGN: TerminalAction = TerminalAction { state: 688, rule: LITERAL, target: 690, tokens: TokenAction::Set(SIGN_TOKENS) };
const MATCH_FLOAT: TerminalAction = TerminalAction { state: 691, rule: LITERAL, target: 710, tokens: TokenAction::Atom(4) };
const MATCH_INTEGER_SIGN: TerminalAction = TerminalAction { state: 683, rule: INTEGER_LITERAL, target: 685, tokens: TokenAction::Set(SIGN_TOKENS) };
const MATCH_INTEGER: TerminalAction = TerminalAction { state: 686, rule: INTEGER_LITERAL, target: 687, tokens: TokenAction::Atom(5) };
const MATCH_BOOLEAN: TerminalAction = TerminalAction { state: 707, rule: LITERAL, target: 710, tokens: TokenAction::Set(&[139, 227, 230, 326]) };
const MATCH_NULL: TerminalAction = TerminalAction { state: 708, rule: LITERAL, target: 710, tokens: TokenAction::Atom(220) };
const MATCH_WRAPPED_OPEN: TerminalAction = literal_atom(COMMON_VALUE, 1136, 15, 1137);
const MATCH_WRAPPED_CLOSE: TerminalAction = literal_atom(COMMON_VALUE, 1138, 16, 1139);
const MATCH_STANDARD_STRING: TerminalAction = TerminalAction { state: 716, rule: CHARACTER_STRING, target: 724, tokens: TokenAction::Atom(6) };
const MATCH_C_ESCAPE_STRING: TerminalAction = TerminalAction { state: 717, rule: CHARACTER_STRING, target: 724, tokens: TokenAction::Atom(7) };
const MATCH_DOLLAR_OPEN: TerminalAction = TerminalAction { state: 718, rule: CHARACTER_STRING, target: 720, tokens: TokenAction::Atom(364) };
const MATCH_DOLLAR_CLOSE: TerminalAction = TerminalAction { state: 722, rule: CHARACTER_STRING, target: 724, tokens: TokenAction::Atom(367) };
const MATCH_DOLLAR_TEXT: TerminalAction = TerminalAction { state: 711, rule: DOLLAR_STRING_TEXT, target: 713, tokens: TokenAction::Atom(368) };
const MATCH_FACTOR: TerminalAction = TerminalAction { state: 1640, rule: COMMON_VALUE, target: 1641, tokens: TokenAction::Set(&[28, 29, 30]) };
const MATCH_TERM: TerminalAction = TerminalAction { state: 1643, rule: COMMON_VALUE, target: 1644, tokens: TokenAction::Set(SIGN_TOKENS) };
const FACTOR_PREDICATE: PredicateAction = PredicateAction { state: 1639, rule: COMMON_VALUE, target: 1640, minimum: 61 };
const TERM_PREDICATE: PredicateAction = PredicateAction { state: 1642, rule: COMMON_VALUE, target: 1643, minimum: 60 };

const fn literal_atom(rule: u16, state: u32, token: i32, target: u32) -> TerminalAction {
    TerminalAction { state, rule, target, tokens: TokenAction::Atom(token) }
}

const MATCH_INTERVAL: TerminalAction = literal_atom(INTERVAL_LITERAL, 666, 174, 668);
const MATCH_INTERVAL_SIGN: TerminalAction = TerminalAction { state: 667, rule: INTERVAL_LITERAL, target: 669, tokens: TokenAction::Set(SIGN_TOKENS) };
const MATCH_INTERVAL_TO: TerminalAction = literal_atom(INTERVAL_QUALIFIER, 726, 321, 727);
const MATCH_INTERVAL_PRECISION: TerminalAction = literal_atom(INTERVAL_PRECISION, 732, 5, 733);
const MATCH_INTERVAL_NON_SECOND_FIELD: TerminalAction = TerminalAction {
    state: 734, rule: NON_SECOND_DATETIME_FIELD, target: 735, tokens: TokenAction::Set(&[109, 162, 206, 207, 362]),
};
const MATCH_INTERVAL_SECOND: TerminalAction = literal_atom(SINGLE_DATETIME_FIELD, 737, 286, 742);
const MATCH_PRECISION_OPEN: TerminalAction = literal_atom(SINGLE_DATETIME_FIELD, 738, 15, 739);
const MATCH_PRECISION_CLOSE: TerminalAction = literal_atom(SINGLE_DATETIME_FIELD, 740, 16, 741);

const MATCH_BINARY_STRING: TerminalAction = literal_atom(LITERAL, 694, 8, 710);
const MATCH_DATE: TerminalAction = literal_atom(DATE_TIME_LITERAL, 674, 106, 675);
const MATCH_TIMESTAMP: TerminalAction = literal_atom(DATE_TIME_LITERAL, 676, 316, 678);

#[derive(Clone, Copy)]
struct TimeZoneActions {
    class: ContextClass,
    prefix: TerminalAction,
    spelling: SyncAction,
    time: TerminalAction,
    zone: TerminalAction,
    timezone: TerminalAction,
}

const WITH_TIME_ZONE: TimeZoneActions = TimeZoneActions {
    class: ContextClass::WithTimeZone,
    prefix: literal_atom(TIME_ZONE, 966, 358, 970),
    spelling: SyncAction { state: 970, rule: TIME_ZONE, shape: SyncShape::RequiredBlock },
    time: literal_atom(TIME_ZONE, 967, 315, 968),
    zone: literal_atom(TIME_ZONE, 968, 363, 971),
    timezone: literal_atom(TIME_ZONE, 969, 318, 971),
};

const WITHOUT_TIME_ZONE: TimeZoneActions = TimeZoneActions {
    class: ContextClass::WithoutTimeZone,
    prefix: literal_atom(TIME_ZONE, 972, 359, 976),
    spelling: SyncAction { state: 976, rule: TIME_ZONE, shape: SyncShape::RequiredBlock },
    time: literal_atom(TIME_ZONE, 973, 315, 974),
    zone: literal_atom(TIME_ZONE, 974, 363, 977),
    timezone: literal_atom(TIME_ZONE, 975, 318, 977),
};

#[derive(Clone, Copy)]
struct PrefixedStringLiteral {
    alternative: usize,
    class: ContextClass,
    prefix: TerminalAction,
    string: CallAction,
}

const fn prefixed_string(
    alternative: usize, class: ContextClass, prefix_state: u32, token: i32, call_state: u32,
) -> PrefixedStringLiteral {
    PrefixedStringLiteral {
        alternative,
        class,
        prefix: TerminalAction { state: prefix_state, rule: LITERAL, target: call_state, tokens: TokenAction::Atom(token) },
        string: CallAction { state: call_state, rule: LITERAL, called: CHARACTER_STRING, start: 32, follow: 710, precedence: 0 },
    }
}

const PREFIXED_STRINGS: [PrefixedStringLiteral; 5] = [
    prefixed_string(6, ContextClass::TimeLiteral, 696, 315, 697),
    prefixed_string(8, ContextClass::DurationLiteral, 699, 123, 700),
    prefixed_string(9, ContextClass::UuidLiteral, 701, 344, 702),
    prefixed_string(10, ContextClass::UriLiteral, 703, 334, 704),
    prefixed_string(11, ContextClass::KeywordLiteral, 705, 180, 706),
];

#[derive(Clone, Copy)]
struct NumericTail {
    class: ContextClass,
    predicate: PredicateAction,
    terminal: TerminalAction,
    rhs: CallAction,
}

const NUMERIC_FACTOR: NumericTail = NumericTail {
    class: ContextClass::NumericFactorExpr, predicate: FACTOR_PREDICATE, terminal: MATCH_FACTOR, rhs: FACTOR_RHS,
};
const NUMERIC_TERM: NumericTail = NumericTail {
    class: ContextClass::NumericTermExpr, predicate: TERM_PREDICATE, terminal: MATCH_TERM, rhs: TERM_RHS,
};

impl<'parse, 'sql> ParserSession<'parse, 'sql, '_, '_> {
    #[cfg(test)]
    pub(in crate::parser::prediction) fn numeric_expression(&mut self) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        let mode = self.generated_mode()?;
        self.numeric_expr_zero(mode, false)
    }

    pub(super) fn derived_column_expression(&mut self, mode: PredictionMode) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_call(CallAction {
            state: 2405, rule: 120, called: EXPR, start: 66, follow: 2407, precedence: 0,
        })?;
        self.numeric_expr_zero(mode, false)
    }

    pub(super) fn generated_row_expression(&mut self, call: CallAction, mode: PredictionMode) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_call(call)?;
        self.numeric_expr_zero(mode, true)
    }

    fn numeric_expr_zero(&mut self, mode: PredictionMode, allow_wrapped_row: bool) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_numeric_recursion(EXPR, 0, |session, entry, local| {
            if session.predict_numeric(EXPR_BASE, mode)?.number() != 3 {
                return session.decline_numeric_site();
            }
            *local = session.label_recursion_base(entry, ContextClass::CommonValueExpr0)?;
            session.call_common_value(EXPR_VALUE, mode, allow_wrapped_row)?;
            session.stop_numeric_base(*local)?;
            if session.predict_numeric(EXPR_LOOP, mode)?.number() == 1 {
                session.predict_numeric(EXPR_TAIL, mode)?;
                return session.decline_numeric_site();
            }
            Ok(())
        })
    }

    fn call_common_value(&mut self, call: CallAction, mode: PredictionMode, allow_wrapped_row: bool) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_call(call)?;
        self.generated_numeric_recursion(COMMON_VALUE, call.precedence, |session, entry, local| {
            match session.predict_numeric(VALUE_BASE, mode)?.number() {
                2 if allow_wrapped_row => {
                    *local = session.label_recursion_base(entry, ContextClass::WrappedExpr)?;
                    session.match_tree_action(*local, MATCH_WRAPPED_OPEN)?;
                    session.generated_call(WRAPPED_EXPRESSION)?;
                    session.numeric_expr_zero(mode, false)?;
                    session.match_tree_action(*local, MATCH_WRAPPED_CLOSE)?;
                }
                3 => {
                    *local = session.label_recursion_base(entry, ContextClass::LiteralExpr)?;
                    session.generated_call(VALUE_LITERAL)?;
                    session.scalar_literal(mode)?;
                }
                _ => return session.decline_numeric_site(),
            }
            session.stop_numeric_base(*local)?;
            let mut next = VALUE_LOOP;
            while session.predict_numeric(next, mode)?.number() == 1 {
                let tail = match session.predict_numeric(VALUE_TAIL, mode)?.number() {
                    2 => NUMERIC_FACTOR,
                    3 => NUMERIC_TERM,
                    _ => return session.decline_numeric_site(),
                };
                let replacement = session.new_recursion_replacement(entry)?;
                session.records[replacement.key.index].class = Some(tail.class);
                *local = replacement.key;
                session.push_recursion_tree(entry, replacement)?;
                session.generated_predicate(tail.predicate)?;
                session.match_tree_action(*local, tail.terminal)?;
                session.call_common_value(tail.rhs, mode, false)?;
                next = VALUE_BACKEDGE;
            }
            Ok(())
        })
    }

    fn scalar_literal(&mut self, mode: PredictionMode) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_ordinary(LITERAL, |session, local| {
            let (class, prefixed) = match session.predict_numeric(LITERAL_BASE, mode)?.number() {
                1 => (ContextClass::FloatLiteral, None),
                2 => (ContextClass::IntegerLiteral0, None),
                3 => (ContextClass::CharacterStringLiteral, None),
                4 => (ContextClass::BinaryStringLiteral, None),
                5 => (ContextClass::DateTimeLiteral0, None),
                7 => (ContextClass::IntervalLiteral0, None),
                12 => (ContextClass::BooleanLiteral, None),
                13 => (ContextClass::NullLiteral, None),
                alternative => {
                    let Some(literal) = PREFIXED_STRINGS.into_iter().find(|literal| literal.alternative == alternative) else {
                        return session.decline_numeric_site();
                    };
                    (literal.class, Some(literal))
                }
            };
            *local = session.labelled_context(*local, class)?;
            session.enter_outer_alt(*local)?;
            if let Some(literal) = prefixed {
                session.match_tree_action(*local, literal.prefix)?;
                session.generated_call(literal.string)?;
                return session.character_string().map(|_| ());
            }
            match class {
                ContextClass::FloatLiteral => session.signed_numeric_terminal(*local, FLOAT_SIGN, MATCH_FLOAT_SIGN, MATCH_FLOAT),
                ContextClass::IntegerLiteral0 => {
                    session.generated_call(LITERAL_INTEGER)?;
                    session.generated_ordinary(INTEGER_LITERAL, |session, integer| {
                        session.signed_numeric_terminal(*integer, INTEGER_SIGN, MATCH_INTEGER_SIGN, MATCH_INTEGER)
                    }).map(|_| ())
                }
                ContextClass::CharacterStringLiteral => {
                    session.generated_call(LITERAL_CHARACTER_STRING)?;
                    session.character_string().map(|_| ())
                }
                ContextClass::BinaryStringLiteral => session.match_tree_action(*local, MATCH_BINARY_STRING),
                ContextClass::DateTimeLiteral0 => {
                    session.generated_call(LITERAL_DATE_TIME)?;
                    session.date_time_literal().map(|_| ())
                }
                ContextClass::IntervalLiteral0 => {
                    session.generated_call(LITERAL_INTERVAL)?;
                    session.interval_literal(mode).map(|_| ())
                }
                ContextClass::BooleanLiteral => session.match_tree_action(*local, MATCH_BOOLEAN),
                ContextClass::NullLiteral => session.match_tree_action(*local, MATCH_NULL),
                _ => unreachable!("selected literal class"),
            }
        })
    }

    fn date_time_literal(&mut self) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_ordinary(DATE_TIME_LITERAL, |session, local| {
            let (class, prefix, string) = match session.lookahead_action(DATE_TIME_BASE)?.number() {
                106 => (ContextClass::DateLiteral, MATCH_DATE, DATE_STRING),
                316 => (ContextClass::TimestampLiteral, MATCH_TIMESTAMP, TIMESTAMP_STRING),
                _ => return session.generated_no_viable().map_err(PassError::from),
            };
            *local = session.labelled_context(*local, class)?;
            session.enter_outer_alt(*local)?;
            session.match_tree_action(*local, prefix)?;
            if class == ContextClass::TimestampLiteral
                && matches!(session.lookahead_action(TIMESTAMP_OPTIONAL_ZONE)?.number(), 358 | 359)
            {
                session.generated_call(TIMESTAMP_ZONE)?;
                session.with_or_without_time_zone()?;
            }
            session.generated_call(string)?;
            session.character_string().map(|_| ())
        })
    }

    fn with_or_without_time_zone(&mut self) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_ordinary(TIME_ZONE, |session, local| {
            let actions = match session.lookahead_action(TIME_ZONE_BASE)?.number() {
                358 => WITH_TIME_ZONE,
                359 => WITHOUT_TIME_ZONE,
                _ => return session.generated_no_viable().map_err(PassError::from),
            };
            *local = session.labelled_context(*local, actions.class)?;
            session.enter_outer_alt(*local)?;
            session.match_tree_action(*local, actions.prefix)?;
            match session.lookahead_action(actions.spelling)?.number() {
                315 => {
                    session.match_tree_action(*local, actions.time)?;
                    session.match_tree_action(*local, actions.zone)
                }
                318 => session.match_tree_action(*local, actions.timezone),
                _ => session.generated_no_viable().map_err(PassError::from),
            }
        })
    }

    fn interval_literal(&mut self, mode: PredictionMode) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_ordinary(INTERVAL_LITERAL, |session, local| {
            session.match_tree_action(*local, MATCH_INTERVAL)?;
            if matches!(session.lookahead_action(INTERVAL_OPTIONAL_SIGN)?.number(), 24 | 27) {
                session.match_tree_action(*local, MATCH_INTERVAL_SIGN)?;
            }
            session.generated_call(INTERVAL_STRING)?;
            session.character_string()?;
            if session.predict_numeric(INTERVAL_HAS_QUALIFIER, mode)?.number() == 1 {
                session.generated_call(INTERVAL_QUALIFIER_CALL)?;
                session.interval_qualifier(mode)?;
            }
            Ok(())
        })
    }

    fn interval_qualifier(&mut self, mode: PredictionMode) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_ordinary(INTERVAL_QUALIFIER, |session, local| {
            let (class, field) = match session.predict_numeric(INTERVAL_QUALIFIER_BASE, mode)?.number() {
                1 => (ContextClass::MultiFieldInterval, INTERVAL_END_FIELD),
                2 => (ContextClass::SingleFieldInterval, INTERVAL_SINGLE_FIELD),
                _ => return session.decline_numeric_site(),
            };
            *local = session.labelled_context(*local, class)?;
            session.enter_outer_alt(*local)?;
            if class == ContextClass::MultiFieldInterval {
                session.generated_call(INTERVAL_START_FIELD)?;
                session.non_second_primary_datetime_field()?;
                session.match_tree_action(*local, MATCH_INTERVAL_TO)?;
            }
            session.generated_call(field)?;
            session.single_datetime_field(mode).map(|_| ())
        })
    }

    fn single_datetime_field(&mut self, mode: PredictionMode) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_ordinary(SINGLE_DATETIME_FIELD, |session, local| {
            match session.lookahead_action(INTERVAL_FIELD_BASE)?.number() {
                109 | 162 | 206 | 207 | 362 => {
                    session.generated_call(INTERVAL_NON_SECOND_FIELD)?;
                    session.non_second_primary_datetime_field().map(|_| ())
                }
                286 => {
                    session.match_tree_action(*local, MATCH_INTERVAL_SECOND)?;
                    if session.predict_numeric(INTERVAL_HAS_PRECISION, mode)?.number() == 1 {
                        session.match_tree_action(*local, MATCH_PRECISION_OPEN)?;
                        session.generated_call(INTERVAL_PRECISION_CALL)?;
                        session.interval_fractional_seconds_precision()?;
                        session.match_tree_action(*local, MATCH_PRECISION_CLOSE)?;
                    }
                    Ok(())
                }
                _ => session.generated_no_viable().map_err(PassError::from),
            }
        })
    }

    fn non_second_primary_datetime_field(&mut self) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_ordinary(NON_SECOND_DATETIME_FIELD, |session, local| {
            session.match_tree_action(*local, MATCH_INTERVAL_NON_SECOND_FIELD)
        })
    }

    fn interval_fractional_seconds_precision(&mut self) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_ordinary(INTERVAL_PRECISION, |session, local| {
            session.match_tree_action(*local, MATCH_INTERVAL_PRECISION)
        })
    }

    fn character_string(&mut self) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_ordinary(CHARACTER_STRING, |session, local| {
            let (class, terminal) = match session.lookahead_action(CHARACTER_STRING_BASE)?.number() {
                6 => (ContextClass::SqlStandardString, MATCH_STANDARD_STRING),
                7 => (ContextClass::CEscapesString, MATCH_C_ESCAPE_STRING),
                364 => (ContextClass::DollarString, MATCH_DOLLAR_OPEN),
                _ => return session.generated_no_viable().map_err(PassError::from),
            };
            *local = session.labelled_context(*local, class)?;
            session.enter_outer_alt(*local)?;
            session.match_tree_action(*local, terminal)?;
            if class == ContextClass::DollarString {
                if session.lookahead_action(DOLLAR_BODY)?.number() == 368 {
                    session.generated_call(DOLLAR_TEXT)?;
                    session.dollar_string_text()?;
                }
                session.match_tree_action(*local, MATCH_DOLLAR_CLOSE)?;
            }
            Ok(())
        })
    }

    fn dollar_string_text(&mut self) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_ordinary(DOLLAR_STRING_TEXT, |session, local| {
            session.lookahead_action(DOLLAR_TEXT_START)?;
            loop {
                session.match_tree_action(*local, MATCH_DOLLAR_TEXT)?;
                if session.lookahead_action(DOLLAR_TEXT_BACKEDGE)?.number() != 368 {
                    return Ok(());
                }
            }
        })
    }

    fn signed_numeric_terminal(
        &mut self, local: InvocationKey<'parse>, sync: SyncAction, sign: TerminalAction, number: TerminalAction,
    ) -> PassResult<'parse, 'sql, ()> {
        self.sync_action(sync)?;
        let (_, next) = self.lookahead_token(1)?;
        if SIGN_TOKENS.contains(&next.number()) {
            self.match_tree_action(local, sign)?;
        }
        self.match_tree_action(local, number)
    }

    fn predict_numeric(&mut self, action: PredictionAction, mode: PredictionMode) -> RuntimeResult<'sql, Alternative> {
        self.sync_action(action.sync)?;
        let agrees = action.agrees(&self.environment.storage.grammar_view());
        if !agrees {
            return self.finish(Err(RuntimeError::Control(RuntimeProblem::PredictionSite)));
        }
        self.adaptive_predict(action.decision, mode)
    }

    fn stop_numeric_base(&mut self, local: InvocationKey<'parse>) -> RuntimeResult<'sql, ()> {
        let result = self.input.lt(-1).map_err(RuntimeError::from);
        let stop = self.finish(result)?;
        self.records[local.index].stop = stop;
        Ok(())
    }

    fn decline_numeric_site<T>(&mut self) -> PassResult<'parse, 'sql, T> {
        self.finish(Err(RuntimeError::Control(RuntimeProblem::PredictionSite))).map_err(PassError::from)
    }

    fn generated_numeric_recursion(
        &mut self, rule: u16, precedence: i32,
        body: impl FnOnce(&mut Self, &RecursionEntry<'parse>, &mut InvocationKey<'parse>) -> PassResult<'parse, 'sql, ()>,
    ) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        let state = self.state;
        let site = match (rule, state, precedence) {
            (EXPR, _, 0) if self.current.is_none() => self.precedence() == 0,
            (EXPR, ParserState::State(1137 | 1953 | 2405), 0) => self.precedence() == 0,
            (COMMON_VALUE, ParserState::State(1000), 0) => self.precedence() == 0,
            (COMMON_VALUE, ParserState::State(1644), 61) => self.precedence() <= TERM_PREDICATE.minimum,
            (COMMON_VALUE, ParserState::State(1641), 62) => self.precedence() <= FACTOR_PREDICATE.minimum,
            _ => false,
        };
        if !site {
            return self.decline_numeric_site();
        }
        let pending = self.new_recursion_entry(rule)?;
        let entry = self.enter_recursion(pending, precedence)?;
        let mut local = entry.key;
        let result = match body(self, &entry, &mut local) {
            Err(PassError::Runtime(RuntimeError::NoViableAlternative(_) | RuntimeError::InputMismatch(_))) => self.catch_recognition(local),
            result => result,
        };
        self.unroll_numeric(&entry)?;
        result.map(|()| local)
    }

    fn unroll_numeric(&mut self, entry: &RecursionEntry<'parse>) -> RuntimeResult<'sql, ()> {
        let result = (|| {
            let record = self.record(entry.key)?;
            let InvocationRole::RecursiveEntry { saved, phase: RecursionPhase::Active { head } } = record.role else {
                return Err(RuntimeError::Control(RuntimeProblem::Recursion));
            };
            if !matches!(record.rule, EXPR | COMMON_VALUE) || !self.numeric_top_is(entry) {
                return Err(RuntimeError::Control(RuntimeProblem::Recursion));
            }
            let current = self.current.ok_or(RuntimeError::Control(RuntimeProblem::Recursion))?;
            if current != head && (record.rule != COMMON_VALUE || !self.failed_literal_is_current(head, current)?) {
                return Err(RuntimeError::Control(RuntimeProblem::Recursion));
            }
            let retctx = self.unroll_entered_recursion(entry.key, saved)?;
            if let Some(parent) = saved.parent {
                self.append_child_storage(parent, ContextChild::Context(retctx))?;
            }
            Ok(())
        })();
        self.finish(result)
    }

    fn numeric_top_is(&self, entry: &RecursionEntry<'parse>) -> bool {
        use super::PrecedenceFrame;

        matches!(self.precedence.last(), Some(PrecedenceFrame::Recursion { entry: top, .. }) if *top == entry.key)
    }

    fn failed_literal_is_current(&self, head: InvocationKey<'parse>, current: InvocationKey<'parse>) -> RuntimeResult<'sql, bool> {
        let literal = self.record(current)?;
        if !matches!(self.control, ParserControl::Failed)
            || self.record(head)?.class != Some(ContextClass::LiteralExpr)
            || literal.rule != LITERAL || literal.parent != Some(head)
            || literal.invoking != ParserState::State(VALUE_LITERAL.state)
            || literal.start.is_none() || literal.stop.is_some()
        {
            return Ok(false);
        }
        let call = match (literal.class, literal.role, self.state) {
            (None, InvocationRole::OrdinaryTreeAttachmentFailed, ParserState::State(28)) => return Ok(true),
            (Some(ContextClass::IntegerLiteral0), InvocationRole::Ordinary, ParserState::State(692)) => LITERAL_INTEGER,
            (Some(ContextClass::CharacterStringLiteral), InvocationRole::Ordinary, ParserState::State(693)) => LITERAL_CHARACTER_STRING,
            (Some(ContextClass::DateTimeLiteral0), InvocationRole::Ordinary, ParserState::State(695)) => LITERAL_DATE_TIME,
            (Some(ContextClass::IntervalLiteral0), InvocationRole::Ordinary, ParserState::State(698)) => LITERAL_INTERVAL,
            (Some(class), InvocationRole::Ordinary, ParserState::State(state)) => {
                let Some(literal) = PREFIXED_STRINGS.into_iter().find(|literal| literal.class == class && literal.string.state == state) else {
                    return Ok(false);
                };
                literal.string
            }
            _ => return Ok(false),
        };
        let Some(child) = self.records.last() else { return Ok(false); };
        if child.start.is_none() || !matches!(child.role, InvocationRole::OrdinaryTreeAttachmentFailed) {
            return Ok(false);
        }
        if child.rule == call.called && child.parent == Some(current) && child.invoking == ParserState::State(call.state) {
            return Ok(true);
        }
        if call.called == DATE_TIME_LITERAL {
            return self.is_failed_date_time_child(current, child);
        }
        if call.called == INTERVAL_LITERAL {
            return self.is_failed_interval_child(current, child);
        }
        if call.called != CHARACTER_STRING || child.rule != DOLLAR_STRING_TEXT
            || child.invoking != ParserState::State(DOLLAR_TEXT.state) || child.stop.is_none()
        {
            return Ok(false);
        }
        let Some(parent) = child.parent else { return Ok(false); };
        let string = self.record(parent)?;
        Ok(string.rule == CHARACTER_STRING && string.class == Some(ContextClass::DollarString)
            && string.invoking == ParserState::State(call.state) && string.parent == Some(current)
            && string.start.is_some() && string.stop.is_some() && matches!(string.role, InvocationRole::Ordinary))
    }

    fn is_failed_interval_child(
        &self, literal: InvocationKey<'parse>, child: &InvocationRecord<'parse, 'sql>,
    ) -> RuntimeResult<'sql, bool> {
        let Some(parent) = child.parent else { return Ok(false); };
        if child.class.is_some() || child.stop.is_none() {
            return Ok(false);
        }
        let invoked = |record: &InvocationRecord<'parse, 'sql>, call: CallAction| {
            record.rule == call.called && record.invoking == ParserState::State(call.state)
        };
        let exited = |record: &InvocationRecord<'parse, 'sql>, call, class| {
            invoked(record, call) && record.class == class && matches!(record.role, InvocationRole::Ordinary)
                && record.start.is_some() && record.stop.is_some()
        };

        let interval = if invoked(child, INTERVAL_STRING) || invoked(child, INTERVAL_QUALIFIER_CALL) {
            parent
        } else if invoked(child, DOLLAR_TEXT) {
            let string = self.record(parent)?;
            if !exited(string, INTERVAL_STRING, Some(ContextClass::DollarString)) {
                return Ok(false);
            }
            let Some(interval) = string.parent else { return Ok(false); };
            interval
        } else {
            let (qualifier, class) = if invoked(child, INTERVAL_START_FIELD) || invoked(child, INTERVAL_END_FIELD) {
                (parent, ContextClass::MultiFieldInterval)
            } else if invoked(child, INTERVAL_SINGLE_FIELD) {
                (parent, ContextClass::SingleFieldInterval)
            } else if invoked(child, INTERVAL_NON_SECOND_FIELD) || invoked(child, INTERVAL_PRECISION_CALL) {
                let field = self.record(parent)?;
                let class = if exited(field, INTERVAL_END_FIELD, None) {
                    ContextClass::MultiFieldInterval
                } else if exited(field, INTERVAL_SINGLE_FIELD, None) {
                    ContextClass::SingleFieldInterval
                } else {
                    return Ok(false);
                };
                let Some(qualifier) = field.parent else { return Ok(false); };
                (qualifier, class)
            } else {
                return Ok(false);
            };
            let qualifier = self.record(qualifier)?;
            if !exited(qualifier, INTERVAL_QUALIFIER_CALL, Some(class)) {
                return Ok(false);
            }
            let Some(interval) = qualifier.parent else { return Ok(false); };
            interval
        };
        let interval = self.record(interval)?;
        Ok(exited(interval, LITERAL_INTERVAL, None) && interval.parent == Some(literal))
    }

    fn is_failed_date_time_child(
        &self, literal: InvocationKey<'parse>, child: &InvocationRecord<'parse, 'sql>,
    ) -> RuntimeResult<'sql, bool> {
        let Some(parent) = child.parent else { return Ok(false); };
        if child.stop.is_none() {
            return Ok(false);
        }
        let (wrapper, rule, invoking) = if child.rule == DOLLAR_STRING_TEXT {
            let string = self.record(parent)?;
            if child.invoking != ParserState::State(DOLLAR_TEXT.state)
                || string.rule != CHARACTER_STRING || string.class != Some(ContextClass::DollarString)
                || !matches!(string.role, InvocationRole::Ordinary) || string.start.is_none() || string.stop.is_none()
            {
                return Ok(false);
            }
            let Some(wrapper) = string.parent else { return Ok(false); };
            (wrapper, CHARACTER_STRING, string.invoking)
        } else {
            (parent, child.rule, child.invoking)
        };
        let wrapper = self.record(wrapper)?;
        if wrapper.rule != DATE_TIME_LITERAL || wrapper.parent != Some(literal)
            || wrapper.invoking != ParserState::State(LITERAL_DATE_TIME.state)
            || !matches!(wrapper.role, InvocationRole::Ordinary) || wrapper.start.is_none() || wrapper.stop.is_none()
        {
            return Ok(false);
        }
        let matches_call = |call: CallAction| rule == call.called && invoking == ParserState::State(call.state);
        Ok(match wrapper.class {
            Some(ContextClass::DateLiteral) => matches_call(DATE_STRING),
            Some(ContextClass::TimestampLiteral) => matches_call(TIMESTAMP_ZONE) || matches_call(TIMESTAMP_STRING),
            _ => false,
        })
    }
}
