use crate::parser::grammar::TokenType;
use super::{ContextClass, InvocationKey, ParserSession, PassError, PassResult};

#[cfg(test)]
#[path = "generated_names_tests.rs"]
mod tests;

const IDENTIFIER: u16 = 22;
const NON_RESERVED_KEYWORD: u16 = 23;
const PRIMARY_DATETIME_FIELD: u16 = 63;
const REGULAR_IDENTIFIER: i32 = 365;
const DELIMITED_IDENTIFIER: i32 = 366;
const FIRST_KEYWORD_ALTERNATIVE: u32 = 761;

const NON_RESERVED_CASES: [&[i32]; 87] = [
    &[298], &[125], &[88], &[329], &[318], &[352], &[308], &[345], &[287], &[167],
    &[333], &[114], &[130], &[292], &[281], &[342], &[241], &[351], &[76], &[334],
    &[229], &[90], &[150], &[61], &[117], &[105], &[190], &[141], &[309], &[201],
    &[307], &[118], &[253], &[310], &[311], &[288],
    &[62, 71, 72, 91, 132, 200, 205, 299, 300, 305, 349, 350],
    &[109, 162, 206, 207, 286, 362],
    &[121, 122, 128, 178, 262, 353],
    &[94, 115, 243, 265],
    &[60], &[67], &[75], &[95], &[142], &[183], &[213], &[226], &[255], &[169],
    &[170], &[171], &[146], &[147], &[69], &[74], &[208], &[68], &[70], &[80],
    &[106], &[110], &[112], &[120], &[123], &[145], &[168], &[172], &[174], &[180],
    &[223], &[224], &[244], &[260], &[267], &[268], &[271], &[273], &[283], &[294],
    &[312], &[315], &[316], &[317], &[327], &[344], &[348],
];

fn non_reserved_case(token: TokenType) -> Option<u32> {
    NON_RESERVED_CASES.iter().position(|tokens| tokens.contains(&token.number()))
        .map(|alternative| FIRST_KEYWORD_ALTERNATIVE + alternative as u32)
}

pub(super) fn is_identifier_token(token: TokenType) -> bool {
    matches!(token.number(), REGULAR_IDENTIFIER | DELIMITED_IDENTIFIER) || non_reserved_case(token).is_some()
}

impl<'parse, 'sql> ParserSession<'parse, 'sql, '_, '_> {
    pub(super) fn generated_identifier(&mut self) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_ordinary(IDENTIFIER, |session, local| {
            session.set_state(759)?;
            session.sync_statement()?;
            let (_, token) = session.lookahead_token(1)?;
            let class = match token.number() {
                DELIMITED_IDENTIFIER => ContextClass::DelimitedIdentifier,
                _ if is_identifier_token(token) => ContextClass::RegularIdentifier,
                _ => return session.generated_no_viable().map_err(PassError::from),
            };
            *local = session.labelled_context(*local, class)?;
            session.enter_outer_alt(*local)?;
            if class == ContextClass::DelimitedIdentifier {
                return session.match_statement_terminal(*local, 758);
            }
            session.set_state(756)?;
            session.sync_statement()?;
            let (_, token) = session.lookahead_token(1)?;
            match token.number() {
                REGULAR_IDENTIFIER => session.match_statement_terminal(*local, 754),
                _ if non_reserved_case(token).is_some() => {
                    session.set_state(755)?;
                    session.non_reserved_keyword().map(|_| ())
                }
                _ => session.generated_no_viable().map_err(PassError::from),
            }
        })
    }

    fn non_reserved_keyword(&mut self) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_ordinary(NON_RESERVED_KEYWORD, |session, local| {
            session.set_state(848)?;
            session.sync_statement()?;
            let (_, token) = session.lookahead_token(1)?;
            match non_reserved_case(token) {
                Some(797) => {
                    session.set_state(797)?;
                    session.keyword_set(147, 2718).map(|_| ())
                }
                Some(798) => {
                    session.set_state(798)?;
                    session.primary_datetime_field().map(|_| ())
                }
                Some(799) => {
                    session.set_state(799)?;
                    session.keyword_set(65, 1900).map(|_| ())
                }
                Some(800) => {
                    session.set_state(800)?;
                    session.keyword_set(44, 1804).map(|_| ())
                }
                Some(state) => session.match_statement_terminal(*local, state),
                None => session.generated_no_viable().map_err(PassError::from),
            }
        })
    }

    fn primary_datetime_field(&mut self) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_ordinary(PRIMARY_DATETIME_FIELD, |session, local| {
            session.set_state(1896)?;
            session.sync_statement()?;
            let (_, token) = session.lookahead_token(1)?;
            match token.number() {
                109 | 162 | 206 | 207 | 362 => {
                    session.set_state(1894)?;
                    session.keyword_set(19, 734).map(|_| ())
                }
                286 => session.match_statement_terminal(*local, 1895),
                _ => session.generated_no_viable().map_err(PassError::from),
            }
        })
    }

    fn keyword_set(&mut self, rule: u16, state: u32) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_ordinary(rule, |session, local| session.match_statement_set_terminal(*local, state))
    }
}
