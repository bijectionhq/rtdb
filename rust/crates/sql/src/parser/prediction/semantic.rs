use crate::parser::grammar::{Grammar, State, TransitionView};
use super::{PredictionError, Problem, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Precedence(i32);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Semantic { Unpredicated, Precedence(Precedence) }

impl Semantic {
    pub(super) fn from_state(grammar: &Grammar<'_>, state: State<'_, '_>, index: usize) -> Result<Self> {
        if !state.belongs_to(grammar) { return Err(PredictionError::ForeignOwner); }
        match state.transitions().nth(index) {
            Some(TransitionView::Precedence { precedence: value @ 1..=67, .. }) => Ok(Self::Precedence(Precedence(value))),
            _ => Err(PredictionError::Artifact(Problem::Precedence)),
        }
    }

    pub(crate) fn and(left: Option<Self>, right: Option<Self>) -> Option<Self> {
        match (left, right) {
            (None | Some(Self::Unpredicated), value) => value,
            (value, None | Some(Self::Unpredicated)) => value,
            (Some(Self::Precedence(a)), Some(Self::Precedence(b))) => Some(Self::Precedence(Precedence(a.0.min(b.0)))),
        }
    }
    pub(crate) fn or(left: Option<Self>, right: Option<Self>) -> Option<Self> {
        match (left, right) {
            (None, value) | (value, None) => value,
            (Some(Self::Unpredicated), _) | (_, Some(Self::Unpredicated)) => Some(Self::Unpredicated),
            (Some(Self::Precedence(a)), Some(Self::Precedence(b))) => Some(Self::Precedence(Precedence(a.0.max(b.0)))),
        }
    }
    pub(crate) fn evaluate(self, current: i32) -> bool {
        match self { Self::Unpredicated => false, Self::Precedence(value) => value.0 >= current }
    }
    pub(crate) fn evaluate_precedence(self, current: i32) -> Option<Self> {
        match self {
            Self::Unpredicated => Some(self),
            Self::Precedence(value) => (value.0 >= current).then_some(Self::Unpredicated),
        }
    }
    pub(super) fn hash(self) -> i32 {
        match self { Self::Unpredicated => 0, Self::Precedence(value) => 31i32.wrapping_add(value.0) }
    }
}
