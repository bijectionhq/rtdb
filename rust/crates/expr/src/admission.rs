use crate::EvaluationScope;
use rtdb_types::Result;

#[derive(Clone, Copy)]
pub(crate) enum Admission<'a> { Tracked(&'a EvaluationScope), Standalone }

impl<'a> Admission<'a> {
    pub(crate) fn for_context(allocation: Option<&'a EvaluationScope>) -> Self {
        match allocation { Some(allocation) => Self::Tracked(allocation), None => Self::Standalone }
    }

    pub(crate) fn reserve(self, bytes: usize) -> Result<()> {
        match self { Self::Tracked(allocation) => allocation.reserve(bytes), Self::Standalone => Ok(()) }
    }
}

impl<'a> From<&'a EvaluationScope> for Admission<'a> {
    fn from(allocation: &'a EvaluationScope) -> Self { Self::Tracked(allocation) }
}
