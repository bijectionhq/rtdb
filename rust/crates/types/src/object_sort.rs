/// Stable object-array algorithm selected when a native runtime is opened.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ObjectSort {
    #[default]
    TimSort,
    LegacyMergeSort,
}

impl ObjectSort {
    pub const PROPERTY: &str = "java.util.Arrays.useLegacyMergeSort";

    /// Resolves the captured property without trimming; only case-insensitive `true` selects legacy sorting.
    /// Missing, empty and other values select TimSort; this method performs no environment or property lookup.
    pub fn from_property(value: Option<&str>) -> Self {
        if value.is_some_and(|value| value.eq_ignore_ascii_case("true")) { Self::LegacyMergeSort } else { Self::TimSort }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn captured_property_matches_java_boolean_parsing_without_trimming() {
        for value in [None,Some(""),Some("false"),Some("yes"),Some(" true"),Some("true\n")] {
            assert_eq!(ObjectSort::from_property(value),ObjectSort::TimSort);
        }
        for value in ["true","TRUE","TrUe"] { assert_eq!(ObjectSort::from_property(Some(value)),ObjectSort::LegacyMergeSort); }
    }
}
