//! Java's locale-sensitive `String.toUpperCase(Locale)` and `String.toLowerCase(Locale)`.

use unicode_normalization::char::canonical_combining_class;

const ABOVE: u8 = 230;
const DOT_ABOVE: char = '\u{307}';
const CAPITAL_SIGMA: char = '\u{3A3}';

/// The case-mapping rules Java's `String` selects from a locale's language.
/// `Root` is Unicode full case mapping with its unconditional special casing (`ß` upper-cases to `SS`) and the
/// final-sigma condition, which is what every language other than Turkish, Azeri and Lithuanian gets.
/// `Turkic` and `Lithuanian` add the language-conditional rows of OpenJDK's `ConditionalSpecialCasing`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CaseLanguage { Root, Turkic, Lithuanian }

impl CaseLanguage {
    /// The rules for a `Locale.getLanguage()` code, compared ASCII case-insensitively as `Locale` normalizes it.
    pub fn of(language: &str) -> Self {
        if ["tr", "az"].iter().any(|code| language.eq_ignore_ascii_case(code)) { Self::Turkic }
        else if language.eq_ignore_ascii_case("lt") { Self::Lithuanian }
        else { Self::Root }
    }

    /// `String.toUpperCase` under a locale of this language.
    pub fn to_upper(self, text: &str) -> String {
        if self == Self::Root { return text.to_uppercase(); }
        let mut out = String::with_capacity(text.len());
        for (index, character) in text.char_indices() {
            match (self, character) {
                (Self::Turkic, 'i') => out.push('\u{130}'),
                (Self::Lithuanian, DOT_ABOVE) if after_soft_dotted(&text[..index]) => {}
                _ => out.extend(character.to_uppercase()),
            }
        }
        out
    }

    /// `String.toLowerCase` under a locale of this language.
    pub fn to_lower(self, text: &str) -> String {
        let root = text.to_lowercase();
        if self == Self::Root { return root; }
        // `str::to_lowercase` maps each character on its own except a capital sigma, which becomes exactly one
        // character under the final-sigma condition; walking both strings in step keeps that context-sensitive result.
        let mut mapped = root.chars();
        let mut out = String::with_capacity(root.len());
        for (index, character) in text.char_indices() {
            let width = if character == CAPITAL_SIGMA { 1 } else { character.to_lowercase().count() };
            let rest = &text[index + character.len_utf8()..];
            let tailored: Option<&str> = match (self, character) {
                (Self::Turkic, '\u{130}') => Some("i"),
                (Self::Turkic, DOT_ABOVE) if after_capital_i(&text[..index]) => Some(""),
                (Self::Turkic, 'I') if !before_dot_above(rest) => Some("\u{131}"),
                (Self::Lithuanian, 'I') if more_above(rest) => Some("i\u{307}"),
                (Self::Lithuanian, 'J') if more_above(rest) => Some("j\u{307}"),
                (Self::Lithuanian, '\u{12E}') if more_above(rest) => Some("\u{12F}\u{307}"),
                (Self::Lithuanian, '\u{CC}') => Some("i\u{307}\u{300}"),
                (Self::Lithuanian, '\u{CD}') => Some("i\u{307}\u{301}"),
                (Self::Lithuanian, '\u{128}') => Some("i\u{307}\u{303}"),
                _ => None,
            };
            match tailored {
                Some(replacement) => { out.push_str(replacement); for _ in 0..width { mapped.next(); } }
                None => out.extend(mapped.by_ref().take(width)),
            }
        }
        out
    }
}

/// Whether the nearest preceding character of combining class 0 or 230 is soft-dotted, per OpenJDK's fixed list.
fn after_soft_dotted(before: &str) -> bool {
    preceding(before, |character| matches!(character, 'i' | 'j' | '\u{12F}' | '\u{268}' | '\u{456}' | '\u{458}' | '\u{1D62}' | '\u{1E2D}' | '\u{1ECB}' | '\u{2071}'))
}

fn after_capital_i(before: &str) -> bool { preceding(before, |character| character == 'I') }

fn preceding(before: &str, matches: impl Fn(char) -> bool) -> bool {
    for character in before.chars().rev() {
        if matches(character) { return true; }
        if matches!(canonical_combining_class(character), 0 | ABOVE) { return false; }
    }
    false
}

/// Whether a combining mark of class 230 follows before any character of class 0.
fn more_above(after: &str) -> bool {
    for character in after.chars() {
        match canonical_combining_class(character) { ABOVE => return true, 0 => return false, _ => {} }
    }
    false
}

/// Whether U+0307 follows before any other character of combining class 0 or 230.
fn before_dot_above(after: &str) -> bool {
    for character in after.chars() {
        if character == DOT_ABOVE { return true; }
        if matches!(canonical_combining_class(character), 0 | ABOVE) { return false; }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::CaseLanguage::{self, Lithuanian, Root, Turkic};

    #[test]
    fn languages_select_rules_as_locale_language_codes() {
        assert_eq!(CaseLanguage::of("tr"), Turkic);
        assert_eq!(CaseLanguage::of("AZ"), Turkic);
        assert_eq!(CaseLanguage::of("lt"), Lithuanian);
        assert_eq!(CaseLanguage::of("en"), Root);
        assert_eq!(CaseLanguage::of(""), Root);
    }

    #[test]
    fn root_rules_apply_unconditional_special_casing_and_final_sigma() {
        assert_eq!(Root.to_upper("straße ﬁne"), "STRASSE FINE");
        assert_eq!(Root.to_upper("istanbul"), "ISTANBUL");
        assert_eq!(Root.to_lower("İSTANBUL"), "i\u{307}stanbul");
        assert_eq!(Root.to_lower("\u{39F}\u{394}\u{39F}\u{3A3} \u{39F}\u{394}\u{39F}\u{3A3}"), "\u{3BF}\u{3B4}\u{3BF}\u{3C2} \u{3BF}\u{3B4}\u{3BF}\u{3C2}");
    }

    #[test]
    fn turkic_rules_keep_the_dot_on_i() {
        assert_eq!(Turkic.to_upper("istanbul ılık"), "İSTANBUL ILIK");
        assert_eq!(Turkic.to_lower("İSTANBUL ILIK"), "istanbul ılık");
        assert_eq!(Turkic.to_lower("I\u{307}"), "i");
        assert_eq!(Turkic.to_lower("I\u{316}\u{307}"), "i\u{316}");
        assert_eq!(Turkic.to_lower("I\u{301}\u{307}"), "\u{131}\u{301}\u{307}");
        assert_eq!(Turkic.to_upper("straße"), "STRASSE");
    }

    #[test]
    fn lithuanian_rules_retain_and_drop_the_dot_above_soft_dotted_letters() {
        assert_eq!(Lithuanian.to_lower("I\u{300}"), "i\u{307}\u{300}");
        assert_eq!(Lithuanian.to_lower("J\u{301}"), "j\u{307}\u{301}");
        assert_eq!(Lithuanian.to_lower("\u{12E}\u{303}"), "\u{12F}\u{307}\u{303}");
        assert_eq!(Lithuanian.to_lower("IJ"), "ij");
        assert_eq!(Lithuanian.to_lower("\u{CC}\u{CD}\u{128}"), "i\u{307}\u{300}i\u{307}\u{301}i\u{307}\u{303}");
        assert_eq!(Lithuanian.to_upper("i\u{307}\u{300}"), "I\u{300}");
        assert_eq!(Lithuanian.to_upper("a\u{307}"), "A\u{307}");
    }

    #[test]
    fn tailored_lower_case_keeps_the_final_sigma_condition() {
        assert_eq!(Turkic.to_lower("\u{39F}\u{394}\u{39F}\u{3A3} \u{130}\u{3A3} \u{3A3}"), "\u{3BF}\u{3B4}\u{3BF}\u{3C2} i\u{3C2} \u{3C3}");
        assert_eq!(Lithuanian.to_lower("\u{391}\u{3A3}\u{CC}"), "\u{3B1}\u{3C3}i\u{307}\u{300}");
    }
}
