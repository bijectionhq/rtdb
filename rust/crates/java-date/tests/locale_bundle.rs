#[path = "../build_support/locale.rs"]
mod locale;

#[test]
fn pinned_locale_bundle_retains_calendar_names_and_provider_metadata() {
    let source = locale::source(include_bytes!("../data/locale/temurin-25.0.1+8.tar")).unwrap();
    for name in ["sun/text/resources/cldr/FormatData_en", "sun/text/resources/cldr/ext/FormatData_fr", "sun/text/resources/cldr/ext/FormatData_ja", "sun/util/cldr/CLDRBaseLocaleDataMetaInfo"] {
        assert!(source.contains(name));
    }
    for key in ["MonthNames", "japanese.Eras", "parentLocalesMap", "languageAliasMap", "tzCanonicalIDMap", "likelyScriptMap"] {
        assert!(source.contains(key));
    }
    assert!(source.contains("janvier"));
    assert!(source.contains(locale::ARCHIVE_SHA256));
}
