use super::Jis0213;

pub(super) const ALIASES: &[(&str, Jis0213)] = &[
    ("ms932-0213", Jis0213::Windows),
    ("ms932:2004", Jis0213::Windows),
    ("ms932_0213", Jis0213::Windows),
    ("shift_jis:2004", Jis0213::Standard),
    ("shift_jis_0213:2004", Jis0213::Standard),
    ("sjis-0213", Jis0213::Standard),
    ("sjis:2004", Jis0213::Standard),
    ("sjis_0213", Jis0213::Standard),
    ("sjis_0213:2004", Jis0213::Standard),
    ("windows-932-0213", Jis0213::Windows),
    ("windows-932:2004", Jis0213::Windows),
    ("x-ms932_0213", Jis0213::Windows),
    ("x-sjis_0213", Jis0213::Standard),
];
