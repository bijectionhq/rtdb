use super::Iso2022Jp;

pub(super) const ALIASES: &[(&str, Iso2022Jp)] = &[
    ("cp50220", Iso2022Jp::Ms5022x),
    ("cp50221", Iso2022Jp::Ms5022x),
    ("csiso2022jp", Iso2022Jp::Standard),
    ("csiso2022jp2", Iso2022Jp::Jp2),
    ("csjisencoding", Iso2022Jp::Standard),
    ("iso-2022-jp", Iso2022Jp::Standard),
    ("iso-2022-jp-2", Iso2022Jp::Jp2),
    ("iso2022jp", Iso2022Jp::Standard),
    ("iso2022jp2", Iso2022Jp::Jp2),
    ("jis", Iso2022Jp::Standard),
    ("jis_encoding", Iso2022Jp::Standard),
    ("ms50220", Iso2022Jp::Ms5022x),
    ("ms50221", Iso2022Jp::Ms5022x),
    ("windows-iso2022jp", Iso2022Jp::MsIso),
    ("x-windows-50220", Iso2022Jp::Ms5022x),
    ("x-windows-50221", Iso2022Jp::Ms5022x),
    ("x-windows-iso2022jp", Iso2022Jp::MsIso),
];
