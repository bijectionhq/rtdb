use super::Iso2022CnKr;

pub(super) const ALIASES: &[(&str, Iso2022CnKr)] = &[
    ("csiso2022cn", Iso2022CnKr::Cn),
    ("csiso2022kr", Iso2022CnKr::Kr),
    ("iso-2022-cn", Iso2022CnKr::Cn),
    ("iso-2022-cn-cns", Iso2022CnKr::Cn),
    ("iso-2022-cn-gb", Iso2022CnKr::Cn),
    ("iso-2022-kr", Iso2022CnKr::Kr),
    ("iso2022cn", Iso2022CnKr::Cn),
    ("iso2022cn_cns", Iso2022CnKr::Cn),
    ("iso2022cn_gb", Iso2022CnKr::Cn),
    ("iso2022kr", Iso2022CnKr::Kr),
    ("x-iso-2022-cn-cns", Iso2022CnKr::Cn),
    ("x-iso-2022-cn-gb", Iso2022CnKr::Cn),
];
