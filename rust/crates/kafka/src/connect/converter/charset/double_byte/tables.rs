use super::Mapping;

pub(super) static TABLES: &[Mapping] = &[
    Mapping(include_bytes!("shift_jis.bin")),
    Mapping(include_bytes!("windows-31j.bin")),
];
