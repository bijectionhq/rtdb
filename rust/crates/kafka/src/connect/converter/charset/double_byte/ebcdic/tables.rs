use super::Mapping;

pub(super) static TABLES: &[Mapping] = &[
    Mapping(include_bytes!("x-ibm1364.bin")),
    Mapping(include_bytes!("x-ibm930.bin")),
    Mapping(include_bytes!("x-ibm933.bin")),
    Mapping(include_bytes!("x-ibm935.bin")),
    Mapping(include_bytes!("x-ibm937.bin")),
    Mapping(include_bytes!("x-ibm939.bin")),
];
