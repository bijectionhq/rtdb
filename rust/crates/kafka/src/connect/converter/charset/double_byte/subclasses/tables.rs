use super::{Decoder, Mapping};

pub(super) static MAPPINGS: &[Mapping] = &[
    Mapping(include_bytes!("jis_x0212-1990.bin")),
    Mapping(include_bytes!("x-ibm1383.bin")),
    Mapping(include_bytes!("x-ibm300.bin")),
    Mapping(include_bytes!("x-ibm834.bin")),
    Mapping(include_bytes!("x-ibm970.bin")),
    Mapping(include_bytes!("x-jis0208.bin")),
];

pub(super) static DECODERS: &[Decoder] = &[
    Decoder::DbcsOnly(&MAPPINGS[0]),
    Decoder::EucSimple(&MAPPINGS[1]),
    Decoder::DbcsOnly(&MAPPINGS[2]),
    Decoder::DbcsOnly(&MAPPINGS[3]),
    Decoder::EucSimple(&MAPPINGS[4]),
    Decoder::DbcsOnly(&MAPPINGS[5]),
];
