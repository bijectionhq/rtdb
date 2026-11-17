use super::{Base, Hkscs, Overlay};

pub(super) static OVERLAYS: &[Overlay] = &[
    Overlay(include_bytes!("hkscsmapping.bin")),
    Overlay(include_bytes!("hkscs2001mapping.bin")),
    Overlay(include_bytes!("hkscs_xpmapping.bin")),
];

pub(super) static TABLES: &[Hkscs] = &[
    Hkscs { overlay: &OVERLAYS[0], base: Base::Big5(&super::super::big5::FAMILY) },
    Hkscs { overlay: &OVERLAYS[1], base: Base::Big5(&super::super::big5::FAMILY) },
    Hkscs { overlay: &OVERLAYS[0], base: Base::Ms950(&super::super::tables::TABLES[19]) },
    Hkscs { overlay: &OVERLAYS[2], base: Base::Ms950(&super::super::tables::TABLES[19]) },
];
