use super::{IbmTables, StandardPlanes};

pub(super) static STANDARD: StandardPlanes = StandardPlanes {
    planes: include_bytes!("euc-tw-planes.bin"),
    supplementary: include_bytes!("euc-tw-supp.bin"),
};
pub(super) static IBM964: IbmTables = IbmTables(include_bytes!("ibm964.bin"));
