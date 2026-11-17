use super::{JisMapping, Mapping};

pub(super) const JIS: JisMapping = JisMapping(include_bytes!("sjis0213.dat"));
pub(super) const MS932: Mapping = Mapping(include_bytes!("windows-31j.bin"));

pub(super) const SINGLE: usize = 524;
pub(super) const DB1: usize = 1048;
pub(super) const DB2: usize = 12902;
pub(super) const SUPPLEMENTARY: usize = 23984;
pub(super) const SUPPLEMENTARY_COUNT: usize = 303;
pub(super) const COMPOSITE: usize = 26416;
pub(super) const COMPOSITE_COUNT: usize = 25;
