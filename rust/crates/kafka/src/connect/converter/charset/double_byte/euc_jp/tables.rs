use super::Mapping;

pub(super) const JIS0201: &[u8; 512] = include_bytes!("jis_x_0201.bin");
pub(super) static JIS0208: Mapping = Mapping(include_bytes!("jis_x_0208.bin"));
pub(super) static JIS0212: Mapping = Mapping(include_bytes!("jis_x_0212.bin"));
pub(super) static SOLARIS0208: Mapping = Mapping(include_bytes!("jis_x_0208_solaris.bin"));
pub(super) static SOLARIS0212: Mapping = Mapping(include_bytes!("jis_x_0212_solaris.bin"));
