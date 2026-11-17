use super::Mapping;

pub(super) static JIS0208: Mapping = Mapping(include_bytes!("jis_x_0208.bin"));
pub(super) static JIS0212: Mapping = Mapping(include_bytes!("jis_x_0212.bin"));
pub(super) static MS5022X_0208: Mapping = Mapping(include_bytes!("jis_x_0208_ms5022x.bin"));
pub(super) static MS5022X_0212: Mapping = Mapping(include_bytes!("jis_x_0212_ms5022x.bin"));
pub(super) static MS932_0208: Mapping = Mapping(include_bytes!("jis_x_0208_ms932.bin"));
