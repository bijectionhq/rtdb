use super::{CnsPlane, Mapping};

pub(super) static GB2312: Mapping = Mapping(include_bytes!("gb2312.bin"));
pub(super) static KSC5601: Mapping = Mapping(include_bytes!("euc-kr.bin"));
pub(super) static CNS1: CnsPlane = CnsPlane(include_bytes!("cns1.bin"));
pub(super) static CNS2: CnsPlane = CnsPlane(include_bytes!("cns2.bin"));
pub(super) static CNS3: CnsPlane = CnsPlane(include_bytes!("cns3.bin"));
