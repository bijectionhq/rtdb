use super::Mapping;

pub(super) static TABLES: [Mapping; 20] = [
    Mapping(include_bytes!("big5.bin")),
    Mapping(include_bytes!("euc-kr.bin")),
    Mapping(include_bytes!("gb2312.bin")),
    Mapping(include_bytes!("gbk.bin")),
    Mapping(include_bytes!("shift_jis.bin")),
    Mapping(include_bytes!("windows-31j.bin")),
    Mapping(include_bytes!("x-ibm1381.bin")),
    Mapping(include_bytes!("x-ibm942.bin")),
    Mapping(include_bytes!("x-ibm942c.bin")),
    Mapping(include_bytes!("x-ibm943.bin")),
    Mapping(include_bytes!("x-ibm943c.bin")),
    Mapping(include_bytes!("x-ibm948.bin")),
    Mapping(include_bytes!("x-ibm949.bin")),
    Mapping(include_bytes!("x-ibm949c.bin")),
    Mapping(include_bytes!("x-ibm950.bin")),
    Mapping(include_bytes!("x-johab.bin")),
    Mapping(include_bytes!("x-mswin-936.bin")),
    Mapping(include_bytes!("x-pck.bin")),
    Mapping(include_bytes!("x-windows-949.bin")),
    Mapping(include_bytes!("x-windows-950.bin")),
];
