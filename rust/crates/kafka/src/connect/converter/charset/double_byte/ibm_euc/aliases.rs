use super::IbmEuc;

pub(super) const ALIASES: &[(&str, IbmEuc)] = &[
    ("29626c", IbmEuc::Ibm29626c),
    ("33722", IbmEuc::Ibm33722),
    ("cp29626c", IbmEuc::Ibm29626c),
    ("cp33722", IbmEuc::Ibm33722),
    ("ibm-29626c", IbmEuc::Ibm29626c),
    ("ibm-33722", IbmEuc::Ibm33722),
    ("ibm-33722_vascii_vpua", IbmEuc::Ibm33722),
    ("ibm-5050", IbmEuc::Ibm33722),
    ("ibm-eucjp", IbmEuc::Ibm29626c),
    ("ibm29626c", IbmEuc::Ibm29626c),
    ("ibm33722", IbmEuc::Ibm33722),
    ("x-ibm29626c", IbmEuc::Ibm29626c),
    ("x-ibm33722", IbmEuc::Ibm33722),
];
