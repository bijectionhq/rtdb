use super::EucTw;

pub(super) const ALIASES: &[(&str, EucTw)] = &[
    ("964", EucTw::Ibm964),
    ("cns11643", EucTw::Standard),
    ("cp964", EucTw::Ibm964),
    ("euc-tw", EucTw::Standard),
    ("euc_tw", EucTw::Standard),
    ("euctw", EucTw::Standard),
    ("ibm-964", EucTw::Ibm964),
    ("ibm-euctw", EucTw::Ibm964),
    ("ibm964", EucTw::Ibm964),
    ("x-euc-tw", EucTw::Standard),
    ("x-ibm964", EucTw::Ibm964),
];
