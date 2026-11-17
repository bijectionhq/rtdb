use super::EucJp;

pub(super) const ALIASES: &[(&str, EucJp)] = &[
    ("cseucpkdfmtjapanese", EucJp::Standard),
    ("euc-jp", EucJp::Standard),
    ("euc-jp-linux", EucJp::Linux),
    ("euc_jp", EucJp::Standard),
    ("euc_jp_linux", EucJp::Linux),
    ("euc_jp_solaris", EucJp::Solaris),
    ("eucjis", EucJp::Standard),
    ("eucjp", EucJp::Standard),
    ("eucjp-open", EucJp::Solaris),
    ("extended_unix_code_packed_format_for_japanese", EucJp::Standard),
    ("x-euc-jp", EucJp::Standard),
    ("x-euc-jp-linux", EucJp::Linux),
    ("x-eucjp", EucJp::Standard),
    ("x-eucjp-open", EucJp::Solaris),
];
