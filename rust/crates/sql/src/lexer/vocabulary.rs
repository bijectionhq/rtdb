#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Keyword(&'static str);

impl Keyword {
    pub(crate) fn name(self) -> &'static str { self.0 }
}

const KEYWORDS: &[&str] = &[
    "ALL", "ALTER", "ANALYZE", "AND", "ANY", "ARRAY", "ARRAY_AGG", "AS", "ASC", "ASSERT", "ASYMMETRIC", "ASYNC", "AT", "ATTACH", "AVG", "AWAIT_TOKEN",
    "BEGIN", "BEGIN_FRAME", "BEGIN_PARTITION", "BETWEEN", "BIGINT", "BOOL", "BOOLEAN", "BOOL_AND", "BOOL_OR", "BOTH", "BPCHAR", "BY", "BYTEA",
    "CASE", "CAST", "CENTURY", "CHAR", "CHARACTERISTICS", "CHARACTERS", "CHARACTER_LENGTH", "CHAR_LENGTH", "CLOCK_TIME", "COALESCE", "COMMIT", "COMMITTED", "CONTAINS", "COPY", "COUNT", "CREATE", "CROSS", "CUME_DIST", "CURRENT", "CURRENT_CATALOG", "CURRENT_DATE", "CURRENT_ROW", "CURRENT_SCHEMA", "CURRENT_SCHEMAS", "CURRENT_SETTING", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER",
    "DATABASE", "DATE", "DATE_BIN", "DATE_TRUNC", "DAY", "DEC", "DECADE", "DECIMAL", "DEFAULT", "DELETE", "DENSE_RANK", "DESC", "DETACH", "DISCARD", "DISTINCT", "DOUBLE", "DOW", "DOY", "DURATION",
    "ELSE", "END", "END_FRAME", "END_PARTITION", "EPOCH", "EQUALS", "ERASE", "ESCAPE", "EVERY", "EXCEPT", "EXCLUDE", "EXECUTE", "EXISTS", "EXPLAIN", "EXTRACT",
    "FALSE", "FETCH", "FILTER", "FIRST", "FIRST_VALUE", "FLAG", "FLOAT", "FLOAT4", "FLOAT8", "FOLLOWING", "FOR", "FORMAT", "FRAME_ROW", "FROM", "FULL",
    "GENERATE_SERIES", "GRANT", "GROUP", "GROUPS",
    "HAS_ANY_COLUMN_PRIVILEGE", "HAS_SCHEMA_PRIVILEGE", "HAS_TABLE_PRIVILEGE", "HAVING", "HOUR",
    "IGNORE", "IMMEDIATELY", "IN", "INNER", "INSERT", "INT", "INT2", "INT4", "INT8", "INTEGER", "INTERSECT", "INTERVAL", "INTO", "IS", "ISOLATION", "ISODOW",
    "JOIN", "KEYWORD",
    "LAG", "LAGS", "LAST", "LAST_VALUE", "LATERAL", "LEAD", "LEADS", "LEADING", "LEFT", "LEVEL", "ILIKE", "LIKE", "LIKE_REGEX", "LIMIT", "LOCAL", "LOCAL_DATE", "LOCAL_TIME", "LOCAL_TIMESTAMP",
    "MATERIALIZED", "MAX", "METADATA", "MICROSECOND", "MILLENNIUM", "MILLISECOND", "MIN", "MINUTE", "MONTH",
    "NAME", "NANOSECOND", "NATURAL", "NEST_MANY", "NEST_ONE", "NEXT", "NO", "NONE", "NOT", "NOW", "NTH_VALUE", "NTILE", "NULL", "NULLIF", "NULLS", "NUMERIC",
    "OBJECT", "OCTETS", "OF", "OFF", "OFFSET", "OID", "ON", "ONLY", "OR", "ORDER", "ORDINALITY", "OTHERS", "OUTER", "OVER", "OVERLAPS", "OVERLAY",
    "PARTITION", "PASSWORD", "PATCH", "PERCENT_RANK", "PERIOD", "PG_GET_EXPR", "PG_GET_INDEXDEF", "PG_GET_USERBYID", "PG_SLEEP", "PG_SLEEP_FOR", "PG_TABLE_IS_VISIBLE", "PLACING", "PLANS", "PORTION", "POSITION", "PRECEDES", "PRECEDING", "PERCENTILE_CONT", "PERCENTILE_DISC", "PRECISION", "PREPARE",
    "QUARTER",
    "RANGE", "RANGE_BINS", "RANK", "READ", "REAL", "RECORD", "RECORDS", "RECURSIVE", "REGCLASS", "REGEXP_REPLACE", "REGPROC", "RENAME", "REPEATABLE", "REPLACE", "RESPECT", "RETURNING", "REVOKE", "RIGHT", "ROLE", "ROLLBACK", "ROW", "ROWS", "ROW_NUMBER",
    "SECOND", "SELECT", "SEQUENCES", "SERIALIZABLE", "SESSION", "SET", "SETTING", "SHOW", "SMALLINT", "SNAPSHOT_TIME", "SNAPSHOT_TOKEN", "SOME", "START", "STDDEV_POP", "STDDEV_SAMP", "STDIN", "STRICTLY", "SUBSTRING", "SUCCEEDS", "SUM", "SYMMETRIC", "SYNC", "SYSTEM_TIME",
    "TABLE", "TEMP", "TEMPORARY", "TEXT", "THEN", "TIES", "TIME", "TIMESTAMP", "TIMESTAMPTZ", "TIMEZONE", "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TO", "TRAILING", "TRANSACTION", "TRIM", "TRIM_ARRAY", "TRUE", "TSTZRANGE",
    "UNBOUNDED", "UNCOMMITTED", "UNION", "UNKNOWN", "UNNEST", "UPDATE", "URI", "URI_FRAGMENT", "URI_HOST", "URI_PATH", "URI_PORT", "URI_QUERY", "URI_SCHEME", "URI_USER_INFO", "USER", "USING", "UUID",
    "VALID_TIME", "VALUES", "VALUE_OF", "VARCHAR", "VAR_POP", "VAR_SAMP", "VARBINARY", "VERSION",
    "WEEK", "WHEN", "WHERE", "WINDOW", "WITHIN", "WITH", "WITHOUT", "WRITE", "XTQL", "YEAR", "ZONE",
];

const ALIASES: &[(&str, &str)] = &[
    ("ANALYSE", "ANALYZE"),
    ("LOCALDATE", "LOCAL_DATE"),
    ("LOCALTIME", "LOCAL_TIME"),
    ("LOCALTIMESTAMP", "LOCAL_TIMESTAMP"),
    ("_PG_EXPANDARRAY", "PG_EXPANDARRAY"),
];

pub(crate) fn keyword(value: &str) -> Option<Keyword> {
    KEYWORDS.iter().find(|candidate| value.eq_ignore_ascii_case(candidate)).map(|value| Keyword(value))
        .or_else(|| ALIASES.iter().find(|(spelling, _)| value.eq_ignore_ascii_case(spelling)).map(|(_, name)| Keyword(name)))
}

macro_rules! punctuation {
    ($($name:ident => ($grammar:literal, $text:literal)),+ $(,)?) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub(crate) enum Punctuation { $($name),+ }
        impl Punctuation {
            pub(crate) fn name(self) -> &'static str { match self { $(Self::$name => $grammar),+ } }
        }
        const PUNCTUATION: &[(&str, Punctuation)] = &[$(($text, Punctuation::$name)),+];
    };
}

punctuation! {
    Comma => ("COMMA", ","), Dot => ("DOT", "."), Semi => ("SEMI", ";"), Colon => ("COLON", ":"),
    Question => ("QUESTION", "?"), LeftParen => ("LPAREN", "("), RightParen => ("RPAREN", ")"),
    LeftBracket => ("LBRACK", "["), RightBracket => ("RBRACK", "]"), LeftBrace => ("LBRACE", "{"), RightBrace => ("RBRACE", "}"),
    Concat => ("CONCAT", "||"), Tilde => ("TILDE", "~"), Ampersand => ("AMPERSAND", "&"), Plus => ("PLUS", "+"),
    JsonArrowText => ("JSON_ARROW_TEXT", "->>"), JsonArrow => ("JSON_ARROW", "->"), Minus => ("MINUS", "-"),
    Asterisk => ("ASTERISK", "*"), Solidus => ("SOLIDUS", "/"), Percent => ("PERCENT", "%"),
    BitwiseOr => ("BITWISE_OR", "|"), JsonPathText => ("JSON_PATH_TEXT", "#>>"), JsonPath => ("JSON_PATH", "#>"), BitwiseXor => ("BITWISE_XOR", "#"),
    ShiftLeft => ("BITWISE_SHIFT_LEFT", "<<"), ShiftRight => ("BITWISE_SHIFT_RIGHT", ">>"),
    Equal => ("EQUAL", "="), NotEqual => ("NOT_EQUAL", "!="), LtGt => ("LT_GT", "<>"), Lt => ("LT", "<"), Gt => ("GT", ">"), Le => ("LE", "<="), Ge => ("GE", ">="),
    PgCast => ("PG_CAST", "::"), PgRegexI => ("PG_REGEX_I", "~*"), PgNotRegex => ("PG_NOT_REGEX", "!~"), PgNotRegexI => ("PG_NOT_REGEX_I", "!~*"),
}

pub(super) fn punctuation(source: &str) -> Option<(Punctuation, usize)> {
    PUNCTUATION.iter().filter(|(spelling, _)| source.starts_with(*spelling))
        .max_by_key(|(spelling, _)| spelling.len()).map(|(spelling, kind)| (*kind, spelling.len()))
}

#[cfg(test)]
pub(crate) fn spellings() -> impl Iterator<Item = (&'static str, &'static str)> {
    KEYWORDS.iter().map(|name| (*name, *name)).chain(ALIASES.iter().copied())
        .chain(PUNCTUATION.iter().map(|(spelling, kind)| (*spelling, kind.name())))
}
