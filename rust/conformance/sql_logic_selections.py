from pathlib import Path
import re


NON_DIRECT = {
    "slt-test": {
        "max_failures": 295, "max_errors": 4,
        "files": ["xtdb.test", "select1.test", "select2.test", "select3.test", "select4.test",
                  "random/expr/slt_good_0.test", "random/aggregates/slt_good_0.test",
                  "random/groupby/slt_good_0.test", "random/select/slt_good_0.test"],
    },
    "slt-test-2": {
        "max_failures": 10, "max_errors": 0,
        "files": ["index/between/1/slt_good_0.test", "index/commute/10/slt_good_0.test",
                  "index/in/10/slt_good_0.test", "index/orderby/10/slt_good_0.test",
                  "index/orderby_nosort/10/slt_good_0.test", "index/random/10/slt_good_0.test"],
    },
}
DIRECT = ["dml", "gcse-statistics", "numeric-value-functions-6.28", "period_predicates", "set-functions", "system_time",
          "period_specifications", "periods-and-derived-cols", "object-array", "limit", "sl-a5", "sl-demo", "no-projected-cols",
          "qualified_joins", "qualified_joins-correlated", "dollar_identifiers"]
SELECTION_SOURCES = ["build.gradle.kts", "src/test/clojure/xtdb/sql/logic_test/direct_sql_test.clj",
                     "src/test/clojure/xtdb/sql/logic_test/runner.clj", "src/test/clojure/xtdb/sql/logic_test/xtdb_engine.clj"]


def validate_pinned_selections(root):
    gradle = (root / SELECTION_SOURCES[0]).read_text()
    for name, expected in NON_DIRECT.items():
        match = re.search(r'createSltTask\(\s*"' + re.escape(name) + r'",([\s\S]*?)\n\)', gradle)
        if not match:
            raise ValueError("pinned Gradle SLT selection disappeared")
        body = re.sub(r"//[^\n]*", "", match[1])
        files = re.search(r"testFiles\s*=\s*listOf\(([\s\S]*?)\)", body)
        if not files or re.findall(r'"([^"]*)"', files[1]) != expected["files"]:
            raise ValueError("pinned Gradle SLT files differ from adapter selection")
        for source_key, key in (("maxFailures", "max_failures"), ("maxErrors", "max_errors")):
            value = re.search(source_key + r"\s*=\s*(\d+)", body)
            if (int(value[1]) if value else 0) != expected[key]:
                raise ValueError("pinned SLT task allowances differ")
    direct = (root / SELECTION_SOURCES[1]).read_text()
    matches = re.findall(r'\(slt/def-slt-test\s+direct-sql--([^\s]+)\s+\{([^}]*)\}(?:\s+"([^"]*)")?\)', direct)
    if [name for name, _, _ in matches] != DIRECT:
        raise ValueError("pinned direct SQL test variants differ")
    for name, options, source in matches:
        expected_options = ":direct-sql true, :decorrelate? false" if name.endswith("-correlated") else ":direct-sql true"
        if options != expected_options or source != ("direct-sql/qualified_joins.test" if name.endswith("-correlated") else ""):
            raise ValueError("pinned direct SQL options differ")


def selections(names, directory=None):
    selected = []
    for name in names:
        if name in NON_DIRECT:
            selected.extend((name, "sqlite-test/" + source, False, True) for source in NON_DIRECT[name]["files"])
        elif name == "direct-sql-test":
            selected.extend(("xtdb.sql.logic-test.direct-sql-test/direct-sql--" + source,
                             "src/test/resources/xtdb/sql/logic_test/direct-sql/" + source.removesuffix("-correlated") + ".test",
                             True, not source.endswith("-correlated")) for source in DIRECT)
        elif name == "slt-test-dir":
            if directory is None:
                raise ValueError("slt-test-dir requires an explicit repository-relative directory")
            root, relative = directory
            path = Path(relative)
            if path.is_absolute() or any(part in (".", "..") for part in path.parts):
                raise ValueError("invalid SLT directory selection")
            base = root / "sqlite-test" / path
            if not base.is_dir() or base.is_symlink() or root.resolve() not in base.resolve().parents:
                raise ValueError("SLT directory lies outside reference checkout")
            for source in sorted(base.rglob("*")):
                if source.is_symlink():
                    raise ValueError("SLT directory contains a symbolic link")
                if source.is_file():
                    selected.append(("slt-test-dir:" + str(path), str(source.relative_to(root)), False, True))
        else:
            raise ValueError("unknown pinned SQL logic selection: " + name)
    identities = [(name, source) for name, source, _, _ in selected]
    if len(set(identities)) != len(identities) or not selected:
        raise ValueError("SQL logic selection is empty or repeated")
    return selected
