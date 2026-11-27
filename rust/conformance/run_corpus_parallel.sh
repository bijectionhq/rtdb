#!/usr/bin/env bash
# Usage: run_corpus_parallel.sh <native-library> <new-out-dir> <jobs> [ns...]
set -euo pipefail
lib=$1; out=$2; jobs=$3; shift 3
repo=$(cd "$(dirname "$0")/../.." && pwd)
[[ "$jobs" =~ ^[1-9][0-9]*$ ]] || { echo "jobs must be positive" >&2; exit 2; }
lib=$(cd "$(dirname "$lib")" && printf '%s/%s\n' "$PWD" "$(basename "$lib")")
[ -f "$lib" ] || { echo "native library does not exist: $lib" >&2; exit 2; }
mkdir -p "$(dirname "$out")"
mkdir "$out"
out=$(cd "$out" && pwd)
tests=$("$repo/rust/conformance/upstream_tests.sh" "$out/.upstream-tests")
python3 - "$repo" "$lib" "$out" "$tests" "$@" <<'PY'
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path

repo, library, output, tests, *selected = sys.argv[1:]
output, tests = Path(output), Path(tests)
root = tests / "src/test/clojure"
available = sorted(".".join(path.relative_to(root).with_suffix("").parts).replace("_", "-")
                   for path in root.rglob("*_test.clj"))
selected = sorted(selected) if selected else available
if (not selected or len(selected) != len(set(selected))
        or any(not re.fullmatch(r"[\w.-]+", name, re.ASCII) or name not in available for name in selected)):
    raise SystemExit("select distinct upstream corpus namespace names")
with open(library, "rb") as native:
    library_sha256 = hashlib.file_digest(native, "sha256").hexdigest()
git = ["git", "-C", repo]
selection = {
    "schema_version": 1,
    "upstream_commit": (tests / ".commit").read_text().strip(),
    "artifact_checkout_revision": subprocess.check_output([*git, "rev-parse", "HEAD"], text=True).strip(),
    "artifact_checkout_dirty": bool(subprocess.check_output([*git, "status", "--porcelain"])),
    "native_library_sha256": library_sha256,
    "namespaces": selected,
    "all_namespaces": selected == available,
}
(output / "selection.json").write_text(json.dumps(selection, indent=2) + "\n")
(output / ".namespaces").write_text("\n".join(selected) + "\n")
PY
init="$out/.corpus-classpath.init.gradle"
cat > "$init" <<'GRADLE'
allprojects { p ->
  p.tasks.matching { it.name == 'corpus' }.configureEach { t ->
    t.doFirst {
      println "CORPUS_CP=" + t.classpath.asPath
      println "CORPUS_JAVA=" + t.javaLauncher.get().executablePath.asFile
      throw new StopExecutionException()
    }
  }
}
GRADLE
if ! "$repo/gradlew" -q --console=plain -p "$repo" -I "$init" :bindings:xtdb-rust-bindings:corpus \
  -PcorpusSources="$tests" -Dxtdb.native.library="$lib" > "$out/.classpath.log" 2>&1; then
  echo "could not resolve the corpus classpath; see $out/.classpath.log" >&2
  python3 "$repo/rust/conformance/corpus_summary.py" "$out"
  exit 1
fi
java=$(sed -n 's/^CORPUS_JAVA=//p' "$out/.classpath.log")
classpath=$(sed -n 's/^CORPUS_CP=//p' "$out/.classpath.log")
[ -x "$java" ] && [ -n "$classpath" ] || { echo "missing corpus Java launcher or classpath" >&2; exit 1; }
printf '%s\n' "$classpath" > "$out/.classpath"
xargs -P "$jobs" -I{} sh -c '
  ns=$1; out=$2; lib=$3; repo=$4; tests=$5; java=$6
  cd "$repo" || exit 1
  "$java" -Dio.netty.noUnsafe=false -Dio.netty.tryReflectionSetAccessible=true \
    -Dxtdb.corpus.out="$out/$ns.jsonl" -Dxtdb.corpus.root="$tests/src/test/clojure" -Dxtdb.corpus.fixtures-root="$tests/src/testFixtures/clojure" -Dxtdb.native.library="$lib" \
    -Dfile.encoding=UTF-8 --enable-native-access=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED \
    -Xmx4g -XX:-OmitStackTraceInFastThrow -cp "$(cat "$out/.classpath")" clojure.main -m xtdb.rust.corpus "$ns" > "$out/$ns.log" 2>&1
  status=$?
  printf "%s\n" "$status" > "$out/$ns.exit"
  echo "$ns exit=$status"
' _ {} "$out" "$lib" "$repo" "$tests" "$java" < "$out/.namespaces"
python3 "$repo/rust/conformance/corpus_summary.py" "$out"
