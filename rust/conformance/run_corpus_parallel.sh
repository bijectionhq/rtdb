#!/usr/bin/env bash
# Runs every upstream XTDB test namespace, at the commit upstream_tests.sh pins, against the native engine in parallel
# JVMs, one per namespace, resolving the corpus task's classpath through Gradle once instead of per namespace.
# Usage: run_corpus_parallel.sh <native-library> <out-dir> <jobs> [ns...]
set -u
lib=$1; out=$2; jobs=$3; shift 3
repo=$(cd "$(dirname "$0")/../.." && pwd)
mkdir -p "$out"
tests=$("$repo/rust/conformance/upstream_tests.sh" "$out/.upstream-tests") || exit 1
init="$out/.corpus-classpath.init.gradle"
cat > "$init" <<'GRADLE'
allprojects { p ->
  p.tasks.matching { it.name == 'corpus' }.configureEach { t ->
    t.doFirst { println "CORPUS_CP=" + t.classpath.asPath; throw new StopExecutionException() }
  }
}
GRADLE
"$repo/gradlew" -q --console=plain -p "$repo" -I "$init" :bindings:xtdb-rust-bindings:corpus \
  -PcorpusOut="$out/.none.jsonl" -PcorpusSources="$tests" -Dxtdb.native.library="$lib" > "$out/.classpath.log" 2>&1
grep '^CORPUS_CP=' "$out/.classpath.log" | sed 's/^CORPUS_CP=//' > "$out/.classpath"
[ -s "$out/.classpath" ] || { echo "could not resolve the corpus classpath; see $out/.classpath.log" >&2; exit 1; }
if [ $# -gt 0 ]; then printf '%s\n' "$@"; else
  (cd "$tests/src/test/clojure" && find . -name '*_test.clj' | sed -e 's|^\./||' -e 's|\.clj$||' -e 's|/|.|g' -e 's|_|-|g' | sort)
fi | xargs -P "$jobs" -I{} sh -c '
  ns=$1; out=$2; lib=$3; repo=$4; tests=$5
  [ -s "$out/$ns.jsonl" ] && exit 0
  cd "$repo" && "${JAVA_HOME:+$JAVA_HOME/bin/}java" -Dio.netty.noUnsafe=false -Dio.netty.tryReflectionSetAccessible=true \
    -Dxtdb.corpus.out="$out/$ns.jsonl" -Dxtdb.corpus.root="$tests/src/test/clojure" -Dxtdb.corpus.fixtures-root="$tests/src/testFixtures/clojure" -Dxtdb.native.library="$lib" \
    -Dfile.encoding=UTF-8 --enable-native-access=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED \
    -Xmx4g -XX:-OmitStackTraceInFastThrow -cp "$(cat "$out/.classpath")" clojure.main -m xtdb.rust.corpus "$ns" > "$out/$ns.log" 2>&1
  status=$?
  if [ ! -s "$out/$ns.jsonl" ] && grep -q "^corpus results" "$out/$ns.log"; then
    printf "{\"var\":\"%s\",\"ns\":\"%s\",\"result\":\"no-tests\"}\n" "$ns" "$ns" > "$out/$ns.jsonl"
  elif [ ! -s "$out/$ns.jsonl" ]; then
    printf "{\"var\":\"%s\",\"ns\":\"%s\",\"result\":\"process-crash\",\"detail\":\"exit %s; see %s.log\"}\n" "$ns" "$ns" "$status" "$ns" > "$out/$ns.jsonl"
  fi
  echo "$ns done"
' _ {} "$out" "$lib" "$repo" "$tests"
python3 "$repo/rust/conformance/corpus_summary.py" "$out"
