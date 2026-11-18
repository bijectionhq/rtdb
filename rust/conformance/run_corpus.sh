#!/usr/bin/env bash
# Runs every original test namespace against the native engine, one JVM per namespace,
# so a native abort loses only that namespace. Usage: run_corpus.sh <native-library> <out-dir> [ns...]
set -u
lib=$1; out=$2; shift 2
repo=$(cd "$(dirname "$0")/../.." && pwd)
mkdir -p "$out"
if [ $# -gt 0 ]; then namespaces=("$@"); else
  namespaces=($(cd "$repo/src/test/clojure" && find . -name '*_test.clj' | sed -e 's|^\./||' -e 's|\.clj$||' -e 's|/|.|g' -e 's|_|-|g' | sort))
fi
for ns in "${namespaces[@]}"; do
  [ -s "$out/$ns.jsonl" ] && continue
  "$repo/gradlew" -q --console=plain -p "$repo" :bindings:xtdb-rust-bindings:corpus \
    -PcorpusNs="$ns" -PcorpusOut="$out/$ns.jsonl" -Dxtdb.native.library="$lib" > "$out/$ns.log" 2>&1
  status=$?
  if [ ! -s "$out/$ns.jsonl" ] && grep -q "^corpus results" "$out/$ns.log"; then
    printf '{"var":"%s","ns":"%s","result":"no-tests"}\n' "$ns" "$ns" > "$out/$ns.jsonl"
  elif [ ! -s "$out/$ns.jsonl" ]; then
    printf '{"var":"%s","ns":"%s","result":"process-crash","detail":"exit %s; see %s.log"}\n' "$ns" "$ns" "$status" "$ns" > "$out/$ns.jsonl"
  fi
  echo "$ns $(python3 -c 'import json,sys,collections; print(dict(collections.Counter(json.loads(l)["result"] for l in open(sys.argv[1]))))' "$out/$ns.jsonl")"
done
