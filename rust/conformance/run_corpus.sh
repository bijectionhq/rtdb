#!/usr/bin/env bash
set -euo pipefail
lib=$1; out=$2; shift 2
exec "$(dirname "$0")/run_corpus_parallel.sh" "$lib" "$out" 1 "$@"
