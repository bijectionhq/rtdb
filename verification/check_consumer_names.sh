#!/usr/bin/env bash
set -u
cd "$(git rev-parse --show-toplevel)" || exit 2
if git grep -n -I -E '(^|[^[:alnum:]_])b[j]_' -- . \
    ':(exclude,glob)rust/vendor/**' \
    ':(exclude,glob)**/test/**' \
    ':(exclude,glob)**/tests/**' \
    ':(exclude,glob)**/test-fixtures/**' \
    ':(exclude,glob)**/test_fixtures/**' \
    ':(exclude,glob)**/fixtures/**' \
    ':(exclude,glob)**/*_test.rs' \
    ':(exclude,glob)**/*_test.clj' \
    ':(exclude,glob)**/*Test.kt'; then
    echo 'Consumer-specific names remain in first-party RTDB files.' >&2
    exit 1
else
    result=$?
    if [ "$result" -ne 1 ]; then exit "$result"; fi
fi
