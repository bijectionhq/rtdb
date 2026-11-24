#!/usr/bin/env bash
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
paths=(
    .
    ':(exclude,glob)rust/vendor/**'
    ':(exclude,glob)**/test/**'
    ':(exclude,glob)**/tests/**'
    ':(exclude,glob)**/test-fixtures/**'
    ':(exclude,glob)**/test_fixtures/**'
    ':(exclude,glob)**/fixtures/**'
    ':(exclude,glob)**/*_test.rs'
    ':(exclude,glob)**/*_test.clj'
    ':(exclude,glob)**/*Test.kt'
)
pattern='(^|[^[:alnum:]_])b[j]_'

if [[ $# -eq 1 ]]; then
    base=$(git rev-parse --verify --end-of-options "$1^{commit}")
    git diff --no-ext-diff --no-textconv --no-color --find-renames --unified=0 "$base" HEAD -- "${paths[@]}" |
        awk -v pattern="$pattern" '
            /^diff --git / { path = $0; hunk = 0 }
            /^@@ / { hunk = 1 }
            hunk && substr($0, 1, 1) == "+" && substr($0, 2) ~ pattern {
                print path
                print $0
                found = 1
            }
            END { exit found ? 1 : 0 }
        '
    exit 0
fi

if [[ $# -ne 0 ]]; then
    echo 'Usage: check_consumer_names.sh [base-commit]' >&2
    exit 2
fi
if git grep -n -I -E "$pattern" -- "${paths[@]}"; then
    echo 'Consumer-specific names remain in first-party RTDB files.' >&2
    exit 1
else
    result=$?
    if [[ $result -ne 1 ]]; then exit "$result"; fi
fi
