#!/usr/bin/env bash
# Extracts upstream XTDB's own Clojure test sources, test resources and test fixtures at the pinned commit into a
# read-only tree, and prints its path. The corpus loads these instead of this repository's copies, which carry RTDB edits.
# Usage: upstream_tests.sh <dest-dir>
#   XTDB_UPSTREAM_COMMIT  the upstream commit (default: upstream-commit.txt)
#   XTDB_UPSTREAM_REPO    a git repository holding that commit (default: this one); without the commit, fetch it first:
#                         git fetch --filter=blob:none https://github.com/xtdb/xtdb.git <commit>
set -euo pipefail
TEST_PATHS=(src/test/clojure src/test/resources src/testFixtures/clojure src/testFixtures/resources)
dest=$1
repo=$(cd "$(dirname "$0")/../.." && pwd)
commit=${XTDB_UPSTREAM_COMMIT:-$(cat "$repo/rust/conformance/upstream-commit.txt")}
source_repo=${XTDB_UPSTREAM_REPO:-$repo}
if ! git -C "$source_repo" cat-file -e "$commit^{commit}" 2>/dev/null; then
  echo "upstream commit $commit is not in $source_repo; run: git -C $source_repo fetch --filter=blob:none https://github.com/xtdb/xtdb.git $commit" >&2
  exit 1
fi
commit=$(git -C "$source_repo" rev-parse "$commit^{commit}")
mkdir -p "$(dirname "$dest")"
mkdir "$dest"
git -C "$source_repo" archive --format=tar "$commit" -- "${TEST_PATHS[@]}" | tar -x -C "$dest"
# git archive stamps every file with the commit time, and Clojure loads a compiled __init.class instead of a .clj
# that is not strictly newer, so restamp the sources to now.
find "$dest" -type f -exec touch {} +
git -C "$source_repo" ls-tree -r "$commit" -- "${TEST_PATHS[@]}" > "$dest/MANIFEST"
echo "$commit" > "$dest/.commit"
chmod -R a-w "$dest"
echo "$dest"
