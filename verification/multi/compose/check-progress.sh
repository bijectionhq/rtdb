#!/usr/bin/env bash
set -euo pipefail

: "${TLA2TOOLS_JAR:?Set TLA2TOOLS_JAR to the TLA+ tool jar}"
jar="$(cd "$(dirname "$TLA2TOOLS_JAR")" && pwd)/$(basename "$TLA2TOOLS_JAR")"
java="${JAVA_BIN:-java}"
case "$java" in /*) ;; */*) java="$PWD/$java";; esac

cd "$(dirname "${BASH_SOURCE[0]}")/../../.."
mkdir -p build/proof-runs/multi
run="$(mktemp -d "$PWD/build/proof-runs/multi/progress.XXXXXXXX")"
module=InstalledJournalProgress
cp "verification/multi/compose/$module.tla" "$run/"
printf 'Progress logs in %s\n' "$run"

run_case() {
    local name="$1" mutation="$2" focus="$3" checks="$4" expected_status="$5" expected_message="$6" status=0
    local log="$run/$name.log"
    cat > "$run/$name.cfg" <<CFG
SPECIFICATION Spec
CONSTANTS Mutation = "$mutation" Focus = "$focus"
CHECK_DEADLOCK FALSE
$checks
CFG
    (
        cd "$run"
        "$java" -XX:+UseParallelGC -Xmx1g -cp "$jar" tlc2.TLC \
            -workers 2 -metadir "$name.states" -config "$name.cfg" "$module.tla"
    ) > "$log" 2>&1 || status=$?

    if [[ "$status" != "$expected_status" ]] || ! rg -Fq "$expected_message" "$log"; then
        cat "$log" >&2
        exit 1
    fi
    printf '%s — %s (%s)\n' "$name" "$expected_message" "$focus"
}

run_case baseline none composed 'INVARIANTS MaintenanceAlwaysAdmitsNext SettlementAccounting TrimRequiresPublication DisposalRequiresElapsedLease CommitBindsCutAndDeadline
PROPERTIES CheckpointEventuallyPublishes PublishedCheckpointEventuallyTrims WithdrawnHoldsEventuallyDisposed
           FreshReadEventuallyAdmitted RequestedErasureEventuallyCommits CleanSlotsEventuallyCommitErasure OpenReservationEventuallyCommitsErasure
           WriterEventuallyGranted WriterEventuallyReady' 0 'No error has been found'
run_case reachability none composed 'INVARIANT NoCompletedScenario' 12 'Invariant NoCompletedScenario is violated'
run_case reserve-headroom unfunded-lane publication 'INVARIANT MaintenanceAlwaysAdmitsNext' 12 'Invariant MaintenanceAlwaysAdmitsNext is violated'

while IFS='|' read -r focus law controls; do
    run_case "$law" none "$focus" "PROPERTY $law" 0 'No error has been found'
    for control in $controls; do
        # TLC 2.19 reports an unnamed temporal violation even with just one configured property.
        run_case "$control" "$control" "$focus" "PROPERTY $law" 13 'Error: Temporal properties were violated.'
        printf '  Stalled at %s\n' "$law"
    done
done <<'LAWS'
publication|CheckpointEventuallyPublishes|unfunded-lane unfunded-checkpoint-copy unfair-settlement unfair-boundary unfair-upload unfair-prepare unfair-copy unfair-publish
trim|PublishedCheckpointEventuallyTrims|holds-never-release no-custodian unfair-trim
disposal|WithdrawnHoldsEventuallyDisposed|unfair-tick unbounded-clock unfair-dispose
read|FreshReadEventuallyAdmitted|no-usable-renewal unfair-renew no-reader-slot writer-displaced weak-read-fairness
erasure|RequestedErasureEventuallyCommits|stale-slot unfair-mirror
clean-slots|CleanSlotsEventuallyCommitErasure|unqualified-authority unfair-commit
clean-slots|OpenReservationEventuallyCommitsErasure|short-deadline
grant|WriterEventuallyGranted|no-majority no-candidate no-activation-slot unfair-grant
recovery|WriterEventuallyReady|no-provider unfair-replay unfair-confirm
LAWS
